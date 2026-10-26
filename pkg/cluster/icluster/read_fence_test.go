package icluster

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

type flagFence struct{ fenced map[uint32]*atomic.Bool }

func newFlagFence(ids ...uint32) *flagFence {
	f := &flagFence{fenced: map[uint32]*atomic.Bool{}}
	for _, id := range ids {
		f.fenced[id] = &atomic.Bool{}
	}
	return f
}

func (f *flagFence) CheckSlotReadable(id uint32) error {
	if b := f.fenced[id]; b != nil && b.Load() {
		return fmt.Errorf("%w: slot %d", types.ErrSlotDiverged, id)
	}
	return nil
}

// T25e-5: a quarantine that starts during the read discards the rows.
func TestFencedReadRecheckDiscardsRows(t *testing.T) {
	f := newFlagFence(3)
	rows, err := FencedRead(f, 3, func() ([]string, error) {
		f.fenced[3].Store(true) // transition between check and read completion
		return []string{"row"}, nil
	})
	if !errors.Is(err, types.ErrSlotDiverged) || rows != nil {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
}

// A fenced read never looks like absence, even when the read found nothing.
func TestFencedReadNeverReportsNotFound(t *testing.T) {
	f := newFlagFence(3)
	f.fenced[3].Store(true)
	_, err := FencedRead(f, 3, func() (int, error) { return 0, wkdb.ErrNotFound })
	if !errors.Is(err, types.ErrSlotDiverged) || errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	f = newFlagFence(3)
	calls := 0
	_, err = FencedRead(f, 3, func() (int, error) {
		calls++
		f.fenced[3].Store(true)
		return 0, wkdb.ErrNotFound
	})
	if !errors.Is(err, types.ErrSlotDiverged) || errors.Is(err, wkdb.ErrNotFound) || calls != 1 {
		t.Fatalf("err = %v calls=%d", err, calls)
	}
}

func TestFencedReadPassesThroughWhenHealthy(t *testing.T) {
	f := newFlagFence(3, 4)
	f.fenced[4].Store(true)
	v, err := FencedRead(f, 3, func() (int, error) { return 7, nil })
	if err != nil || v != 7 {
		t.Fatalf("v=%d err=%v", v, err)
	}
	_, err = FencedRead(f, 3, func() (int, error) { return 0, wkdb.ErrNotFound })
	if !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestFencedReadSlotsRequiresEverySlot(t *testing.T) {
	f := newFlagFence(1, 2)
	f.fenced[2].Store(true)
	read := 0
	_, err := FencedReadSlots(f, []uint32{1, 2}, func() (int, error) { read++; return 1, nil })
	if !errors.Is(err, types.ErrSlotDiverged) || read != 0 {
		t.Fatalf("err=%v read=%d", err, read)
	}
	f.fenced[2].Store(false)
	_, err = FencedReadSlots(f, []uint32{1, 2}, func() (int, error) { f.fenced[1].Store(true); return 1, nil })
	if !errors.Is(err, types.ErrSlotDiverged) {
		t.Fatalf("recheck err=%v", err)
	}
}
