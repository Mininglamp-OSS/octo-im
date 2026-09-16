package slot

import (
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func TestApplyDoesNotSerializeDifferentSlotsInSameDBShard(t *testing.T) {
	t.Parallel()

	storage, entered, release := newBlockingApplyStorage(t)

	errCh := make(chan error, 2)
	go func() {
		errCh <- storage.Apply("1", []types.Log{{Index: 1}})
	}()

	waitForApply(t, entered, "first slot")

	go func() {
		errCh <- storage.Apply("2", []types.Log{{Index: 1}})
	}()

	waitForApply(t, entered, "second slot")
	close(release)

	for range 2 {
		if err := <-errCh; !errors.Is(err, errStopBeforeAppliedIndex) {
			t.Fatalf("Apply() error = %v, want %v", err, errStopBeforeAppliedIndex)
		}
	}
}

func TestApplySerializesSameSlot(t *testing.T) {
	t.Parallel()

	storage, entered, release := newBlockingApplyStorage(t)

	errCh := make(chan error, 2)
	go func() {
		errCh <- storage.Apply("1", []types.Log{{Index: 1}})
	}()

	waitForApply(t, entered, "first apply")

	go func() {
		errCh <- storage.Apply("1", []types.Log{{Index: 2}})
	}()

	select {
	case <-entered:
		t.Fatal("second Apply() entered OnApply while the same slot was still applying")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	waitForApply(t, entered, "second apply")

	for range 2 {
		if err := <-errCh; !errors.Is(err, errStopBeforeAppliedIndex) {
			t.Fatalf("Apply() error = %v, want %v", err, errStopBeforeAppliedIndex)
		}
	}
}

var errStopBeforeAppliedIndex = errors.New("stop before writing applied index")

func newBlockingApplyStorage(t *testing.T) (*PebbleShardLogStorage, <-chan uint32, chan struct{}) {
	t.Helper()

	entered := make(chan uint32, 2)
	release := make(chan struct{})
	server := &Server{opts: &Options{
		OnApply: func(slotID uint32, _ []types.Log) error {
			entered <- slotID
			<-release
			return errStopBeforeAppliedIndex
		},
	}}
	storage := NewPebbleShardLogStorage(server, t.TempDir(), 1)
	if err := storage.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return storage, entered, release
}

func waitForApply(t *testing.T, entered <-chan uint32, name string) {
	t.Helper()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s to enter OnApply", name)
	}
}
