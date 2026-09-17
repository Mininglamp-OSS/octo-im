package slot

import (
	"fmt"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func TestGroupedSlotWritesSurviveReopen(t *testing.T) {
	path := t.TempDir()
	server := &Server{opts: &Options{
		OnApply: func(uint32, []types.Log) error { return nil },
	}}
	storage := NewPebbleShardLogStorage(server, path, 1)
	if err := storage.Open(); err != nil {
		t.Fatal(err)
	}
	// Every slot shares one physical database, so concurrent writes exercise
	// the group-commit worker without sharing a logical slot lock.
	const slots = 16
	var wg sync.WaitGroup
	for slot := range slots {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := SlotIdToKey(uint32(slot))
			logs := []types.Log{{Index: 1, Term: 1, Data: []byte(fmt.Sprintf("slot-%d", slot))}}
			if err := storage.AppendLogs(key, logs, &types.TermStartIndexInfo{Term: 1, Index: 1}); err != nil {
				t.Error(err)
				return
			}
			if err := storage.Apply(key, logs); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}
	if t.Failed() {
		return
	}

	storage = NewPebbleShardLogStorage(server, path, 1)
	if err := storage.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			t.Error(err)
		}
	})
	for slot := range slots {
		key := SlotIdToKey(uint32(slot))
		logs, err := storage.GetLogs(key, 1, 2, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(logs) != 1 || logs[0].Index != 1 || logs[0].Term != 1 || string(logs[0].Data) != fmt.Sprintf("slot-%d", slot) {
			t.Fatalf("slot %d: unexpected persisted logs: %+v", slot, logs)
		}
		index, err := storage.AppliedIndex(key)
		if err != nil || index != 1 {
			t.Fatalf("slot %d: applied index = %d, error = %v", slot, index, err)
		}
		index, err = storage.GetTermStartIndex(key, 1)
		if err != nil || index != 1 {
			t.Fatalf("slot %d: term start index = %d, error = %v", slot, index, err)
		}
	}
}
