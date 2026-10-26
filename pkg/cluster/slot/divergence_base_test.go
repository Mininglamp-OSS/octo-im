package slot

import (
	"bytes"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot/key"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

// The tests in this file use only APIs that also exist before divergence
// containment, so they can be run against the earlier code to show the
// defect.

func openTestStorage(t *testing.T, path string) *PebbleShardLogStorage {
	t.Helper()
	server := &Server{opts: &Options{OnApply: func(uint32, []types.Log) error { return nil }}, Log: wklog.NewWKLog("slotTest")}
	storage := NewPebbleShardLogStorage(server, path, 1)
	if err := storage.Open(); err != nil {
		t.Fatal(err)
	}
	return storage
}

func seedSlot(t *testing.T, storage *PebbleShardLogStorage, shardNo string, last, applied uint64, term uint32) []types.Log {
	t.Helper()
	logs := make([]types.Log, 0, last)
	for i := uint64(1); i <= last; i++ {
		logs = append(logs, types.Log{Index: i, Term: term, Id: 1000 + i, Data: []byte{byte(i), 'v'}})
	}
	if err := storage.AppendLogs(shardNo, logs, &types.TermStartIndexInfo{Term: term, Index: 1}); err != nil {
		t.Fatal(err)
	}
	if applied > 0 {
		if err := storage.SetAppliedIndex(shardNo, applied); err != nil {
			t.Fatal(err)
		}
	}
	return logs
}

// rawLog returns the stored bytes of one log key, including the time suffix.
func rawLog(t *testing.T, storage *PebbleShardLogStorage, shardNo string, index uint64) []byte {
	t.Helper()
	data, closer, err := storage.shardDB(shardNo).Get(key.NewLogKey(shardNo, index))
	if err != nil {
		t.Fatalf("log %d: %v", index, err)
	}
	defer closer.Close()
	return append([]byte(nil), data...)
}

// T1: a truncation below the applied index fails and changes nothing.
func TestTruncateBelowAppliedRefusedWithoutMutation(t *testing.T) {
	storage := openTestStorage(t, t.TempDir())
	defer storage.Close()
	const shardNo = "7"
	seedSlot(t, storage, shardNo, 10, 8, 2)
	before := make(map[uint64][]byte)
	for i := uint64(1); i <= 10; i++ {
		before[i] = rawLog(t, storage, shardNo, i)
	}

	err := storage.TruncateLogTo(shardNo, 7)
	if err == nil {
		t.Fatal("truncation below the applied index reported success")
	}

	last, _ := storage.LastIndex(shardNo)
	applied, _ := storage.AppliedIndex(shardNo)
	termStart, _ := storage.GetTermStartIndex(shardNo, 2)
	if last != 10 || applied != 8 || termStart != 1 {
		t.Fatalf("state changed: last=%d applied=%d termStart=%d", last, applied, termStart)
	}
	for i := uint64(1); i <= 10; i++ {
		if !bytes.Equal(before[i], rawLog(t, storage, shardNo, i)) {
			t.Fatalf("log %d changed", i)
		}
	}
	// Truncating to exactly the applied index stays allowed.
	if err := storage.TruncateLogTo(shardNo, 8); err != nil {
		t.Fatalf("truncate to applied: %v", err)
	}
	if last, _ := storage.LastIndex(shardNo); last != 8 {
		t.Fatalf("last after truncate to applied = %d", last)
	}
}

// T2b: a conflicting entry at or below the applied index rejects the whole
// batch before anything is written, including the term start index.
func TestAppendConflictBelowAppliedRejectsWholeBatch(t *testing.T) {
	storage := openTestStorage(t, t.TempDir())
	defer storage.Close()
	const shardNo = "3"
	seedSlot(t, storage, shardNo, 6, 5, 2)
	before5 := rawLog(t, storage, shardNo, 5)

	batch := []types.Log{
		{Index: 5, Term: 3, Id: 9005, Data: []byte("other")}, // conflicts below applied
		{Index: 6, Term: 3, Id: 9006, Data: []byte("x")},
		{Index: 7, Term: 3, Id: 9007, Data: []byte("y")},
	}
	err := storage.AppendLogs(shardNo, batch, &types.TermStartIndexInfo{Term: 3, Index: 5})
	if err == nil {
		t.Fatal("conflicting append below the applied index reported success")
	}
	if !bytes.Equal(before5, rawLog(t, storage, shardNo, 5)) {
		t.Fatal("applied entry 5 was overwritten")
	}
	if last, _ := storage.LastIndex(shardNo); last != 6 {
		t.Fatalf("last index = %d, want 6 (no entry of the batch written)", last)
	}
	if idx, _ := storage.GetTermStartIndex(shardNo, 3); idx != 0 {
		t.Fatalf("term start index for term 3 = %d, want absent", idx)
	}
}

// T3: applied > last at open is reported, and the stored applied index is
// not rewritten.
func TestGetStateDoesNotRewindAppliedBeyondLog(t *testing.T) {
	storage := openTestStorage(t, t.TempDir())
	defer storage.Close()
	const shardNo = "12"
	seedSlot(t, storage, shardNo, 4, 0, 1)
	if err := storage.SetAppliedIndex(shardNo, 9); err != nil {
		t.Fatal(err)
	}
	state, err := storage.GetState(shardNo)
	if err != nil {
		t.Fatal(err)
	}
	if state.AppliedIndex != 9 {
		t.Fatalf("reported applied index = %d, want 9 (unchanged)", state.AppliedIndex)
	}
	if applied, _ := storage.AppliedIndex(shardNo); applied != 9 {
		t.Fatalf("stored applied index rewritten to %d", applied)
	}
}
