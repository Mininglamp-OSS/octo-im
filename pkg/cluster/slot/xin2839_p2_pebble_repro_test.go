package slot

import (
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// TestXIN2839_P2C1_PebbleStorage_OverwriteTimestampAndTailLoss verifies the disk-level effect
// of P2-C1 on real PebbleShardLogStorage: when queue.nextStoreLogs repeatedly emits the same
// K logs [T-2K+1..T-K], PebbleShardLogStorage.AppendLogs stamps a fresh time.Now().UnixNano()
// on [T-2K+1..T-K] on every cycle while the tail [T-K+1..T] is completely absent from Pebble.
func TestXIN2839_P2C1_PebbleStorage_OverwriteTimestampAndTailLoss(t *testing.T) {
	server := &Server{opts: &Options{
		OnApply: func(slotID uint32, logs []types.Log) error {
			return nil
		},
	}}
	storage := NewPebbleShardLogStorage(server, t.TempDir(), 1)
	if err := storage.Open(); err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}
	defer func() {
		_ = storage.Close()
	}()

	slotKey := "4"
	// Step 1: Persist initial logs [1..100]
	initLogs := make([]types.Log, 0, 100)
	for i := uint64(1); i <= 100; i++ {
		initLogs = append(initLogs, types.Log{
			Id:    i,
			Index: i,
			Term:  2,
			Data:  []byte(fmt.Sprintf("init-log-%d", i)),
		})
	}
	if err := storage.AppendLogs(slotKey, initLogs, &types.TermStartIndexInfo{Term: 2, Index: 1}); err != nil {
		t.Fatalf("AppendLogs init error = %v", err)
	}

	// Step 2: Simulate the exact batch emitted by P2-C1 after K=3 unstored logs [101..103]
	// and overlapping SyncResp [98..106] (M=9):
	// Tick 1 batch is old[101..103] + new[98..103], so highest index written to Pebble is 103 (T-K),
	// leaving [104..106] missing from Pebble.
	tick1Batch := []types.Log{
		{Id: 101, Index: 101, Term: 2, Data: []byte("stale-unstored-101")},
		{Id: 102, Index: 102, Term: 2, Data: []byte("stale-unstored-102")},
		{Id: 103, Index: 103, Term: 2, Data: []byte("stale-unstored-103")},
		{Id: 198, Index: 98, Term: 3, Data: []byte("new-leader-98")},
		{Id: 199, Index: 99, Term: 3, Data: []byte("new-leader-99")},
		{Id: 200, Index: 100, Term: 3, Data: []byte("new-leader-100")},
		{Id: 201, Index: 101, Term: 3, Data: []byte("new-leader-101")},
		{Id: 202, Index: 102, Term: 3, Data: []byte("new-leader-102")},
		{Id: 203, Index: 103, Term: 3, Data: []byte("new-leader-103")},
	}
	if err := storage.AppendLogs(slotKey, tick1Batch, nil); err != nil {
		t.Fatalf("AppendLogs tick1 error = %v", err)
	}

	lastIdx1, err := storage.LastIndex(slotKey)
	if err != nil {
		t.Fatalf("LastIndex tick1 error = %v", err)
	}
	logsRound1, err := storage.GetLogs(slotKey, 101, 107, 0)
	if err != nil {
		t.Fatalf("GetLogs round1 error = %v", err)
	}
	if len(logsRound1) != 3 || logsRound1[len(logsRound1)-1].Index != 103 {
		t.Fatalf("expected Pebble to only contain [101..103] (missing 104..106), got len=%d", len(logsRound1))
	}
	tsRound1 := logsRound1[2].Time // timestamp of index 103

	time.Sleep(25 * time.Millisecond)

	// Step 3: Tick 2 in P2-C1 repeatedly flushes r.logs[:K] = [101..103] to Pebble
	tick2Batch := tick1Batch[6:9] // [101..103]
	if err := storage.AppendLogs(slotKey, tick2Batch, nil); err != nil {
		t.Fatalf("AppendLogs tick2 error = %v", err)
	}
	lastIdx2, err := storage.LastIndex(slotKey)
	if err != nil {
		t.Fatalf("LastIndex tick2 error = %v", err)
	}
	logsRound2, err := storage.GetLogs(slotKey, 101, 107, 0)
	if err != nil {
		t.Fatalf("GetLogs round2 error = %v", err)
	}
	tsRound2 := logsRound2[2].Time

	t.Logf("[PEBBLE P2-C1 VERIFICATION] LastIndex_R1=%d LastIndex_R2=%d (missing 104..106, gap=3) | Index103_StoredAt_R1=%s (%d) -> Index103_StoredAt_R2=%s (%d) | TimestampAdvanced=%v",
		lastIdx1, lastIdx2,
		tsRound1.Format(time.RFC3339Nano), tsRound1.UnixNano(),
		tsRound2.Format(time.RFC3339Nano), tsRound2.UnixNano(),
		tsRound2.After(tsRound1))

	if lastIdx1 != 103 || lastIdx2 != 103 || !tsRound2.After(tsRound1) {
		t.Fatalf("expected LastIndex stuck at 103 and StoredAt timestamp advancing on each rewrite")
	}
}

func TestP3_SlotGetLogsBoundedByCountAndBytesAndShardGuard(t *testing.T) {
	dir := t.TempDir()
	server := &Server{opts: NewOptions(
		WithDataDir(dir),
		WithSlotDbShardNum(2),
		WithSlotMemTableSize(2*1024*1024),
		WithMaxLogCountPerBatch(25),
		WithOnApply(func(slotID uint32, logs []types.Log) error { return nil }),
	)}
	storage := NewPebbleShardLogStorage(server, dir+"/logdb", 2)
	if err := storage.Open(); err != nil {
		t.Fatalf("storage.Open() error = %v", err)
	}

	slotKey := "1"
	logs := make([]types.Log, 0, 80)
	for i := uint64(1); i <= 80; i++ {
		logs = append(logs, types.Log{
			Id:    i,
			Index: i,
			Term:  1,
			Data:  []byte(fmt.Sprintf("slot-log-payload-%03d", i)),
		})
	}
	if err := storage.AppendLogs(slotKey, logs, &types.TermStartIndexInfo{Term: 1, Index: 1}); err != nil {
		t.Fatalf("AppendLogs error = %v", err)
	}

	// 1. With large limitSize (e.g. 2 MiB), GetLogs must cap at MaxLogCountPerBatch (25).
	batchByCount, err := storage.GetLogs(slotKey, 1, 81, 2*1024*1024)
	if err != nil {
		t.Fatalf("GetLogs by count error = %v", err)
	}
	if len(batchByCount) != 25 {
		t.Fatalf("expected GetLogs capped at 25 logs by MaxLogCountPerBatch, got %d", len(batchByCount))
	}

	// 2. With small limitSize=200 bytes, GetLogs must cap by bytes while still making progress (>= 1 log).
	batchByBytes, err := storage.GetLogs(slotKey, 1, 81, 200)
	if err != nil {
		t.Fatalf("GetLogs by bytes error = %v", err)
	}
	if len(batchByBytes) == 0 || len(batchByBytes) >= 25 {
		t.Fatalf("expected GetLogs capped by bytes (1..24), got %d", len(batchByBytes))
	}

	// 3. TruncateLogTo(0) when appliedIdx == 0 clears all unapplied logs.
	if err := storage.TruncateLogTo(slotKey, 0); err != nil {
		t.Fatalf("TruncateLogTo(0) error = %v", err)
	}
	lastIdx, err := storage.LastIndex(slotKey)
	if err != nil || lastIdx != 0 {
		t.Fatalf("expected LastIndex=0 after TruncateLogTo(0), got %d (err=%v)", lastIdx, err)
	}

	_ = storage.Close()

	// 4. Reopening non-empty logdb with mismatched shardNum (4 instead of 2) must fail fast.
	storageMismatch := NewPebbleShardLogStorage(server, dir+"/logdb", 4)
	if err := storageMismatch.Open(); err == nil {
		_ = storageMismatch.Close()
		t.Fatalf("expected storageMismatch.Open() to fail on shard count mismatch")
	}
}
