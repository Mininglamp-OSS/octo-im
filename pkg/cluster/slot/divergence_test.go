package slot

import (
	"bytes"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/require"
)

func TestTruncateBelowAppliedTypedAndCounted(t *testing.T) {
	storage := openTestStorage(t, t.TempDir())
	defer storage.Close()
	seedSlot(t, storage, "7", 10, 8, 2)
	err := storage.TruncateLogTo("7", 7)
	if !errors.Is(err, types.ErrTruncateBelowApplied) {
		t.Fatalf("err = %v, want ErrTruncateBelowApplied", err)
	}
	if got := storage.s.DivergenceStats().TruncateRefused; got != 1 {
		t.Fatalf("truncate refused counter = %d, want 1", got)
	}
}

// T2: an equal replay at or below the applied index is skipped without a
// write: the stored time suffix stays the same. Entries above it are written.
func TestAppendEqualReplayBelowAppliedSkipped(t *testing.T) {
	storage := openTestStorage(t, t.TempDir())
	defer storage.Close()
	const shardNo = "3"
	logs := seedSlot(t, storage, shardNo, 6, 5, 2)
	before4 := rawLog(t, storage, shardNo, 4)

	replay := append([]types.Log(nil), logs[3:6]...) // 4, 5 (applied) and 6
	replay = append(replay, types.Log{Index: 7, Term: 2, Id: 1007, Data: []byte("new")})
	if err := storage.AppendLogs(shardNo, replay, nil); err != nil {
		t.Fatalf("equal replay: %v", err)
	}
	if !bytes.Equal(before4, rawLog(t, storage, shardNo, 4)) {
		t.Fatal("applied entry rewritten by an equal replay")
	}
	if last, _ := storage.LastIndex(shardNo); last != 7 {
		t.Fatalf("last = %d, want 7", last)
	}
	st := storage.s.DivergenceStats()
	if st.AppendOverlapConflict != 0 || st.SameTermConflict != 0 {
		t.Fatalf("counters = %+v, want zero", st)
	}
}

func TestAppendConflictBelowAppliedTyped(t *testing.T) {
	cases := map[string]types.Log{
		"different id":   {Index: 5, Term: 2, Id: 1, Data: []byte{5, 'v'}},
		"different data": {Index: 5, Term: 2, Id: 1005, Data: []byte("z")},
		"different term": {Index: 5, Term: 3, Id: 1005, Data: []byte{5, 'v'}},
		"beyond the log": {Index: 7, Term: 2, Id: 1007, Data: []byte("q")},
	}
	for name, entry := range cases {
		t.Run(name, func(t *testing.T) {
			storage := openTestStorage(t, t.TempDir())
			defer storage.Close()
			seedSlot(t, storage, "3", 6, 5, 2)
			if entry.Index == 7 {
				// applied beyond the log: 7 is "applied" but absent.
				if err := storage.SetAppliedIndex("3", 8); err != nil {
					t.Fatal(err)
				}
			}
			err := storage.AppendLogs("3", []types.Log{entry}, nil)
			if !errors.Is(err, types.ErrAppendBelowApplied) {
				t.Fatalf("err = %v, want ErrAppendBelowApplied", err)
			}
			if got := storage.s.DivergenceStats().AppendOverlapConflict; got != 1 {
				t.Fatalf("conflict counter = %d", got)
			}
		})
	}
}

// Above the applied index a replacement is ordinary repair. A same-term
// replacement is only counted.
func TestAppendSameTermConflictAboveAppliedCountedOnly(t *testing.T) {
	storage := openTestStorage(t, t.TempDir())
	defer storage.Close()
	seedSlot(t, storage, "3", 6, 4, 2)
	if err := storage.AppendLogs("3", []types.Log{{Index: 6, Term: 2, Id: 77, Data: []byte("r")}}, nil); err != nil {
		t.Fatal(err)
	}
	logs, _ := storage.GetLogs("3", 6, 7, 0)
	if len(logs) != 1 || logs[0].Id != 77 {
		t.Fatalf("entry 6 = %+v, want replaced", logs)
	}
	if got := storage.s.DivergenceStats().SameTermConflict; got != 1 {
		t.Fatalf("same-term counter = %d, want 1", got)
	}
	// A different-term replacement above applied is not a same-term conflict.
	if err := storage.AppendLogs("3", []types.Log{{Index: 6, Term: 3, Id: 78, Data: []byte("s")}}, nil); err != nil {
		t.Fatal(err)
	}
	if got := storage.s.DivergenceStats().SameTermConflict; got != 1 {
		t.Fatalf("same-term counter = %d after a term change, want 1", got)
	}
}

func TestTermStartIndexWrittenWithLogsAtomically(t *testing.T) {
	storage := openTestStorage(t, t.TempDir())
	defer storage.Close()
	if err := storage.AppendLogs("4", []types.Log{{Index: 1, Term: 5, Id: 1, Data: []byte("a")}}, &types.TermStartIndexInfo{Term: 5, Index: 1}); err != nil {
		t.Fatal(err)
	}
	if idx, _ := storage.GetTermStartIndex("4", 5); idx != 1 {
		t.Fatalf("term start = %d", idx)
	}
}

// T3 and T14a: applied > last or a durable marker opens quarantined.
func TestGetStateReportsDivergence(t *testing.T) {
	path := t.TempDir()
	storage := openTestStorage(t, path)
	seedSlot(t, storage, "12", 4, 3, 1)
	seedSlot(t, storage, "13", 4, 3, 1)
	if err := storage.SetAppliedIndex("12", 9); err != nil {
		t.Fatal(err)
	}
	if err := storage.MarkDiverged("13", types.ErrTruncateBelowApplied); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	storage = openTestStorage(t, path)
	defer storage.Close()
	for shardNo, want := range map[string]bool{"12": true, "13": true} {
		state, err := storage.GetState(shardNo)
		if err != nil {
			t.Fatal(err)
		}
		if state.Diverged != want {
			t.Fatalf("slot %s Diverged = %v", shardNo, state.Diverged)
		}
	}
	state, _ := storage.GetState("13")
	if state.AppliedIndex != 3 || state.LastLogIndex != 4 {
		t.Fatalf("slot 13 state = %+v", state)
	}
}

// The in-memory fence is up before the marker write starts.
func TestMarkDivergedFencesBeforeWriting(t *testing.T) {
	storage := openTestStorage(t, t.TempDir())
	defer storage.Close()
	var fencedDuringWrite error
	setDivergedMarkerHook = func(shardNo string) error {
		fencedDuringWrite = storage.s.CheckSlotReadable(KeyToSlotId(shardNo))
		return nil
	}
	defer func() { setDivergedMarkerHook = nil }()
	if err := storage.MarkDiverged("21", types.ErrTruncateBelowApplied); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(fencedDuringWrite, types.ErrSlotDiverged) {
		t.Fatalf("fence during marker write = %v", fencedDuringWrite)
	}
	if got := storage.s.ReplicaState(21); got != ReplicaQuarantined {
		t.Fatalf("state = %v", got)
	}
	if err := storage.s.CheckSlotReadable(22); err != nil {
		t.Fatalf("other slot fenced: %v", err)
	}
	if marked, _ := storage.DivergedMarker("21"); !marked {
		t.Fatal("marker not persisted")
	}
}

// T14b: a failed marker write fail-stops the slot replica, keeps the process
// and other slots running, and after a restart the replica is uncontained
// until its next refused truncation (exposure A4, asserted as an exposure).
func TestMarkerPersistFailureFailStopsAndRestartExposure(t *testing.T) {
	path := t.TempDir()
	storage := openTestStorage(t, path)
	seedSlot(t, storage, "5", 10, 8, 2)
	setDivergedMarkerHook = func(string) error { return errors.New("injected ENOSPC") }
	err := storage.MarkDiverged("5", types.ErrTruncateBelowApplied)
	setDivergedMarkerHook = nil
	if err == nil {
		t.Fatal("marker write failure not reported")
	}
	if got := storage.s.ReplicaState(5); got != ReplicaFailStopped {
		t.Fatalf("state = %v, want fail-stopped", got)
	}
	if got := storage.s.DivergenceStats().PersistFailed; got != 1 {
		t.Fatalf("persist failed counter = %d", got)
	}
	if err := storage.s.CheckSlotReadable(6); err != nil {
		t.Fatalf("other slot fenced: %v", err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: no marker, applied == 8 <= last == 10. Exposure A4.
	storage = openTestStorage(t, path)
	defer storage.Close()
	state, err := storage.GetState("5")
	if err != nil {
		t.Fatal(err)
	}
	if state.Diverged {
		t.Fatal("expected the A4 exposure: replica reopens uncontained without a marker")
	}
	if storage.s.ReplicaState(5) != ReplicaHealthy {
		t.Fatal("expected a fresh process to start healthy")
	}
	// The next truncation below applied is refused again.
	if err := storage.TruncateLogTo("5", 6); !errors.Is(err, types.ErrTruncateBelowApplied) {
		t.Fatalf("second refusal = %v", err)
	}
}

// T16: the pre-upgrade scan is read-only and exact.
func TestScanDivergenceReadOnlyAndExact(t *testing.T) {
	path := t.TempDir()
	server := &Server{opts: &Options{OnApply: func(uint32, []types.Log) error { return nil }}, Log: wklog.NewWKLog("slotTest")}
	storage := NewPebbleShardLogStorage(server, path, 3)
	if err := storage.Open(); err != nil {
		t.Fatal(err)
	}
	seedSlot(t, storage, "1", 5, 5, 1) // healthy, applied == last
	seedSlot(t, storage, "2", 5, 2, 1) // healthy
	seedSlot(t, storage, "3", 2, 0, 1) // applied beyond log
	if err := storage.SetAppliedIndex("3", 4); err != nil {
		t.Fatal(err)
	}
	seedSlot(t, storage, "40", 3, 1, 1) // marker
	if err := storage.MarkDiverged("40", types.ErrAppendBelowApplied); err != nil {
		t.Fatal(err)
	}
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	for run := 0; run < 2; run++ {
		flagged, err := ScanDivergence(path, 3, 64)
		if err != nil {
			t.Fatal(err)
		}
		if len(flagged) != 2 {
			t.Fatalf("run %d: flagged = %+v, want slots 3 and 40", run, flagged)
		}
		want := map[uint32]SlotScanResult{
			3:  {SlotId: 3, AppliedIndex: 4, LastLogIndex: 2, AppliedBeyondLog: true},
			40: {SlotId: 40, AppliedIndex: 1, LastLogIndex: 3, Marker: true},
		}
		for _, r := range flagged {
			if r != want[r.SlotId] {
				t.Fatalf("run %d: %+v, want %+v", run, r, want[r.SlotId])
			}
		}
	}

	// Nothing was rewritten: the stored applied index is unchanged.
	storage = NewPebbleShardLogStorage(server, path, 3)
	if err := storage.Open(); err != nil {
		t.Fatal(err)
	}
	defer storage.Close()
	if applied, _ := storage.AppliedIndex("3"); applied != 4 {
		t.Fatalf("scan rewrote applied index to %d", applied)
	}
}

func TestScanDivergenceFailsOnMissingStorage(t *testing.T) {
	if _, err := ScanDivergence(t.TempDir()+"/absent", 2, 4); err == nil {
		t.Fatal("scan of a missing directory succeeded")
	}
}

// Metric registration is idempotent and safe on the global meter.
func TestRegisterDivergenceMetricsOnce(t *testing.T) {
	s := &Server{opts: &Options{NodeId: 9}}
	require.NotPanics(t, func() {
		s.registerDivergenceMetrics()
		s.registerDivergenceMetrics()
	})
}
