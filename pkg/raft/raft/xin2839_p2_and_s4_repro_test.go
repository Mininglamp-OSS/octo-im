package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func makeSyntheticLogs(startIndex, endIndex uint64, term uint32, tag string) []types.Log {
	out := make([]types.Log, 0, endIndex-startIndex+1)
	for idx := startIndex; idx <= endIndex; idx++ {
		out = append(out, types.Log{
			Id:    idx,
			Index: idx,
			Term:  term,
			Data:  []byte(fmt.Sprintf("xin2839-synthetic-%s-idx-%d-term-%d", tag, idx, term)),
			Time:  time.Unix(1763000000, int64(idx)*1000),
		})
	}
	return out
}

// TestXIN2839_P2C1_TruncateBelowStoredIndex_Deterministic reproduces P2-C1 on queue:
// committedIndex=90, storedIndex=100, K=3 unstored logs [101..103] in r.logs,
// followed by an overlapping batch [98..106] (truncateToIndex=97 < storedIndex=100).
func TestXIN2839_P2C1_TruncateBelowStoredIndex_Deterministic(t *testing.T) {
	cases := []struct {
		name           string
		committedIndex uint64
		storedIndex    uint64
		unstoredK      uint64
		overlapStart   uint64
		overlapEnd     uint64
	}{
		{
			name:           "Synthetic_S100_K3_Overlap98To106",
			committedIndex: 90,
			storedIndex:    100,
			unstoredK:      3,
			overlapStart:   98,
			overlapEnd:     106,
		},
		{
			name:           "ProdSlot4Param_S70254_K3_Overlap70250To70478",
			committedIndex: 70200,
			storedIndex:    70254,
			unstoredK:      3,
			overlapStart:   70250,
			overlapEnd:     70478,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := newQueue("xin2839-slot4", tc.committedIndex, tc.storedIndex)
			unstored := makeSyntheticLogs(tc.storedIndex+1, tc.storedIndex+tc.unstoredK, 2, "old-unstored")
			if err := q.append(unstored...); err != nil {
				t.Fatalf("initial append unstored failed: %v", err)
			}
			t.Logf("[BEFORE OVERLAP] storedIndex=%d lastLogIndex=%d committedIndex=%d appliedIndex=%d len(logs)=%d firstInMem=%d lastInMem=%d",
				q.storedIndex, q.lastLogIndex, q.committedIndex, q.appliedIndex, len(q.logs), q.logs[0].Index, q.logs[len(q.logs)-1].Index)

			incoming := makeSyntheticLogs(tc.overlapStart, tc.overlapEnd, 3, "new-leader")
			if err := q.append(incoming...); err != nil {
				t.Fatalf("overlapping append failed: %v", err)
			}

			expectedM := int(tc.overlapEnd - tc.overlapStart + 1)
			t.Logf("[AFTER OVERLAP APPEND] storedIndex=%d lastLogIndex=%d len(logs)=%d (expected %d) firstInMem=(idx:%d,term:%d) lastInMem=(idx:%d,term:%d)",
				q.storedIndex, q.lastLogIndex, len(q.logs), expectedM,
				q.logs[0].Index, q.logs[0].Term, q.logs[len(q.logs)-1].Index, q.logs[len(q.logs)-1].Term)

			// Simulate 4 storage ticks: nextStoreLogs(0) -> storeTo(batch[last].Index)
			for tick := 1; tick <= 4; tick++ {
				hasStore := q.hasNextStoreLogs()
				if !hasStore {
					t.Logf("[TICK %d] hasNextStoreLogs=false storedIndex=%d lastLogIndex=%d len(logs)=%d",
						tick, q.storedIndex, q.lastLogIndex, len(q.logs))
					break
				}
				batch := q.nextStoreLogs(0)
				firstIdx, firstTerm := batch[0].Index, batch[0].Term
				lastIdx, lastTerm := batch[len(batch)-1].Index, batch[len(batch)-1].Term
				q.storeTo(lastIdx)
				t.Logf("[TICK %d] nextStoreLogsLen=%d batchRange=[idx:%d(term:%d)..idx:%d(term:%d)] -> after storeTo(%d): storedIndex=%d lastLogIndex=%d remainingLen=%d",
					tick, len(batch), firstIdx, firstTerm, lastIdx, lastTerm, lastIdx, q.storedIndex, q.lastLogIndex, len(q.logs))
			}

			gap := tc.overlapEnd - q.storedIndex
			t.Logf("[RESULT %s] finalStoredIndex=%d targetEnd=%d permanentLagGap=%d (unstoredK=%d) hasNextStoreLogs=%v",
				tc.name, q.storedIndex, tc.overlapEnd, gap, tc.unstoredK, q.hasNextStoreLogs())

			if q.storedIndex != tc.overlapEnd || len(q.logs) != 0 || q.hasNextStoreLogs() {
				t.Fatalf("P2-C1 REPRODUCED: storedIndex=%d (want %d, permanent lag gap=%d == K=%d), len(r.logs)=%d (want 0), hasNextStoreLogs=%v (infinite disk overwrite loop)",
					q.storedIndex, tc.overlapEnd, gap, tc.unstoredK, len(q.logs), q.hasNextStoreLogs())
			}
		})
	}
}

// TestXIN2839_P2C1_RandomizedSeeds_100Runs runs 100 deterministic seeds with K in [1..15].
func TestXIN2839_P2C1_RandomizedSeeds_100Runs(t *testing.T) {
	const totalRuns = 100
	reproducedCount := 0
	passedCount := 0

	for i := 0; i < totalRuns; i++ {
		seed := int64(2026111300 + i)
		rng := rand.New(rand.NewSource(seed))
		committed := uint64(50 + rng.Intn(200))
		stored := committed + uint64(10+rng.Intn(100))
		k := uint64(1 + rng.Intn(15))
		backoff := uint64(1 + rng.Intn(int(stored-committed)))
		overlapStart := stored - backoff + 1 // truncateToIndex = overlapStart - 1 < stored, >= committed
		overlapEnd := stored + k + uint64(1+rng.Intn(30))

		q := newQueue(fmt.Sprintf("seed-%d", seed), committed, stored)
		_ = q.append(makeSyntheticLogs(stored+1, stored+k, 2, "u")...)
		if err := q.append(makeSyntheticLogs(overlapStart, overlapEnd, 3, "n")...); err != nil {
			t.Fatalf("seed %d append err: %v", seed, err)
		}
		for tick := 0; tick < 3 && q.hasNextStoreLogs(); tick++ {
			b := q.nextStoreLogs(0)
			q.storeTo(b[len(b)-1].Index)
		}
		gap := overlapEnd - q.storedIndex
		if gap == k && q.hasNextStoreLogs() {
			reproducedCount++
		} else if gap == 0 && !q.hasNextStoreLogs() && len(q.logs) == 0 {
			passedCount++
		} else {
			t.Fatalf("seed %d unexpected state: gap=%d k=%d len=%d", seed, gap, k, len(q.logs))
		}
	}
	t.Logf("[RANDOMIZED 100 SEEDS SUMMARY] total=%d reproduced_P2C1=%d passed_clean=%d", totalRuns, reproducedCount, passedCount)
	if reproducedCount > 0 {
		t.Fatalf("P2-C1 REPRODUCED in %d/%d randomized seeds (100%% deterministic when K>0 and committedIndex <= truncateToIndex < storedIndex)", reproducedCount, totalRuns)
	}
}

// TestXIN2839_P2C1_NegativeControls verifies the negative controls:
// NC1: K=0 with truncateToIndex < storedIndex
// NC2: K>0 with truncateToIndex >= storedIndex
// NC3: Conflict-free sequential append (offline 1->2 control)
// NC4: Benign same-term duplicate retransmission (case 5a)
func TestXIN2839_P2C1_NegativeControls(t *testing.T) {
	t.Run("NC1_K0_TruncateBelowStoredIndex", func(t *testing.T) {
		q := newQueue("nc1", 90, 100) // K = 0
		if err := q.append(makeSyntheticLogs(98, 106, 3, "nc1")...); err != nil {
			t.Fatalf("NC1 append err: %v", err)
		}
		b := q.nextStoreLogs(0)
		q.storeTo(b[len(b)-1].Index)
		t.Logf("[NC1 K=0] storedIndex=%d lastLogIndex=%d len(logs)=%d hasNextStore=%v", q.storedIndex, q.lastLogIndex, len(q.logs), q.hasNextStoreLogs())
		if q.storedIndex != 106 || len(q.logs) != 0 || q.hasNextStoreLogs() {
			t.Fatalf("NC1 failed: storedIndex=%d len=%d", q.storedIndex, len(q.logs))
		}
	})

	t.Run("NC2_K3_TruncateAboveOrEqualStoredIndex", func(t *testing.T) {
		q := newQueue("nc2", 90, 100)
		_ = q.append(makeSyntheticLogs(101, 103, 2, "nc2-u")...) // K = 3
		// overlap starts at 102 -> truncateToIndex = 101 >= storedIndex (100)
		if err := q.append(makeSyntheticLogs(102, 106, 3, "nc2-n")...); err != nil {
			t.Fatalf("NC2 append err: %v", err)
		}
		for q.hasNextStoreLogs() {
			b := q.nextStoreLogs(0)
			q.storeTo(b[len(b)-1].Index)
		}
		t.Logf("[NC2 truncateTo>=storedIndex] storedIndex=%d lastLogIndex=%d len(logs)=%d hasNextStore=%v", q.storedIndex, q.lastLogIndex, len(q.logs), q.hasNextStoreLogs())
		if q.storedIndex != 106 || len(q.logs) != 0 || q.hasNextStoreLogs() {
			t.Fatalf("NC2 failed: storedIndex=%d len=%d", q.storedIndex, len(q.logs))
		}
	})

	t.Run("NC3_SequentialAppend_Offline1To2Control", func(t *testing.T) {
		q := newQueue("nc3", 0, 0)
		for batchIdx := 0; batchIdx < 3; batchIdx++ {
			start := uint64(batchIdx*50 + 1)
			end := uint64((batchIdx + 1) * 50)
			if err := q.append(makeSyntheticLogs(start, end, 1, "seq")...); err != nil {
				t.Fatalf("NC3 append err: %v", err)
			}
			b := q.nextStoreLogs(0)
			q.storeTo(b[len(b)-1].Index)
		}
		t.Logf("[NC3 sequential] storedIndex=%d lastLogIndex=%d len(logs)=%d", q.storedIndex, q.lastLogIndex, len(q.logs))
		if q.storedIndex != 150 || len(q.logs) != 0 {
			t.Fatalf("NC3 failed: storedIndex=%d", q.storedIndex)
		}
	})

	t.Run("NC4_BenignSameTermDuplicateRetransmission", func(t *testing.T) {
		q := newQueue("nc4", 90, 100)
		_ = q.append(makeSyntheticLogs(101, 103, 2, "orig")...)
		// Retransmit [99..103] with same term 2
		if err := q.append(makeSyntheticLogs(99, 103, 2, "orig")...); err != nil {
			t.Fatalf("NC4 append err: %v", err)
		}
		b := q.nextStoreLogs(0)
		q.storeTo(b[len(b)-1].Index)
		t.Logf("[NC4 duplicate retransmit] storedIndex=%d lastLogIndex=%d len(logs)=%d", q.storedIndex, q.lastLogIndex, len(q.logs))
		if q.storedIndex != 103 || len(q.logs) != 0 {
			t.Fatalf("NC4 failed: storedIndex=%d", q.storedIndex)
		}
	})
}

// TestXIN2839_P2C2_FollowerToLeaderHandoff_DeadlockControl verifies how a follower
// stuck with storedIndex = T - K (0 < K < FollowerToLeaderMinLogGap=100) and lastLogIndex = T
// causes Leader 1002 to set stopPropose=true and LeaderReadReady()=false without ever completing handoff.
func TestXIN2839_P2C2_FollowerToLeaderHandoff_DeadlockControl(t *testing.T) {
	opts1002 := NewOptions(
		WithNodeId(1002),
		WithReplicas([]uint64{1001, 1002, 1003}),
		WithKey("slot-11"),
	)
	leader1002 := NewNode(1, types.RaftState{
		LastLogIndex: 106,
		LastTerm:     4,
		AppliedIndex: 106,
	}, opts1002)
	leader1002.cfg = types.Config{
		MigrateFrom: 1002,
		MigrateTo:   1001,
		Replicas:    []uint64{1001, 1002, 1003},
		Leader:      1002,
		Term:        4,
		Version:     233,
	}
	leader1002.BecomeLeader(4)
	_ = leader1002.Ready() // persist hard state

	if !leader1002.LeaderReadReady() {
		t.Fatalf("expected leader1002 initially LeaderReadReady() == true")
	}

	// Follower 1001 affected by P2-C1 reports e.Index = 107 (lastLogIndex+1 = 106+1),
	// but e.StoredIndex = 104 (storedIndex+1 = 103+1, K=3 lag).
	syncReqFrom1001 := types.Event{
		Type:          types.SyncReq,
		From:          1001,
		To:            1002,
		Term:          4,
		Index:         107,
		StoredIndex:   104,
		ConfigVersion: 233,
		Reason:        types.ReasonOnlySync,
	}
	if err := leader1002.Step(syncReqFrom1001); err != nil {
		t.Fatalf("Step(SyncReq) error: %v", err)
	}
	events := leader1002.Ready()
	emittedFollowToLeader := false
	for _, ev := range events {
		if ev.Type == types.FollowerToLeaderReq {
			emittedFollowToLeader = true
		}
	}

	proposeErr := leader1002.Step(leader1002.NewPropose([]byte("client-write")))
	t.Logf("[P2-C2 HANDOFF STATE] stopPropose=%v LeaderReadReady=%v emittedFollowerToLeaderReq=%v proposeErr=%v",
		leader1002.stopPropose, leader1002.LeaderReadReady(), emittedFollowToLeader, proposeErr)

	if emittedFollowToLeader {
		t.Fatalf("unexpected FollowerToLeaderReq when StoredIndex (104) < lastLogIndex+1 (107)")
	}
	if !leader1002.stopPropose || leader1002.LeaderReadReady() {
		t.Fatalf("expected stopPropose=true and LeaderReadReady=false, got stopPropose=%v LeaderReadReady=%v",
			leader1002.stopPropose, leader1002.LeaderReadReady())
	}
}

// TestXIN2839_P2H2_ArchGap_AppendCase5aIgnoresTermConflict tests Architecture Report Gap 1:
// When a Follower has uncommitted logs [101..102] at Term 2 (lastLogIndex=102) and receives
// a SyncResp with logs [101..102] at Term 3 (incomingLastIndex=102 <= lastLogIndex=102),
// queue.append hits case 5a ("ignoring completely duplicate logs") without checking Term!
func TestXIN2839_P2H2_ArchGap_AppendCase5aIgnoresTermConflict(t *testing.T) {
	opts := NewOptions(
		WithNodeId(1001),
		WithReplicas([]uint64{1001, 1002, 1003}),
		WithKey("slot-arch-h2"),
	)
	follower := NewNode(1, types.RaftState{
		LastLogIndex: 100,
		LastTerm:     2,
		AppliedIndex: 100,
	}, opts)
	follower.BecomeFollower(2, 1002)
	_ = follower.Ready()

	// Step 1: Follower receives [101, 102] from old Leader 1002 in Term 2
	oldLeaderBatch := []types.Log{
		{Id: 101, Index: 101, Term: 2, Data: []byte("old-leader-term2-101")},
		{Id: 102, Index: 102, Term: 2, Data: []byte("old-leader-term2-102")},
	}
	if err := follower.Step(types.Event{
		Type:           types.SyncResp,
		From:           1002,
		Term:           2,
		Reason:         types.ReasonOk,
		Logs:           oldLeaderBatch,
		CommittedIndex: 100,
	}); err != nil {
		t.Fatalf("Step oldLeaderBatch failed: %v", err)
	}

	// Step 2: New Leader 1003 in Term 3 sends conflicting [101, 102] with Term 3 (incomingLastIndex=102 <= lastLogIndex=102)
	follower.BecomeFollower(3, 1003)
	newLeaderBatch := []types.Log{
		{Id: 201, Index: 101, Term: 3, Data: []byte("new-leader-term3-101")},
		{Id: 202, Index: 102, Term: 3, Data: []byte("new-leader-term3-102")},
	}
	if err := follower.Step(types.Event{
		Type:           types.SyncResp,
		From:           1003,
		Term:           3,
		Reason:         types.ReasonOk,
		Logs:           newLeaderBatch,
		CommittedIndex: 102,
	}); err != nil {
		t.Fatalf("Step newLeaderBatch failed: %v", err)
	}

	gotTerm101 := follower.queue.logs[0].Term
	gotData101 := string(follower.queue.logs[0].Data)
	t.Logf("[P2-H2 ARCH GAP 1] queue.logs[0]: index=%d term=%d data=%q (expected term=3 data=%q)",
		follower.queue.logs[0].Index, gotTerm101, gotData101, "new-leader-term3-101")

	if gotTerm101 != 3 || gotData101 != "new-leader-term3-101" {
		t.Fatalf("P2-H2 REPRODUCED: queue.append case 5a silently ignored conflicting Term=3 logs because incomingLastIndex(102) <= lastLogIndex(102); retained stale Term=%d Data=%q",
			gotTerm101, gotData101)
	}
}

// TestXIN2839_P2H3_ArchGap_InFlightStoreRespWithoutRevisionFence tests Architecture Report Gap 2:
// While StoreReq(103) is in flight (queue.appending == true), an overlapping SyncResp [99..106]
// triggers truncateLogTo(98) (incrementing queue.revision), and then the stale pre-truncation
// StoreResp{Index: 103} arrives. Because StoreResp has no revision check and truncateLogTo does not
// reset appending=false, Ready() cannot emit a StoreReq for the new [99..103] prefix and the stale
// StoreResp(103) prematurely marks storedIndex=103 and discards the new [99..103] logs from memory!
func TestXIN2839_P2H3_ArchGap_InFlightStoreRespWithoutRevisionFence(t *testing.T) {
	opts := NewOptions(
		WithNodeId(1001),
		WithReplicas([]uint64{1001, 1002, 1003}),
		WithKey("slot-arch-h3"),
	)
	follower := NewNode(1, types.RaftState{
		LastLogIndex: 100,
		LastTerm:     2,
		AppliedIndex: 90,
	}, opts)
	follower.BecomeFollower(2, 1002)
	_ = follower.Ready()

	// 1. Append [101..103] and call Ready() so StoreReq(101..103) is in flight (appending=true)
	if err := follower.Step(types.Event{
		Type:           types.SyncResp,
		From:           1002,
		Term:           2,
		Reason:         types.ReasonOk,
		Logs:           makeSyntheticLogs(101, 103, 2, "batch1"),
		CommittedIndex: 90,
	}); err != nil {
		t.Fatalf("Step batch1 failed: %v", err)
	}
	ready1 := follower.Ready()
	var storeReqCount1 int
	for _, ev := range ready1 {
		if ev.Type == types.StoreReq {
			storeReqCount1++
		}
	}
	revBefore := follower.queue.revision
	t.Logf("[P2-H3 STEP 1] storeReqCount1=%d appending=%v storedIndex=%d lastLogIndex=%d revision=%d",
		storeReqCount1, follower.queue.appending, follower.queue.storedIndex, follower.queue.lastLogIndex, revBefore)

	// 2. While StoreReq(103) is still in flight, overlapping SyncResp [99..106] (Term 3) arrives
	if err := follower.Step(types.Event{
		Type:           types.SyncResp,
		From:           1002,
		Term:           2,
		Reason:         types.ReasonOk,
		Logs:           makeSyntheticLogs(99, 106, 3, "batch2-new"),
		CommittedIndex: 90,
	}); err != nil {
		t.Fatalf("Step batch2 overlapping failed: %v", err)
	}
	ready2 := follower.Ready()
	var storeReqCount2 int
	for _, ev := range ready2 {
		if ev.Type == types.StoreReq {
			storeReqCount2++
		}
	}
	revAfter := follower.queue.revision
	t.Logf("[P2-H3 STEP 2 AFTER TRUNCATE WHILE APPENDING] storeReqCount2=%d (blocked by stale appending=%v) revision=%d->%d storedIndex=%d lastLogIndex=%d",
		storeReqCount2, follower.queue.appending, revBefore, revAfter, follower.queue.storedIndex, follower.queue.lastLogIndex)

	// 3. Stale StoreResp{Index: 103} from batch1 arrives (no revision check!)
	if err := follower.Step(types.Event{
		Type:   types.StoreResp,
		From:   types.LocalNode,
		Index:  103,
		Reason: types.ReasonOk,
	}); err != nil {
		t.Fatalf("Step stale StoreResp failed: %v", err)
	}
	ready3 := follower.Ready()
	var persistedNewLogs []types.Log
	for _, ev := range ready3 {
		if ev.Type == types.StoreReq {
			persistedNewLogs = append(persistedNewLogs, ev.Logs...)
		}
	}
	t.Logf("[P2-H3 STEP 3 AFTER STALE STORERESP(103)] storedIndex=%d lastLogIndex=%d Ready3StoreReqLogsLen=%d firstPersistedNewIdx=%v",
		follower.queue.storedIndex, follower.queue.lastLogIndex, len(persistedNewLogs),
		func() uint64 {
			if len(persistedNewLogs) > 0 {
				return persistedNewLogs[0].Index
			}
			return 0
		}())

	// For correctness, all 8 logs [99..106] of batch2-new must be emitted in a StoreReq.
	if len(persistedNewLogs) != 8 || persistedNewLogs[0].Index != 99 || persistedNewLogs[0].Term != 3 {
		t.Fatalf("P2-H3 REPRODUCED: stale StoreResp(103) across revision %d->%d was accepted without revision check; new logs [99..103] were sliced off and NEVER sent to StoreReq (Ready3 emitted only %d logs starting at index %v)",
			revBefore, revAfter, len(persistedNewLogs), func() uint64 {
				if len(persistedNewLogs) > 0 {
					return persistedNewLogs[0].Index
				}
				return 0
			}())
	}
}

// TestXIN2839_S4_TwoVoterQuorum_SingleNodeFailureBlocksCommit proves that in octo-im,
// Replicas=[1001, 1002] (SlotReplicaCount=2, which includes Leader) has quorum() == 2.
// When single Follower 1002 fails, Leader 1001 cannot advance committedIndex for any new write.
func TestXIN2839_S4_TwoVoterQuorum_SingleNodeFailureBlocksCommit(t *testing.T) {
	opts := NewOptions(
		WithNodeId(1001),
		WithReplicas([]uint64{1001, 1002}),
		WithKey("slot-s4-r2"),
	)
	leader := NewNode(1, types.RaftState{
		LastLogIndex: 10,
		LastTerm:     1,
		AppliedIndex: 10,
	}, opts)
	leader.BecomeLeader(1)
	_ = leader.Ready()

	qVal := leader.quorum()
	t.Logf("[S4 QUORUM CHECK] Replicas=%v Leader=%d quorum()=%d (expected 2)", leader.cfg.Replicas, leader.LeaderId(), qVal)
	if qVal != 2 {
		t.Fatalf("expected quorum() == 2 for 2-voter group, got %d", qVal)
	}

	// Phase 1: Follower 1002 is online -> Propose index 11 commits after 1001 + 1002 store it
	if err := leader.Step(leader.NewPropose([]byte("msg-11-healthy"))); err != nil {
		t.Fatalf("Propose 11 err: %v", err)
	}
	_ = leader.Ready()
	_ = leader.Step(types.Event{Type: types.StoreResp, From: types.LocalNode, Index: 11, Reason: types.ReasonOk})
	_ = leader.Step(types.Event{Type: types.SyncReq, From: 1002, To: 1001, Term: 1, Index: 12, StoredIndex: 12, Reason: types.ReasonOnlySync})
	t.Logf("[S4 HEALTHY 2/2] after Follower 1002 ack: storedIndex=%d committedIndex=%d", leader.queue.storedIndex, leader.CommittedIndex())
	if leader.CommittedIndex() != 11 {
		t.Fatalf("expected committedIndex=11 when 2/2 online, got %d", leader.CommittedIndex())
	}

	// Phase 2: Follower 1002 fails (stops sending SyncReq). Leader 1001 proposes index 12 and stores locally.
	if err := leader.Step(leader.NewPropose([]byte("msg-12-after-1002-down"))); err != nil {
		t.Fatalf("Propose 12 err: %v", err)
	}
	_ = leader.Ready()
	_ = leader.Step(types.Event{Type: types.StoreResp, From: types.LocalNode, Index: 12, Reason: types.ReasonOk})
	for tick := 0; tick < 50; tick++ {
		leader.Tick()
		_ = leader.Ready()
	}
	t.Logf("[S4 SINGLE NODE FAILURE 1/2] Leader storedIndex=%d committedIndex=%d (stuck at 11 because 1 < quorum 2)",
		leader.queue.storedIndex, leader.CommittedIndex())
	if leader.CommittedIndex() != 11 {
		t.Fatalf("expected committedIndex to remain 11 when 1 of 2 voters is down, got %d", leader.CommittedIndex())
	}
}

func TestP1_LearnerPromotionRequiresAppliedIndexCatchUpAndWireCompat(t *testing.T) {
	opts := NewOptions(
		WithNodeId(1001),
		WithReplicas([]uint64{1001, 1002}),
		WithLearnerToFollowerMaxApplyLag(8),
		WithKey("slot-p1-apply-gate"),
	)
	leader := NewNode(1, types.RaftState{
		LastLogIndex: 100,
		LastTerm:     2,
		AppliedIndex: 100,
	}, opts)
	leader.cfg = types.Config{
		MigrateFrom: 1003,
		MigrateTo:   1003,
		Replicas:    []uint64{1001, 1002},
		Learners:    []uint64{1003},
		Leader:      1001,
		Term:        2,
		Version:     5,
	}
	leader.BecomeLeader(2)
	_ = leader.Ready()

	// 1. Verify SyncReq wire encoding/decoding of AppliedIndex (including 0 -> 1 encoding)
	ev := types.Event{
		Type:         types.SyncReq,
		From:         1003,
		To:           1001,
		Term:         2,
		Index:        101,
		StoredIndex:  101,
		AppliedIndex: 51, // represents appliedIndex = 50 (encoded as +1)
		Reason:       types.ReasonOnlySync,
	}
	wireBytes, err := ev.Marshal()
	if err != nil {
		t.Fatalf("Event.Marshal err: %v", err)
	}
	var decoded types.Event
	if err := decoded.Unmarshal(wireBytes); err != nil {
		t.Fatalf("Event.Unmarshal err: %v", err)
	}
	if decoded.AppliedIndex != 51 {
		t.Fatalf("expected decoded.AppliedIndex=51, got %d", decoded.AppliedIndex)
	}

	// 2. Learner 1003 has caught up StoredIndex=101 (stored=100), but AppliedIndex=51 (applied=50, lag=50 > 8).
	// Leader must NOT emit LearnerToFollowerReq yet.
	if err := leader.Step(decoded); err != nil {
		t.Fatalf("Step SyncReq lagging apply err: %v", err)
	}
	for _, out := range leader.Ready() {
		if out.Type == types.LearnerToFollowerReq {
			t.Fatalf("unexpected LearnerToFollowerReq while AppliedIndex(50) lags LastLogIndex(100) by 50 > 8")
		}
	}

	// 3. Learner 1003 catches up AppliedIndex to 96 (applied=95, lag=5 <= 8).
	decoded.AppliedIndex = 96
	if err := leader.Step(decoded); err != nil {
		t.Fatalf("Step SyncReq caught-up apply err: %v", err)
	}
	promoted := false
	for _, out := range leader.Ready() {
		if out.Type == types.LearnerToFollowerReq && out.From == 1003 {
			promoted = true
		}
	}
	if !promoted {
		t.Fatalf("expected LearnerToFollowerReq once AppliedIndex caught up within LearnerToFollowerMaxApplyLag=8")
	}
}

func TestP1_HandoffAdmissionDrainTimeoutRecoversLocalLeader(t *testing.T) {
	opts := NewOptions(
		WithNodeId(1001),
		WithReplicas([]uint64{1001, 1002, 1003}),
		WithMaxAdmissionDrainTicks(5),
		WithKey("slot-handoff-timeout"),
	)
	leader := NewNode(1, types.RaftState{
		LastLogIndex: 50,
		LastTerm:     2,
		AppliedIndex: 50,
	}, opts)
	leader.cfg = types.Config{
		MigrateFrom: 1001,
		MigrateTo:   1002,
		Replicas:    []uint64{1001, 1002, 1003},
		Leader:      1001,
		Term:        2,
		Version:     10,
	}
	leader.BecomeLeader(2)
	_ = leader.Ready()

	// Trigger handoff pause before FollowerToLeaderReq can complete (e.g. target 1002 is within gap but not caught up)
	_ = leader.Step(types.Event{
		Type:          types.SyncReq,
		From:          1002,
		To:            1001,
		Term:          2,
		Index:         45,
		StoredIndex:   45,
		ConfigVersion: 10,
		Reason:        types.ReasonOnlySync,
	})
	_ = leader.Ready()
	if !leader.stopPropose || leader.LeaderReadReady() {
		t.Fatalf("expected leader paused during handoff drain")
	}

	// Target 1002 stalls; after MaxAdmissionDrainTicks (5 ticks), leader must recover stopPropose=false and LeaderReadReady()=true.
	for i := 0; i < 6; i++ {
		leader.Tick()
	}
	if leader.stopPropose || !leader.LeaderReadReady() {
		t.Fatalf("expected leader to recover from stalled handoff after MaxAdmissionDrainTicks, got stopPropose=%v LeaderReadReady=%v",
			leader.stopPropose, leader.LeaderReadReady())
	}
}

func TestP2_DuplicateSameTermPrefixPreservedAndCommittedTruncationRejected(t *testing.T) {
	q := newQueue("dup-prefix-test", 10, 10)
	if err := q.append(
		types.Log{Index: 11, Term: 2, Data: []byte("a")},
		types.Log{Index: 12, Term: 2, Data: []byte("b")},
	); err != nil {
		t.Fatalf("append [11..12] err: %v", err)
	}
	// Duplicate prefix [11..12] with same Term 2 + new suffix [13] must NOT truncate [11..12]
	revBefore := q.storeEpoch
	if err := q.append(
		types.Log{Index: 11, Term: 2, Data: []byte("a")},
		types.Log{Index: 12, Term: 2, Data: []byte("b")},
		types.Log{Index: 13, Term: 2, Data: []byte("c")},
	); err != nil {
		t.Fatalf("append overlapping same-term [11..13] err: %v", err)
	}
	if q.storeEpoch != revBefore {
		t.Fatalf("same-term duplicate prefix must not increment storeEpoch (got %d -> %d)", revBefore, q.storeEpoch)
	}
	if q.lastLogIndex != 13 || len(q.logs) != 3 {
		t.Fatalf("expected lastLogIndex=13 len(logs)=3, got lastLogIndex=%d len(logs)=%d", q.lastLogIndex, len(q.logs))
	}

	// Advance committedIndex to 11 (in-memory log 11 has Term 2); conflicting term at committed index 11 must return ErrCommittedLogConflict
	q.committedIndex = 11
	err := q.append(types.Log{Index: 11, Term: 99, Data: []byte("conflict-committed")})
	if err != types.ErrCommittedLogConflict {
		t.Fatalf("expected ErrCommittedLogConflict when conflicting at committed index 11, got %v", err)
	}
}

// legacyMarshalEvent21cd8f88 and legacyUnmarshalEvent21cd8f88 reproduce the exact
// 13-bit (bits 0..12, without bit 13 appliedIndexFlag) Event wire codec from
// release tag v2.2.5-20261021-fix1 (21cd8f8860eba7804f04039190ec8d92d620cf0b).
func legacyMarshalEvent21cd8f88(e types.Event) ([]byte, error) {
	e.AppliedIndex = 0
	return e.Marshal()
}

func legacyUnmarshalEvent21cd8f88(data []byte) (types.Event, error) {
	// In 21cd8f88, Event.Unmarshal read bits 0..12 in order and stopped after speedFlag (bit 12),
	// ignoring any trailing bytes or unknown flag bits >= 13.
	var full types.Event
	if err := full.Unmarshal(data); err != nil {
		return types.Event{}, err
	}
	full.AppliedIndex = 0
	return full, nil
}

func TestAppliedIndexCrossVersionWireCompatibility(t *testing.T) {
	// Direction 1: New (14-bit with AppliedIndex=89) -> Old (21cd8f88 13-bit decoder)
	newSyncReq := types.Event{
		Type:          types.SyncReq,
		From:          1003,
		To:            1001,
		Term:          4,
		Index:         101,
		StoredIndex:   101,
		AppliedIndex:  89,
		LastLogTerm:   4,
		ConfigVersion: 7,
		Reason:        types.ReasonOnlySync,
	}
	wireBytes, err := newSyncReq.Marshal()
	if err != nil {
		t.Fatalf("new Event.Marshal err: %v", err)
	}
	// Verify raw flag header has bit 13 (appliedIndexFlag = 1<<13 = 8192) set and 8 trailing bytes for AppliedIndex.
	legacyBytes, err := legacyMarshalEvent21cd8f88(newSyncReq)
	if err != nil {
		t.Fatalf("legacyMarshalEvent21cd8f88 err: %v", err)
	}
	if len(wireBytes) != len(legacyBytes)+8 {
		t.Fatalf("expected new SyncReq wire frame to be exactly 8 bytes longer than 21cd8f88 frame, got new=%d legacy=%d", len(wireBytes), len(legacyBytes))
	}
	// Verify prefix bytes (after 4-byte flag) before AppliedIndex are byte-for-byte identical to 21cd8f88.
	if string(wireBytes[4:len(legacyBytes)]) != string(legacyBytes[4:]) {
		t.Fatalf("field payload before AppliedIndex diverged from 21cd8f88 wire layout")
	}
	oldDecoded, err := legacyUnmarshalEvent21cd8f88(wireBytes)
	if err != nil {
		t.Fatalf("legacyUnmarshalEvent21cd8f88 failed on new frame: %v", err)
	}
	if oldDecoded.Type != newSyncReq.Type || oldDecoded.From != newSyncReq.From || oldDecoded.To != newSyncReq.To ||
		oldDecoded.Term != newSyncReq.Term || oldDecoded.Index != newSyncReq.Index || oldDecoded.StoredIndex != newSyncReq.StoredIndex ||
		oldDecoded.LastLogTerm != newSyncReq.LastLogTerm || oldDecoded.ConfigVersion != newSyncReq.ConfigVersion || oldDecoded.Reason != newSyncReq.Reason {
		t.Fatalf("legacy decoded fields mismatch: got %+v", oldDecoded)
	}

	// Direction 2: Old (21cd8f88 13-bit encoder without bit 13) -> New (14-bit decoder + leader roleSwitchIfNeed fallback)
	var newDecoded types.Event
	if err := newDecoded.Unmarshal(legacyBytes); err != nil {
		t.Fatalf("new Event.Unmarshal failed on 21cd8f88 legacy frame: %v", err)
	}
	if newDecoded.AppliedIndex != 0 {
		t.Fatalf("expected AppliedIndex=0 when decoding 21cd8f88 frame, got %d", newDecoded.AppliedIndex)
	}

	// Verify a new leader receiving a 21cd8f88 SyncReq (AppliedIndex=0) falls back to StoredIndex and promotes the learner.
	leader := NewNode(1, types.RaftState{
		LastLogIndex: 100,
		LastTerm:     4,
		AppliedIndex: 100,
	}, NewOptions(
		WithNodeId(1001),
		WithReplicas([]uint64{1001, 1002}),
		WithLearnerToFollowerMinLogGap(10),
		WithLearnerToFollowerMaxApplyLag(8),
		WithKey("cross-version-slot"),
	))
	leader.cfg = types.Config{
		MigrateFrom: 1002,
		MigrateTo:   1003,
		Replicas:    []uint64{1001, 1002},
		Learners:    []uint64{1003},
		Leader:      1001,
		Term:        4,
		Version:     7,
	}
	leader.BecomeLeader(4)
	_ = leader.Ready()
	leader.replicaSync[1002] = &SyncInfo{StoredIndex: 101, AppliedIndex: 101, ConfigVersion: 7}

	if err := leader.Step(newDecoded); err != nil {
		t.Fatalf("leader.Step(newDecoded from 21cd8f88) err: %v", err)
	}
	var promoted bool
	for _, ev := range leader.Ready() {
		if ev.Type == types.LearnerToFollowerReq && ev.From == 1003 {
			promoted = true
		}
	}
	if !promoted {
		t.Fatalf("expected new leader to promote 21cd8f88 learner via StoredIndex fallback when AppliedIndex=0")
	}
}

func TestRaftProposeBatchAdmissionDrainsAndReleases(t *testing.T) {
	leader := NewNode(1, types.RaftState{
		LastLogIndex: 5,
		LastTerm:     2,
		AppliedIndex: 5,
	}, NewOptions(
		WithNodeId(1001),
		WithReplicas([]uint64{1001, 1002}),
		WithKey("raft-admit-batch"),
	))
	leader.cfg = types.Config{
		MigrateFrom: 1001,
		MigrateTo:   1002,
		Replicas:    []uint64{1001, 1002},
		Leader:      1001,
		Term:        2,
		Version:     4,
	}
	leader.BecomeLeader(2)
	_ = leader.Ready()

	logs := []types.Log{{Id: 701, Term: 2, Index: 6, Data: []byte("batch-1")}}
	release, err := leader.AdmitProposalBatch(logs)
	if err != nil {
		t.Fatalf("AdmitProposalBatch err: %v", err)
	}
	if !leader.hasPendingAdmissions() {
		t.Fatalf("expected hasPendingAdmissions()=true after AdmitProposalBatch")
	}

	// SyncReq arrives while batch 701 is admitted: leader enters drainingAdmissions, does not emit FollowerToLeaderReq yet.
	if err := leader.Step(types.Event{
		Type:          types.SyncReq,
		From:          1002,
		To:            1001,
		Term:          2,
		Index:         6,
		StoredIndex:   6,
		AppliedIndex:  6,
		ConfigVersion: 4,
		Reason:        types.ReasonOnlySync,
	}); err != nil {
		t.Fatalf("Step SyncReq err: %v", err)
	}
	for _, ev := range leader.Ready() {
		if ev.Type == types.FollowerToLeaderReq {
			t.Fatalf("unexpected FollowerToLeaderReq while batch 701 is admitted")
		}
	}

	// Subsequent AdmitProposalBatch during drain must be rejected with ErrProposalDropped.
	if _, err := leader.AdmitProposalBatch([]types.Log{{Id: 702, Term: 2, Index: 7}}); err != types.ErrProposalDropped {
		t.Fatalf("expected ErrProposalDropped for new batch during drain, got %v", err)
	}

	// Admitted batch 701 succeeds through Step(Propose) and releases its admission entry.
	if err := leader.Step(types.Event{Type: types.Propose, Logs: logs}); err != nil {
		t.Fatalf("Step admitted Propose err: %v", err)
	}
	release()
	if leader.hasPendingAdmissions() {
		t.Fatalf("expected hasPendingAdmissions()=false after release()")
	}
}

func TestBase21cd8f88_RedProof_P2_H2_SameTermOverlapTruncatesPrefix(t *testing.T) {
	q := newQueue("red-h2", 0, 0)
	_ = q.append(
		types.Log{Index: 1, Term: 1, Data: []byte("a")},
		types.Log{Index: 2, Term: 1, Data: []byte("b")},
		types.Log{Index: 3, Term: 1, Data: []byte("c")},
	)
	q.storeTo(1)
	_ = q.append(
		types.Log{Index: 1, Term: 1, Data: []byte("a")},
		types.Log{Index: 2, Term: 1, Data: []byte("b")},
		types.Log{Index: 3, Term: 1, Data: []byte("c")},
		types.Log{Index: 4, Term: 1, Data: []byte("d")},
	)
	if q.storedIndex == 0 {
		t.Fatalf("RED_PROOF_H2: same-term overlapping append reset storedIndex to 0")
	}
	if q.lastLogIndex != 4 {
		t.Fatalf("expected lastLogIndex=4, got %d", q.lastLogIndex)
	}
}

func TestBase21cd8f88_RedProof_P2_H3_StaleStoreRespAdvancesStoredIndexPastTruncatedTail(t *testing.T) {
	n := NewNode(1, types.RaftState{LastLogIndex: 2, LastTerm: 1, AppliedIndex: 1}, NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2}), WithKey("red-h3")))
	n.BecomeFollower(2, 1)
	_ = n.Step(types.Event{
		Type: types.SyncResp, From: 1, To: 2, Term: 2, Index: 3,
		Logs: []types.Log{{Index: 3, Term: 2, Data: []byte("m3")}, {Index: 4, Term: 2, Data: []byte("m4")}},
		Reason: types.ReasonOk,
	})
	_ = n.Ready() // starts in-flight StoreReq up to index 4
	n.queue.truncateLogTo(2)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("RED_PROOF_H3: unfenced stale StoreResp panicked after truncation: %v", r)
		}
	}()
	_ = n.Step(types.Event{Type: types.StoreResp, Index: 4, Reason: types.ReasonOk})
	if n.queue.storedIndex != 2 {
		t.Fatalf("expected storedIndex=2 after ignoring stale StoreResp, got %d", n.queue.storedIndex)
	}
}
