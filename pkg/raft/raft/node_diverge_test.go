package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// requireFenced checks INV-C3 on a quarantined replica: over many ticks and
// steps it emits no sync, store, apply, ping, vote or role-switch event,
// rejects proposals and leader configuration, and certifies no read.
func requireFenced(t *testing.T, n *Node) {
	t.Helper()
	require.True(t, n.Diverged())
	require.False(t, n.IsLeader())
	require.False(t, n.LeaderReadReady())
	tickN(n, 200)
	n.Step(types.Event{Type: types.NotifySync, Term: n.cfg.Term, From: 2})
	n.Step(types.Event{Type: types.Ping, Term: n.cfg.Term, From: 2, CommittedIndex: 100})
	n.Step(types.Event{Type: types.SyncResp, Term: n.cfg.Term, From: 2, Reason: types.ReasonOk, Logs: termLogs(n.cfg.Term, n.queue.lastLogIndex+1, n.queue.lastLogIndex+3), CommittedIndex: 100})
	n.Step(types.Event{Type: types.SyncResp, Term: n.cfg.Term, From: 2, Reason: types.ReasonTruncate, Index: 1})
	n.Step(types.Event{Type: types.SyncReq, Term: n.cfg.Term, From: 3, Index: 1, StoredIndex: 1})
	n.Step(types.Event{Type: types.VoteReq, Term: n.cfg.Term + 1, From: 3, Logs: []types.Log{{Index: 100, Term: 100}}})
	n.Step(types.Event{Type: types.Campaign})
	for _, e := range n.Ready() {
		require.Contains(t, []types.EventType{types.ProposalFence}, e.Type, "fenced replica emitted %s", e.Type)
	}
	require.False(t, n.HasReady() && len(n.events) > 0)

	_, err := n.Admit([]types.Log{{Data: []byte("p")}})
	require.ErrorIs(t, err, types.ErrSlotDiverged)

	err = n.Step(types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1, 2, 3}, Leader: n.opts.NodeId, Role: types.RoleLeader, Term: n.cfg.Term + 1, Version: n.cfg.Version + 1}})
	require.ErrorIs(t, err, types.ErrSlotDiverged, "ConfChange to leader refused")
	require.False(t, n.IsLeader())
	require.Empty(t, n.Ready())
}

// T4/T5: a refused truncation quarantines a follower or learner.
func TestRefusedTruncationQuarantinesReplica(t *testing.T) {
	for _, learner := range []bool{false, true} {
		for _, reason := range []types.Reason{types.ReasonDiverged, types.ReasonDivergedFailStop} {
			t.Run(map[bool]string{false: "follower", true: "learner"}[learner]+"/"+reason.String(), func(t *testing.T) {
				n := newTestNode(1, []uint64{1, 2, 3})
				if learner {
					makeLearner(n, 2, 2)
				} else {
					makeFollower(n, 2, 2)
				}
				n.onlySync = true
				n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonOk, Logs: termLogs(2, 1, 10), CommittedIndex: 0})
				clearEvents(n)
				n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 10})
				clearEvents(n)
				n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonTruncate, Index: 4})
				trunc, ok := findEvent(collectEvents(n), types.TruncateReq)
				require.True(t, ok)

				committedBefore, lastBefore := n.queue.committedIndex, n.queue.lastLogIndex
				n.Step(types.Event{Type: types.TruncateResp, Reason: reason, Index: trunc.Index})
				require.Equal(t, reason == types.ReasonDivergedFailStop, n.FailStopped())
				require.EqualValues(t, lastBefore, n.queue.lastLogIndex, "no in-memory truncation")
				require.EqualValues(t, committedBefore, n.queue.committedIndex)
				requireFenced(t, n)
			})
		}
	}
}

// A refused append (StoreResp) quarantines the replica the same way.
func TestRefusedAppendQuarantinesReplica(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 2, 2)
	n.onlySync = true
	n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonOk, Logs: termLogs(2, 1, 5)})
	store, ok := findEvent(collectEvents(n), types.StoreReq)
	require.True(t, ok)
	n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonDiverged, Index: store.Logs[len(store.Logs)-1].Index})
	require.False(t, n.queue.appending)
	require.EqualValues(t, 0, n.queue.storedIndex, "nothing marked stored")
	requireFenced(t, n)
}

// Fail-stop dominates and the state never goes back.
func TestQuarantineIsSticky(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 2, 2)
	n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonDiverged})
	require.False(t, n.FailStopped())
	n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonDivergedFailStop})
	require.True(t, n.FailStopped())
	n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonDiverged})
	require.True(t, n.FailStopped())
	// A ConfChange naming another leader is bookkeeping only.
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1, 2, 3}, Leader: 3, Role: types.RoleFollower, Term: 3, Version: 9}}))
	require.EqualValues(t, 3, n.LeaderId())
	requireFenced(t, n)
}

// Defensive: a leader that becomes diverged steps down, emits a proposal
// fence for its term and never replicates or advances commit again.
func TestDivergedLeaderStepsDown(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	n.BecomeLeader(4)
	clearEvents(n)
	_, err := n.Admit([]types.Log{{Data: []byte("a")}})
	require.NoError(t, err)
	clearEvents(n)
	n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonDiverged, Index: 1})
	fence, ok := findEvent(n.events, types.ProposalFence)
	require.True(t, ok, "leader waiters are fenced")
	require.EqualValues(t, 4, fence.Term)
	require.EqualValues(t, None, n.LeaderId())
	committed := n.queue.committedIndex
	n.Step(types.Event{Type: types.SyncReq, Term: 4, From: 2, Index: 2, StoredIndex: 2})
	n.Step(types.Event{Type: types.SyncReq, Term: 4, From: 3, Index: 2, StoredIndex: 2})
	require.EqualValues(t, committed, n.queue.committedIndex, "no commit advance")
	requireFenced(t, n)
}

// T3 at the node: applied > last with Diverged opens quarantined instead of
// panicking, even when its configuration names it leader.
func TestNodeOpensQuarantinedWhenDiverged(t *testing.T) {
	opts := NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithKey("5"))
	var n *Node
	require.NotPanics(t, func() {
		n = NewNode(1, types.RaftState{LastLogIndex: 4, LastTerm: 2, AppliedIndex: 9, Diverged: true}, opts)
	})
	require.EqualValues(t, 9, n.AppliedIndex(), "applied index reported as stored")
	err := n.Step(types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1, 2, 3}, Leader: 1, Role: types.RoleLeader, Term: 3, Version: 1}})
	require.ErrorIs(t, err, types.ErrSlotDiverged)
	requireFenced(t, n)

	// Without Diverged the old invariant check remains for other storages.
	require.Panics(t, func() {
		NewNode(1, types.RaftState{LastLogIndex: 4, LastTerm: 2, AppliedIndex: 9}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3})))
	})
}

// A single-replica slot that opens diverged does not lead itself.
func TestSingleReplicaOpensQuarantined(t *testing.T) {
	n := NewNode(1, types.RaftState{LastLogIndex: 1, LastTerm: 1, AppliedIndex: 1, Diverged: true}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1})))
	require.False(t, n.IsLeader())
	require.True(t, n.Diverged())
}

// T26: ordinary storage errors, timeouts and transient failures never
// quarantine a replica.
func TestOrdinaryFailuresNeverQuarantine(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 2, 2)
	n.onlySync = true
	n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonOk, Logs: termLogs(2, 1, 5)})
	clearEvents(n)
	n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonError})
	n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonError})
	n.Step(types.Event{Type: types.ApplyResp, Reason: types.ReasonError})
	n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonError})
	tickN(n, 500) // sync response timeouts
	n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonUnknown})
	require.False(t, n.Diverged())

	l := newTestNode(1, []uint64{1, 2, 3})
	l.BecomeLeader(3)
	_, err := l.Admit([]types.Log{{Data: []byte("a")}})
	require.NoError(t, err)
	l.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonError})
	tickN(l, 500)
	require.False(t, l.Diverged())
	require.True(t, l.IsLeader())
}

func migratingLeader(t *testing.T, disabled bool, toLearner bool) *Node {
	t.Helper()
	opts := NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithKey("6"), WithLeaderRoleSwitchDisabled(disabled),
		WithLearnerToLeaderMinLogGap(100), WithFollowerToLeaderMinLogGap(100))
	n := NewNode(0, types.RaftState{}, opts)
	cfg := types.Config{Replicas: []uint64{1, 2, 3}, Leader: 1, Role: types.RoleLeader, Term: 2, Version: 1, MigrateFrom: 1, MigrateTo: 3}
	if toLearner {
		cfg.Replicas = []uint64{1, 2}
		cfg.Learners = []uint64{3}
		cfg.MigrateTo = 3
	}
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.True(t, n.IsLeader())
	for i := 0; i < 5; i++ {
		_, err := n.Admit([]types.Log{{Data: []byte("x")}})
		require.NoError(t, err)
	}
	clearEvents(n)
	n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 5})
	clearEvents(n)
	return n
}

// T22: with leader role switches disabled, a migration away from the leader
// never hands off leadership by role switch and never pauses proposals.
func TestLeaderRoleSwitchDisabled(t *testing.T) {
	for _, toLearner := range []bool{false, true} {
		name := map[bool]string{false: "follower", true: "learner"}[toLearner]
		t.Run(name, func(t *testing.T) {
			want := types.FollowerToLeaderReq
			if toLearner {
				want = types.LearnerToLeaderReq
			}
			// Control: without the option the hand-off is requested.
			enabled := migratingLeader(t, false, toLearner)
			enabled.Step(types.Event{Type: types.SyncReq, Term: 2, From: 3, Index: 6, StoredIndex: 6})
			_, ok := findEvent(collectEvents(enabled), want)
			require.True(t, ok, "control: %s requested without the option", want)

			n := migratingLeader(t, true, toLearner)
			// Close enough to pause proposals, then fully caught up.
			n.Step(types.Event{Type: types.SyncReq, Term: 2, From: 3, Index: 3, StoredIndex: 3})
			require.False(t, n.stopPropose, "proposals are never paused for a hand-off")
			n.Step(types.Event{Type: types.SyncReq, Term: 2, From: 3, Index: 6, StoredIndex: 6})
			for _, e := range collectEvents(n) {
				require.NotEqual(t, types.FollowerToLeaderReq, e.Type)
				require.NotEqual(t, types.LearnerToLeaderReq, e.Type)
			}
			require.False(t, n.stopPropose)
			_, err := n.Admit([]types.Log{{Data: []byte("still leading")}})
			require.NoError(t, err)
		})
	}
}

// A learner promotion that does not move leadership is unaffected.
func TestLearnerToFollowerStillAllowedWithRoleSwitchDisabled(t *testing.T) {
	opts := NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2}), WithLeaderRoleSwitchDisabled(true), WithLearnerToFollowerMinLogGap(100))
	n := NewNode(0, types.RaftState{}, opts)
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1, 2}, Learners: []uint64{3}, Leader: 1, Role: types.RoleLeader, Term: 2, Version: 1, MigrateFrom: 2, MigrateTo: 3}}))
	_, err := n.Admit([]types.Log{{Data: []byte("x")}})
	require.NoError(t, err)
	n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 1})
	clearEvents(n)
	n.Step(types.Event{Type: types.SyncReq, Term: 2, From: 3, Index: 2, StoredIndex: 2})
	_, ok := findEvent(collectEvents(n), types.LearnerToFollowerReq)
	require.True(t, ok)
}

// Diverge reasons are local storage results; one that arrives from a peer is
// not acted on.
func TestDivergeReasonFromPeerIgnored(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 2, 2)
	n.Step(types.Event{Type: types.TruncateResp, From: 2, Term: 2, Reason: types.ReasonDiverged})
	n.Step(types.Event{Type: types.StoreResp, From: 3, Term: 2, Reason: types.ReasonDivergedFailStop})
	require.False(t, n.Diverged())
}

// The owner observes every quarantine transition.
func TestOnQuarantineCallback(t *testing.T) {
	var calls []bool
	opts := NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithOnQuarantine(func(failStop bool, _ string) { calls = append(calls, failStop) }))
	n := NewNode(0, types.RaftState{}, opts)
	n.BecomeFollower(2, 2)
	n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonDiverged})
	n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonDiverged})
	n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonDivergedFailStop})
	require.Equal(t, []bool{false, true}, calls)
}
