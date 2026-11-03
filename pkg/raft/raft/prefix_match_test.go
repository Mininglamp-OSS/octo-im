package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// A restarted replica can have an uncommitted old-term tail at exactly the
// index which the new leader has committed with different contents.
func TestRestartedReplicaDoesNotApplyUnmatchedTail(t *testing.T) {
	for _, learner := range []bool{false, true} {
		t.Run(map[bool]string{false: "follower", true: "learner"}[learner], func(t *testing.T) {
			n := NewNode(1, types.RaftState{LastLogIndex: 12, LastTerm: 1, AppliedIndex: 11}, NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3})))
			if learner {
				makeLearner(n, 2, 1)
			} else {
				makeFollower(n, 2, 1)
			}
			require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 1, Term: 2, CommittedIndex: 12}))
			require.Equal(t, uint64(11), n.CommittedIndex(), "a heartbeat cannot certify the old local tail")
			require.Zero(t, countEvents(collectEvents(n), types.ApplyReq))
			require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 2, Reason: types.ReasonTruncate, Index: 11}))
			clearEvents(n)
			require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 1, Term: 2, CommittedIndex: 12}))
			require.Equal(t, uint64(11), n.CommittedIndex(), "truncation has not reached disk yet")
			require.NoError(t, n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonOk, Index: 11}))
			clearEvents(n)
			require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 2, Reason: types.ReasonOk, Index: 12,
				CommittedIndex: 12, Logs: []types.Log{{Index: 12, Term: 2, Data: []byte("canonical")}}}))
			require.Equal(t, uint64(11), n.CommittedIndex(), "received data is not yet durable")
			events := collectEvents(n)
			stored, ok := findEvent(events, types.StoreReq)
			require.True(t, ok)
			require.Equal(t, []byte("canonical"), stored.Logs[0].Data)
			require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 12}))
			require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 1, Term: 2, CommittedIndex: 12}))
			apply, ok := findEvent(collectEvents(n), types.ApplyReq)
			require.True(t, ok, "repaired durable data must still make progress")
			require.Equal(t, uint64(12), apply.StartIndex)
			require.Equal(t, uint64(13), apply.EndIndex)
		})
	}
}

func TestLeaderDoesNotCountUnmatchedSyncPosition(t *testing.T) {
	n := NewNode(12, types.RaftState{LastLogIndex: 12, LastTerm: 2, AppliedIndex: 11}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3})))
	makeLeader(n, 2)
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 2, Term: 2, Index: 13, StoredIndex: 13, LastLogTerm: 1}))
	require.Equal(t, uint64(11), n.CommittedIndex(), "same index with an unchecked log term is not a quorum acknowledgement")
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 2, Term: 2, Index: 13, StoredIndex: 13, LastLogTerm: 2, Reason: types.ReasonOnlySync}))
	require.Equal(t, uint64(12), n.CommittedIndex(), "confirmed durable progress must count")
}

func TestSyncErrorDoesNotValidateLocalPrefix(t *testing.T) {
	n := NewNode(1, types.RaftState{LastLogIndex: 12, LastTerm: 1, AppliedIndex: 11}, NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3})))
	makeFollower(n, 2, 1)
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 2, Reason: types.ReasonError}))
	require.False(t, n.onlySync)
	require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 1, Term: 2, CommittedIndex: 12}))
	require.Equal(t, uint64(11), n.CommittedIndex())
}

func TestUncheckedSyncCannotPromoteLearner(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	n.cfg.Learners = []uint64{2}
	n.cfg.MigrateFrom, n.cfg.MigrateTo = 2, 2
	n.queue.lastLogIndex, n.queue.storedIndex, n.queue.committedIndex = 1, 1, 1
	n.queue.appliedIndex = 1
	n.replicaSync[2] = &SyncInfo{}
	e := types.Event{From: 2, Index: 2, StoredIndex: 2}
	n.roleSwitchIfNeed(e)
	require.Empty(t, n.Ready(), "an unchecked tail must not authorize a new voter")
	e.Reason = types.ReasonOnlySync
	n.roleSwitchIfNeed(e)
	require.Len(t, findEventsOfType(n.Ready(), types.LearnerToFollowerReq), 1)
}

func TestPrefixAlignmentDoesNotSurviveLeaderChange(t *testing.T) {
	n := NewNode(1, types.RaftState{LastLogIndex: 12, LastTerm: 1, AppliedIndex: 11}, NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3})))
	makeFollower(n, 2, 1)
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 2, Reason: types.ReasonOk, Index: 13}))
	require.True(t, n.onlySync)
	makeFollower(n, 3, 3)
	require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 3, Term: 3, CommittedIndex: 12}))
	require.Equal(t, uint64(11), n.CommittedIndex())
	// A delayed off-owner read from the old term cannot be sent as a new
	// leader's successful alignment response.
	makeLeader(n, 4)
	require.NoError(t, n.Step(types.Event{Type: types.GetLogsResp, Term: 3, To: 1, Reason: types.ReasonOk, Index: 13}))
	require.Zero(t, countEvents(n.Ready(), types.SyncResp))
}

func TestMixedTermCatchupPreservesEveryTermBoundary(t *testing.T) {
	n := newTestNode(2, []uint64{1, 2, 3})
	makeFollower(n, 4, 1)
	require.NoError(t, n.queue.append([]types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 2}, {Index: 4, Term: 3}}...))
	var starts []types.TermStartIndexInfo
	for _, last := range []uint64{2, 3, 4} {
		e, ok := findEvent(n.Ready(), types.StoreReq)
		require.True(t, ok)
		require.Equal(t, last, e.Logs[len(e.Logs)-1].Index)
		for _, log := range e.Logs {
			require.Equal(t, e.Logs[0].Term, log.Term)
		}
		require.NotNil(t, e.TermStartIndexInfo)
		starts = append(starts, *e.TermStartIndexInfo)
		require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Index: last, Reason: types.ReasonOk}))
	}
	require.Equal(t, []types.TermStartIndexInfo{{Term: 1, Index: 1}, {Term: 2, Index: 3}, {Term: 3, Index: 4}}, starts)
	require.Equal(t, uint64(4), n.queue.storedIndex)
	require.Equal(t, uint32(3), n.LastLogTerm())
}
