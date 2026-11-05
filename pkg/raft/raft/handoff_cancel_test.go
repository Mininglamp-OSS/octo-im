package raft

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func pausedHandoff(t *testing.T, near bool) (*Node, types.Event) {
	t.Helper()
	replicas, applied, next := []uint64{1}, uint64(100), uint64(101)
	if near {
		replicas, applied, next = []uint64{1, 2, 3}, 90, 100
	}
	n := NewNode(1, types.RaftState{LastLogIndex: 100, LastTerm: 7, AppliedIndex: applied}, NewOptions(WithNodeId(1), WithReplicas(replicas)))
	n.BecomeLeader(7)
	cfg := n.Config().Clone()
	cfg.Version, cfg.Learners, cfg.MigrateFrom, cfg.MigrateTo = 1, []uint64{4}, 1, 4
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	n.Ready()
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 4, Term: 7, Index: next, StoredIndex: next, ConfigVersion: 1, Reason: types.ReasonOnlySync}))
	req, found := findEvent(n.Ready(), types.LearnerToLeaderReq)
	require.Equal(t, !near, found)
	require.False(t, n.LeaderReadReady())
	return n, req
}

func cancelledHandoff(t *testing.T, n *Node) {
	t.Helper()
	cfg := n.Config().Clone()
	cfg.Version++
	cfg.Learners, cfg.MigrateFrom, cfg.MigrateTo = nil, 0, 0
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
}

func handoffResponse(req types.Event, reason types.Reason) types.Event {
	req.Type = types.LearnerToLeaderResp
	req.Reason = reason
	return req
}

func commitAfterHandoff(t *testing.T, n *Node) {
	t.Helper()
	for range 100 {
		n.Tick()
		n.Ready()
	}
	require.True(t, n.IsLeader())
	require.Equal(t, uint32(7), n.LastTerm())
	proposal := n.NewPropose([]byte("after-cancellation"))
	require.NoError(t, n.Step(proposal))
	stored, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	index := stored.Logs[len(stored.Logs)-1].Index
	require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Index: index, Reason: types.ReasonOk}))
	if len(n.Config().Replicas) > 1 {
		require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 2, Term: 7, Index: index + 1,
			StoredIndex: index + 1, ConfigVersion: n.Config().Version, Reason: types.ReasonOnlySync}))
	}
	require.Equal(t, index, n.CommittedIndex())
}

func TestHandoffCancellationReleasesOnlyItsFence(t *testing.T) {
	for _, near := range []bool{true, false} {
		reasons := []types.Reason{types.ReasonUnknown, types.ReasonError, types.ReasonOk}
		if near {
			reasons = reasons[:1]
		}
		for _, reason := range reasons {
			t.Run(fmt.Sprintf("near=%t/late=%d", near, reason), func(t *testing.T) {
				n, req := pausedHandoff(t, near)
				cancelledHandoff(t, n)
				if !near && reason != types.ReasonUnknown {
					require.NoError(t, n.Step(handoffResponse(req, reason)))
				}
				commitAfterHandoff(t, n)
			})
		}
	}
}

func TestHandoffCompletionWaitsForAuthoritativeConfig(t *testing.T) {
	for _, reason := range []types.Reason{types.ReasonError, types.ReasonOk} {
		t.Run(fmt.Sprint(reason), func(t *testing.T) {
			n, req := pausedHandoff(t, false)
			require.NoError(t, n.Step(handoffResponse(req, reason)))
			require.ErrorIs(t, n.Step(n.NewPropose([]byte("uncertain-transfer"))), types.ErrProposalDropped)
			cancelledHandoff(t, n)
			commitAfterHandoff(t, n)
		})
	}
}

func TestLateHandoffCompletionCannotReleaseNewTask(t *testing.T) {
	for _, sameTarget := range []bool{false, true} {
		t.Run(fmt.Sprintf("same-target=%t", sameTarget), func(t *testing.T) {
			n, old := pausedHandoff(t, false)
			cfg := n.Config().Clone()
			if sameTarget {
				cancelledHandoff(t, n)
				cfg.Version += 2
			} else {
				cfg.Version++
				cfg.Learners, cfg.MigrateTo = []uint64{4, 5}, 5
			}
			require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
			require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: cfg.MigrateTo, Term: 7, Index: 101, StoredIndex: 101, ConfigVersion: cfg.Version, Reason: types.ReasonOnlySync}))
			_, found := findEvent(n.Ready(), types.LearnerToLeaderReq)
			require.True(t, found)
			require.NoError(t, n.Step(handoffResponse(old, types.ReasonError)))
			require.ErrorIs(t, n.Step(n.NewPropose([]byte("new-task"))), types.ErrProposalDropped)
		})
	}
}

func TestLateHandoffCompletionCannotReleaseRetry(t *testing.T) {
	n, old := pausedHandoff(t, false)
	require.NoError(t, n.Step(handoffResponse(old, types.ReasonError)))
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 4, Term: 7, Index: 101, StoredIndex: 101, ConfigVersion: 1, Reason: types.ReasonOnlySync}))
	_, found := findEvent(n.Ready(), types.LearnerToLeaderReq)
	require.True(t, found)
	require.NoError(t, n.Step(handoffResponse(old, types.ReasonOk)))
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("new-attempt"))), types.ErrProposalDropped)
}

func TestNetworkRoleResponseCannotReleaseHandoff(t *testing.T) {
	n, req := pausedHandoff(t, false)
	response := handoffResponse(req, types.ReasonError)
	encoded, err := response.Marshal()
	require.NoError(t, err)
	var network types.Event
	require.NoError(t, network.Unmarshal(encoded))
	network.Term++ // a wire response must not masquerade as local completion
	require.NoError(t, n.Step(network))
	require.Equal(t, uint32(7), n.LastTerm())
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("network-completion"))), types.ErrProposalDropped)
}

func TestHandoffCancellationPreservesTruncateBarrier(t *testing.T) {
	n, _ := pausedHandoff(t, false)
	n.truncating = true
	cancelledHandoff(t, n)
	require.True(t, n.truncating)
	require.False(t, n.stopPropose, "only the cancelled handoff's fence is released")
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("physical-cut-pending"))), types.ErrProposalDropped)
}

func TestHandoffVersionRefreshRetainsUnknownOutcome(t *testing.T) {
	n, old := pausedHandoff(t, false)
	require.NoError(t, n.Step(handoffResponse(old, types.ReasonError)))
	cfg := n.Config().Clone()
	cfg.Version++
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.True(t, n.handoff.attempted, "a same-task refresh must not look like unattempted admission draining")
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("version-refresh"))), types.ErrProposalDropped)
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 4, Term: 7, Index: 101, StoredIndex: 101,
		ConfigVersion: cfg.Version, Reason: types.ReasonOnlySync}))
	retry, found := findEvent(n.Ready(), types.LearnerToLeaderReq)
	require.True(t, found)
	require.Greater(t, retry.RoleChangeID, old.RoleChangeID)
	require.NoError(t, n.Step(handoffResponse(old, types.ReasonOk)))
	require.True(t, n.replicaSync[4].roleSwitching, "an old completion must not reset the new request")
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("old-generation"))), types.ErrProposalDropped)
}

func TestHandoffVersionRefreshWithUnspecifiedRoleRetainsFence(t *testing.T) {
	n, req := pausedHandoff(t, false)
	require.NoError(t, n.Step(handoffResponse(req, types.ReasonError)))
	cfg := n.Config().Clone()
	cfg.Version++
	cfg.Role = types.RoleUnknown
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("same-leader-role-unspecified"))), types.ErrProposalDropped)
	require.NotNil(t, n.handoff)
	require.True(t, n.handoff.attempted)
	cancelledHandoff(t, n)
	commitAfterHandoff(t, n)
}
