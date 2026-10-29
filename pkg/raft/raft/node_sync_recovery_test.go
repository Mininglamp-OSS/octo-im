package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestRemainingLearnerResumesAfterPeerPromotion(t *testing.T) {
	peer := func(id uint64) *Node {
		return NewNode(1, types.RaftState{LastLogIndex: 4, LastTerm: 1, AppliedIndex: 4,
			HardState: types.HardState{Term: 1, Vote: 1}}, NewOptions(WithNodeId(id),
			WithReplicas([]uint64{1}), WithElectionOn(false), WithAdvance(func() {})))
	}
	leader, learner := peer(1), peer(2)
	cfg := types.Config{Term: 1, Version: 1, Leader: 1, Replicas: []uint64{1},
		Learners: []uint64{2, 3}, MigrateFrom: 3, MigrateTo: 3}
	for _, n := range []*Node{leader, learner} {
		require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
		n.Ready()
	}
	require.NoError(t, learner.Step(types.Event{Type: types.SyncResp, From: 1, To: 2, Term: 1,
		Index: 5, CommittedIndex: 4, Reason: types.ReasonOk, Speed: types.SpeedSuspend}))
	learner.Ready()
	require.True(t, learner.Suspend())
	cfg.Version, cfg.Replicas, cfg.Learners = 2, []uint64{1, 3}, []uint64{2}
	cfg.MigrateFrom, cfg.MigrateTo = 0, 0
	require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Empty(t, leader.replicaSync, "configuration publication discards prior sync bookkeeping")
	require.NoError(t, leader.Step(types.Event{Type: types.LearnerToFollowerResp,
		From: 3, To: 1, Term: 1, Reason: types.ReasonOk}))
	wakes := 0
	for _, e := range leader.Ready() {
		if e.Type == types.NotifySync && e.To == 2 {
			wakes++
			require.NoError(t, learner.Step(e))
		}
	}
	require.Equal(t, 1, wakes, "wake the remaining learner even when syncInfo was reset")
	require.False(t, learner.Suspend())
	requests := 0
	for _, e := range learner.Ready() {
		if e.Type == types.SyncReq {
			requests++
		}
	}
	require.Equal(t, 1, requests)
}

func TestFailedPromotionDoesNotWakeRemainingLearners(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	cfg := types.Config{Term: 1, Leader: 1, Replicas: []uint64{1}, Learners: []uint64{2, 3}}
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	n.Ready()
	require.NoError(t, n.Step(types.Event{Type: types.LearnerToFollowerResp, From: 3, Reason: types.ReasonError}))
	for _, e := range n.Ready() {
		require.NotEqual(t, types.NotifySync, e.Type)
	}
}

func TestNewLeaderHeartbeatCannotCommitUnverifiedSuffix(t *testing.T) {
	for _, role := range []types.Role{types.RoleFollower, types.RoleLearner} {
		t.Run(role.String(), func(t *testing.T) {
			n := NewNode(2, types.RaftState{LastLogIndex: 4, LastTerm: 2, AppliedIndex: 1,
				HardState: types.HardState{Term: 2}}, NewOptions(WithNodeId(2), WithAdvance(func() {})))
			cfg := types.Config{Term: 3, Leader: 3, Role: role, Replicas: []uint64{1, 2, 3}}
			if role == types.RoleLearner {
				cfg.Replicas, cfg.Learners = []uint64{1, 3}, []uint64{2}
			}
			require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
			n.Ready()
			require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 3, To: 2, Term: 3,
				Index: 4, CommittedIndex: 4}))
			require.Equal(t, uint64(1), n.CommittedIndex(), "the old durable suffix may conflict with the new leader")
			for _, e := range n.Ready() {
				require.NotEqual(t, types.ApplyReq, e.Type)
			}
		})
	}
}

func TestTruncateAndFailedSyncKeepPrefixVerificationRequired(t *testing.T) {
	for _, reason := range []types.Reason{types.ReasonTruncate, types.ReasonError} {
		n := NewNode(2, types.RaftState{LastLogIndex: 4, LastTerm: 2, AppliedIndex: 1},
			NewOptions(WithNodeId(2), WithAdvance(func() {})))
		n.BecomeFollower(3, 1)
		require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 3, Index: 1, Reason: reason}))
		require.False(t, n.onlySync, "only a successful prefix check may enable append-only synchronization")
	}
}
