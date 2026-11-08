package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func transferTestLeader(t *testing.T, learner bool) *Node {
	t.Helper()
	n := NewNode(1, types.RaftState{LastLogIndex: 12, LastTerm: 1, AppliedIndex: 12,
		HardState: types.HardState{Term: 1, Vote: 1}}, NewOptions(WithNodeId(1), WithAdvance(func() {})))
	cfg := types.Config{Term: 1, Version: 7, Leader: 1, Replicas: []uint64{1, 2, 3},
		MigrateFrom: 1, MigrateTo: 3}
	if learner {
		cfg.Replicas, cfg.Learners = []uint64{1, 2}, []uint64{3}
	}
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	n.Ready()
	return n
}

func beginTestTransfer(t *testing.T, n *Node) types.Event {
	t.Helper()
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3,
		Index: 13, StoredIndex: 13, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
	for _, e := range n.Ready() {
		if e.Type == types.FollowerToLeaderReq || e.Type == types.LearnerToLeaderReq {
			return e
		}
	}
	t.Fatal("caught-up transfer did not start")
	return types.Event{}
}

func TestLeadershipTransferRequestKeepsOwnerEpoch(t *testing.T) {
	n := transferTestLeader(t, false)
	request := beginTestTransfer(t, n)
	require.Equal(t, n.Config().Term, request.Term)
	require.Equal(t, n.Config().Version, request.ConfigVersion)
}

func TestLeadershipTransferFenceSurvivesVersionPublication(t *testing.T) {
	n := transferTestLeader(t, false)
	request := beginTestTransfer(t, n)
	cfg := n.Config().Clone()
	cfg.Version++
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.NoError(t, n.Step(types.Event{Type: types.FollowerToLeaderResp, From: 3,
		Term: request.Term, ConfigVersion: request.ConfigVersion, RoleChangeID: request.RoleChangeID, Reason: types.ReasonOk}))
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("same leader, newer config"))), types.ErrProposalDropped)
}

func TestLeadershipTransferRejectsLateProposal(t *testing.T) {
	for _, learner := range []bool{false, true} {
		t.Run(map[bool]string{false: "follower", true: "learner"}[learner], func(t *testing.T) {
			n := transferTestLeader(t, learner)
			late := n.NewPropose([]byte("configuration arriving during SaveConfig"))
			beginTestTransfer(t, n)
			require.ErrorIs(t, n.Step(late), types.ErrProposalDropped)
			require.Equal(t, uint64(12), n.LastLogIndex())
			require.Equal(t, uint64(12), n.CommittedIndex())
		})
	}
}

func TestLeadershipTransferSuccessWaitsForOwnerConfiguration(t *testing.T) {
	n := transferTestLeader(t, false)
	request := beginTestTransfer(t, n)
	require.NoError(t, n.Step(types.Event{Type: types.FollowerToLeaderResp, From: 3,
		Term: request.Term, ConfigVersion: request.ConfigVersion, RoleChangeID: request.RoleChangeID, Reason: types.ReasonOk}))
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("owner configuration still pending"))), types.ErrProposalDropped)
	cfg := n.Config().Clone()
	cfg.Leader, cfg.Term, cfg.Version, cfg.Role = 3, 2, 8, types.RoleFollower
	cfg.MigrateFrom, cfg.MigrateTo = 0, 0
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("old leader"))), types.ErrNotLeader)
}

func TestLeadershipTransferFailureKeepsFenceAndAllowsReconciliation(t *testing.T) {
	n := transferTestLeader(t, false)
	request := beginTestTransfer(t, n)
	require.NoError(t, n.Step(types.Event{Type: types.FollowerToLeaderResp, From: 3,
		Term: request.Term, ConfigVersion: request.ConfigVersion, RoleChangeID: request.RoleChangeID, Reason: types.ReasonError}))
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("save may have committed"))), types.ErrProposalDropped)
	require.ErrorIs(t, n.AdmitMessage(123), types.ErrProposalDropped)
	// The next sync can retry the same version-checked transition, not writes.
	beginTestTransfer(t, n)
}

func TestLeadershipTransferIgnoresUnrelatedCompletion(t *testing.T) {
	n := transferTestLeader(t, false)
	request := beginTestTransfer(t, n)
	for _, completion := range []types.Event{
		{Type: types.LearnerToFollowerResp, From: 2, Reason: types.ReasonOk},
		{Type: types.FollowerToLeaderResp, From: 2, Term: request.Term, ConfigVersion: request.ConfigVersion, RoleChangeID: request.RoleChangeID, Reason: types.ReasonError},
		{Type: types.FollowerToLeaderResp, From: 3, Term: request.Term, ConfigVersion: request.ConfigVersion + 1, Reason: types.ReasonError},
	} {
		require.NoError(t, n.Step(completion))
		require.ErrorIs(t, n.Step(n.NewPropose([]byte("unrelated completion"))), types.ErrProposalDropped)
	}
}
