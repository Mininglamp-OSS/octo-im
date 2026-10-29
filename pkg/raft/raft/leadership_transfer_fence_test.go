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
		Term: request.Term, ConfigVersion: request.ConfigVersion, Reason: types.ReasonOk}))
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("same leader, newer config"))), types.ErrProposalDropped)
}

func TestFullSyncInvalidatesPreviousPeerMatch(t *testing.T) {
	n := transferTestLeader(t, false)
	n.cfg.MigrateFrom, n.cfg.MigrateTo = 0, 0
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3,
		Index: 13, StoredIndex: 13, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
	require.Equal(t, uint64(12), n.GetReplicaLastLogIndex(3))
	// A fresh peer with the same node ID has lost its previous disk contents.
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3,
		Index: 1, StoredIndex: 1, LastLogTerm: 0}))
	require.Zero(t, n.GetReplicaLastLogIndex(3))
	require.Zero(t, n.replicaSync[3].StoredIndex)
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
		Term: request.Term, ConfigVersion: request.ConfigVersion, Reason: types.ReasonOk}))
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("owner configuration still pending"))), types.ErrProposalDropped)
	cfg := n.Config().Clone()
	cfg.Leader, cfg.Term, cfg.Version, cfg.Role = 3, 2, 8, types.RoleFollower
	cfg.MigrateFrom, cfg.MigrateTo = 0, 0
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("old leader"))), types.ErrNotLeader)
}

func TestLeadershipTransferFailureRestoresAdmission(t *testing.T) {
	n := transferTestLeader(t, false)
	request := beginTestTransfer(t, n)
	require.NoError(t, n.Step(types.Event{Type: types.FollowerToLeaderResp, From: 3,
		Term: request.Term, ConfigVersion: request.ConfigVersion, Reason: types.ReasonError}))
	require.NoError(t, n.Step(n.NewPropose([]byte("retry after failed configuration save"))))
}

func TestLeadershipTransferIgnoresUnrelatedCompletion(t *testing.T) {
	n := transferTestLeader(t, false)
	request := beginTestTransfer(t, n)
	for _, completion := range []types.Event{
		{Type: types.LearnerToFollowerResp, From: 2, Reason: types.ReasonOk},
		{Type: types.FollowerToLeaderResp, From: 2, Term: request.Term, ConfigVersion: request.ConfigVersion, Reason: types.ReasonError},
		{Type: types.FollowerToLeaderResp, From: 3, Term: request.Term, ConfigVersion: request.ConfigVersion + 1, Reason: types.ReasonError},
	} {
		require.NoError(t, n.Step(completion))
		require.ErrorIs(t, n.Step(n.NewPropose([]byte("unrelated completion"))), types.ErrProposalDropped)
	}
}

func TestUnverifiedSyncCannotCommitOrTransferLeadership(t *testing.T) {
	n := NewNode(13, types.RaftState{LastLogIndex: 13, LastTerm: 2, AppliedIndex: 12,
		HardState: types.HardState{Term: 2, Vote: 3}}, NewOptions(WithNodeId(3), WithAdvance(func() {})))
	cfg := types.Config{Term: 2, Version: 7, Leader: 3, Replicas: []uint64{1, 2, 3}, MigrateFrom: 3, MigrateTo: 1}
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	n.Ready()
	// The peer's old term-1 entry at index 13 is not an acknowledgement of
	// this leader's conflicting term-2 entry at the same index.
	e := types.Event{Type: types.SyncReq, From: 1, To: 3, Term: 2,
		Index: 14, StoredIndex: 14, LastLogTerm: 1}
	require.NoError(t, n.Step(e))
	require.Equal(t, uint64(12), n.CommittedIndex())
	for _, ready := range n.Ready() {
		require.NotEqual(t, types.FollowerToLeaderReq, ready.Type)
		require.NotEqual(t, types.ApplyReq, ready.Type)
	}
	// A subsequent append-only request follows a successful prefix check.
	e.Reason, e.LastLogTerm = types.ReasonOnlySync, 2
	require.NoError(t, n.Step(e))
	require.Equal(t, uint64(13), n.CommittedIndex())
	transfers := 0
	for _, ready := range n.Ready() {
		if ready.Type == types.FollowerToLeaderReq {
			transfers++
		}
	}
	require.Equal(t, 1, transfers)
}
