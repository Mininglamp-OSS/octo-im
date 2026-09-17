package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestLocalProposalAdmissionByRole(t *testing.T) {
	for _, role := range []types.Role{types.RoleUnknown, types.RoleFollower, types.RoleCandidate, types.RoleLearner, types.RoleLeader} {
		t.Run(role.String(), func(t *testing.T) {
			n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithAdvance(func() {})))
			switch role {
			case types.RoleFollower:
				n.BecomeFollower(1, 2)
			case types.RoleCandidate:
				n.BecomeCandidate()
			case types.RoleLearner:
				n.BecomeLearner(1, 2)
			case types.RoleLeader:
				n.BecomeLeader(1)
			}
			err := n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Index: 1, Term: 1, Data: []byte("cleanup")}}})
			if role == types.RoleLeader {
				require.NoError(t, err)
				require.Equal(t, uint64(1), n.LastLogIndex())
			} else {
				require.ErrorIs(t, err, types.ErrNotLeader)
				require.Zero(t, n.LastLogIndex(), "rejection must not append a log")
			}
		})
	}
}

func TestLocalProposalAfterImplicitLeaderConfiguration(t *testing.T) {
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithAdvance(func() {})))
	// Cluster-config bootstrap supplies a leader without an explicit Role. This
	// release installs stepLeader but retains RoleUnknown in the copied config.
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{
		Replicas: []uint64{1}, Leader: 1, Term: 1,
	}}))
	require.True(t, n.IsLeader())
	require.NoError(t, n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Index: 1, Term: 1, Data: []byte("init")}}}))
	require.Equal(t, uint64(1), n.LastLogIndex())
}
