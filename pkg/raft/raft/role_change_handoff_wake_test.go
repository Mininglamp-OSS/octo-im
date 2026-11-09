package raft

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestRoleChangeWakePreservesLeadershipTransferFence(t *testing.T) {
	for _, reason := range []types.Reason{types.ReasonOk, types.ReasonError} {
		for _, cleared := range []bool{false, true} {
			t.Run(fmt.Sprintf("reason%d/cleared%t", reason, cleared), func(t *testing.T) {
				n := transferTestLeader(t, true)
				cfg := n.Config().Clone()
				cfg.Learners = append(cfg.Learners, 4)
				require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
				n.Ready()
				request := beginTestTransfer(t, n)
				if cleared {
					n.replicaSync = make(map[uint64]*SyncInfo)
				}
				n.replicaSync[4] = &SyncInfo{suspend: true, emptySyncTick: 20}
				require.NoError(t, n.Step(types.Event{Type: types.LearnerToLeaderResp,
					From: request.From, Term: request.Term, ConfigVersion: request.ConfigVersion, RoleChangeID: request.RoleChangeID, Reason: reason}))
				var wokeRemaining bool
				for _, event := range n.Ready() {
					wokeRemaining = wokeRemaining || event.Type == types.NotifySync && event.To == 4
				}
				require.True(t, wokeRemaining, "wake remaining learners even after replicaSync was cleared")
				require.False(t, n.replicaSync[4].suspend)
				require.Zero(t, n.replicaSync[4].emptySyncTick)
				require.True(t, n.stopPropose, "waking sync must not reopen a pending or uncertain transfer")
				require.ErrorIs(t, n.AdmitMessage(99), types.ErrProposalDropped)
				require.ErrorIs(t, n.Step(n.NewPropose([]byte("late write"))), types.ErrProposalDropped)

				// Another role callback must not open the drain after a failed
				// SaveConfig callback cleared the pending transfer identifier.
				require.NoError(t, n.Step(types.Event{Type: types.LearnerToFollowerResp, From: 4, Reason: types.ReasonOk}))
				require.True(t, n.stopPropose)
				require.ErrorIs(t, n.AdmitMessage(100), types.ErrProposalDropped)
			})
		}
	}
}

func TestStaleRoleChangeCannotReopenTransferWithRemainingLearners(t *testing.T) {
	n := transferTestLeader(t, true)
	cfg := n.Config().Clone()
	cfg.Learners = append(cfg.Learners, 4)
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	n.Ready()
	request := beginTestTransfer(t, n)
	require.NoError(t, n.Step(types.Event{Type: types.LearnerToLeaderResp,
		From: request.From, Term: request.Term, ConfigVersion: request.ConfigVersion - 1, RoleChangeID: request.RoleChangeID, Reason: types.ReasonError}))
	require.NotNil(t, n.handoff)
	require.Equal(t, request.From, n.handoff.to)
	require.Equal(t, request.ConfigVersion, n.handoff.version)
	require.True(t, n.stopPropose)
	require.ErrorIs(t, n.AdmitMessage(99), types.ErrProposalDropped)
}
