package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestLeaderHandoffFencesWritesUntilFailedSaveReturns(t *testing.T) {
	for _, learner := range []bool{true, false} {
		t.Run(map[bool]string{true: "learner", false: "follower"}[learner], func(t *testing.T) {
			replicas := []uint64{1, 2, 3}
			requestType, responseType := types.FollowerToLeaderReq, types.FollowerToLeaderResp
			if learner {
				replicas = []uint64{1}
				requestType, responseType = types.LearnerToLeaderReq, types.LearnerToLeaderResp
			}
			n := NewNode(1, types.RaftState{LastLogIndex: 8, LastTerm: 2, AppliedIndex: 8}, NewOptions(WithNodeId(1), WithReplicas(replicas)))
			n.BecomeLeader(2)
			cfg := n.Config()
			cfg.Version = 1
			cfg.MigrateFrom, cfg.MigrateTo = 1, 2
			if learner {
				cfg.Learners = []uint64{2}
			}
			require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
			clearEvents(n)
			// D has the complete durable tail. Hold the asynchronous SaveConfig
			// completion while a new proposal arrives at the current leader.
			require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 2, Term: 2, Index: 9, StoredIndex: 9, Reason: types.ReasonOnlySync, ConfigVersion: 1}))
			_, ok := findEvent(collectEvents(n), requestType)
			require.True(t, ok)
			commitStored := func(index uint64) {
				require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Index: index, Reason: types.ReasonOk}))
				if !learner {
					// A third voter can acknowledge a write while the transfer
					// destination still holds only index 8.
					require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 3, Term: 2, Index: index + 1, StoredIndex: index + 1, Reason: types.ReasonOnlySync, ConfigVersion: 1}))
				}
			}
			proposal := types.Event{Type: types.Propose, Logs: []types.Log{{Index: 9, Term: 2, Data: []byte("during-handoff")}}}
			err := n.Step(proposal)
			events := collectEvents(n)
			if stored, ok := findEvent(events, types.StoreReq); ok {
				commitStored(stored.Logs[len(stored.Logs)-1].Index)
			}
			require.Equal(t, uint64(8), n.CommittedIndex(), "the old leader must not commit beyond the destination's handoff tail")
			require.ErrorIs(t, err, types.ErrProposalDropped)
			require.Zero(t, countEvents(events, types.StoreReq))
			require.Equal(t, uint64(8), n.LastLogIndex())
			// A failed save has not moved leadership; the old leader must become
			// writable again and commit the retried request normally.
			require.NoError(t, n.Step(types.Event{Type: responseType, From: 2, Reason: types.ReasonError}))
			require.Equal(t, uint64(1), n.LeaderId())
			require.NoError(t, n.Step(proposal))
			stored, ok := findEvent(collectEvents(n), types.StoreReq)
			require.True(t, ok)
			require.Equal(t, proposal.Logs, stored.Logs)
			commitStored(9)
			require.Equal(t, uint64(9), n.CommittedIndex())
		})
	}
}
