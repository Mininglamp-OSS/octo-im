package raft

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestElectionMembershipDiscoveryDoesNotGrantLearnerVotes(t *testing.T) {
	n := newTestNode(2, []uint64{1, 2})
	n.opts.ElectionOn = true
	n.cfg.Version = 1
	n.cfg.Learners = []uint64{3}
	clearEvents(n)
	term := n.LastTerm()
	for _, from := range []uint64{99, 3} {
		require.NoError(t, n.Step(types.Event{Type: types.VoteReq, From: from, Term: term + 10, ConfigVersion: 2}))
		require.Equal(t, term, n.LastTerm())
		require.Zero(t, n.voteFor)
		events := n.Ready()
		require.Empty(t, findEventsOfType(events, types.VoteResp))
		if from == 99 {
			require.Empty(t, events)
		} else {
			require.Len(t, findEventsOfType(events, types.ConfigReq), 1)
		}
	}
	cfg := types.Config{Version: 2, Replicas: []uint64{1, 2, 3}}
	require.NoError(t, n.Step(types.Event{Type: types.ConfigResp, Reason: types.ReasonOnlySync, From: 99, Config: cfg}))
	require.Equal(t, uint64(1), n.cfg.Version)
	cfg.Leader = 3
	require.NoError(t, n.Step(types.Event{Type: types.ConfigResp, Reason: types.ReasonOnlySync, From: 3, Config: cfg}))
	require.Equal(t, uint64(1), n.cfg.Version, "discovery cannot establish a leader")
	cfg.Leader = 0
	require.NoError(t, n.Step(types.Event{Type: types.ConfigResp, Reason: types.ReasonOnlySync, From: 3, Config: cfg}))
	require.Equal(t, uint64(2), n.cfg.Version)
	require.True(t, n.isVoter(3))
	require.Zero(t, n.voteFor)
	require.Equal(t, term, n.LastTerm())
}

func TestElectionMembershipSharesOnlyAppliedConfig(t *testing.T) {
	n := newTestNode(3, []uint64{1, 2, 3})
	n.opts.ElectionOn = true
	n.cfg.Version = 5
	n.queue.appliedIndex = 4
	clearEvents(n)
	req := types.Event{Type: types.ConfigReq, Reason: types.ReasonOnlySync, From: 2, ConfigVersion: 5}
	require.NoError(t, n.Step(req))
	require.Empty(t, n.Ready())
	n.queue.appliedIndex = 5
	require.NoError(t, n.Step(req))
	events := findEventsOfType(n.Ready(), types.ConfigResp)
	require.Len(t, events, 1)
	require.Zero(t, events[0].Term)
	require.Zero(t, events[0].Config.Leader)
	require.Zero(t, events[0].Config.Term)
	require.Equal(t, uint64(5), events[0].Config.Version)
}

func TestElectionMembershipCampaignAbortKeepsNamedLeader(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	n.BecomeCandidate()
	cfg := n.cfg.Clone()
	cfg.Term++
	cfg.Leader = 2
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Equal(t, types.RoleFollower, n.cfg.Role)
	require.Equal(t, uint64(2), n.cfg.Leader)
}
