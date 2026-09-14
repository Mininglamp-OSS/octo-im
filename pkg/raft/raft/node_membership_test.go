package raft

import (
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestElectionMembershipLearnerCannotVoteOrCampaign(t *testing.T) {
	n := newTestNode(4, []uint64{1, 2, 3})
	cfg := types.Config{Term: 3, Version: 1, Leader: 1, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}}
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Equal(t, types.RoleLearner, n.cfg.Role)
	require.NoError(t, n.Step(types.Event{Type: types.VoteReq, From: 1, Term: 4, Logs: []types.Log{{Term: 3}}}))
	require.NoError(t, n.Step(types.Event{Type: types.Campaign}))
	n.BecomeCandidate()
	require.Equal(t, types.RoleLearner, n.cfg.Role)
	require.Equal(t, uint32(3), n.LastTerm())
	require.Empty(t, n.Ready())
	// The same node is permitted to campaign only after the applied config promotes it.
	cfg.Version++
	cfg.Learners = nil
	cfg.Replicas = append(cfg.Replicas, 4)
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Equal(t, types.RoleFollower, n.cfg.Role)
	require.NoError(t, n.Step(types.Event{Type: types.Campaign}))
	require.Equal(t, types.RoleCandidate, n.cfg.Role)
	require.Len(t, findEventsOfType(n.Ready(), types.VoteReq), 4)
}

func TestElectionMembershipRejectsForeignAndLearnerVotes(t *testing.T) {
	for _, id := range []uint64{0, 4, 99} {
		t.Run(fmt.Sprintf("node_%d", id), func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2, 3, 4})
			n.cfg.Learners = []uint64{4} // even an overlapping entry is not a voter
			n.campaign()
			clearEvents(n)
			term := n.LastTerm()
			require.NoError(t, n.Step(types.Event{Type: types.VoteReq, From: id, Term: term + 10, Logs: []types.Log{{Term: term + 10}}}))
			require.NoError(t, n.Step(types.Event{Type: types.VoteResp, From: id, Term: term + 10, Reason: types.ReasonOk}))
			require.Equal(t, term, n.LastTerm())
			require.Equal(t, types.RoleCandidate, n.cfg.Role)
			require.Empty(t, n.Ready())
			require.Empty(t, n.votes)
		})
	}
}

func TestElectionMembershipCountsOnlyDistinctCurrentVotes(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3, 4, 5})
	n.cfg.Version = 7
	n.campaign()
	clearEvents(n)
	vote := func(from uint64, version uint64) {
		require.NoError(t, n.Step(types.Event{Type: types.VoteResp, From: from, Term: n.LastTerm(), ConfigVersion: version, Reason: types.ReasonOk}))
	}
	vote(1, 7)
	vote(2, 6)
	vote(2, 7)
	vote(2, 7)
	vote(99, 7)
	require.Equal(t, types.RoleCandidate, n.cfg.Role)
	require.Len(t, n.votes, 2)
	vote(3, 7)
	require.True(t, n.IsLeader(), "three distinct eligible voters form a majority")
}

func TestElectionMembershipConfigurationEndsOldCampaign(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	n.cfg.Version = 1
	n.campaign()
	clearEvents(n)
	term := n.LastTerm()
	require.NoError(t, n.Step(types.Event{Type: types.VoteResp, From: 1, Term: term, ConfigVersion: 1, Reason: types.ReasonOk}))
	cfg := n.Config().Clone()
	cfg.Version = 2
	cfg.Replicas = []uint64{1, 2, 4}
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Equal(t, types.RoleFollower, n.cfg.Role)
	require.NoError(t, n.Step(types.Event{Type: types.VoteResp, From: 2, Term: term, ConfigVersion: 1, Reason: types.ReasonOk}))
	require.False(t, n.IsLeader())
	n.campaign()
	clearEvents(n)
	require.Greater(t, n.LastTerm(), term)
	// Removal while campaigning also prevents later campaigning, even by a local event.
	cfg.Version++
	cfg.Replicas = []uint64{2, 4}
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Equal(t, types.RoleLearner, n.cfg.Role)
	n.campaign()
	require.False(t, n.IsLeader())
	require.Equal(t, types.RoleLearner, n.cfg.Role)
}

func TestMembershipUpdatePreservesLeaderAndAnnouncesPromotion(t *testing.T) {
	leader := newTestNode(1, []uint64{1})
	leader.opts.ElectionOn = true
	learner := newTestNode(2, []uint64{1})
	cfg := types.Config{Term: 1, Version: 1, Leader: 1, Replicas: []uint64{1}, Learners: []uint64{2}}
	require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.NoError(t, learner.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Equal(t, types.RoleLearner, learner.cfg.Role)
	// The leader applies the promotion first; the learner's application is delayed.
	cfg.Version = 2
	cfg.Replicas = []uint64{1, 2}
	cfg.Learners = nil
	cfg.Leader = 0
	require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.True(t, leader.IsLeader(), "adding a voter must not unnecessarily strand the learner without a leader")
	leader.sendPing(All)
	ping, ok := findEvent(leader.Ready(), types.Ping)
	require.True(t, ok)
	require.Equal(t, uint64(2), ping.ConfigVersion)
	require.NoError(t, learner.Step(ping))
	req, ok := findEvent(learner.Ready(), types.ConfigReq)
	require.True(t, ok)
	require.NoError(t, leader.Step(req))
	resp, ok := findEvent(leader.Ready(), types.ConfigResp)
	require.True(t, ok)
	require.NoError(t, learner.Step(resp))
	require.Equal(t, types.RoleFollower, learner.cfg.Role)
	require.Equal(t, uint64(1), learner.LeaderId())
	require.Equal(t, uint64(2), learner.cfg.Version)
}

func TestMembershipCommitQuorumExcludesNonVoters(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 2)
	n.cfg.Learners = []uint64{4}
	n.queue.lastLogIndex = 10
	n.queue.storedIndex = 10
	n.replicaSync[1] = &SyncInfo{StoredIndex: 11} // self must not be counted twice
	n.replicaSync[4] = &SyncInfo{StoredIndex: 11}
	n.replicaSync[99] = &SyncInfo{StoredIndex: 11}
	require.Zero(t, n.committedIndexForLeader())
	n.replicaSync[2] = &SyncInfo{StoredIndex: 11}
	require.Equal(t, uint64(10), n.committedIndexForLeader())
}

func TestLearnerPromotionWaitsForDurableCommittedPrefix(t *testing.T) {
	for _, transfer := range []bool{false, true} {
		n := newTestNode(1, []uint64{1, 2, 3})
		makeLeader(n, 2)
		n.cfg.Learners = []uint64{4}
		if transfer {
			n.cfg.MigrateFrom = 1
			n.cfg.MigrateTo = 4
		}
		n.queue.lastLogIndex = 10
		n.queue.storedIndex = 10
		n.queue.committedIndex = 10
		n.queue.appliedIndex = 10
		n.replicaSync[4] = &SyncInfo{}
		n.roleSwitchIfNeed(types.Event{From: 4, Index: 11, StoredIndex: 10})
		require.False(t, n.replicaSync[4].roleSwitching)
		require.Empty(t, n.Ready())
		n.roleSwitchIfNeed(types.Event{From: 4, Index: 11, StoredIndex: 11})
		require.True(t, n.replicaSync[4].roleSwitching)
		require.Len(t, n.Ready(), 1)
	}
}

func TestPromotionTargetMustActuallyBeLearner(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 2)
	n.cfg.MigrateFrom = 2
	n.cfg.MigrateTo = 99
	_, err := newTestRaftWithNode(n).learnTo(99)
	require.Error(t, err)
}

func TestSingleVoterPromotionRequiresStoredTail(t *testing.T) {
	for _, orphan := range []bool{false, true} {
		n := newTestNode(1, []uint64{1})
		makeLeader(n, 2)
		n.cfg.Learners = []uint64{4}
		if !orphan {
			n.cfg.MigrateFrom = 4
			n.cfg.MigrateTo = 4
		}
		n.queue.lastLogIndex = 10
		n.replicaSync[4] = &SyncInfo{}
		for _, stored := range []uint64{5, 10} {
			n.roleSwitchIfNeed(types.Event{From: 4, Index: 11, StoredIndex: stored})
			require.False(t, n.replicaSync[4].roleSwitching)
		}
		n.roleSwitchIfNeed(types.Event{From: 4, Index: 11, StoredIndex: 11})
		require.True(t, n.replicaSync[4].roleSwitching)
	}
}

func TestElectionMembershipVersionSkewDoesNotRejectVotes(t *testing.T) {
	for _, version := range []uint64{0, 2, 100} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2, 3})
			n.cfg.Version = 7
			require.NoError(t, n.Step(types.Event{Type: types.VoteReq, From: 2, Term: 3, ConfigVersion: version, Logs: []types.Log{{Term: 1}}}))
			resp, ok := findEvent(n.Ready(), types.VoteResp)
			require.True(t, ok)
			require.Equal(t, types.ReasonOk, resp.Reason)
			// A differing version must not bypass one-vote-per-term.
			require.NoError(t, n.Step(types.Event{Type: types.VoteReq, From: 3, Term: 3, ConfigVersion: version, Logs: []types.Log{{Term: 1}}}))
			resp, ok = findEvent(n.Ready(), types.VoteResp)
			require.True(t, ok)
			require.Equal(t, types.ReasonError, resp.Reason)
		})
	}
}

func TestElectionMembershipVersionUpdatePreservesCampaign(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	n.campaign()
	clearEvents(n)
	term := n.LastTerm()
	require.NoError(t, n.Step(types.Event{Type: types.VoteResp, From: 1, Term: term, Reason: types.ReasonOk}))
	cfg := n.cfg.Clone()
	cfg.Version++
	cfg.Role = types.RoleUnknown
	require.NoError(t, n.switchConfig(cfg))
	require.Equal(t, types.RoleCandidate, n.cfg.Role)
	require.Len(t, n.votes, 1)
	require.NoError(t, n.Step(types.Event{Type: types.VoteResp, From: 2, Term: term, ConfigVersion: 0, Reason: types.ReasonOk}))
	require.True(t, n.IsLeader())
}

func TestElectionMembershipLearnerOverlapEndsCampaign(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	n.campaign()
	cfg := n.cfg.Clone()
	cfg.Learners = []uint64{3} // voter eligibility changed, even at the same version
	require.NoError(t, n.switchConfig(cfg))
	require.Equal(t, types.RoleFollower, n.cfg.Role)
	require.Empty(t, n.votes)
}

func TestMembershipExplicitRemoteLeaderOverridesRole(t *testing.T) {
	for _, role := range []types.Role{types.RoleLeader, types.RoleUnknown} {
		n := newTestNode(1, []uint64{1, 2, 3})
		makeLeader(n, 2)
		cfg := n.cfg.Clone()
		cfg.Leader, cfg.Role = 2, role
		require.NoError(t, n.switchConfig(cfg))
		require.Equal(t, types.RoleFollower, n.cfg.Role)
		require.Equal(t, uint64(2), n.LeaderId())
		require.False(t, n.IsLeader())
	}
}

func TestLearnerPromotionGapRequiresDurableCommittedPrefix(t *testing.T) {
	for _, orphan := range []bool{false, true} {
		n := newTestNode(1, []uint64{1, 2, 3})
		makeLeader(n, 2)
		n.cfg.Learners = []uint64{4}
		if !orphan {
			n.cfg.MigrateFrom, n.cfg.MigrateTo = 2, 4
		}
		n.queue.lastLogIndex, n.queue.storedIndex, n.queue.committedIndex = 100, 100, 90
		n.opts.LearnerToFollowerMinLogGap = 20
		n.replicaSync[4] = &SyncInfo{}
		n.roleSwitchIfNeed(types.Event{From: 4, Index: 95, StoredIndex: 90})
		require.False(t, n.replicaSync[4].roleSwitching)
		n.roleSwitchIfNeed(types.Event{From: 4, Index: 95, StoredIndex: 91})
		require.True(t, n.replicaSync[4].roleSwitching)
	}
}

func TestMembershipBroadcastCommitClampedToDurableLogs(t *testing.T) {
	leader := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(leader, 2)
	leader.cfg.Version = 7
	leader.queue.committedIndex = 10
	leader.sendPing(All)
	for _, ping := range findEventsOfType(leader.Ready(), types.Ping) {
		require.Equal(t, uint64(10), ping.CommittedIndex)
		require.Equal(t, uint64(7), ping.ConfigVersion)
		follower := newTestNode(ping.To, []uint64{1, 2, 3})
		makeFollower(follower, 2, 1)
		follower.queue.lastLogIndex, follower.queue.storedIndex = 10, 8
		require.NoError(t, follower.Step(ping))
		require.Equal(t, uint64(8), follower.queue.committedIndex)
	}
}
