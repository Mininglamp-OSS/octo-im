package raft

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestRoleChangeResponseNotifiesCurrentLearners(t *testing.T) {
	for _, eventType := range []types.EventType{types.LearnerToFollowerResp, types.LearnerToLeaderResp, types.FollowerToLeaderResp} {
		for _, reason := range []types.Reason{types.ReasonOk, types.ReasonError} {
			for _, cleared := range []bool{false, true} {
				t.Run(fmt.Sprintf("event%d/reason%d/cleared%t", eventType, reason, cleared), func(t *testing.T) {
					n := newTestNode(1, []uint64{1, 2})
					makeLeader(n, 1)
					n.cfg.Learners = []uint64{3, 4}
					n.stopPropose = true
					if !cleared {
						n.replicaSync[2] = &SyncInfo{roleSwitching: true}
						n.replicaSync[3] = &SyncInfo{suspend: true, emptySyncTick: 20}
					}
					require.NoError(t, n.Step(types.Event{Type: eventType, From: 2, Reason: reason,
						Config: types.Config{Learners: []uint64{2, 99}}}))
					notifications := findEventsOfType(collectEvents(n), types.NotifySync)
					var recipients []uint64
					for _, e := range notifications {
						recipients = append(recipients, e.To)
					}
					require.ElementsMatch(t, []uint64{3, 4}, recipients, "notify current membership, not stale response membership")
					require.False(t, n.stopPropose)
					if !cleared {
						require.False(t, n.replicaSync[2].roleSwitching)
						require.False(t, n.replicaSync[3].suspend)
						require.Zero(t, n.replicaSync[3].emptySyncTick)
					}
				})
			}
		}
	}
}

func TestRoleChangeResponseSkipsSelfAndEmptyLearners(t *testing.T) {
	for _, learners := range [][]uint64{nil, {1}, {1, 3}} {
		n := newTestNode(1, []uint64{1, 2})
		makeLeader(n, 1)
		n.cfg.Learners = learners
		require.NoError(t, n.Step(types.Event{Type: types.LearnerToFollowerResp, From: 2}))
		notifications := findEventsOfType(collectEvents(n), types.NotifySync)
		if len(learners) < 2 {
			require.Empty(t, notifications)
		} else {
			require.Len(t, notifications, 1)
			require.Equal(t, uint64(3), notifications[0].To)
		}
	}
}

func TestNotifySyncAllStillExcludesLearners(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2})
	makeLeader(n, 1)
	n.cfg.Learners = []uint64{3, 4}
	n.sendNotifySync(All)
	notifications := findEventsOfType(collectEvents(n), types.NotifySync)
	require.Len(t, notifications, 1)
	require.Equal(t, uint64(2), notifications[0].To)
}

func TestRoleChangeResponseResumesSecondLearnerPromotion(t *testing.T) {
	leader := newTestNode(1, []uint64{1})
	initial := types.Config{Replicas: []uint64{1}, Learners: []uint64{2, 3},
		Leader: 1, Role: types.RoleLeader, Term: 1, Version: 1, MigrateFrom: 2, MigrateTo: 2}
	require.NoError(t, leader.switchConfig(initial))
	leader.opts.AutoSuspend = true
	leader.opts.SuspendAfterEmptySyncTick = 0
	leader.queue.lastLogIndex, leader.queue.storedIndex = 10, 10
	leader.queue.committedIndex, leader.queue.appliedIndex = 10, 10

	learner := newTestNode(3, []uint64{1})
	local := initial.Clone()
	local.Role = types.RoleLearner
	require.NoError(t, learner.switchConfig(local))
	learner.queue.lastLogIndex, learner.queue.storedIndex = 10, 10
	learner.queue.committedIndex, learner.queue.appliedIndex = 10, 10
	learner.onlySync = true
	clearEvents(leader)
	clearEvents(learner)
	learner.sendSyncReq()
	request, ok := findEvent(collectEvents(learner), types.SyncReq)
	require.True(t, ok)
	require.NoError(t, leader.Step(request))
	response, ok := findEvent(collectEvents(leader), types.SyncResp)
	require.True(t, ok)
	require.Equal(t, types.SpeedSuspend, response.Speed)
	require.NoError(t, learner.Step(response))
	require.True(t, learner.suspend)

	// The first promotion is applied before its asynchronous response. The
	// remaining peer keeps its learner role and can still be suspended.
	r := newTestRaftWithNode(leader)
	updated, err := r.learnTo(2)
	require.NoError(t, err)
	updated.Version = 2
	require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: updated}))
	require.Empty(t, leader.replicaSync)
	local = updated.Clone()
	local.Role = types.RoleLearner
	require.NoError(t, learner.Step(types.Event{Type: types.ConfChange, Config: local}))
	require.True(t, learner.suspend)

	require.NoError(t, leader.Step(types.Event{Type: types.LearnerToFollowerResp, From: 2, Reason: types.ReasonOk}))
	notifications := findEventsOfType(collectEvents(leader), types.NotifySync)
	require.Len(t, notifications, 1)
	require.Equal(t, uint64(3), notifications[0].To)
	require.NoError(t, learner.Step(notifications[0]))
	require.False(t, learner.suspend)
	request, ok = findEvent(collectEvents(learner), types.SyncReq)
	require.True(t, ok)
	require.NoError(t, leader.Step(request))
	promotion, ok := findEvent(collectEvents(leader), types.LearnerToFollowerReq)
	require.True(t, ok)
	require.Equal(t, uint64(3), promotion.From)
	finished, err := r.learnTo(promotion.From)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{1, 2, 3}, finished.Replicas)
	require.Empty(t, finished.Learners)
	require.Equal(t, uint64(1), finished.Leader)
}
