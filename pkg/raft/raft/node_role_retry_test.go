package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestFailedPromotionWaitsForTickAndRetriesWithoutWrites(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	cfg := types.Config{Replicas: []uint64{1}, Learners: []uint64{2}, Leader: 1, Role: types.RoleLeader, Term: 1, Version: 1}
	require.NoError(t, n.switchConfig(cfg))
	n.queue.lastLogIndex, n.queue.storedIndex = 10, 10
	n.queue.committedIndex, n.queue.appliedIndex = 10, 10
	clearEvents(n)
	request := types.Event{Type: types.SyncReq, From: 2, To: 1, Term: 1, Index: 11, StoredIndex: 11}
	require.NoError(t, n.Step(request))
	require.Len(t, findEventsOfType(collectEvents(n), types.LearnerToFollowerReq), 1)
	for failure := 0; failure < 8; failure++ {
		require.NoError(t, n.Step(types.Event{Type: types.LearnerToFollowerResp, From: 2, Reason: types.ReasonError}))
		require.Empty(t, findEventsOfType(collectEvents(n), types.NotifySync), "failure must not immediately restart the request cycle")
		for i := 0; i < 100; i++ {
			require.NoError(t, n.Step(request))
			require.Empty(t, findEventsOfType(collectEvents(n), types.LearnerToFollowerReq))
		}
		woke := false
		for tick := 0; tick < 128; tick++ {
			n.Tick()
			if len(findEventsOfType(collectEvents(n), types.NotifySync)) != 0 {
				woke = true
				break
			}
		}
		require.True(t, woke, "retry must not depend on another business write")
		require.NoError(t, n.Step(request))
		require.Len(t, findEventsOfType(collectEvents(n), types.LearnerToFollowerReq), 1)
	}
}

func TestPromotionCooldownSurvivesConfigurationRefresh(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	cfg := types.Config{Replicas: []uint64{1}, Learners: []uint64{2}, Leader: 1, Role: types.RoleLeader, Term: 1, Version: 1}
	require.NoError(t, n.switchConfig(cfg))
	for i := 0; i < 3; i++ {
		require.NoError(t, n.Step(types.Event{Type: types.LearnerToFollowerResp, From: 2, Reason: types.ReasonError}))
	}
	clearEvents(n)
	cfg.Version++
	require.NoError(t, n.switchConfig(cfg))
	clearEvents(n)
	for i := 0; i < 3; i++ {
		n.Tick()
		require.Empty(t, findEventsOfType(collectEvents(n), types.NotifySync))
	}
	n.Tick()
	require.Len(t, findEventsOfType(collectEvents(n), types.NotifySync), 1)
}
