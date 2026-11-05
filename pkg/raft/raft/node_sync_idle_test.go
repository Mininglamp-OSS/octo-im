package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestUnansweredSyncDoesNotKeepIdleReplicaAlive(t *testing.T) {
	for _, role := range []types.Role{types.RoleLearner, types.RoleFollower} {
		t.Run(role.String(), func(t *testing.T) {
			n := newTestNode(2, []uint64{1})
			cfg := types.Config{Replicas: []uint64{1, 2}, Leader: 1, Role: role, Term: 1, Version: 1}
			if role == types.RoleLearner {
				cfg.Replicas, cfg.Learners = []uint64{1}, []uint64{2}
			}
			require.NoError(t, n.switchConfig(cfg))
			n.opts.AutoDestory = true
			n.opts.DestoryAfterIdleTick = 30
			n.opts.SyncIntervalTick, n.opts.SyncRespTimeoutTick = 2, 10
			clearEvents(n)
			requests := 0
			for tick := 0; tick < 100; tick++ {
				n.Tick()
				for _, event := range collectEvents(n) {
					if event.Type == types.SyncReq {
						requests++
					}
					if event.Type == types.Destory {
						require.Positive(t, requests)
						require.LessOrEqual(t, tick, 31)
						return
					}
				}
			}
			t.Fatal("unanswered requests kept the idle replica alive")
		})
	}
}

func TestSyncIdleRetainsResponsiveAndNonRetiringReplicas(t *testing.T) {
	for _, autoDestroy := range []bool{true, false} {
		for _, role := range []types.Role{types.RoleLearner, types.RoleFollower} {
			n := newTestNode(2, []uint64{1})
			cfg := types.Config{Replicas: []uint64{1, 2}, Leader: 1, Role: role, Term: 1, Version: 1}
			if role == types.RoleLearner {
				cfg.Replicas, cfg.Learners = []uint64{1}, []uint64{2}
			}
			require.NoError(t, n.switchConfig(cfg))
			n.opts.AutoDestory, n.opts.DestoryAfterIdleTick = autoDestroy, 30
			clearEvents(n)
			for tick := 0; tick < 1000; tick++ {
				if autoDestroy && tick%10 == 0 {
					require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, To: 2, Term: 1, Reason: types.ReasonOk}))
				}
				n.Tick()
				for _, event := range collectEvents(n) {
					require.NotEqual(t, types.Destory, event.Type)
				}
			}
		}
	}
}
