package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestSlotRetirementPlannerAfterElection(t *testing.T) {
	for _, leader := range []uint64{2, 3, 4} {
		for _, lost := range []bool{false, true} {
			cfg := &types.Config{SlotReplicaCount: 3}
			for _, id := range []uint64{1, 2, 3, 4} {
				cfg.Nodes = append(cfg.Nodes, &types.Node{Id: id, Online: id != 1, AllowVote: true, Status: types.NodeStatus_NodeStatusJoined})
			}
			st := &types.Slot{Id: 7, Leader: leader, Term: 3, Replicas: []uint64{1, 2, 3, 4}, MigrateFrom: 3, MigrateTo: 4}
			if lost {
				st.MigrateFrom, st.MigrateTo = 0, 0
			}
			cfg.Slots = []*types.Slot{st}
			planned := planSlotExpansion(cfg)
			require.Len(t, planned, 1)
			next := planned[0]
			require.Equal(t, uint64(1), next.MigrateFrom)
			require.NotEqual(t, leader, next.MigrateTo)
			require.NotEqual(t, next.MigrateFrom, next.MigrateTo)
			require.Len(t, next.Replicas, 4, "only the installed-majority callback may remove the voter")
			cfg.Slots = planned
			require.Empty(t, planSlotExpansion(cfg), "repeated scans must preserve a finishable task")
		}
	}
}
