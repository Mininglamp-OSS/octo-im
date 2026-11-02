package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func expansionTestConfig() *types.Config {
	cfg := &types.Config{SlotCount: 64, SlotReplicaCount: 3}
	for _, id := range []uint64{1, 2, 3, 4} {
		cfg.Nodes = append(cfg.Nodes, &types.Node{Id: id, Online: id != 3, AllowVote: true, Status: types.NodeStatus_NodeStatusJoined})
	}
	for id := uint32(0); id < 64; id++ {
		cfg.Slots = append(cfg.Slots, &types.Slot{Id: id, Leader: 1, Term: 2, Replicas: []uint64{1, 2, 3}})
	}
	return cfg
}

func TestSlotExpansionRepairsEveryOfflineReplicaBeforeBalancing(t *testing.T) {
	cfg := expansionTestConfig()
	before := cfg.Clone()
	planned := planSlotExpansion(cfg)
	require.Len(t, planned, 64)
	for _, slot := range planned {
		require.Equal(t, []uint64{1, 2, 3}, slot.Replicas, "the old voting set remains until catch-up")
		require.Equal(t, []uint64{4}, slot.Learners)
		require.Equal(t, uint64(3), slot.MigrateFrom)
		require.Equal(t, uint64(4), slot.MigrateTo)
		require.Equal(t, uint64(1), slot.Leader)
	}
	require.True(t, proto.Equal(before, cfg))
	cfg.Nodes = cfg.Nodes[:3] // losing C without external capacity never deletes its data replicas
	require.Empty(t, planSlotExpansion(cfg))
}

func TestSlotExpansionResumesAndCancelsOnlyDeadLearner(t *testing.T) {
	cfg := expansionTestConfig()
	cfg.Slots = cfg.Slots[:1]
	cfg.Slots[0].Learners = []uint64{4}
	cfg.Slots[0].MigrateFrom, cfg.Slots[0].MigrateTo = 3, 4
	require.Empty(t, planSlotExpansion(cfg), "a repeated scan must not duplicate a live learner")
	cfg.Nodes[2].Online = true // C comes back; do not overwrite the committed in-flight task
	require.Empty(t, planSlotExpansion(cfg))
	cfg.Nodes[3].Online = false
	planned := planSlotExpansion(cfg)
	require.Len(t, planned, 1)
	require.Empty(t, planned[0].Learners)
	require.Zero(t, planned[0].MigrateTo)
	require.Equal(t, []uint64{1, 2, 3}, planned[0].Replicas)
	cfg.Slots[0] = planned[0]
	require.Empty(t, planSlotExpansion(cfg), "C restored capacity, so D is no longer needed")
}

func TestSlotExpansionKeepsOriginalQuorumAndFailsOverLeader(t *testing.T) {
	cfg := expansionTestConfig()
	cfg.Nodes[1].Online = false
	require.Empty(t, planSlotExpansion(cfg), "one old voter and an empty node cannot redefine quorum")
	cfg.Nodes[1].Online = true
	cfg.Slots[0].Leader = 3
	planned := planSlotExpansion(cfg)
	require.Equal(t, types.SlotStatus_SlotStatusCandidate, planned[0].Status)
	require.Contains(t, []uint64{1, 2}, planned[0].ExpectLeader)
	require.Empty(t, planned[0].Learners, "elect a surviving leader before adding a learner")
}

func TestSlotExpansionPlansEachSlotsActualCapacity(t *testing.T) {
	cfg := expansionTestConfig()
	cfg.Slots[0].Replicas = []uint64{1, 2, 4}
	cfg.Slots[1].Replicas = []uint64{1, 2}
	planned := planSlotExpansion(cfg)
	require.Len(t, planned, 63)
	require.Equal(t, uint32(1), planned[0].Id)
	require.Equal(t, uint64(4), planned[0].MigrateFrom)
	require.Equal(t, planned[0].MigrateFrom, planned[0].MigrateTo)
}

func TestHealthySlotExpansionBalancesThreeToFour(t *testing.T) {
	cfg := expansionTestConfig()
	cfg.Nodes[2].Online = true
	for i, slot := range cfg.Slots {
		slot.Leader = uint64(i%3 + 1)
	}
	require.Empty(t, planSlotExpansion(cfg))
	planned := balanceJoiningSlots(cfg, 4)
	require.Len(t, planned, 48)
	counts := map[uint64]int{1: 64, 2: 64, 3: 64, 4: 0}
	seen := map[uint32]bool{}
	for _, slot := range planned {
		require.False(t, seen[slot.Id])
		seen[slot.Id] = true
		require.Equal(t, []uint64{4}, slot.Learners)
		counts[slot.MigrateFrom]--
		counts[slot.MigrateTo]++
	}
	for _, count := range counts {
		require.Equal(t, 48, count)
	}
}

func TestSlotExpansionCancelsFailedPromotedDestination(t *testing.T) {
	cfg := expansionTestConfig()
	cfg.Nodes[2].Online = true
	cfg.Nodes[3].Online = false
	cfg.Slots = cfg.Slots[:1]
	cfg.Slots[0].Replicas = []uint64{1, 2, 3, 4}
	cfg.Slots[0].MigrateFrom, cfg.Slots[0].MigrateTo = 2, 4
	planned := planSlotExpansion(cfg)
	require.Len(t, planned, 1)
	require.Equal(t, []uint64{1, 2, 3}, planned[0].Replicas)
	require.Zero(t, planned[0].MigrateFrom)
	require.Zero(t, planned[0].MigrateTo)
	cfg.Nodes[2].Online = false
	require.Empty(t, planSlotExpansion(cfg), "the old four-member majority is still required")
}
