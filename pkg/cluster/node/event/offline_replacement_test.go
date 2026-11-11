package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestJoinReplacesAllOfflineSlotReplicasBeforeBalancing(t *testing.T) {
	cfg := joiningTestConfig(true)
	for _, n := range cfg.Nodes {
		n.Status = types.NodeStatus_NodeStatusJoined
	}
	cfg.Nodes[1].Online = false
	cfg.Nodes[1].OfflineCount = 1
	// Configuration catch-up retires the failed configuration voter before
	// the joining event schedules replacement of its slot replicas.
	cfg.Nodes[1].AllowVote = false
	cfg.Nodes = append(cfg.Nodes, &types.Node{Id: 1004, AllowVote: true, Online: true, Status: types.NodeStatus_NodeStatusJoining})
	s := newJoiningTestServer(t, cfg)
	before := s.cfgServer.GetClusterConfig().Clone()
	require.NoError(t, s.handleNodeJoining())
	for _, st := range s.cfgServer.Slots() {
		require.Equal(t, uint64(1002), st.MigrateFrom, "slot %d must replace the failed replica, not a healthy voter", st.Id)
		require.Equal(t, uint64(1004), st.MigrateTo)
		require.Equal(t, []uint64{1004}, st.Learners)
		require.ElementsMatch(t, []uint64{1001, 1002, 1003}, st.Replicas, "promotion must wait for durable catchup")
		require.Equal(t, before.Slots[st.Id].Leader, st.Leader, "replacement must not transfer a healthy leader")
	}
}

func TestOfflineSlotReplacementSafetyAndRetry(t *testing.T) {
	base := joiningTestConfig(true)
	for _, node := range base.Nodes {
		node.Status = types.NodeStatus_NodeStatusJoined
	}
	base.Nodes[1].Online, base.Nodes[1].OfflineCount = false, 1
	base.Nodes = append(base.Nodes, &types.Node{Id: 1004, Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusJoined})
	base.Slots = base.Slots[:1]
	for _, tc := range []struct {
		name   string
		change func(*types.Config)
	}{
		{"healthy", func(c *types.Config) { c.Nodes[1].Online = true }},
		{"unconfirmed failure", func(c *types.Config) { c.Nodes[1].OfflineCount = 0 }},
		{"no quorum", func(c *types.Config) { c.Nodes[2].Online = false }},
		{"offline leader", func(c *types.Config) { c.Slots[0].Leader = 1002 }},
		{"candidate", func(c *types.Config) { c.Slots[0].Status = types.SlotStatus_SlotStatusCandidate }},
		{"learner in flight", func(c *types.Config) { c.Slots[0].Learners = []uint64{1005} }},
		{"migration in flight", func(c *types.Config) { c.Slots[0].MigrateFrom, c.Slots[0].MigrateTo = 1001, 1003 }},
		{"offline spare", func(c *types.Config) { c.Nodes[3].Online = false }},
		{"nonvoting spare", func(c *types.Config) { c.Nodes[3].AllowVote = false }},
		{"unjoined spare", func(c *types.Config) { c.Nodes[3].Status = types.NodeStatus_NodeStatusWillJoin }},
		{"existing replica", func(c *types.Config) { c.Nodes[3].Id = 1003 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base.Clone()
			tc.change(cfg)
			before := cfg.Clone()
			require.Empty(t, planOfflineSlotReplacements(cfg, 0))
			require.Equal(t, before, cfg, "planning must not mutate a published snapshot")
		})
	}
	busy := base.Clone()
	busy.Slots[0].MigrateFrom, busy.Slots[0].MigrateTo = 1001, 1003
	require.Empty(t, planOfflineSlotReplacements(busy, 0))
	busy.Slots[0].MigrateFrom, busy.Slots[0].MigrateTo = 0, 0
	plans := planOfflineSlotReplacements(busy, 0)
	require.Len(t, plans, 1, "a later control-loop pass must repair a slot skipped during join")
	require.Equal(t, uint64(1002), plans[0].MigrateFrom)
	require.Equal(t, uint64(1004), plans[0].MigrateTo)
	require.Empty(t, busy.Slots[0].Learners)
}
