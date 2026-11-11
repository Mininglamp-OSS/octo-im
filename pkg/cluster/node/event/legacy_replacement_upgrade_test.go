package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestLegacyReplacementSnapshotStillRepairsSlotCapacity(t *testing.T) {
	cfg := joiningTestConfig(true)
	for _, node := range cfg.Nodes {
		node.Status = types.NodeStatus_NodeStatusJoined
	}
	// Version-2 promotion retained C's address/identity, but removed its vote.
	cfg.Nodes[1].Online, cfg.Nodes[1].AllowVote, cfg.Nodes[1].OfflineCount = false, false, 1
	cfg.Nodes = append(cfg.Nodes, &types.Node{Id: 1004, Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusJoined})
	s := newJoiningTestServer(t, cfg) // load the real JSON snapshot and start config Raft
	require.NoError(t, s.handleNodeJoining())
	for _, slot := range s.cfgServer.Slots() {
		require.Equal(t, uint64(1002), slot.MigrateFrom)
		require.Equal(t, uint64(1004), slot.MigrateTo)
		require.Equal(t, []uint64{1004}, slot.Learners)
		require.ElementsMatch(t, []uint64{1001, 1002, 1003}, slot.Replicas, "old data voters remain until catch-up")
		require.Greater(t, slot.Generation, cfg.Version, "repair must use the versioned conditional assignment path")
	}
	require.NoError(t, s.cfgServer.ReconcileExpansionMembership())
	require.Len(t, s.cfgServer.Nodes(), 4, "do not reinterpret retained historical metadata as an unconditional deletion")
	require.False(t, s.cfgServer.Node(1002).AllowVote)
	require.NoError(t, s.handleNodeJoining())
	for _, slot := range s.cfgServer.Slots() {
		require.Equal(t, []uint64{1004}, slot.Learners, "the next scan must not duplicate or replace the persisted task")
	}
}
