package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestOfflineJoiningCompletionPreservesAnotherPendingAdmission(t *testing.T) {
	cfg := joiningTestConfig(true)
	for _, node := range cfg.Nodes {
		node.Status = types.NodeStatus_NodeStatusJoined
	}
	cfg.Nodes[1].Status = types.NodeStatus_NodeStatusJoining
	cfg.Nodes[1].Online = false
	cfg.Nodes = append(cfg.Nodes, &types.Node{Id: 1004, Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusWillJoin})
	cfg.Learners, cfg.MigrateFrom, cfg.MigrateTo = []uint64{1004}, 1004, 1004
	s := newJoiningTestServer(t, cfg)
	require.NoError(t, s.handleNodeJoining())
	require.Equal(t, types.NodeStatus_NodeStatusJoined, s.cfgServer.Node(1002).Status)
	require.Equal(t, []uint64{1004}, s.cfgServer.Snapshot().Learners, "completing the offline voter must preserve the other admission")
	require.Equal(t, uint64(1004), s.cfgServer.Snapshot().MigrateTo)
	require.Equal(t, types.NodeStatus_NodeStatusWillJoin, s.cfgServer.Node(1004).Status)

	// The pending learner's subsequent failure is cleaned by its own durable
	// path. It must not become permanently attached to the completed voter.
	require.NoError(t, s.cfgServer.ProposeNodeOnlineStatus(1004, false))
	require.NoError(t, s.cfgServer.ReconcileExpansionMembership())
	require.Nil(t, s.cfgServer.Node(1004))
	require.Empty(t, s.cfgServer.Snapshot().Learners)
	require.Zero(t, s.cfgServer.Snapshot().MigrateFrom)
	require.Zero(t, s.cfgServer.Snapshot().MigrateTo)
	version, err := s.cfgServer.ProposeJoinWithVersion(&types.Node{Id: 1005, ClusterAddr: "127.0.0.1:21005",
		Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusWillJoin})
	require.NoError(t, err, "a later external node must still be able to join")
	require.NotZero(t, version)
	require.Equal(t, []uint64{1005}, s.cfgServer.Snapshot().Learners)
}
