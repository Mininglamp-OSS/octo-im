package cluster

import (
	"testing"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestClusterJoinResponseRecoveryBoundaryCodec(t *testing.T) {
	original := ClusterJoinResp{Nodes: []*NodeInfo{{NodeId: 1, ServerAddr: "127.0.0.1:1001"}}, JoinVersion: 42}
	data, err := original.Marshal()
	require.NoError(t, err)
	var decoded ClusterJoinResp
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, original, decoded)
	legacy := data[:len(data)-8]
	require.NoError(t, decoded.Unmarshal(legacy))
	require.Zero(t, decoded.JoinVersion, "legacy responses never reuse a prior recovery proof")
	require.Equal(t, original.Nodes, decoded.Nodes)
	for n := 1; n < 8; n++ {
		require.Error(t, decoded.Unmarshal(data[:len(legacy)+n]), "truncated boundaries must fail closed")
	}
}

func TestClusterJoinResponseCarriesPassiveRecoveryMembership(t *testing.T) {
	original := ClusterJoinResp{JoinVersion: 10, LeaderID: 1, Config: &nodetypes.Config{Version: 12, Learners: []uint64{3}, Nodes: []*nodetypes.Node{{Id: 1, AllowVote: true}, {Id: 2, AllowVote: true}, {Id: 3, AllowVote: true}}}}
	data, err := original.Marshal()
	require.NoError(t, err)
	var decoded ClusterJoinResp
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, original.JoinVersion, decoded.JoinVersion)
	require.Equal(t, original.LeaderID, decoded.LeaderID)
	require.True(t, proto.Equal(original.Config, decoded.Config))
	legacy, err := (&ClusterJoinResp{JoinVersion: 10}).Marshal()
	require.NoError(t, err)
	require.NoError(t, decoded.Unmarshal(legacy))
	require.Nil(t, decoded.Config)
	require.Zero(t, decoded.LeaderID)
	for n := len(legacy) + 1; n < len(data); n++ {
		require.Error(t, decoded.Unmarshal(data[:n]))
	}
}

func TestMembershipProbeDoesNotRewriteExistingMember(t *testing.T) {
	servers, _ := newConfigRPCServersWithSlots(t, 3, 1)
	before := servers[0].cfgServer.Snapshot()
	// Health probes must stay read-only for an existing ID, even if a caller
	// has no advertised address or stale role information during startup.
	resp, err := servers[1].rpcClient.RequestClusterJoin(1, &ClusterJoinReq{NodeId: 2, Probe: true, Role: nodetypes.NodeRole_NodeRoleProxy})
	require.NoError(t, err)
	require.Zero(t, resp.JoinVersion)
	after := servers[0].cfgServer.Snapshot()
	require.Len(t, after.Nodes, len(before.Nodes))
	require.Equal(t, before.Learners, after.Learners)
	var old, current *nodetypes.Node
	for _, n := range before.Nodes {
		if n.Id == 2 {
			old = n
		}
	}
	for _, n := range after.Nodes {
		if n.Id == 2 {
			current = n
		}
	}
	require.True(t, proto.Equal(old, current), "a probe must never demote or reset an existing member")
}
