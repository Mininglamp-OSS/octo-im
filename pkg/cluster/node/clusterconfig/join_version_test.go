package clusterconfig_test

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestJoinVersionIdentifiesExactAppliedCommand(t *testing.T) {
	transport := newTestTransport()
	s := clusterconfig.New(newTestOptions(t, 1, map[uint64]string{1: ""}, clusterconfig.WithTransport(transport)))
	transport.serverMap[1] = s
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.True(t, waitHasLeader(ctx, s))
	require.NoError(t, s.ProposeConfig(&pb.Config{Nodes: []*pb.Node{{Id: 1, AllowVote: true, Status: pb.NodeStatus_NodeStatusJoined}}}))
	node := &pb.Node{Id: 2, ClusterAddr: "127.0.0.1:1002", Role: pb.NodeRole_NodeRoleProxy, Status: pb.NodeStatus_NodeStatusWillJoin}
	version, err := s.ProposeJoinWithVersion(node)
	require.NoError(t, err)
	require.NotZero(t, version)
	require.NoError(t, s.ProposeApiServerAddr(1, "http://after-join.example"))
	require.Greater(t, s.GetClusterConfig().Version, version, "later configuration updates must not move this boot's join boundary")
	logs, err := s.GetLogsByLimit(version, version+1, 1)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	var cmd clusterconfig.CMD
	require.NoError(t, cmd.Unmarshal(logs[0].Data))
	require.Equal(t, clusterconfig.CMDTypeNodeJoin, cmd.CmdType)
	var joined pb.Node
	require.NoError(t, joined.Unmarshal(cmd.Data))
	require.Equal(t, node.Id, joined.Id)
}
