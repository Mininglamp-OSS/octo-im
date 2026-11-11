package clusterconfig_test

import (
	"context"
	"encoding/binary"
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
	require.Equal(t, uint16(1), binary.BigEndian.Uint16(logs[0].Data), "new joins use explicit removal, not historical implicit replacement")
	var cmd clusterconfig.CMD
	require.NoError(t, cmd.Unmarshal(logs[0].Data))
	require.Equal(t, clusterconfig.CMDTypeNodeJoin, cmd.CmdType)
	var joined pb.Node
	require.NoError(t, joined.Unmarshal(cmd.Data))
	require.Equal(t, node.Id, joined.Id)
}

func TestExistingVoterRejoinPreservesConcurrentAdmission(t *testing.T) {
	transport := newTestTransport()
	s := clusterconfig.New(newTestOptions(t, 1, map[uint64]string{1: ""}, clusterconfig.WithTransport(transport)))
	transport.serverMap[1] = s
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.True(t, waitHasLeader(ctx, s))
	require.NoError(t, s.ProposeConfig(&pb.Config{
		Nodes: []*pb.Node{
			{Id: 1, ClusterAddr: "127.0.0.1:1001", AllowVote: true, Status: pb.NodeStatus_NodeStatusJoined, CreatedAt: 42, OfflineCount: 3},
			{Id: 2, Online: true, Status: pb.NodeStatus_NodeStatusJoining},
			{Id: 3, Online: true, AllowVote: true, Status: pb.NodeStatus_NodeStatusWillJoin},
		},
		Learners: []uint64{3}, MigrateFrom: 3, MigrateTo: 3,
	}))
	version, err := s.ProposeJoinWithVersion(&pb.Node{Id: 1, ClusterAddr: "127.0.0.1:2001", Online: true, AllowVote: true, Status: pb.NodeStatus_NodeStatusWillJoin})
	require.NoError(t, err)
	got := s.Snapshot()
	require.Equal(t, []uint64{3}, got.Learners, "an existing voter must not become a learner again")
	require.Equal(t, uint64(3), got.MigrateFrom)
	require.Equal(t, uint64(3), got.MigrateTo)
	require.Equal(t, pb.NodeStatus_NodeStatusJoining, s.Node(1).Status)
	require.True(t, s.Node(1).Online)
	require.Equal(t, "127.0.0.1:2001", s.Node(1).ClusterAddr)
	require.Equal(t, int64(42), s.Node(1).CreatedAt)
	require.Equal(t, uint32(3), s.Node(1).OfflineCount)
	logs, err := s.GetLogsByLimit(version, version+1, 1)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint16(1), binary.BigEndian.Uint16(logs[0].Data), "an existing voter recovery must not implicitly retire another voter")
	var cmd clusterconfig.CMD
	require.NoError(t, cmd.Unmarshal(logs[0].Data))
	require.Equal(t, clusterconfig.CMDTypeNodeJoining, cmd.CmdType, "reuse the existing committed recovery boundary")
	_, err = s.ProposeJoinWithVersion(&pb.Node{Id: 4, ClusterAddr: "127.0.0.1:1004", Online: true, AllowVote: true, Status: pb.NodeStatus_NodeStatusWillJoin})
	require.ErrorIs(t, err, clusterconfig.ErrMembershipBusy, "new voters must still serialize")
}
