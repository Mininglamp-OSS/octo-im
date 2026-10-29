package clusterconfig_test

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

// ProposeClusterAddr 必须走真实的 propose→apply→复制路径：leader 回填本节点的
// cluster 通讯地址后，follower 存储记录里对应节点的 ClusterAddr 必须最终收敛为该地址。
// 这证明 CMDTypeConfigClusterAddrChange 的 apply handler 生效，且值会被复制到从节点，
// 而不是只改了 leader 的内存。
//
// 采用与 membership 集成测试相同的“单投票者引导 + 学习者晋升”拓扑：s1 是稳定 leader，
// 避免两个投票者同时竞选造成的选举抖动。
func TestProposeClusterAddr(t *testing.T) {
	transport := newTestTransport()
	s1 := clusterconfig.New(newTestOptions(t, 1, map[uint64]string{1: ""}, clusterconfig.WithTransport(transport)))
	s2 := clusterconfig.New(newTestOptions(t, 2, nil, clusterconfig.WithSeed("1@127.0.0.1:11110"), clusterconfig.WithTransport(transport)))
	transport.serverMap[1] = s1
	transport.serverMap[2] = s2
	require.NoError(t, s1.Start())
	defer s1.Stop()
	require.NoError(t, s2.Start())
	defer s2.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.True(t, waitHasLeader(ctx, s1))

	// 引导节点 1 记录（clusterAddr 初始为空，模拟 standalone 首启），并把节点 2 作为
	// 投票者加入，形成需要两节点共同确认的复制组。
	require.NoError(t, s1.ProposeConfig(&pb.Config{Nodes: []*pb.Node{
		{Id: 1, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusJoined},
	}}))
	require.NoError(t, s1.ProposeJoin(&pb.Node{Id: 2, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusWillJoin}))
	require.Eventually(t, func() bool {
		node := s2.Node(2)
		return node != nil && node.Status == pb.NodeStatus_NodeStatusJoining
	}, 10*time.Second, 20*time.Millisecond)
	require.True(t, s1.IsLeader(), "s1 应保持稳定 leader")

	// 回填前：从节点应看到节点 1 记录且 clusterAddr 为空。
	require.Eventually(t, func() bool {
		return s2.Node(1) != nil
	}, 5*time.Second, 20*time.Millisecond)
	require.Empty(t, s2.Node(1).ClusterAddr, "回填前 clusterAddr 应为空")

	// 回填 clusterAddr，断言复制到从节点后最终收敛。
	const addr = "127.0.0.1:12345"
	require.NoError(t, s1.ProposeClusterAddr(1, addr))
	require.Eventually(t, func() bool {
		node := s2.Node(1)
		return node != nil && node.ClusterAddr == addr
	}, 5*time.Second, 20*time.Millisecond, "从节点的 ClusterAddr 应收敛为提案值")
}
