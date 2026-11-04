package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAddrTestServer 构造一个仅够跑 addOrUpdateNodes 的最小 Server：
// 不启动网络/存储，只需 opts、nodeManager 和日志。
func newAddrTestServer(t *testing.T, selfNodeId uint64) *Server {
	opts := NewOptions(WithConfigOptions(clusterconfig.NewOptions(clusterconfig.WithNodeId(selfNodeId))))
	s := &Server{
		opts: opts,
		Log:  wklog.NewWKLog("addr-test"),
	}
	s.nodeManager = newNodeManager(opts)
	return s
}

// 空地址不可拨号：standalone 节点未配置 cluster.serverAddr 时，其配置记录里的
// cluster_addr 为空。addOrUpdateNodes 收到空地址必须跳过，保留 seed join 已经
// 建立的可用连接，绝不能用空地址替换/停掉现有节点。
func TestAddOrUpdateNodes_SkipEmptyAddr(t *testing.T) {
	s := newAddrTestServer(t, 1)

	// seed 一个已连上的邻居节点（NewImprovedNode 不会启动 goroutine/socket）
	existing := NewImprovedNode(2, s.serverUid(1), "127.0.0.1:12002", s.opts)
	s.nodeManager.addNode(existing)

	// 收到空地址：必须原样保留现有节点
	s.addOrUpdateNodes(map[uint64]string{2: ""})

	got := s.nodeManager.node(2)
	require.NotNil(t, got, "空地址不应移除现有节点")
	assert.Same(t, existing, got, "空地址不应用新节点替换现有连接")
	assert.Equal(t, "127.0.0.1:12002", got.addr, "地址不应被清空")
}
