package clusterconfig

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/assert"
)

// A standalone node bootstraps its config record with an empty ClusterAddr
// (no cluster.serverAddr configured yet). After the operator adds the
// cluster section and restarts, the record must be backfilled via
// CMDTypeConfigClusterAddrChange so join responses carry a dialable address.
func TestClusterAddrChange_RoundTrip(t *testing.T) {
	data, err := EncodeClusterAddrChange(1001, "node1.wk.local:11110")
	assert.NoError(t, err)

	nodeId, addr, err := DecodeClusterAddrChange(data)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1001), nodeId)
	assert.Equal(t, "node1.wk.local:11110", addr)
}

func TestClusterAddrChange_RoundTripEmpty(t *testing.T) {
	data, err := EncodeClusterAddrChange(1002, "")
	assert.NoError(t, err)

	nodeId, addr, err := DecodeClusterAddrChange(data)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1002), nodeId)
	assert.Equal(t, "", addr)
}

func TestUpdateClusterAddr(t *testing.T) {
	c := &Config{
		cfg: &types.Config{
			Nodes: []*types.Node{
				{Id: 1001, ClusterAddr: ""},
				{Id: 2002, ClusterAddr: "node2.wk.local:11110"},
			},
		},
		Log: wklog.NewWKLog("test"),
	}

	// 只回填匹配的节点，其它节点保持不变
	c.updateClusterAddr(1001, "node1.wk.local:11110")
	assert.Equal(t, "node1.wk.local:11110", c.cfg.Nodes[0].ClusterAddr)
	assert.Equal(t, "node2.wk.local:11110", c.cfg.Nodes[1].ClusterAddr, "非匹配节点不应被改动")

	// 不存在的节点：不 panic，且不影响任何现有节点
	c.updateClusterAddr(9999, "x")
	assert.Equal(t, "node1.wk.local:11110", c.cfg.Nodes[0].ClusterAddr)
	assert.Equal(t, "node2.wk.local:11110", c.cfg.Nodes[1].ClusterAddr)
}
