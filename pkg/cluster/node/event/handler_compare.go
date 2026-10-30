package event

import (
	"strings"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"go.uber.org/zap"
)

func (h *handler) handleCompare() {
	if h.cfgServer.LeaderId() == 0 {
		return
	}
	localNode := h.cfgServer.Node(h.cfgOptions.NodeId)
	// 如果配置里自己节点的apiServerAddr配置不存在或不同，则提案配置
	if strings.TrimSpace(h.cfgOptions.ApiServerAddr) != "" {
		if localNode != nil && localNode.ApiServerAddr != h.cfgOptions.ApiServerAddr {
			err := h.cfgServer.ProposeApiServerAddr(h.cfgOptions.NodeId, h.cfgOptions.ApiServerAddr)
			if err != nil {
				h.Error("ProposeApiServerAddr failed", zap.Error(err))
			}
		}
	}
	// Standalone records can predate serverAddr. Backfill only an empty
	// address; an already advertised address may come from a different source.
	if shouldProposeClusterAddr(h.cfgOptions.ServerAddr, localNode) {
		if err := h.cfgServer.ProposeClusterAddr(h.cfgOptions.NodeId, strings.TrimSpace(h.cfgOptions.ServerAddr)); err != nil {
			h.Error("ProposeClusterAddr failed", zap.Error(err))
		}
	}
}

func shouldProposeClusterAddr(serverAddr string, localNode *types.Node) bool {
	return strings.TrimSpace(serverAddr) != "" && localNode != nil && strings.TrimSpace(localNode.ClusterAddr) == ""
}
