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
	// 同一 tick 里两条 reconcile 都要读 Node 记录,hoist 一次省掉一次 RLock。
	localNode := h.cfgServer.Node(h.cfgOptions.NodeId)
	// 如果配置里自己节点的apiServerAddr配置不存在或不同，则提案配置。
	// 提案失败只记日志：apiServerAddr 与下面的 clusterAddr 是两条独立 reconcile，
	// 不能因为前者失败就阻断后者（下一 tick 会重试）。
	if strings.TrimSpace(h.cfgOptions.ApiServerAddr) != "" {
		if localNode != nil && localNode.ApiServerAddr != h.cfgOptions.ApiServerAddr {
			if err := h.cfgServer.ProposeApiServerAddr(h.cfgOptions.NodeId, h.cfgOptions.ApiServerAddr); err != nil {
				h.Error("ProposeApiServerAddr failed", zap.Error(err))
			}
		}
	}
	// 如果配置里自己节点的clusterAddr为空，则提案回填。
	// standalone 首启时 ServerAddr 为空，记录会带着空 cluster_addr 出生；
	// 之后补配 serverAddr 时必须回填，否则 join 响应会把空地址发给新节点。
	// propose 的值先 TrimSpace，避免带前后空白的地址被下发到 addOrUpdateNodes。
	// ServerAddr 本身的合法性(bind-all / portless)已在摄入点校验过,详见
	// internal/options/options.go 与 pkg/cluster/node/types/addr.go。
	if shouldProposeClusterAddr(h.cfgOptions.ServerAddr, localNode) {
		clusterAddr := strings.TrimSpace(h.cfgOptions.ServerAddr)
		if err := h.cfgServer.ProposeClusterAddr(h.cfgOptions.NodeId, clusterAddr); err != nil {
			h.Error("ProposeClusterAddr failed", zap.Error(err))
		}
	}
}

// shouldProposeClusterAddr 判定是否需要回填本节点的 cluster 通讯地址。
//
// 只在“存储里的 ClusterAddr 为空/空白”时才提案，其它情况一律保留存储值：
//   - ClusterAddr 由两条独立配置项播种：cluster 模式来自 InitNodes[self]（在
//     internal/server/server.go 里剥过 "tcp://" 前缀），standalone 模式来自
//     cfgOptions.ServerAddr（原样透传）。二者天然可以文本不等，例如文档示例的
//     initNodes[self]="127.0.0.1:11110" + serverAddr="0.0.0.0:11110"。
//   - 如果按“文本差异即覆盖”触发，就会把源 A 播下去的正确值覆盖成源 B 的绑定
//     地址（如 0.0.0.0），然后全集群 addOrUpdateNodes 重连到这个不能用的地址。
//
// 本次修复的原始场景（standalone 首启后再补配 serverAddr）里，节点的 ClusterAddr
// 是空的，被此判定覆盖；而其它场景下值非空，此判定拒绝覆盖，从而不会掉进那个陷阱。
// ServerAddr 未配置时同样不提案，避免把空地址写回。
func shouldProposeClusterAddr(serverAddr string, localNode *types.Node) bool {
	if strings.TrimSpace(serverAddr) == "" {
		return false
	}
	if localNode == nil {
		return false
	}
	return strings.TrimSpace(localNode.ClusterAddr) == ""
}
