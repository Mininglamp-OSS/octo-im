package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/assert"
)

// handleCompare 每个 tick 都会调用 shouldProposeClusterAddr 决定是否回填。
// 门控只允许一种触发：ServerAddr 已配置、存储里的 ClusterAddr 为空/空白。
// 存储值非空一律不覆盖 —— 因为 ClusterAddr 是从 InitNodes[self] 或 serverAddr
// 播种，二者是独立配置项，可能天然文本不等(例如 initNodes=127.0.0.1:11110、
// serverAddr=0.0.0.0:11110)；把文本差异当变更来同步会把正确的播种值覆盖成
// 绑定地址(0.0.0.0),再持久化 + 复制给所有 peer，反过来把工作连接打断。
func TestShouldProposeClusterAddr(t *testing.T) {
	const addr = "127.0.0.1:11110"

	// 未配置 ServerAddr（standalone 未配 cluster.serverAddr）：绝不回填,避免写回空
	assert.False(t, shouldProposeClusterAddr("", &types.Node{ClusterAddr: ""}))
	assert.False(t, shouldProposeClusterAddr("   ", &types.Node{ClusterAddr: ""}))

	// 本地还没有该节点记录：不提案,也不能 panic
	assert.False(t, shouldProposeClusterAddr(addr, nil))

	// 存储值与配置一致：已收敛,不提案
	assert.False(t, shouldProposeClusterAddr(addr, &types.Node{ClusterAddr: addr}))

	// 存储值为空/空白 + 已配 ServerAddr：唯一需要回填的场景
	assert.True(t, shouldProposeClusterAddr(addr, &types.Node{ClusterAddr: ""}))
	assert.True(t, shouldProposeClusterAddr(addr, &types.Node{ClusterAddr: "   "}))

	// 存储值非空(来自 InitNodes[self] 的播种)+ ServerAddr 是不同文本：不覆盖。
	// initNodes 里写内网 IP、cluster.serverAddr 写 0.0.0.0 监听所有网卡的场景下,
	// 不能把 127.0.0.1:11110 覆盖成 0.0.0.0:11110,否则 peer 会拨到本机上,
	// 发生"peer 反向连自己"。
	// (bind-all 本身在 options 摄入点已被拒绝,这里再兜底一层,防止 raw storage
	// 里因为历史遗留值走到覆盖路径。)
	assert.False(t, shouldProposeClusterAddr("0.0.0.0:11110", &types.Node{ClusterAddr: "127.0.0.1:11110"}))

	// 存储值非空 + operator 改了 serverAddr(旧地址→新地址):保持不覆盖。
	// 目前代码里没有专门的地址变更路径 —— 已知 P2,由后续 PR 引入
	// 显式的运维触发通道,而不是让这里的差异性触发去猜。
	assert.False(t, shouldProposeClusterAddr(addr, &types.Node{ClusterAddr: "old.addr:1"}))
}
