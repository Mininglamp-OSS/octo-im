package slot

import (
	"context"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

type Options struct {
	BeforePropose func(context.Context, uint32, types.ProposeReqSet) error
	// 节点Id
	NodeId uint64
	// 数据目录
	DataDir string
	// 槽位数据库分片数量
	SlotDbShardNum int
	// 节点接口
	Node icluster.Node
	// 传输层
	Transport raftgroup.ITransport
	// 槽数量
	SlotCount uint32
	// api接口
	RPC icluster.RPC
	// OnApply 应用日志回调
	OnApply func(slotId uint32, logs []types.Log) error
	// ReplaySafeApply promises that OnApply durably commits both its effects
	// and replay protection before returning. Only then may the separate Raft
	// applied cursor lag on a crash. Log append durability is never relaxed.
	ReplaySafeApply bool
	// ApplyErrorRetry enables tick-paced retries for state-machine apply errors.
	ApplyErrorRetry bool

	// OnSaveConfig 保存槽配置
	OnSaveConfig func(slotId uint32, cfg types.Config) error
}

func NewOptions(opt ...Option) *Options {
	defaultOpts := &Options{
		DataDir:        "clusterdata",
		SlotDbShardNum: 8,
	}
	for _, o := range opt {
		o(defaultOpts)
	}

	return defaultOpts
}

type Option func(*Options)

func WithBeforePropose(f func(context.Context, uint32, types.ProposeReqSet) error) Option {
	return func(o *Options) { o.BeforePropose = f }
}

func WithNodeId(nodeId uint64) Option {
	return func(o *Options) {
		o.NodeId = nodeId
	}
}

func WithDataDir(dataDir string) Option {
	return func(o *Options) {
		o.DataDir = dataDir
	}
}

func WithSlotDbShardNum(slotDbShardNum int) Option {
	return func(o *Options) {
		o.SlotDbShardNum = slotDbShardNum
	}
}

func WithTransport(transport raftgroup.ITransport) Option {
	return func(o *Options) {
		o.Transport = transport
	}
}

func WithNode(node icluster.Node) Option {
	return func(o *Options) {
		o.Node = node
	}
}

func WithSlotCount(slotCount uint32) Option {
	return func(o *Options) {
		o.SlotCount = slotCount
	}
}

func WithOnApply(onApply func(slotId uint32, logs []types.Log) error) Option {
	return func(o *Options) {
		o.OnApply = onApply
		o.ReplaySafeApply = false
	}
}

// WithReplaySafeOnApply is for a state machine with durable per-entry replay
// protection. Arbitrary callbacks must use WithOnApply instead.
func WithReplaySafeOnApply(onApply func(slotId uint32, logs []types.Log) error) Option {
	return func(o *Options) {
		o.OnApply = onApply
		o.ReplaySafeApply = true
	}
}

func WithApplyErrorRetry(enabled bool) Option {
	return func(o *Options) {
		o.ApplyErrorRetry = enabled
	}
}

func WithOnSaveConfig(onSaveConfig func(slotId uint32, cfg types.Config) error) Option {
	return func(o *Options) {
		o.OnSaveConfig = onSaveConfig
	}
}
func WithRPC(rpc icluster.RPC) Option {
	return func(o *Options) {
		o.RPC = rpc
	}
}
