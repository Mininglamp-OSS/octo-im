package slot

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/cockroachdb/pebble"
)

type Options struct {
	// 节点Id
	NodeId uint64
	// 数据目录
	DataDir string
	// 槽位数据库分片数量
	SlotDbShardNum int
	// 槽位数据库MemTable大小
	SlotMemTableSize int
	// 共享Pebble BlockCache
	BlockCache *pebble.Cache
	// 单批最大日志字节数
	MaxLogSizePerBatch uint64
	// 单批最大日志条数
	MaxLogCountPerBatch uint64
	// Learner晋升或计划切主前允许的最大应用延迟条数
	LearnerToFollowerMaxApplyLag uint64
	// 计划切主排空超时tick数
	MaxAdmissionDrainTicks int
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

	// OnSaveConfig 保存槽配置
	OnSaveConfig func(slotId uint32, cfg types.Config) error
}

func NewOptions(opt ...Option) *Options {
	defaultOpts := &Options{
		DataDir:                      "clusterdata",
		SlotDbShardNum:               8,
		SlotMemTableSize:             16 * 1024 * 1024,
		MaxLogSizePerBatch:           2 * 1024 * 1024,
		MaxLogCountPerBatch:          500,
		LearnerToFollowerMaxApplyLag: 8,
		MaxAdmissionDrainTicks:       100,
	}
	for _, o := range opt {
		o(defaultOpts)
	}

	return defaultOpts
}

type Option func(*Options)

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

func WithSlotMemTableSize(slotMemTableSize int) Option {
	return func(o *Options) {
		o.SlotMemTableSize = slotMemTableSize
	}
}

func WithBlockCache(cache *pebble.Cache) Option {
	return func(o *Options) {
		o.BlockCache = cache
	}
}

func WithMaxLogSizePerBatch(maxBytes uint64) Option {
	return func(o *Options) {
		o.MaxLogSizePerBatch = maxBytes
	}
}

func WithMaxLogCountPerBatch(maxCount uint64) Option {
	return func(o *Options) {
		o.MaxLogCountPerBatch = maxCount
	}
}

func WithLearnerToFollowerMaxApplyLag(lag uint64) Option {
	return func(o *Options) {
		o.LearnerToFollowerMaxApplyLag = lag
	}
}

func WithMaxAdmissionDrainTicks(ticks int) Option {
	return func(o *Options) {
		o.MaxAdmissionDrainTicks = ticks
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
