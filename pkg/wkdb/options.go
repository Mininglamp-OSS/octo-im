package wkdb

import "github.com/cockroachdb/pebble"

type Options struct {
	NodeId            uint64
	DataDir           string
	ConversationLimit int // 最近会话查询数量限制
	SlotCount         int // 槽位数量
	// 耗时配置开启
	EnableCost                 bool
	ShardNum                   int // 数据库分区数量，一但设置就不能修改
	MemTableSize               int
	BlockCache                 *pebble.Cache
	BlockCacheSize             int64  // Pebble BlockCache 总大小（字节）
	MaxMessagesPerBatch        int    // AppendMessages 单次子批次最大消息条数
	MaxBatchBytes              uint64 // AppendMessages 单次子批次最大字节数
	MaxMessageLogCountPerBatch int    // LoadNextRangeMsgsForSize 单次最大消息条数

	BatchPerSize int // 每个batch里key的大小
}

func NewOptions(opt ...Option) *Options {
	o := &Options{
		DataDir:                    "./data",
		ConversationLimit:          10000,
		SlotCount:                  128,
		EnableCost:                 true,
		ShardNum:                   8,
		MemTableSize:               16 * 1024 * 1024,
		BlockCacheSize:             16 * 1024 * 1024,
		MaxMessagesPerBatch:        256,
		MaxBatchBytes:              2 * 1024 * 1024,
		MaxMessageLogCountPerBatch: 500,
		BatchPerSize:               10240,
	}
	for _, f := range opt {
		f(o)
	}
	return o
}

type Option func(*Options)

func WithDir(dir string) Option {
	return func(o *Options) {
		o.DataDir = dir
	}
}

func WithNodeId(nodeId uint64) Option {
	return func(o *Options) {
		o.NodeId = nodeId
	}
}

func WithSlotCount(slotCount int) Option {
	return func(o *Options) {
		o.SlotCount = slotCount
	}
}

func WithConversationLimit(limit int) Option {
	return func(o *Options) {
		o.ConversationLimit = limit
	}
}

func WithEnableCost() Option {
	return func(o *Options) {
		o.EnableCost = true
	}
}

func WithShardNum(shardNum int) Option {
	return func(o *Options) {
		o.ShardNum = shardNum
	}
}

func WithMemTableSize(size int) Option {
	return func(o *Options) {
		o.MemTableSize = size
	}
}

func WithBlockCache(cache *pebble.Cache) Option {
	return func(o *Options) {
		o.BlockCache = cache
	}
}

func WithBlockCacheSize(size int64) Option {
	return func(o *Options) {
		o.BlockCacheSize = size
	}
}

func WithMaxMessagesPerBatch(maxMsgs int) Option {
	return func(o *Options) {
		o.MaxMessagesPerBatch = maxMsgs
	}
}

func WithMaxBatchBytes(maxBytes uint64) Option {
	return func(o *Options) {
		o.MaxBatchBytes = maxBytes
	}
}

func WithMaxMessageLogCountPerBatch(maxCount int) Option {
	return func(o *Options) {
		o.MaxMessageLogCountPerBatch = maxCount
	}
}
