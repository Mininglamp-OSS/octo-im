package wkdb

import "time"

type RecoveryGroupCommitOptions struct {
	Enabled   bool
	Window    time.Duration
	MaxCount  int
	MaxBytes  int
	OldestAge time.Duration
	QueueHard int
}

type Options struct {
	NodeId            uint64
	DataDir           string
	ConversationLimit int // 最近会话查询数量限制
	SlotCount         int // 槽位数量
	// 耗时配置开启
	EnableCost   bool
	ShardNum     int // 数据库分区数量，一但设置就不能修改
	MemTableSize int

	BatchPerSize int // 每个batch里key的大小

	SubscriberRecoveryEnabled bool
	RecoveryGroupCommit       RecoveryGroupCommitOptions
}

func NewOptions(opt ...Option) *Options {
	o := &Options{
		DataDir:             "./data",
		ConversationLimit:   10000,
		SlotCount:           128,
		EnableCost:          true,
		ShardNum:            8,
		MemTableSize:        16 * 1024 * 1024,
		BatchPerSize:        10240,
		RecoveryGroupCommit: RecoveryGroupCommitOptions{Window: 2 * time.Millisecond, MaxCount: 32, MaxBytes: 1 << 20, OldestAge: 5 * time.Millisecond, QueueHard: 256},
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

func WithSubscriberRecoveryEnabled(enabled bool) Option {
	return func(o *Options) {
		o.SubscriberRecoveryEnabled = enabled
	}
}

func WithRecoveryGroupCommit(options RecoveryGroupCommitOptions) Option {
	return func(o *Options) { o.RecoveryGroupCommit = options }
}
