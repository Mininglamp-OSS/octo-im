package wkdb

import "github.com/cockroachdb/pebble/vfs"

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

	// FS overrides the Pebble filesystem. Nil keeps the default disk FS; tests
	// use it for crash simulation through wkdbtest.OpenWithFS.
	FS vfs.FS
}

func NewOptions(opt ...Option) *Options {
	o := &Options{
		DataDir:           "./data",
		ConversationLimit: 10000,
		SlotCount:         128,
		EnableCost:        true,
		ShardNum:          8,
		MemTableSize:      16 * 1024 * 1024,
		BatchPerSize:      10240,
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

// WithFS sets the Pebble filesystem. It is meant for tests.
func WithFS(fs vfs.FS) Option {
	return func(o *Options) {
		o.FS = fs
	}
}
