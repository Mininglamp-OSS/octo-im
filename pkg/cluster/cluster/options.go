package cluster

import (
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/auth"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
)

type Options struct {
	// 节点地址
	Addr string
	// 数据目录
	DataDir string

	// 分布式配置
	ConfigOptions *clusterconfig.Options

	// slot数据传输层
	SlotTransport raftgroup.ITransport

	// channel数据传输层
	ChannelTransport raftgroup.ITransport

	// MaxMessageBatchSize 节点之间每次发送消息的最大大小（单位字节）
	MaxMessageBatchSize uint64
	// ReceiveQueueLength 副本接收队列的长度。
	ReceiveQueueLength uint64
	// SendQueueLength 是用于在节点主机之间交换消息的发送队列的长度。
	SendQueueLength int
	// MaxSendQueueSize 是每个发送队列的最大大小(以字节为单位)。
	// 如果达到最大大小,后续复制消息将被丢弃以限制内存使用。
	// 当设置为0时,表示发送队列大小没有限制。
	MaxSendQueueSize uint64

	DB struct {
		WKDbShardNum        int   // wkdb 分片数量
		WKDbMemTableSize    int   // wkdb MemTable大小
		SlotShardNum        int   // 分片数量
		SlotMemTableSize    int   // MemTable大小
		BlockCacheSize      int64 // 共享 Pebble BlockCache 大小（字节）
		MaxMessagesPerBatch int   // 单次 Pebble Batch 最大消息数
		MaxBatchBytes       int   // 单次 Pebble Batch 最大字节数
	}

	MaxLogSizePerBatch              uint64        // 每次拉取日志的最大字节数
	MaxLogCountPerBatch             int           // 每次拉取日志的最大条数
	LearnerToFollowerMaxApplyLag    uint64        // Learner 晋升 Follower 允许的最大应用滞后条数
	MaxReadyApplyLag                uint64        // 节点就绪探针允许的最大本地槽位应用滞后条数
	MaxApplyStallTicks              int           // 节点就绪探针允许的最大连续应用停滞 tick 数
	MaxSlotApplyWait                time.Duration // 槽领导读取前等待本地应用追平的最大超时时间
	HandoffDrainTimeoutTick         int           // 迁主/准入排空超时 tick 数
	MaxConcurrentChannelMigrations  int           // 后台并发执行的频道 Learner 迁移上限
	ChannelLeaderAutoBalance        bool          // 是否开启后台频道领导权自动均衡

	ServerAddr string // 服务地址

	Role types.NodeRole

	// 种子节点
	Seed string

	Auth auth.AuthConfig // 认证配置

	PageSize int // 分页大小

	ReqTimeout time.Duration

	AppVersion string

	IsCmdChannel func(channel string) bool
}

func NewOptions(opt ...Option) *Options {
	opts := &Options{
		MaxMessageBatchSize: 64 * 1024 * 1024, // 64M
		ReceiveQueueLength:  1024,
		SendQueueLength:     4056 * 10,
		Addr:                "tcp://127.0.0.1:11110",
		ReqTimeout:          5 * time.Second,
		DB: struct {
			WKDbShardNum        int
			WKDbMemTableSize    int
			SlotShardNum        int
			SlotMemTableSize    int
			BlockCacheSize      int64
			MaxMessagesPerBatch int
			MaxBatchBytes       int
		}{
			WKDbShardNum:        8,
			WKDbMemTableSize:    16 * 1024 * 1024,
			SlotShardNum:        8,
			SlotMemTableSize:    16 * 1024 * 1024,
			BlockCacheSize:      16 * 1024 * 1024,
			MaxMessagesPerBatch: 256,
			MaxBatchBytes:       2 * 1024 * 1024,
		},
		MaxLogSizePerBatch:             2 * 1024 * 1024,
		MaxLogCountPerBatch:            500,
		LearnerToFollowerMaxApplyLag:   8,
		MaxReadyApplyLag:               64,
		MaxApplyStallTicks:             50,
		MaxSlotApplyWait:               500 * time.Millisecond,
		HandoffDrainTimeoutTick:        100,
		MaxConcurrentChannelMigrations: 8,
		ChannelLeaderAutoBalance:       false,
		PageSize:                       20,
	}
	for _, o := range opt {
		o(opts)
	}
	return opts
}

type Option func(*Options)

func WithDataDir(dataDir string) Option {
	return func(o *Options) {
		o.DataDir = dataDir
	}
}

func WithConfigOptions(configOptions *clusterconfig.Options) Option {
	return func(o *Options) {
		o.ConfigOptions = configOptions
	}
}

func WithSlotTransport(transport raftgroup.ITransport) Option {
	return func(o *Options) {
		o.SlotTransport = transport
	}
}

func WithChannelTransport(transport raftgroup.ITransport) Option {
	return func(o *Options) {
		o.ChannelTransport = transport
	}
}

func WithMaxMessageBatchSize(maxMessageBatchSize uint64) Option {
	return func(o *Options) {
		o.MaxMessageBatchSize = maxMessageBatchSize
	}
}

func WithReceiveQueueLength(receiveQueueLength uint64) Option {
	return func(o *Options) {
		o.ReceiveQueueLength = receiveQueueLength
	}
}

func WithSendQueueLength(sendQueueLength int) Option {
	return func(o *Options) {
		o.SendQueueLength = sendQueueLength
	}
}

func WithMaxSendQueueSize(maxSendQueueSize uint64) Option {
	return func(o *Options) {
		o.MaxSendQueueSize = maxSendQueueSize
	}
}

func WithAddr(addr string) Option {
	return func(o *Options) {
		o.Addr = addr
	}
}

func WithServerAddr(serverAddr string) Option {
	return func(o *Options) {
		o.ServerAddr = serverAddr
	}
}

func WithRole(role types.NodeRole) Option {
	return func(o *Options) {
		o.Role = role
	}
}

func WithSeed(seed string) Option {
	return func(o *Options) {
		o.Seed = seed
	}
}

func WithAuth(auth auth.AuthConfig) Option {
	return func(o *Options) {
		o.Auth = auth
	}
}

func WithDBSlotShardNum(shardNum int) Option {
	return func(o *Options) {
		o.DB.SlotShardNum = shardNum
	}
}

func WithDBSlotMemTableSize(memTableSize int) Option {
	return func(o *Options) {
		o.DB.SlotMemTableSize = memTableSize
	}
}

func WithDBWKDbShardNum(shardNum int) Option {
	return func(o *Options) {
		o.DB.WKDbShardNum = shardNum
	}
}

func WithDBWKDbMemTableSize(memTableSize int) Option {
	return func(o *Options) {
		o.DB.WKDbMemTableSize = memTableSize
	}
}

func WithPageSize(pageSize int) Option {
	return func(o *Options) {
		o.PageSize = pageSize
	}
}

func WithReqTimeout(reqTimeout time.Duration) Option {
	return func(o *Options) {
		o.ReqTimeout = reqTimeout
	}
}

func WithAppVersion(appVersion string) Option {
	return func(o *Options) {
		o.AppVersion = appVersion
	}
}

func WithIsCmdChannel(isCmdChannel func(channel string) bool) Option {
	return func(o *Options) {
		o.IsCmdChannel = isCmdChannel
	}
}

func WithDBBlockCacheSize(blockCacheSize int64) Option {
	return func(o *Options) {
		o.DB.BlockCacheSize = blockCacheSize
	}
}

func WithDBMaxMessagesPerBatch(maxMessagesPerBatch int) Option {
	return func(o *Options) {
		o.DB.MaxMessagesPerBatch = maxMessagesPerBatch
	}
}

func WithDBMaxBatchBytes(maxBatchBytes int) Option {
	return func(o *Options) {
		o.DB.MaxBatchBytes = maxBatchBytes
	}
}

func WithMaxLogSizePerBatch(maxLogSizePerBatch uint64) Option {
	return func(o *Options) {
		o.MaxLogSizePerBatch = maxLogSizePerBatch
	}
}

func WithMaxLogCountPerBatch(maxLogCountPerBatch int) Option {
	return func(o *Options) {
		o.MaxLogCountPerBatch = maxLogCountPerBatch
	}
}

func WithLearnerToFollowerMaxApplyLag(lag uint64) Option {
	return func(o *Options) {
		o.LearnerToFollowerMaxApplyLag = lag
	}
}

func WithMaxReadyApplyLag(lag uint64) Option {
	return func(o *Options) {
		o.MaxReadyApplyLag = lag
	}
}

func WithMaxApplyStallTicks(ticks int) Option {
	return func(o *Options) {
		o.MaxApplyStallTicks = ticks
	}
}

func WithMaxSlotApplyWait(d time.Duration) Option {
	return func(o *Options) {
		o.MaxSlotApplyWait = d
	}
}

func WithHandoffDrainTimeoutTick(ticks int) Option {
	return func(o *Options) {
		o.HandoffDrainTimeoutTick = ticks
	}
}

func WithMaxConcurrentChannelMigrations(maxMigrations int) Option {
	return func(o *Options) {
		o.MaxConcurrentChannelMigrations = maxMigrations
	}
}

func WithChannelLeaderAutoBalance(autoBalance bool) Option {
	return func(o *Options) {
		o.ChannelLeaderAutoBalance = autoBalance
	}
}
