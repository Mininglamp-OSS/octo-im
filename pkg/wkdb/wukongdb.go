package wkdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/bwmarrin/snowflake"
	"github.com/cockroachdb/pebble"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/lni/goutils/syncutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ DB = (*wukongDB)(nil)

type wukongDB struct {
	lifecycleCache        *lru.Cache[string, lifecycleCacheEntry]
	lifecycleCacheVersion [64]atomic.Uint64
	recoveryChannelLocks  [64]sync.RWMutex
	subscriberRecoveryMu  []sync.Mutex
	recoveryUserLocks     [64]sync.Mutex
	recoveryActive        atomic.Bool
	dbs                   []*pebble.DB
	wkdbs                 []*BatchDB
	shardNum              uint32 // 分区数量，这个一但设置就不能修改
	opts                  *Options
	sync                  *pebble.WriteOptions
	endian                binary.ByteOrder
	wklog.Log
	prmaryKeyGen *snowflake.Node // 消息ID生成器
	noSync       *pebble.WriteOptions
	dblock       *dblock
	cancelCtx    context.Context
	cancelFunc   context.CancelFunc

	metrics trace.IDBMetrics

	channelSeqCache     *channelSeqCache
	conversationCache   *ConversationCache
	channelInfoCache    *ChannelInfoCache
	permissionCache     *PermissionCache           // 统一的权限缓存（替代 denylistCache, subscriberCache, allowlistCache）
	clusterConfigCache  *ChannelClusterConfigCache // 频道集群配置缓存
	deviceCache         *DeviceCache               // 设备缓存
	userLastMsgSeqCache *userLastMsgSeqCache       // 用户在频道内发送的最后一条消息序号缓存
	cacheManager        *CacheManager              // 缓存管理器
	performanceMonitor  *PerformanceMonitor        // 性能监控器

	h hash.Hash32
}

func NewWukongDB(opts *Options) DB {
	prmaryKeyGen, err := snowflake.NewNode(int64(opts.NodeId))
	if err != nil {
		panic(err)
	}

	var metrics trace.IDBMetrics
	if trace.GlobalTrace != nil {
		metrics = trace.GlobalTrace.Metrics.DB()
	} else {
		metrics = trace.NewDBMetrics()
	}

	endian := binary.BigEndian

	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	wk := &wukongDB{
		opts:                 opts,
		subscriberRecoveryMu: make([]sync.Mutex, opts.ShardNum),
		shardNum:             uint32(opts.ShardNum),
		prmaryKeyGen:         prmaryKeyGen,
		endian:               endian,
		cancelCtx:            cancelCtx,
		cancelFunc:           cancelFunc,
		metrics:              metrics,
		channelSeqCache:      newChannelSeqCache(1000, endian),
		conversationCache:    NewConversationCache(1000),         // 缓存1000个 GetLastConversations 查询结果
		channelInfoCache:     NewChannelInfoCache(1000),          // 缓存频道信息
		permissionCache:      NewPermissionCache(1000),           // 缓存权限查询结果（统一缓存）
		clusterConfigCache:   NewChannelClusterConfigCache(1000), // 缓存集群配置
		deviceCache:          NewDeviceCache(1000),               // 缓存1000个设备
		userLastMsgSeqCache:  newUserLastMsgSeqCache(10000),      // 缓存10000个用户在频道内发送的最后一条消息序号
		performanceMonitor:   NewPerformanceMonitor(),            // 性能监控器
		h:                    fnv.New32(),
		sync: &pebble.WriteOptions{
			Sync: true,
		},
		noSync: &pebble.WriteOptions{
			Sync: false,
		},
		Log:    wklog.NewWKLog("wukongDB"),
		dblock: newDBLock(),
	}
	wk.recoveryActive.Store(opts.SubscriberRecoveryEnabled)
	wk.lifecycleCache, _ = lru.New[string, lifecycleCacheEntry](10000)

	// 创建缓存管理器
	wk.cacheManager = NewCacheManager(
		wk.permissionCache,
		wk.conversationCache,
		wk.channelInfoCache,
		wk.clusterConfigCache,
		wk.deviceCache,
	)

	return wk
}

func (wk *wukongDB) defaultPebbleOptions() *pebble.Options {
	blockSize := 32 * 1024
	sz := 16 * 1024 * 1024
	levelSizeMultiplier := 2

	lopts := make([]pebble.LevelOptions, 0)
	var numOfLevels int64 = 7
	for l := int64(0); l < numOfLevels; l++ {
		opt := pebble.LevelOptions{
			// Compression:    pebble.NoCompression,
			BlockSize:      blockSize,
			TargetFileSize: 16 * 1024 * 1024,
		}
		sz = sz * levelSizeMultiplier
		lopts = append(lopts, opt)
	}
	return &pebble.Options{
		Levels:             lopts,
		FormatMajorVersion: pebble.FormatNewest,
		// Share WAL syncs from concurrent metadata and message writers without
		// acknowledging any synchronous write before its durability barrier.
		WALMinSyncInterval: func() time.Duration { return 20 * time.Millisecond },
		// 控制写缓冲区的大小。较大的写缓冲区可以减少磁盘写入次数，但会占用更多内存。
		MemTableSize: wk.opts.MemTableSize,
		// 当队列中的MemTables的大小超过 MemTableStopWritesThreshold*MemTableSize 时，将停止写入，
		// 直到被刷到磁盘，这个值不能小于2
		MemTableStopWritesThreshold: 4,
		// MANIFEST 文件的大小
		MaxManifestFileSize:       128 * 1024 * 1024,
		LBaseMaxBytes:             4 * 1024 * 1024 * 1024,
		L0CompactionFileThreshold: 8,
		L0StopWritesThreshold:     24,
	}
}

func (wk *wukongDB) Open() error {

	wk.dblock.start()

	opts := wk.defaultPebbleOptions()
	for i := 0; i < int(wk.shardNum); i++ {

		db, err := pebble.Open(filepath.Join(wk.opts.DataDir, "wukongimdb", fmt.Sprintf("shard%03d", i)), opts)
		if err != nil {
			wk.closeOpenedShards()
			return err
		}
		wk.dbs = append(wk.dbs, db)
	}
	if err := wk.loadSubscriberRecoveryActive(); err != nil {
		wk.closeOpenedShards()
		return err
	}
	if err := wk.migrateSubscriberWorkPages(); err != nil {
		wk.closeOpenedShards()
		return err
	}
	for i, db := range wk.dbs {
		wkdb := NewBatchDB(i, db)
		wkdb.Start()
		wk.wkdbs = append(wk.wkdbs, wkdb)
	}

	go wk.collectMetricsLoop()

	// 启动缓存管理器
	wk.cacheManager.Start()

	return nil
}

func (wk *wukongDB) closeOpenedShards() {
	for _, db := range wk.dbs {
		if err := db.Close(); err != nil {
			wk.Error("close db after open failure", zap.Error(err))
		}
	}
	wk.dbs = nil
	wk.dblock.stop()
}

func (wk *wukongDB) Close() error {
	wk.cancelFunc()

	// 停止缓存管理器
	if wk.cacheManager != nil {
		wk.cacheManager.Stop()
	}

	for _, db := range wk.dbs {
		if err := db.Close(); err != nil {
			wk.Error("close db error", zap.Error(err))
		}
	}

	for _, wkd := range wk.wkdbs {
		wkd.Stop()
	}
	wk.dblock.stop()
	return nil
}

func (wk *wukongDB) shardDB(v string) *pebble.DB {
	shardId := wk.shardId(v)
	return wk.dbs[shardId]
}

func (wk *wukongDB) sharedBatchDB(v string) *BatchDB {
	shardId := wk.shardId(v)
	return wk.wkdbs[shardId]
}

func (wk *wukongDB) shardId(v string) uint32 {
	if v == "" {
		wk.Panic("shardId key is empty")
	}
	if wk.opts.ShardNum == 1 {
		return 0
	}
	h := fnv.New32()
	h.Write([]byte(v))

	return h.Sum32() % wk.shardNum
}

func (wk *wukongDB) shardDBById(id uint32) *pebble.DB {
	return wk.dbs[id]
}

func (wk *wukongDB) shardBatchDBById(id uint32) *BatchDB {
	return wk.wkdbs[id]
}

func (wk *wukongDB) defaultShardDB() *pebble.DB {
	return wk.dbs[0]
}

func (wk *wukongDB) defaultShardBatchDB() *BatchDB {
	return wk.wkdbs[0]
}

func (wk *wukongDB) channelSlotId(channelId string) uint32 {
	return wkutil.GetSlotNum(int(wk.opts.SlotCount), channelId)
}

// GetShardNum 获取数据库分片数量
func (wk *wukongDB) GetShardNum() int {
	return int(wk.shardNum)
}

// GetChannelShardIndex 获取频道所在的分片索引
func (wk *wukongDB) GetChannelShardIndex(channelId string, channelType uint8) uint32 {
	return uint32(key.ChannelToNum(channelId, channelType) % uint64(wk.shardNum))
}

func (wk *wukongDB) collectMetricsLoop() {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			wk.collectMetrics()
		case <-wk.cancelCtx.Done():
			return
		}
	}
}

func (wk *wukongDB) collectMetrics() {

	for i := uint32(0); i < uint32(wk.shardNum); i++ {
		ms := wk.dbs[i].Metrics()

		// ========== compact 压缩相关 ==========
		wk.metrics.CompactTotalCountSet(i, ms.Compact.Count)
		wk.metrics.CompactDefaultCountSet(i, ms.Compact.DefaultCount)
		wk.metrics.CompactDeleteOnlyCountSet(i, ms.Compact.DeleteOnlyCount)
		wk.metrics.CompactElisionOnlyCountSet(i, ms.Compact.ElisionOnlyCount)
		wk.metrics.CompactEstimatedDebtSet(i, int64(ms.Compact.EstimatedDebt))
		wk.metrics.CompactInProgressBytesSet(i, ms.Compact.InProgressBytes)
		wk.metrics.CompactMarkedFilesSet(i, int64(ms.Compact.MarkedFiles))
		wk.metrics.CompactMoveCountSet(i, ms.Compact.MoveCount)
		wk.metrics.CompactMultiLevelCount(i, ms.Compact.MultiLevelCount)
		wk.metrics.CompactNumInProgressSet(i, ms.Compact.NumInProgress)
		wk.metrics.CompactReadCountSet(i, ms.Compact.ReadCount)
		wk.metrics.CompactRewriteCountSet(i, ms.Compact.RewriteCount)

		// ========== flush 相关 ==========
		wk.metrics.FlushCountAdd(i, int64(ms.Flush.Count))
		wk.metrics.FlushBytesAdd(i, ms.Flush.WriteThroughput.Bytes)
		wk.metrics.FlushNumInProgressAdd(i, ms.Flush.NumInProgress)
		wk.metrics.FlushAsIngestCountAdd(i, int64(ms.Flush.AsIngestCount))
		wk.metrics.FlushAsIngestTableCountAdd(i, int64(ms.Flush.AsIngestTableCount))
		wk.metrics.FlushAsIngestBytesAdd(i, int64(ms.Flush.AsIngestBytes))

		// ========== memtable 内存表相关 ==========
		wk.metrics.MemTableCountSet(i, int64(ms.MemTable.Count))
		wk.metrics.MemTableSizeSet(i, int64(ms.MemTable.Size))
		wk.metrics.MemTableZombieSizeSet(i, int64(ms.MemTable.ZombieSize))
		wk.metrics.MemTableZombieCountSet(i, ms.MemTable.ZombieCount)

		// ========== Snapshots 镜像相关 ==========
		wk.metrics.SnapshotsCountSet(i, int64(ms.Snapshots.Count))

		// ========== TableCache 相关 ==========
		wk.metrics.TableCacheSizeSet(i, ms.TableCache.Size)
		wk.metrics.TableCacheCountSet(i, ms.TableCache.Count)
		wk.metrics.TableItersCountSet(i, ms.TableIters)

		// ========== WAL 相关 ==========
		wk.metrics.WALFilesCountSet(i, ms.WAL.Files)
		wk.metrics.WALSizeSet(i, int64(ms.WAL.Size))
		wk.metrics.WALPhysicalSizeSet(i, int64(ms.WAL.PhysicalSize))
		wk.metrics.WALObsoleteFilesCountSet(i, ms.WAL.ObsoleteFiles)
		wk.metrics.WALObsoletePhysicalSizeSet(i, int64(ms.WAL.ObsoletePhysicalSize))
		wk.metrics.WALBytesInSet(i, int64(ms.WAL.BytesIn))
		wk.metrics.WALBytesWrittenSet(i, int64(ms.WAL.BytesWritten))

		// ========== Write 相关 ==========
		wk.metrics.LogWriterBytesSet(i, ms.LogWriter.WriteThroughput.Bytes)

		wk.metrics.DiskSpaceUsageSet(i, int64(ms.DiskSpaceUsage()))

		// ========== level 相关 ==========

		wk.metrics.LevelNumFilesSet(i, ms.Total().NumFiles)
		wk.metrics.LevelFileSizeSet(i, int64(ms.Total().Size))
		wk.metrics.LevelCompactScoreSet(i, int64(ms.Total().Score))
		wk.metrics.LevelBytesInSet(i, int64(ms.Total().BytesIn))
		wk.metrics.LevelBytesIngestedSet(i, int64(ms.Total().BytesIngested))
		wk.metrics.LevelBytesMovedSet(i, int64(ms.Total().BytesMoved))
		wk.metrics.LevelBytesReadSet(i, int64(ms.Total().BytesRead))
		wk.metrics.LevelBytesCompactedSet(i, int64(ms.Total().BytesCompacted))
		wk.metrics.LevelBytesFlushedSet(i, int64(ms.Total().BytesFlushed))
		wk.metrics.LevelTablesCompactedSet(i, int64(ms.Total().TablesCompacted))
		wk.metrics.LevelTablesFlushedSet(i, int64(ms.Total().TablesFlushed))
		wk.metrics.LevelTablesIngestedSet(i, int64(ms.Total().TablesIngested))
		wk.metrics.LevelTablesMovedSet(i, int64(ms.Total().TablesMoved))

	}
}

func (wk *wukongDB) NextPrimaryKey() uint64 {
	return uint64(wk.prmaryKeyGen.Generate().Int64())
}

// 批量提交
func Commits(bs []*Batch) error {
	if len(bs) == 0 {
		return nil
	}
	newBatchs := groupBatch(bs)
	if len(newBatchs) == 1 {
		return newBatchs[0].CommitWait()
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	g, _ := errgroup.WithContext(timeoutCtx)
	g.SetLimit(200)
	for _, b := range newBatchs {
		b1 := b
		g.Go(func() error {
			return b1.CommitWait()
		})
	}
	return g.Wait()
}

// 将batch集合操作按照db进行聚合到一起

func groupBatch(bs []*Batch) []*Batch {
	newBatchs := make([]*Batch, 0, len(bs))
	for _, b := range bs {
		exist := false
		for _, nb := range newBatchs {
			if nb.db == b.db {
				exist = true
				nb.setKvs = append(nb.setKvs, b.setKvs...)
				nb.delKvs = append(nb.delKvs, b.delKvs...)
				nb.delRangeKvs = append(nb.delRangeKvs, b.delRangeKvs...)
				break
			}
		}
		if !exist {
			newBatchs = append(newBatchs, b)
		}
	}
	return newBatchs
}

type BatchDB struct {
	db *pebble.DB

	batchChan chan *Batch

	stopper *syncutil.Stopper
	Index   int
}

func NewBatchDB(index int, db *pebble.DB) *BatchDB {
	return &BatchDB{
		batchChan: make(chan *Batch, 4000),
		stopper:   syncutil.NewStopper(),
		db:        db,
		Index:     index,
	}
}

func (wk *BatchDB) NewBatch() *Batch {

	return &Batch{
		db: wk,
	}
}

func (wk *BatchDB) Start() {
	for i := 0; i < 1; i++ {
		wk.stopper.RunWorker(wk.loop)
	}
}

func (wk *BatchDB) Stop() {
	wk.stopper.Stop()
}

func (wk *BatchDB) loop() {
	batchSize := 100
	done := false
	batches := make([]*Batch, 0, batchSize)
	for {
		select {
		case bt := <-wk.batchChan:
			// 获取所有的batch
			batches = append(batches, bt)
			for !done {
				select {
				case b := <-wk.batchChan:
					batches = append(batches, b)
					if len(batches) >= batchSize {
						done = true
					}
				default:
					done = true
				}
			}
			wk.executeBatch(batches) // 批量执行
			batches = batches[:0]
			done = false

		case <-wk.stopper.ShouldStop():
			return
		}
	}
}

func (wk *BatchDB) executeBatch(bs []*Batch) {

	bt := wk.db.NewBatch()
	defer bt.Close()

	// start := time.Now()

	for _, b := range bs {

		// fmt.Println("batch-->:", b.String())

		// trace.GlobalTrace.Metrics.DB().SetAdd(int64(len(b.setKvs)))
		// trace.GlobalTrace.Metrics.DB().DeleteAdd(int64(len(b.delKvs)))
		// trace.GlobalTrace.Metrics.DB().DeleteRangeAdd(int64(len(b.delRangeKvs)))

		for _, kv := range b.delKvs {
			if err := bt.Delete(kv.key, pebble.NoSync); err != nil {
				b.err = err
				break
			}
		}

		for _, kv := range b.delRangeKvs {
			if err := bt.DeleteRange(kv.key, kv.val, pebble.NoSync); err != nil {
				b.err = err
				break
			}
		}

		for _, kv := range b.setKvs {
			if err := bt.Set(kv.key, kv.val, pebble.NoSync); err != nil {
				b.err = err
				break
			}
		}

	}
	// trace.GlobalTrace.Metrics.DB().CommitAdd(1)
	err := bt.Commit(pebble.Sync)
	if err != nil {
		for _, b := range bs {
			b.err = err
			if b.waitC != nil {
				b.waitC <- err
			}
		}
		return
	}

	// end := time.Since(start)
	// fmt.Println("executeBatch耗时--->", end, len(bs))

	for _, b := range bs {
		if b.waitC != nil {
			b.waitC <- b.err
		}
		// 释放资源
		b.release()
	}

}

type Batch struct {
	db          *BatchDB
	setKvs      []kv
	delKvs      []kv
	delRangeKvs []kv
	waitC       chan error
	err         error
}

func (b *Batch) Set(key, value []byte) {
	// 预分配切片容量
	if cap(b.setKvs) == 0 {
		b.setKvs = make([]kv, 0, 100) // 假设预估大小为100
	}
	b.setKvs = append(b.setKvs, kv{
		key: key,
		val: value,
	})
}

func (b *Batch) Delete(key []byte) {
	b.delKvs = append(b.delKvs, kv{
		key: key,
		val: nil,
	})
}

func (b *Batch) DeleteRange(start, end []byte) {
	b.delRangeKvs = append(b.delRangeKvs, kv{
		key: start,
		val: end,
	})
}

func (b *Batch) Commit() error {
	b.db.batchChan <- b
	return nil
}

func (b *Batch) CommitWait() error {
	waitC := make(chan error, 1)
	b.waitC = waitC
	b.db.batchChan <- b
	return <-waitC
}

func (b *Batch) release() {
	b.setKvs = nil
	b.delKvs = nil
	b.delRangeKvs = nil
	b.waitC = nil
	b.err = nil
}

func (b *Batch) String() string {
	return fmt.Sprintf("setKvs:%d, delKvs:%d, delRangeKvs:%d", len(b.setKvs), len(b.delKvs), len(b.delRangeKvs))
}

func (b *Batch) DbIndex() int {
	return b.db.Index
}

type kv struct {
	key []byte
	val []byte
}

// GetPerformanceMonitor 获取性能监控器
func (wk *wukongDB) GetPerformanceMonitor() *PerformanceMonitor {
	return wk.performanceMonitor
}

// GetCacheManager 获取缓存管理器
func (wk *wukongDB) GetCacheManager() *CacheManager {
	return wk.cacheManager
}
