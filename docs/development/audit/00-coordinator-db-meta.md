# 协调者核实：`pkg/db/meta` 全局锁与无界缓存（Unit 30 前置结论）

> 由协调者在 worker 排队期间亲自读源码核实。三条均已逐行确认，可直接进入总清单。
> 若 Unit 30 worker 后续运行，应在此基础上继续，不必重复核实这三条。

---

## [P1] M-1. `channelCache` 完全无界，按频道数无限增长

**位置**：`pkg/db/meta/db.go:17,90-97` + `pkg/db/meta/table_channel.go:119-127`

```go
// db.go:10-19
type MetaDB struct {
	engine *engine.DB
	mu         sync.Mutex
	shards     map[HashSlot]*Shard
	shardLocks map[HashSlot]*sync.Mutex
	channelCache map[string]Channel      // ← 无容量上限、无 TTL
	testLocked   []HashSlot
}

// db.go:90-97
func (db *MetaDB) rememberChannel(cacheKey []byte, channel Channel) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.channelCache == nil { db.channelCache = make(map[string]Channel) }
	db.channelCache[string(cacheKey)] = channel   // ← 只插入，从不按容量淘汰
}
```

**已穷举全部增删路径（`grep` 全包核实）**
- **写入**：只有 `table_channel.go:126`，即 `GetChannel` 未命中缓存后回填；以及 `batch.go:302`。
- **删除**：`forgetChannel` 仅在该频道被 Add/Update/Delete 时删**单个** key
  （`table_channel.go:88,97,106,142`、`table_subscriber.go:179`、`batch.go:305`）。
- **整体清空**：只有 `clearChannelCache`，仅在快照应用时调用（`snapshot.go:126,165`）。
- **容量上限**：**不存在**。全包 `grep channelCache` 只有上述这些点，没有任何 cap/LRU/TTL。
- `channelCacheSize()`（`db.go:118-122`）**只有测试调用**
  （`channel_cache_test.go`、`snapshot_test.go`、`subscriber_test.go`、`batch_test.go`），
  是测试探针，**不是**容量控制。

**触发路径**：节点每读取一个此前未缓存的频道元数据（`GetChannel` 未命中），
就永久新增一条缓存项，key 是 `(hashSlot, channelID, channelType)`。
频道是 IM 系统里基数最大的实体，且缓存项只会因**该频道被写入**或**快照应用**才消失。
一个长期运行、只读不写的只读型节点，缓存会一直涨到进程内存耗尽。

**后果**：按频道基数线性增长的内存泄漏。建议加 LRU 上限或 TTL。

---

## [P1] M-2. 单把非读写全局互斥锁串行化全节点频道元数据读取

**位置**：`pkg/db/meta/db.go:13` + `:99-104` + `pkg/db/meta/table_channel.go:111-127`

```go
mu sync.Mutex        // db.go:13 —— 注意是 Mutex，不是 RWMutex

func (db *MetaDB) cachedChannel(cacheKey []byte) (Channel, bool) {
	db.mu.Lock()                    // db.go:100 —— 读路径拿的是排他锁
	defer db.mu.Unlock()
	channel, ok := db.channelCache[string(cacheKey)]
	return channel, ok
}
```

`GetChannel`（`table_channel.go:111-128`）：
- `:119` 命中路径 → `cachedChannel` → **一次全局排他锁**
- `:126` 未命中路径 → 再 `rememberChannel` → **第二次全局排他锁**

**这一把 `db.mu` 同时保护四样互不相关的东西**：`shards`（`HashSlot()`，`db.go:36-47`）、
`shardLocks`（`lockForHashSlot`，`db.go:70-82`）、`channelCache`（`db.go:90-122`）、
`testLocked`（`db.go:56-66,84-88`）。

**后果**：频道元数据读取是读多写少的热路径，却全节点（跨所有 hash slot）串行在**一把非 RWMutex** 上。
这是典型的"单把全局锁保护热点读 map"。建议至少改 `RWMutex`，更好的做法是按 shard 分锁、
把 `channelCache` 与 `shards`/`shardLocks` 的锁拆开。

---

## [P2] M-3. 测试专用的锁顺序探针留在生产写路径上，额外制造全局锁争用

**这条是本次审计新发现的，不在任何扫描器输出里。**

**位置**：`pkg/db/meta/db.go:18,49-68,84-88` + `pkg/db/meta/batch.go:276`

```go
// db.go:18 —— 字段名直书 test
testLocked   []HashSlot

// db.go:49-68 —— 每锁一个 hash slot，额外做一次全局锁往返，只为维护 testLocked
func (db *MetaDB) lockHashSlots(hashSlots []HashSlot) func() {
	ordered := orderedHashSlots(hashSlots)
	for _, hashSlot := range ordered {
		lock := db.lockForHashSlot(hashSlot)   // ← 这里已经拿了一次 db.mu
		lock.Lock()
		locks = append(locks, lock)
		db.mu.Lock()                            // ← 再拿一次，纯粹为了 testLocked
		db.testLocked = append(db.testLocked, hashSlot)
		db.mu.Unlock()
	}
	return func() {
		...
		db.mu.Lock()                            // ← 解锁时再一次
		db.testLocked = nil
		db.mu.Unlock()
	}
}

// db.go:84-88 —— 每次调用都整份复制一遍切片
func (db *MetaDB) testLockedOrder() []HashSlot {
	db.mu.Lock(); defer db.mu.Unlock()
	return append([]HashSlot(nil), db.testLocked...)
}
```

**关键点**：`testLockedOrder()` 并非只被测试调用 —— 它有**一个生产调用方**：
`pkg/db/meta/batch.go:276`：`b.lastLocked = b.db.testLockedOrder()`

**后果**：一个锁住 N 个 hash slot 的批量写，为了维护测试可观测的加锁顺序，
额外付出 **2N 次 `db.mu` 排他锁往返**（`lockForHashSlot` 内已有 N 次，这里再加 N 次写 + 1 次清空），
外加 `testLockedOrder()` 在生产路径上的**一次整份切片拷贝**。
而 M-2 已经说明这把 `db.mu` 同时是频道读缓存的锁 —— 所以**批量写会直接加剧频道读的争用**，
两个本该无关的路径被这把锁耦合在一起。

**建议**：把 `testLocked` 挪到测试构建标签后面，或改为仅在测试注入的 hook 中记录；
`batch.go:276` 的 `lastLocked` 若生产逻辑确实需要，应从 `lockHashSlots` 直接返回 `ordered` 切片，
无需经由共享状态和全局锁。
