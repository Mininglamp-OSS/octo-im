# Hash-slot 元数据存储（`pkg/db/meta`）

> 只读审计，未修改任何仓库文件。

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/db/meta/compat.go | 1881 | 是 |
| pkg/db/meta/table_runtime.go | 1037 | 是 |
| pkg/db/meta/table_channel_migration.go | 769 | 是 |
| pkg/db/meta/table_runtime_meta.go | 644 | 是 |
| pkg/db/meta/inspect.go | 633 | 是 |
| pkg/db/meta/compat_channel_migration_helpers.go | 536 | 是 |
| pkg/db/meta/table_conversation.go | 481 | 是 |
| pkg/db/meta/table_hashslot_migration.go | 480 | 是 |
| pkg/db/meta/snapshot.go | 422 | 是 |
| pkg/db/meta/batch.go | 366 | 是 |
| pkg/db/meta/keys.go | 252 | 是 |
| pkg/db/meta/table_cmd_conversation.go | 247 | 是 |
| pkg/db/meta/table_plugin_binding.go | 222 | 是 |
| pkg/db/meta/table_channel.go | 221 | 是 |
| pkg/db/meta/table_subscriber.go | 216 | 是 |
| pkg/db/meta/table_key.go | 166 | 是 |
| pkg/db/meta/db.go | 122 | 是 |
| pkg/db/meta/table_user.go | 120 | 是 |
| pkg/db/meta/table_registry.go | 119 | 是 |
| pkg/db/meta/tx_helpers.go | 83 | 是 |
| pkg/db/meta/table_device.go | 83 | 是 |
| pkg/db/meta/types.go | 55 | 是 |
| pkg/db/meta/shard.go | 34 | 是 |
| pkg/db/meta/schema.go | 15 | 是 |
| **合计** | **9204** | |

> 协调者已预先确认三条（M-1 `channelCache` 无界、M-2 单把全局 `sync.Mutex` 串行化频道读、
> M-3 `testLocked` 测试探针留在生产写路径），见 `/tmp/octo-im-audit/00-coordinator-db-meta.md`，
> 本报告**不重复**这三条。

## 发现

### [P0] 1. 快照解码中 `keyLen+valueLen` uint64 溢出绕过长度校验 → 远程 raft 快照可触发切片越界 panic

- **位置**：`pkg/db/meta/snapshot.go:384-405`（校验在 :396，越界在 :399-401）
- **类别**：安全 / 远程可达 panic / 正确性
- **代码**：
  ```go
  func visitParsedSlotSnapshotPayload(meta slotSnapshotMeta, body []byte, fn func(key, value []byte) error) error {
  	for i := 0; i < meta.Stats.EntryCount; i++ {
  		keyLen, n := binary.Uvarint(body)        // :386  uint64，可到 2^64-1
  		...
  		valueLen, n := binary.Uvarint(body)      // :391  uint64
  		...
  		if uint64(len(body)) < keyLen+valueLen { // :396  ← keyLen+valueLen 无符号溢出
  			return dberrors.ErrCorruptValue
  		}
  		keyEnd := int(keyLen)                    // :399  int(0xFFFF_FFFF_FFFF_FFFC) = -4
  		valueEnd := keyEnd + int(valueLen)       // :400
  		if err := fn(body[:keyEnd], body[keyEnd:valueEnd]); err != nil {  // :401  panic
  ```
- **触发路径**：
  1. `keyLen` / `valueLen` 都是 `binary.Uvarint` 读出的**无上界** `uint64`。
     取 `keyLen = 0xFFFFFFFFFFFFFFFC`（2^64−4）、`valueLen = 8`，则
     `keyLen+valueLen` 在 uint64 下回绕为 `4`。只要 `len(body) >= 4`，:396 的校验就**通过**。
  2. `keyEnd := int(keyLen)` 得到 **−4**，`body[:−4]` 直接
     `panic: runtime error: slice bounds out of range [:-4]`。
  3. 载荷来源是**网络**：`pkg/slot/multiraft/slot.go:348` 把 etcd/raft `Ready.Snapshot`
     （即对端通过 raft transport 发来的 `MsgSnap`）的 `Data` 原样传给
     `stateMachine.Restore` → `pkg/slot/fsm/statemachine.go:646`
     `m.db.ImportHashSlotSnapshot(ctx, metadb.SlotSnapshot{Data: snap.Data})`
     → `snapshot.go:138 decodeSlotSnapshotPayload(snap.Data)` → 本函数。
     `pkg/slot/multiraft/slot.go:143`（节点启动时从本地 storage 恢复快照）是第二条入口。
  4. 唯一的前置校验是 `parseSlotSnapshotPayload:346` 的 **CRC32**——CRC32 不是 MAC，
     构造者自己算得出来，无法阻止有意构造的载荷；对随机 bit 翻转也只能以 2^-32 概率漏过。
  5. `grep -rn "recover()" pkg/slot pkg/cluster pkg/raftlog` → **零命中**，
     `slot.go:348` 的调用栈上没有任何 `recover`。
- **后果**：任一 raft 对端（或被攻陷/有 bug 的节点，或存储上被损坏的本地快照）发出的一个
  快照消息即可让接收节点**进程崩溃**；由于快照会在重启后从本地 storage 再次被
  `slot.go:143` 读取并重放，节点会进入**崩溃循环**，该 slot 上的服务永久不可用。
- **建议**：先分别用 `keyLen > uint64(len(body))` / `valueLen > uint64(len(body))-keyLen`
  这类不会溢出的形式做上界检查，再做 `int` 转换；并对单条 entry 的长度设硬上限。

### [P0] 2. 快照头部 `entryCount` 未做上界校验即用于 `make` 预分配 → 远程可触发 OOM / makeslice panic

- **位置**：`pkg/db/meta/snapshot.go:368-370` + `:329`
- **类别**：安全 / 远程可达 panic / 资源
- **代码**：
  ```go
  // parseSlotSnapshotPayload:368-370
  entryCount := binary.BigEndian.Uint64(body[:8])   // 攻击者完全控制，无任何上界检查
  body = body[8:]
  return slotSnapshotMeta{HashSlots: hashSlots,
  	Stats: SnapshotStats{EntryCount: int(entryCount), Bytes: len(data)}}, body, nil

  // decodeSlotSnapshotPayload:329
  entries := make([]snapshotEntry, 0, meta.Stats.EntryCount)   // ← 直接按声明值预分配
  ```
- **触发路径**：
  1. 同上，载荷来自 raft `MsgSnap`（`pkg/slot/multiraft/slot.go:348` → `fsm/statemachine.go:646`）。
  2. 头部只有 `len(body) < hashSlotCount*2+8`（:360）这一条长度检查，它**只覆盖 hashSlot 数组**，
     `entryCount` 与实际 body 长度之间**没有任何一致性校验**。
  3. 构造一个总长仅几十字节、但 `entryCount = 0x0000_1000_0000_0000`（2^44）的载荷，
     CRC32 自行算对：`make([]snapshotEntry, 0, 1<<44)`，`snapshotEntry` 是两个 slice header
     = 48 字节 → 试图分配 **~768 TB** → `panic: runtime error: makeslice: cap out of range`。
  4. 取 `entryCount = 0x8000_0000_0000_0001`，`int(entryCount)` 为**负数** → 同样
     `makeslice: cap out of range` panic。
  5. 取一个刚好能分配成功的中等值（如 2^28 = 2.68 亿 × 48B ≈ 12 GB）则是纯 OOM，
     进程被 OOM killer 杀掉，同样进入重启循环。
- **后果**：单个畸形快照消息即可远程使节点 panic 或 OOM。与发现 1 共用同一入口，
  且这一条**不需要**整数溢出技巧，构造门槛更低。
- **建议**：`entryCount` 必须先与剩余 body 长度做下界一致性检查
  （每条 entry 至少 2 字节 uvarint，故 `entryCount <= len(body)/2`），
  再用 `min(entryCount, 某个上限)` 做预分配；或干脆不预分配、边解码边 append。

### [P2] 3. 快照导入对**每一条 entry** 重建全部 hash-slot span 前缀，O(N·S) 次堆分配

- **位置**：`pkg/db/meta/snapshot.go:238-268`（调用点 `:157-161`）
- **类别**：性能
- **代码**：
  ```go
  for _, entry := range decoded.Entries {                                   // :157  N 条
  	if err := db.stageSlotSnapshotEntry(batch, entry, normalized, preserveMigrationMeta); err != nil {

  func (db *MetaDB) stageSlotSnapshotEntry(...) error {
  	if !snapshotEntryInHashSlots(entry.Key, hashSlots) { ... }             // :239
  	if preserveMigrationMeta && isHashSlotMigrationSnapshotKey(entry.Key, hashSlots) { ... }  // :242

  func snapshotEntryInHashSlots(key []byte, hashSlots []HashSlot) bool {
  	for _, hashSlot := range hashSlots {                                   // :251  S 个
  		for _, span := range hashSlotAllDataSpans(hashSlot) {              // :252  每次都重新构造
  ```
- **触发路径**：`hashSlotAllDataSpans` 每次调用都会构造 3 个 `Span`，每个 `Span` 经
  `encodeHashSlotSpacePrefix` → `keycodec.Builder.Key()`（`builder.go:87-89`
  `return append([]byte(nil), b.buf...)`，**必然分配**）→ `prefixSpan` →
  `keycodec.NewPrefixSpan`（`span.go:12-14`，`Start`、`End` **再各分配一次**）。
  即**每条 entry、每个 hash slot 约 9 次堆分配**，且这些前缀在整个导入过程中是**完全不变的常量**。
  `isHashSlotMigrationSnapshotKey` 再额外每条 entry 每 slot 分配一次
  `encodeHashSlotMigrationRowPrefix`。
  触发时机：raft 快照恢复（`pkg/slot/multiraft/slot.go:143,348`）与
  hash-slot 迁移（`pkg/cluster/managed_slots.go:364`）——都是节点加入 / 扩缩容 / 落后追赶的关键路径。
- **后果**：导入一个 100 万行的元数据快照会产生约 1000 万次纯属浪费的短命分配，
  全程还持有 `lockHashSlots` 拿到的 **hash-slot 独占锁**（`:145`），
  GC 压力与锁持有时间被同比放大，直接延长新节点追赶 / 迁移窗口。
- **建议**：在 `importHashSlotSnapshot` 进入 entry 循环前，把 S×3 个 span
  （以及迁移行前缀）预计算成一个切片，循环内只做 `bytes.Compare`，不再重建。

### [P1] 4. `DB.ListChannelRuntimeMeta` 无界全库扫描，且被上游按 slot 数放大成 S 次全库扫描

- **位置**：`pkg/db/meta/compat.go:604-639`
- **类别**：性能
- **代码**：
  ```go
  func (db *DB) ListChannelRuntimeMeta(ctx context.Context) ([]ChannelRuntimeMeta, error) {
  	...
  	iter, err := db.meta.engine.NewIter(engine.Span{}, engine.IterOptions{})   // :608 空 Span = 扫全库
  	...
  	var out []ChannelRuntimeMeta                                              // :613 无容量上限
  	for ok := iter.First(); ok; ok = iter.Next() {
  		key := iter.Key()
  		hashSlot, ok := isMetaRowKeyForTable(key, TableIDChannelRuntimeMeta)  // :619 逐 key 过滤
  		if !ok { continue }
  		prefix := encodeChannelRuntimeMetaRowPrefix(hashSlot)                 // :623 每条命中再分配一次前缀
  ```
- **触发路径**：
  1. `engine.Span{}` 的 `Start`/`End` 都为 nil，即 **无边界迭代器**：它会遍历整个元数据
     Pebble 实例的**每一个 key**——全部 10 张表 × 全部 hash slot 的 row / index / system 空间，
     再用 `isMetaRowKeyForTable`（`compat.go:591-600`，纯字节前缀比较）逐条丢弃不匹配的。
     即使 `ChannelRuntimeMeta` 只占全库 1% 的行，也要付出 100% 的扫描代价。
     另有 `keycodec` 的 row 前缀 `encodeRowPrefix` 完全可以直接算出 `TableIDChannelRuntimeMeta`
     的有界 span——**这个界是现成的，代码却没有用**。
  2. **上游按 slot 数放大**：`pkg/slot/proxy/store.go:166-179`
     ```go
     for _, slotID := range s.cluster.SlotIDs() {
     	groupMetas, err := s.listChannelRuntimeMetaAuthoritative(ctx, slotID)
     ```
     而 `listChannelRuntimeMetaAuthoritative`（`runtime_meta_rpc.go:73-82`）在本地服务该 slot 时
     执行 `metas, err := s.db.ListChannelRuntimeMeta(ctx)` 后再
     `filterChannelRuntimeMetaBySlot(...)` **在内存里过滤**。
     所以本节点服务 S 个 slot 时，一次 `Store.ListChannelRuntimeMeta` = **S 次完整全库扫描**，
     每次扫完再丢掉其中 (S−1)/S 的结果。复杂度 O(S × 全库行数)。
  3. 远程入口：`pkg/slot/proxy/runtime_meta_rpc.go:224` 的 `runtimeMetaRPCList` 处理器，
     每收到一个对端节点的 list 请求就触发一次全库扫描。
  4. 结果切片 `out` 无 limit、无游标——同包内明明已经有分页版
     `ListChannelRuntimeMetaPage`（`table_runtime_meta.go`，`runtime_meta_rpc.go:349` 在用），
     这个全量版是没有必要的第二条路径。
- **后果**：随频道总数与本节点 slot 数**二次方**增长的 CPU / Pebble block-cache 冲刷；
  同时一次性把全部运行时元数据读进内存（无上限），大集群上是 GC 尖峰 + 可能的 OOM。
  由于它同时挂在远程 RPC 处理器上，一个对端节点的轮询即可放大成本节点的全库扫描。
- **建议**：用 `encodeRowPrefix(hashSlot, TableIDChannelRuntimeMeta)` 构造有界 span；
  给 API 加 hashSlot 参数与 limit，让 proxy 的 per-slot 循环只扫自己那一段，
  或直接让上游改用已有的 `ListChannelRuntimeMetaPage`。

### [P2] 5. `DB.ListHashSlotMigrationStates` 同样是无界全库扫描

- **位置**：`pkg/db/meta/compat.go:690-717`
- **类别**：性能
- **代码**：
  ```go
  func (db *DB) ListHashSlotMigrationStates(ctx context.Context) ([]HashSlotMigrationState, error) {
  	...
  	iter, err := db.meta.engine.NewIter(engine.Span{}, engine.IterOptions{})  // :694 同样空 Span
  	...
  	for ok := iter.First(); ok; ok = iter.Next() {
  		key := iter.Key()
  		hashSlot, ok := isMetaRowKeyForTable(key, TableIDHashSlotMigration)
  		if !ok || !bytesHasPrefix(key, encodeHashSlotMigrationStateKey(hashSlot)) {   // :703 每 key 再分配一次
  			continue
  ```
- **触发路径**：迁移状态行在稳态下**几乎为空**（每个 hash slot 至多一行），
  却要为了找到它们扫过整个元数据库的每一个 key。
  调用方 `pkg/cluster/hashslot_migration.go:549` 与 `pkg/cluster/cluster.go:1995` 都在
  集群协调循环里周期性调用。更糟的是 `:703` 对**每一条被跳过的 key** 都调用
  `encodeHashSlotMigrationStateKey(hashSlot)`——一次 `keycodec.Builder.Key()` 堆分配
  （`builder.go:87-89`）——即"为了丢弃它而分配"。
- **后果**：周期性协调任务在稳态下也要付出全库扫描 + 每 key 一次分配的代价，
  代价随元数据总量增长而与迁移状态数量无关。
- **建议**：对每个已知 hash slot 直接 `get(encodeHashSlotMigrationStateKey(slot))`，
  或至少把 span 收窄到 `TableIDHashSlotMigration` 的 row 前缀；
  循环内的前缀应提到循环外按 hashSlot 缓存。

### [P1] 6. `WriteBatch.stageSubscribers` 绕过批内 overlay 直读已提交存储，同批次的 `UpsertChannel` / `DeleteChannel` 会被静默覆盖或复活

- **位置**：`pkg/db/meta/compat.go:981-1028`（关键在 :990-1002 与 :1017-1022）
- **类别**：正确性 / 分布式一致性
- **代码**：
  ```go
  b.batch.addOp(hs, func(ctx context.Context, state *batchCommitState, batch *engine.Batch) error {
  	primaryKey := encodeChannelRowKey(hs, channelID, channelType, channelPrimaryFamilyID)
  	value, ok, err := state.db.get(primaryKey)          // ← 读 *已提交* 存储，绕过所有 overlay
  	...
  	channel := Channel{ChannelID: channelID, ChannelType: channelType}
  	if ok { channel, err = decodeChannelValue(channelID, channelType, value) ... }
  	if mutationVersion > 0 {
  		if ok && channel.SubscriberMutationVersion > mutationVersion { return dberrors.ErrConflict }
  		channel.SubscriberMutationVersion = mutationVersion
  	}
  	...
  	if mutationVersion > 0 {
  		if err := (&Shard{db: state.db, hashSlot: hs}).stageChannel(batch, primaryKey, channel); err != nil { ... }
  		state.channelPublishes[string(primaryKey)] = channel
  		delete(state.channelDeletes, string(primaryKey))   // ← 撤销同批次的删除标记
  	}
  ```
- **触发路径**：
  1. `state.db.get` 走 `tx_helpers.go:30-35` → `engine.DB.Get` → `pdb.Get`
     （`pkg/db/internal/engine/db.go:54-66`），读的是**已提交的 Pebble DB**；
     同一 `*pebble.Batch` 里尚未提交的 `Set`/`Delete` **对它不可见**。
     而 `batchCommitState` 明明为此准备了 `channelPublishes` / `channelDeletes`
     （`batch.go:53-54`）——`stageSubscribers` 一个都没查。
  2. `pkg/slot/fsm/statemachine.go:174` 的 `ApplyBatch` 对**一批 raft 命令共用一个
     `WriteBatch`**：`wb := m.db.NewWriteBatch()`，然后 `for i, cmd := range cmds { decoded.apply(wb, hashSlot) }`。
     `upsertChannelCmd`（`command.go:268`→`wb.UpsertChannel`）与
     `addSubscribersCmd`（`command.go:323`→`wb.AddSubscribers`）是两条独立命令，
     一次 raft `Ready` 同时提交两者即落入同一个 `WriteBatch`。
  3. **方向 A（先 Upsert 后 AddSubscribers）**：`UpsertChannel` 的 op 先
     `stageChannel(batch, primaryKey, newChannel)`（`table_channel.go:157-173`，
     `batch.Set(primaryKey, value)` **整行覆盖**全部 5 个字段）；
     随后 `stageSubscribers` 的 op 读到**批前的旧行**，再次整行 `Set`。
     → `UpsertChannel` 带来的 `Ban` / `Disband` / `SendBan` / `AllowStranger` 变更**全部丢失**。
  4. **方向 B（先 AddSubscribers 后 Upsert）**：`UpsertChannel` 后写，
     把 `stageSubscribers` 刚写入的 `SubscriberMutationVersion` 回退成命令自带的值。
     该字段正是 `:997` 的**订阅者变更防回退栅栏**，栅栏被静默重置后，
     一条本该被 `ErrConflict` 拒绝的陈旧订阅者变更会在下一批被接受。
  5. **方向 C（同批次 `DeleteChannel` + `AddSubscribers`）**：`DeleteChannel`
     （`compat.go:889-910`）已 `batch.Delete(primaryKey)`、删 channel-id 索引、
     `DeleteRange` 掉整个订阅者前缀、并置 `state.channelDeletes`；
     随后 `stageSubscribers` 读到**仍然存在的已提交行**，`stageChannel` 把主行和索引
     **重新 Set 回来**，把订阅者键 Set 在 DeleteRange 之后（Pebble 批内按序生效，后写者胜），
     还 `delete(state.channelDeletes, ...)` 把删除标记抹掉、改成 `channelPublishes`。
     → **已删除的频道连同订阅者一起复活，并被写回频道缓存**。
- **后果**：一次 raft 提交内的元数据变更互相覆盖。由于所有副本执行同样的顺序，
  各副本状态仍然**一致地错误**（不是副本分歧），但相对客户端语义是**静默数据丢失 /
  已删除频道复活 / 订阅者版本栅栏失效**，且没有任何错误返回。
- **建议**：`stageSubscribers` 读取频道行时应先查 `state.channelPublishes` /
  `state.channelDeletes`（必要时增加 channel 的 `tableRowOverlay`），
  与 `loadRuntimeMeta`（`batch.go:333-344`）、`loadChannelMigrationTask`（`:346-357`）
  已有的 read-your-writes 模式保持一致。

### [P2] 7. 迁移任务 GC 用主键序全表扫描代替已有的 `completedAt` 有序索引：最老的任务可能永远不被回收，且返回的"已删除数"是提交前的估算值

- **位置**：`pkg/db/meta/compat.go:1753-1808`
- **类别**：正确性 / 无界增长 / 性能
- **代码**：
  ```go
  func (b *WriteBatch) DeleteTerminalChannelMigrationTasksBefore(hashSlot uint16, req ChannelMigrationTaskGCRequest) (int, error) {
  	...
  	planned, err := b.db.ForHashSlot(hashSlot).shard.CountTerminalChannelMigrationTasksBefore(
  		context.Background(), req.BeforeMS, req.Limit)      // :1758 锁外、提交前、丢弃调用方 ctx
  	...
  	b.batch.addOp(hs, func(ctx context.Context, st *batchCommitState, batch *engine.Batch) error {
  		tasks, err := shard.ListChannelMigrationTasks(ctx)   // :1765 主键序全表扫描，limit=0
  		...
  		for _, task := range tasks {
  			if deleted >= req.Limit { break }                // :1770 按 channelID 序取前 Limit 个
  			if !task.IsTerminal() || task.CompletedAtMS >= req.BeforeMS { continue }
  			... batch.Delete(taskKey) ... deleted++          // deleted 只在闭包内，最终被丢弃
  		}
  		return nil
  	})
  	return planned, nil                                      // :1807 返回的是估算值，不是实际删除数
  ```
- **触发路径**：
  1. **顺序不一致**：`CountTerminalChannelMigrationTasksBefore`（`table_channel_migration.go:431-461`）
     走的是 `encodeChannelMigrationTerminalIndexPrefix`，键布局
     `KeyLayout{KeyInt64Ordered(completedAt), ...}`（`table_channel_migration.go:617-624`）
     —— 即 **completedAt 升序**，"最老优先"，遇到 `completedAt >= beforeMS` 即 `break`。
     而实际删除走 `ListChannelMigrationTasks`（`table_channel_migration.go:392-398`）
     的 `scanPrimary(..., 0, ...)`，是 **(channelID, channelType, taskID) 主键序**。
  2. **饥饿**：当符合条件的终态任务数超过 `req.Limit` 时，每轮 GC 都只删掉
     **channelID 字典序最小**的那 `Limit` 个。若某个 channelID 靠前的频道持续产生终态任务，
     字典序靠后的频道的终态任务**永远不会被回收**，其主行与终态索引项无限累积。
     那条为"按时间回收"而专门维护的 `idx_channel_migration_terminal` 索引在删除路径上完全没被使用。
  3. **返回值不可信**：`planned` 在 `addOp` 之前、**未持 hash-slot 锁**时计算
     （`Shard.lock()`/`lockHashSlots` 要到 `Batch.Commit` 的 `batch.go:275` 才获取）；
     真正的删除发生在提交时。闭包内算出的真实 `deleted` 被丢弃。
     该值经 `pkg/slot/fsm/channel_migration_cmds.go:125-136`
     （`c.deleted = deleted` → `applyResult()` → `EncodeGarbageCollectTerminalChannelMigrationTasksResult`）
     作为 **raft apply 结果**返回给提案方，即上层拿到的回收计数与实际落盘行为可能不符。
  4. **全表扫描**：`ListChannelMigrationTasks` 无 limit，在**持有 hash-slot 独占锁的提交路径内**
     把该 slot 的全部迁移任务解码进内存（每条都要 `json.Unmarshal`，
     见 `table_channel_migration.go:735-745`），只为挑出其中的终态子集。
  5. `:1758` 的 `context.Background()` 丢弃了调用方的取消/超时。
- **后果**：GC 在有 `Limit` 时不是"最老优先"，可导致部分频道的终态迁移任务行与索引项无限增长；
  上报的回收数量不准确；每轮 GC 在锁内做一次全表 JSON 解码。
- **建议**：删除路径改为正向 seek `idx_channel_migration_terminal`（与 count 路径同一顺序），
  取前 `Limit` 条，同时用真实删除计数作为返回值；`context.Background()` 换成调用方 ctx。

### [P1] 8. hash-slot 迁移的 applied-delta 去重行永不回收，且被快照策略主动保留 → 磁盘上的永久单调增长

- **位置**：`pkg/db/meta/table_hashslot_migration.go:135-152, 196-213, 31-34` + `pkg/db/meta/keys.go:157-166`
- **类别**：无界增长 / 资源
- **代码**：
  ```go
  // table_hashslot_migration.go:135-152 —— 每应用一条 delta 就写一行，value 为空
  func (s *Shard) MarkAppliedHashSlotDelta(ctx context.Context, delta AppliedHashSlotDelta) error {
  	...
  	if err := batch.Set(encodeAppliedHashSlotDeltaKey(delta), nil); err != nil { return err }
  	return batch.Commit(true)
  }

  // table_hashslot_migration.go:31-34 —— 该表的快照策略是「导入时保留本地行」
  var hashSlotMigrationTable = registerMetaTable(TableSpec[HashSlotMigrationState]{
  	ID:             TableIDHashSlotMigration,
  	Name:           "hashslot_migration",
  	SnapshotPolicy: SnapshotPolicy{PreserveOnImport: true},
  ```
- **触发路径**：
  1. 每当目标 slot 应用一条迁移 delta，`pkg/slot/fsm/statemachine.go:259-261`
     执行 `wb.MarkAppliedHashSlotDelta(appliedDeltaRecord)`，落一行
     key = `rowPrefix(hashSlot, TableIDHashSlotMigration) || 0x02 || sourceSlot(8B) || sourceIndex(8B)`
     （`keys.go:157-166`），value 为空。一次 hash-slot 迁移要重放 N 条源日志 → **N 行**。
  2. **全仓穷举回收路径，结果为零**：`grep -rn "DeleteAppliedHashSlotDelta|ListAppliedHashSlotDeltas"`
     在 `pkg/db/meta/` **之外**、非测试文件中 **零命中**；包内的
     `compat.go:666, 765, 1216` 只是 `DB`→`ShardStore`→`Shard` 的纯转发壳，没有任何真实调用方。
     记录类型字节 `hashSlotMigrationRecordAppliedDelta`（0x02）在全仓只出现在
     `table_hashslot_migration.go:16` 与 `keys.go:159`，**没有任何 `DeleteRange` 覆盖这个子空间**。
  3. 迁移收尾的两个清理动作都**够不到**这些行：
     `DeleteHashSlotMigrationState` 只删记录类型 `0x01` 的状态行
     （`keys.go:152-155`），`DeleteAllHashSlotMigrationOutbox` 只
     `DeleteRange` 记录类型 `0x03` 的 outbox 子空间（`table_hashslot_migration.go:354-368`）。
  4. **快照也清不掉**：该表声明 `PreserveOnImport: true`，于是
     `hashSlotSnapshotReplaceSpans`（`snapshot.go:217-228`）在保留式导入时
     把整个 `TableIDHashSlotMigration` 行前缀从 `DeleteRange` 列表中**排除**，
     且 `stageSlotSnapshotEntry`（`snapshot.go:242-246`）对本地已存在的同 key 直接跳过。
     所以即使节点做一次完整的 slot 快照重同步，历史 applied-delta 行依然原封不动。
     唯一能清除它们的只有 `DeleteHashSlotData`（`snapshot.go:108`），
     而那是"整个 hash slot 下线"时才走的路径。
  5. 与此同时 `ListAppliedHashSlotDeltas`（`:167-194`）**没有 limit 参数**，
     一次性把该 hash slot 的全部去重行读进内存（`make(..., 0, 8)` 只是初始容量）。
- **后果**：每经历一次 hash-slot 迁移（扩容 / 缩容 / 再平衡），该 hash slot 的元数据
  就永久新增 N 行去重记录，N = 本次迁移重放的源日志条数。反复再平衡的集群上，
  这些行数与集群生命周期内的迁移总量成正比，永不下降，
  持续占用磁盘、拖慢该 hash slot 行空间上的所有范围扫描（包括快照导出，
  见 `snapshot.go:66 hashSlotAllDataSpans` 会把它们全部序列化进每一次快照），
  并因此让每次快照体积也单调增长。
- **旁证（与兄弟单元 22 的交叉确认）**：`pkg/slot/fsm/statemachine.go:53-54,87,794`
  的内存态 `appliedDelta map[deltaReplayKey]struct{}` 确实**只写不读**
  （全仓只有 `m.appliedDelta[key] = struct{}{}` 一处写入，无任何读取）；
  真正生效的去重是 `statemachine.go:242` 的 `m.db.HasAppliedHashSlotDelta`，
  即本处的**持久化路径**。所以这些行不能简单删掉了事——需要一个带水位的裁剪策略。
- **建议**：迁移进入 `hashSlotMigrationPhaseDone` 后，按 `(sourceSlot, sourceIndex <= FenceIndex)`
  对 `encodeAppliedHashSlotDeltaPrefix` 子空间做 `DeleteRange` 裁剪
  （去重只需覆盖尚可能重放的窗口）；并给 `ListAppliedHashSlotDeltas` 加 limit/游标。

### [P1] 9. 存储实际顺序是「长度优先」，但本包的文档承诺「UID 升序」且写路径用 `sort.Strings`（内容优先）—— 两个相反的比较器共存于同一文件

- **位置**：`pkg/db/meta/table_subscriber.go:82,99,214` + `pkg/db/meta/table_user.go:77` +
  `pkg/db/meta/table_key.go:69-73`（经 `pkg/db/internal/keycodec/codec.go:46-53`）
- **类别**：正确性 / 架构与文档一致性
- **代码**：
  ```go
  // pkg/db/internal/keycodec/codec.go:46-53 —— 字符串键部件：先写 2 字节大端长度，再写内容
  func AppendString(dst []byte, value string) []byte {
  	if len(value) > maxStringLen { panic(...) }
  	dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))   // ← 长度前缀在内容之前
  	return append(dst, value...)
  }

  // pkg/db/meta/table_subscriber.go:198-216 —— 写路径按 Go 字典序排序
  func normalizeSubscriberUIDs(uids []string) ([]string, error) {
  	...
  	sort.Strings(out)          // :214  内容优先（"aa" < "z"）
  	return out, nil
  }

  // pkg/db/meta/table_subscriber.go:82,99 与 table_user.go:77 —— 文档承诺的读顺序
  // SnapshotSubscribers returns all subscribers in stable UID order.
  // ListSubscribersPage returns subscribers after cursorUID in stable UID order.
  // ListUsersPage returns users after cursorUID in ascending UID order.
  ```
- **触发路径**：
  1. subscriber 主键布局是 `KeyLayout{KeyString, KeyInt64Ordered, KeyString}`
     （`table_subscriber.go:31`），UID 那一段经 `encodeKeyParts` 的
     `case KeyString: keycodec.AppendString(...)`（`table_key.go:69-73`）编码。
     因此 Pebble 的字节序把 **长度前缀排在内容之前** → 实际迭代顺序是
     **先按 UID 长度、再按内容**。具体地：UID `"z"` 编码为 `00 01 7a`，
     `"aa"` 编码为 `00 02 61 61` → 存储中 **`"z"` 排在 `"aa"` 之前**，
     而 Go 的 `"z" < "aa"` 为 **false**。两者对普通输入就相反。
  2. `ListSubscribersPage`（`:100-126`）与 `ListUsersPage`（`table_user.go:78`）
     的分页正是沿这个长度优先顺序推进（`scanPrimary` → `span.Start = PrefixEnd(afterKey)`，
     `table_runtime.go:670-677`）。所以它们返回的**既不是**字典序，
     也不是文档所写的"ascending UID order"。
  3. 同一个文件的写路径 `normalizeSubscriberUIDs` 却用 `sort.Strings` ——
     即 `pkg/db/meta` 内部**同时存在两个互相矛盾的 UID 比较器**：
     写入时按内容优先排序，读出时按长度优先返回。
  4. 这个契约被跨节点消费：`pkg/slot/proxy/subscriber_rpc.go:44,191`
     （`ListSubscribersPage`，游标是裸 `afterUID string`）、
     `:65,180`（`ListSubscribersSnapshot`）、
     `pkg/slot/proxy/identity_rpc.go:224`（`ListUsersPage`）。
     兄弟单元 23 已核实 proxy 的分页归并堆用 Go 字符串 `<` 比较 UID ——
     即它采用的正是文档字面承诺的那个顺序，与存储实际顺序相反，
     导致**分页列表静默丢行**。本条从存储侧确认了根因：
     契约描述与编码顺序不一致，且导出的游标类型（`UserCursor{UID string}`、
     `ChannelCursor{ChannelID string}`、`ListSubscribersPage` 的 `afterUID string`）
     是裸字符串，天然诱导调用方用 `<` 比较。
  5. 本包内部的分页本身是自洽的（`SnapshotSubscribers` 把 `next` 原样回灌，
     `ListSubscribersPage` 再编码成键，始终沿同一顺序），
     所以**单 shard 读取不丢行**——缺陷只在跨 shard/跨 slot 归并时显现，
     这也正是它长期没被发现的原因。
- **后果**：任何按"UID 升序"做跨分片归并、二分查找或去重的调用方都会出错；
  已实际导致 proxy 分页列表丢行。同时写路径的 `sort.Strings` 对存储布局毫无作用
  （既不是存储顺序，也不影响批内正确性），是误导性的伪保证。
- **建议**：把文档改成准确描述（"encoded key order: length-then-content"），
  并把跨分片归并所需的比较器以显式函数形式从本包导出
  （或让 `keycodec` 提供 `CompareString`），禁止调用方自行用 `<`；
  若确实需要字典序，应改用不带长度前缀的可比较字符串编码。

### [P2] 10. `stageUniqueIndexChecks` 对每一次批内写入遍历并解码**全部** overlay 行，而本包没有任何表声明唯一索引 —— 批量写 O(N²) 纯浪费

- **位置**：`pkg/db/meta/table_runtime.go:811-892`（关键在 :823-848）
- **类别**：性能 / 死代码
- **代码**：
  ```go
  	if state != nil {
  		rowPrefix := encodeRowPrefix(hashSlot, t.spec.ID)
  		for primaryKey, overlay := range state.tableRows {          // :824 遍历全部 overlay 行
  			if !overlay.exists { continue }
  			overlayPK, ok := t.decodePrimaryRowKey(rowPrefix, []byte(primaryKey))
  			if !ok || overlayPK.Equal(pk) { continue }
  			overlayRow, err := t.decodeValue([]byte(primaryKey), overlayPK, overlay.value)  // :833 无条件解码
  			if err != nil { return err }
  			for _, index := range t.spec.Indexes {
  				if index.DescriptorOnly { continue }
  				if !index.Unique { continue }                        // :841 唯一性判断在解码之后
  ```
- **触发路径**：
  1. `stageUniqueIndexChecks` 被 `stageWrite`（`table_runtime.go:563`）在**每一次**
     `StageCreate`/`StageUpdate`/`StageUpsert` 的提交闭包里调用。
  2. `:841` 的 `if !index.Unique { continue }` 位于**遍历和解码之后**。
     `state.tableRows` 随批次内已执行的写入单调增长（`table_runtime.go:572`
     每次成功写入都 `state.tableRows[string(primaryKey)] = ...`），
     所以一个含 N 次同表写入的批次，第 k 次写入要遍历 k−1 条 overlay
     并对其中同表的每一条做一次完整 `decodeValue` → 总计 **O(N²) 次解码**。
  3. **而这些解码的结果永远用不上**：全包 `grep -rn "Unique" pkg/db/meta/table_*.go`
     （排除 `table_runtime.go` 自身的 schema 管线）**零命中** ——
     `table_channel.go:50-70`、`table_channel_migration.go:54-62`、
     `table_conversation.go`、`table_plugin_binding.go` 等所有 `IndexSpec` 字面量
     都没有设置 `Unique`，默认为 `false`。因此 `:841` **总是 continue**，
     整个 overlay 循环的唯一效果就是浪费 CPU。
  4. 触发场景：`pkg/slot/fsm/statemachine.go:173` 的 `ApplyBatch` 把一个 raft `Ready`
     里的全部命令攒进同一个 `WriteBatch`。一批 500 条 `upsertUserCmd`
     （`command.go:238` → `userTable.StageUpsert`）就会产生约 500²/2 ≈ 12.5 万次
     无用的 user 行解码，全部发生在**持有 hash-slot 独占锁的提交路径内**。
- **后果**：raft 批量应用的 CPU 与锁持有时间随批大小平方增长；
  批越大（正是吞吐高峰时）惩罚越重，形成反向的批处理收益。
- **建议**：把 `if !index.Unique` 的判断提到函数入口——先确认该表存在唯一索引，
  否则直接返回；overlay 循环也应改为按索引元组建索引，而非逐行全量比对。

### [P0] 11. `decodeUint64Slice` 的 `count*8` uint64 溢出绕过长度一致性校验 → 经快照导入植入的 runtime-meta 行可触发 `makeslice` panic

- **位置**：`pkg/db/meta/table_runtime_meta.go:593-608`（校验在 :599，分配在 :602）
- **类别**：安全 / 远程可达 panic
- **代码**：
  ```go
  func decodeUint64Slice(value []byte) ([]uint64, error) {
  	count, n := binary.Uvarint(value)          // :594 攻击者控制的 uint64，无上界
  	if n <= 0 { return nil, dberrors.ErrCorruptValue }
  	value = value[n:]
  	if uint64(len(value)) != count*8 {         // :599 count*8 在 uint64 下回绕
  		return nil, dberrors.ErrCorruptValue
  	}
  	out := make([]uint64, 0, count)            // :602 按声明值预分配
  ```
- **触发路径**：
  1. 取 `count = 2^61 + 1`（合法 uvarint）。则 `count*8 = 2^64 + 8` 在 uint64 下
     **回绕为 8**。只要 uvarint 之后恰好还剩 8 字节，:599 的一致性校验就**通过**。
  2. `make([]uint64, 0, 2^61+1)` 需要约 2^64 字节 →
     `panic: runtime error: makeslice: cap out of range`。
     （`for len(value) > 0` 的循环只会跑 1 次，但 panic 已经发生在 `make`。）
  3. **可达性——经由快照导入植入任意字节**：`decodeUint64Slice` 由
     `decodeRuntimeMetaColumn`（`table_runtime_meta.go:502-517`，
     `runtimeMetaColumnReplicas` / `runtimeMetaColumnISR` 两个分支）调用，
     即解码存储中的 `ChannelRuntimeMeta` 行值时触发。
     正常写入路径 `encodeChannelRuntimeMetaValue` 永远不会产生这种值，
     但 `ImportHashSlotSnapshot` **不做任何值级校验**：
     `snapshot.go:238-248 stageSlotSnapshotEntry` 只验证 key 落在本次 hash-slot 的
     span 内，随后 `batch.Set(entry.Key, entry.Value)` **原样写入攻击者提供的 value**。
     快照 `Data` 来自 raft `MsgSnap`（`pkg/slot/multiraft/slot.go:348`
     → `pkg/slot/fsm/statemachine.go:646`）。
  4. **连 CRC 都不用算**：`rowcodec.Unwrap`（`pkg/db/internal/rowcodec/envelope.go:56-67`）
     只在 `flags&FlagChecksum != 0` 时才校验校验和 —— `flags` 就是 value 的第 3 个字节，
     由攻击者控制，置 0 即**完全跳过校验**。
     即便要过校验，`envelopeChecksum`（`envelope.go:76-84`）也只是
     **无密钥的 CRC32C**，构造者自行算得出来。
     `decodeChannelRuntimeMetaValue` 之后只检查 `env.Version` 与 `env.Codec`
     两个同样由攻击者控制的字节。
  5. 植入后，任何一次读取该频道 runtime meta 的操作都会 panic：
     `GetChannelRuntimeMeta`、`ListChannelRuntimeMetaPage`，
     以及会扫到它的 `DB.ListChannelRuntimeMeta`（`compat.go:604`，全库扫描，
     见发现 4 —— 它会遍历到每一行，因此**一行毒数据即可让全量列表接口永久 panic**）。
     调用栈上无 `recover`（`grep -rn "recover()" pkg/slot pkg/cluster pkg/raftlog` 零命中）。
- **后果**：一条畸形快照即可在受害节点磁盘上留下**持久化**的毒行；此后每次读取
  （包括重启后的元数据同步与管理接口列表）都 panic ——
  与发现 1、2 的一次性崩溃不同，这一条是**持久化的崩溃循环**，
  且只能通过手工删除该行或 `DeleteHashSlotData` 恢复。
- **建议**：`:599` 改为不会溢出的形式（先 `count > uint64(len(value))/8` 拒绝，再比较相等），
  并给 `count` 加硬上限（副本数量级）；更根本的是
  `stageSlotSnapshotEntry` 在写入前应对每条 entry 做**值级解码校验**，
  而不是把对端字节直接 `Set` 进本地存储；`rowcodec.Unwrap` 也不应允许
  由 value 自身的 `flags` 字节决定是否校验校验和。

### [P2] 12. `StorePrimaryValue` 把整条频道行复制进每个 channel-id 索引项，而**全仓没有任何代码读取索引项的值** —— 纯写放大

- **位置**：`pkg/db/meta/table_channel.go:56-58` + `pkg/db/meta/table_runtime.go:736-744`
  （对照 `table_runtime.go:307-380` 索引扫描路径）
- **类别**：性能 / 死代码
- **代码**：
  ```go
  // table_channel.go:50-62 —— channel_id 索引声明把行值也存进索引项
  {
  	ID:      channelIDIndexID,
  	Name:    "idx_channel_id",
  	...
  	// Keep channel bytes in index values for old channel-index readers.
  	StorePrimaryValue: true,          // :58

  // table_runtime.go:736-744 —— 写入时整份拷贝行值
  		var indexValue []byte
  		if index.StorePrimaryValue {
  			indexValue = append([]byte(nil), value...)   // :740
  		}
  		if err := batch.Set(key, indexValue); err != nil {
  ```
- **触发路径**：
  1. 注释声称这是给 "old channel-index readers" 保留的。**但这样的 reader 不存在**：
     `grep -rn "iter.Value()" pkg/db/meta/*.go`（非测试）只有 6 处 ——
     `compat.go:559`（`listChannelsPage`，主行）、`compat.go:628`/`:709`（两个全库扫描，主行）、
     `snapshot.go:76`（快照导出，逐字节搬运，不解释语义）、
     `table_hashslot_migration.go:288`（outbox 行）、
     `table_runtime.go:682`（`scanPrimary`，**主行**）。
     **索引扫描函数 `scanIndexWithOptions`（`table_runtime.go:333-380`）从头到尾没有调用 `iter.Value()`** ——
     它只解码索引 key，然后用 `getByPrimaryKey`（`:363`）回主行点查。
  2. 同时 `grep -rn "channelIDIndex|encodeChannelIDIndex"` 在 `pkg/db/meta` **之外零命中**，
     没有任何外部包直接读这个索引的字节。
     包内唯一的读者 `ListChannelsByChannelID`（`table_channel.go:154`）走的是
     `ScanIndexAll` → 同样是 key 解码 + 主行点查。
  3. 于是每次 `UpsertChannel` / `CreateChannel` / `UpdateChannel`，以及每次
     `stageSubscribers` 的 `stageChannel`（见发现 6），都把
     `encodeChannelValue` 产生的 40 字节行值**额外写一份**进索引项，
     Pebble 侧则是一份额外的 WAL 记录 + memtable 条目 + compaction 输入。
  4. 顺带：`scanIndexWithOptions` 的索引→主行连接对每条索引项做一次 `engine.Get`
     （`table_runtime.go:363`），而索引项里明明已经带着行值却不用。
     `ListUserConversationActive`（`table_conversation.go:290-302`，
     IM 客户端登录/同步时拉会话列表的热路径）就走这条路：
     limit=100 时是 100 次索引迭代 + **100 次额外点查与解码**。
- **后果**：频道元数据写入的字节量与 compaction 量约翻倍，收益为零；
  同时索引扫描本可零点查却做了 N 次点查。两者方向相反地各自浪费一遍。
- **建议**：要么去掉 `StorePrimaryValue`（确认无外部读者后），
  要么让 `scanIndexWithOptions` 在 `index.StorePrimaryValue` 时直接用
  `iter.Value()` 解码、跳过 `getByPrimaryKey`。二者必须选一个，现状是最差组合。

### [P0] 13. `User.Token` / `Device.Token` 长度完全未校验，`appendValueString` 在超长时 `panic` —— 一个 HTTP 请求即可让整个 slot 复制组进入永久崩溃循环

- **位置**：`pkg/db/meta/table_user.go:34,96-101` + `pkg/db/meta/table_device.go:60-62,64-68`
  + `pkg/db/meta/tx_helpers.go:51-53`（panic 发生在 `pkg/db/internal/keycodec/codec.go:46-50`）
- **类别**：安全 / 远程可达 panic / 可用性
- **代码**：
  ```go
  // table_user.go:34 —— Validate 只校验 UID，Token 一个字都没查
  	Validate: func(user User) error { return validateKeyString(user.UID) },

  // table_user.go:96-101
  func encodeUserValue(user User) []byte {
  	value := appendValueString(nil, user.Token)     // :97
  	...

  // table_device.go:60-68 —— 同样只校验 UID
  func validateDevice(device Device) error { return validateKeyString(device.UID) }
  func encodeDeviceValue(device Device) []byte {
  	value := appendValueString(nil, device.Token)   // :65
  	...

  // tx_helpers.go:51-53 —— 直通到会 panic 的原语
  func appendValueString(dst []byte, value string) []byte { return keycodec.AppendString(dst, value) }

  // pkg/db/internal/keycodec/codec.go:46-50
  func AppendString(dst []byte, value string) []byte {
  	if len(value) > maxStringLen {                  // maxStringLen = 1<<16 - 1 = 65535
  		panic(fmt.Sprintf("keycodec: string key part too long: %d", len(value)))
  ```
- **触发路径**（逐步）：
  1. 向更新 token 的 HTTP 接口 POST 一个 `token` 字段长度 > 65535 的 JSON。
     `internal/access/api/user_token.go:11-16` 的 `updateTokenRequest.Token` 是裸 `string`，
     `ShouldBindJSON` **不做任何长度校验**；`:28-32` 直接把 `req.Token` 传下去。
  2. `internal/usecase/user/token.go:40-45`：
     `a.devices.UpsertDevice(ctx, metadb.Device{UID: cmd.UID, Token: cmd.Token, ...})`
     —— 全程没有长度检查。
  3. `pkg/slot/proxy/store.go:215-220`：`UpsertDevice` **不是本地写，而是提交 raft 提案**
     （`cmd := metafsm.EncodeUpsertDeviceCommand(d)` → `proposeWithHashSlot`）。
     `pkg/slot/fsm/command.go:461-468` 的编码器按 `tokenLen := len(d.Token)`
     分配缓冲，**同样没有上界检查**，所以超长 token 被成功编码、复制并**持久化进 raft 日志**。
  4. 提交后，**每个副本**在 FSM apply goroutine 里执行
     `ApplyBatch` → `upsertDeviceCmd.apply`（`command.go:258`）→
     `wb.UpsertDevice` → `deviceTable.StageUpsert` → `stageWrite`
     （`table_runtime.go:526-540`）：
     `Validate`（只查 UID）通过 → `t.encodeValue(primaryKey, row)` →
     `encodeDeviceValue` → `appendValueString` → **panic**。
  5. panic 发生在 **FSM apply goroutine**，不是 gin 的 HTTP handler goroutine，
     所以 gin 的 `Recovery()` 中间件救不了。全仓非测试代码只有 **两处** `recover()`
     （`internal/access/plugin/handlers_lifecycle.go:48`、
     `internal/runtime/channelmeta/activate.go:41`），**都不在 raft apply 栈上**。
  6. 由于该 raft 日志条目已经**持久化且已提交**，节点重启后会**重放同一条目并再次 panic**
     → 该 slot 的**所有副本**进入永久崩溃循环，只能靠手工删日志恢复。
  7. `User.Token` 走 `upsertUserCmd`（`command.go:238`）经同样路径；
     `internal/usecase/management/users.go:344` 的管理面重置 token 亦然。
- **关键不对称性（说明这是疏漏而非设计）**：**键**路径已经防住了同一个原语 ——
  `encodeKeyParts`（`table_key.go:69-73`）在 `case KeyString` 里先
  `if len(part.S) > maxKeyStringLen { return nil, dberrors.ErrInvalidArgument }`
  **再**调用 `AppendString`；而**值**路径的 `appendValueString`（`tx_helpers.go:51-53`）
  是无保护直通。`validateKeyString` 这个现成的检查函数就在同一文件里（`:44-49`），
  只是没有人对 Token 调用它。
- **后果**：单个未认证/低权限可达的 HTTP 请求（取决于该接口的鉴权配置）
  即可造成**集群级永久拒绝服务** —— 不是一次崩溃，而是落盘的崩溃循环。
  这是本分片中危害最高的一条。
- **建议**：在 `validateDevice` / userTable 的 `Validate` 中对 `Token`
  （以及所有进入 `appendValueString` 的值字符串）调用长度检查并返回
  `ErrInvalidArgument`；同时把 `appendValueString` 改成返回 error 的形式，
  让"值超长"在**提案之前**就被拒绝；入口层也应对 token 长度设合理上限。

### [P3] 14. 8 个 U1000 死函数逐条解释：其中两条是"宽松版/流式版"被弃用、一条是恒等的空操作

- **类别**：死代码 / 架构
- **逐条说明**（每条都已 `grep` 全仓确认无非测试调用方）：

  | 位置 | 函数 | 它是什么 / 为什么值得注意 |
  |---|---|---|
  | `compat.go:1739` | `clearRuntimeFence` | 与 `compat_channel_migration_helpers.go:~358` 的 `clearChannelRuntimeMetaFence` **函数体逐行相同**（token=""、version++、reason=0、untilMS=0）。纯重复副本。 |
  | `compat.go:1747` | `replaceUint64` | 与 helpers 里在用的 `replaceUint64Member` 是**语义不同的"宽松版"**：它只替换**第一个**匹配项并**立即 return，不调用 `normalizeUint64Set`**；而在用的版本替换**全部**匹配项并归一化排序去重。即正是《宽松版与严格版并存》的反模式——所幸宽松版这次是死的。若有人误用它，`Replicas`/`ISR` 会失去排序归一化，进而让 `runtimeRouteChanged` 的 `slices.Equal` 产生假变更。 |
  | `compat.go:1876` | `normalizeCompatError` | `if errors.Is(err, dberrors.ErrConflict) { return ErrStaleMeta }` —— 而 `compat.go:26` 就是 `ErrStaleMeta = dberrors.ErrConflict`。**它把 ErrConflict 映射成 ErrConflict，是一个恒等空操作**，即便被调用也不做任何事。 |
  | `snapshot.go:373` | `visitSlotSnapshotPayload` | **流式**快照解析器（回调式，不物化 entry）。在用的是 `decodeSlotSnapshotPayload`（`:324-337`），它把**每一条 entry 的 key 和 value 各做一次 `append([]byte(nil), ...)` 拷贝并全量物化进 `entries` 切片**。也就是说：**内存高效的那个版本是死的，在用的是全量物化版**——这直接放大了发现 2 的 OOM 面，也让大快照导入的峰值内存约等于两倍快照体积。 |
  | `snapshot.go:412` | `encodeSlotSnapshotPayload` | 批量编码器。在用的导出路径 `ExportHashSlotSnapshot`（`:63-93`）改成了增量三段式（`beginSlotSnapshotPayload` + 循环 `appendSlotSnapshotEntry` + `finishSlotSnapshotPayload`），于是这个"先收集成 `[]snapshotEntry` 再编码"的版本被弃用。与上一条合起来说明：快照编解码存在一套**完整的、未接线的第二实现**（visit/encode 这一对），但它只有约 20 行，不构成 `command_codec.go` 那种千行级镜像实现。 |
  | `keys.go:127,132` | `encodePluginBindingRowPrefix` / `encodePluginBindingRowKey` | 手写的 plugin-binding 主键编码器。该表（`table_plugin_binding.go:40-54`）已迁到表运行时，主键统一由 `Table.primaryRowKey`（`table_runtime.go:961-969`）生成，故这两个手写版本被绕过。属于"迁移到表运行时后未清理的旧键编码器"。 |
  | `table_channel_migration.go:729` | `decodeChannelMigrationTaskRowKey` | 手写的迁移任务行键解码器，同样已被 `Table.decodePrimaryRowKey`（`table_runtime.go:971-985`）取代。 |
  | `table_registry.go:46` | `(*metaTableRegistry).mustRegister` | `register` 的 panic 版本。实际的 panic 行为在 `registerMetaTable`（`table_runtime.go:78-84`）里实现了，故此函数多余。 |
  | `tx_helpers.go:37` | `commitSet` | `batch.Set` + `batch.Commit(true)` 的一步封装。各表都改为自己管理 batch 生命周期后被弃用。 |

- **结论（回答"`compat.go` 是不是第二套实现"）**：**不是**。`compat.go` 的 1881 行里，
  绝大部分是 `DB` / `ShardStore` / `WriteBatch` 三层对 `MetaDB` / `Shard` / `Batch` 的
  **薄转发壳**（典型形态：`validate()` 后直接 `return s.shard.XXX(...)`），
  与 `pkg/db/meta/FLOW.md:19` 的自述一致（"callers use this package through the
  compatibility `DB`、`ShardStore`、`WriteBatch` surface"），**是生产主路径，不是死代码**。
  真正的重复只有上表这几个小函数，以及 `CreateChannelMigrationTaskWithRuntimeGuard`
  在 `batch.go:234-259` 与 `compat.go:1329-1364` 的两份实现（语义等价，守卫拆分方式不同）。
  `compat.go` 中**自有实质逻辑**的部分是：`listChannelsPage`（:530-575）、
  两个全库扫描（发现 4、5）、`stageSubscribers`（发现 6）、
  `DeleteTerminalChannelMigrationTasksBefore`（发现 7）与那一组
  `stageChannelMigrationTaskAndMeta` 状态机转换——这些才是审计价值所在，
  并且已分别成条。

### [P3] 15. `table_conversation.go:394` 结构体字面量应为类型转换（staticcheck S1016）

- **位置**：`pkg/db/meta/table_conversation.go:394`
- **类别**：可读性
- **代码**：
  ```go
  	return validateConversationKey(ConversationKey{ChannelID: cursor.ChannelID, ChannelType: cursor.ChannelType})
  ```
- **说明**：`ConversationCursor`（`:97-102`）与 `ConversationKey`（`:88-93`）字段完全相同，
  可直接 `ConversationKey(cursor)`。纯风格问题，无行为影响。
  （同文件 `:173,212,250,290` 的四处 `batch.Commit(true)` 属于单元 29 已记录的
  "每次逻辑写单独 fsync" 全局性问题，此处仅交叉引用，不重复计条。）

### 阶段 0 机械发现的逐条裁决（gosec 12 条 / staticcheck 9 条）

**gosec G115 —— 12 条全部为误报**，理由分三类：

- **保序位转换（8 条）**：`tx_helpers.go:64,71`（`appendValueInt64`/`readValueInt64`，
  `uint64(int64)` 与 `int64(uint64)` 是同一对无损位转换，往返恒等）、
  `table_key.go:106,113`、`table_conversation.go:480`、`table_runtime_meta.go:570`
  （均为 `int64(ordered ^ (uint64(1)<<63))`，即标准的**保序 int64 编码**的逆变换，
  对全部 2^64 个位模式双射，不存在"溢出"语义）。
- **已证明非负（2 条）**：`snapshot.go:318`（`uint64(entryCount)`，`entryCount` 是循环计数器）、
  `snapshot.go:301`（`uint16(len(hashSlots))`，`hashSlots` 已经过
  `orderedHashSlots` 去重，而 `HashSlot` 本身是 uint16，故 distinct 值上限 65536；
  只有恰好传入全部 65536 个 slot 才会截断为 0，而调用方
  `pkg/slot/fsm/statemachine.go:657` 传的是本节点持有的 slot 子集，写不出触发路径）。
- **有显式上界保护（2 条）**：`inspect.go:616`（`uint64(-(value+1))+1`，
  这是**正确的**溢出安全取负写法，专门避免 `-math.MinInt64` 溢出）。
- **注意：`snapshot.go:370,399,400` 这 3 条 gosec 命中是真问题**，
  已分别升格为发现 1 与发现 2（`int(entryCount)`、`int(keyLen)`、`int(valueLen)`）。

**staticcheck 9 条**：8 条 U1000 已在发现 14 逐条解释，1 条 S1016 为发现 15。

## 已排除的候选项

- **`inspect.go:432-448` 把 `token` 明文放进 InspectRow** —— `inspectUserRow` 与
  `inspectDeviceRow` 都输出 `"token"` 字段，`Filters` 还允许按 token 过滤，
  看起来是凭据泄露。但 `InspectScan` 的唯一消费者是 `pkg/db/inspect`，
  而它的唯一 importer 是 `cmd/wkdb`（`cmd/wkdb/main.go`、`config.go`、`output.go`），
  一个**直接打开本地 Pebble 目录的离线 CLI**，没有任何网络入口。
  能运行 `wkdb` 的人本就对原始 DB 文件有读权限，**不构成权限提升**，写不出触发路径。
- **`compat.go:77-85` `DB.Close()` 无锁置空 `db.engine`/`db.meta`** —— 与并发
  `ForHashSlot`/`NewWriteBatch` 存在数据竞争，且两个 goroutine 并发 Close
  可双重 `eng.Close()`。但全仓 `Close()` 只在应用关停时调用一次，
  且 `Open`（`compat.go:69-76`）也只在启动时调用；我找不出与业务读写并发的真实调用序列，
  故不成条（若后续出现热重载 DB 的需求，这里会立刻变成真问题）。
- **`batch.go:234-259` `CreateChannelMigrationTaskWithRuntimeGuard` 在
  `CreateChannelMigrationTask` 返回错误后仍留下已入队的守卫 op** ——
  调用方拿到 error 后不会 Commit（`pkg/slot/fsm/statemachine.go:253` 直接
  `return nil, fmt.Errorf(...)`，整个 `wb` 被 `defer wb.Close()` 丢弃），残留 op 永不执行。
- **同一 `WriteBatch` 内"先 Abort 任务、再 Create 替换任务"会因
  `ensureChannelMigrationActiveAvailable`（`table_channel_migration.go:575-591`）
  读已提交存储而误报 `ErrAlreadyExists`** —— 确实存在（它读 `s.db.get(activeIndexKey)`
  而非 overlay），但 `isStaleMetaCommitError`（`pkg/slot/fsm/statemachine.go:340-344`）
  把 `ErrAlreadyExists` 列为可重试，于是
  `applyCommandsIndividuallyAfterStaleCommit`（`:296-310`）会把该批命令**逐条重放**，
  第二轮 Abort 先提交、Create 再看到提交后状态从而成功。**有正确的兜底路径**，
  不会导致 slot 卡死，故排除。（代价是整批命令被逐条重跑、每条一次 fsync，
  属于单元 22 的 fsm 分片范畴。）
- **`encodeChannelMigrationTaskValue`（`table_channel_migration.go:735-741`）
  在 `json.Marshal` 出错时 `panic(err)`** —— `ChannelMigrationTask` 只含
  string / 整型 / bool / 一个整型子结构，没有 chan、func、循环引用或
  NaN/Inf 浮点，`encoding/json` 对非法 UTF-8 字符串是替换为 U+FFFD 而非报错。
  `json.Marshal` 在此类型上**不可能失败**，写不出触发路径。
- **`ChannelMigrationTask` 用无 json tag 的 `json.Marshal` 做持久化格式** ——
  字段名即 Go 字段名，重命名字段会静默破坏兼容；`json.Unmarshal` 对缺失字段留零值、
  对未知字段静默忽略，理论上存在滚动升级/降级丢字段的风险。
  但 raft 是确定性复制（同一条命令在各副本算出同一个值），我无法构造出
  在**单一版本**下的具体错误结果，故按"写不出触发路径"排除，仅在此留痕。
- **`channelRuntimeMetaEqual`（`compat_channel_migration_helpers.go:417-421`）
  在提交路径用 `reflect.DeepEqual`** —— 反射比较确实比
  同包已有的手写比较器 `runtimeRouteChanged`（`table_runtime_meta.go:409-425`）慢，
  但只在**迁移状态机转换**（`stageChannelMigrationTaskAndMeta:1706`）时触发，
  频率是每个频道每次迁移数次，不在消息/连接热路径上，性能影响不可观测。
- **`inspectCheckDB`（`inspect.go:167-176`）为探活开一个全库迭代器再立刻关闭** ——
  每次 `InspectScan` 多一次 Pebble 迭代器分配。只在离线 CLI 路径，影响可忽略。
- **`ListChannelRuntimeMetaPage`（`table_runtime_meta.go:214-241`）的 `limit+1` 可溢出** ——
  `limit == math.MaxInt` 时溢出为负，`ScanPrimary` 会因 `limit <= 0` 返回空页且
  `done=true`。是错误结果但不是崩溃，且所有调用方传的都是配置化的小 limit，
  找不到会传 MaxInt 的路径。
- **`scanIndexWithOptions`（`table_runtime.go:363-372`）在返回前多做一次
  `getByPrimaryKey` 点查** —— `if !unlimited && len(rows) == limit { return }` 位于
  点查之后，故每页多一次无用点查与解码。但 `scanIndexRows` 路径
  （`stopAtLimit=true`，`:375`）会在恰好满页时提前返回，实际只影响
  `ScanIndex` 的每次调用 1 次，量级太小，并入发现 12 的建议而不单独成条。

## 本分片整体评价

这是**生产主路径**代码（`FLOW.md:19` 明确 slot FSM、proxy、cluster、runtime、access、usecase
都经 `compat.go` 的 `DB`/`ShardStore`/`WriteBatch` 访问本包），不是 v2 未上线栈。
整体工程质量在本仓中偏上：表运行时（`table_runtime.go`）抽象干净、
迁移状态机的 fencing/守卫逻辑（`compat_channel_migration_helpers.go`）写得相当严谨、
批内 read-your-writes 对 runtime meta 与迁移任务都做对了，
死代码也只有几个小函数——**`compat.go` 不是 `command_codec.go` 那种千行级第二实现，
而是货真价实的薄转发层**，协调者怀疑的架构级重复在这里不成立。

真正的问题集中在**信任边界**：本包把"来自 raft 对端的快照字节"和"来自 HTTP 的用户输入"
都当作可信数据。`ImportHashSlotSnapshot` 会把对端提供的 key/value **原样 `Set` 进本地存储**
而不做任何值级校验，三处长度/计数解析（`snapshot.go:396`、`snapshot.go:368`、
`table_runtime_meta.go:599`）又都存在 uint64 溢出或缺失上界，
且 `rowcodec` 允许 value 自己的 flags 字节决定是否校验校验和——
这四点叠加，使得一条畸形快照可以造成从一次性 panic 到**持久化崩溃循环**的不同程度后果。

**最该优先处理的一个问题是发现 13**：`User.Token` / `Device.Token` 长度完全未校验，
而 `appendValueString` 底层的 `keycodec.AppendString` 在超长时 `panic`。
它的独特之处在于——**键**路径已经用 `encodeKeyParts` 里的显式长度检查防住了同一个原语，
**值**路径却是裸直通；触发只需一个 `token` 超过 64 KiB 的 HTTP 请求；
panic 发生在 raft apply goroutine（全仓仅两处 `recover()`，都不在这条栈上），
而那条 raft 日志条目**已经提交落盘**，于是重启即重放、重放即再崩，
整个 slot 复制组进入无法自愈的崩溃循环。修复成本极低（对 Token 调用同文件已有的
`validateKeyString`），危害却是集群级永久 DoS。

次高优先级是发现 6 的批内读写不一致（同一 raft 批次里的频道变更会互相覆盖、
已删除频道会连同订阅者一起复活）——它不会崩溃，但会**静默**产生错误状态，
且各副本会一致地错误，因此不会被任何一致性校验发现。
