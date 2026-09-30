# 存储引擎与消息日志（pkg/db 根 + internal/{engine,commit,cache,keycodec,rowcodec,schema,dberrors} + message + inspect）

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/db/db.go | 74 | 是 |
| pkg/db/options.go | 58 | 是 |
| pkg/db/types.go | 23 | 是 |
| pkg/db/errors.go | 22 | 是 |
| pkg/db/FLOW.md | 14 | 是（先于代码） |
| pkg/db/message/FLOW.md | 40 | 是（先于代码） |
| pkg/db/message/db.go | 40 | 是 |
| pkg/db/message/channel_log.go | 84 | 是 |
| pkg/db/message/append.go | 192 | 是 |
| pkg/db/message/read.go | 234 | 是 |
| pkg/db/message/keys.go | 152 | 是 |
| pkg/db/message/codec.go | 208 | 是 |
| pkg/db/message/row.go | 56 | 是 |
| pkg/db/message/types.go | 174 | 是 |
| pkg/db/message/indexes.go | 112 | 是 |
| pkg/db/message/idempotency.go | 71 | 是 |
| pkg/db/message/catalog.go | 71 | 是 |
| pkg/db/message/schema.go | 111 | 是 |
| pkg/db/message/history.go | 167 | 是 |
| pkg/db/message/snapshot.go | 104 | 是 |
| pkg/db/message/apply_fetch.go | 78 | 是 |
| pkg/db/message/checkpoint.go | 116 | 是 |
| pkg/db/message/retention.go | 159 | 是 |
| pkg/db/message/truncate.go | 78 | 是 |
| pkg/db/message/compat.go | 1524 | 是（分段全读） |
| pkg/db/message/inspect.go | 225 | 是 |
| pkg/db/internal/engine/db.go | 113 | 是 |
| pkg/db/internal/engine/batch.go | 75 | 是 |
| pkg/db/internal/engine/iter.go | 61 | 是 |
| pkg/db/internal/engine/span.go | 15 | 是 |
| pkg/db/internal/commit/coordinator.go | 323 | 是 |
| pkg/db/internal/commit/batch.go | 21 | 是 |
| pkg/db/internal/cache/tiny.go | 73 | 是 |
| pkg/db/internal/keycodec/builder.go | 94 | 是 |
| pkg/db/internal/keycodec/codec.go | 93 | 是 |
| pkg/db/internal/keycodec/span.go | 27 | 是 |
| pkg/db/internal/rowcodec/envelope.go | 86 | 是 |
| pkg/db/internal/rowcodec/codec.go | 304 | 是 |
| pkg/db/internal/rowcodec/value.go | 23 | 是 |
| pkg/db/internal/schema/schema.go | 83 | 是 |
| pkg/db/internal/schema/validate.go | 79 | 是 |
| pkg/db/internal/dberrors/errors.go | 22 | 是 |
| pkg/db/inspect/store.go | 77 | 是 |
| pkg/db/inspect/types.go | 83 | 是 |
| pkg/db/inspect/cursor.go | 111 | 是 |
| pkg/db/inspect/parser.go | 328 | 是 |
| pkg/db/inspect/planner.go | 274 | 是 |
| pkg/db/inspect/execute.go | 544 | 是 |

`pkg/db/meta/` 属 unit 30，未审计（仅读了跨包调用处以理解上下文）。跨包佐证阅读：`pkg/raftlog/pebble_writer.go`、`pkg/channel/replica/durable_store.go`、`pkg/channel/handler/{message_query,message_sync,seq_read,fetch,codec,append}.go`、`internal/app/{build,channelmeta}.go`、`internal/access/api/message_send.go`、`pkg/channel/channel.go`。全部 go 工具命令均加 `GOTOOLCHAIN=go1.23.4`；`go vet ./pkg/db/...` 通过；工作树保持干净。

## 发现

### [P0] 0. keycodec.AppendString 对超长 key 部件 panic，可由一次 HTTP 请求远程触发节点崩溃

- **位置**：`pkg/db/internal/keycodec/codec.go:47-53`
- **类别**：安全 / 远程可达 panic
- **代码**：
  ```go
  // AppendString appends a length-prefixed string key part.
  func AppendString(dst []byte, value string) []byte {
      if len(value) > maxStringLen {            // maxStringLen = 1<<16 - 1
          panic(fmt.Sprintf("keycodec: string key part too long: %d", len(value)))
      }
      dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))
      return append(dst, value...)
  }
  ```
  写路径命中点（`pkg/db/message/append.go:120-135`）：
  ```go
  if row.ClientMsgNo != "" {
      if err := batch.Set(encodeMessageClientMsgNoIndexKey(l.key, row.ClientMsgNo, row.MessageSeq), ...
  ```
  `encodeMessageClientMsgNoIndexKey`（keys.go:65-68）→ `keycodec.AppendString(key, clientMsgNo)`，无任何长度校验。
- **触发路径**：逐跳已核实，全程无长度上界、无 recover：
  1. `POST /message/send`，body `{"from_uid":"u","channel_id":"c","channel_type":1,"payload":"<base64>","client_msg_no":"<70000 字节>"}`。`internal/access/api/message_send.go:39-61` 只校验 FromUID/Payload/ChannelID **非空**，不校验长度；该路由**没有** `http.MaxBytesReader`（全仓仅 `internal/access/api/bench.go:126` 与 `plugin.go:21` 设了 body 上限），gin 默认亦无 body 限制。
  2. `message_send.go:95` 把原样字符串放入 `message.SendCommand{ClientMsgNo: req.ClientMsgNo}`；`internal/usecase/message/send.go:508` 原样传递（`grep ClientMsgNo internal/usecase/message/*.go` 确认无裁剪、无校验）。
  3. 该 HTTP 路径**不经过** `pkg/protocol/codec`，因此 2 字节长度前缀（`pkg/protocol/frame/common.go:316` `StringFixLenByteSize = 2`）这条天然 64KB 上界**在此不生效**。
  4. `pkg/channel/handler/codec.go:163-184` 以 **uint32** 长度前缀（`appendSizedString`）编码 durable record，上界 4GB，70KB 顺利通过；`compat.go:1315` 解回 `row.ClientMsgNo`。
  5. `ChannelStore.Append`（compat.go:202）→ `commitRowsLocked`（compat.go:1070）→ `stageMessageRows` → `stageMessageRow`（append.go:124）→ `encodeMessageClientMsgNoIndexKey` → **panic**。
  6. `grep -rn 'recover()' pkg/channel internal/usecase/message internal/runtime/channelplane internal/access/api` → **零结果**；且该 append 发生在 `pkg/channel/replica/durable_store.go:613` 的副本持久化 goroutine 中，而非 gin 请求 goroutine，即便 gin 有 Recovery 中间件也救不了。
  同一 panic 还有第二条入口：`channelPartitionID`（keys.go:10-12）对 `ChannelKey` 做 `AppendString`，而 ChannelKey = `"channel/<type>/" + base64url(channel_id)`（`pkg/channel/channel.go:311-321`），base64 放大 4/3 倍 —— 约 49KB 的 `channel_id` 即可越界；`encodeCatalogValue`（catalog.go:57-60）对 `id.ID` 同理。`from_uid`（经 `encodeMessageIdempotencyIndexKey`，keys.go:82-86）也是同一类无界输入。
- **后果**：**一次未认证边界内的 HTTP 请求即可 panic 掉持有该频道 leader 的节点进程**。panic 发生在 `batch.Commit` 之前，故无半写数据；但崩溃前 header/payload 已 stage、批被丢弃，消息丢失。由于该消息未进入持久化日志，重启后不会重放，属一次性崩溃而非 crashloop；然而攻击者可无限重复，等价于对任意频道 leader 节点的远程 DoS。
- **建议**：在编解码边界（HTTP/usecase 入口）对 `client_msg_no`/`channel_id`/`from_uid` 施加长度上限并返回 400；`keycodec.AppendString` 改为返回 error 而非 panic，让存储层永不因输入崩溃。

### [P1] 1. ReadReverse / readRowsReverse 把"最近 N 条消息"退化成全量正序扫描 + 全量物化

- **位置**：`pkg/db/message/read.go:29-55`（ReadReverse）、`pkg/db/message/compat.go:1230-1256`（readRowsReverse）
- **类别**：性能
- **代码**：
  ```go
  // pkg/db/message/read.go:41
  all, err := l.readForward(ctx, 1, fromSeq, ReadOptions{})
  if err != nil {
      return nil, err
  }
  messages := make([]Message, 0, boundedCapacity(len(all), opts.Limit))
  ```
  ```go
  // pkg/db/message/compat.go:1238
  all, err := l.readRows(ctx, 1, fromSeq, ReadOptions{})
  ```
  根因在 `pkg/db/internal/engine/iter.go:10-23`：`Iter` 只暴露 `First`/`SeekGE`/`Next`，没有 `Last`/`Prev`/`SeekLT`，且 `engine.IterOptions.Reverse` 字段（`span.go:14`）写着 "reserved for future"，从未接线。
- **触发路径**：客户端冷启动/翻页请求"最新 20 条消息" → `pkg/channel/handler/message_query.go:152` 或 `pkg/channel/handler/message_sync.go:79` 调 `ListMessagesBySeq(startSeq, limit+1, math.MaxInt, true)` → `compat.go:377` 转入 `readRowsReverse` → `readRows(ctx, 1, fromSeq, ReadOptions{})` 从 seq=1 起把 1..fromSeq 的**每一条**消息读出：每个 key/value 都经 `iter.Key()`/`iter.Value()` 各拷贝一次、`Unwrap` CRC32C 校验 + 再拷贝一次 payload、header 全列扫描反序列化、`validateMaterializedMessageRow` 全 payload FNV 重算 —— 然后在 `read.go:47-53` / `compat.go:1244-1254` 倒序取最后 N 条丢弃其余。一个存了 100 万条消息的活跃群，其历史越高，每次"拉最新 20 条"的代价越大，与请求量无关。
- **后果**：最常见的 IM 读路径复杂度 O(历史全部消息) 而非 O(limit)。CPU 与分配随日志长度线性恶化；`all []Message`（含全部 payload 拷贝）造成瞬时大对象分配，GC 压力随之上升；长频道在高读放大下可把读请求延迟拖到秒级。
- **建议**：给 `engine.Iter` 接线 Pebble 的 `Last/Prev/SeekLT`（`IterOptions.Reverse` 已预留），让 reverse 读从 `fromSeq` 向前走 N 步即返回。

### [P1] 2. 全部 17 处写路径逐请求 `Commit(true)`，Group-Commit 引擎建成却零调用 —— 每次逻辑写一次独立 fsync

- **位置**：`pkg/db/internal/commit/coordinator.go:93-105`（死代码）、`pkg/db/message/append.go:36`（实际路径）
- **类别**：性能（持久化路径）
- **代码**：
  ```go
  // pkg/db/internal/commit/coordinator.go:93 — 完整的 group-commit 协调器
  func NewCoordinator(db *engine.DB, cfg Config) *Coordinator {
      ...
      c.commitFunc = func(batch *engine.Batch) error { return batch.Commit(true) }
      go c.run()
      return c
  }
  ```
  全仓非测试 importer：仅 `coordinator_test.go`（`grep -rn 'db/internal/commit'` 证实）。而实际写路径：
  ```go
  // pkg/db/message/append.go:36
  if err := batch.Commit(true); err != nil {
  ```
  17 处 `Commit(true)` 分布于 append.go:36、apply_fetch.go:73、checkpoint.go:49、history.go:76,100、compat.go:445,662,722,893,950,1047,1086、retention.go:50,116、snapshot.go:44,93、truncate.go:47 —— 每次逻辑写（哪怕只存一条 cursor / checkpoint）都独占一次 `pebble.Sync` fsync。
- **触发路径**：配置面早已为它建好——`internal/app/build.go:203,207` 打开引擎后调用 `ConfigureCommitCoordinator(CommitCoordinatorConfig{...})`，`compat.go:92` 把配置存进 `Engine.commitCfg`，然后**没有任何代码消费它**：全仓 `CommitCoordinatorConfig()` 的唯一非测试调用方是 `internal/app/lifecycle_test.go:2236`。运行时 N 条消息/秒 → 逐条 append 每条一个独立 fsync，而不是 N 条合并成一次。
- **后果**：写吞吐被 fsync 次数而不是带宽限制。SSD 上单次 fsync 约 0.1-1ms，1000 msg/s 即约等于 1000 fsync/s；合批后本可降一个数量级。同时这是"组件已写好、测试已通过、只差接线"的纯浪费——`pkg/raftlog/pebble_writer.go:86-120`（`runWriteWorker` 从 channel 排空请求合并成一个 batch 一次 `Commit(pebble.Sync)`）证明同一仓库里这个模式是已知且已正确实现的，pkg/db 却没接入。
- **建议**：要么把 `Coordinator` 接进 ChannelLog 写路径，要么删掉它和 `NodeStoreOptions.Commit` / `ConfigureCommitCoordinator` 这套假配置面，避免"看起来有合批"的误导。

### [P1] 3. LEO 冷恢复是每频道 O(全部历史) 的全 keyspace 正向扫描，持 appendMu 阻塞首个 append

- **位置**：`pkg/db/message/channel_log.go:53-84`
- **类别**：性能
- **代码**：
  ```go
  var leo uint64
  for ok := iter.First(); ok; ok = iter.Next() {
      if err := ctx.Err(); err != nil {
          return 0, err
      }
      seq, familyID, ok := decodeMessageRowKey(l.key, iter.Key())
      if !ok || familyID != messageHeaderFamilyID {
          continue
      }
      if seq > leo {
          leo = seq
      }
  }
  ```
- **触发路径**：进程重启（或首次触碰某频道）→ 任意 `Append/Read/LEO` 调用 `loadLEOLocked` → `recoverLEO` 对该频道 message row 前缀做 `iter.First()` 到尾的全量迭代（header+payload 两族都要走过，`decodeMessageRowKey` 逐 key 分配解码），只为求 max(seq)。期间持有 `appendMu`（`channel_log.go:35-37`），该频道第一个 append 被阻塞整个扫描时长。
- **后果**：一个有 500 万条历史消息的频道重启后，首条消息写入要等全量扫完；大量频道重启时恢复时间随各频道历史长度线性叠加。key 布局是 `(prefix, seq, family)` 升序的，max(seq) 本可以一次反向 seek 到 keyspace 末尾 O(1) 求得（与发现 1 同根因），或从 retention state / catalog 直接恢复。
- **建议**：给 Iter 加反向定位后 seek 到该频道 keyspace 末尾取第一个 header family key；或把 LEO 冗余存进系统族单点读。

### [P2] 4. AppendStrict 模式下，批量 append 的每条记录在持锁期间各做一次索引点查

- **位置**：`pkg/db/message/append.go:148-156,168-174`（validateAppendRow）、`pkg/db/message/append.go:20-23`（整批持 appendMu）
- **类别**：性能
- **代码**：
  ```go
  if mode == AppendStrict {
      existingSeq, ok, err := l.lookupMessageIDSeq(ctx, row.MessageID)
      ...
  }
  ...
  hit, ok, err := l.lookupIdempotency(ctx, key)
  ```
  `lookupMessageIDSeq`/`lookupIdempotency` → `engine.Get` 各一次，`lookupIdempotency` 命中时还要再 `getRowBySeq` 把整条消息（header+payload 两次 Get + CRC + FNV）物化出来。
- **触发路径**：生产 append 走 `compat.go:202` `Append → AppendStrict`（`durable_store.go:613`），一批 B 条记录 → 持 `appendMu` 串行做 B×(1~2) 次点查 + 可能的整行物化，然后才建 batch 提交。同批内已有 `seenMessageIDs`/`seenIdempotencyKeys` 去重，但跨调用与磁盘的重复检查全部串行落在锁内。
- **后果**：append 持锁时间 ∝ 批大小 × 磁盘点查延迟；写吞吐上不去，读请求（`Read` 不持锁但共享 Pebble）也受影响。批量场景本可一次 `NewIter` 对已排序的索引 key 区间做合并扫描替代逐条 Get。
- **建议**：批内对索引 key 排序后用单个区间迭代器做存在性检查，或提供缓存乐观校验。

### [P2] 5. 每条消息读取做双重校验 + 四层冗余拷贝

- **位置**：`pkg/db/message/read.go:219-227` + `pkg/db/internal/rowcodec/envelope.go:61-72` + `pkg/db/internal/engine/iter.go:26-43` + `pkg/db/message/codec.go:215`
- **类别**：性能
- **代码**：
  ```go
  // read.go:223 — 读路径上对整个 payload 重算 FNV-1a
  if row.PayloadHash != hashPayload(row.Payload) {
      return fmt.Errorf("%w: payload hash mismatch at seq %d", ...)
  }
  // envelope.go:72 — Unwrap 再拷贝一次
  Payload: append([]byte(nil), value[envelopeHeaderLen:]...),
  // iter.go:42 — Iter.Value() 已经从 Pebble 拷过一次
  return append([]byte(nil), value...), nil,
  // messageFromRow (read.go:215) — 出口再拷一次
  Payload: append([]byte(nil), row.Payload...),
  ```
- **触发路径**：任意一条消息读取（`getRowBySeq`/`readForward`）：Pebble 内部拷贝 → `Iter.Value()` 拷贝① → `Unwrap` CRC32C 校验（envelope 已覆盖 payload 完整性，`envelope.go:76-86`）+ 拷贝② → `validateMaterializedMessageRow` 又对整个 payload 重算 FNV-1a（`row.go:49-56`，纯 Go 逐字节循环）→ `messageFromRow` 拷贝③。CRC32C 已在 Unwrap 里验证过同一份数据，FNV 是纯重复工作且 FNV-64 无抗碰撞价值。
- **后果**：每条消息读 = 3 次全量 payload 拷贝 + 1 次逐字节哈希。对 1KB payload、1 万 msg/s 的读放大场景，仅冗余拷贝就 ~30MB/s 额外分配，FNV 循环占满一个核的量级。
- **建议**：FNV 校验保留在写入时计算即可（读时依赖 CRC32C）；`messageFromRow` 对 scan 路径可交出 Unwrap 拷贝避免第③次。

### [P2] 6. 每条消息 key 编码 10+ 次临时堆分配，partition-id 每次重新转换

- **位置**：`pkg/db/message/keys.go:10-12`、`pkg/db/internal/keycodec/builder.go:8-12,87-89`、`pkg/db/message/codec.go:17,178`、`pkg/db/internal/rowcodec/envelope.go:42-53`
- **类别**：性能
- **代码**：
  ```go
  // keys.go:10
  func channelPartitionID(key ChannelKey) []byte {
      return keycodec.AppendString(nil, string(key))
  }
  // builder.go:87
  func (b *Builder) Key() []byte {
      return append([]byte(nil), b.buf...)
  }
  ```
  `stageMessageRow`（append.go:102-138）每条消息构造 header key、payload key、ID-index key、可选 clientMsgNo-index 与 idempotency-index key；每个 encode 都新建 `keycodec.Builder`（buf 从 nil 增长）、`channelPartitionID`（string→[]byte 转换+新分配）、`Builder.Key()` 再防御拷贝，头部/payload 值各新建一个 `rowcodec.Writer`（`codec.go:17,178`，零复用）并 `Wrap` 做 `make`+`copy`。decode 侧 `decodeMessageRowKey`（keys.go:34-44）每次调用还先重新 `encodeMessageRowPrefix` 整套 builder 流程。
- **触发路径**：每条消息 append/读/索引查询都走到；`ChannelKey` 在 `ChannelLog` 实例生命周期内不变（`channel_log.go:14-22`），但 partition-id 字节从不缓存。
- **后果**：单条消息仅 key/值构造即 10+ 次小对象分配，全部落 GC。属于典型热路径分配风暴，Pebble 写入放大之外又叠一层 CPU 浪费。
- **建议**：`ChannelLog` 启动时缓存 partition-id 前缀字节；`rowcodec.Writer` 池化；decode 路径缓存 prefix。

### [P2] 7. TrimPrefixThrough 在持 appendMu 下正序全量读出 trim 前缀以逐条点删，而不用 DeleteRange

- **位置**：`pkg/db/message/retention.go:72-103`、`pkg/db/message/truncate.go:32-43`、`compat.go:696,924`（truncateLocked / TrimMessagesThrough 同模式）
- **类别**：性能
- **代码**：
  ```go
  messages, err := l.Read(ctx, 1, ReadOptions{})   // retention.go:72 — 无 Limit、无 MaxBytes
  ...
  for _, msg := range messages {
      if msg.MessageSeq > throughSeq {
          break
      }
      if err := l.stageDeleteMessage(batch, msg); err != nil {  // 逐条 Delete
  ```
- **触发路径**：retention worker 到期触发 `TrimPrefixThrough(ctx, throughSeq)`：先把 seq 1..N **全部物化**（含 payload 拷贝 + 双重校验，无上限），再在同一个 mega-batch 里对每条消息 stage 2~5 个 point delete，一次提交。消息行 key 与索引 key 全部可由 seq+row 数据直接推导，range delete 或分批处理都可行。批大小无上限：一次 trim 100 万条过期消息 = 一个含数百万 delete 的单 batch，超出 Pebble memtable 常规预算。
- **后果**：trim 期间长事务阻塞该频道所有 append（持 appendMu 全程）；单批过大触发内存峰值与提交延迟毛刺；读出全部 payload 纯属浪费（delete 只需要 MessageID/ClientMsgNo/FromUID，header family 足矣）。
- **建议**：主行用 DeleteRange（前缀+seq 边界），索引删除按批次分桶提交；或至少限制单批条数并释放 appendMu 分段执行。

### [P2] 8. truncation 后 retention state 丢失时 recoverLEO 会把已被删的 seq 视为存活，LEO 可能虚高

- **位置**：`pkg/db/message/truncate.go:50-51`、`pkg/db/message/channel_log.go:78-82`、`pkg/db/message/retention.go:87-89`
- **类别**：正确性 / 分布式一致性
- **代码**：
  ```go
  // truncate.go:50 — TruncateFrom 收尾
  l.leo.Store(fromSeq - 1)
  // channel_log.go:78-82 — 重启后 recoverLEO
  if state, ok, err := l.LoadRetentionState(ctx); err != nil {
      return 0, err
  } else if ok && state.RetainedMaxSeq > leo {
      leo = state.RetainedMaxSeq
  }
  // retention.go:87-89 — TrimPrefixThrough 里写的 state
  if leo > state.RetainedMaxSeq {
      state.RetainedMaxSeq = leo
  }
  ```
- **触发路径**：频道 A 有 LEO=1000（`RetainedMaxSeq` 随 append 不断被推高到 1000）。运维/迁移执行 `TruncateFrom(ctx, 800)`（truncate.go 是 typed API，`internal/app` 迁移路径可达）：行删除后 LEO 置 799，但**不更新 retention state**，`RetainedMaxSeq` 仍为 1000。节点随后重启 → `recoverLEO` 扫描 keyspace 得 799，但 `state.RetainedMaxSeq(1000) > 799` 分支把 LEO 改回 1000。之后 append 从 1001 开始 —— 中间 800..1000 是空洞，`ReadReverse`/seq 读取会永久看到不存在的区间，follower 拉取 `fromSeq=801` 会拿到空并可能卡住重试。
- **后果**：重启后 truncate 产生的日志空洞被固化，后续写入序列不连续，依赖连续 seq 的恢复/对账逻辑（FLOW.md 第 3 条"assigns contiguous sequences"承诺被破坏）将失败或静默跳段。需要配合一次 truncate（typed `TruncateFrom` 不写 state，compat `truncateLocked` 只写 `retentionStateAfterTruncate` 改 `RetainedMaxSeq=to` 的分支——见 `compat.go:1130-1133` 只有 to < RetainedMaxSeq 时才降，二者行为不一致本身就是坑）。
- **建议**：`TruncateFrom` 与 `recoverLEO` 对 RetainedMaxSeq 的语义二选一统一：要么 truncate 同步压缩 `RetainedMaxSeq`，要么 recoverLEO 不采用 RetainedMaxSeq 覆盖扫描结果。

### [P2] 9. PutIdempotency 可写入悬空索引条目，LookupIdempotency 随后把重复发送误判为存储损坏

- **位置**：`pkg/db/message/compat.go:424-446`（写入）、`pkg/db/message/idempotency.go:34-40`（校验）
- **类别**：正确性
- **代码**：
  ```go
  // compat.go:424 — "stores a legacy idempotency entry without requiring a message row"
  func (s *ChannelStore) PutIdempotency(key channel.IdempotencyKey, entry channel.IdempotencyEntry) error {
      ...
      value, err := encodeIdempotencyIndexValue(messageRow{
          MessageSeq:  entry.MessageSeq,
          MessageID:   entry.MessageID,
      ...
  // idempotency.go:38 — 读取时校验
  if !ok || row.MessageID != hit.MessageID || row.PayloadHash != hit.PayloadHash || ... {
      return IdempotencyHit{}, false, fmt.Errorf("%w: stale idempotency index", dberrors.ErrCorruptState)
  }
  ```
- **触发路径**：`PutIdempotency` 允许存一条指向不存在（或已被 retention 物理删除）的 seq 的索引项，且 `entry.PayloadHash` 缺省时被 `normalizeMessageRow` 按空 payload 填为 hash("")——与真实消息行不一致。之后客户端带相同 `(FromUID, ClientMsgNo)` 重发：`pkg/channel/handler/append.go:333` 调 `LookupIdempotency` → `getRowBySeq` 找不到行或字段不匹配 → 返回 `ErrCorruptState` 而不是"未存储"，append 路径把可恢复的陈旧索引当致命状态错误抛出。
- **后果**：合法的重复/重发被永久拒绝并上报 corrupt state；若上层把 ErrCorruptState 当不可恢复错误处理（禁写频道/告警），一条悬空索引即可造成该发送者对频道的持续失败。删除路径 `stageDeleteMessage`（truncate.go:72-76）会一并删索引，但 PutIdempotency 建的条目对应的行删除时若 FromUID/ClientMsgNo 为空则索引漏删，同样制造悬空。
- **建议**：`PutIdempotency` 要么校验目标行存在且一致，要么把 lookup 里 stale index 的语义降级为"未命中+异步清理"。

### [P2] 10. 只删了一半的 compat 二次编解码：读路径每条消息重复走一次 per-row 全量重编码

- **位置**：`pkg/db/message/compat.go:1347-1379`（compatibilityRecordFromRow）与 `pkg/channel/handler/codec.go`（handler 侧重复实现）
- **类别**：性能 / 架构
- **代码**：
  ```go
  // compat.go:1347 — 把持久化行重新编码回 legacy 线上格式
  func compatibilityRecordFromRow(row messageRow) (channel.Record, error) {
      ...
      payload = append(payload, channel.DurableMessageCodecVersion)
      payload = binary.BigEndian.AppendUint64(payload, row.MessageID)
      ... // 逐字段重建 45 字节头 + 7 个 length-prefixed 字段
  ```
  `ListMessagesBySeq`（compat.go:384-388）每条消息：Pebble 读 → 解 header/payload envelope → **再重编码回 legacy `channel.Record`/`channel.Message`**（含全 payload 再拷贝，compat.go:1441）→ handler 层（message_sync.go 等）直接消费。持久化格式本就是 legacy 格式的有损往返（compat.go:1287-1345 解码端 Timestamp 只保留低 32 位：`Timestamp: int64(int32(binary.BigEndian.Uint32(payload[33:37])))`，写入端 compat.go:1369 同样 `uint32(row.Timestamp)` 截断）。
- **触发路径**：`pkg/channel/handler/message_sync.go:79,106,144`、`seq_read.go:131`、`fetch.go:70`、`message_query.go:152` 的每次同步/查询，每条消息都要经历 编码→存储→解码→重编码 四跳，其中重编码与原持久化内容等价。
- **后果**：热读路径多一整套序列化 CPU + 每条消息至少 2 次全量 payload 拷贝；同时 `Timestamp` 经 uint32 截断在 2038-01-19 后会变负数（`int32(uint32(ts))`），存量新写入的数据已带此缺陷，属于埋雷的持久化格式。
- **建议**：读路径直接从持久化行组装 Message，跳过 legacy 往返；Timestamp 持久化字段升为 int64（需格式版本演进）。

### [P2] 11. ChannelStore.ForChannel 对相同 key 不同 id 直接 panic

- **位置**：`pkg/db/message/compat.go:135-139`
- **类别**：正确性（可用性）
- **代码**：
  ```go
  if st := e.stores[key]; st != nil {
      if st.id != id {
          panic("message: inconsistent channel key and channel id")
      }
      return st
  }
  ```
- **触发路径**：key 由 `pkg/channel/channel.go:311-321` `effectiveChannelKey` 从 `base64(ID)+type` 推导，正常下 key↔id 一一对应；但 `ForChannel` 的调用方（`compat.go:172,180` `Read/ReadReverse` 用 `ChannelID{}` 空身份）与正常路径（`ForChannel(key, id)` 带真实身份）在**同一 Engine** 上先后命中同一 key 时——先走带 id 的路径创建 store，再走 `Read` 传 `ChannelID{}` —— 即触发 panic。`e.db.Channel(ChannelKey(channelKey), ChannelID{})`（compat.go:172）就是一个现成的第二种身份来源。
- **后果**：进程崩溃级；虽然触发依赖调用顺序，但两个入口共用 stores map 的设计使 panic 是结构性的，且 panic 无 recover 兜底。
- **建议**：返回 error 而非 panic；或 `Read/ReadReverse` 复用已存在 store 的 id。

### [P2] 12. context 全部被吞：compat 层所有存储调用用 context.Background()，超时无法传导到 Pebble

- **位置**：`pkg/db/message/compat.go:202,207,248,286,316,341,353,377,396,416,453,476,481,486,491,...`（共 30+ 处 `context.Background()`）
- **类别**：context
- **代码**：
  ```go
  // compat.go:202
  func (s *ChannelStore) Append(records []channel.Record) (uint64, error) {
      return s.appendRecords(context.Background(), records, AppendStrict)
  }
  ```
  `ChannelStore.Append/AppendTrusted/Read/LEO/GetMessageBySeq/ListMessagesBySeq/ListMessagesByClientMsgNo/PutIdempotency/...` 接口不收 ctx，内部一律 `context.Background()`。而 `readForward`/`appendRecords` 内部只有 `ctxErr` 快速检查，Pebble 迭代循环中的 `ctx.Err()`（compat.go:1186）在 Background 下永不触发。
- **触发路径**：上游 RPC 带着短超时 context 调 `pkg/channel/handler/seq_read.go:131` → `ListMessagesBySeq` → Background — 客户端取消/超时后，一次 O(全部历史) 的扫描（发现 1/3）照跑到底，无法取消。
- **后果**：慢查询/大扫描不可取消，goroutine 与 IO 被僵尸请求占用；在发现 1、3 的放大下，取消失效会被严重放大。
- **建议**：compat 接口加 ctx 参数（调用方已有 ctx），或至少把 request ctx 贯穿到 readRows 迭代循环。

### [P3] 13. 死代码：internal/cache/tiny.go 整包零引用

- **位置**：`pkg/db/internal/cache/tiny.go:6-73`
- **类别**：架构（死代码）
- **代码**：
  ```go
  // Tiny is a small bounded map-backed cache for explicit domain hot paths.
  type Tiny[K comparable, V any] struct {
      mu       sync.RWMutex
      capacity int
      items    map[K]V
  }
  ```
- **触发路径**：`grep -rn 'db/internal/cache'` 全仓只有 `tiny_test.go`。这也侧面说明 pkg/db 内部没有任何"显式热路径缓存"落地（发现 4/5/6 的重复读无处缓冲）。
- **后果**：约 73 行死代码 + staticcheck 级别的维护噪音。
- **建议**：删除，或在接入点真正使用。

### [P3] 14. FLOW.md 与实现不一致（3 处）

- **位置**：`pkg/db/message/FLOW.md:4,9,13`、`pkg/db/options.go:9-11`、`pkg/db/types.go:11-12`
- **类别**：架构/文档
- **代码**：
  FLOW.md:4 "Channel returns a cached typed ChannelLog for one channel partition." — `MessageDB.Channel`（db.go:25-40）确实缓存，但 `logs` map **只增不减**，且 `NodeStore.Messages()`（db.go:48-53）每次调用都 `message.NewDB(s.message)` 新建一个 MessageDB，其缓存互相独立；实际生产入口 `Engine.stores`/`Engine.db.Channel` 才是常驻缓存。
  FLOW.md:9 "retention state preserves LEO across reopen after prefix trim" — 语义与发现 8 中 typed `TruncateFrom` 的行为冲突（后者绕过 retention state）。
  FLOW.md/options.go: "CommitOptions controls group-commit batching for coordinated writes"（options.go:13）、`CoordinatedSync` 常量（types.go:11）— 全部无实现路径（发现 2），文档描述了一个不存在的合批行为。
- **触发路径**：阅读文档的新维护者会以为 ①每个 store 共享一份 ChannelLog 缓存 ②写路径有 group commit ③truncate 一定维护 retention state——三者在代码里都不成立。
- **后果**：认知误导，属于会直接导致错误设计决策的文档偏差。
- **建议**：随发现 2/8 的修复同步修订 FLOW.md。

### [P3] 15. InspectMessages 为扫描构造孤儿 ChannelLog 绕过缓存，InspectChannels 断言解码无需防御

- **位置**：`pkg/db/message/inspect.go:73,176,194`
- **类别**：健壮性（离线工具内）
- **代码**：
  ```go
  log := &ChannelLog{db: db, key: ChannelKey(req.ChannelKey)}   // :176
  ...
  result.Next = &InspectMessageCursor{AfterChannelKey: result.Rows[len(result.Rows)-1]["channel_key"].(string)} // :73
  result.Next = &InspectMessageCursor{AfterSeq: result.Rows[len(result.Rows)-1]["message_seq"].(uint64)}        // :194
  ```
  不带 ok 的类型断言对刚构造的 map 是安全的（key 由 `inspectChannelRow`/`inspectMessageRow` 保证写入），故仅记录为低风险惯例问题。孤儿 `ChannelLog` 不入缓存符合 FLOW.md 第 11 条意图，且 inspect 是 `cmd/wkdb` 离线 CLI 专用（全仓 importer 仅 `cmd/wkdb/*`），其 sort/全表扫描在此场景正当。
- **触发路径**：无生产触发路径；仅当未来有人把 inspect API 暴露到在线服务时，`.() 断言` 与无缓存扫描才会变成问题。
- **后果**：无（当前）。记录为防御性惯例问题与离线用途佐证。
- **建议**：保持 inspect 离线定位；若上在线需重写断言与分页边界。

## 已排除的候选项

- **`pkg/db/message/compat.go:1430,1436` 等 gosec G115（14 条）** — 逐一核对：`uint32(row.Expire)`/`uint32(row.Timestamp)`（compat.go:1366,1369,1414,1419）是 legacy 线上格式的既有 32 位字段宽度，编码/解码两端对称，不存在算术溢出路径；`uint64(value<<1) ^ uint64(value>>63)`（rowcodec/value.go:6）、`int64→uint64` 有序编码（keycodec/codec.go:85,91）是标准 order-preserving 变换，非截断错误；engine/db.go:105 `uint64(opts.MemTableSize)` 上游 normalize 过正数；inspect/execute.go:495 `uint16(slot)` 处 `metadb.HashSlot` 值域即 uint16；keycodec/codec.go:51 `uint16(len(value))` 前一行有 `maxStringLen` panic 守卫。全部为误报（compat.go:1366/1369 的 Timestamp 32 位截断语义问题已在发现 10 以 P2 记录，非 G115 意义上的溢出 bug）。
- **`pkg/db/message/retention.go:72` `l.Read(ctx, 1, ReadOptions{})` 疑似死循环/无界内存** — ReadOptions 零值意味着 Limit=0 → 不截断，但 Pebble keyspace 有限且 ctx 检查在循环内（read.go:103），最坏是有界但昂贵的扫描，已在发现 7 归并报告，不单列。
- **`pkg/db/internal/commit/coordinator.go:143-155` Submit 在 ctx.Done 后请求仍可能被提交（"请求被提交但返回错误"，最多一次执行语义）** — 这是 group commit 设计的固有语义（构建成功即持久化，done 有 1 缓冲，无泄漏），且该组件目前是死代码（发现 2），不构成现行缺陷。
- **`pkg/db/message/db.go:31-33` `db.logs == nil` 重建分支** — `NewDB` 恒初始化 logs，nil 分支不可达，最多算冗余防御，无触发路径，删除。
- **`pkg/db/db.go:48-53` `Messages()` 每次新建 MessageDB** — 当前生产路径不经过它（见发现 14 讨论），只造成 FLOW.md 文档矛盾，已并入发现 14，不单列资源问题。
- **`pkg/db/inspect/` 的 sort.Slice 与全表扫描** — 侦察指示要求核实离线属性：全仓 importer 仅 `cmd/wkdb`（独立调试二进制，非 wukongim 主程序），Pebble 以 `ReadOnly: true` 打开（store.go:41,49），确认离线专用，按指示排除，不报性能问题。
- **`pkg/db/internal/rowcodec/codec.go:262` `append([]byte(nil), rest[:length]...)` 每列拷贝** — String/Bytes 列必须拷贝以保证 Scanner 值不依赖底层 value 生命周期，成本已在发现 5 归并讨论；非缺陷。
- **`pkg/db/internal/keycodec/span.go:17-27` `PrefixEnd` 对全 0xff 前缀返回 `[]byte{0xff}`（End < Start，会产生空/非法区间）** — 所有前缀都以 domain 字节 `0x01`/`0x02` 开头（keycodec/codec.go:15-17），第一个字节永不为 0xff，for 循环必在首字节前返回，全 0xff 分支不可达，误报。
- **`pkg/db/message/read.go:120,151` 与 `compat.go:1199-1200` 提前 return 时未检查 `iter.Error()`（疑似吞掉 Pebble 迭代错误）** — 逐一核对：Pebble 迭代错误会让 `Next()` 返回 false 从而退出循环并走到末尾的 `iter.Error()` 检查；提前 return 只发生在 Limit/MaxBytes 主动 stop 时，此时尚无错误产生。`iter.Value()` 的错误也单独显式返回（read.go:126-129）。安全，删除。
- **`pkg/db/message/compat.go:1291-1292` `payload[0] != channel.DurableMessageCodecVersion` 直接 ErrCorruptValue** — 上游 `decodeCompatibilityRecordPayload` 只处理已校验长度的持久化记录，恶意输入不可达（无网络直接接触此解码器），不列。

## 本分片整体评价

这一层的工程水准明显高于仓库平均：错误分类清晰（dberrors 语义化 + toChannelError 映射）、编码格式带版本号和 CRC、原子批提交贯穿一致、schema 描述符完整，`go vet ./pkg/db/...` 干净也印证了机械质量；九类检查中"资源生命周期"一项结果尤其好——全部 8 个 `NewIter` 与 17 个 `NewBatch` 站点都紧跟 `defer Close()`，所有迭代循环都检查 `iter.Error()`，未发现一例 Pebble 迭代器/批泄漏（这是该类代码最典型的缺陷，此处确实干净）。

但它有三个真问题。**最高优先级是发现 0**：存储层最底层的 key 编码器用 `panic` 而非 error 处理超长输入，而 HTTP 发消息接口对 `client_msg_no`/`channel_id`/`from_uid` 既无长度校验也无 body 上限，且该 HTTP 路径绕开了协议层天然的 2 字节长度框定——一次请求即可远程崩溃任意频道 leader 节点，且全链路无 `recover()`。这个必须先修，修复成本极低（入口加长度校验 + AppendString 返回 error）。

其次是两个侦察判断准确的"结构性浪费"：**反向迭代能力缺失导致所有最新消息读取和 LEO 冷恢复退化为全量正扫**（发现 1/3，最常见 IM 读路径复杂度 O(频道历史)，并连锁放大了发现 4/5/7/12 的成本），以及**写好的 group-commit 引擎连同其配置面被完整建好、测试通过、却从未接线**（发现 2，17 处写路径各自独立 fsync；同仓 `pkg/raftlog/pebble_writer.go:86-120` 已正确实现同一模式，证明这不是缺思路而是缺接线）。这两处修复都不难，收益是数量级的。本分片全部为生产运行代码（`pkg/db/message` 由 `internal/app` 与 `pkg/channel/handler` 直接依赖），严重度按实际计；唯一例外是 `pkg/db/inspect`，已核实仅 `cmd/wkdb` 离线 CLI 引用并以 ReadOnly 打开，按指示排除其扫描类性能问题。
