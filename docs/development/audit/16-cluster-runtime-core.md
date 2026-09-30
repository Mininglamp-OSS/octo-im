# cluster 运行时核心：启动/配置/路由/转发/发现

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/cluster/cluster.go | 2011 | 是 |
| pkg/cluster/operator.go | 596 | 是 |
| pkg/cluster/config.go | 570 | 是 |
| pkg/cluster/agent.go | 482 | 是 |
| pkg/cluster/transport.go | 217 | 是 |
| pkg/cluster/dynamic_discovery.go | 204 | 是 |
| pkg/cluster/readiness.go | 147 | 是 |
| pkg/cluster/transport_glue.go | 132 | 是 |
| pkg/cluster/observer.go | 129 | 是 |
| pkg/cluster/api.go | 85 | 是 |
| pkg/cluster/forward.go | 74 | 是 |
| pkg/cluster/router.go | 72 | 是 |
| pkg/cluster/errors.go | 69 | 是 |
| pkg/cluster/runtime_state.go | 67 | 是 |
| pkg/cluster/retry.go | 56 | 是 |
| pkg/cluster/static_discovery.go | 33 | 是 |
| pkg/cluster/discovery.go | 14 | 是 |

## 发现

### [P0] 1. 未认证的 Slot Raft snapshot chunk 帧可用一个 96 字节报文触发任意大小内存分配（远程 OOM 杀进程）

- **位置**：`pkg/cluster/cluster.go:923-940`（handler 注册在 `pkg/cluster/cluster.go:207`）；分配点 `pkg/cluster/snapshot_chunks.go:89-99`；解码点 `pkg/cluster/codec.go:152-183`
- **类别**：安全 / 远程可达的进程崩溃
- **代码**：

  `pkg/cluster/cluster.go:923-940`：
  ```go
  // handleRaftSnapshotChunkMessage reassembles chunked MsgSnap frames before handing them to Raft.
  func (c *Cluster) handleRaftSnapshotChunkMessage(body []byte) {
      if c.runtime == nil {
          return
      }
      chunk, err := decodeRaftSnapshotChunkBody(body)
      if err != nil {
          return
      }
      assembled, ok, err := c.getRaftSnapshotAssembler().add(chunk)
  ```

  `pkg/cluster/codec.go:169-183`（`total` 是唯一没有被 body 长度约束的字段）：
  ```go
  msgLen := binary.BigEndian.Uint64(body[64:72])
  dataLen := binary.BigEndian.Uint64(body[72:80])
  if msgLen > uint64(math.MaxInt) || dataLen > uint64(math.MaxInt) {
      return raftSnapshotChunk{}, fmt.Errorf("raft snapshot chunk length overflows int")
  }
  want := raftSnapshotChunkHeaderSize + int(msgLen) + int(dataLen)
  if want != len(body) {          // 只校验 msgLen/dataLen 与 body 长度一致
      return raftSnapshotChunk{}, ...
  }
  ...
  if chunk.offset > chunk.total || uint64(len(chunk.data)) > chunk.total-chunk.offset {
  ```

  `pkg/cluster/snapshot_chunks.go:88-99`（用线路上的 `total` 直接 make）：
  ```go
  entry := a.pending[key]
  if entry == nil {
      if chunk.total > uint64(math.MaxInt) {     // 唯一的上界：MaxInt
          return assembledRaftSnapshot{}, false, fmt.Errorf("raft snapshot total overflows int: %d", chunk.total)
      }
      entry = &raftSnapshotChunkAssembly{
          message:    append([]byte(nil), chunk.message...),
          total:      chunk.total,
          data:       make([]byte, int(chunk.total)),   // ← 远程输入直接决定分配大小
  ```

- **触发路径**：
  1. `pkg/transport` 无认证、无 TLS（同会话 unit 15 已确认：非测试 `pkg/transport/*.go` 中 `tls\.|Token|auth|Secret|Credential` 零命中），所以任何能连上 `Config.ListenAddr` 的人都能发送任意 message type 的帧。
  2. 攻击者构造一个 `msgTypeRaftSnapshotChunk` 帧，body 恰好 96 字节：header 80 字节 + `msgLen=16`（16 字节任意 protobuf 字节）+ `dataLen=0`。
  3. header 中把 `total` 置为 `0x0000_4000_0000_0000`（约 70 TB），`offset=0`。
  4. `decodeRaftSnapshotChunkBody` 全部校验通过：`msgLen/dataLen` 与 body 长度一致；`offset(0) <= total`；`len(data)(0) <= total-offset`。`total` 本身从未与 `transport.MaxMessageSize` 或任何 snapshot 尺寸上限比较。
  5. `raftSnapshotAssembler.add` 走 `entry == nil` 分支，`chunk.total <= math.MaxInt` 通过，执行 `make([]byte, 70e12)` → Go runtime `fatal error: out of memory`（不可 recover）→ 进程立即死亡。
  6. 变体（更隐蔽的资源耗尽）：发 N 个 `total=1<<30`、`chunkID` 各不相同的帧。key 是 `{slotID, chunkID, from, to, index, term}` 全部由攻击者控制，`pending` map 无条目数上限、无总字节上限，`pruneExpiredLocked` 只淘汰超过 2 分钟 TTL 的条目 → 2 分钟内可稳定占住 N GiB 常驻内存。
- **后果**：任何能连到集群端口的人（无需任何凭证）用单个 96 字节 TCP 报文即可杀掉任意节点；重复发送即持续拒绝服务。第二种变体可在不触发 OOM 的情况下把节点内存推到 OOMKill。
- **建议**：`decodeRaftSnapshotChunkBody` 里把 `total` 与一个显式的 snapshot 上限（如 `SlotLogCompaction` 的 snapshot 尺寸预算或一个配置化的 `MaxSnapshotBytes`）比较后再入 assembler；assembler 侧对 `pending` 加条目数与总字节水位上限，并按需增长 `data` 而不是一次性 `make(total)`；同时给 `pkg/transport` 加节点间认证，使该帧不再可由任意客户端注入。

### [P1] 2. `Cluster.Stop()` 先关 runtime / controllerHost 再关 transport server，且无锁置 nil —— 与在途入站 RPC 存在 use-after-stop 数据竞争

- **位置**：`pkg/cluster/cluster.go:592-668`（顺序）；`pkg/cluster/cluster.go:630-632`（无锁置 nil）；读侧 `pkg/cluster/controller_handler.go:94-100,119-131,239-242`
- **类别**：并发 / 资源
- **代码**：

  `pkg/cluster/cluster.go:592-596, 626-647`：
  ```go
  func (c *Cluster) Stop() {
      c.stopped.Store(true)
      if c.observer != nil {
          c.observer.Stop()
          c.observer = nil
      }
      ... // 8 个 observer 依次 Stop + 置 nil
      if c.runtime != nil {
          _ = c.runtime.Close()          // ① runtime 先关
      }
      if c.controllerHost != nil {
          c.controllerHost.Stop()        // ② controllerHost 再关
          c.controllerHost = nil         //    无锁写
      } else { ... }
      if c.transportLayer != nil {
          c.transportLayer.Stop()        // ③ 监听端口最后才关
          c.transportLayer = nil
      }
  ```

- **触发路径**：
  1. 节点收到 SIGTERM，`Cluster.Stop()` 在主 goroutine 执行。
  2. `c.runtime.Close()` 与 `c.controllerHost.Stop()` 已完成，但 `c.transportLayer.Stop()` 还没执行 —— 监听 socket 仍在 accept，`pkg/transport/server.go:186` 仍在用 `go s.handleRPCRequest(...)` 为每个入站 RPC 起 goroutine。
  3. 此窗口内某个 peer 的 `rpcServiceController` 请求进来，走 `handleControllerRPC` → `controller_handler.go:94 if c.controllerHost == nil`（此时主 goroutine 还没执行到第 632 行，检查通过）→ `controller_handler.go:100 c.controllerHost.applyObservation(...)`。
  4. 主 goroutine 在这两行之间执行 `c.controllerHost = nil`：这是一个**无同步的指针字段写**与**同一字段的读**并发，`-race` 下必然报 data race；且第 100 行是对该字段的第二次独立 load，可能读到 nil。
  5. 即使读到 nil 也不会 panic（`controller_host.go:217-219`、`234-236`、`252-255` 等方法都有 `if h == nil` 守卫），但若读到旧指针，就是对一个已 `Stop()` 的 controllerHost 调用 `applyObservation` / `applyRuntimeReport` → 往已停的 observation cache 写入、`markPlannerDirty()` 往可能已无消费者的 wake channel 投递。
  6. `handleRaftMessage` / `handleRaftBatchMessage` / `handleRaftSnapshotChunkMessage`（`cluster.go:877,903,924`）在该窗口同样仍被调度，且它们的守卫只有 `if c.runtime == nil` —— 而 `Stop()` **从不把 `c.runtime` 置 nil**，所以守卫恒为假，仍会调用 `c.runtime.Step(...)`。
- **后果**：`-race` 构建下的确定性 data race；优雅关闭窗口内对已停组件的写入（observation 状态、planner dirty 标志）；`handleRaft*` 的 `c.runtime == nil` 守卫在 `Stop()` 后完全失效。
- **建议**：把 `transportLayer.Stop()`（停止 accept 并 join 在途 handler goroutine）提到 `runtime.Close()` 和 `controllerHost.Stop()` **之前**；`Stop()` 加 `sync.Once` 并对这些字段的读写统一用一把 mutex 或改成 `atomic.Pointer`，而不是裸赋 nil；`handleRaft*` 的守卫改为检查 `c.stopped.Load()`（`handleForwardRPC` 已经这么做了，`cluster.go:55`）。


### [P1] 3. 稳态下每 200ms 对每个本地 Slot 的整个 meta keyspace 做一次全量 Pebble 迭代

- **位置**：`pkg/cluster/cluster.go:1962-1998`（`migrationObserveOnce` → `hasActiveMigrationObservationWork` → `hasPersistedHashSlotMigrationStates`）；循环创建于 `pkg/cluster/cluster.go:523-526`；扫描实现 `pkg/db/meta/compat.go:689-718`
- **类别**：性能
- **代码**：

  `pkg/cluster/cluster.go:523-526`（用一个名为 "Timeout" 的常量当循环周期）：
  ```go
  c.migrationObserver = newObserverLoop(c.controllerObservationInterval(), func(ctx context.Context) {
      c.migrationObserveOnce(ctx)
  })
  c.migrationObserver.Start(context.Background())
  ```

  `pkg/cluster/cluster.go:1969-1998`：
  ```go
  func (c *Cluster) hasActiveMigrationObservationWork() bool {
      if c == nil || c.migrationWorker == nil { return false }
      if len(c.pendingHashSlotAborts) > 0 { return true }
      if len(c.pendingHashSlotDeltaCleanups) > 0 { return true }
      if len(c.migrationWorker.ActiveMigrations()) > 0 { return true }
      table := c.GetHashSlotTable()
      if table != nil && len(table.ActiveMigrations()) > 0 { return true }
      return c.hasPersistedHashSlotMigrationStates(context.Background())   // ← 所有廉价检查都为 false 时才走到这
  }

  func (c *Cluster) hasPersistedHashSlotMigrationStates(ctx context.Context) bool {
      for _, store := range c.hashSlotMigrationStores() {     // 每个本地 Slot 一次
          states, err := store.ListHashSlotMigrationStates(ctx)
  ```

  `pkg/db/meta/compat.go:693-706`（`engine.Span{}` 无上下界）：
  ```go
  iter, err := db.meta.engine.NewIter(engine.Span{}, engine.IterOptions{})
  ...
  for ok := iter.First(); ok; ok = iter.Next() {
      key := iter.Key()
      hashSlot, ok := isMetaRowKeyForTable(key, TableIDHashSlotMigration)
      if !ok || !bytesHasPrefix(key, encodeHashSlotMigrationStateKey(hashSlot)) {
          continue                                            // 在 Go 侧过滤，不是用 prefix span
      }
  ```
  （`pkg/db/internal/engine/span.go:7` 注释确认：`// End is the exclusive upper bound. Empty means unbounded.`）

- **触发路径**：
  1. 任何启用 controller 的正常集群（`startObservationLoop` 无条件创建 `migrationObserver`）。
  2. `Timeouts.ControllerObservation` 默认值是 `defaultControllerObservationTimeout = 200 * time.Millisecond`（`pkg/cluster/config.go:28,475-477`），`observerLoop` 以它为 ticker 周期 → 每秒 5 次 tick。
  3. `EnableHashSlotMigration` 默认关闭（FLOW.md 第 4 节自述"默认关闭的实验性 hash-slot 迁移开关"），所以 `pendingHashSlotAborts`、`pendingHashSlotDeltaCleanups`、`migrationWorker.ActiveMigrations()`、`table.ActiveMigrations()` **全部恒为空** —— 即每一次 tick 都必然落到最后一行。
  4. `hashSlotMigrationStores()` 返回本节点持有的每个 Slot 的 state machine store；对每个 store 执行一次全 meta keyspace 的 `First()/Next()` 迭代，把每条 key 交给 `isMetaRowKeyForTable` 做前缀判断后 `continue`。
  5. 结论：一个持有 S 个 Slot、每个 Slot meta DB 有 K 个 key 的节点，稳态下每秒执行 `5 × S × K` 次 Pebble iterator 步进，且 100% 的结果都是"没有迁移状态"。K 随 channel / 会话元数据线性增长。
- **后果**：随元数据规模线性增长的常驻 CPU 与 Pebble block-cache 污染；在 meta DB 有百万级 key、节点持有 10+ Slot 时是每秒千万级迭代步进，会持续挤占 block cache 并抬高所有 meta 读的延迟。这条路径在迁移功能默认关闭的情况下 100% 是纯浪费。
- **建议**：`ListHashSlotMigrationStates` 改用 `keycodec.NewPrefixSpan(TableIDHashSlotMigration 前缀)` 而不是空 Span；`hasActiveMigrationObservationWork` 里把持久化探测改成启动时一次性探测 + 内存标志（或仅在 `EnableHashSlotMigration` 为真时才探测）；同时 `migrationObserver` 的周期不应复用名为 `...Timeout` 的 `ControllerObservation`（200ms），应有独立的、秒级的 interval 配置。

### [P3] 4. `observeOnce` / `Cluster.observer` 是死代码，FLOW.md 仍把它描述为生效的降级保障

- **位置**：`pkg/cluster/cluster.go:672-699`（`observeOnce`）、`pkg/cluster/cluster.go:78`（字段声明）、`pkg/cluster/cluster.go:593-597`（Stop 中的死分支）；文档 `pkg/cluster/FLOW.md:609`
- **类别**：架构 / 文档一致性 + 行为退化
- **代码**：

  `pkg/cluster/cluster.go:672-699`：
  ```go
  func (c *Cluster) observeOnce(ctx context.Context) {
      if c.agent == nil || c.runtime == nil || c.stopped.Load() { return }
      ...
      assignCtx, cancel := c.withControllerTimeout(ctx)
      err := c.agent.SyncAssignments(assignCtx)
      cancel()
      shouldApply := err == nil
      if !shouldApply && controllerReadFallbackAllowed(err) && len(c.ListCachedAssignments()) > 0 {
          shouldApply = true          // ← FLOW.md:609 描述的就是这段降级逻辑
      }
      if shouldApply {
          _ = c.agent.ApplyAssignments(ctx)
      }
  ```

  实际生效的路径 `pkg/cluster/cluster.go:1949-1961`（**没有**这段降级）：
  ```go
  func (c *Cluster) syncObservationDeltaOnce(ctx context.Context, hint observationHint) error {
      if c == nil || c.agent == nil || c.runtime == nil || c.stopped.Load() { return ErrNotStarted }
      deltaCtx, cancel := c.withControllerTimeout(ctx)
      err := c.agent.SyncObservationDelta(deltaCtx, hint)
      cancel()
      if err != nil {
          return err               // 直接返回，不回退本地缓存
      }
      _ = c.agent.ApplyAssignments(ctx)
  ```

  `pkg/cluster/FLOW.md:609`：
  > `observeOnce 容忍 SyncAssignments 失败`: 即使 `SyncAssignments` 返回错误，只要本地有缓存的 assignments 且错误是可降级的，仍会触发 `ApplyAssignments`。保证网络抖动时调和不停滞。

- **触发路径**：
  1. `c.observer` 字段只在 `Stop()`（`cluster.go:593-597`）中被读和置 nil，`startObservationLoop`（`cluster.go:487-527`）创建的是 `heartbeatObserver` / `runtimeObserver` / `wakeObserver` / `slowSyncObserver` / `plannerWakeObserver` / `plannerObserver` / `migrationObserver` —— **从不给 `c.observer` 赋值**。全仓 `grep -rn "observeOnce"` 的非测试命中只有函数定义本身；三处调用全在 `pkg/cluster/cluster_test.go:2605,2707` 和 `pkg/cluster/agent_internal_integration_test.go:139`。
  2. 因此生产运行路径上只有 `wakeReconcileOnce` / `slowSyncOnce` → `syncObservationDeltaOnce`。
  3. controller leader 抖动（`FetchObservationDelta` 返回 not-leader / deadline exceeded）时，`syncObservationDeltaOnce` 直接返回错误，不会像 FLOW.md 承诺的那样用本地缓存的 assignments 继续 `ApplyAssignments`。
- **后果**：FLOW.md 声明的"网络抖动时调和不停滞"保障在实际代码里不存在：controller 读不通期间本节点的 assignment 调和完全停摆，直到下一次成功的 delta fetch。另有一段带完整降级逻辑的死代码只被测试覆盖，会让后续读者误以为该保障已生效。
- **建议**：把 `observeOnce` 的缓存降级逻辑合并进 `syncObservationDeltaOnce`（或明确删除 `observeOnce` + `c.observer` 字段 + `Stop()` 里的死分支），并同步修正 FLOW.md 第 609 行。

### [P1] 5. `ListSlotAssignments` 的只读降级路径会把**陈旧的** HashSlotTable 装回 Router，且 `UpdateHashSlotTable` 完全不比较 version

- **位置**：`pkg/cluster/cluster.go:1630-1655`（降级分支）、`pkg/cluster/cluster.go:1221-1234`（`syncRouterHashSlotTableFromStore`）、`pkg/cluster/router.go:40-46`（无 version 比较的 Store）、`pkg/cluster/cluster.go:1236-1265`（同时推给所有本地状态机）
- **类别**：分布式一致性
- **代码**：

  `pkg/cluster/cluster.go:1630-1655`：
  ```go
  func (c *Cluster) ListSlotAssignments(ctx context.Context) ([]controllermeta.SlotAssignment, error) {
      if c.controllerClient != nil {
          err := c.retryControllerCommand(ctx, func(attemptCtx context.Context) error { ... })
          if err == nil { return assignments, nil }
          if !controllerReadFallbackAllowed(err) || c.controllerMeta == nil {
              return nil, err
          }
      }
      if c.controllerMeta != nil {
          assignments, err := c.controllerMeta.ListAssignments(ctx)   // ← 本地 raft follower 副本，可能落后
          if err != nil { return nil, err }
          if err := c.syncRouterHashSlotTableFromStore(ctx); err != nil {   // ← 并且把陈旧表装回 Router
              return nil, err
          }
          return assignments, nil
      }
  ```

  `pkg/cluster/cluster.go:1715-1721`（哪些错误允许降级）：
  ```go
  func controllerCommandRetryAllowed(err error) bool {
      return errors.Is(err, ErrNotLeader) ||
          errors.Is(err, ErrNoLeader) ||
          errors.Is(err, context.DeadlineExceeded)
  }
  ```

  `pkg/cluster/router.go:40-46`（**没有任何单调性检查**）：
  ```go
  func (r *Router) UpdateHashSlotTable(table *HashSlotTable) {
      if table == nil {
          r.hashSlotTable.Store(nil)
          return
      }
      r.hashSlotTable.Store(table.Clone())     // 直接覆盖，从不与现有 table.Version() 比较
  }
  ```
  （`HashSlotTable` 确有版本号：`pkg/cluster/hashslot/hashslottable.go:210-215 func (t *HashSlotTable) Version() uint64`，`Cluster.HashSlotTableVersion()` 也对外暴露它 —— 只是写入侧从不用它。）

- **触发路径**：
  1. 节点 A 是 controller 的 **follower**（`startControllerRaftIfLocalPeer` 给所有本地 controller peer 都设了 `c.controllerMeta = host.meta`，不区分 leader/follower），其 controller raft applied index 落后于 leader（刚重启追日志 / 刚从网络分区恢复）。
  2. leader 上已提交一次 hash-slot 变更（例如 hashSlot 7 从物理 slot 3 迁到 slot 5，表版本 V → V+1）。节点 A 通过心跳响应 `applyHashSlotTablePayload` 已经拿到并装载了 V+1，但它本地的 controller meta store 仍停留在 V。
  3. `internal/app/observability.go:651-653` 的周期性 metrics 刷新调用 `ListSlotAssignments(assignmentsCtx)`，其中 `assignmentsCtx` 是 `context.WithTimeout(parent, timeout)` —— 负载高时很容易超时。
  4. `retryControllerCommand` 返回 `context.DeadlineExceeded` → `controllerReadFallbackAllowed` 为 true → `c.controllerMeta != nil` → 进入降级分支。
  5. `syncRouterHashSlotTableFromStore` 从本地 store 读到 **版本 V** 的表，调用 `c.updateRuntimeHashSlotTable(table)`。
  6. `router.UpdateHashSlotTable(V)` 无条件覆盖内存中的 V+1 —— 路由表**版本回退**；紧接着 `updateRuntimeHashSlotTable`（`cluster.go:1250-1263`）对每个已注册状态机调用 `UpdateOwnedHashSlots(table.HashSlotsOf(slotID))`，于是 slot 3 的状态机**重新声称自己拥有 hashSlot 7**。
  7. 此后节点 A 上 `SlotForKey`/`HashSlotForKey` 把 hashSlot 7 的写路由回 slot 3，而 slot 3 的状态机不再对它 fence（因为它认为自己是 owner）→ 写入落到错误的物理 Slot。
  8. 同一降级分支在 `ListSlotAssignmentsStrict` 的 leader 分支（`cluster.go:1660-1668`）和 `agent.SyncAssignments`（`agent.go:75-84`）中重复出现。另外 `managedSlotsReady`（`cluster.go:1734`）在 `WaitForManagedSlotsReady` 轮询里也走 `ListSlotAssignments`。
- **后果**：一个纯只读的观测/指标刷新路径可以让节点的 hash-slot 路由表回退到旧版本，并让已经交出 hash slot 的物理 Slot 重新接受该 hash slot 的写 → 同一 key 的写在集群内落到两个不同的物理 Slot（split-brain 式的数据分叉）。超时越频繁（正是高负载时）回退越频繁。
- **建议**：`Router.UpdateHashSlotTable` 改为 compare-and-swap 语义，只接受 `table.Version() > current.Version()` 的更新（`nil` 清空另行走显式 API）；只读 API 的本地降级路径不应有副作用 —— 把 `syncRouterHashSlotTableFromStore` 从 `ListSlotAssignments` / `ListSlotAssignmentsStrict` 中移除，只保留在权威（leader 或心跳响应）来源上。

### [P0] 6. `handleForwardRPC` 对任何能连上端口的客户端开放"向任意 Slot 提交任意 raft 命令"，无任何鉴权或来源校验

- **位置**：`pkg/cluster/forward.go:50-74`（handler）、`pkg/cluster/transport_glue.go:57`（注册）、`pkg/cluster/codec.go:212-219`（payload 解码）
- **类别**：安全 / 分布式一致性
- **代码**：

  `pkg/cluster/forward.go:49-73`：
  ```go
  // handleForwardRPC is the server-side RPC handler for forwarded proposals.
  func (c *Cluster) handleForwardRPC(ctx context.Context, body []byte) ([]byte, error) {
      slotID, cmd, err := decodeForwardPayload(body)
      if err != nil {
          return encodeForwardResp(errCodeNoSlot, nil), nil
      }
      if c.stopped.Load() {
          return encodeForwardResp(errCodeTimeout, nil), nil
      }
      _, err = c.runtime.Status(multiraft.SlotID(slotID))
      if err != nil {
          return encodeForwardResp(errCodeNoSlot, nil), nil
      }
      future, err := c.runtime.Propose(ctx, multiraft.SlotID(slotID), cmd)   // ← cmd 原封不动来自线路
  ```

  `pkg/cluster/codec.go:212-219`（除了 8 字节长度下限外没有任何校验）：
  ```go
  func decodeForwardPayload(payload []byte) (slotID uint64, cmd []byte, err error) {
      if len(payload) < 8 {
          return 0, nil, fmt.Errorf("forward payload too short: %d", len(payload))
      }
      slotID = binary.BigEndian.Uint64(payload[0:8])
      cmd = payload[8:]
      return slotID, cmd, nil
  }
  ```

  `pkg/cluster/transport_glue.go:57`（注册在同一个无认证监听器上）：
  ```go
  t.rpcMux.Handle(rpcServiceForward, handleForward)
  ```

- **触发路径**：
  1. `pkg/transport` 无认证、无 TLS（unit 15 已确认并经我复核：非测试 `pkg/transport/*.go` 中 `tls\.|Token|auth|Secret|Credential` 零命中；`Server.serveConn` 在 `pkg/transport/server.go:143-160` 里对任何 accept 到的连接直接建 `MuxConn` 并开始 dispatch，没有任何握手校验）。
  2. 攻击者 TCP 连上 `Config.ListenAddr`，发一个 `MsgTypeRPCRequest` 帧，payload 为 `encodeRPCServicePayload(rpcServiceForward, encodeForwardPayload(slotID, cmd))`，这两个编码格式都在开源仓库里（`codec.go:204-208`）。
  3. `handleForwardRPC` 唯一的"校验"是 `c.runtime.Status(slotID)` 是否报错 —— 那只确认本节点持有该 Slot，与调用方身份无关。
  4. `c.runtime.Propose(ctx, slotID, cmd)` 直接把攻击者的 `cmd` 送进 raft 日志。`cmd` 的格式是 `encodeProposalPayload` 产生的 `[hashSlot:2][命令体:N]`（`codec.go:186-192`），命令体由 `pkg/slot/fsm` 的状态机解释 —— 即攻击者可以伪造任意元数据变更命令（channel 创建/删除、成员变更、消息元数据写入等）。
  5. 一旦提交，该命令会被复制到该 Slot 的所有副本并 apply —— 这是**持久化、已复制、不可区分于合法写**的数据篡改。
- **后果**：无凭证的网络可达方可对集群任意 Slot 执行任意状态机写入，写入会被 raft 复制并持久化。这是数据完整性的完全丧失，且事后无法从 raft 日志区分合法与伪造的提案（日志里不带调用方身份）。配合 `rpcServiceManagedSlot` / `rpcServiceController` 同样注册在这个 mux 上，运维面命令也一并暴露。
- **建议**：给 `pkg/transport` 加节点间双向认证（mTLS 或预共享 token 的连接握手），并让 `handleForwardRPC` / `handleControllerRPC` / `handleManagedSlotRPC` 只接受来自已认证的、且在当前 membership 中的节点的请求；forward payload 里带上发起节点 ID 并与认证身份比对。

### [P1] 7. `handleForwardRPC` 用连接级、永不超时的 ctx 等待提案提交 —— 失去 quorum 的 Slot 会持续泄漏 goroutine 和 pending future

- **位置**：`pkg/cluster/forward.go:62-72`；ctx 来源 `pkg/transport/server.go:146`；对照的本地路径 `pkg/cluster/cluster.go:1023-1031`
- **类别**：资源泄漏 / 并发
- **代码**：

  `pkg/cluster/forward.go:62-72`（`ctx` 是 handler 收到的连接级 ctx，未再加 deadline）：
  ```go
  future, err := c.runtime.Propose(ctx, multiraft.SlotID(slotID), cmd)
  if err != nil {
      return encodeForwardResp(errCodeNotLeader, nil), nil
  }
  result, err := future.Wait(ctx)          // ← 无 deadline，只在 TCP 连接断开时才返回
  if err != nil {
      if ctx.Err() != nil {
          return encodeForwardResp(errCodeTimeout, nil), nil
      }
      return encodeForwardResp(errCodeNotLeader, nil), nil
  }
  ```

  `pkg/transport/server.go:143-146`（连接级 ctx 只有 cancel，没有 timeout）：
  ```go
  func (s *Server) serveConn(raw net.Conn) {
      defer s.wg.Done()

      connCtx, cancelConn := context.WithCancel(context.Background())
  ```

  对照：本地 propose 路径用的是被 `ForwardRetryBudget` 约束的 `attemptCtx`（`pkg/cluster/cluster.go:1023-1031`）：
  ```go
  future, err := c.runtime.Propose(attemptCtx, slotID, payload)
  ...
  result, err := future.Wait(attemptCtx)
  ```

- **触发路径**：
  1. 节点 B 是 slot 5 的 raft leader，随后失去 quorum（3 副本中 2 个宕机 / 被隔离）。B 尚未触发选举超时，仍自认为 leader。
  2. 节点 A 的业务写走 `ProposeWithHashSlotResult` → `router.LeaderOf(slot5)` 返回 B → `forwardToLeaderResult` → B 的 `handleForwardRPC`。
  3. B 上 `c.runtime.Propose` 成功（entry 被 append 到本地日志），`future.Wait(ctx)` 阻塞等提交。
  4. 该 entry **永远不会提交**（无 quorum）。`pkg/slot/multiraft` 只在 4 种情况下 fail pending future：`Runtime.Close`（`api.go:21`）、`CloseSlot`（`api.go:115`）、`persistReady` 出错（`slot.go:722`）、`fail(fatalErr)`（`slot.go:326-328`）。**丢失 leadership 不在其中** —— 我 grep 了全部 `failPending` 调用点确认。
  5. A 侧 `retry.Do` 在 `ForwardRetryBudget`（默认 300ms）后放弃并返回错误；但 A 放弃 RPC 只是 `MuxConn.RPC` 的 `defer mc.pending.Delete(reqID)`（`pkg/transport/conn.go:54`），**不关闭连接**（这是池化的长连接）。因此 B 上的 `connCtx` 不会被 cancel。
  6. 于是 B 上每一次被转发的写都永久留下：1 个阻塞在 `future.Wait` 的 `handleRPCRequest` goroutine（还持有 `s.wg` 计数）+ 1 个 slot 内的 pending future + 攻击/业务传来的 `cmd` 字节。业务层重试 → 稳定速率累积。在 slot 5 上仅 100 writes/s 的正常负载下，一小时即 36 万个永不退出的 goroutine。
- **后果**：一个失去 quorum 的 Slot（单点故障的常见形态）会让**所有转发写入方向的 leader 节点**以业务写入速率持续泄漏 goroutine 与内存，最终 OOM —— 故障从"一个 Slot 不可写"放大成"leader 节点进程死亡"。本地 propose 路径有 `ForwardRetryBudget` 约束，唯独转发路径没有，是明显的不对称。
- **建议**：`handleForwardRPC` 在 `Propose`/`Wait` 之前用 `context.WithTimeout(ctx, cfg.ForwardTimeout)`（`Config.ForwardTimeout` 已存在且默认 5s，但当前在转发服务端完全没被使用）包一层，并在超时时返回 `errCodeTimeout`；更彻底的修复是让 `pkg/slot/multiraft` 在 leadership 丢失时 fail 掉所有 pending proposal future。

### [P1] 8. join 模式下 `Start()` 会无限期重试 JoinCluster，且此时 SIGTERM 无法停止进程（与 App 生命周期锁互锁）

- **位置**：`pkg/cluster/cluster.go:206-210`（`context.Background()`）、`pkg/cluster/cluster.go:322-374`（无界重试循环）、`pkg/cluster/cluster.go:398-408`（重试判定）
- **类别**：正确性 / 可运维性（启动期死锁）
- **代码**：

  `pkg/cluster/cluster.go:206-210`：
  ```go
  c.startControllerClient()
  if err := c.joinClusterIfConfigured(context.Background()); err != nil {   // ← 没有 deadline
      c.Stop()
      return err
  }
  ```

  `pkg/cluster/cluster.go:330-372`（唯一出口是成功 / 不可重试错误 / `c.stopped`）：
  ```go
  for {
      if c.stopped.Load() {
          return transport.ErrStopped
      }
      resp, err := c.controllerClient.JoinCluster(ctx, req)
      if err == nil { ...; return nil }
      if !retryableJoinClusterError(err) {
          return err
      }
      select {
      case <-ctx.Done():          // ctx 是 Background，永远不会 Done
          return ctx.Err()
      case <-time.After(backoff): // backoff 上限 1s
      }
  ```

  `pkg/cluster/cluster.go:398-408`（默认可重试 —— 只有 3 类错误不重试）：
  ```go
  func retryableJoinClusterError(err error) bool {
      var joinErr *joinClusterError
      if errors.As(err, &joinErr) {
          return joinErr.Retryable()
      }
      if errors.Is(err, ErrInvalidConfig) || errors.Is(err, transport.ErrStopped) {
          return false
      }
      return true          // 连接被拒、无 leader、超时…… 全部无限重试
  }
  ```

- **触发路径**：
  1. 节点以 join 模式启动（`len(cfg.Nodes) == 0 && len(cfg.Seeds) > 0`，即 FLOW.md 5.1 ⑥ 描述的 dynamic membership 部署）。
  2. seeds 指向的 controller 集群此刻没有 leader（正在选举、或 seed 进程起来了但 controller raft 还没 bootstrap）。`JoinCluster` 返回 `ErrNoLeader` 或 `joinErrorTemporary`。
  3. `retryableJoinClusterError` 判为可重试 → `select` 等 backoff（上限 1s）→ 回到循环顶部。`ctx` 是 `context.Background()`，`<-ctx.Done()` 分支永远不会命中。循环唯一的另一个出口是 `c.stopped.Load()`。
  4. `c.stopped` 只由 `Cluster.Stop()` 设置。生产路径上 `Cluster.Stop()` 只能经 `App.Stop()` → `stopLifecycleManager` → `stopCluster` 到达（`internal/app/lifecycle.go:496-508`）。
  5. 但 `App.Start()`（`internal/app/lifecycle.go:18-23`）在**整个启动期间持有 `a.lifecycle.Lock()`**，而 `App.Stop()`（`internal/app/lifecycle.go:40-45`）第一件事就是 `a.lifecycle.Lock()`。
  6. 于是：`App.Start()` 阻塞在 `cluster.Start()` → `joinClusterWithRetry` 的无限循环里 → SIGTERM 触发的 `App.Stop()` 阻塞在 lifecycle 锁上 → 永远不会执行到 `Cluster.Stop()` → `c.stopped` 永远是 false → 循环永不退出。**互相等待，进程只能被 SIGKILL 杀掉。**
  7. `joinClusterWithRetry` 全程没有任何日志输出，运维在现场看不到任何"正在重试 join"的线索。
- **后果**：join 模式下只要 controller 暂时无 leader，新节点就会在启动期硬挂死，且无法优雅关闭（编排系统的 SIGTERM 超时后只能 SIGKILL）。在滚动升级 / 全集群冷启动这类 controller 选举窗口较长的场景下，这是可重现的启动期死锁。
- **建议**：`joinClusterIfConfigured` 使用一个带上限的 ctx（新增 `Timeouts.JoinClusterBudget`，或复用 `ControllerLeaderWait`），超时后返回错误让 `Start()` 快速失败；同时让 `App.Start()` 不要在阻塞的组件启动期间持有与 `Stop()` 相同的互斥锁（或让 `Cluster.Stop()` 通过一个独立的、不经生命周期锁的 stop channel 中断 join 循环）；并在每次 join 重试时打一条 WARN 日志。

### [P2] 9. `Rebalance` 逐个启动迁移，首个失败即返回，已启动的迁移既不回滚也不告知调用方

- **位置**：`pkg/cluster/operator.go:265-276`
- **类别**：分布式正确性（中间态不可恢复）
- **代码**：
  ```go
  func (c *Cluster) Rebalance(ctx context.Context) ([]MigrationPlan, error) {
      table := c.GetHashSlotTable()
      if table == nil {
          return nil, ErrNotStarted
      }
      plan := ComputeRebalancePlan(table)
      for _, migration := range plan {
          if err := c.StartHashSlotMigration(ctx, migration.HashSlot, migration.To); err != nil {
              return nil, err            // ← 丢弃 plan，前面已成功启动的迁移无人知晓
          }
      }
      return plan, nil
  }
  ```
- **触发路径**：
  1. 运维通过管理面 `POST /slots/rebalance`（`internal/usecase/management/slot_recover_rebalance.go:78-95 RebalanceSlots`）触发。
  2. `ComputeRebalancePlan` 产出 N 项迁移计划，`Rebalance` 顺序调用 `StartHashSlotMigration`。
  3. 第 k 项（1 <= k < N）因 controller leader 切换返回 `ErrNotLeader`（`submitHashSlotMigration` → `retryControllerCommand` 在 `ControllerLeaderWait` 预算耗尽后返回错误）。
  4. `Rebalance` 立刻 `return nil, err`。此时前 k-1 项迁移**已经被 controller 提交并进入 active 状态**，但返回给调用方的 plan 是 `nil`。
  5. 上层 `RebalanceSlots` 把错误直接透传（`slot_recover_rebalance.go:90-93`），HTTP 响应里没有任何"哪些 hash slot 已经开始迁移"的信息。
  6. 运维重试 `POST /slots/rebalance` 时，`RebalanceSlots` 的前置检查 `len(a.cluster.GetMigrationStatus()) > 0` 会返回 `ErrSlotMigrationsInProgress` —— 于是既无法继续也无法回滚，只能手工对每个 hash slot 调 abort。
- **后果**：rebalance 变成一个非原子、无进度反馈、失败后需要人工逐个收拾的操作。（注：`StartHashSlotMigration` 有 `EnableHashSlotMigration` 开关保护，默认关闭，所以当前生产影响受限于显式开启该实验开关的部署。）
- **建议**：`Rebalance` 出错时把**已成功启动**的那部分 plan 连同错误一起返回（`return started, err`），让管理面能展示中间态；或改为先把整个 plan 作为一条 controller 命令提交，由 controller 状态机原子地转成 N 个迁移。

### [P2] 10. `RecoverStrategyLatestLiveReplica` 只统计可达副本数，完全没有"latest"（日志水位比较）逻辑

- **位置**：`pkg/cluster/operator.go:147-190`
- **类别**：正确性 / 架构
- **代码**：
  ```go
  func (c *Cluster) recoverSlotWithAssignments(ctx context.Context, slotID uint32, strategy RecoverStrategy, assignments []controllermeta.SlotAssignment) error {
      if strategy != RecoverStrategyLatestLiveReplica {
          return ErrInvalidConfig
      }
      ...
      reachable := 0
      for _, peer := range peers {
          ...
          respBody, err := c.RPCService(ctx, multiraft.NodeID(peer), multiraft.SlotID(slotID), rpcServiceManagedSlot, body)
          if err != nil {
              continue                                    // 静默吞掉
          }
          if _, err := decodeManagedSlotResponse(respBody); err == nil {
              reachable++                                 // 只数个数，不看 commit/applied index
          }
      }
      if reachable < len(peers)/2+1 {
          return ErrManualRecoveryRequired
      }
      return nil                                          // 不执行任何恢复动作
  }
  ```
- **触发路径**：
  1. 运维在某个 Slot 失去 leader 后调用管理面 `POST /slots/{id}/recover`（`internal/access/manager/slot_operator.go:99`）→ `management.RecoverSlot` → `cluster.RecoverSlotStrict` → 本函数。
  2. 函数对每个 desired peer 发一个 `managedSlotRPCStatus` 探活。`managedSlotRPCStatus` 的响应里是带 commit/applied watermark 的（`API.SlotLogStatusOnNode` 走的就是这条 RPC），但这里 `decodeManagedSlotResponse` 的结果被 `_` 丢弃，**水位完全没有参与判断**。
  3. 只要过半 peer 应答，函数 `return nil`，上层据此返回 `Result: "quorum_reachable"`。
  4. 即：策略名承诺的"从最新的存活副本恢复"从未发生；这个 API 只是一个探活接口，不会修复任何东西。
- **后果**：运维调用 "recover" 得到成功响应，但 Slot 的实际状态没有任何改变；在真正需要人工干预的场景下会给出"已恢复"的误导信号。另外 `err != nil { continue }` 把网络错误与"该副本确实不可达"混为一谈，无法区分。
- **建议**：要么把 `RecoverStrategyLatestLiveReplica` 实现为真正的恢复（比较各 peer 的 applied index，选水位最高者做 leader transfer 或强制成员变更），要么把 API 和策略常量重命名为 `CheckQuorumReachable` 之类，并在 `API` 注释与 FLOW.md 中明确它只做探测。

### [P2] 11. 未认证的 observation hint 帧可用一个超大 `LeaderGeneration` 永久关闭本节点的 hint 驱动调和

- **位置**：`pkg/cluster/cluster.go:1894-1913`（handler，注册在 `pkg/cluster/cluster.go:208`）；判定逻辑 `pkg/cluster/observation_hint.go:116-136`
- **类别**：安全 / 可用性
- **代码**：

  `pkg/cluster/cluster.go:1894-1913`：
  ```go
  func (c *Cluster) handleObservationHintMessage(body []byte) {
      hint, err := decodeObservationHint(body)
      if err != nil {
          return
      }
      c.handleObservationHint(hint)
  }

  func (c *Cluster) handleObservationHint(hint observationHint) bool {
      if c == nil || c.wakeState == nil {
          return false
      }
      accepted := c.wakeState.observeHint(uint64(c.controllerLeaderID()), hint)
  ```

  `pkg/cluster/observation_hint.go:116-136`（唯一的来源校验是 leader ID 相符；generation 单调递增即被接受）：
  ```go
  if currentLeaderID != 0 && hint.LeaderID != currentLeaderID {
      return false
  }
  ...
  if s.hint.LeaderID != 0 {
      if hint.LeaderID == s.hint.LeaderID && hint.LeaderGeneration < s.hint.LeaderGeneration {
          return false                                  // ← 之后真 leader 的 hint 全被这条挡掉
      }
      if hint.LeaderID != s.hint.LeaderID || hint.LeaderGeneration > s.hint.LeaderGeneration {
          s.hint = cloneObservationHint(hint)
          s.pending = true
          return true
      }
  }
  ```

- **触发路径**：
  1. 攻击者连上目标节点的集群端口（无认证，见发现 6），读一次管理面或直接枚举小整数拿到当前 controller leader 的 NodeID（集群节点 ID 是 1、2、3 这类小整数）。
  2. 发送一个 `msgTypeObservationHint` 帧，`LeaderID` = 当前 leader，`LeaderGeneration` = `math.MaxUint64`。
  3. `observeHint` 的 leader ID 检查通过；`hint.LeaderGeneration > s.hint.LeaderGeneration` 成立 → `s.hint` 被替换为攻击者的 hint，generation 锁死在 MaxUint64。
  4. 此后真实 controller leader 发来的所有 hint（generation 是真实的、远小于 MaxUint64）都在第 128 行 `hint.LeaderGeneration < s.hint.LeaderGeneration` 被拒绝，`observeHint` 返回 false → `signalObservationWake()` 不再被调用 → `wakeObserver` 永远不再被唤醒。
  5. 该节点只剩 `slowSyncObserver`（`ObservationSlowSyncInterval`，默认 2s）这一条兜底路径来同步 assignment；hint 驱动的秒级以下调和被永久关闭，直到 controller leader 换届（换届后 `LeaderID` 不同，第 131 行条件重新成立）。
- **后果**：单个未认证报文即可把目标节点的观测调和从"事件驱动（毫秒级）"降级为"2 秒轮询兜底"，显著拉长成员变更 / 迁移 / 故障切换在该节点上的收敛时间。对集群中每个节点各发一个报文即可全局降级。
- **建议**：随发现 6 一并修复传输层认证；另外 `observeHint` 应校验 hint 的 `LeaderGeneration` 与本地 `controllerHost.leaderGeneration()` / 已应用的 observation state 一致，而不是只做单调性比较。

## 已排除的候选项

- `pkg/cluster/observer.go:53-67` / `:115-129`（`observerLoop.Stop()` / `signalLoop.Stop()` 的 `if l.stop == nil` → `close(l.stop)` → `l.stop = nil` 无锁 check-then-act，看起来是典型双关闭 panic）——**我无法证明并发 `Stop()` 可达，因此排除**。论证：(1) 这些 loop 只被 `Cluster.Stop()`（`cluster.go:592-625`）调用，全仓 grep 无其它调用方；(2) 生产上 `Cluster.Stop()` 只能经 `internal/app/lifecycle.go:496-508 stopCluster`，而该函数第一行是 `if !a.clusterOn.Swap(false) { return }`，是一次性的；(3) `Start()` 的 4 个 `c.Stop()` 错误分支（`cluster.go:199,203,208,213`）中，前 3 个都发生在 `startObservationLoop()` 之前（此时 8 个 observer 字段全是 nil）；第 4 个在 `seedLegacySlotsIfConfigured()` 之后，而该函数在 `c.cfg.ControllerEnabled()` 为真时**立即返回 nil**（`cluster.go:529-532`），`startObservationLoop` 又在 `c.controllerClient == nil` 时立即返回（`cluster.go:488-490`）、而 `controllerClient` 只在 `ControllerEnabled()` 为真时才被创建（`cluster.go:301-303`）—— 两个条件互斥，所以"observer 存活 + Start 错误路径调 Stop"这个组合不可能出现。**结论：代码模式确实脆弱（任何新增的 Stop 调用方都会让它变成真 panic），但当前不可达。**
- `pkg/cluster/cluster.go:877,903,924`（`handleRaft*` 在 `runtime.Close()` 之后仍调用 `c.runtime.Step`，疑似 use-after-close panic）—— `pkg/slot/multiraft/api.go:125-132` 的 `Step` 和 `:144-152` 的 `Propose` 都在最前面做 `r.mu.RLock(); if r.closed { return ErrRuntimeClosed }`，不会 panic。守卫失效的问题已并入发现 2。
- `pkg/cluster/agent.go:142-150 assignmentReconciler()`（`if a.reconciler == nil { a.reconciler = newReconciler(a) }` 无锁惰性初始化）—— 生产路径上 `ApplyAssignments` 只从 `syncObservationDeltaOnce`（`cluster.go:1959`）调用，而它的两个调用方 `wakeReconcileOnce`（`cluster.go:706`）和 `slowSyncOnce`（`cluster.go:726`）都先做 `c.observationSyncStart()` 的 `CompareAndSwap(false, true)` 互斥；另一个调用方 `observeOnce` 是死代码（发现 4）。当前串行，排除。
- `pkg/cluster/retry.go:44-46`（`if r.Interval <= 0 { continue }` 会变成不 sleep 的忙等循环）—— 全部构造点（`cluster.go:1005-1011`、`operator.go:583-589`、`slot_manager.go` 等）的 Interval 都来自 `scaleDerivedInterval`，而它在 `defaultInterval > 0` 时有 `minimumDerivedRetryOrPollPeriod = 5ms` 的下限（`readiness.go:104-119`），对应的 `defaultInterval` 常量（50ms / 100ms）都非零。没有 Interval 为 0 的实际构造点，排除。
- `pkg/cluster/agent.go` 与 `pkg/cluster/retry.go` 整体 —— 通读后确认与侦察结论一致，除上面两条外无其它问题。`runtimeState`（`runtime_state.go`）的所有读写都在 `sync.RWMutex` 下且进出都做切片拷贝，`StaticDiscovery`（`static_discovery.go`）的 map 在构造后只读，`errors.go` 纯声明 —— 均干净。
- `pkg/cluster/readiness.go:132` G115（`uint32(slotID)`，`multiraft.SlotID` 是 `uint64`）—— 误报。`controllermeta.SlotRuntimeView.SlotID` 本身就是 `uint32`，且 `Config.validate`（`config.go:169-171`）强制 `InitialSlotCount <= math.MaxUint16`，slot ID 在整个 controller 元数据里都是 uint32 域。
- `pkg/cluster/readiness.go:140,143` G115（`uint32(len(currentVoters))` / `uint32(len(currentPeers))`）—— 误报，切片长度是副本数（个位数）。
- `pkg/cluster/config.go:267` G115（`uint16(c.InitialSlotCount)`）—— 误报，紧邻的 `c.InitialSlotCount <= math.MaxUint16` 就是上界保证（`config.go:266`）。
- `pkg/cluster/config.go:336` G115（`uint32(len(c.Slots))`）—— 误报，静态配置长度。
- `pkg/cluster/cluster.go:566,571,582,587,976` G115（`uint32(g.SlotID)` / `uint32(slotID)` 传给 ObserverHooks）—— 误报，同上，slot ID 域被 `validate` 限制在 uint16 以内，且这些值只用于 metrics label。
- `pkg/cluster/operator.go:352,353` G115（`uint32(slotID)` 求 maxSlotID）—— 误报，同上。
- `pkg/cluster/cluster.go:1412` G115（`hashSlots := []uint16{uint16(slotID)}`，uint64→uint16）—— **保留疑虑但排除**。理论上 `nextSlotDefinition`（`operator.go:357`）返回 `maxSlotID+1` 且不与 `MaxUint16` 比较，连续 65536 次 `AddSlot` 后 `uint16(slotID)` 会回绕，让新 Slot 的状态机误认领另一个 Slot 的 hash slot。但 (1) `AddSlot` 被 `EnableHashSlotMigration`（默认关闭）挡住；(2) 需要 6.5 万次 AddSlot；(3) 这只是 `router.HashSlotsOf(slotID)` 为空时的 fallback 分支。写不出现实的触发序列，排除。
- `pkg/cluster/cluster.go:404-437 applyClusterNodes` 被只读 API（`ListNodes`）调用并可能通过 `DynamicDiscovery.UpdateNodes` → `OnAddressChange` → `Pool.ClosePeer`（`transport_glue.go:92-96`）踢掉所有连接 —— 考虑过"controller 返回空/缩小的节点列表 → 丢连接"这条路径，但 `applyClusterNodes` 先把 `c.cfg.Nodes` 作为底线合入 `configsByID`（`cluster.go:414-420`），静态部署下不可能丢节点；join 模式下 `cfg.Nodes` 为空，但 controller 返回 `err == nil` 且节点列表不含本节点已加入的成员这一状态，与"本节点已成功 join"自相矛盾。构造不出触发序列，排除。（`ListSlotAssignments` 那条确有副作用的路径已单独作为发现 5 保留。）
- `pkg/cluster/discovery.go:11 Discovery.GetNodes()` —— 接口方法在非测试代码中零调用方（`transport.Pool` 只用 `Resolve`）。属于死接口方法，价值太低不单列为发现。
- `pkg/transport` 无认证 / `pkg/transport/server.go:186` 无 `recover()` —— 由 unit 15 负责，本报告只在发现 1、6、11 中作为前提引用，不重复计数。

## 本分片整体评价

这部分代码是**在跑的生产 v1 栈**，工程完成度不低：`Router` 用 `atomic.Pointer`、`runtimeState` / `DynamicDiscovery` 的锁边界干净（通知回调刻意放在锁外）、`Retry` 与超时常量体系化、`controllerHost` 的方法普遍带 nil-receiver 守卫。缺陷集中在两个地方：**(a) 所有入站帧都默认对端是可信的同集群节点**，而传输层实际上没有任何认证 —— 这让 `handleRaftSnapshotChunkMessage`（一个 96 字节报文 `make([]byte, total)` 打爆内存，发现 1）、`handleForwardRPC`（任意人向任意 Slot 提交任意 raft 命令，发现 6）、`handleObservationHintMessage`（一个报文永久关闭 hint 调和，发现 11）三个 handler 全部变成可直接利用的入口；**(b) 生命周期与降级路径不严谨** —— `Stop()` 把监听端口放在最后关且无锁置 nil（发现 2）、join 模式 `Start()` 无界重试并与 App 生命周期锁互锁到只能 SIGKILL（发现 8）、只读 API 的本地降级会把陈旧的 HashSlotTable 装回 Router 且 `UpdateHashSlotTable` 根本不比较 version（发现 5）。

**最该优先处理的一条是发现 1**：它不需要任何前置条件、不需要猜任何 ID、单个 TCP 报文就能确定性地杀死集群里任意一个节点，且 Go 的 OOM 是 `fatal error` 无法 recover。发现 6 的危害面更大（持久化数据篡改）但修复要动传输层认证，周期更长；发现 1 只需在 `decodeRaftSnapshotChunkBody` 里给 `total` 加一个上界即可立刻止血。

另外值得单独提一句的是发现 3：`migrationObserver` 用一个名叫 `defaultControllerObservationTimeout`（200ms）的常量当循环周期，而它调用的 `ListHashSlotMigrationStates` 用的是**空 Span 的全 keyspace 迭代**。在迁移功能默认关闭的集群上，这是每秒 5 次、随元数据规模线性增长的纯浪费，属于低风险高收益的性能修复。
