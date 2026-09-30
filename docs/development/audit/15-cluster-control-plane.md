# cluster 控制面宿主与编解码 (15-cluster-control-plane)

> 单元范围：`pkg/cluster/` 控制面宿主侧（controller host / handler / client / raft 诊断读写）与控制面线路编解码。
> **本单元是 v1 生产代码**（`cmd/wukongim/main.go` → `internal/app` → `pkg/cluster`），严重度按实际算。

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/cluster/codec_control.go | 2926 | 是 |
| pkg/cluster/controller_host.go | 928 | 是 |
| pkg/cluster/codec_managed.go | 667 | 是 |
| pkg/cluster/controller_handler.go | 654 | 是 |
| pkg/cluster/controller_client.go | 612 | 是 |
| pkg/cluster/controller_metadata_snapshot.go | 409 | 是 |
| pkg/cluster/codec.go | 235 | 是 |
| pkg/cluster/controller_raft_status.go | 227 | 是 |
| pkg/cluster/controller_log_entries.go | 175 | 是 |
| pkg/cluster/controller_raft_compaction.go | 84 | 是 |
| pkg/cluster/controller_context.go | 32 | 是 |
| pkg/cluster/controller_info.go | 10 | 是 |

先读了 `pkg/cluster/FLOW.md`（614 行）再读代码。

**为建立触发路径而额外读过的包外上下文**（不计入本单元发现）：`pkg/controller/meta/store.go`、
`pkg/transport/server.go`、`pkg/cluster/node_health_scheduler.go`、`pkg/cluster/slot_log_entries.go`、
`pkg/cluster/cluster.go:221,1893`。

### 关键前置事实（已核实，两条 P0 都依赖它）

1. **`pkg/transport` 与 `pkg/cluster` 全包 0 处 `recover()`**
   （`grep -rn "recover()" pkg/transport pkg/cluster` → 无匹配；全仓仅 `internal/runtime/channelmeta/activate.go:41` 有一处）。
2. **入站 RPC 在裸 goroutine 中执行**：`pkg/transport/server.go:186` `go s.handleRPCRequest(connCtx, mc, holder.handler, copied)`，
   只有 `defer s.wg.Done()`，没有 `recover`。因此解码路径中的任何 panic = **整个进程退出**。
3. **传输层无任何认证**：`grep -n "token\|auth\|handshake\|magic" pkg/transport/*.go` 在 `conn.go`/`frame.go` 无匹配；
   `server.go:139` `go s.serveConn(raw)` 接受任意 TCP 连接。`JoinToken` 只在 `join_cluster` 业务分支里校验
   （`controller_handler.go:533`），**不是连接级校验**。
4. **解码发生在一切授权检查之前**：`controllerHandler.Handle` 的第一条语句就是
   `decodeControllerRequest(body)`（`controller_handler.go:20`），leader 检查、
   `isNodeAuthorizedForObservation`、nil 检查全在其后。`cluster.go:221` 无条件注册
   `c.handleControllerRPC`，因此**非 controller 节点同样可被打崩**。

---

## 发现

### [P0] 1. 控制面解码器用线路长度前缀直接 `make()`，无上界 → 未认证远程打崩进程

- **位置**：`pkg/cluster/codec_control.go:2823-2838`（`readUint64Slice`）、`:2848-2863`（`readUint32Slice`）、
  `:2484-2499`（`consumeRuntimeViews`）；入站可达点 `:1893`、`:1805`、`:2029`、`:2620`
- **类别**：安全 / 远程输入可达的 panic
- **代码**（`codec_control.go:2848-2856`）：
  ```go
  func readUint32Slice(src []byte) ([]uint32, []byte, error) {
  	count, rest, err := readUvarint(src)   // count 完全来自线路，最大 2^64-1
  	if err != nil {
  		return nil, nil, err
  	}
  	values := make([]uint32, 0, count)     // ← 先分配 count*4 字节，之后才逐个校验
  	for i := uint64(0); i < count; i++ {
  		value, next, err := readUint32(rest)
  ```
- **触发路径**（已用可运行 PoC 验证，见下）：
  1. 攻击者 TCP 连到任意节点的 cluster 监听端口（`WK_CLUSTER_ADDR`），无需凭证。
  2. 发一个 `MsgTypeRPCRequest` 帧，serviceID = `rpcServiceController`(14)。
  3. body = `[codecVersion=1][kind=7 (fetch_observation_delta)][slotID:4][payloadLen uvarint][payload]`；
     payload = `[leaderID:8][leaderGeneration:8][revisions:32][forceFullSync:1][uvarint count]`
     —— 共约 **58 字节**，其中最后 9 字节是 `count = 2^62` 的 uvarint。
  4. `readControllerPayload`(:1702) 只校验 `payloadLen == len(body)`，通过；
     `decodeControllerRequestPayload`(:401) → `decodeObservationDeltaRequest`(:1876)
     → `readUint32Slice(rest[1:])`(:1893)。
  5. `make([]uint32, 0, 2^62)` → `panic: runtime error: makeslice: cap out of range`，
     无 `recover` → **进程退出**。
     若改用 `count = 2^33`（32 GiB），则走真实分配 → `fatal error: runtime: out of memory`（不可恢复）。

  实测（`readUvarint`+`readUint32Slice` 逐字复制原函数）：
  ```
  attacker uvarint bytes: 9 (808080808080808040)
  panic: runtime error: makeslice: cap out of range
  ```
  同样可达的入站点还有三条，无需成为 controller leader 即可触发解码：
  `decodeRuntimeObservationReport:1805`（`runtime_report`，`closedSlots`）、
  `consumeRuntimeViews:2489`（同一请求的 `Views`，元素是较大的 `SlotRuntimeView` 结构体，所需 count 更小）、
  `decodeAddSlotRequest:2029`（`add_slot`）、`consumeRuntimeViewRecordV1:2620`。
- **后果**：单个 ~58 字节报文、无需任何凭证，使集群任意节点进程崩溃；对 controller leader 反复发送即可
  阻止选主收敛，集群控制面完全不可用。滚动升级中版本偏斜的 peer 发出畸形长度前缀也会同样打崩对端。
- **同包内已有正确写法（证明这是遗漏而非设计取舍）** —— 同一个包里存在 **三种**正确的上界惯用法，
  却有 14 处解码点一处都没用：
  - `codec.go:101` `if count > (len(body)-4)/12 { return error }`
  - `codec_managed.go:613,644,664` `if count > uint64(len(rest)) && count != 0 { return ErrInvalidConfig }`
  - `codec_control.go:1015-1018` `const minControllerRaftPeerProgressWireSize = ...; if count > uint64(len(body)/minControllerRaftPeerProgressWireSize) { ... }`

  **全部 14 处缺上界的 `make`**：`codec_control.go:819, 1159, 2182, 2215, 2320, 2489, 2514, 2828, 2853`、
  `codec_managed.go:377`、以及 `codec_control.go:1341, 1430, 1449`（onboarding moves/plan/reasons）、
  `codec_control.go:1188`（onboarding jobs）。
- **建议**：给 `readUvarint` 驱动的每个 `make` 加"count × 单元素最小线路字节数 ≤ 剩余 body 长度"的前置校验
  （直接复用 `decodeControllerRaftStatusResponse` 的写法）；并在 `transport` 的 RPC 分发处补 `recover`，
  使编解码缺陷降级为丢连接而非进程退出。

### [P0] 2. `readString`/`readBytes` 的长度校验被 uint64→int 截断绕过 → 远程切片越界 panic

- **位置**：`pkg/cluster/codec_control.go:2779-2788`、`:2790-2799`、`:2588-2602`；
  `pkg/cluster/codec_managed.go:216-220`
- **类别**：安全 / 远程输入可达的 panic
- **代码**（`codec_control.go:2779-2788`）：
  ```go
  func readString(src []byte) (string, []byte, error) {
  	length, rest, err := readUvarint(src)      // length: uint64
  	if err != nil {
  		return "", nil, err
  	}
  	if len(rest) < int(length) {               // int(2^63) = 负数 → 比较恒为 false
  		return "", nil, ErrInvalidConfig       // ← 永远不会走到
  	}
  	return string(rest[:length]), rest[length:], nil   // ← 切片越界 panic
  }
  ```
- **触发路径**（已用 PoC 验证）：任何调用 `readString`/`readBytes` 的**入站**解码点，
  把长度前缀写成 `length = 2^63` 的 uvarint 即可。最短路径：
  `controller_logs` 请求 → `decodeControllerLogEntriesRequest`；或
  `join_cluster` → `decodeJoinClusterRequest`（:2067，Name/Addr/Token/Version 四个 `readString`，
  且 token 校验在解码**之后**）；或 `create_onboarding_plan` → `decodeNodeOnboardingPlanRequest`(:1077)。
  实测：
  ```
  length=9223372036854775808  int(length)=-9223372036854775808  -> guard 'len(rest) < int(length)' is false
  panic: runtime error: slice bounds out of range [:9223372036854775808] with capacity 6
  ```
- **后果**：同 P0-1，未认证远程进程崩溃。`consumeControllerRecord:2597` 与
  `decodeManagedSlotResponse:216-220`（`messageEnd := offset + int(messageLen)`）是同一缺陷的另两个实例。
- **注意对比**：`readControllerPayload:1708` 和 `decodeManagedSlotRequest:84` 用的是 `!=` 而非 `<`，
  截断成负数后比较仍为真、仍会返回错误，**因此是安全的**。缺陷只出现在用 `<` 的四处。
  gosec 已经把这四处混在 396 条 G115 噪声里报出来了（`codec_control.go:2597,2784,2795`、`codec_managed.go:216`）。
- **建议**：在 `readUvarint` 之后立刻拒绝 `length > uint64(len(rest))`（在任何 `int` 转换之前比较），
  统一替换掉 `len(rest) < int(length)` 这个模式。

### [P1] 3. `controller_raft_compact` / `controller_logs` / `controller_raft_status` 三个 RPC 完全无授权检查

- **位置**：`pkg/cluster/controller_handler.go:52-81`
- **类别**：安全 / 架构
- **代码**（`controller_handler.go:52-58`，位于 switch 最前面）：
  ```go
  switch req.Kind {
  case controllerRPCControllerRaftCompact:
  	result, err := c.localCompactControllerRaftLog(ctx, uint64(c.NodeID()))
  	if err != nil {
  		return nil, err
  	}
  	return encodeControllerResponse(req.Kind, controllerRPCResponse{ControllerRaftCompaction: &result})
  ```
- **触发路径**：任意能连到 cluster 端口的主体发一个 `kind=controllerKindControllerRaftCompact`(26)、
  payload 为空的 controller RPC（`decodeControllerRequestPayload` 对该 kind 只要求 `len(payload)==0`）。
  该分支**没有** leader 检查、没有 `isNodeAuthorizedForObservation`、没有 JoinToken 校验
  —— 对比同一 switch 里 `heartbeat`(:88-93) 和 `runtime_report`(:121-123) 都调用了
  `isNodeAuthorizedForObservation`，`join_cluster` 走 `validateJoinClusterRequest` 的
  `subtle.ConstantTimeCompare` token 校验。三个诊断/运维 kind 是唯一的例外。
  → `localCompactControllerRaftLog`(controller_raft_compaction.go:37) → `service.CompactLog(ctx)`，
  按当前 applied index 生成 Controller Raft snapshot 并裁剪本地 entries。
- **后果**：
  (a) 未认证远程可无限触发 Controller Raft 日志压缩 —— 每次强制生成 snapshot + 截断日志，
      造成磁盘 I/O 放大；并且把落后 follower 的 `Next` 推到本地 `FirstIndex` 之前，
      迫使其只能走全量 snapshot 传输追赶（`deriveControllerRaftPeerStatus` 里的 `NeedsSnapshot` 正是这个条件）。
  (b) `controller_logs` 未认证泄露 Controller Raft 日志条目的解码内容
      （`inspectControllerLogEntryPayload` → `controllerraft.DecodeCommandInspection(entry.Data)` 的完整
      `inspection.Payload`），即整个集群拓扑：节点 ID / 地址 / assignment / 任务状态。
      已确认 join token **不在** 提交的 `NodeJoin` 命令里（`controller_handler.go:479-487` 只写
      NodeID/Name/Addr/CapacityWeight/JoinedAt），故不泄露凭证。
- **建议**：把这三个 kind 与其余运维 RPC 一起纳入统一的调用方鉴权（至少要求 JoinToken 或已提交
  membership 中的已知节点），并把 `compact` 这类写操作与只读诊断分开授权。

### [P1] 4. `controller_logs` 服务端路径跳过 `normalizeSlotLogEntriesOptions`，攻击者可令节点读取并解码整个 Controller Raft 日志

- **位置**：`pkg/cluster/controller_handler.go:60-66` vs `pkg/cluster/controller_log_entries.go:36-45`
- **类别**：安全 / 性能（内存放大 DoS）
- **代码**（`controller_handler.go:60-66`，注意**没有** normalize）：
  ```go
  page, err := c.localControllerLogEntries(ctx, uint64(c.NodeID()), ControllerLogEntriesOptions{
  	Limit:  req.ControllerLogs.Limit,     // ← 直接来自线路，未归一化
  	Cursor: req.ControllerLogs.Cursor,
  })
  ```
  而 manager 入口是做了归一化的（`controller_log_entries.go:36-44`）：
  ```go
  func (c *Cluster) ControllerLogEntriesOnNode(ctx context.Context, nodeID uint64, opts ControllerLogEntriesOptions) (ControllerLogEntries, error) {
  	opts = normalizeSlotLogEntriesOptions(opts)     // ← 把 Limit 夹到 maxSlotLogEntryLimit
  	if c.IsLocal(multiraft.NodeID(nodeID)) {
  		return c.localControllerLogEntries(ctx, nodeID, opts)
  ```
- **触发路径**：
  1. 发 `kind=controller_logs` 的 RPC，payload 为 `[limit uvarint][cursor uvarint]`，
     令 `limit = 2^32`（`decodeControllerLogEntriesRequest:776` 把它 `int(limit)` 原样带出，无上界）。
  2. 因为走的是 `handler` 直调 `localControllerLogEntries`，`normalizeSlotLogEntriesOptions`
     （`slot_log_entries.go:28-36`，本应把 Limit 夹到 `maxSlotLogEntryLimit`）被绕过。
  3. `slotLogEntryWindow`(slot_log_entries.go:121-142)：`limit := uint64(opts.Limit)`；
     当日志条目数 < limit 时 `hi > first+limit` 为假 → `lo = first`
     → 窗口变成 `[first, last+1)`，**整个本地 Controller Raft 日志**。
  4. `storage.Entries(ctx, lo, hi, 0)` 读出全部条目，
     `controllerLogEntriesFromRaft`(controller_log_entries.go:123) 对**每一条**都调
     `inspectControllerLogEntryPayload` 做完整命令解码，每条生成一个 `map[string]any`，
     再全部编码进一个响应 body。
- **后果**：一个 ~60 字节的未认证请求（叠加发现 3 的无授权）即可让目标节点把整个 Controller Raft 日志
  读进内存、逐条解码成 map、再序列化 —— 长期运行的集群日志可达数十万条，直接 OOM 或长时间 STW；
  `defaultSlotLogEntryLimit`/`maxSlotLogEntryLimit` 这两个防护对远程调用方完全失效。
- **建议**：在 `controllerHandler.Handle` 的 `controller_logs` 分支里对 `req.ControllerLogs` 同样调用
  `normalizeSlotLogEntriesOptions`（或把归一化下沉进 `localControllerLogEntries`，使任何入口都无法绕过）。

### [P2] 5. `controllerHost.Stop()` 不 join 任何它自己启动的后台 goroutine，`warmupNodeMirror` 可在 `healthScheduler.reset()` 之后重新挂上健康超时定时器

- **位置**：`pkg/cluster/controller_host.go:174-204`（`Stop`）、`:586-588`、`:779-800`（`warmupNodeMirror`）
- **类别**：并发 / 资源泄漏
- **代码**（`controller_host.go:779-800`，两处 term 守卫都挡不住 Stop）：
  ```go
  func (h *controllerHost) warmupNodeMirror(term uint64) {
  	...
  	ctx, cancel := h.withWarmupTimeout()
  	nodes, err := h.loadNodeMirror(ctx)
  	cancel()
  	if err != nil {
  		return
  	}
  	s := h.healthScheduler
  	if !h.isLocalLeaderTerm(term) { return }
  	if !h.isTerm(term) { return }
  	s.primeFromNodes(nodes)          // ← Stop() 不改 leaderTerm/warmupLeaderID，两个守卫仍然通过
  }
  ```
- **触发路径**：
  1. 本节点成为 controller leader → `handleLeaderChange`(:585-588) 启动
     `go h.warmupNodeMirror(term)` 与 `h.enqueueHashSlotTableReload(term)`(→ `:827 go h.hashSlotTableReloadWorker(term)`)，
     `metadataSnapshotState.onLocalLeaderAcquiredAsync` 另起 `go s.reloadWorker(...)`
     （`controller_metadata_snapshot.go:179`、`:219`，即 gosec 两处 G118）。
     **全仓无任何 WaitGroup 跟踪这四个 goroutine**（`controllerHost` 结构体 :22-73 只有 mutex/atomic，无 `sync.WaitGroup`）。
  2. `warmupNodeMirror` 已成功取回 `nodes`、`cancel()` 已执行，但还没走到 `primeFromNodes`。
  3. 此刻 `Stop()` 整段执行完：`bgCancel()`(:184) → `healthScheduler.reset()`(:192，停掉所有定时器并清空 map)
     → `service.Stop()`(:196) → `raftDB.Close()`(:199) → `meta.Close()`(:202)。
     `Stop()` 既不 bump `leaderTerm` 也不清 `warmupLeaderID`，所以 `isLocalLeaderTerm(term)`、`isTerm(term)` 依然为真。
  4. `primeFromNodes`(node_health_scheduler.go:259-345) 重建 `s.nodes` 并对每个 alive/suspect 节点
     挂上最多 2 个 `time.AfterFunc`（suspect 3s / dead 10s，见 `newControllerHost` :137-139）。
  5. 最长 10 秒后这些定时器触发 `handleDeadline` → `proposeStatusTransition`，
     调用已停止的 `h.service.Propose` 和已关闭的 `h.meta.GetNode`。
- **后果**：`Stop()` 返回后仍有最多 2N 个定时器存活最长 10 秒，持有 `controllerHost`、`healthScheduler`
  和整份 nodes 快照不被回收，并在停机后继续尝试 Propose。进程整体退出时无害；
  但对进程内 `Stop()`→`Start()` 重启、以及测试/嵌入式复用场景，是确定的 goroutine+定时器泄漏与停机后写尝试。
  **需要说明的是**：这里不会发生 use-after-close 崩溃 —— `controllermeta.Store` 已正确加固（见「已排除的候选项」第 1 条）。
- **建议**：给 `controllerHost` 加一个 `sync.WaitGroup`（或 stopped 标志），四处 `go` 都登记，
  `Stop()` 在 `bgCancel()` 之后先 `Wait()` 再 `reset()`/关闭存储；并让 `primeFromNodes` 之前检查 stopped。

### [P2] 6. 观测热路径上用 `context.Background()` 做无超时 Pebble 全量扫描

- **位置**：`pkg/cluster/controller_host.go:486`、`:683`
- **类别**：性能 / context
- **代码**（`controller_host.go:479-490`，`refreshWarmupReady`）：
  ```go
  func (h *controllerHost) refreshWarmupReady() {
  	if h == nil || h.meta == nil {
  		return
  	}
  	h.syncLeaderWarmupState()
  	nodes, err := h.meta.ListNodes(context.Background())   // ← 无超时、无取消、无缓存
  	if err != nil {
  		return
  	}
  ```
- **触发路径**：`refreshWarmupReady` 在两条热路径上被调用：
  (a) `applyRuntimeReport`(:246-249) —— 每个带 `FullSync=true` 的 `runtime_report` 入站 RPC 都触发一次；
  (b) `handleCommittedCommand`(:617-621) —— 每次提交 `NodeStatusUpdate`/`OperatorRequest`/`NodeJoin`/`NodeJoinActivate` 命令都触发一次。
  每次都是对 Pebble node 前缀的**全量 range 扫描**（`store.listNodesLocked` → `listRecords`），
  且丢掉了调用方（入站 RPC）的 ctx，既不继承 deadline 也无法被 `bgCancel` 打断。
  `nodeStatusAfterCommit:683` 的 `h.meta.GetNode(context.Background(), nodeID)` 同类，按每个节点状态变更调用。
- **后果**：controller leader 上，节点数 N、每节点周期性 FullSync 时，稳态每秒产生 O(N) 次全量 node 扫描；
  Pebble 卡顿时这些调用无超时地堆积在 RPC handler goroutine 上（`applyRuntimeReport` 由入站 RPC 直接驱动），
  把存储抖动放大成 controller RPC 队列堆积。与 FLOW.md「Controller metadata 读快路径」的设计意图相悖 ——
  这条路径本应能复用 `metadataSnapshotState`，却直接打穿到 Pebble。
- **建议**：`refreshWarmupReady` 改为接收调用方 ctx 并套 `metadataReloadTimeout`；
  优先读已有的 leader-local `metadataSnapshot()`（clean 时直接用 `snapshot.Nodes`），仅在 cold/dirty 时回落存储。

### [P3] 7. `onLocalLeaderAcquiredAsync` 的死写 + 无条件启动打破 `reloadScheduled` 的"单 worker"不变式

- **位置**：`pkg/cluster/controller_metadata_snapshot.go:203-220`
- **类别**：并发 / 可读性
- **代码**：
  ```go
  shouldStart := false
  s.mu.Lock()
  s.snapshot = controllerMetadataSnapshot{ LeaderID: local, Generation: s.snapshot.Generation + 1, ... }
  s.reloadPending = false      // ← 死写：下面两行立刻覆盖
  s.reloadScheduled = false    // ← 死写
  s.reloadPending = true
  s.reloadScheduled = true     // ← 无视"是否已有 worker 在跑"
  shouldStart = true           // ← 恒为 true，shouldStart 这个变量本身已无意义
  s.mu.Unlock()
  if shouldStart {
  	go s.reloadWorker(ctx, store, local, timeout)
  }
  ```
  对比同文件 `markDirtyAndEnqueueReload:162-179` 就正确地用 `if !s.reloadScheduled` 做了守卫。
- **触发路径**：controller 领导权在本节点反复抖动（local → remote → local，分区恢复或滚动重启时常见）。
  每次本节点重新拿到 leader 都无条件再起一个 `reloadWorker`；旧 worker 只有在其当前
  `s.reload()` 返回后才会发现 generation 变化并退出，而 `s.reload` 被 `reloadMu`(:304) 串行化，
  于是多个 worker 同时存在并排队。随后某个 worker 把 `reloadScheduled` 置回 false 时，
  另一个 worker 其实仍在循环中，`markDirtyAndEnqueueReload` 便会再起一个，不变式被破坏。
- **后果**：领导权抖动期间堆积数个并发 reload worker，每个持有一份完整 metadata 快照
  （Nodes+Assignments+Tasks+OnboardingJobs）并重复做全量 Pebble 扫描 —— 冗余工作与瞬时内存尖峰。
  因为都被 `reloadMu` 串行、且都以 generation 检查收敛，**不会**造成快照内容错乱或无界泄漏，故定为 P3。
- **建议**：删掉两行死写，与 `markDirtyAndEnqueueReload` 一致地用 `if !s.reloadScheduled` 守卫是否启动 worker。

### [P3] 8. FLOW.md 的 Controller RPC 操作清单缺 3 项（且正是发现 3 中无授权的 3 项）

- **位置**：`pkg/cluster/FLOW.md:552` vs `pkg/cluster/codec_control.go:639-698`
- **类别**：架构 / 文档一致性
- **代码**：FLOW.md:552 自称 `**Controller RPC 操作** (23 种)` 并逐个列出 23 个；
  实际 `controllerKindCode` 支持 **26** 个 —— 多出
  `controller_logs`、`controller_raft_status`、`controller_raft_compact`
  （常量见 `codec_control.go:85-87` `controllerKindControllerLogs/ControllerRaftStatus/ControllerRaftCompact`）。
  FLOW.md §5.8 的服务端 `Handle` 分发表同样没有这三项，
  尽管 §8「避坑清单」在 `ControllerRaftStatusOnNode`/`CompactSlotRaftLogOnNode` 条目里提到了相关语义。
- **触发路径**：`AGENTS.md` 要求先读 FLOW.md 再读代码。按 FLOW.md 做控制面攻击面审查的人，
  会完全看不到这三个 RPC —— 而它们恰好是本单元唯一三个不做任何授权检查的入口（发现 3）。
- **后果**：文档与代码不一致，且不一致的部分正好掩盖了真实的安全缺口。
- **建议**：把三个 kind 补进 FLOW.md:552 的清单和 §5.8 的分发表，并标注其授权要求。

---

## 已排除的候选项

- **`controller_host.go:199-202` `raftDB.Close()`/`meta.Close()` 的 use-after-close（任务书给的 P1 线索）
  —— 实际安全，已排除崩溃可能**。`pkg/controller/meta/store.go` 做了完整加固：
  `Close()`(:26-38) 在 `s.mu.Lock()` 内把 `s.db` 置 nil 后才 `db.Close()`；所有读路径
  （`ListNodes:222`、`LoadHashSlotTable:166`、`GetNode:184`）先 `ensureOpen()`(:865) 再
  `s.mu.RLock()`，且**整个 Pebble 迭代都在 RLock 内完成**
  （`listNodesLocked:727` → `listRecords:743`，其中 `:744 if db == nil { return ErrClosed }` 二次校验；
  `getValueLocked:785` 调 `ensureOpenLocked()`）。因此 `db.Close()` 不可能与进行中的读重叠，
  并发的后来者拿到 `ErrClosed` 而非 nil 解引用。goroutine 未被 join 仍是缺陷，
  但后果是定时器/goroutine 泄漏（已按发现 5 记录），**不是** Pebble 崩溃。
- `controller_metadata_snapshot.go:179` / `:219` —— gosec G118「goroutine 使用 context.Background」：
  误报。两处传入的是 `h.bgCtx`（`controller_host.go:121` 创建，`Stop()` 里 `bgCancel()`），
  是可取消的；`:260` 的 `context.Background()` 兜底分支在生产中不可达（`bgCtx` 永不为 nil）。
  真正的问题是 worker 未被 join，已并入发现 5。
- `controller_metadata_snapshot.go:218` `shouldStart` 守卫是否存在竞态 —— 检查与置位都在
  `s.mu` 内完成，无竞态；问题是该守卫**恒为 true**（逻辑缺陷而非竞态），已按发现 7 记录。
- `controller_host.go:836-840` `hashSlotTableReloadWorker` 在 `loadHashSlotTable` 出错时
  `continue` → 因 `hashSlotReloadPending` 已被置 false 而直接退出，reload 请求被丢弃 ——
  **实际可自愈，不构成缺陷**：`controller_handler.go:36-49` 的 `loadHashSlotTable` 闭包在
  snapshot miss 时会回落 `ensureControllerHashSlotTable` 并**回填** `storeHashSlotTableSnapshot(table)`，
  下一次 heartbeat / list_assignments 即恢复快路径，与 FLOW.md §8「snapshot cold miss 才会回落…回填后再继续返回」一致。
- `controller_handler.go:163/:186/:199` `if assignments == nil { fallback }` 判空而非判长度 ——
  看似空切片会被误判为 cold，但 `controllerMetadataSnapshot.clone()`(:72-80) 对空集合本就产出 nil，
  回落只会多读一次 Pebble 且结果相同，无正确性问题。
- `controller_client.go:428-437` `call()` 信任对端返回的 `resp.LeaderID`/`LeaderAddr` 并
  `upsertLeaderSeed` 写入 discovery —— 确实允许一个恶意 peer 把 leader 地址指向任意主机，
  但触发前提是"已在 membership 中的 peer 变为恶意"，与本单元发现 1/2/3 的未认证前提不同层级，
  且 `upsertLeaderSeed`(:507) 只新增 seed 不删除既有节点；`tried` map(:396) 也排除了重定向死循环。
  归属传输/发现层的信任模型问题，留给协调者统一判断，不在此计为本单元发现。
- `codec.go:100-103` `decodeRaftBatchBody` 的 `count := int(uint32)` 在 32 位平台会变负数并令
  `make([]raftBatchItem, 0, count)` panic —— 生产为 64 位，`int` 为 64 位，uint32 无法变负，误报。
- gosec G115 误报（编码侧 int→uint 转换，均受 transport 单帧预算约束）：
  `codec.go:82,87`（`uint32(len(items))`/`uint32(len(item.data))`）、
  `codec_control.go:762,794`（`uint64(req.Limit)`/`uint64(entry.DataSize)`）、
  `codec_managed.go:354`。
- gosec G115 误报（无损往返）：`codec_control.go:2898` `appendInt64` 的 `uint64(value)` 与
  `:2903` `readInt64` 的 `int64(value)` 是同一编码的正反两半，位模式无损。
- gosec G115 误报（`!=` 比较使截断无害）：`codec_control.go:1708` `if len(body) != int(payloadLen)`、
  `codec_managed.go:84` `if len(body[offset:]) != int(payloadLen)` —— 截断成负数后比较仍为真，
  仍然返回错误。**与发现 2 中用 `<` 的四处形成对照**，正是这个差异决定了安全与否。
- gosec G115 误报（已有正确上界）：`codec_control.go:998`
  `make([]ControllerRaftPeerProgress, 0, int(count))` —— 紧邻的 `:1015-1018` 有
  `count > uint64(len(body)/minControllerRaftPeerProgressWireSize)` 校验，是全包最规范的写法。
- `codec_control.go:833` / `codec_managed.go:394` `entry.DataSize = int(dataSize)` ——
  该字段只作为展示用的字节数摘要，不参与任何分配或索引，截断无安全后果。
- `pkg/cluster/` 下其余 gosec 命中（`slot_manager.go`、`slot_executor.go`、`reconciler.go`、
  `cluster.go`、`onboarding.go`、`operator.go`、`readiness.go`、`config.go`、`snapshot_chunks.go`、
  `hashslot/`、`slotmigration/` 等）不属于本单元文件清单，未审。
- staticcheck 在 `pkg/cluster/` 全包 **0 条命中**，无可核实项。

---

## 本分片整体评价

这部分代码在**分布式语义**上写得相当扎实：leader term / generation fencing 贯穿
`warmupNodeMirror`、`hashSlotTableReloadWorker`、`metadataSnapshotState` 三条异步路径，
且都做了"取数后再校验 term 未变才写入"的双重检查；`controllerClient.call` 的 per-peer 超时、
`tried` 去重、leader 重定向前插都处理得当；`consumeControllerRecord` 的 magic + 内部版本号
为 assignment / runtime view 提供了干净的线路演进路径。AGENTS.md 的分层约定没有被违反，
FLOW.md 除了漏掉 3 个 RPC 之外与代码高度吻合。

**问题几乎全部集中在编解码层的输入校验上，而且是同一个类型的缺陷重复了 18 次。**
最需要优先处理的是发现 1：`codec_control.go` 有 14 处用线路 uvarint 直接 `make()` 而不设上界，
外加发现 2 的 4 处 `len(rest) < int(length)` 被整数截断绕过。由于
`pkg/transport` 与 `pkg/cluster` 全包没有一处 `recover()`、入站 RPC 又跑在裸 goroutine 里、
传输层本身没有认证、且解码发生在所有授权检查之前，这些缺陷合起来等于
**任何能连到 cluster 端口的主体都能用一个几十字节的报文打崩任意节点的进程**（已用 PoC 验证两种 panic）。
值得强调的是，同一个包里已经存在三种正确的上界写法
（`codec.go:101`、`codec_managed.go:613`、`codec_control.go:1015`），
说明这是系统性遗漏而非设计取舍 —— 修复方向明确，照抄本包既有惯用法即可。
建议同时在 transport 的 RPC 分发处补一层 `recover`，把这一整类编解码缺陷从"进程退出"降级为"丢一个连接"。
