# 管理用例：分布式任务 / 缩容 / 迁移 / 网络

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/usecase/management/distributed_tasks.go | 968 | 是 |
| internal/usecase/management/node_scalein.go | 773 | 是 |
| internal/usecase/management/network.go | 725 | 是 |
| internal/usecase/management/channel_migration.go | 597 | 是 |
| internal/usecase/management/channel_runtime_meta.go | 443 | 是 |
| internal/usecase/management/node_scalein_channel.go | 406 | 是 |

合计 3912 行非测试代码，全部通读。已确认 `internal/usecase/management/` 下无 `FLOW.md`（最近的父级是 `internal/FLOW.md`）。

为核实触发路径另外读了以下文件（**非本分片，仅作证据链**）：`internal/access/manager/node_scalein.go`、
`internal/access/node/runtime_summary_rpc.go`、`pkg/cluster/cluster.go`、`pkg/transport/{client,pool,conn}.go`、
`pkg/slot/proxy/{store,runtime_meta_rpc}.go`、`pkg/db/meta/{batch,compat,compat_channel_migration_helpers,table_runtime_meta}.go`、
`pkg/db/internal/keycodec/codec.go`、`pkg/controller/plane/planner.go`、`internal/app/network_observability.go`。

## 状态机（先写出来，再逐条回答四个问题）

**节点缩容** —— 唯一驱动者是运维的 HTTP 请求，**没有后台循环**（已 grep 确认 `AdvanceNodeScaleIn`
在全仓非测试代码里只有 `internal/access/manager/node_scalein.go:198` 一个调用方）：

```
blocked ──(preflight 全通过)──> not_started
not_started ──POST /start (MarkNodeDraining)──> migrating_replicas
migrating_replicas ──(controller planner 自动搬迁；planner.go:296 把 Draining 视为不可用)──> transferring_leaders
transferring_leaders ──POST /advance (TransferSlotLeader)──> waiting_channel_migrations / draining_channels
draining_channels ──POST /advance（每次最多建 1 个、上限 5 个 channel 迁移任务）──> waiting_channel_migrations
waiting_channel_migrations ──(channel 迁移执行器跑完)──> draining_channels ──> waiting_connections
waiting_connections ──(连接清零)──> ready_to_remove
任意状态 ──POST /cancel (ResumeNode)──> not_started
```

1. **哪些步骤不是原子的，协调进程中途重启怎么恢复**：`loadNodeScaleInSnapshot`
   （`node_scalein.go:476-527`）是 **7 次互相独立的 strict 读 + 1 次跨节点 RPC**，
   没有任何事务或版本号把它们串成一致快照；`AdvanceNodeScaleIn` 更是读了**两份互不相关的快照**
   （`node_scalein.go:372` 的 report 与 `node_scalein.go:385` 的 snapshot）后混用二者做决策。
   协调进程（即处理该 HTTP 请求的 goroutine）中途重启时没有任何持久化进度，
   恢复路径就是"运维再 POST 一次" —— 因为每一步动作都是从零重新推导的，这一点反而救了它（见问题 3）。
2. **能否永久卡在中间态**：能，一处。见 **[P1] 3** —— 卡在 `verify_new_leader` / `verify_membership` /
   `clear_fence` 三个阶段的 channel 迁移任务**在本用例里无法 abort**，且会让它所涉及的节点缩容
   永久停在 `waiting_channel_migrations`。这是本分片唯一真正"无运维逃生舱"的状态。
3. **重试是否幂等**：是，而且这是本分片设计得最好的地方。每次 `/advance` 都重新扫描、重新推导候选；
   `createScaleInChannelMigration` 前有 `validateNoActiveChannelMigration`；落库用
   `CreateChannelMigrationTaskWithRuntimeGuard`（`pkg/db/meta/batch.go:234-258`，guard 在 batch
   提交时于同一原子操作内校验 epoch/leader/fence）；`scaleInChannelMigrationRace`
   （`node_scalein_channel.go:271-284`）把 `ErrStaleMeta` / `ErrAlreadyExists` / `active_task_exists`
   统一当作"竞态，下次再来"。**不会重复下发**。唯一的非幂等残留是 leader transfer
   循环的部分应用（见 **[P2] 4**）。
4. **有没有 fence 防止写落到旧 owner**：**有，而且是真 fence，不是 TOCTOU 检查**。
   channel 迁移的 phase 序列里显式包含 `write_fence` / `cutover_fence` / `clear_fence`
   （`channel_migration.go:23-45`）；元数据带 `WriteFenceToken` + `WriteFenceVersion`；
   创建任务时用 `ChannelMigrationRuntimeGuard`（含 `ExpectedFenceToken` / `ExpectedFenceVersion` /
   `ExpectedLeaderEpoch` / `ExpectedLeader`，`channel_migration.go:499-508`）在 raft apply 内做 CAS；
   `TransferChannelLeader` / `MigrateChannelReplica` 在 `channelMigrationWriteFenceActive(meta)`
   为真时直接拒绝（`channel_migration.go:163-165`、`207-209`）。
   **这一类里我找不到消息丢失缺陷** —— 缩容/迁移的一致性骨架是可靠的。真正的问题全部落在
   **可用性、可完成性与可观测性**上，见下。

## 发现

### [P1] 1. 缩容状态读取向"正在被缩掉的那个节点"发无超时 RPC，一个僵死节点能把整个缩容控制面和任务中心永久挂住

- **位置**：`internal/usecase/management/node_scalein.go:503-514`
- **类别**：并发 / 资源泄漏 / 分布式可用性
- **代码**：
  ```go
  	runtime := NodeRuntimeSummary{NodeID: nodeID, Unknown: true}
  	runtimeReadFailure := true
  	if a.runtimeSummary != nil {
  		runtime, err = a.runtimeSummary.NodeRuntimeSummary(ctx, nodeID)
  		if err == nil && (runtime.NodeID == 0 || runtime.NodeID == nodeID) && !runtime.Unknown {
  			runtime.NodeID = nodeID
  			runtimeReadFailure = false
  		} else {
  			runtime = NodeRuntimeSummary{NodeID: nodeID, Unknown: true}
  		}
  	}
  ```
- **证据链（逐跳确认无 deadline）**：
  `internal/access/node/runtime_summary_rpc.go:40-50` → `c.cluster.RPCService(ctx, ...)`，未包 `WithTimeout`；
  `pkg/cluster/cluster.go:1363-1371` → `c.fwdClient.RPCService(ctx, ...)`，未包；
  `pkg/transport/client.go:43-75` → `c.RPC(ctx, ...)`，未包；
  `pkg/transport/pool.go:94-110` → `mc.RPC(ctx, ...)`，未包；
  终点 `pkg/transport/conn.go:63-69`：
  ```go
  	select {
  	case resp := <-ch:
  		return resp.body, resp.err
  	case <-ctx.Done():
  		return nil, ctx.Err()
  	case <-mc.readerDone:
  		return nil, ErrStopped
  	}
  ```
  即只有"对端回包 / 调用方 ctx 取消 / TCP 断开"三种解除条件，**传输层自身没有超时**。
  而调用方 ctx 就是 `c.Request.Context()`（`internal/access/manager/node_scalein.go:142,155,169,198,220`），
  且 `internal/access/manager/` 全目录 `grep 'WithTimeout|ReadTimeout|WriteTimeout|ReadHeaderTimeout'`
  **零命中** —— 请求 ctx 没有任何 deadline。
- **触发路径**：节点 5 已是 `Draining`（缩容的正常中间态），其进程存活但 RPC 处理僵死
  —— 最典型的就是 K8s 已发 SIGTERM、进程在慢速 shutdown 中仍持有 TCP 连接，
  或其 runtime summary 处理 goroutine 卡在一把锁上。此时：
  1. `GET /nodes/5/scale-in` → `GetNodeScaleInStatus`（`node_scalein.go:340-345`）→ `PlanNodeScaleIn`
     → `loadNodeScaleInSnapshot` → `NodeRuntimeSummary(ctx, 5)` **永久阻塞**；
  2. `POST /nodes/5/scale-in/advance`、`/start` 的第一件事就是调 `GetNodeScaleInStatus`
     （`node_scalein.go:372`、`node_scalein.go:322`）→ 同样永久阻塞，**缩容无法推进**；
  3. 更糟：`GET /distributed-tasks` 与 `/distributed-tasks/summary` 也一起挂 ——
     `distributed_tasks.go:700-710` 对**每一个** Draining 节点调 `GetNodeScaleInStatus`，
     一个僵死节点把整个任务中心拖死。
- **后果**：缩容控制面完全不可用（`/cancel` 是唯一部分例外：`ResumeNode` 在服务端确实执行了，
  只是 `node_scalein.go:367` 最后那次 `GetNodeScaleInStatus` 挂住，调用方拿不到响应）。
  每个挂住的请求泄漏 1 个 goroutine + 1 个 `MuxConn.pending` map 表项 +
  1 个 `transport.Client.inflight` 计数，**直到 TCP 断开才释放**。管理面板会周期性轮询
  `/distributed-tasks`，于是 goroutine 无界累积。结合 unit 05 的结论
  （`WK_MANAGER_AUTH_ON` 代码默认 false，缩容端点可无鉴权访问），
  这是一条**未鉴权可触发的资源耗尽路径**。
  讽刺之处：这段代码对该读的**出错**已经做了完美的 fail-closed 处理
  （`runtimeReadFailure=true` → `ActiveConnectionsUnknown` → `waiting_connections`，
  永不 `ready_to_remove`）—— **只差一个 deadline** 就完全正确。
- **建议**：在 usecase 里给这次跨节点读包一个独立的短 deadline（几秒级），让它退化成
  已有的 `runtimeReadFailure` 分支；`loadNodeScaleInSnapshot` 的 7 次 strict 读同样应各自有上界。
  这与 unit 08 在 `monitor_metrics.go:143-162` / `runtime_summary_rpc.go:48` 发现的是同一个根因，
  但后果严重得多：那里挂的是一块面板，这里挂的是一个正在变更集群拓扑的流程，应当一起修。

### [P1] 2. 缩容排空 channel 是 O(N²)：每次 /advance 做 3 次全量 channel 元数据表扫描，只为创建最多 1 个迁移任务

- **位置**：`internal/usecase/management/node_scalein.go:292`、`371-427`；
  `internal/usecase/management/node_scalein_channel.go:57-113`、`183-215`、`18-20`
- **类别**：性能 / 可完成性
- **代码**（全量扫描本体，`node_scalein_channel.go:63-80`）：
  ```go
  	slotIDs := append([]multiraft.SlotID(nil), a.cluster.SlotIDs()...)
  	sort.Slice(slotIDs, func(i, j int) bool { return slotIDs[i] < slotIDs[j] })

  	for _, slotID := range slotIDs {
  		after := metadb.ChannelRuntimeMetaCursor{}
  		for {
  			page, cursor, done, err := a.channelRuntimeMeta.ScanChannelRuntimeMetaSlotPage(ctx, slotID, after, scaleInChannelScanPageLimit)
  			...
  			for _, meta := range page {
  				inventory.addRuntimeMeta(ctx, a.channelMigration, nodeID, meta, seenTasks)
  ```
  且 `addRuntimeMeta`（`node_scalein_channel.go:335`）对**每一个**引用目标节点的 channel 再补一次点查：
  ```go
  	task, ok, err := store.GetActiveChannelMigrationTask(ctx, meta.ChannelID, meta.ChannelType)
  ```
  而每步只建 1 个任务（`node_scalein_channel.go:18-20`）：
  ```go
  	scaleInChannelTaskScanLimit     = 1024
  	scaleInDefaultChannelMigrations = 1
  	scaleInMaxChannelMigrations     = 5
  ```
- **触发路径**：128 个物理 slot、20 万 channel，其中 4 万个的 replica/ISR 含被缩节点。
  运维执行 `POST /nodes/5/scale-in/advance`（不带 body ⇒ `clampScaleInChannelMigrations(0)` = 1）：
  1. `node_scalein.go:372` `GetNodeScaleInStatus` → `PlanNodeScaleIn` → `node_scalein.go:292`
     `loadNodeScaleInChannelInventory` = **全表扫描第 1 次**（20 万行 + 4 万次点查）；
  2. `node_scalein.go:410` `advanceNodeScaleInChannels` → `loadNodeScaleInChannelDrainCandidates`
     （`node_scalein_channel.go:183-215`）= **全表扫描第 2 次**（20 万行）；
  3. 建 1 个迁移任务后，`node_scalein.go:426` 再 `GetNodeScaleInStatus` = **全表扫描第 3 次**。
  排空 4 万个 channel 需重复 4 万次 ⇒ 约 2.4×10¹⁰ 次元数据行读。
  并且 `ScanChannelRuntimeMetaSlotPage` 在 slot 非本地时是**跨节点 RPC**
  （`pkg/slot/proxy/runtime_meta_rpc.go:101-116`：`s.shouldServeSlotLocally(slotID)`
  为假就走 `callRuntimeMetaRPC`），所以这些扫描大部分是网络往返。
- **后果**：中等规模以上集群的节点缩容在实践中无法完成 —— 不是"慢"，是排空所需的请求数
  与 channel 总量成正比、每个请求的代价又与 channel 总量成正比。同时这也是一条
  **未鉴权的请求放大**通道：一次 `POST /advance`（甚至只是 `GET /distributed-tasks`，
  只要集群里存在一个 Draining 节点）就能让全集群的 slot leader 做一遍全量元数据扫描。
- **建议**：给 channel runtime meta 建"按 owner 节点"的二级索引，让排空候选能按节点直接查；
  或把排空改成 controller 侧的持久化 job（带可恢复 cursor），而不是每个 HTTP 请求从零重新推导。

### [P1] 3. 卡在 verify / clear_fence 阶段的 channel 迁移任务无法 abort，会永久堵死该 channel 的所有后续迁移和相关节点的缩容

- **位置**：`internal/usecase/management/channel_migration.go:279-317`（唯一的 abort 路径）；
  `internal/usecase/management/app.go:189-198`（接口里没有强制终止能力）
- **类别**：正确性 / 分布式可恢复性（中间态不可恢复）
- **代码**（abort 必须先成功读到当前 runtime meta，并用它构造 guard）：
  ```go
  	meta, err := a.getMigrationRuntimeMeta(ctx, id)
  	if err != nil {
  		return ChannelMigrationDetail{}, err
  	}
  	nowMS := a.now().UnixMilli()
  	req := metadb.ChannelMigrationAbortRequest{
  		Guard:         channelMigrationGuardFromTask(task),
  		RuntimeGuard:  channelMigrationRuntimeGuardFromMeta(meta),
  		Status:        metadb.ChannelMigrationStatusAborted,
  ```
  接口里可用的写操作只有这一个（`app.go:196-197`）：
  ```go
  	// AbortChannelMigration marks an active migration task aborted.
  	AbortChannelMigration(ctx context.Context, req metadb.ChannelMigrationAbortRequest) error
  ```
- **关键约束（存储层，用来证明"卡住"是真的）**：
  `pkg/db/meta/compat_channel_migration_helpers.go:275-302` 定义了可 abort 的阶段白名单 ——
  leader transfer 只允许 `validate / probe_target / write_fence / drain_leader /
  final_target_catch_up / commit_leader_meta`；replica replace 只允许
  `validate / add_learner / bootstrap_target / warm_catch_up / cutover_fence /
  final_target_catch_up / promote_and_remove`。
  对照 `channel_migration.go:23-41` 的完整 phase 序列，
  **`verify_new_leader`、`verify_membership`、`clear_fence` 三个尾部阶段不在白名单内**，
  `requireChannelMigrationAbortTransition`（同文件 `:242-260`）会直接返回 `dberrors.ErrConflict`。
- **触发路径**：
  1. 某 channel 的 `replica_replace` 任务推进到 `clear_fence`（该阶段的职责正是**释放写 fence**），
     此时执行器所在节点宕机、或该 channel 的 slot 短暂失去 quorum，任务停在 `clear_fence`。
  2. 运维调 `DELETE /channel-migration/{type}/{channel_id}?task_id=...`
     → `AbortChannelMigration` → 存储层 `requireChannelMigrationAbortTransition` 返回 `ErrConflict`。
     **本用例没有任何 force-abort、也没有清 fence 的端点** —— 运维在管理面上无解。
  3. 该 channel 从此对所有迁移关闭：`validateNoActiveChannelMigration`（`channel_migration.go:351-362`）
     返回 blocker `active_task_exists`；同时 `meta.WriteFenceToken != ""` 让
     `channelMigrationWriteFenceActive`（`channel_migration.go:415-417`）再加一条 `write_fence_active`。
  4. 级联到缩容：`loadNodeScaleInChannelInventory` 的 `addRuntimeMeta`（`node_scalein_channel.go:321-343`）
     会把这个任务计入 `activeMigrations` ⇒ `report.Progress.ActiveChannelMigrationsInvolvingNode > 0`
     ⇒ `scaleInStatus`（`node_scalein.go:750-770`，第 763-764 行）返回 `waiting_channel_migrations`
     ⇒ `AdvanceNodeScaleIn`（`node_scalein.go:421-423`）：
     ```go
     	if report.Progress.ActiveChannelMigrationsInvolvingNode > 0 {
     		return report, nil
     	}
     ```
     **永久返回 HTTP 200 + 状态 `waiting_channel_migrations`，不做任何事**。
     只要这个 channel 引用了被缩节点，该节点的缩容就永久卡住。
- **后果**：单个卡住的 channel 迁移任务 = 该 channel 永久不能再做 leader transfer / replica 替换，
  且托管它的节点永久无法缩容。状态对运维是**可见的**，但**不可操作** —— 管理 API 上没有逃生舱。
  这是本分片唯一真正满足"迁移/缩容中间态不可恢复"的缺陷。
- **建议**：给 `ChannelMigrationStore` 增加一个受审计的强制终止 / 清 fence 能力
  （不依赖 runtime-meta 可读、不依赖阶段白名单），并在管理 API 上暴露；
  同时把 `verify_*` / `clear_fence` 纳入常规可 abort 阶段 —— 这些阶段本身不改变数据，
  只做校验与释放，拒绝 abort 没有安全收益。

### [P2] 4. AdvanceNodeScaleIn 混用两份独立快照；leader transfer 循环部分应用后返回动作前的报告，且单个无候选的 slot 阻塞其余全部

- **位置**：`internal/usecase/management/node_scalein.go:371-396`
- **类别**：分布式一致性 / 健壮性
- **代码**：
  ```go
  	report, err := a.GetNodeScaleInStatus(ctx, nodeID)      // 快照 A（内部自己又读了一整套）
  	...
  	snapshot, err := a.loadNodeScaleInSnapshot(ctx, nodeID)  // 快照 B，与 A 无任何关联
  	...
  	if report.Status == NodeScaleInStatusTransferringLeaders {    // 用 A 的结论判定
  		limit := clampScaleInLeaderTransfers(req.MaxLeaderTransfers)
  		transferred := 0
  		for _, view := range snapshot.views {                     // 遍历 B 的数据
  			if view.LeaderID != nodeID { continue }
  			candidate, ok := selectScaleInLeaderCandidate(snapshot.nodes, snapshot.assignments, view, nodeID)
  			if !ok {
  				return report, &NodeScaleInReportError{Err: ErrInvalidNodeScaleInState, Report: report}
  			}
  			if err := a.cluster.TransferSlotLeader(ctx, view.SlotID, multiraft.NodeID(candidate)); err != nil {
  				return report, err
  			}
  ```
- **触发路径**：被缩节点同时是 slot 7、slot 19、slot 23 的 leader，`MaxLeaderTransfers=3`。
  - **交错 A（部分应用 + 误导性报告）**：slot 7 转移成功 → slot 19 的 `TransferSlotLeader`
    因目标节点瞬时不可达失败 → `return report, err`，而这个 `report` 是**快照 A**（动作之前）的。
    运维看到"leader 数量未变"的报告加一个错误，但 slot 7 的 leader 其实已经换了。
    重试本身是安全的（重新推导），缺陷是报告与实际状态不符。
  - **交错 B（队头阻塞）**：slot 7 的 `assignment.DesiredPeers` 与 `view.CurrentPeers` 里恰好
    没有任何 `alive + active + data` 候选（例如其余 peer 正好都处于 suspect），
    `selectScaleInLeaderCandidate`（`node_scalein.go:690-702`）返回 `!ok`
    → **第一个 slot 就直接 return**，slot 19 / 23 明明有合法候选却永远轮不到。
  - 另外 `loadNodeScaleInSnapshot`（`node_scalein.go:480-502`）自身的 7 次 strict 读也不是一致快照：
    `nodes` 读到之后、`views` 读到之前发生的 leader 变更，会让 `selectScaleInLeaderCandidate`
    基于混合时点的状态选目标。
- **后果**：运维拿到与实际集群状态不符的报告；单个不可转移的 slot 造成队头阻塞、缩容停滞。
  可用 `/cancel` 退出，不是永久卡死，故为 P2 而非 P1。
- **建议**：`AdvanceNodeScaleIn` 只取一份快照并据此判定状态；转移循环应跳过无候选的 view
  继续尝试其余 view，并在返回前刷新报告，让返回值反映已经生效的动作。

### [P2] 5. listChannelRuntimeMetaFiltered 没有扫描预算：零命中的过滤查询会扫全集群 channel，且对每一行（而非命中行）都额外读一次 max message seq

- **位置**：`internal/usecase/management/channel_runtime_meta.go:193-229`、`301-314`
- **类别**：性能 / 安全（未鉴权请求放大）
- **代码**：
  ```go
  	for i := startIndex; i < len(slotIDs); i++ {
  		...
  		for {
  			page, nextCursor, done, err := a.channelRuntimeMeta.ScanChannelRuntimeMetaSlotPage(ctx, slotID, after, channelRuntimeMetaFilteredScanLimit(limit))
  			if err != nil { ... }
  			items, err := a.managerChannelRuntimeMetaItems(ctx, slotID, page, filter.includeMaxMessageSeq)
  			if err != nil { ... }
  			for _, item := range items {
  				if filter.channelIDQuery != "" && !strings.Contains(item.ChannelID, filter.channelIDQuery) {
  					continue
  				}
  ```
  `managerChannelRuntimeMetaItems`（`channel_runtime_meta.go:301-310`）在**过滤之前**就对每一行取 max seq：
  ```go
  	for _, meta := range metas {
  		var maxMessageSeq *uint64
  		if includeMaxMessageSeq {
  			value, err := a.channelMaxMessageSeqForMeta(ctx, meta)
  ```
- **触发路径**：`GET /channel-runtime-meta?limit=1&channel_id_query=zzzzzzzzzz&include_max_message_seq=true`。
  未过滤分支由 `len(resp.Items) < req.Limit` 限界（`channel_runtime_meta.go:135`），
  但过滤分支的双层循环**没有任何行数预算、没有 `select { case <-ctx.Done(): }`、也没有提前 break**：
  它会把全部 slot × 每个 slot 的全部 channel 都拉一遍（每页一次可能的跨节点 RPC），
  并对**每一行**再打一次消息存储的 max-seq 读。一次请求 = O(全集群 channel 数) 元数据读
  + O(全集群 channel 数) 消息存储读。
- **后果**：一条 curl 就能让全集群的 slot leader 与消息存储满载。
  结合 unit 05 的 `WK_MANAGER_AUTH_ON` 默认 false，未鉴权即可触发。
- **建议**：给过滤扫描加行数预算（超出即返回 `HasMore` + cursor，交客户端翻页），
  并把 max-seq 的取值移到过滤之后、只对进入结果集的行执行。

### [P2] 6. 任务中心的两个 per-node 扇出源没有单节点超时，且任一节点出错就丢弃已收集的全部结果

- **位置**：`internal/usecase/management/distributed_tasks.go:700-712`、`795-812`
- **类别**：健壮性 / 可观测性
- **代码**（scale-in 源，`distributed_tasks.go:700-710`）：
  ```go
  	for _, node := range nodes {
  		if node.Role != controllermeta.NodeRoleData || node.Status != controllermeta.NodeStatusDraining {
  			continue
  		}
  		report, reportErr := s.app.GetNodeScaleInStatus(ctx, node.NodeID)
  		if reportErr != nil {
  			return nil, nil, reportErr
  		}
  ```
  （channel migration 源，`distributed_tasks.go:804-812`）：
  ```go
  		tasks, hasMore, err := s.app.channelMigration.ListActiveChannelMigrationTasksForNode(ctx, node.NodeID, distributedTaskSourceScanLimit)
  		if err != nil {
  			return nil, nil, err
  		}
  		for _, task := range tasks {
  			seen[channelMigrationDistributedTaskKey(task)] = task
  		}
  		if hasMore {
  			return distributedTasksFromChannelMigrationMap(seen), []DistributedTaskWarning{{
  				Domain:  DistributedTaskDomainChannelMigration,
  				Code:    "source_truncated",
  ```
- **触发路径**：10 节点集群，节点 7 的 meta shard 短暂失去 quorum。
  `GET /distributed-tasks` → channel migration 源在节点 7 上取错 → `return nil, nil, err`
  → `collectDistributedTasks`（`distributed_tasks.go:327-350`）把它记成一条 `source_unavailable`
  warning ⇒ **整个 channel_migration 域从任务列表里消失**，尽管另外 9 个节点都正常返回、
  且已经收集到的任务行被直接丢掉。
  `hasMore` 分支更隐蔽：从节点 k 命中截断就立即 `return`，节点 k+1..n **根本没被扫过**，
  但对外只报一条笼统的 `source_truncated`，运维无法知道哪些节点被跳过。
  同时这两个循环都没有单节点 deadline，所以 **[P1] 1** 的挂死会沿着这条扇出传播。
- **后果**：聚合层本身是为部分结果设计的（`DistributedTaskWarning` / `Partial` 字段齐备，
  另两个源也确实会产出 per-source warning），但这两个源从不产出 per-node warning，
  于是集群降级时运维的任务中心恰好看不到正在进行的迁移任务 —— 最需要它的时候它是空的。
- **建议**：把 per-node 失败降级为 per-node warning 并保留已收集结果；
  给每个节点的读加独立 deadline；截断 warning 里带上已扫描/未扫描的节点范围。

### [P3] 7. 三处死代码 / 恒真分支，其中一处造成"已校验"的错误安全感

- **位置**：`internal/usecase/management/node_scalein.go:417-420`；
  `internal/usecase/management/node_scalein_channel.go:177-180`；
  `internal/usecase/management/channel_runtime_meta.go:260-274`
- **类别**：架构 / 可读性
- **代码**：
  ```go
  	// node_scalein.go:417-420 —— TransferringLeaders 在 375-396 行的分支里必定已 return，此 case 不可达
  	switch report.Status {
  	case NodeScaleInStatusMigratingReplicas, NodeScaleInStatusTransferringLeaders, NodeScaleInStatusFailed, NodeScaleInStatusBlocked:
  		return report, nil
  	}
  ```
  ```go
  	// node_scalein_channel.go:177-180 —— 两个 return 完全相同，if 判断无意义
  	if missingTarget || len(leaderCandidates) > 0 || len(replicaCandidates) > 0 {
  		return 0, []string{"no_channel_migration_target"}, false, nil
  	}
  	return 0, []string{"no_channel_migration_target"}, false, nil
  ```
  ```go
  	// channel_runtime_meta.go:270-273 —— 条件恒等（两个子句等价）且两个分支结果相同
  	if cursor.ChannelID != "" && len(cursor.ChannelID) > 0 {
  		return nil
  	}
  	return nil
  ```
- **触发路径**：不产生错误的运行时行为。但第三处意味着 `validateChannelRuntimeMetaListCursor`
  对 `cursor.SlotID != 0 && cursor.ChannelID != ""` 这一最常见的组合**实际上没有做任何校验**
  —— 函数名读起来像"已校验"而其实没有。cursor 是客户端可控输入
  （`internal/access/manager/cursor_codec.go:140` 解码后传入），后续若有人依赖这个函数
  拦截非法 cursor 就会踩空。
- **后果**：可维护性；第三处是潜在的错误安全感来源。
- **建议**：删掉不可达 case 与恒真分支；`validateChannelRuntimeMetaListCursor`
  要么补上真实校验（长度上界、SlotID 与 ChannelID 的组合合法性），要么直接删掉。

### [P3] 8. 缩容盘点把"只是 leader"的 channel 也计入 replica 计数，对运维报表数字虚高

- **位置**：`internal/usecase/management/node_scalein_channel.go:321-332`
- **类别**：正确性（可观测性）
- **代码**：
  ```go
  	referencesTarget := false
  	if meta.Leader == nodeID {
  		i.leaders++
  		referencesTarget = true
  	}
  	if referencesTarget || scaleInUint64sContain(meta.Replicas, nodeID) || scaleInUint64sContain(meta.ISR, nodeID) {
  		i.replicas++
  		referencesTarget = true
  	}
  ```
- **触发路径**：某 channel 的 `Leader == 5`，但成员变更已把 5 从 `Replicas`/`ISR` 中移除
  （迁移过程中的正常瞬态，`AbortChannelMigration` 的 learner 回滚路径就会产生这种状态）。
  此时 `leaders++` 使 `referencesTarget=true`，于是第二个 if 因短路求值直接成立，
  `replicas++` 也被执行 —— 尽管节点 5 并不是该 channel 的 replica 成员。
- **后果**：`NodeScaleInProgress.ChannelReplicas` 的注释是 "authoritative channel replica
  memberships still referencing the target"，实际数字被高估。不影响安全判定
  （同一条路径上 `ChannelLeaders > 0` 已经阻塞缩容），只误导运维对排空进度的估计。
- **建议**：`replicas` 的判定去掉 `referencesTarget ||`，只看 `Replicas` / `ISR` 的成员关系。

## 已排除的候选项

- `node_scalein_channel.go:145-162` —— `advanceNodeScaleInChannels` 的 `raced` 分支让
  `/advance` 返回 HTTP 200 且不报任何 blocker，一度怀疑是"静默无进展"。
  实际是**正确的竞态处理**：`report` 取自快照 A 的 `ActiveChannelMigrationsInvolvingNode` 预检
  （`node_scalein.go:421`），候选取自之后的新扫描，两者之间确实可能有任务出现；
  而且此时状态会变成 `waiting_channel_migrations`，运维**看得到**。
  真正不可恢复的只有 **[P1] 3** 那条。
- `node_scalein_channel.go:390-400` `scaleInChannelInventoryCursorAfter` —— 它按
  **长度优先、再字典序**比较 cursor 是否前进，看起来与 Pebble 的字典序迭代不符
  （会把 "aaaa" → "zzz" 误判为未前进 ⇒ `partial=true` ⇒ 缩容被 `channel_inventory_unavailable`
  永久阻塞）。实际**安全**：`pkg/db/internal/keycodec/codec.go:46-52` 的 `AppendString` 用
  `BigEndian uint16 长度前缀 + 字节`，因此 key 的字节序本身就是"长度优先、再字典序"，
  该比较与存储层排序完全一致。误报。
- `node_scalein_channel.go:200-211` / `channel_runtime_meta.go:220-227` —— 分页 `after = nextCursor`
  的无进展检查（前者有、后者无），担心死循环。实际不可能：
  `pkg/db/meta/table_runtime_meta.go:235-244` 与 `pkg/slot/proxy/runtime_meta_rpc.go:318-345`
  在 `len(rows)==0` / 归并队列耗尽时必定返回 `done=true`，
  不存在"cursor 不变且 done=false"的组合。
- `node_scalein_channel.go:229-232` `migrationApp := *a` + `scaleInChannelMigrationCluster`
  把 Draining 源节点伪装成 Alive（`node_scalein_channel.go:41-54`）—— 逐个核对了被影响的校验：
  `activeMigrationDataNodeCount`（`single_node_cluster`）虽被抬高，但双节点集群下
  `selectScaleInChannelReplicaTarget` 找不到非源的 alive 节点会先返回 `!ok`，走不到创建；
  `hasEligibleEmbeddedLeader`（`channel_migration.go:419-430`）的循环显式 `continue` 掉 source；
  `isActiveMigrationDataNode(nodes, req.TargetNodeID)` 中 target 永不等于 source。
  唯一被真正改变的是 `source_leader_not_alive`，而这正是代码注释声明的意图。
  按值拷贝 `App` 也安全（`app.go:355-388` 的 `App` 无任何锁字段，全仓 `go vet` copylocks 干净）。
- `channel_migration.go:351-362` `validateNoActiveChannelMigration` 的 check-then-act ——
  看似 TOCTOU，但落库走 `CreateChannelMigrationTaskWithRuntimeGuard`
  （`pkg/db/meta/batch.go:234-258`），guard 在 batch 提交时于同一原子操作内重新校验
  runtime meta 的 epoch/leader/fence，冲突返回 `ErrConflict`；调用侧的
  `scaleInChannelMigrationRace` 也把 `ErrAlreadyExists` / `ErrStaleMeta` 当竞态处理。安全。
- `node_scalein.go:564-574` `scaleInRemainingAliveDataNodes` 的 `out := alive[:0]` 原地过滤 ——
  典型的"看着危险"写法。实际安全：`alive` 是 `activeAliveDataNodes` 新分配的私有切片，
  range 的 `node` 是值拷贝、且写入下标恒不大于读取下标，无调用方共享底层数组。
- `node_scalein.go:417-420` 缩容停在 `migrating_replicas` 是否无人推进 —— 有人推进：
  `pkg/controller/plane/planner.go:296` 把 `NodeStatusDraining` 归入不可用节点，
  controller planner 会自动把 slot replica 搬走。这一段状态转移是自愈的，不是缺陷。
- `channel_migration.go:279-317` `AbortChannelMigration` 的 guard 含 `ExpectedUpdatedAtMS` /
  `ExpectedOwnerLeaseUntilMS`（`channel_migration.go:487-498`），单次 CAS 无内部重试，
  理论上会输给正在热重试的执行器。但执行器有 `NextRunAtMS` 退避（秒级），
  abort 的窗口足够宽，写不出稳定的活锁触发序列。排除。
- `distributed_tasks.go:827` gosec G115 `uint8(channelType)` ——
  `decodeChannelMigrationDistributedTaskID`（`:911-925`）只校验 `ChannelType > 0`、不校验 ≤255，
  构造 `channel_type=257` 会截断成 1。但这是只读详情查询，随后有 `detail.TaskID != taskID`
  相等性检查，`GetChannelMigration` 还会经 `validateManagementChannelID`（`channel_migration.go:512-517`）
  拒掉截断成 0 的情况。至多是同一资源的另一种编码，写不出造成错误结果的触发路径。
- `distributed_tasks.go:436` gosec G115 `uint64(task.Scope.ChannelType)` ——
  只用于 `distributedTaskKeywordText` 拼关键字搜索串，负值变大数只影响搜索匹配，无后果。误报。
- `node_scalein_channel.go:230`、`channel_runtime_meta.go:167/321/337/409` gosec G115 ——
  这些 `uint8(meta.ChannelType)` / `uint32(slotID)` 的入参来自集群自己写入的元数据或内部 slot ID，
  不由远程输入驱动，且 channel type 在线路协议里本身就是 uint8。误报。
  （staticcheck 在本分片 6 个文件上**零命中**。）
- `network.go:381-580` `ListNetworkSummary` —— 检查了对 `snapshot.PeerErrors` map 的 range
  是否可能与生产者并发写（concurrent map read/write 是 fatal）。生产者
  `internal/app/network_observability.go:313-345` 在 `o.mu.Lock()` 下构造**全新的** map 后返回，
  是真正的 copy-safe 快照；`copyNetworkTraffic/History/Discovery/Events`
  （`network.go:700-725`）也都做了切片深拷贝。另外 `network.go:382` 的 `a.now()`
  没有 `a == nil` 前置检查（`now` 是字段而非方法，nil receiver 会 panic），
  与同包其它方法的防御风格不一致，但访问层持有的 `*App` 恒非 nil，写不出触发路径。
- `network.go:5` import `pkg/transport` —— 不违反分层：`AGENTS.md:203` 规定的方向是
  `usecase -> runtime/pkg`，`pkg/transport` 属于 pkg，不是入口协议依赖。
  本分片 6 个文件均无 gateway / frame / HTTP 依赖，category (i) 通过。
- **category (e) 无界增长** —— 本分片**没有任何长生命周期的 map/slice**。
  `distributed_tasks.go` 是纯只读聚合（**不存在任务注册表或历史 map**，每次请求都重新向各源拉取），
  `seenTasks`、`peers`、`viewBySlot`、`trafficByType` 等 map 全是请求生命周期内的局部变量，
  随请求结束回收；`Warnings` / `BlockedReasons` 的 append 上界由节点数与检查项数决定。无泄漏。
- **category (a)(d) 并发与资源** —— 本分片 6 个文件对
  `go func` / `sync.Mutex` / `sync.RWMutex` / `time.NewTicker` / `time.NewTimer` / `make(chan` /
  `.Close()` / `NewIter` 的 grep **全部为 0 命中**。全是请求内的同步调用链，
  这两类除 [P1] 1 的 goroutine 阻塞泄漏外无其它适用发现。

## 本分片整体评价

这是在跑的生产 v1 代码，而且**一致性骨架写得相当好**：channel 迁移有真正的 write fence
（token + version + epoch，落库时在 raft apply 内做 CAS），而不是 TOCTOU 式的先查后写；
所有动作都从零重新推导因而天然幂等；缩容的 preflight 是彻底 fail-closed 的
（runtime 连接计数读不到就永不 `ready_to_remove`）。我在这里**找不到消息丢失或脑裂类缺陷**
—— 问题全部集中在**可用性、可完成性、可观测性**三类。

最需要优先处理的是 **[P1] 1**：`loadNodeScaleInSnapshot` 向"正在被缩掉的那个节点"
发一个彻底无 deadline 的 RPC（证据链已逐跳走到 `pkg/transport/conn.go:63` 的三路 select）。
被缩的节点恰恰最可能处于"TCP 还在、进程不回包"的状态，一挂就把 `/scale-in` 全部端点
连同整个 `/distributed-tasks` 任务中心永久挂死，并且每次面板轮询泄漏一个 goroutine
—— 而这段代码对该读的**出错**已经处理得完美无缺，只差一个超时。这与 unit 08 在
`monitor_metrics.go` / `runtime_summary_rpc.go` 的发现同根，应当作为一个修复一起做掉。

第二优先是 **[P1] 2** 的 O(N²)：每次 `/advance` 做 3 次全量 channel 元数据表扫描
（大部分是跨节点 RPC）却只创建 1 个迁移任务，使中等规模集群的节点缩容在工程上不可完成；
**[P1] 3** 则是唯一真正"无逃生舱"的状态 —— 卡在 `clear_fence` 的任务既不能 abort，
又会永久堵死该 channel 的迁移与相关节点的缩容。
最后补一条上下文：按 unit 05 的结论（`WK_MANAGER_AUTH_ON` 代码默认 false），
上述缩容与迁移端点在默认配置下**可被未鉴权调用方触发**，
这让 [P1] 1、[P1] 2、[P2] 5 三条从"脆弱"升级为"可被远程放大"。
