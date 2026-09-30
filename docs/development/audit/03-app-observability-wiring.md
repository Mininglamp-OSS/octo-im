# app 可观测性 + channel/plugin/cmdsync/manager 装配

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/app/observability.go | 976 | 是 |
| internal/app/network_observability.go | 828 | 是 |
| internal/app/channelcluster.go | 488 | 是 |
| internal/app/plugin.go | 363 | 是 |
| internal/app/channelmeta.go | 318 | 是 |
| internal/app/manager_message_retention.go | 313 | 是 |
| internal/app/channelmigration.go | 272 | 是 |
| internal/app/cmdsync_intent.go | 260 | 是 |
| internal/app/channelretention.go | 241 | 是 |
| internal/app/plugin_receive_observer.go | 232 | 是 |
| internal/app/cmdsync.go | 220 | 是 |
| internal/app/manager_messages.go | 196 | 是 |
| internal/app/diagnostics.go | 193 | 是 |
| internal/app/plugin_committed.go | 108 | 是 |
| internal/app/manager_channel_cluster_operations.go | 89 | 是 |
| internal/app/channelmeta_liveness.go | 82 | 是 |
| internal/app/management_connections.go | 56 | 是 |
| internal/app/benchdata_adapter.go | 56 | 是 |
| internal/app/channelplane.go | 44 | 是 |
| internal/app/monitor_metrics.go | 39 | 是 |
| internal/app/channelmeta_statechange.go | 27 | 是 |

## 发现

### [P1] 1. `/healthz/details` 在**无鉴权**公网 API 上默认开启，泄露完整集群拓扑、slot leader/peer 归属、磁盘容量与数据目录绝对路径

- **位置**：`internal/app/observability.go:364-487`（生产者）、`internal/app/observability.go:765-790`（storage 段）、`internal/access/api/routes.go:10-12`（注册点）
- **类别**：安全（信息泄露）
- **代码**：

  注册点只受 `healthDetailEnabled` 控制，engine 上唯一的中间件是 CORS：
  ```go
  // internal/access/api/routes.go:10-12
  s.engine.GET("/healthz", s.handleHealthz)
  if s.healthDetailEnabled && s.healthDetails != nil {
      s.engine.GET("/healthz/details", s.handleHealthzDetails)
  }
  ```
  ```go
  // internal/access/api/server.go:190-191
  engine := gin.New()
  engine.Use(openCORSMiddleware())
  ```
  载荷里逐 slot 暴露 leader 与成员集合、配置 epoch：
  ```go
  // internal/app/observability.go:408-416
  slotDetails = append(slotDetails, map[string]any{
      "id":             view.SlotID,
      "role":           localSlotRole(a.cfg.Node.ID, view),
      "leader_id":      view.LeaderID,
      "healthy_voters": view.HealthyVoters,
      "has_quorum":     view.HasQuorum,
      "current_peers":  view.CurrentPeers,
      "applied_epoch":  view.ObservedConfigEpoch,
      "last_report_at": view.LastReportAt,
  })
  ```
  storage 段把 `os` 错误字符串原样回吐（`*fs.PathError` 含数据目录**绝对路径**），并给出剩余磁盘字节：
  ```go
  // internal/app/observability.go:800（错误串来源）
  storeErrors[store.name] = err.Error()
  // internal/app/observability.go:782,787
  snapshot["disk_free_bytes"] = int64(diskFreeBytes)
  snapshot["errors"] = storeErrors
  ```
- **触发路径**：
  1. 默认配置下 `internal/app/config.go:1355-1357` 把 `HealthDetailEnabled` 置为 `true`（`if !c.Observability.healthDetailEnabledSet { c.Observability.HealthDetailEnabled = true }`），且 `cfg.API.ListenAddr` 默认绑 `0.0.0.0:5001`。
  2. 任何能路由到该端口的人执行 `curl http://<node>:5001/healthz/details` —— 公网 engine 上没有任何认证中间件（仅 `openCORSMiddleware`），handler 直接返回 `healthDetailsSnapshot()` 的全部字段。
  3. 返回体即包含：`node_id`/`node_name`（:465-466）、每个 slot 的 `leader_id` + `current_peers`（集群节点 ID 集合）+ `applied_epoch` + `has_quorum`、`alive_nodes`/`suspect_nodes`/`dead_nodes`、`active_tasks_by_type`、`active_migrations`、`hash_slot_table_version`、每个存储的 `disk_usage_bytes`、`disk_free_bytes`、各协议在线连接数、`active_channels`/`max_channels`。
  4. 若任一数据目录不可读（权限变更、卷卸载），`storeErrors` 把 `open /data/octo/channel_log: permission denied` 这类含绝对路径的串直接回吐。
- **后果**：外部攻击者无需任何凭据即可：绘制完整集群拓扑与 raft 成员分布（哪个节点是哪些 slot 的 leader → 精准选择攻击目标）；持续轮询 `has_quorum`/`suspect_nodes`/`active_migrations` 以侦测集群正处于脆弱期（迁移中、失去 quorum）并择机加压；读取 `disk_free_bytes` 判断磁盘打满攻击还需多少数据；通过 `max_channels` 与 `active_channels` 推算容量水位；通过错误串获得宿主机文件系统布局。这是典型的攻击前侦察面。
- **说明（与设计规则的关系）**：本条**不**涉及 `from_uid`/消息内容/按用户元数据 —— 我核对了 `healthDetailsSnapshot` 的全部返回字段，没有任何用户维度数据，`docs/superpowers/plans/2026-05-14-diagnostics-dynamic-tracking.md:20` 的 "Do not expose `from_uid` in manager/web event DTOs" 规则在本文件中**未被违反**。泄露的是基础设施与拓扑维度，因此定 P1 而非 P0。
- **建议**：`/healthz/details` 不应与业务 API 共用无鉴权 engine —— 要么移到 manager 平面（走已有的权限中间件），要么默认 `HealthDetailEnabled=false`，要么至少把 `current_peers`/`leader_id`/`disk_free_bytes`/`errors` 这几类降级为仅在显式开启 debug 时输出，并把 `os` 错误串替换为不含路径的分类码。

### [P1] 2. `/metrics` 与 `/healthz/details` 每次请求同步递归 walk 5 个 Pebble 数据目录，无缓存、无鉴权 —— 可被匿名请求放大成 I/O DoS

- **位置**：`internal/app/observability.go:351-362`（handler）、`731-738`（无缓存刷新）、`792-806`（5 个目录）、`939-963`（`filepath.Walk`）
- **类别**：性能 / 安全（资源放大 DoS）
- **代码**：
  ```go
  // internal/app/observability.go:351-362
  func (a *App) metricsHandler() http.Handler {
      handler := a.metrics.Handler()
      return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
          a.refreshControllerMetrics()
          a.refreshTransportMetrics()
          a.refreshStorageMetrics()
          handler.ServeHTTP(w, r)
      })
  }
  ```
  ```go
  // internal/app/observability.go:731-738 —— 注意：没有任何时间缓存
  func (a *App) refreshStorageMetrics() {
      if a == nil || a.metrics == nil || a.metrics.Storage == nil {
          return
      }
      usageByStore, _, _ := a.collectStorageUsage()
      a.metrics.Storage.SetDiskUsage(usageByStore)
  }
  ```
  ```go
  // internal/app/observability.go:939-957 —— Walk 用 Lstat + 每层目录排序
  err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
      if err != nil { return err }
      if info == nil || info.IsDir() { return nil }
      total += info.Size()
      return nil
  })
  ```
- **触发路径**：
  1. `collectStorageUsage`（:792-806）对 `storageStorePaths()`（:809-823）返回的 **5 个**目录 —— `meta` / `raft` / `channel_log` / `controller_meta` / `controller_raft` —— 逐个调 `dirUsageBytes`。
  2. `dirUsageBytes` 用 `filepath.Walk`（而非 `filepath.WalkDir`），因此对**每个文件**都做一次 `Lstat` syscall，并对每层目录的 entry 名做一次 `sort.Strings`。一个跑了一段时间的 IM 节点，5 个 Pebble store 加起来的 SST/WAL/MANIFEST 文件数轻易上万。
  3. 对比证据：同一个 handler 里的 `refreshControllerMetrics`（:686-697）走的是 `cachedObservedClusterState()`，带 `controllerMetricsRefreshInterval = 2 * time.Second`（:36）的缓存 + `CompareAndSwap` 单飞（:710-729）。**只有 storage 这条路径漏掉了缓存**，说明这是遗漏而非有意设计。
  4. `MetricsEnabled` 默认 `true`（`internal/app/config.go:1349-1351`），`/metrics` 注册在**无鉴权**的公网 engine 上（`internal/access/api/routes.go:17-20` + `server.go:190-191` 只有 CORS）。攻击者 `while true; do curl -s http://<node>:5001/metrics >/dev/null; done` 即可让节点持续对数据盘做全目录 stat 风暴。
  5. 同一条路径**第二次**暴露：`storageHealthSnapshot()`（:765-790）也调 `collectStorageUsage()`，而它由 `/healthz/details`（同样默认开启、同样无鉴权）触发。两个端点可叠加。
- **后果**：page cache 被目录元数据挤占、与 Pebble compaction 争抢磁盘 IOPS；在 HDD 或受 IOPS 限额的云盘上，单个匿名客户端的循环请求即可把写入延迟推高，进而拖慢 raft apply 与消息落盘。同时因为 walk 是在 HTTP handler 里**同步**执行的，正常的 Prometheus 抓取也会随数据量增长而逐渐超时，导致监控自身先失效。
- **建议**：给 `refreshStorageMetrics` / `storageHealthSnapshot` 套上与 controller 指标同构的 TTL 缓存 + 单飞（复用 `observedClusterStateCache` 的模式），把 walk 移到后台周期任务；顺手把 `filepath.Walk` 换成 `filepath.WalkDir` 省掉逐文件 `Lstat`。

### [P1] 3. 组合根里手写了第二套 retention 决策实现，与 `internal/runtime/channelretention` 重复；且默认配置下 TTL=0 使唯一的修复路径失效，manager 侧两步写失败后元数据与运行时边界永久不一致

- **位置**：`internal/app/manager_message_retention.go:82-199`（重复实现）对照 `internal/runtime/channelretention/worker.go:119-159, 292-338, 352-368`（正版实现）；`internal/app/channelretention.go:195-198`（TTL=0 时 worker 为 nil）
- **类别**：架构（分层违规 + v1/v1 重复实现）/ 正确性（状态不一致）
- **代码**：

  组合根版本 —— 门限决策、leader 租约 fencing、两步写全部手写在 `internal/app`：
  ```go
  // internal/app/manager_message_retention.go:185-199
  func managerRetentionDecision(requested uint64, current uint64, gates []managerRetentionGate) (uint64, managementusecase.MessageRetentionBlockedReason) {
      through := requested
      reason := managementusecase.MessageRetentionBlockedReasonNone
      for _, gate := range gates {
          if gate.seq < through {
              through = gate.seq
              reason = gate.reason
          }
      }
      ...
  }
  ```
  ```go
  // internal/app/manager_message_retention.go:161-170
  func (o managerMessageRetentionOperator) validateRetentionLeaderView(view channel.RetentionView) error {
      if view.Leader != channel.NodeID(o.localNodeID) || !view.CommitReady {
          return channel.ErrStaleMeta
      }
      if leaseUntil := view.LeaseUntil; !leaseUntil.IsZero() && !o.clockNow().Before(leaseUntil) {
          return channel.ErrStaleMeta
      }
      return nil
  }
  ```
  运行时正版 —— 同一套语义已经存在于 `internal/runtime/*`：
  ```go
  // internal/runtime/channelretention/worker.go:145-159
  func calculateRetentionBoundary(expiredThrough, current uint64, gates []boundaryGate) boundaryDecision {
      if expiredThrough == 0 {
          return boundaryDecision{AdvanceThroughSeq: current, BlockedReason: BlockedNoExpiredPrefix}
      }
      candidate := expiredThrough
      ...
  }
  // internal/runtime/channelretention/worker.go:352-361
  func (w *Worker) channelEligible(view channel.RetentionView, now time.Time) bool {
      if view.Leader != w.cfg.LocalNodeID { return false }
      if !view.CommitReady { return false }
      return view.LeaseUntil.IsZero() || now.Before(view.LeaseUntil)
  }
  ```
  两处的两步写完全同构（先改元数据，再改运行时边界，中间无补偿）：
  ```go
  // internal/app/manager_message_retention.go:142-147
  if err := o.metadata.AdvanceChannelRetentionThroughSeq(ctx, managerRetentionAdvanceRequest(id, latest, throughSeq, o.clockNow())); err != nil {
      return managementusecase.AdvanceMessageRetentionResponse{}, err
  }
  if err := o.runtime.ApplyRetentionBoundary(ctx, key, throughSeq); err != nil {
      return managementusecase.AdvanceMessageRetentionResponse{}, err
  }
  ```
  运行时 worker 为这种不一致准备了修复路径，但它只在 worker 存在时才跑：
  ```go
  // internal/runtime/channelretention/worker.go:363-368
  func (w *Worker) needsExistingBoundaryApply(view channel.RetentionView) bool {
      if view.RetentionThroughSeq == 0 { return false }
      return view.LocalRetentionThroughSeq < view.RetentionThroughSeq ||
          view.PhysicalRetentionThroughSeq < view.RetentionThroughSeq
  }
  ```
  而 worker 在 TTL 未配置时根本不被创建：
  ```go
  // internal/app/channelretention.go:195-198
  func newAppChannelRetentionWorker(cfg appChannelRetentionConfig, ...) *appretention.Worker {
      if cfg.ttl <= 0 {
          return nil
      }
  ```
- **触发路径**：
  1. `ChannelMessageRetention.TTL` **没有任何默认值** —— `internal/app/config.go:1468-1476` 只给 `ScanInterval`(1h) / `ChannelBatchSize` / `MaxTrimMessages` 兜底，`TTL` 全程只有 `:927` 的 `< 0` 校验。因此**默认配置下 TTL == 0**，`newAppChannelRetentionWorker` 返回 `nil`，`internal/app/lifecycle.go:247-250` 直接跳过 Start。**后台 retention worker 默认不运行**。
  2. 运维调用 `POST /manager/channels/messages/retention`（`internal/access/manager/routes.go:272`）→ `managerMessageRetentionOperator.advanceLocal`。
  3. 走到 `:142`，`AdvanceChannelRetentionThroughSeq` 成功 —— 元数据（集群权威）已经把 `RetentionThroughSeq` 推进到 N 并持久化。
  4. 紧接着 `:145` 的 `ApplyRetentionBoundary` 失败。真实失败源：`pkg/channel/runtime/retention.go` 在 channel 已被 idle-evict / `RemoveChannel` 后返回 `ErrChannelNotFound`；或步骤 3、4 之间 leader 漂移导致运行时拒绝。注意第 3 步的 CAS 用的是**第 2 步读到的** `latest` 视图，两步之间存在真实的时间窗。
  5. handler 向运维返回 error，运维合理地认为"没生效"。但元数据已经推进。
  6. 因为 worker 是 nil，`needsExistingBoundaryApply` 这条**唯一的**修复路径永远不会执行。不一致永久存在。
- **后果**：
  - **状态不一致（主要后果）**：元数据宣称 `RetentionThroughSeq = N`，而运行时的 `LocalRetentionThroughSeq` / `PhysicalRetentionThroughSeq` 仍停留在旧值。其它节点按元数据认为 `< N` 的消息已被裁剪、拒绝为其服务读请求，而本地磁盘上这些消息仍占着空间且从未被回收 —— 既读不到又删不掉。重试也不会自愈：重试路径从 `RetentionView`（运行时视图）读 `RetentionThroughSeq`，读到的是元数据侧的值还是运行时侧的值取决于视图构造，且第 2 步的 `req.ThroughSeq <= view.RetentionThroughSeq` 早退可能直接把重试判成 noop。
  - **规则绕过**：worker 版本的 `expiredThrough` 来自 `ScanExpiredMessagePrefix(fromSeq, now-TTL, limit)`（worker.go:311），即**只允许推进到真正 TTL 过期的前缀**；manager 版本的 `through` 直接取运维传入的 `req.ThroughSeq`，**完全没有 TTL/过期门限**。同一个不变式在两条路径上强度不同，manager 路径可以裁掉尚未过期的消息。
  - **架构**：违反 `AGENTS.md:182,184`（"`internal/usecase/*` 承载业务编排"、"`internal/app/*` 是唯一组合根；**依赖装配只放这里**"）。`manager_message_retention.go` 313 行里，leader 路由、租约 fencing、四门限 min 决策、重读-再校验的 TOCTOU 缓解、两步提交、status/blocked-reason 分类、错误→blocked 分类（`managerRetentionCursorBlocked:238-240`）全是业务规则，不是装配。对照组就在同一个分片里：`internal/app/channelretention.go` 全文 241 行是**纯适配器**（把 `channelstore.Engine`、runtime port、metadata store 适配成 `appretention.Config` 的端口），业务规则老老实实放在 `internal/runtime/channelretention`。同一个 retention 领域，一条路径做对了，另一条把同样的规则抄进了组合根。
- **建议**：把 `managerRetentionDecision` / `validateRetentionLeaderView` / 两步提交序列下沉，复用 `internal/runtime/channelretention` 已有的 `calculateRetentionBoundary` / `channelEligible` / `needsExistingBoundaryApply`，让 manager 手动推进与后台 worker 共享**同一份**决策与修复代码；`internal/app` 只保留端口装配。另外给 `ChannelMessageRetention.TTL` 一个显式默认值，或在 TTL=0 时仍然创建一个只做 `needsExistingBoundaryApply` 修复、不做 TTL 裁剪的 worker，否则手动路径永远没有补偿。

### [P1] 4. `NetworkSnapshot` 在持有全局 `o.mu` 期间对最多 ~15 万个 duration 样本做 3 次排序，而同一把锁被**每一次**跨节点 send / RPC 同步争用

- **位置**：`internal/app/network_observability.go:318`（Lock）→ `:434`（Unlock）；热点写入方 `:588-601`（`recordDial`）、`:604-622`（`recordEnqueue`）、`:624-654`（`recordRPCClient`）；排序 `:802-816`
- **类别**：性能 / 并发（锁持有时间）
- **代码**：

  快照全程持锁，中途对每个 service 累加器做 3 次全量拷贝 + 排序：
  ```go
  // internal/app/network_observability.go:318-321
  o.mu.Lock()
  o.pruneLocked(cutoff)
  ```
  ```go
  // internal/app/network_observability.go:412-414 —— 跨全部时间桶累加，累加器本身无上限
  acc.durations = append(acc.durations, aggregate.durations...)
  ```
  ```go
  // internal/app/network_observability.go:425-433
  for _, acc := range services {
      if len(acc.durations) > 0 {
          acc.service.P50Ms = durationPercentileMs(acc.durations, 0.50)
          acc.service.P95Ms = durationPercentileMs(acc.durations, 0.95)
          acc.service.P99Ms = durationPercentileMs(acc.durations, 0.99)
      }
      snap.Services = append(snap.Services, acc.service)
  }
  ...
  o.mu.Unlock()   // :434
  ```
  ```go
  // internal/app/network_observability.go:802-808 —— 每次调用都重新拷贝 + 重新排序
  func durationPercentileMs(values []time.Duration, quantile float64) float64 {
      if len(values) == 0 { return 0 }
      sorted := append([]time.Duration(nil), values...)
      sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
  ```
  被争用的是同一把 `o.mu`，而它在每次跨节点 RPC 完成时都会被拿走：
  ```go
  // internal/app/network_observability.go:624-634
  func (o *networkObservability) recordRPCClient(event transport.RPCClientEvent) {
      at := o.now()
      key := networkRPCKey{targetNode: uint64(event.TargetNode), serviceID: event.ServiceID}
      o.mu.Lock()
      ...
  ```
- **触发路径**：
  1. 样本量估算（**每个** `(targetNode, serviceID)` 累加器）：单桶 durations 确实有上限 `networkObservabilityMaxDurations = 256`（:23，`:645` 处 `if len(aggregate.durations) < networkObservabilityMaxDurations` 守住），但 `:412` 是**跨桶累加**且累加器没有上限。桶宽 `networkObservabilityBucketSize = 100ms`（:20），窗口 `defaultNetworkObservabilityWindow = time.Minute`（:17）→ 窗口内 **600 个桶**，再乘以 result 维度（ok / expected_timeout / timeout / queue_full / remote_error / other）。仅 `result="ok"` 一项在持续压力下就是 600 × 256 = **153,600** 个 duration。桶要填满只需该 peer-service 在 100ms 内有 ≥256 次 RPC，即 2,560 RPC/s —— 繁忙集群的 channel data-plane 完全达得到。
  2. 三次 `durationPercentileMs` = 3 次 `append` 拷贝（每次 153,600 × 8B ≈ 1.2MB 新分配）+ 3 次 `sort.Slice` ≈ 3 × 153,600 × log₂(153,600) ≈ **790 万次比较**，而且是带接口间接调用的 `sort.Slice`（非 `slices.Sort`）。这还只是**一个** service 累加器；`services` map 的规模是 (对端节点数 × RPC service 数)，几十个累加器时总量再放大一个量级。
  3. 与此同时，`o.mu` 是数据面的必经之路：`pkg/transport/pool.go:86,90`（`Pool.SendWithContext` —— **每一条**跨节点消息）和 `:102,108`（`Pool.RPC`）同步调 `observeEnqueue` → `OnEnqueue` → `recordEnqueue` → `o.mu.Lock()`；`pkg/transport/client.go:67-73` 在每次 RPC 返回后同步调 `OnRPCClient` → `recordRPCClient` → `o.mu.Lock()`。全部在**调用方自己的 goroutine 上**执行，没有异步化。
  4. 入口：运维打开集群网络页 → `GET /manager/network/summary`（`internal/access/manager/routes.go:146-150`，需 `cluster.network:r`）→ `internal/usecase/management/network.go:402` → `NetworkSnapshot`。仪表盘通常**自动轮询**（数秒一次），于是这个停顿周期性重复。
  5. `NetworkEnabled` 默认 `true`（`internal/app/config.go:1352-1354`），这套 hook 默认就装上了。
- **后果**：快照期间，本节点**所有**跨节点消息发送与 RPC 在 `o.mu` 上排队 —— 这直接卡住 channel log 复制、slot raft 消息、data-plane 投递。越是繁忙的集群样本越多、停顿越长，恰好在运维因为集群变慢而去打开网络监控页时最严重，形成"观测行为本身加剧故障"的正反馈。因为需要 manager 认证与 `cluster.network:r` 权限，不构成匿名 DoS，故定 P1 而非 P0。
- **佐证（作者已意识到这个模式）**：同一文件里 traffic 维度**专门**做了分片以避开全局锁 —— `networkTrafficBucketStore`（:100-110）带 `networkObservabilityTrafficShards = 16`（:24）个独立 `sync.Mutex`，且 `:317` 的 `o.trafficBuckets.snapshot(cutoff)` 刻意放在 `o.mu.Lock()` **之前**。dial / enqueue / rpc 三个维度没有享受同样待遇。
- **建议**：把 P50/P95/P99 换成固定桶宽的 histogram（写入时 O(1) 落桶，读取时无需排序、无需保留原始样本），或至少在持锁期间只做浅拷贝、把排序与百分位计算移到 `o.mu.Unlock()` 之后；同时给 `acc.durations` 加上与单桶一致的总量上限（蓄水池采样即可）。

### [P2] 5. 组合根装配的 `memoryGenerationStore` 结构性无法淘汰条目，按"本节点历史上激活过的不同 channel"单调增长

- **位置**：`internal/app/channelmeta.go:187-221`（实现与装配）；接口 `pkg/channel/runtime/types.go:303-306`；漏删点 `pkg/channel/runtime/runtime.go:223-256`
- **类别**：资源（内存泄漏 / 无界增长）
- **代码**：
  ```go
  // internal/app/channelmeta.go:187-190
  type memoryGenerationStore struct {
      mu     sync.RWMutex
      values map[channel.ChannelKey]uint64
  }
  ```
  ```go
  // internal/app/channelmeta.go:216-221 —— 只有 Store，没有 Delete
  func (s *memoryGenerationStore) Store(channelKey channel.ChannelKey, generation uint64) error {
      s.mu.Lock()
      defer s.mu.Unlock()
      s.values[channelKey] = generation
      return nil
  }
  ```
  淘汰不可能，因为接口本身就没有删除方法：
  ```go
  // pkg/channel/runtime/types.go:303-306
  type GenerationStore interface {
      Load(key core.ChannelKey) (uint64, error)
      Store(key core.ChannelKey, generation uint64) error
  }
  ```
  `RemoveChannel` 把其它所有按 channel 维度的状态都清理了，唯独 generation 没有（也清理不了）：
  ```go
  // pkg/channel/runtime/runtime.go:239-241,249-254
  r.tombstones.add(key, ch.gen, r.cfg.Now().Add(r.cfg.Tombstones.TombstoneTTL))
  delete(shard.channels, key)
  ...
  stopTimers(r.clearReplicationRetries(key, 0, false))
  stopTimers(r.clearPendingChannelCommit(key))
  for _, peer := range r.peerRequests.clearChannel(key) {
      r.drainPeerQueue(peer)
  }
  if r.cfg.Limits.MaxChannels > 0 {
      r.releaseChannelSlot()
  }
  ```
- **触发路径**：
  1. `allocateGeneration`（`pkg/channel/runtime/runtime.go:470-479`）在 channel 创建路径上（由 `:161` 调用）对**每一个此前未见过的 channel key** 写入一条 `values[key]`。
  2. `channel.ChannelKey` 是 `string`（`pkg/channel/types.go:11`），由 `KeyFromChannelID`（`pkg/channel/handler/key.go:15-23`）从 `id.Type` + **base64 编码的 `id.ID`** 拼成，而 `id.ID` 直接来自客户端指定的 channelID（p2p 会话键、临时频道等）。因此 key 空间由客户端行为驱动、实际无上界。
  3. Channel 被移除时（显式 `RemoveChannel`，或 `:670-696` 的 idle 淘汰循环调用 `RemoveChannel`），`shard.channels`、tombstone（**带 TTL**）、复制重试定时器、pending commit、peer 队列、channel slot 计数全部被回收 —— 唯独 generation 条目留下。
  4. `Limits.MaxChannels` 与 idle 淘汰只约束**常驻**（`shard.channels`）规模，`memoryGenerationStore.values` 完全在这个约束之外。于是"当前活跃 channel 数"可以长期稳定在上限内，而 generation map 随**累计**激活过的不同 channel 数单调上涨。
  5. 触发不需要恶意流量：一个正常的 IM 集群，用户之间的 p2p 会话与临时群随时间自然积累。恶意场景下更快：客户端循环向随机 channelID 发消息即可按请求速率线性注入条目。
- **后果**：进程内存随节点运行时长单调增长且**永不回落**，重启才能释放。每条约 100+ 字节（string header + base64 后的 key 字节 + uint64 + map 桶开销），累计千万级不同 channel 时达 GB 量级。因为是纯增长型且需要时间累积，不是立即可触发的 OOM，定 P2；但在长期不重启的节点上它是确定性的。
- **建议**：给 `GenerationStore` 接口补一个 `Delete(key)` 并在 `RemoveChannel` 中调用；注意 generation 的语义是"同一 channel 重建时必须单调递增"，所以删除必须与现有的 tombstone TTL 机制对齐（tombstone 存活期间保留 generation，TTL 过后一并清理），否则会退化成 generation 回绕。或者干脆把 generation 落到已有的持久化元数据里，不再维护独立的内存 map。

### [P1] 6. 组合根把迁移 cutover 证明的三个安全字段硬编码为零值，导致 3 个闸门中 2 个永不触发、第 3 个永远走粗粒度回退 —— 需要截断/快照的目标节点无法被识别

- **位置**：`internal/app/channelmigration.go:186-221`；消费方 `internal/runtime/channelmigration/proof.go:17-22, 74-79`；线路格式 `pkg/channel/runtime/types.go:106-131`
- **类别**：分布式一致性 / 正确性
- **代码**：

  适配器**请求**扩展响应，却把三个安全字段原地写死：
  ```go
  // internal/app/channelmigration.go:183-189
  resp, err := p.client.Probe(ctx, nodeID, channelruntime.ReconcileProbeRequestEnvelope{
      ChannelKey:              meta.Key,
      Epoch:                   meta.Epoch,
      LeaderEpoch:             meta.LeaderEpoch,
      ReplicaID:               p.localNode,
      RequireExtendedResponse: true,
  })
  ```
  ```go
  // internal/app/channelmigration.go:214-220
      CheckpointHW:     resp.CheckpointHW,
      EpochHistory:     nil,
      TruncateTo:       nil,
      SnapshotRequired: false,
  }
  ```
  三个闸门全部建立在这三个字段之上：
  ```go
  // internal/runtime/channelmigration/proof.go:17-22
  target := req.Target
  if target.SnapshotRequired {
      return FinalTargetProof{}, channel.ErrSnapshotRequired
  }
  if target.TruncateTo != nil {
      return FinalTargetProof{}, channel.ErrNotReady
  }
  ```
  ```go
  // internal/runtime/channelmigration/proof.go:74-79
  func targetOffsetEpochAt(report ProbeReport, offset uint64) (uint64, error) {
      if len(report.EpochHistory) == 0 {
          if report.OffsetEpoch == 0 {
              return 0, channel.ErrStaleMeta
          }
          return report.OffsetEpoch, nil
      }
  ```
- **触发路径**：
  1. `EvaluateFinalTargetProof` 在**活跃**迁移路径上：`internal/runtime/channelmigration/replica_replace.go:264` 与 `leader_transfer.go:206` 都调它，executor 由 `internal/app/build.go:446` → `newAppChannelMigrationExecutor`（`channelmigration.go:223`）装配进生产二进制。
  2. 由于 `:218` 恒为 `nil`，`proof.go:20` 的 `target.TruncateTo != nil` **永远为假**；由于 `:219` 恒为 `false`，`proof.go:17` 的 `target.SnapshotRequired` **永远为假**。两个闸门是死代码。
  3. 由于 `:217` 恒为 `nil`，`targetOffsetEpochAt` **永远**走 `len(report.EpochHistory) == 0` 分支，返回 `report.OffsetEpoch`。按 `pkg/channel/runtime/types.go:121-122` 的注释，`OffsetEpoch` 的语义是 **"owns LogEndOffset when richer epoch history is unavailable"** —— 即"拥有 LEO 的那个 epoch"；而调用方 `proof.go:38` 要问的是 `targetOffsetEpochAt(target, req.CutoverHW)`，即 **"CutoverHW 这个偏移处的 epoch"**。只要目标副本的日志跨越多个 epoch（迁移前发生过 leader 变更 —— 这恰恰是迁移场景的典型前提），这两者就不是同一个值。
  4. 具体错判序列：目标副本在旧 epoch E1 下写入了 `[0, X)`，随后集群发生 leader 变更进入 E2，目标副本在 E2 下继续追到 LEO。此时 `CutoverHW < X`（cutover 点落在 E1 区间内），但 `OffsetEpoch` 报告的是 E2。`proof.go:41-47` 拿 E2 去比 `expectedOffsetEpoch`（= `req.CutoverOffsetEpoch` 或 `req.Meta.Epoch`，即当前 epoch E2）→ **相等 → 通过**。而真实情况是 cutover 前缀来自 E1，可能与权威副本已经分叉。
  5. 数据其实存在，只是没被传回来：本节点确实维护着 per-channel epoch history（`internal/app/channelmeta.go:285-291` 的 `channelEpochHistoryStore` 提供 `Load() ([]channel.EpochPoint, error)` / `Append` / `TruncateTo`）。问题出在 `ReconcileProbeResponseEnvelope`（`pkg/channel/runtime/types.go:106-131`）**根本没有** `EpochHistory` / `TruncateTo` / `SnapshotRequired` 这三个字段 —— 我在 `pkg/channel/transport/` 全目录确认过，`EpochHistory` 和 `SnapshotRequired` 一次都没出现；那里的 `TruncateTo`（`longpoll.go:99`、`codec.go:115-170`）属于 append/fetch 响应，与 reconcile probe 是两条不同的消息。所以 `RequireExtendedResponse: true` 请求的"扩展"里并不包含它们，适配器写 nil 不是偷懒，而是**线路上确实拿不到**。
- **后果**：cutover 前的最终安全证明被架空。需要日志截断或快照引导的目标副本不会被 `ErrNotReady` / `ErrSnapshotRequired` 拦下，且 epoch 一致性校验在跨 epoch 日志上会给出假阴性。迁移因此可能把 leader 切到一个 committed 前缀与权威副本分叉的副本上 —— 在 channel log 层面即**已提交消息丢失或回退**，且发生在"证明已通过"的静默路径上，没有任何告警。定 P1 而非 P0：需要"迁移期间恰好跨过 leader 变更、且 cutover 点落在旧 epoch 区间"这一组合条件，不是每次迁移必现。
- **架构备注**：这同时是一条 FLOW/分层问题 —— `internal/app` 作为组合根，本不该决定"证明字段填什么"。当端口要求的字段线路上不存在时，正确做法是让适配器**报错**（或让 executor 显式降级并记录"证明能力不完整"），而不是静默填零值，使下游的安全检查在类型上通过、在语义上失效。
- **建议**：把 `EpochHistory` / `TruncateTo` / `SnapshotRequired` 补进 `ReconcileProbeResponseEnvelope` 与对应的 v3 编解码（目标侧数据已由 `channelEpochHistoryStore` 持有）；在补齐之前，让 `channelMigrationProbeReportFromResponse` 在 `RequireExtendedResponse` 得不到满足时返回错误而非零值，让迁移显式失败而不是静默放行。

### [P2] 7. cmdsync 把瞬时的 `ErrNotReady` 当成"命令日志不存在"吞掉，并用**错误文本子串匹配**做分类，导致副本恢复期间客户端收到"无待同步命令"的成功响应

- **位置**：`internal/app/cmdsync.go:196-206`（分类器）；吞掉点 `:132-134`、`:156-158`、`:170-172`、`:188-190`
- **类别**：错误处理 / 正确性
- **代码**：
  ```go
  // internal/app/cmdsync.go:196-206
  func isMissingCommandLog(err error) bool {
      if err == nil {
          return false
      }
      if errors.Is(err, channel.ErrNotReady) || errors.Is(err, channel.ErrChannelNotFound) {
          return true
      }
      msg := err.Error()
      return strings.Contains(msg, channel.ErrNotReady.Error()) ||
          strings.Contains(msg, channel.ErrChannelNotFound.Error())
  }
  ```
  四个调用点全部把它翻译成"成功且为空"：
  ```go
  // internal/app/cmdsync.go:186-192（loadRemote）
  page, err := s.remote.QueryChannelMessages(ctx, nodeID, accessnode.ChannelMessagesQuery{...})
  if isMissingCommandLog(err) {
      return nil, nil
  }
  if err != nil {
      return nil, err
  }
  ```
- **触发路径**：
  1. 两个 error 的语义完全不同：`pkg/channel/errors.go:12` 的 `ErrNotReady = errors.New("channel: not ready")` 表示"副本存在但**尚未就绪**"（瞬时状态），而 `ErrChannelNotFound` 表示"确实没有这个 channel"（稳定事实）。函数名 `isMissingCommandLog` 只描述后者，却把前者也算进去。
  2. 具体序列：客户端连到节点 A 做离线 CMD 同步；该 command channel 的 leader 是节点 B；B 刚重启或正在恢复 channel replica，尚未 `CommitReady`。A 走 `loadRemote`（`:178`）→ 远端返回 `ErrNotReady` → `isMissingCommandLog` 为真 → `return nil, nil`。
  3. 上游 `internal/usecase/cmdsync/app.go:112-115` 只在 `err != nil` 时中断；拿到 `nil, nil` 就当作该 channel 没有消息，继续拼装结果并返回**成功**响应。客户端收到 HTTP 200 + 空命令列表 —— 与"你已完全同步"无法区分。
  4. 子串匹配进一步放大：`errors.Is` 已经覆盖了正常的 `%w` 包装链，额外的 `strings.Contains` 是为跨 RPC 丢失 error 身份的场景兜底，但它把判定面扩大到"**任何**错误文本里恰好出现 `channel: not ready` 或 `channel: channel not found`"。`loadRemote` 的 error 来自远端节点、已被降级成字符串，其中可能嵌套了与本 channel 无关的下层原因，这类错误同样会被静默转成"空且成功"。
  5. 同一分片内存在**严格版**对照：`internal/app/manager_messages.go:65-85` 的 `MaxMessageSeqForMeta` 走几乎相同的 local/remote 分流，但对 error 一律 `return 0, err`，不做任何吞咽。宽松版恰好用在客户端主路径上。
- **后果**：不是永久数据丢失 —— `internal/usecase/cmdsync/app.go:110` 的 `fromSeq := candidate.readSeq + 1` 依赖已持久化的 readSeq，空响应不会推进游标，因此 B 恢复后下一轮同步能补回来。真实危害是**把瞬时不可用伪装成权威的"空"**：客户端失去了重试/退避信号，在 B 的整个恢复窗口内持续被告知"没有待同步命令"；任何把空同步结果当作"已追平"来更新本地状态或 UI 的客户端实现，都会在这个窗口里表现为命令丢失。同时子串匹配让无关错误也被静默吞掉，故障期间失去可观测性。
- **建议**：把 `ErrNotReady` 从 `isMissingCommandLog` 中摘出去并向上返回（让客户端得到可重试的错误或明确的"稍后再试"状态），只保留 `ErrChannelNotFound` 走"空且成功"；跨 RPC 的 error 身份应通过结构化错误码传递，而不是靠 `strings.Contains` 匹配错误文本。

### [P2] 8. 组合根把与 manager 平面**完全相同**的无限制消息读取器交给插件子系统，绕过已有的 plugin↔UID binding 模型

- **位置**：`internal/app/plugin.go:80-89`；对照装配点 `internal/app/build.go:796-800`（message usecase）与 `internal/app/build.go:882-887`（manager 平面）；消费方 `internal/usecase/plugin/host_rpc.go:38-56`
- **类别**：安全（权限边界）/ 架构
- **代码**：
  ```go
  // internal/app/plugin.go:80-89
  func pluginMessageReader(a *App) managerMessageReader {
      if a == nil {
          return managerMessageReader{}
      }
      return managerMessageReader{
          localNodeID: a.cfg.Node.ID,
          channelLog:  a.channelLogDB,
          metas:       a.store,
          remote:      a.nodeClient,
      }
  }
  ```
  完全相同的字面量同时被交给受 JWT + 权限中间件保护的 manager 平面：
  ```go
  // internal/app/build.go:882-887
  Messages: managerMessageReader{
      localNodeID: cfg.Node.ID,
      channelLog:  app.channelLogDB,
      metas:       app.store,
      remote:      app.nodeClient,
  },
  ```
  而插件侧的消费方**显式丢弃**了插件身份：
  ```go
  // internal/usecase/plugin/host_rpc.go:37-45
  func (a *App) ChannelMessages(ctx context.Context, req *pluginproto.ChannelMessageBatchReq, _ string) (*pluginproto.ChannelMessageBatchResp, error) {
      ...
      for _, item := range req.GetChannelMessageReqs() {
          page, err := a.messageReader.SyncMessages(ctx, channelMessageQueryFromPluginReq(item))
  ```
- **触发路径**：
  1. 插件是**独立进程**，通过 Unix socket 上的 wkrpc 与主进程通信（`internal/FLOW.md` 组件表：`plugin.Runtime` = "节点内插件进程、Unix socket、热重载"；`plugin.Server` = "PDK host RPC：将插件进程 wkrpc 调用适配到插件用例"）。host RPC 就是这条信任边界。
  2. 系统**确实**建模了插件的作用域：`internal/usecase/plugin/binding.go:30-35` 的 `PluginBinding{PluginNo, UID}` 把插件绑定到特定 UID（注释："the user id whose offline Receive hook targets the plugin"），并配有 `BindingStore` / `BindingCache`（`internal/usecase/plugin/app.go:20-22`）。
  3. 但 `ChannelMessages` 的第三个参数 —— 调用方插件的 `no` —— 被签名里的 `_ string` 直接丢弃，随后用一个**无作用域**的 reader 去读请求里指定的**任意** channel，全程不查 binding。
  4. 因此任一已加载的插件，在其 host RPC 会话里构造 `ChannelMessageBatchReq` 指定任意 `channelID`/`channelType`，即可读到该 channel 的历史消息 —— 包括与它毫无绑定关系的用户的 p2p 会话内容。`managerMessageReader` 还带 `remote: a.nodeClient`，意味着目标 channel 的 leader 在**其它节点**时也会被代理读取，作用域是**整个集群**而非本节点。
- **后果**：插件的实际权限等同于通过 JWT 认证、持有相应权限位的后台管理员，远超 binding 模型所描述的范围。一个只应服务于若干绑定 UID 的插件（哪怕只是被投毒或存在漏洞），可以把集群内任意频道的消息内容读出并外传。定 P2 而非更高：插件由运维安装、不是匿名外部输入，且 authz 缺失的**代码本体**位于 `internal/usecase/plugin/host_rpc.go`（unit 11 的范围）；我这里认定的是**组合根的授权决策** —— 在 binding 模型已经存在的前提下，`internal/app` 仍选择把 manager 级的完整权限对象未经任何收窄地注入插件子系统。
- **建议**：`pluginMessageReader` 不应直接返回 `managerMessageReader`，而应返回一个按调用方插件 `no` 收窄的装饰器（依据 `BindingStore` 解析该插件可访问的 UID/channel 集合后过滤）；同时把 `host_rpc.go:38` 的 `_ string` 改回具名参数并参与鉴权。更根本地，manager 平面与插件平面不应共用同一个权限对象类型。

### [P2] 9. 单个 UID 的 slot 处于 leader 选举中，会使**整条** CMD conversation intent（含所有其它 UID）被丢弃，且 fallback 路径只记 Warn 不重试

- **位置**：`internal/app/cmdsync_intent.go:136-151`（全量中止）、`:50-54`（早退）、`:88-101`（丢弃结果）
- **类别**：正确性 / 错误处理
- **代码**：

  owner 分组遇到**任一** UID 解析失败就放弃整张表：
  ```go
  // internal/app/cmdsync_intent.go:136-151
  func (r cmdConversationIntentRouter) groupReadSeqsByOwner(readSeqs map[string]uint64) (map[uint64]map[string]uint64, error) {
      groups := make(map[uint64]map[string]uint64)
      for uid, readSeq := range readSeqs {
          ownerNodeID, err := r.ownerNodeID(uid)
          if err != nil {
              return nil, err
          }
          ...
  ```
  ```go
  // internal/app/cmdsync_intent.go:153-165
  func (r cmdConversationIntentRouter) ownerNodeID(uid string) (uint64, error) {
      ...
      leaderID, err := r.cluster.LeaderOf(slotID)
      if err != nil {
          return 0, err
      }
      if leaderID == 0 {
          return 0, raftcluster.ErrNoLeader
      }
  ```
  `PushIntent` 在分组阶段就早退，**一个 group 都没推**：
  ```go
  // internal/app/cmdsync_intent.go:50-54
  groups, err := r.groupCMDConversationIntentByOwner(normalized)
  if err != nil {
      return false, err
  }
  ```
  fallback 观察者丢弃 `fullyAccepted` 返回值，只打一条 Warn：
  ```go
  // internal/app/cmdsync_intent.go:96-101
  if _, err := o.sink.PushIntent(ctx, intent); err != nil && o.logger != nil {
      o.logger.Warn("cmd conversation intent route failed",
          wklog.Event("cmdsync.intent.route.failed"),
          wklog.Error(err),
      )
  }
  ```
- **触发路径**：
  1. 一条消息投递到有 N 个收件人的群频道，`BuildConversationIntent`（:92）为这 N 个 UID 生成一张 `UserReadSeqs` 表。
  2. 这 N 个 UID 按 `SlotForKey(uid)` 散落在多个 slot 上。其中**一个** slot 恰好处于 leader 选举窗口 → `LeaderOf` 返回错误，或返回 `leaderID == 0` → `ownerNodeID` 返回 `raftcluster.ErrNoLeader`。
  3. `groupReadSeqsByOwner` 在遍历到该 UID 时 `return nil, err`，**丢弃已经分好组的其它 owner**；`PushIntent` 在 `:52` 早退，返回 `(false, err)` —— 此时**没有任何一个 group 被推送**，包括那些 owner 完全健康、本可以立即成功的 UID。注意 map 遍历顺序随机，所以"已分好多少组"每次都不同，但结果一律是全部丢弃。
  4. 上游 `OnResolvedUIDPage`（:88）是 `deliveryrouting.go:1416` 的 **fire-and-forget** 调用（函数无返回值），它忽略 `fullyAccepted` 这个 bool，只把 error 写进 Warn 日志。没有任何重试、补偿或降级队列。
  5. 这条路径本身就是 fallback：`:89` 的 `page.Envelope.CMDConversationIntentSubmitted` 为真时直接跳过 —— 说明主路径（`internal/usecase/message/send.go:406` 用 `intentSubmitted` 设置该标志）已经处理过的才跳过。也就是说，**只有主路径没能完成提交时才会走到这里**，而这个"最后一道补救"恰恰是全丢且不重试的。
- **后果**：这 N 个用户的 CMD conversation 读进度（`UserReadSeqs`）全部不落地。用户侧表现为会话未读计数与已读位置停留在旧值，且**不会自愈** —— 只有该频道产生下一条消息并成功走完 intent 路由时才会被覆盖修正。选举窗口通常是秒级，但窗口内该频道的每条消息都会命中同一问题，且影响面被放大到全部收件人而非受影响的那一个。不涉及消息本体丢失，故定 P2。
- **建议**：`groupReadSeqsByOwner` 改为"部分成功"语义 —— 把无法解析 owner 的 UID 收集到一个 failed 集合，其余 group 照常推送，再把 failed 部分连同 `fullyAccepted=false` 返回；`OnResolvedUIDPage` 应当消费这个 bool（写回 `CMDConversationIntentSubmitted` 或投入重试队列），而不是丢弃。现有的 `retryStaleCMDConversationIntentGroup`（:115-129）已经示范了按 group 重路由的模式，`ErrNoLeader` 也应纳入同样的重试。

### [P3] 10. leader 修复策略被劈成两半：判定在组合根，反应在 runtime，靠"字符串 reason"耦合，且 runtime 侧的 `default` 分支是不安全的兜底

- **位置**：`internal/app/channelmeta_liveness.go:27-53`（判定）；注入点 `internal/app/build.go:471,493`；消费方 `internal/runtime/channelmeta/repair.go:238-245, 265-272`
- **类别**：架构（分层违规）
- **代码**：

  判定逻辑（业务规则）住在 `internal/app`：
  ```go
  // internal/app/channelmeta_liveness.go:27-53
  func (s *channelMetaSync) needsLeaderRepair(meta metadb.ChannelRuntimeMeta) (bool, string) {
      if meta.Status != uint8(channel.StatusActive) {
          return false, ""
      }
      if meta.Leader == 0 {
          return true, channel.LeaderRepairReasonLeaderMissing.String()
      }
      if !containsUint64(meta.Replicas, meta.Leader) {
          return true, channel.LeaderRepairReasonLeaderNotReplica.String()
      }
      status, ok := s.nodeLivenessStatus(meta.Leader)
      if ok {
          switch status {
          case controllermeta.NodeStatusDead:
              return true, channel.LeaderRepairReasonLeaderDead.String()
          case controllermeta.NodeStatusDraining:
              return true, channel.LeaderRepairReasonLeaderDraining.String()
          }
      }
      ...
  ```
  它被当作策略函数注入 runtime：
  ```go
  // internal/app/build.go:471
  RepairPolicy: app.channelMetaSync.needsLeaderRepair,
  ```
  runtime 侧再对同一批 reason **字符串**做分支，`default` 表示"不排除当前 leader / 不裁剪 ISR"：
  ```go
  // internal/runtime/channelmeta/repair.go:238-245
  func (r *LeaderRepairer) shouldSkipRepairCandidate(meta metadb.ChannelRuntimeMeta, reason string, replicaID uint64) bool {
      switch reason {
      case channel.LeaderRepairReasonLeaderDead.String(),
          channel.LeaderRepairReasonLeaderDraining.String():
          return replicaID == meta.Leader
      default:
          return false
      }
  }
  ```
  ```go
  // internal/runtime/channelmeta/repair.go:265-272
  func evaluationMetaForRepair(meta metadb.ChannelRuntimeMeta, reason string) metadb.ChannelRuntimeMeta {
      switch reason {
      case channel.LeaderRepairReasonLeaderDead.String(),
          channel.LeaderRepairReasonLeaderDraining.String():
      default:
          return meta
      }
  ```
- **触发路径（代码演进型，非运行时）**：**我核实过当前所有 reason 都被正确处理，现存代码没有可触发的错误行为**，所以这是一条结构性/可维护性发现，不是运行时缺陷。风险在于：新增一个"当前 leader 不可信"类的 repair reason 时，作者只需修改 `internal/app/channelmeta_liveness.go`（判定侧）即可让它生效并流入 runtime；而 `repair.go:238` 与 `:265` 两处 `switch` 不会报错、不会编译失败，新 reason 静默落入 `default` —— 含义变成"**不要**把失效的 leader 从候选里排除"、"**不要**把它从 ISR 里裁掉"。于是修复流程会把刚被判定为不可信的那个节点重新选为 leader。两个 `switch` 分散在不同函数，漏改一个就够了。
- **后果**：策略的"判定"与"反应"跨越 `app` / `runtime` 两层，仅靠 `pkg/channel` 里的字符串常量做隐式契约，没有任何编译期或测试期的完备性保证。违反 `AGENTS.md:182-184`（业务编排应在 `usecase`，`internal/app/*` 只放依赖装配）。当前无实际错误行为，定 P3。
- **对比**：同一分片内的绝大多数文件是**正面例子** —— `manager_channel_cluster_operations.go`、`management_connections.go`、`channelplane.go`、`monitor_metrics.go`、`channelmeta_statechange.go` 都是纯粹的端口适配与 DTO 转换，不含任何决策逻辑。问题集中在 `manager_message_retention.go`（见发现 3）与本条。
- **建议**：把 `needsLeaderRepair` 下沉到 `internal/runtime/channelmeta`（它需要的 `nodeLivenessStatus` / `LocalRuntimeLeaderRepairReason` 本来就在那里，`resolver.go:368-378` 已经提供），让判定与反应同层；同时把 reason 从字符串换成带穷尽性检查的枚举类型，使新增 reason 时两处 `switch` 必须显式处理。

### [P2] 11. 消息 append 热路径上，循环不变量 `KeyFromChannelID` 被逐条消息重复构造（每次一次 `make` + base64 编码），同文件的转发路径却做对了

- **位置**：`internal/app/channelcluster.go:195-222`（错误写法）对照 `:416-441`（同文件正确写法）；被调函数 `pkg/channel/handler/key.go:15-23`；无条件求值的原因 `pkg/observability/sendtrace/sendtrace.go:134-143`
- **类别**：性能（热路径分配）
- **代码**：

  `req.ChannelID` 在整个循环里恒定，但 key 在**每次迭代**都重新构造：
  ```go
  // internal/app/channelcluster.go:203-221
  for i, item := range result.Items {
      if item.Err != nil {
          continue
      }
      var msg channel.Message
      if i < len(req.Messages) {
          msg = req.Messages[i]
      }
      sendtrace.Record(sendtrace.Event{
          TraceID:     req.TraceID,
          Stage:       sendtrace.StageChannelAppendLocal,
          At:          start,
          Duration:    sendtrace.Elapsed(start, time.Now()),
          NodeID:      c.localNodeID,
          ChannelKey:  string(channelhandler.KeyFromChannelID(req.ChannelID)),
          ...
  ```
  每次调用都分配一个新切片并做 base64 编码：
  ```go
  // pkg/channel/handler/key.go:16-23
  encodedIDLen := base64.RawURLEncoding.EncodedLen(len(id.ID))
  bufLen := len(keyPrefix) + decimalUint8Len(id.Type) + 1 + encodedIDLen
  buf := make([]byte, bufLen)
  pos := 0
  pos += copy(buf[pos:], keyPrefix)
  pos += appendUint8ToFixed(buf[pos:], id.Type)
  buf[pos] = '/'
  ```
  同一文件里的转发路径把它正确地提到了循环外（用一次性取到的 `meta.Key`）：
  ```go
  // internal/app/channelcluster.go:433（循环内，但 meta 在 :364 已取好一次）
  ChannelKey:  string(meta.Key),
  ```
- **触发路径**：
  1. 每条客户端消息发送都会走到 `AppendBatch`（:170）→ `appendLocalBatch`（:195）。批量发送或群聊 fan-in 场景下 `result.Items` 长度为 N。
  2. 循环对每个成功 item 构造一个 `sendtrace.Event` 字面量。`req.ChannelID` 在循环内**从不改变**，所以 `KeyFromChannelID(req.ChannelID)` 的返回值恒等，却被算了 N 次 —— N-1 次是纯浪费，每次含一次堆分配（`make([]byte, bufLen)`）与一次 base64 编码。
  3. **关键：这笔开销无法被"tracing 未开启"省掉。** `sendtrace.Record` 是普通函数、按值接收 `Event`：
     ```go
     // pkg/observability/sendtrace/sendtrace.go:134-138
     func Record(event Event) {
         holder := activeSink.Load()
         if holder == nil || holder.sink == nil {
             return
         }
     ```
     Go 在调用前必须**完整求值实参**，所以 `KeyFromChannelID(...)` 和 `Elapsed(start, time.Now())` 在 sink 判空**之前**就已经执行，且整个 `Event` 结构体（14 个字段）按值拷贝一次。
  4. 而且 sink 默认就是装上的：`internal/app/build.go:103-112` 在 `cfg.Observability.Diagnostics.Enabled` 时 `sendtrace.SetSink(sink)`，该配置项在 `internal/app/config.go:1361-1363` 默认为 `true`。因此完整的 `RecordSendTrace` → 采样器路径默认也在跑。
- **后果**：每条消息在 append 路径上多付 (N-1) 次小对象分配与 base64 编码（N 为批次内消息数）。单次成本很小，但位置在每条消息必经的写路径上，直接转化为 GC 压力与 append 延迟。这里**不涉及** `fmt.Sprintf`，成本是 `make`+base64+结构体拷贝，不要按字符串格式化来估算。
- **建议**：把 `channelhandler.KeyFromChannelID(req.ChannelID)` 提到 `for` 循环之前算一次并复用（与 `:433` 使用 `meta.Key` 的写法对齐）。若要进一步省掉 tracing 关闭时的开销，可在循环外先判一次 sink 是否存在（`sendtrace` 需暴露一个廉价的 `Enabled()`），或把 `Record` 改成接收指针/惰性构造。

### [P3] 12. gateway 指标观察者按**收件人**（而非按消息）触发，每个出站帧产生 2 次 `WithLabelValues` 查找；标签基数只有个位数，完全可以预解析缓存

- **位置**：`internal/app/observability.go:122-145`（调用点）；频率来源 `pkg/gateway/core/server.go:709,722,1476-1488`；成本 `pkg/metrics/gateway.go:129-150`
- **类别**：性能（热路径）
- **代码**：
  ```go
  // internal/app/observability.go:129-145
  func (o gatewayMetricsObserver) OnFrameOut(event accessgateway.FrameEvent) {
      if o.metrics == nil || event.FrameType != "RECV" {
          return
      }
      o.metrics.Gateway.MessageDelivered(event.Protocol, event.Bytes)
  }

  func (o gatewayMetricsObserver) OnFrameHandled(event accessgateway.FrameHandleEvent) {
      if o.metrics == nil {
          return
      }
      o.metrics.Gateway.FrameHandled(event.FrameType, event.Duration)
  }
  ```
  每次调用做**两次**标签查找：
  ```go
  // pkg/metrics/gateway.go:137-143
  func (m *GatewayMetrics) MessageDelivered(protocol string, bytes int) {
      if m == nil { return }
      m.messagesDeliveredTotal.WithLabelValues(protocol).Inc()
      m.messagesDeliveredBytes.WithLabelValues(protocol).Add(float64(bytes))
  }
  ```
- **实测频率（brief 要求澄清的问题）**：
  - `OnFrameIn` → `MessageReceived`：由 `observeFrameIn`（`pkg/gateway/core/server.go:1465`）触发，**每条客户端入站消息一次**（`:126` 已用 `event.FrameType != "SEND"` 过滤）。
  - `OnFrameOut` → `MessageDelivered`：由 `observeFrameOut` 触发，而它的调用点是 `pkg/gateway/core/server.go:709` 与 `:722` —— **每次向单个连接写帧一次**。因此对一条投递给 N 个收件人（设备）的消息，它被调用 **N 次，不是 1 次**。这就是 fan-out 放大所在。
  - `OnFrameHandled` → `FrameHandled`：每个被处理的帧一次（入站出站都算），1 次标签查找 + 1 次 histogram `Observe`。
- **诚实的成本估算**：`pkg/metrics` 里**没有** `fmt.Sprintf`，我已确认（`gateway.go:129-150` 只有 `WithLabelValues` + `Inc`/`Add`/`Observe`），所以这**不是**字符串构造问题，不要按那个量级估。真实成本是：每个出站 RECV 帧 2 次 `WithLabelValues`（各含一次标签值哈希 + metric map 读锁）+ 2 次原子加。一条 1000 收件人的群消息 ≈ 2000 次查找，按每次 60–80ns 计约 **150µs**。标签基数极小（`protocol` 只有寥寥几个取值，`frameType` 是固定枚举），所以这些查找**每次都命中同一批对象**，纯属重复劳动。另外 `:130` 的 `event.FrameType != "RECV"` 也是每个出站帧一次字符串比较，而上游 `server.go:1484` 为构造事件已经调了一次 `f.GetFrameType().String()`。
- **后果**：高 fan-out 场景（大群广播）下，投递路径上每个收件人多付两次 map 查找与锁操作。绝对值不大，属于可回收的常数开销而非瓶颈，故定 P3 —— 我没有观测到它造成实际瓶颈，不把它拔高。
- **边界说明**：`pkg/metrics` 的内部实现归 unit 32；本条只认定**调用点频率**这一侧。
- **建议**：在 `gatewayMetricsObserver` 构造时（或 `GatewayMetrics` 内部）按 `protocol` / `frameType` 预解析出具体的 `prometheus.Counter` / `Observer` 并缓存（取值集合是有限枚举，完全可枚举），把热路径上的 `WithLabelValues` 降为一次指针解引用；`FrameType` 比较也可改为枚举值比较而非字符串比较。

### [P2] 13. 每一次跨节点 send / RPC 都为 nodeID 标签做一次 `strconv.FormatUint` 堆分配，而节点 ID 集合是稳定的小集合

- **位置**：`internal/app/observability.go:206-217, 218-229`；合并点 `internal/app/build.go:188-192`；触发频率 `pkg/transport/pool.go:86,90,102,108`
- **类别**：性能（热路径分配）
- **代码**：
  ```go
  // internal/app/observability.go:212-217
  OnEnqueue: func(event transport.EnqueueEvent) {
      if o.metrics == nil {
          return
      }
      o.metrics.Transport.ObserveEnqueue(strconv.FormatUint(uint64(event.TargetNode), 10), event.Kind, event.Result)
  },
  ```
  ```go
  // internal/app/observability.go:218-229
  OnRPCClient: func(event transport.RPCClientEvent) {
      ...
      targetNode := strconv.FormatUint(uint64(event.TargetNode), 10)
      service := transportRPCServiceName(event.ServiceID)
      o.metrics.Transport.SetRPCInflight(targetNode, service, event.Inflight)
  ```
- **触发路径**：
  1. `OnEnqueue` 由 `Pool.observeEnqueue` 触发，而后者在 `pkg/transport/pool.go:86,90`（`SendWithContext` —— **每一条**跨节点消息）与 `:102,108`（`RPC` —— 每一次跨节点 RPC）被同步调用。这是 channel log 复制、slot raft 消息、data-plane 投递的公共路径。
  2. 每次调用执行 `strconv.FormatUint(uint64(event.TargetNode), 10)`。对于 nodeID ≥ 10 的情况，`strconv` 无法命中其小整数字符串缓存，会**分配一个新 string**。`OnRPCClient` 同理，且 `transportRPCServiceName`（:233-287）在 `default` 分支还有一次 `"service_" + strconv.FormatUint(...)` 拼接（已知 serviceID 走 switch 返回常量，不分配；未知 serviceID 才分配）。
  3. 集群节点 ID 是**稳定的小集合**（`cfg.Cluster.Nodes`），同一个 nodeID 的字符串在进程生命周期内完全相同，却被逐条消息重新构造。
  4. 而且这条路径上还叠了第二个观察者：`internal/app/build.go:188-192` 用 `mergeTransportObserverHooks` 把 `transportMetricsObserver` 与 `networkObservability.TransportHooks()` 串联，两者**都**在每次事件时执行。所以每条跨节点消息 = 1 次 `FormatUint` 分配（metrics 侧）+ 1 次 `o.mu.Lock()`（network 侧，见发现 4）。
- **后果**：跨节点数据面上每条消息一次小对象分配，直接累加到 GC 扫描压力上；在高复制吞吐下这是持续的垃圾产生源。绝对值单次很小，但频率等于集群内部消息总量，且完全可以消除。
- **建议**：在构造 `transportMetricsObserver` 时预先把已知 nodeID 渲染成字符串存进一个 `map[transport.NodeID]string`（或直接用 `[N]string` 数组索引），热路径只做一次查表；未知 nodeID 再走慢路径。`transportRPCServiceName` 已经用 switch 返回常量字符串，是正确示范，`targetNode` 应比照处理。

### [P1] 14. 无鉴权的 `/healthz/details` 每次请求向**集群 controller leader** 发起 4 次带重试的 RPC，绕过了同文件已有的 2 秒缓存

- **位置**：`internal/app/observability.go:386`（未走缓存的调用）、`:633-635`、`:637-683`（4 次查询 + 5 次排序）；对照 `:691` + `:699-708`（走缓存的调用）
- **类别**：性能 / 安全（跨节点资源放大）
- **代码**：

  健康详情走的是**未缓存**版本：
  ```go
  // internal/app/observability.go:386（healthDetailsSnapshot 内）
  clusterState := a.collectObservedClusterState()
  ```
  ```go
  // internal/app/observability.go:633-635
  func (a *App) collectObservedClusterState() observedClusterState {
      return a.collectObservedClusterStateWithTimeout(context.Background(), observabilityQueryTimeout)
  }
  ```
  一次调用 = 4 次 controller 查询 + 1 次迁移状态 + 5 次排序：
  ```go
  // internal/app/observability.go:645-664
  nodesCtx, cancel := context.WithTimeout(parent, timeout)
  state.nodes, state.nodesErr = a.cluster.ListNodes(nodesCtx)
  cancel()

  assignmentsCtx, cancel := context.WithTimeout(parent, timeout)
  state.assignments, state.assignmentsErr = a.cluster.ListSlotAssignments(assignmentsCtx)
  cancel()

  viewsCtx, cancel := context.WithTimeout(parent, timeout)
  state.views, state.viewsErr = a.cluster.ListObservedRuntimeViews(viewsCtx)
  cancel()

  tasksCtx, cancel := context.WithTimeout(parent, timeout)
  state.tasks, state.tasksErr = a.cluster.ListTasks(tasksCtx)
  cancel()
  ```
  而指标路径**有**缓存，说明缓存机制是现成的：
  ```go
  // internal/app/observability.go:691（refreshControllerMetrics 内）
  clusterState := a.cachedObservedClusterState()
  // :699-708 —— 2 秒 TTL + CompareAndSwap 单飞
  state, stale := a.observedClusterCache.snapshot(controllerMetricsRefreshInterval)
  ```
- **触发路径**：
  1. `/healthz/details` 默认注册且**无任何认证**（见发现 1：`internal/access/api/routes.go:10-12` + `server.go:190-191` 只有 CORS，`HealthDetailEnabled` 默认 true）。
  2. 每个请求 → `healthDetailsSnapshot()`（:364）→ `:386` 的 `collectObservedClusterState()`。注意它调用的是**未缓存**版本，而不是 `:699` 的 `cachedObservedClusterState()` —— 同一个文件里两种写法并存，健康检查这条选错了。
  3. `ListNodes` 并非本地读：`pkg/cluster/cluster.go:1474-1486` 走 `c.controllerClient` 并包在 `retryControllerCommand` 里，即**向 controller leader 发起远程 RPC，失败还会重试**。其余三个查询同构。
  4. 于是：1 个匿名 HTTP 请求 → 4 次跨节点 controller RPC（每次带重试）。攻击者从集群外循环请求 `/healthz/details`，即可把**整个集群唯一的 controller leader** 打满 —— 放大倍数是 4×（再乘重试次数），且负载落在集群的单点上，而非发起请求的那个节点。
  5. 与发现 2 叠加：同一个请求在 `:420` 还会触发 `storageHealthSnapshot()` → 5 次递归 `filepath.Walk`。所以一个匿名请求同时打**本地磁盘**和**远端 controller**。
  6. `/readyz`（`readyzReport`，:490）同样无鉴权，且在 :506-509 调 `WaitForManagedSlotsReady`、在 `refreshLocalNodeDrainState`（:545-563）调 `ListNodes` —— 放大倍数较小但性质相同。
- **后果**：controller leader 是集群元数据的单点，被打满会拖慢 slot 分配、迁移编排、节点心跳处理等全局控制面操作，影响范围是**整个集群**而不只是被请求的节点。而且因为 `ListNodes` 内部带重试，controller 已经变慢时重试会进一步放大压力，形成正反馈。健康检查端点本应是最廉价的探针，这里反而是最贵的。
- **建议**：把 `:386` 改为调用 `a.cachedObservedClusterState()`（缓存与单飞机制在 `:699-729` 已经写好，直接复用即可）；健康探针本就不需要秒级新鲜的集群视图。同时按发现 1 的建议把 `/healthz/details` 移出无鉴权平面，或默认关闭。


## 已排除的候选项

- **`from_uid` 设计规则：代码遵守了自己的规则（已逐路径核实，非发现）**。brief 要求核对 `docs/superpowers/plans/2026-05-14-diagnostics-dynamic-tracking.md:20` 的 "Do not expose `from_uid` in manager/web event DTOs"。核实结论：**遵守**。(1) `internal/observability/diagnostics/store.go` 上唯一的导出读取方法是 `Query`（`:113`），其唯一向结果追加事件的语句是 `:148` 的 `out = append(out, redactEvent(event))`，而 `redactEvent`（`event.go:175-178`）就是 `event.FromUID = ""`；其余所有分支都返回不含事件的 `notFoundResult`。因此**不存在**绕过脱敏的读取路径。(2) 写入侧保留 `FromUID`（`sendtrace_adapter.go:50`、`index.go:50-51`、`sampler.go:126`、`store.go:211`、`tracking.go:251`）属于规则明确允许的 "allowed only as an internal match/query key"。(3) 我这一分片的 `internal/app/diagnostics.go:30` 与 `:81-90` 只是把 `Query` 的结果透传（本地）或经 node RPC 取回（远端，远端同样在序列化前脱敏），没有自己重建事件 DTO，因此不引入泄露。(4) `internal/access/manager/messages.go:42-43` 的 `FromUID string \`json:"from_uid"\`` 是**消息** DTO 而非**事件** DTO，消息的发送者本就是消息的组成部分，不在该规则约束范围内。
- `internal/app/plugin_receive_observer.go:107` — 一度怀疑 goroutine 泄漏。实际生命周期完整：`Start`（:78-110）用 `o.wg.Add(concurrency)` 登记全部 worker，`:107-110` 的看护 goroutine 在 `wg.Wait()` 后 `close(done)`；`Stop`（:112-135）先 `cancel()` 再 `select` 等 `<-done`（带调用方 ctx 超时上界），返回前 `drainQueuedWork()`。生产者 `OfflineResolved`（:137-153）用 `select { case queue <- ...; case <-done: }`，关闭后不会永久阻塞。排除。
- `internal/app/channelcluster.go:110-134`（`lockApplyMeta`）— 一度怀疑按 channel key 的 per-key 锁 map 无界增长。实际是标准引用计数实现：`lock.refs++` 在 `applyMu` 下自增，解锁闭包中 `lock.refs--` 且 `refs == 0` 时 `delete(c.applyLocks, key)`。无泄漏，排除。
- `internal/app/manager_messages.go:72`、`internal/app/manager_message_retention.go:59,293` — gosec G115 `int64 -> uint8`（`uint8(meta.ChannelType)` / `uint8(req.ChannelType)`）。`manager_messages.go:72` 与 `manager_message_retention.go:293` 的输入来自数据库中已存在的 `ChannelRuntimeMeta` / 内部响应结构，不是远程输入。`manager_message_retention.go:59` 的 `req.ChannelType` 虽来自 manager HTTP 请求，但该请求需 JWT 认证与写权限；且 `:60` 的 `GetChannelRuntimeMeta(ctx, req.ChannelID, req.ChannelType)` 用的是**未截断**的 int64，channelType > 255 时查不到对应 meta 而提前返回错误。综合判定为误报（但下方"整体评价"中提到，截断值与未截断值在同一函数内混用是一个值得清理的隐患）。

- `internal/app/benchdata_adapter.go:1-56` — **不是发现（闸门是对的）**。回答 brief 的问题：该适配器（`benchUserWriter.UpdateToken` / `benchChannelWriter.UpsertChannel` / `AddSubscribers`）本身是纯 DTO 转发，不加任何自己的闸门；但它**双重**受同一个 `cfg.Bench.APIEnabled` 保护：(1) `internal/app/build.go:917-924` 里 `if cfg.Bench.APIEnabled` 为假时 `benchData` 保持 nil，适配器根本不被构造；(2) `internal/access/api/routes.go:29-31` 的 `if s.benchEnabled { s.registerBenchRoutes() }` 使 `/bench/v1/*` 路由不注册。`Bench.APIEnabled` 在 `internal/app/config.go` 中**没有任何默认值设置逻辑**，因此是 bool 零值 `false`。这正是 `wukongim.conf.example:215-216` 警告的那个 `WK_BENCH_API_ENABLE=false`（"Keep false in production and shared environments because these routes intentionally skip auth"）。所以 unit 10 发现的"benchdata usecase 无 UID 命名空间限制、可覆盖真实用户 login token"在默认生产配置下**不可达**；它是一个"一旦误开则后果严重"的配置风险，而不是 app 装配侧的缺陷。
- `internal/app/channelmigration.go:164` — gosec G115：`SlotForKey` 返回 `multiraft.SlotID`（uint64），实际值恒 < SlotCount，转 uint32 无溢出，误报。
- `internal/app/observability.go:782` — gosec G115 `uint64 -> int64`（`int64(diskFreeBytes)`）。`diskFreeBytes` 来自 `filesystemFreeBytes`（:965-973）的 `stat.Bavail * uint64(stat.Bsize)`，是真实磁盘剩余字节，不可能接近 2^63，且不由远程输入驱动。误报。
- `internal/app/observability.go:566-624`（`debugConfigSnapshot` / `debugClusterSnapshot`）— 一度怀疑是无鉴权泄露（这两个函数暴露 `node_name`、集群节点地址等更敏感的内容），但 `internal/access/api/routes.go:32-40` 由 `s.debugEnabled` 把守，其来源 `cfg.Observability.HealthDebugEnabled` 在 `internal/app/config.go:1358-1360` 被**显式**置为 `false`（`if !c.Observability.healthDebugEnabledSet { c.Observability.HealthDebugEnabled = false }`）。默认不注册，排除。
- `internal/app/diagnostics.go:11-160` — 一度怀疑公网 API 上的诊断路由泄露 `from_uid` / 消息内容。实际：(1) `internal/access/api/routes.go:26-28` 的注册条件是 `s.diagnosticsDebugEnabled`，其来源 `internal/app/build.go:949` 为 `cfg.Observability.Diagnostics.Enabled && cfg.Observability.Diagnostics.DebugAPIEnabled`，而 `DebugAPIEnabled` 在 `config.go:1382-1384` 默认 `false` —— 虽然 `Diagnostics.Enabled` 默认 true，**合取式为假**，公网诊断路由默认不注册；(2) 其余诊断入口（`managementDiagnosticsReader` / `managementDiagnosticsTrackingReader`）全部只经 manager 平面，走权限中间件。因此 `docs/superpowers/plans/2026-05-14-diagnostics-dynamic-tracking.md:20` 的 "Do not expose `from_uid` in manager/web event DTOs" 规则在本分片文件中既未被违反、相关 DTO 也未默认暴露。排除。

## 本分片整体评价

**这是 v1 生产代码**（`cmd/wukongim/main.go` → `internal/app`），不是未上线的 v2 栈，严重度按实际算。

本分片 21 个文件、约 5.4k 行，绝大多数是**质量相当高的薄适配器** —— `manager_channel_cluster_operations.go`、`management_connections.go`、`channelplane.go`、`monitor_metrics.go`、`channelmeta_statechange.go`、`manager_messages.go`、`plugin.go`、`benchdata_adapter.go` 都严格遵守"只做端口装配与 DTO 转换"的职责，错误一律上抛；`channelretention.go` 是把业务规则正确下沉到 `internal/runtime/*` 的教科书示例；`plugin_receive_observer.go` 的 goroutine 生命周期（WaitGroup + done + 有界 Stop）和 `channelcluster.go:110-134` 的引用计数 per-key 锁都写得干净无泄漏；`network_observability.go` 里 traffic 维度的 16 分片设计也说明作者清楚热路径锁的代价。缺陷集中在少数几处，而非弥散的低质量。

**最需要优先处理的一个问题**：无鉴权公网 API 上的 `/healthz/details` 与 `/metrics`。这一处同时踩中三条（发现 1、2、14）—— 它默认开启、没有任何认证中间件（engine 上只有 CORS）、泄露完整集群拓扑与 slot leader 归属，并且每个匿名请求会同步触发 5 次 Pebble 数据目录递归 walk **加上** 4 次向集群 controller leader 的带重试 RPC。最讽刺的是修复成本极低：同文件 `:699-729` 已经写好了带 TTL 和单飞的缓存，controller 指标路径用了它，健康检查路径（`:386`）却调了未缓存版本 —— 一行之差。建议按"默认关闭 + 移到 manager 鉴权平面 + 复用现成缓存"三步走。

**次优先**：发现 6（迁移 cutover 证明的三个安全字段被组合根硬编码为零值）虽然触发条件较窄，但它是本分片里唯一可能导致**已提交消息丢失**的问题，且失效方式是"检查在类型上通过、在语义上失效"的静默放行 —— 这类缺陷不会产生任何告警，最难在生产中发现。值得注意的是目标副本**本来就持有** epoch history（`channelmeta.go:285-291`），缺的只是线路格式字段，属于可以彻底修好而非只能缓解的问题。

**一个贯穿性的架构观察**：本分片的问题几乎都出在"组合根越界承担业务决策"这一个模式上 —— 发现 3（retention 决策整套抄进 `internal/app`，与 `internal/runtime/channelretention` 形成 v1/v1 重复实现）、发现 6（证明字段填值决策）、发现 10（leader 修复判定与反应跨层劈开）、发现 8（授权范围决策）。`internal/FLOW.md` 把 `internal/app` 定义为"聚合所有子系统、构建依赖图、管理启停生命周期"，`AGENTS.md:184` 更明确写"依赖装配**只**放这里"；代码在这四处偏离了自己的文档。与 unit 05 发现的 `internal/access/manager` 直接 import `pkg/channel`/`pkg/cluster` 并做业务错误分类相互印证 —— manager 这个特性似乎是跨多层同时渗漏的，本分片是同一道缝的 `app` 侧。

**关于 `from_uid` 规则**：brief 要求核对的 `docs/superpowers/plans/2026-05-14-diagnostics-dynamic-tracking.md:20` 规则，我逐路径核实后确认**代码遵守了自己的规则** —— `diagnostics.Store` 唯一的导出读取方法 `Query` 在唯一的事件追加语句上调用了 `redactEvent`，不存在绕过路径；`internal/app/diagnostics.go` 只做透传，不重建 DTO。详见「已排除的候选项」。同样地，`benchdata_adapter.go` 的闸门是正确的（双重受默认为 false 的 `Bench.APIEnabled` 保护），unit 10 发现的 benchdata UID 越权在默认配置下不可达。
