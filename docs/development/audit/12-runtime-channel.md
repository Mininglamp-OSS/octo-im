# 节点内 channel 运行时：meta / plane / migration / retention

审计范围：`internal/runtime/channelmeta/`、`internal/runtime/channelplane/`、
`internal/runtime/channelmigration/`、`internal/runtime/channelretention/`（6970 行非测试代码）。
全部为 **v1 生产运行路径**（`internal/app` 组合根可达），严重度按实际算。

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/runtime/channelmeta/FLOW.md | - | 是 |
| internal/runtime/channelmeta/activate.go | 61 | 是 |
| internal/runtime/channelmeta/bootstrap.go | 230 | 是 |
| internal/runtime/channelmeta/cache.go | 468 | 是 |
| internal/runtime/channelmeta/interfaces.go | 204 | 是 |
| internal/runtime/channelmeta/leader_transfer.go | 151 | 是 |
| internal/runtime/channelmeta/liveness.go | 115 | 是 |
| internal/runtime/channelmeta/repair_types.go | 78 | 是 |
| internal/runtime/channelmeta/repair.go | 504 | 是 |
| internal/runtime/channelmeta/resolver.go | 1012 | 是 |
| internal/runtime/channelmeta/statechange.go | 173 | 是 |
| internal/runtime/channelmeta/types.go | 2 | 是 |
| internal/runtime/channelmigration/executor.go | 343 | 是 |
| internal/runtime/channelmigration/gc.go | 19 | 是 |
| internal/runtime/channelmigration/leader_transfer.go | 551 | 是 |
| internal/runtime/channelmigration/metrics.go | 57 | 是 |
| internal/runtime/channelmigration/proof.go | 105 | 是 |
| internal/runtime/channelmigration/replica_replace.go | 478 | 是 |
| internal/runtime/channelmigration/types.go | 182 | 是 |
| internal/runtime/channelplane/FLOW.md | - | 是 |
| internal/runtime/channelplane/channel_cell.go | 223 | 是 |
| internal/runtime/channelplane/command.go | 27 | 是 |
| internal/runtime/channelplane/effect_executor.go | 126 | 是 |
| internal/runtime/channelplane/errors.go | 20 | 是 |
| internal/runtime/channelplane/event.go | 37 | 是 |
| internal/runtime/channelplane/future.go | 39 | 是 |
| internal/runtime/channelplane/metrics.go | 13 | 是 |
| internal/runtime/channelplane/options.go | 137 | 是 |
| internal/runtime/channelplane/owner.go | 3 | 是 |
| internal/runtime/channelplane/peer_reactor.go | 535 | 是 |
| internal/runtime/channelplane/peer_rpc.go | 52 | 是 |
| internal/runtime/channelplane/plane.go | 153 | 是 |
| internal/runtime/channelplane/reactor.go | 248 | 是 |
| internal/runtime/channelplane/resolver.go | 95 | 是 |
| internal/runtime/channelplane/route.go | 55 | 是 |
| internal/runtime/channelplane/scheduler.go | 34 | 是 |
| internal/runtime/channelplane/tracing.go | 3 | 是 |
| internal/runtime/channelretention/worker.go | 437 | 是 |

## 发现

### [P1] 1. `ActivationCache.shards[i].generations` 是永不回收的无界 map —— 缓存的 cap/prune 逻辑漏掉了它

- **位置**：`internal/runtime/channelmeta/cache.go:58`（字段声明）、`cache.go:267-286`（唯一写入点）、
  `cache.go:396-407`（prune）、`cache.go:409-417`（cap）、`cache.go:452-454`（计数）
- **类别**：资源泄漏 / 无界增长
- **代码**：
  ```go
  // cache.go:54-60
  type activationCacheShard struct {
      mu          sync.Mutex
      positive    map[channel.ChannelKey]cachedChannelMeta
      negative    map[channel.ChannelKey]cachedChannelMetaError
      generations map[channel.ChannelKey]uint64
      lastPruneAt time.Time
  }

  // cache.go:282-285 —— 每次 Invalidate 都为该 key 建 / 递增一条 generations 记录
      if shard.generations == nil {
          shard.generations = make(map[channel.ChannelKey]uint64)
      }
      shard.generations[key]++
  ```
  而三条回收逻辑全部只覆盖 `positive` / `negative`：
  ```go
  // cache.go:396-407
  func pruneActivationCacheShardExpiredLocked(shard *activationCacheShard, now time.Time) {
      for key, entry := range shard.positive { ... delete(shard.positive, key) }
      for key, entry := range shard.negative { ... delete(shard.negative, key) }
  }
  // cache.go:452-454 —— cap 用的计数器同样不含 generations
  func activationCacheShardEntryCountLocked(shard *activationCacheShard) int {
      return len(shard.positive) + len(shard.negative)
  }
  ```
  `capActivationCacheShardLocked`（409-417）按 `activationCacheShardEntryCountLocked > 256` 触发
  `evictOldestActivationCacheEntryLocked`（419-450），后者也只 `delete(shard.negative/positive, ...)`。
  唯一会清空 `generations` 的是 `Clear()`（cache.go:248-264，`shard.generations = nil`）。

- **触发路径**（每一跳都已核实）：
  1. `Clear()` 在生产路径上只有一个调用点：`resolver.go:684` 的 `observeHashSlotTableVersion()`，
     且只在 **hash slot table 版本变化**（集群 slot 拓扑变更，运维级别的罕见事件）时才执行：
     ```go
     // resolver.go:676-686
     if s.lastHashSlotTableVersion != 0 && version != s.lastHashSlotTableVersion {
         s.resetAppliedLocalTrackingLocked()
         changed = true
     }
     ...
     if changed { s.cache.Clear() }
     ```
     稳态运行时它永远不触发。
  2. `Invalidate` 在业务热路径上被高频调用。最强的驱动是 channelplane 的 route 租约过期重解析：
     `internal/runtime/channelplane/channel_cell.go:58-62`
     ```go
     if c.cachedRouteExpired(*c.route) {
         c.reactor.plane.opts.Resolver.InvalidateRoute(cmd.req.ChannelID, c.route.RouteGeneration)
         c.route = nil
         c.startResolve(cmd)
         return
     }
     ```
     该 `Resolver` 在组合根里就是 `appChannelPlaneRouteResolver`
     （`internal/app/channelplane.go:26-30`），`InvalidateRoute` → `channelMetaSync.InvalidateChannelMeta(id)`
     （`internal/app/channelmeta.go:111-115`）→ `Sync.InvalidateChannelMeta`（`resolver.go:389-395`）
     → `ActivationCache.Invalidate(key)`。
  3. 时间窗口决定了这是**常态而非异常**：channel leader 租约 `BootstrapLease = 30 * time.Second`
     （`bootstrap.go:17`），而 channelplane 的 cell 空闲淘汰是 `defaultCellIdleTTL = 10 * time.Minute`
     （`channelplane/options.go:22`）。任何一个 channel 只要"发一条消息 → 静默 30 秒~10 分钟 → 再发一条"，
     cell 还在、route 已过期，就必然走到 `cachedRouteExpired` 分支并为该 key 写入一条 `generations` 记录。
     IM 场景下几乎所有单聊/群聊都是这个节奏。
  4. 另外三个 Invalidate 入口（`LoadPositive` cache.go:68-70 命中写栅栏、
     `cachedHealthyBusinessMeta` resolver.go:413-415、`invalidateIfWriteFenceChanged` cache.go:318-320
     在 ownership / RouteGeneration / 写栅栏变化时）在迁移与选主期间会再叠加。
  5. `channel.ChannelKey` 是 `string`（`pkg/channel/types.go:11`），条目大小随 channel ID 长度增长。
     结果：`generations` 的条目数 = **本进程生命周期内曾被 invalidate 过的不同 channel key 总数**，
     单调递增，永不回收；`positive`/`negative` 被严格限制在 4096 条，而 `generations` 没有任何上界。

- **后果**：长跑进程内存单调增长。按每条目 ~100 字节（key string + map bucket 开销）估算，
  100 万个活跃过的 channel ≈ 100 MB 永久驻留；且这部分内存永远不会随 channel 变冷而释放，
  只有 slot 拓扑变更或进程重启才会清掉。同时 `generations` 变大后，`Clear()` 与
  shard 遍历的开销也随之上升。

- **补充（跨分片模式）**：同一审计中另一分片在 `pkg/db/meta` 里独立确认了一个非常相似的缺陷
  ——一个没有 cap/TTL/LRU 的 `channelCache`，其唯一的 size 访问器还是 test-only。
  两处合起来说明这是一个**模式性问题**：本代码库里按 channel 维度建的缓存普遍缺少淘汰设计。
  本处更隐蔽，因为它**有**完整的 TTL + cap + LRU 淘汰逻辑，只是三处回收代码整齐地漏掉了第三个 map。

- **建议**：把 `generations` 纳入 `pruneActivationCacheShardExpiredLocked` /
  `activationCacheShardEntryCountLocked` / `evictOldestActivationCacheEntryLocked` 的覆盖范围；
  或者改用带时间戳的 generation 条目、在 key 的 positive/negative 条目都被淘汰且超过一个安全窗口后
  一并删除（注意不能直接删——删掉会让 in-flight 的 `storePositiveEntryAtGeneration` 误判为同代而写入陈旧值，
  需要配合 `globalGeneration` 或最小保留窗口）。

### [P1] 2. `SlotRefreshScheduler.Schedule` 在去重闸门之前先做一次 O(n) 全量扫描，且扫描要克隆整张 `appliedLocal` map（持 `s.mu`）

- **位置**：`internal/runtime/channelmeta/statechange.go:23-48`、
  `internal/runtime/channelmeta/resolver.go:459-473`（`ScheduleSlotLeaderRefresh`，持 `scheduleMu`）、
  `resolver.go:817-820`（`snapshotAppliedLocal` 持 `s.mu` 全量克隆）、
  `resolver.go:945-961`（`snapshotAppliedLocalKeysForSlot`）
- **类别**：性能 / 锁竞争
- **代码**：
  ```go
  // statechange.go:23-48 —— 注意 27 行的 O(n) 探测在 38 行的去重闸门之前
  func (s *SlotRefreshScheduler) Schedule(ctx context.Context, wg *sync.WaitGroup, slotID multiraft.SlotID,
      keys func(multiraft.SlotID) []channel.ChannelKey, refresh func(context.Context, channel.ChannelKey), timeout time.Duration) {
      if s == nil || ctx == nil || wg == nil || keys == nil || refresh == nil { return }
      if len(keys(slotID)) == 0 {          // ← 每次调用都完整跑一遍 keys()
          return
      }
      ...
      s.mu.Lock()
      if _, ok := s.pendingSlots[slotID]; ok {   // ← 去重在后面，已经白跑了一次全量扫描
          s.dirtySlots[slotID] = struct{}{}
          s.mu.Unlock()
          return
      }
  ```
  `keys` 就是 `snapshotAppliedLocalKeysForSlot`，它的第一步是：
  ```go
  // resolver.go:817-820
  func (s *Sync) snapshotAppliedLocal() map[channel.ChannelKey]struct{} {
      s.mu.Lock()
      defer s.mu.Unlock()
      return cloneAppliedLocalSet(s.appliedLocal)   // 持锁整表克隆
  }
  ```
  随后对克隆出的 **每一个** key 做 `channelhandler.ParseChannelKey(key)` 字符串解析 + `SlotForKey` 哈希
  （resolver.go:952-958）。goroutine 里 `for _, key := range keys(slotID)`（statechange.go:53）
  又会再跑一遍，所以一次真正生效的 Schedule 至少扫两遍。

- **触发路径**：
  1. `s.mu` 是 channel 元数据热路径锁：`ApplyAuthoritativeMeta`（resolver.go:292-294）在**每一次** channel
     激活 / 权威刷新且本节点是副本时都会 `s.mu.Lock(); s.trackAppliedLocalKeyLocked(key); s.mu.Unlock()`。
     `appliedLocal` 的规模 = 本节点持有本地 runtime 的 channel 数（IM 场景可达 10^5~10^6）。
  2. `ScheduleSlotLeaderRefresh` 有三个高频入口，全部会命中上面这条 O(n) 路径：
     - `internal/app/build.go:147-152` 的 `OnLeaderChange`：每次 slot leader 变更（集群抖动时成批发生）；
     - `resolver.go:721-740` `observeLocalReplicaStateChange` → `ObserveLocalReplicaStateChange`
       （statechange.go:136-146）：**每一条**检测到 leader drift 的本地副本状态变更通知，都会调用一次；
     - `resolver.go:963-990` `scheduleLeaderHealthRefresh`，由 `UpdateNodeLiveness` 同步调用
       （`internal/app/build.go:153-159` 的 `OnNodeStatusChange` 回调，跑在 cluster observer goroutine 上）。
  3. 最坏组合：某节点被判定 dead → `UpdateNodeLiveness` → `scheduleLeaderHealthRefresh`：
     先做 1 次全表克隆（持 `s.mu`）+ n 次 `runtime.Channel(key)` + n 次 `ParseChannelKey`
     （resolver.go:970-980），算出受影响的 slot 集合 S（该节点做 leader 的 channel 所在的所有 slot，
     均衡集群里接近本节点全部 slot），然后**同步地**对 S 中每个 slot 调用 `ScheduleSlotLeaderRefresh` ——
     每次都要 `scheduleMu.Lock()` 串行 + 一次持 `s.mu` 的全表克隆 + n 次 `ParseChannelKey`。
     总代价 O(|S| × n)，全部发生在 cluster observer 的回调栈上。
  4. 去重闸门形同虚设：即便 slot 已经 pending（只需要打个 dirty 标记），第 27 行仍然把 O(n) 扫描跑满。
     leader 批量切换时同一个 slot 会被反复 Schedule，每次都付全价。

- **后果**：节点状态抖动或 slot leader 批量切换时，`s.mu` 被长时间独占（n=10^6 时单次克隆就是百毫秒量级，
  ×|S| 后可达数秒），期间**所有** channel 激活 / 权威元数据刷新（`ApplyAuthoritativeMeta`）阻塞，
  表现为全节点消息投递延迟尖刺甚至超时。同时 cluster observer 的回调 goroutine 被占住，
  会拖慢后续的集群状态观测。

- **建议**：把去重/dirty 判断提到 `keys()` 调用之前；给 `Sync` 维护一张
  `slotID -> map[ChannelKey]struct{}` 的正向索引（`trackAppliedLocalKeyLocked` 已经在算 slotID 了，
  顺手维护即可），让按 slot 取 key 变成 O(该 slot 的 key 数) 而不是 O(全部本地 channel)；
  并让 `scheduleLeaderHealthRefresh` 走异步而不是挂在 observer 回调栈上。

### [P2] 3. 迁移执行器一次 tick 中任一任务的存储错误会中止整轮：其余任务与终态任务 GC 全部跳过

- **位置**：`internal/runtime/channelmigration/executor.go:89-104`
- **类别**：正确性 / 可用性（分布式运维流程）
- **代码**：
  ```go
  // executor.go:89-104
  for _, task := range tasks {
      taskNowMS := e.freshNowMS(nowMS)
      if !e.isRunnableTask(task, taskNowMS) { continue }
      if ok := e.confirmLocalSlotLeader(ctx, task); !ok { continue }
      if !limits.Allow(task) { continue }
      if err := e.runOne(ctx, task, taskNowMS); err != nil {
          return err          // ← 整个 tick 终止
      }
  }
  return e.gcTerminalTasks(ctx, e.now())   // ← 上面 return 后这行永远不执行
  ```
- **触发路径**：
  1. `runOne` 只对两类错误做了吞掉处理（executor.go:118-125）：`errOwnerCheckFailed` 与
     `ErrPhaseNotImplemented`。**所有存储/Raft 错误都会往上抛**，例如
     `leaderTransferWriteFence` 里的 `e.store.SetChannelWriteFence(ctx, req)`
     （`leader_transfer.go:113-115`，错误直接 `return err`，没有走 `retryTask` 退避）、
     `advanceTask` 里的 `e.store.AdvanceChannelMigrationTask`（`leader_transfer.go:373-375`）、
     `claimOwnerLease` 里除 `ErrStaleMeta`/`ErrNotFound` 之外的错误（executor.go:146-151）。
  2. 任务列表顺序是**确定的**：`pkg/slot/proxy/channel_migration_rpc.go:106-130`
     按 `cluster.SlotIDs()` → `HashSlotsOf(slotID)` → Pebble 有序迭代 `ListChannelMigrationTasks`
     逐层展开，没有随机化、没有轮转起点。
  3. 具体场景：slot S 的 leader 仍是本节点（`LeaderOf` 返回本地，所以
     `confirmLocalSlotLeader`（executor.go:199-207）放行），但 S 已失去多数派（3 副本挂了 2 个）。
     于是 `ClaimChannelMigrationTask` / `SetChannelWriteFence` 的 Raft 提议超时返回错误
     → `Tick` 立刻 `return err`。S 在 `SlotIDs()` 里的位置固定，所以**每一轮 tick 都在同一个任务上中断**。
  4. 后果链：排在 S 之后的所有 slot 上、完全健康的 channel 迁移任务永远不推进；
     `gcTerminalTasks`（executor.go:104）也永远不执行，terminal 任务无限累积在 slot 元数据里。
     `internal/app/channelmigration.go:118-122` 只是把错误打一条 Warn 日志，下个 interval 原样重来。
  5. 讽刺的是节点故障 / 部分失去多数派正是迁移流程存在的理由——它在最需要工作的时候被单点错误卡死。

- **后果**：集群局部故障期间 channel 迁移（leader transfer / replica replace）整体停摆，缩容与故障疏散无法完成；
  终态迁移任务在 slot 元数据中无界累积（GC 被同一条 `return` 跳过）。

- **建议**：把 per-task 错误局部化——记录 metrics/日志并 `continue` 下一个任务，把真正需要中止整轮的情况
  限定为 `ctx` 取消；`gcTerminalTasks` 用 `defer` 或在循环之后无条件执行；
  另外给 `ListRunnable...` 加轮转起点或让失败任务进入退避，避免固定的头部阻塞。

### [P1] 4. 迁移任务无重试上限：drain 目标不可达时 channel 写栅栏被无限期反复重建，导致该 channel 长期不可写，且唯一的 fence 时长指标从未被调用

- **位置**：`internal/runtime/channelmigration/leader_transfer.go:122-165`（DrainLeader）、
  `leader_transfer.go:316-331`（`resetExpiredLeaderTransferFence`）、
  `leader_transfer.go:378-386`（`retryTask`，只设 backoff、`Attempt+1`，无上限判断）、
  `internal/runtime/channelmigration/metrics.go:35-38`（`RecordFenceDuration` / `RecordTaskDuration` 声明但从未调用）
- **类别**：正确性 / 可用性（分布式一致性流程）
- **代码**：
  ```go
  // leader_transfer.go:378-386 —— 只有退避，没有任何 Attempt 上限
  func (e *Executor) retryTask(ctx context.Context, task Task, nowMS int64, cause error, progress slotmeta.ChannelMigrationProgress) error {
      if cause == nil { cause = channel.ErrNotReady }
      return e.advanceTask(ctx, task, nowMS, slotmeta.ChannelMigrationStatusRunning, task.Phase, advanceTaskOptions{
          Progress:    progress,
          LastError:   cause.Error(),
          NextRunAtMS: nowMS + e.cfg.RetryBackoff.Milliseconds(),
      })
  }

  // leader_transfer.go:316-331 —— 栅栏过期后把 phase 退回 WriteFence，下一 tick 重新加栅栏
  func (e *Executor) resetExpiredLeaderTransferFence(...) error {
      ...
      req := slotmeta.ChannelMigrationResetFenceRequest{
          ..., Phase: slotmeta.ChannelMigrationPhaseWriteFence, NowMS: nowMS, ...
      }
  ```
- **触发路径**（用默认配置逐步算）：
  默认值：`ScanInterval = 1s`、`FenceTTL = 1min`、`RetryBackoff = 1min`
  （`internal/app/config.go:1761,1767,1769`）。
  1. 管理面为 channel C 建一个 leader transfer 任务，走到 `WriteFence` 阶段：
     `leaderTransferWriteFence`（leader_transfer.go:93-120）写入 `FenceUntilMS = now + 60s`，phase → `DrainLeader`。
  2. `leaderTransferDrainLeader` 调 `e.migrationControl.FenceAndDrain(ctx, meta.Leader, ...)`（leader_transfer.go:136-144）。
     若当前 channel leader 所在节点网络不可达 / 进程挂了 / 一直返回错误
     → `retryTask`（leader_transfer.go:146-148），`NextRunAtMS = now + 60s`，**没有任何 Attempt 上限**。
  3. t≈60s：任务重新可运行，仍在 `DrainLeader` phase，此时 `fenceExpired` 为真（leader_transfer.go:416-422）
     → `resetExpiredLeaderTransferFence` 清栅栏、phase 退回 `WriteFence`。
  4. t≈61s（下一个 ScanInterval）：`leaderTransferWriteFence` 再次加栅栏，`FenceUntilMS = now + 60s`。
     **回到第 2 步，无限循环。**
  5. 占空比：每 60 秒的周期里，channel C 只有约 1 秒（一个 ScanInterval）处于无栅栏状态，
     其余 ~59 秒写栅栏有效。而写栅栏在数据面是 fail-closed 的：
     `ActivationCache.LoadPositive`（cache.go:68-71）一旦发现 `WriteFence.Token != ""` 就作废缓存，
     业务刷新读到权威 meta 后 `ProjectChannelMeta`（resolver.go:764-770）把 fence 投给本地 runtime，
     append 被 owner 以 `channel.ErrWriteFenced` 拒绝（`isRouteInvalidationError`
     在 `channelplane/channel_cell.go:221` 把它列为 route 失效错误，重试一次后仍然失败）。
  6. 全程没有任何机制让任务放弃：`failTask` 只在 Validate 阶段的静态校验失败时调用
     （leader_transfer.go:44-58）；`blockTask` 只在 `ErrSnapshotRequired` 时调用
     （leader_transfer.go:215-217）；`task.Attempt` 只被 `+1`（executor.go:179、leader_transfer.go:359），
     从没有任何地方读它做上限判断。
  7. 而且这个状态**没有指标可见**：`Metrics` 接口里专门为此设计的
     `RecordFenceDuration(task, duration)`（metrics.go:35-36）和 `RecordTaskDuration`（metrics.go:37-38）
     在整个非测试代码里**零调用点**（只有 `noopMetrics` 的空实现），
     实际被调用的只有 `RecordActiveTasks`、`RecordPhaseTransition`、`RecordRetry`、
     `RecordBlocker`、`RecordGarbageCollection`。运维无法从指标上看出"某个 channel 的写栅栏已经挂了 6 小时"。

- **后果**：单个 channel（可能是一个大群）在 leader 节点不可达期间进入**事实上无限期的写入不可用**
  （约 98% 占空比被 fenced），消息发送持续失败；该状态既不会自动失败退出，也没有 fence 时长指标暴露，
  只能靠人工发现并手动取消任务。同时每 60 秒一轮 reset+refence 都是两次 slot Raft 写，
  在批量迁移场景下叠加成持续的元数据写放大。

- **建议**：给任务加最大 Attempt / 最大总时长，超限后 `blockTask` 到一个明确的 blocker code
  并**保持栅栏清除**（宁可回到未迁移状态也不要长期封写）；
  在 `Tick` 里为每个观察到的任务调用已经声明好的 `RecordFenceDuration` / `RecordTaskDuration`，
  让 fence 持有时长成为可告警指标。

### [P3] 5. `internal/runtime/channelplane/resolver.go` 整个 `Resolver`（singleflight + route 缓存 + invalidation serial）在生产上是死代码，FLOW.md 却把它列为核心组件

- **位置**：`internal/runtime/channelplane/resolver.go:1-95`（整文件）、
  `internal/runtime/channelplane/FLOW.md` 第 3 节组件表与第 4.2 节、
  `internal/app/build.go:507`（实际接线）
- **类别**：架构 / 文档一致性 / 死代码
- **代码**：
  ```go
  // channelplane/resolver.go:16-22 —— 带 cache/serial 的 singleflight resolver
  type Resolver struct {
      source RouteSource
      mu     sync.Mutex
      calls  map[channel.ChannelID]*routeCall
      cache  map[channel.ChannelID]ChannelRoute
      serial map[channel.ChannelID]uint64
  }
  func NewRouteResolver(source RouteSource) *Resolver { ... }
  ```
  ```go
  // internal/app/build.go:507 —— 生产接线直接用裸 adapter，没有经过上面的 Resolver
  Resolver: appChannelPlaneRouteResolver{meta: app.channelMetaSync},
  ```
- **触发路径 / 核实**：
  1. `grep -rn 'NewRouteResolver' --include='*.go' .` 全仓只有 4 处命中：
     `internal/runtime/channelplane/resolver.go:32`（定义）+ `plane_test.go:92,120,153`（**仅测试**）。
     没有任何非测试代码构造它，所以 staticcheck 的 U1000 也没报（被测试引用了）。
  2. `internal/app/channelplane.go:15-30` 的 `appChannelPlaneRouteResolver` 直接
     `r.meta.RefreshChannelMeta(ctx, id)` → `channelmeta.Sync.ActivateByID`，
     singleflight / 代际保护实际由 `channelmeta.ActivationCache`（activate.go + cache.go）完成。
  3. FLOW.md 第 3 节把 `RouteResolver` 列为核心组件并描述为
     "权威路由读取/缓存/失效：singleflight、route generation fencing、metadata refresh"，
     4.2 节进一步描述 "resolver 维护 invalidation serial；失效后新的 resolve 不会 join 更早的
     in-flight lookup，旧 lookup 返回后也不能重新污染 cache"
     —— 这些行为全部只存在于这段死代码里，生产路径上跑的是 channelmeta 的另一套实现。
  4. 附带风险：这段死代码里的 `r.cache` **没有任何 TTL**（resolver.go:42-45 命中即返回），
     一旦有人按 FLOW.md 的描述把它接上生产，`channelCell.handleResolveComplete`
     （channel_cell.go:78-90）在拿到 resolve 结果后**不做 `cachedRouteExpired` 检查**就
     `c.route = &done.route` 并直接 `startAppend`，就会用一条租约早已过期的 route 发起 append。
     现在不构成缺陷只是因为它没被接线。

- **后果**：读 FLOW.md 的人会以为 route 缓存与 invalidation serial 由 channelplane 负责，
  排查路由陈旧问题时会看错文件；95 行逻辑有测试覆盖但零生产覆盖，属于双实现漂移风险。

- **建议**：要么把生产接线改成 `NewRouteResolver(appChannelPlaneRouteResolver{...})`
  （并补上 cache TTL 与 `handleResolveComplete` 的租约校验），要么删掉这个文件并修正 FLOW.md，
  把 singleflight/代际保护的归属明确写成 `internal/runtime/channelmeta`。

### [P3] 6. `ChannelRoute` 不携带写栅栏，但 channelplane FLOW.md 声称 resolver 用 fence 做 RPC epoch fencing

- **位置**：`internal/runtime/channelplane/route.go:20-39`、
  `internal/app/channelplane.go:32-44`、`internal/runtime/channelplane/FLOW.md` 第 4.2 节
- **类别**：文档一致性
- **代码**：
  ```go
  // route.go:20-39 —— 字段里没有任何 WriteFence
  type ChannelRoute struct {
      ChannelID channel.ChannelID
      Leader channel.NodeID
      RouteGeneration uint64
      ChannelEpoch uint64
      LeaderEpoch uint64
      Replicas []channel.NodeID
      ISR []channel.NodeID
      LeaseUntil time.Time
      Status channel.Status
  }
  ```
  ```go
  // internal/app/channelplane.go:32-44 —— 投影时把 meta.WriteFence 整个丢掉
  func appChannelPlaneRouteFromMeta(meta channel.Meta) runtimechannelplane.ChannelRoute {
      return runtimechannelplane.ChannelRoute{
          ChannelID: meta.ID, Leader: meta.Leader, RouteGeneration: meta.RouteGeneration,
          ChannelEpoch: meta.Epoch, LeaderEpoch: meta.LeaderEpoch,
          Replicas: ..., ISR: ..., LeaseUntil: meta.LeaseUntil, Status: meta.Status,
      }   // 没有 WriteFence
  }
  ```
- **触发路径**：FLOW.md 4.2 写 "并用 `RouteGeneration`、`ChannelEpoch`、`LeaderEpoch`、lease 与 fence
  共同做 RPC epoch fencing"。实际 channelplane 从头到尾看不到 fence：一个已被 fenced 的 channel，
  `appChannelPlaneRouteResolver.ResolveRoute` 照样返回一条"正常"的 route，
  `channelCell.startAppend` 照样把 append 发到 leader，由 owner 侧在 append 时返回
  `channel.ErrWriteFenced`，再经 `isRouteInvalidationError`（channel_cell.go:221）触发一次重解析。
  也就是说 fence 是在 **append 层**而不是 **route 层**生效的。
  （功能上仍然 fail-closed，所以这是文档问题不是安全问题；
  但每次被 fence 的 append 都要额外白跑一次 leader RPC + 一次权威刷新。）
- **后果**：文档误导；迁移期间被 fence 的 channel 会产生额外的一整轮 RPC + 权威元数据读，
  而这一轮本可以在本地 route 层短路掉。
- **建议**：要么在 `ChannelRoute` 上补 `WriteFence` 并在 `startAppend` 前短路，
  要么修正 FLOW.md，明确写清 fence 由 owner 的 append 路径强制，route 层不参与。

### [P1] 7. `MaxInflightRPC` 无法从 `Plane` 配置，永久为 1；peer lane 的 flush 队列满时**立即失败**而非等待，正常跨节点 RTT 下远端 append 会大比例返回 `ErrPeerBackpressured`

- **位置**：`internal/runtime/channelplane/plane.go:38-48`（构造 PeerReactor 时漏传 `MaxInflightRPC`）、
  `internal/runtime/channelplane/options.go:19`（`defaultPeerMaxInflightRPC = 1`）、
  `internal/runtime/channelplane/options.go:49-88`（`Options` 结构体里没有 `MaxInflightRPC` 字段）、
  `internal/runtime/channelplane/peer_reactor.go:261-268`（`rpcQueue` 容量 = `MaxInflightRPC`）、
  `internal/runtime/channelplane/peer_reactor.go:419-432`（`enqueueFlush` 满即失败）
- **类别**：性能 / 可用性
- **代码**：
  ```go
  // plane.go:38-48 —— 注意没有 MaxInflightRPC 这一项
  if opts.PeerClient != nil {
      p.peer = NewPeerReactor(PeerReactorOptions{
          Client:          opts.PeerClient,
          LaneCount:       opts.PeerLaneCount,
          MaxBatchWait:    opts.PeerBatchMaxWait,
          RPCTimeout:      opts.PeerRPCTimeout,
          MaxBatchRecords: opts.PeerBatchMaxRecords,
          MaxBatchBytes:   opts.PeerBatchMaxBytes,
          MaxPending:      opts.PeerMaxPending,
      })
  }
  ```
  ```go
  // peer_reactor.go:261-268 —— rpcQueue 的容量就是 MaxInflightRPC，也就是常量 1
  inbox:    make(chan *peerAppendTask, parent.opts.MaxPending),
  rpcQueue: make(chan []*peerAppendTask, parent.opts.MaxInflightRPC),
  ```
  ```go
  // peer_reactor.go:419-432 —— 队列满不是等待，而是把整批任务直接判失败
  func (l *peerLane) enqueueFlush(tasks []*peerAppendTask) {
      select {
      case l.rpcQueue <- tasks:
      case <-l.stopc:
          for _, task := range tasks { l.complete(task, channel.AppendBatchResult{}, ErrClosed) }
      default:
          for _, task := range tasks { l.complete(task, channel.AppendBatchResult{}, ErrPeerBackpressured) }
      }
  }
  ```
- **触发路径**（按默认配置逐拍推演）：
  1. `PeerReactorOptions.MaxInflightRPC` 在 `Plane.New` 里从未被赋值，
     `setDefaults()`（peer_reactor.go:98-100）把它兜成 `defaultPeerMaxInflightRPC = 1`。
     `channelplane.Options` 结构体里**根本没有这个字段**，所以外部（含 `internal/app/build.go:501-510`）
     无论怎么配都改不了它 —— 每个 peer lane 恒定 1 个 RPC worker + 容量 1 的 flush 队列。
  2. lane 事件循环（peer_reactor.go:383-417）的 flush 触发条件：batch 达到
     `MaxBatchRecords=128` / `MaxBatchBytes=256KB`，或 `MaxBatchWait` 定时器到期。
     生产 `MaxBatchWait = cfg.ChannelPlane.PeerBatchMaxWait`，默认 **500µs**
     （`internal/app/config.go:864-866`）。
  3. 设跨节点 `AppendBatches` RPC 往返 2ms（同机房的常见值），某 lane 上有稳定但远未饱和的流量
     （比如 1 万条/秒，每 500µs 约 5 条，远低于 128 条的 batch 上限，所以 flush 全由定时器驱动）：
     - t=0.0ms：flush A → `rpcQueue` 空 → 入队 → 空闲 worker 立刻取走并发起 RPC(A)，2ms 后返回；
     - t=0.5ms：flush B → `rpcQueue` 空 → 入队（worker 忙，B 排队）；
     - t=1.0ms：flush C → `rpcQueue` **满** → 走 `default` → **C 里所有消息以 `ErrPeerBackpressured` 完成**；
     - t=1.5ms：flush D → 仍满 → **D 全部失败**；
     - t=2.0ms：RPC(A) 返回，worker 取 B，发起 RPC(B)；
     - 之后进入稳态：每个 2ms RPC 周期里 4 个 flush 窗口，**约 2 个成功、2 个直接失败**。
  4. `ErrPeerBackpressured` **不在** `isRouteInvalidationError`（`channel_cell.go:221`）的列表里，
     所以 `channelCell.handleAppendComplete` 不会重试，直接
     `c.complete(done.cmd, done.res, done.err, done.route)` 把错误交给调用方。
     FLOW.md 第 5 节也确认："typed backpressure 会通过 `ErrOverloaded` / `ErrPeerBackpressured`
     归一化给上层，message usecase 只看到最终 append 失败"。
  5. `MaxPending = 1024` 这个"看起来很大"的背压上界其实不起作用：任务从 inbox 被取出后瓶颈就变成了
     容量 1 的 `rpcQueue`。也就是说文档承诺的背压水位和真实水位差了三个数量级。

- **后果**：leader 在远端节点的 channel（正常集群里占 (N-1)/N 的比例），在毫秒级 RTT + 中等负载下
  就会有相当比例的消息发送直接失败；RTT 越大失败率越高。这不是过载保护，而是被一个不可配置的
  常量 1 造成的人为吞吐天花板。同时 FLOW.md 第 3 / 4.3 / 5 节反复写 "固定数量 worker"、
  "慢 RPC 不阻塞 lane"、"RPC worker 数均有界"，读起来像是可调的，实际不可调。

- **建议**：把 `MaxInflightRPC` 提升为 `channelplane.Options` 的字段并在 `Plane.New` 传下去，
  默认给一个与 `PeerLaneCount` / 目标节点数相称的值（例如 4~8）；
  同时把 `rpcQueue` 容量与 `MaxInflightRPC` 解耦（队列应显著大于 worker 数），
  并在队列满时优先考虑短暂等待（受 task ctx 约束）而不是立刻把整批判失败。

### [P1] 8. 激活缓存的分片设计在写路径上完全失效：`RunSingleflight` / 所有 store / `Invalidate` 全部串行在单一全局 `c.mu` 上，且一次 store 会持着 `c.mu` 遍历全部 16 个分片 —— 与 FLOW.md 的明确承诺相反

- **位置**：`internal/runtime/channelmeta/cache.go:145-172`（store 持 `c.mu` 调 `pruneForWrite`）、
  `cache.go:370-387`（`pruneForWrite` 遍历全部 16 分片）、`cache.go:409-450`（cap 的 O(n) 淘汰扫描）、
  `internal/runtime/channelmeta/activate.go:19-38`（`RunSingleflight` 取 `c.mu`）、
  `internal/runtime/channelmeta/FLOW.md`（"Business refresh paths" 段落）
- **类别**：性能 / 锁竞争 / 文档一致性
- **代码**：
  ```go
  // cache.go:145-172 —— c.mu 从 153 行一直持到函数返回，包含 171 行的全分片 prune
  func (c *ActivationCache) storePositiveEntryAtGeneration(key channel.ChannelKey, entry cachedChannelMeta, generation ActivationCacheGeneration) {
      ...
      c.mu.Lock()
      defer c.mu.Unlock()
      shard := c.shard(key)
      shard.mu.Lock()
      if !c.isCurrentGenerationLocked(shard, key, generation) { shard.mu.Unlock(); return }
      ...
      shard.positive[key] = entry
      now := entry.expiresAt.Add(-channelMetaPositiveCacheTTL)
      c.pruneShardForWriteLocked(shard, now)   // 内含 cap 的 O(shard) 反复扫描
      shard.mu.Unlock()
      c.pruneForWrite(now)                     // ← 仍在 c.mu 之下，遍历全部 16 个分片
  }
  ```
  ```go
  // cache.go:370-387 —— 全分片 prune + cap
  func (c *ActivationCache) pruneForWrite(now time.Time) {
      if c == nil || !c.pruneMu.TryLock() { return }
      defer c.pruneMu.Unlock()
      if !shouldPruneActivationCacheShard(c.lastPruneAt, now) { return }
      c.lastPruneAt = now
      for i := range c.shards {
          shard := &c.shards[i]
          shard.mu.Lock()
          pruneActivationCacheShardExpiredLocked(shard, now)  // 两张 map 全量扫描
          capActivationCacheShardLocked(shard)                // 超限时每次淘汰再全量扫描一遍
          shard.lastPruneAt = now
          shard.mu.Unlock()
      }
  }
  ```
  ```go
  // cache.go:414-417 + 419-450 —— 每淘汰一条就把 positive+negative 两张 map 整个扫一遍找最旧
  for activationCacheShardEntryCountLocked(shard) > maxEntries {
      evictOldestActivationCacheEntryLocked(shard)
  }
  ```
- **触发路径**：
  1. `c.mu` 是**全局**（非分片）互斥锁，被以下**全部**路径获取：
     `RunSingleflight`（activate.go:19，每一次激活都走）、`finishActivationCall`（activate.go:56）、
     `storePositiveEntryAtGeneration`（cache.go:153）、`storeNegativeAtGeneration`（cache.go:222）、
     `Invalidate`（cache.go:271）、`isCurrentGeneration`（cache.go:292）、`Clear`（cache.go:252）。
     只有纯读路径 `loadPositiveEntry` / `LoadNegative` 是分片本地的。
  2. 业务热路径必经 `c.mu`：`Sync.activate`（resolver.go:531）对**每一次**缓存未命中都调
     `s.cache.RunSingleflight(...)`，其第 19 行第一件事就是 `c.mu.Lock()`；
     成功后在 resolver.go:561 调 `storeAuthoritativePositiveAtGeneration`
     → `storePositiveEntryAtGeneration`，又一次 `c.mu.Lock()` 并持锁做上面的全分片 prune。
     `channelplane` 的每次 route 解析（`appChannelPlaneRouteResolver.ResolveRoute`
     → `RefreshChannelMeta` → `ActivateByID`）都落在这条路上。
  3. 容量上限是硬编码的 `channelMetaActivationCacheMaxEntries = 4096`（cache.go:16），
     分 16 片后每片 **256** 条（cache.go:410）。IM 节点上并发活跃 channel 数轻易超过 4096，
     于是稳定处于"超限"状态：每次 store 都触发
     `capActivationCacheShardLocked` → 循环调 `evictOldestActivationCacheEntryLocked`，
     后者每次都要把该分片的 `positive` 与 `negative` **两张 map 完整遍历一遍**才能找出最旧条目。
  4. 叠加效果：活跃 channel 数超过 4096 后，(a) 缓存命中率崩塌，几乎每次业务刷新都要读权威元数据；
     (b) 每次权威读之后的 store 都在**全局** `c.mu` 之下做 O(256) 的淘汰扫描，
     并且每秒至少一次（`shouldPruneActivationCacheShard` 的间隔是
     `channelMetaNegativeCacheTTL = 1s`，cache.go:389-394）在 `c.mu` 之下把 16 个分片全部扫一遍。
     所有并发激活在这段时间里全部排队在 `c.mu` 上。
  5. **与 FLOW.md 直接冲突**：`internal/runtime/channelmeta/FLOW.md` 写道
     "The activation cache is sharded, TTL-pruned, and capacity-bounded so hot-channel lookups
     **do not share one global lock** or grow without bound."
     实际上写路径与 singleflight 恰恰共享唯一一把全局锁；而"不会无界增长"也被本报告第 1 条否定
     （`generations` 无界）。分片只对读生效。

- **后果**：活跃 channel 超过 4096（生产必然）后，channel 元数据激活成为一个全局串行点，
  吞吐被单把互斥锁 + 周期性的 4096 级别全量扫描封顶；表现为消息发送在元数据刷新环节出现
  与并发度无关的延迟平台。缓存本身也因为容量太小而基本失去意义（几乎全是 miss + 权威读）。

- **建议**：把 `c.mu` 的职责拆开——singleflight 的 `calls` map 与代际计数下沉到分片级；
  store 路径不要在持锁时做跨分片 prune（`pruneForWrite` 应完全在 `c.mu` 之外、
  或交给一个独立的后台清理 goroutine）；容量上限改成可配置并按活跃 channel 规模取值；
  淘汰改用近似 LRU / 时间轮，避免每次淘汰都做全 map 扫描。

### [P2] 9. `ActivationCache` 容量上限 4096 硬编码且不可配置，超过后业务刷新退化为每次都读权威元数据

- **位置**：`internal/runtime/channelmeta/cache.go:12-17`、`cache.go:409-417`
- **类别**：性能
- **代码**：
  ```go
  // cache.go:12-17
  const (
      channelMetaPositiveCacheTTL          = 5 * time.Second
      channelMetaNegativeCacheTTL          = time.Second
      channelMetaActivationCacheShards     = 16
      channelMetaActivationCacheMaxEntries = 4096
  )
  ```
  ```go
  // cache.go:409-413 —— 每分片 4096/16 = 256 条
  func capActivationCacheShardLocked(shard *activationCacheShard) {
      maxEntries := channelMetaActivationCacheMaxEntries / channelMetaActivationCacheShards
      if maxEntries <= 0 { maxEntries = 1 }
      for activationCacheShardEntryCountLocked(shard) > maxEntries {
          evictOldestActivationCacheEntryLocked(shard)
      }
  }
  ```
- **触发路径**：
  1. 这四个常量都是包私有 `const`，`SyncOptions`（resolver.go:23-48）没有任何对应字段，
     `internal/app` 也没有配置项可以调 —— 无论部署规模多大，上限恒为 4096 条。
  2. 每个活跃 channel 在缓存里占 1 条（positive）。一个承载 10 万活跃会话的节点，
     缓存只能覆盖 4%，其余每次业务刷新都走 `activate` 的 `load` 分支：
     `s.source.GetChannelRuntimeMeta`（权威 slot 元数据读，本节点非 slot leader 时是跨节点 RPC）
     → `reconcileChannelRuntimeMetaForRefresh`（可能再触发一次 `RenewChannelLeaderLease` 的 Raft 写）
     → `ApplyAuthoritativeMeta`。
  3. 淘汰策略是"最早过期优先"（`evictOldestActivationCacheEntryLocked`，cache.go:419-450），
     而所有 positive 条目的 TTL 都是同一个 5 秒，所以实际等价于"最早写入的先淘汰"——
     纯 FIFO，没有任何访问频率信息，热 channel 一样会被冷 channel 挤掉。
- **后果**：中大型部署下激活缓存形同虚设，每条消息的路由解析都可能变成一次权威元数据读
  （潜在跨节点 RPC）；配合第 8 条的全局锁，构成 channel 元数据层的吞吐上限。
- **建议**：把上限、TTL、分片数提升为 `SyncOptions` 字段并从 `internal/app` 配置注入，
  默认值按"每节点预期活跃 channel 数"量级取；淘汰换成带访问信息的近似 LRU。

### [P2] 10. 保留期 worker 的单 channel 错误会中止整轮扫描 —— 与代码自身注释声明的设计意图直接矛盾

- **位置**：`internal/runtime/channelretention/worker.go:270-289`（`RunOnce` 循环）、
  `worker.go:20-22`（`ErrChannelUnavailable` 的注释）、`worker.go:292-340`（`runChannel` 的 6 个错误出口）
- **类别**：正确性 / 资源（磁盘无界增长）
- **代码**：
  ```go
  // worker.go:20-22 —— 代码自己写明的设计意图
  // ErrChannelUnavailable marks a channel that cannot currently provide a
  // retention view. The worker skips it so one cold channel does not block a pass.
  var ErrChannelUnavailable = errors.New("channel retention: channel unavailable")
  ```
  ```go
  // worker.go:270-289 —— 但只有这一种错误被跳过，其它全部中止整轮
  for _, ch := range channels {
      if err := ctx.Err(); err != nil { return err }
      if err := w.runChannel(ctx, ch); err != nil {
          if errors.Is(err, ErrChannelUnavailable) {
              w.logger().Warn("channel retention channel skipped", ...)
              continue
          }
          return err     // ← 后面所有 channel 这一轮都不处理
      }
  }
  ```
- **触发路径**：
  1. `runChannel`（worker.go:292-340）有 **6 个**不是 `ErrChannelUnavailable` 的错误出口：
     `Runtime.RetentionView`（294、323 两处）、`Runtime.ApplyRetentionBoundary`（302、339）、
     `Stores.StoreForChannel`（307）、`Store.ScanExpiredMessagePrefix`（311）、
     `Store.ConfirmCommittedDispatchCursorDurable`（319）、
     `Metadata.AdvanceChannelRetentionThroughSeq`（336）。任意一个返回错误都直接中止整轮。
  2. 最容易复现的一类：`AdvanceChannelRetentionThroughSeq` 是一个带 CAS 的 slot Raft 写，
     守卫字段包含 **`ExpectedLeaseUntilMS`**（`retentionAdvanceRequest`，worker.go:370-381）。
     而 channel leader 租约会被 `channelmeta` 的 `RenewChannelLeaderLease`
     （`internal/runtime/channelmeta/bootstrap.go:174-176`）独立刷新
     （`BootstrapLease = 30s`，提前 1 秒续期）。
     `runChannel` 在 323 行读 `latest`、336 行才发起 Raft 提议，中间只要发生一次租约续期，
     `ExpectedLeaseUntilMS` 就不再匹配 → CAS 失败 → 错误上抛 → **整轮扫描中止**，
     排在后面的所有 channel 这一分钟都不做保留期推进。
     一轮里 channel 越多，撞上的概率越高（N 个 channel × 每次几毫秒的窗口）。
  3. 更严重的确定性场景：如果 `ListRetentionChannels` 返回的某个 channel 的本地 store
     打不开或 `ScanExpiredMessagePrefix` 稳定报错（本地状态损坏 / 文件权限 / Pebble 错误），
     那么**每一分钟的每一轮都在同一个 channel 上中止**，它之后的所有 channel 永远不做保留期清理。
  4. 这与第 20-22 行注释明示的 "one cold channel does not block a pass" 完全相反 ——
     设计意图是逐 channel 隔离，实现只隔离了六分之一的错误来源。

- **后果**：保留期（TTL）清理在部分 channel 上静默停摆，对应的消息数据无限期保留，
  磁盘占用持续增长且没有告警（只有一条 `channel_retention.pass.failed` 的 Warn 日志，
  并且看不出"哪些 channel 被跳过了"）；由于列表顺序稳定，受影响的永远是同一批 channel。

- **建议**：把逐 channel 的错误全部按"记录 + `continue`"处理（只有 `ctx` 取消才中止整轮），
  并在一轮结束后汇总返回 `errors.Join`；同时把 `ExpectedLeaseUntilMS` 从保留期推进的 CAS 守卫里去掉
  （租约续期与保留期边界是正交的，epoch + leader 已经足够 fencing）。

### [P2] 11. 显式 leader transfer（`TransferChannelLeaderAuthoritative`）不检查写栅栏，可以在迁移 cutover 中途搬走 leader

- **位置**：`internal/runtime/channelmeta/leader_transfer.go:71-128`、`leader_transfer.go:130-141`
  （`validateLeaderTransferTarget`）
- **类别**：分布式一致性 / 缺失守卫
- **代码**：
  ```go
  // leader_transfer.go:130-141 —— 校验项里没有 WriteFence
  func validateLeaderTransferTarget(meta metadb.ChannelRuntimeMeta, targetNodeID uint64) error {
      if meta.Status != uint8(channel.StatusActive) { return ErrLeaderTransferInactiveChannel }
      if targetNodeID == 0 || !containsUint64(meta.Replicas, targetNodeID) { return ErrLeaderTransferTargetNotReplica }
      if !containsUint64(meta.ISR, targetNodeID) { return ErrLeaderTransferTargetNotISR }
      return nil
  }
  ```
  ```go
  // leader_transfer.go:102-108 —— 直接改 Leader 并 +1 LeaderEpoch，没有任何 fence 判断
  updated := latest
  updated.Leader = req.TargetNodeID
  updated.LeaderEpoch++
  updated.LeaseUntilMS = r.currentTime().Add(BootstrapLease).UnixMilli()
  if err := r.store.UpsertChannelRuntimeMetaIfLocalLeader(ctx, updated); err != nil { return nil, err }
  ```
- **触发路径**：
  1. 迁移任务处于 `FinalTargetCatchUp` / `CommitLeaderMeta` 阶段：写栅栏已生效，旧 leader 已被
     `FenceAndDrain` 封停，任务里记着 `CutoverLEO/CutoverHW/DrainedLeaderNode/
     DrainedChannelEpoch/DrainedLeaderEpoch/DrainedFenceVersion`。
  2. 运维通过管理面触发一次显式 leader transfer（`internal/access/node` → `TransferChannelLeader`
     → `TransferIfSafe` → 本函数）。`validateLeaderTransferTarget` 只看 Status/Replicas/ISR，
     `observedTransferEpochsStale` 只比 epoch，**没有一处看 `latest.WriteFenceToken`**，
     `needsLeaderRepair`（`internal/app/channelmeta_liveness.go:27-53`）同样不排除被 fence 的 channel，
     所以自动 repair 路径（`repair.go:148-155`）也有同样的缺口。于是 leader 被换成另一个 ISB 节点，
     `LeaderEpoch` +1。
  3. **后续是安全的（已核实）**：slot 层的 `requireChannelMigrationCutoverProof`
     （`pkg/db/meta/compat_channel_migration_helpers.go:352-358`）严格要求
     `task.DrainedLeaderNode == meta.Leader` 且 `task.DrainedLeaderEpoch == meta.LeaderEpoch`，
     所以 `CommitChannelLeaderTransfer` 会以 `ErrConflict` 被拒绝，不会用陈旧的 drain 证明提交。
     但这条错误随后走到本报告第 3 条的路径（`Tick` 整轮中止），任务要等到栅栏过期
     （`FenceLease` 默认 1 分钟）后经 `resetExpiredLeaderTransferFence` 退回 `WriteFence`
     重新走一遍 drain 才能恢复。
- **后果**：不是数据不一致（slot 层 proof 守卫兜住了），而是一次运维动作就能让目标 channel 白白
  多背 1 分钟左右的写栅栏不可用，并且顺带触发一整轮迁移 tick 中止（第 3 条）。
  缺口在于 runtime 层把"迁移进行中不得改 leader"这条约束完全外推给了 slot 层，本层零防御。
- **建议**：`validateLeaderTransferTarget` 与 `needsLeaderRepair` 增加"存在活跃写栅栏时拒绝显式 transfer /
  降级 repair 行为"的判断（`latest.WriteFenceToken != "" && latest.WriteFenceUntilMS > now`），
  让冲突在提议之前就被拒绝并给调用方一个明确的 typed error，而不是走到 slot 层 CAS 冲突。

### [P3] 12. 死代码：`(*Sync).maybeRepairChannelRuntimeMeta`、`replicaReplaceCaughtUp`、以及仅测试可达的 `(*Sync).syncOnce` / `(*PeerReactor).AppendRemoteBatch`

- **位置**：`internal/runtime/channelmeta/resolver.go:643-646`、
  `internal/runtime/channelmigration/replica_replace.go:441-443`、
  `internal/runtime/channelmeta/resolver.go:571-597`、
  `internal/runtime/channelplane/peer_reactor.go:161-182`
- **类别**：死代码 / 架构
- **代码**：
  ```go
  // resolver.go:643-646 —— staticcheck U1000，全仓零调用点
  func (s *Sync) maybeRepairChannelRuntimeMeta(ctx context.Context, meta metadb.ChannelRuntimeMeta) (metadb.ChannelRuntimeMeta, error) {
      repaired, _, err := s.maybeRepairChannelRuntimeMetaForRefresh(ctx, meta)
      return repaired, err
  }
  ```
  ```go
  // replica_replace.go:441-443 —— staticcheck U1000
  func replicaReplaceCaughtUp(leader, target ProbeReport) bool {
      return target.LogEndOffset >= leader.LogEndOffset && target.CheckpointHW >= leader.CheckpointHW
  }
  ```
- **核实与结论**：
  1. `maybeRepairChannelRuntimeMeta` 只是 `maybeRepairChannelRuntimeMetaForRefresh` 的丢弃返回值包装，
     真正的自愈路径（`reconcileChannelRuntimeMetaForRefresh` → `maybeRepairChannelRuntimeMetaForRefresh`
     → `repairer.RepairIfNeeded`）**是接线的、在跑的**（resolver.go:657-673、636-652）。
     所以这里不存在"自愈从未接上"的问题，只是一层多余的薄包装。
  2. `replicaReplaceCaughtUp`（严格版"完全追平"判定）虽然是死的，但**晋升安全门没有缺失**：
     - 热追阶段用的是宽松版 `replicaReplaceWithinCatchUpThreshold`（replica_replace.go:445-453），
       它只决定何时进入 `CutoverFence`，不决定晋升；
     - 真正的晋升门是 `ProofEvaluator.EvaluateFinalTargetProof`（`proof.go:7-61`），
       要求 `target.LogEndOffset >= CutoverLEO`、`target.CheckpointHW >= CutoverHW`、
       `target.LogStartOffset <= CutoverHW`、`!SnapshotRequired`、`TruncateTo == nil`、
       `CommitReady`，并用 `targetOffsetEpochAt` 精确核对 `CutoverHW` 处的 epoch。
       这比死掉的那个谓词**更严格**。所以这条只是死代码，不是安全漏洞。
  3. `syncOnce`（resolver.go:571-597）是唯一的"全量重新对账"路径（列出全部权威 meta、
     移除不再属于本节点的 local runtime、重建 `appliedLocal`），
     `grep -rn 'syncOnce' internal/runtime/channelmeta/` 只有 `resolver_test.go:1073,1107` 两处调用
     —— **生产上从不执行**。这与 FLOW.md "`Start` only runs lightweight active-slot leader watchers
     and refresh workers for hot channels" 是一致的（有意如此），
     但意味着 `appliedLocal` 的漂移没有任何周期性兜底，只能靠 slot 刷新路径顺带自愈。
  4. `AppendRemoteBatch`（同步版远端 append）只被 `peer_reactor_test.go` 使用（12 处），
     生产走的是 `AppendRemoteBatchAsync`（`channel_cell.go:117`）。
- **后果**：无运行时影响；增加维护噪音，并且 `syncOnce` 这种"看起来像兜底机制"的死路径
  容易让人误以为存在周期性对账。
- **建议**：删掉 1、2、4 三处；`syncOnce` 要么删掉、要么真的接成一个低频（分钟级）的后台对账任务
  并在 FLOW.md 里说明。

## 已排除的候选项

### gosec 机械命中（8 条，全部误报）
- `channelmeta/resolver.go:373`、`resolver.go:746`、`channelmeta/repair.go:73`、
  `channelmeta/leader_transfer.go:29`、`channelmigration/leader_transfer.go:469`
  —— G115 `int64 -> uint8`，都是 `uint8(meta.ChannelType)` / `uint8(task.ChannelType)`。
  `ChannelType` 的来源是协议层的 `channel.ChannelID.Type uint8`，写入时是
  `ChannelType: int64(id.Type)`（`channelmeta/bootstrap.go:120`），是一个 `uint8 → int64 → uint8`
  的闭环往返，值域天然 ≤255。**误报。**
  （唯一残留风险：读路径没有对 `ChannelType > 255` 的上界校验，一条被外部工具手写/损坏的元数据行
  会静默别名到另一个 channel type；但这不是远程输入可达的路径，不构成本次的发现。）
- `channelmeta/resolver.go:768` —— G115 `uint64 -> uint8`？实际该行是
  `MessageSeqFormat: channel.MessageSeqFormat(meta.Features)`，`MessageSeqFormat` 是枚举类型转换，
  不是长度/偏移，且只影响序号编码格式的分支选择。**误报。**
- `channelplane/plane.go:152`、`channelplane/peer_reactor.go:251` —— G115 `int -> uint32`，
  分别是 `uint32(len(p.reactors))` 与 `uint32(p.opts.LaneCount)` 作哈希取模的除数。
  两者都在构造时被 `validate()`（`options.go:133`）/ `Start()`（`peer_reactor.go:105`）
  校验为 `> 0`，且都是配置级小整数。**误报。**

### staticcheck U1000（2 条）
- 两条都是真的死代码，已合并为本报告第 12 条（并说明了"晋升安全门未缺失"的核实结论）。

### 语义层面查过但排除的候选
- **`channelmeta/activate.go:41` 的 `recover()`（全仓仅 2 处非测试 `recover()` 之一）** ——
  这是标准 singleflight 的 panic 传播模式，与 `golang.org/x/sync/singleflight` 一致：
  捕获后写入 `call.panicked/panicValue`、`finishActivationCall` 关闭 `done` 并从 `calls` map 摘除、
  然后 **`panic(recovered)` 原样重抛**（activate.go:40-48），等待者在 activate.go:31-33
  也重抛同一个值。既没有吞掉 panic，也不会留下悬挂的 singleflight 条目。**不是缺陷。**
- **`channelmeta/resolver.go:148-175` 的 `stop()`** —— 正向参考实现，已核实：
  `scheduleMu` + `mu` 双锁下取出 `cancel`/`done` 并把 `runCtx`/`cancel`/`done`/`stateChanges` 置 nil，
  两把锁都释放后才 `cancel()` → `<-done` → `refreshWG.Wait()`。
  `refreshWG.Add(1)` 在 `Start`（resolver.go:130）里于启动 goroutine **之前**完成，
  由 `watchLocalReplicaStateChanges` 的 `defer s.refreshWG.Done()`（resolver.go:713）配对；
  `watchActiveSlotLeaders` 由 `defer close(done)`（resolver.go:697）配对；
  `SlotRefreshScheduler.Schedule` 的 `wg.Add(1)` 在 `ScheduleSlotLeaderRefresh` 持 `scheduleMu`
  的临界区内完成，而 `stop()` 先拿 `scheduleMu` 再置空 `runCtx`，
  因此不存在 "Add after Wait" 竞态。本分片内没有比它更差的 shutdown 实现。
- **`channelplane/reactor.go:69-83` 的 `post` 持 `submitMu` 跨阻塞 channel 发送** ——
  确实在 inbox 满时会持锁阻塞，导致同 shard 的 `submit`（reactor.go:49-67，
  本该靠 `default: return ErrOverloaded` 立即返回）被卡在互斥锁上。
  但排查后确认**不会死锁**：run loop 里所有可能长阻塞的下游调用
  （`effects.submit`、`peer.AppendRemoteBatchAsync` → `lane.submit`）都是带 `default` 的非阻塞 select
  （`effect_executor.go:71-81`、`peer_reactor.go:319-331`），所以 run loop 一定会继续消费 inbox，
  阻塞时长上界是一次 `handle()` 迭代。影响是微秒级延迟抖动，写不出有实际后果的触发路径。
- **`channelplane/plane.go:95-112` 的 Stop 顺序（先 join reactor，后停 effects）** ——
  reactor 退出后，仍在跑的 effect worker 完成 `AppendLocalBatch` 再 `r.post(...)`，
  会走 `completePostedAfterStop`（reactor.go:85-89）以 `ErrClosed` 完成 future，
  于是"已经成功落盘的 append"对调用方报失败。看起来像停机期的重复消息风险，
  但不存在 send-on-closed-channel（inbox 从不 close）也不会 panic，
  且写不出"必然产生重复消息"的路径（上层有 `ClientMsgNo` 去重），故排除。
- **`channelmeta/bootstrap.go:174-176` 的 `RenewChannelLeaderLease` 全记录读-改-写** ——
  它把先前读到的整条 `meta`（含 `Replicas`/`ISR`/`WriteFence*`）连同新租约一起 upsert，
  看起来会覆盖并发修改。核实后确认被 slot FSM 的单调守卫挡住：
  `pkg/db/meta/table_runtime_meta.go:363-391` 的 `resolveMonotonicChannelRuntimeMeta` 里
  `candidate.ChannelEpoch < existing.ChannelEpoch` / `LeaderEpoch <` → `MonotonicIgnoredStale`；
  `candidate.Leader != existing.Leader`（同 epoch）→ `MonotonicConflict`；
  `candidateHadRouteGeneration && candidate.RouteGeneration < existing.RouteGeneration` → 同样 IgnoredStale；
  且 `preserveRuntimeMetaState`（399-404）强制保留更高的 `WriteFenceVersion` 与
  `RetentionThroughSeq`。而所有会改成员关系的迁移命令都会顶 epoch
  （如 `AddChannelLearner` 的 `nextMeta.ChannelEpoch++`，`pkg/db/meta/compat.go:1550-1553`），
  所以陈旧读的成员关系写不回去。**不是缺陷。**
- **"节点本地缓存的 fence token 是否可能过期后仍放行写入"（协调者指定的最高价值问题）** ——
  核查结论是**否，写栅栏在本层被正确遵守**，理由链如下，故不作为发现：
  1. 激活缓存对 fence 彻底 fail-closed：`LoadPositive` 一旦读到
     `entry.meta.WriteFence.Token != ""` 就 `Invalidate` 并返回 miss（`cache.go:68-71`）；
     `storePositiveEntry` / `storePositiveEntryAtGeneration` 拒绝缓存任何带 fence 的 meta
     （`cache.go:116-119`、`149-152`）——也就是被 fence 的视图**根本进不了缓存**；
     `cachedHealthyBusinessMeta` 再加一道（`resolver.go:412-415`）。
  2. `invalidateIfWriteFenceChanged`（`cache.go:300-321`）在每次 `ApplyAuthoritativeMeta`
     开头执行（`resolver.go:284`），对 `RouteGeneration` / `WriteFenceVersion` / `WriteFenceToken` /
     `WriteFenceReason` / `WriteFenceUntilMS` 任一变化都作废缓存。
  3. ownership 变更由 `runtimeGuardFromMeta`（`channelmigration/leader_transfer.go:506-516`）
     带着 `ExpectedChannelEpoch/ExpectedLeaderEpoch/ExpectedLeader/ExpectedFenceToken/
     ExpectedFenceVersion` 在 Raft apply 内部 CAS，不是 TOCTOU 检查。
  4. cutover 提交额外受 `requireChannelMigrationCutoverProof`
     （`pkg/db/meta/compat_channel_migration_helpers.go:352-358`）保护，严格要求
     `task.DrainedLeaderNode == meta.Leader`、`task.DrainedLeaderEpoch == meta.LeaderEpoch`、
     `task.DrainedFenceVersion == meta.WriteFenceVersion`。
  5. 最终放行/拒绝发生在 owner 的 append 路径（返回 `channel.ErrWriteFenced`），
     owner 用的是自己 applied 的权威 meta，而 `FenceAndDrain` 在取 drain 证明**之前**
     就已经同步封停了 owner，所以不存在"证明已取、写入仍被放行"的窗口。
     （残留的 ≤5 秒窗口：fence 由别的节点写入时，本节点已有的未 fence 正向缓存要等
     `channelMetaPositiveCacheTTL = 5s` 到期才失效；但这期间的 append 仍要经 owner 侧强制，
     且发生在 drain 之前，不会丢写。）
- **`channelmigration` 的 `ErrMissingDependency`（probe client / migration control 为 nil）
  会中止整轮 tick** —— `validateReady`（`executor.go:231-255`）没有校验这两个依赖，
  看起来可以造成永久 tick 中止；但 `internal/app/channelmigration.go:235-244` 的构造
  总会注入 `ProbeClient` 与 `MigrationControl`，生产不可达，故排除
  （错误处理的结构性问题已在第 3 条里覆盖）。
- **`GetChannelRuntimeMeta` 返回 `ErrNotFound` 造成迁移任务永久卡死** ——
  需要有人删掉 channel 运行时元数据。`EncodeDeleteChannelRuntimeMetaCommand`
  （`pkg/slot/fsm/command.go:559-563`）全仓**零调用点**，`DeleteChannelRuntimeMeta`
  也没有任何 `internal/` 调用方，所以这条路径生产不可达，排除。
- **`PeerReactor.lanes` / `LivenessCache.values` / `Sync.slotLeaders` / `Sync.appliedSlots`** ——
  都是按"集群节点数"或"slot 数"维度增长的 map，有明确上界（节点数 × LaneCount、slot 数），
  且 `untrackAppliedLocalKeyLocked`（`resolver.go:894-921`）会在计数归零时删除条目并把空 map 置 nil。
  不是无界增长。
- **`peerLane` 的 `close(l.rpcQueue)`** —— 看起来像"可能对正在发送的 channel 执行 close"，
  但 `enqueueFlush` 唯一调用点在 `l.run()` 自己的 goroutine（`peer_reactor.go:378-381` 的 `flush`），
  而 `close(l.rpcQueue)` 在同一 goroutine 的 defer 里（`peer_reactor.go:333-336`），
  不存在并发 send/close。设计正确。
- **`scheduler.pop` 的 `s.ready = s.ready[1:]`**（`channelplane/scheduler.go:25-34）——
  会前移切片而不重置底层数组，但弹出时用 `s.ready[0] = channel.ChannelID{}` 清零了元素
  （不残留字符串引用），且 cap 耗尽后 `append` 会重新分配。是内存周期性抖动，不是泄漏。

## 本分片整体评价

这四个包（6970 行，**全部在 v1 生产运行路径上**）的**分布式正确性设计明显高于本仓库平均水平**：
写栅栏端到端 fail-closed、迁移命令一律走 Raft apply 内的 CAS（`RuntimeGuard` + cutover proof）、
晋升只从 ISR 里选候选人（learner 不会被选为 leader）、`EvaluateFinalTargetProof` 的证明链严密，
`channelmeta.Sync.stop()` 是全仓最规范的关停实现之一。协调者指定的最高价值问题
——"节点本地缓存的 fence token 会不会过期后仍放行写入"——核查结论是**不会**，理由链见「已排除的候选项」。

真正的问题集中在**资源治理与错误处理的工程化**上，而且几乎都表现为"设计意图写在文档/注释里，
实现只落实了一部分"：
1. `ActivationCache` 有完整的 TTL + cap + LRU 三件套，却整齐地漏掉了第三张 map `generations`
   （第 1 条，无界泄漏）——而且 FLOW.md 恰好承诺了 "grow without bound" 不会发生。
2. 同一个缓存宣称"分片以避免共享全局锁"，但写路径与 singleflight 全部串行在单把 `c.mu` 上，
   还会持着它遍历 16 个分片（第 8 条）；容量上限硬编码 4096（第 9 条）使缓存在生产规模下近乎失效。
3. 两处"一个坏元素毁掉一整轮"的头部阻塞（迁移 executor 第 3 条、保留期 worker 第 10 条），
   后者与代码自身注释 "one cold channel does not block a pass" 直接矛盾。
4. 迁移任务没有重试上限，drain 目标不可达时写栅栏会被无限期反复重建，把单个 channel
   打成事实上永久不可写，而唯一为此设计的 `RecordFenceDuration` 指标从未被调用（第 4 条）。

**最需要优先处理的一个问题**：第 4 条。它是唯一能在不触发任何告警的情况下造成
**用户可感知的、长期的消息发送失败**的缺陷——既没有自动放弃路径，也没有指标暴露，
运维只能靠用户投诉发现。修复成本也最低：加一个 Attempt/时长上限 + 调用两个已经声明好的指标方法。
紧随其后的是第 1 条（长跑内存单调增长，与 `pkg/db/meta` 的同类缺陷构成
"本代码库的按-channel 缓存普遍缺少淘汰设计"这一**模式性问题**）与第 8 条（元数据激活的全局锁）。

## 附：分层检查（九类检查 i）结论

用 `GOTOOLCHAIN=go1.23.4 go list -deps` 展开四个包的完整传递依赖，
过滤 `internal/access`、`internal/usecase`、`internal/app`、`pkg/gateway` —— **零命中**。
`internal/runtime/channelmeta/FLOW.md` 与 `internal/runtime/channelplane/FLOW.md`
各自声明的"禁止依赖"清单被严格遵守，`internal/runtime/*` 里没有入口协议逻辑，
依赖方向（`runtime -> pkg`）正确。**本项无发现。**
