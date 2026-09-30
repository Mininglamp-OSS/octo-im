# channel runtime 生命周期与背压

## 覆盖情况

分片路径：`pkg/channel/runtime/`（全部 18 个非测试 .go，5649 行）+ 根级 `pkg/channel/*.go`（6 个非测试 .go，909 行）。全部逐行通读完毕。

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/channel/runtime/backpressure.go | 1194 | 是 |
| pkg/channel/runtime/runtime.go | 771 | 是 |
| pkg/channel/runtime/replicator.go | 547 | 是 |
| pkg/channel/runtime/lanes.go | 486 | 是 |
| pkg/channel/runtime/channel.go | 459 | 是 |
| pkg/channel/runtime/types.go | 351 | 是 |
| pkg/channel/runtime/longpoll.go | 351 | 是 |
| pkg/channel/runtime/lane_session.go | 288 | 是 |
| pkg/channel/runtime/lane_dispatcher.go | 281 | 是 |
| pkg/channel/runtime/scheduler.go | 277 | 是 |
| pkg/channel/runtime/snapshot.go | 227 | 是 |
| pkg/channel/runtime/commit_notify.go | 103 | 是 |
| pkg/channel/runtime/tombstone.go | 89 | 是 |
| pkg/channel/runtime/lane_directory.go | 55 | 是 |
| pkg/channel/runtime/activation.go | 48 | 是 |
| pkg/channel/runtime/sendtrace_helpers.go | 47 | 是 |
| pkg/channel/runtime/session.go | 44 | 是 |
| pkg/channel/runtime/retention.go | 31 | 是 |
| pkg/channel/types.go | 475 | 是 |
| pkg/channel/channel.go | 374 | 是 |
| pkg/channel/errors.go | 32 | 是 |
| pkg/channel/leader_repair_reason.go | 17 | 是 |
| pkg/channel/durable_codec.go | 9 | 是 |
| pkg/channel/doc.go | 2 | 是 |

按 AGENTS.md 要求先读了 `pkg/channel/FLOW.md`（297 行）。发现的 FLOW.md 分歧见发现 8 与发现 6。

跨包佐证阅读（不作为发现落点）：`pkg/channel/transport/session.go`、`transport/transport.go`、`pkg/channel/replica/replica.go`、`replica/pooled_loop_driver.go`、`replica/loop.go`、`replica/follower_apply.go`、`replica/progress.go`、`internal/app/build.go`、`internal/app/config.go`、`internal/app/channelmeta.go`、`internal/runtime/channelmeta/resolver.go`、`cmd/wukongim/config.go`。

生产配置事实（影响严重度判定，均已核实）：`internal/app/build.go:322` 设 `AutoRunScheduler=true`；`LongPollLaneCount` 默认 8（config.go:1166）→ long poll 是生产唯一复制主路径；`DataPlaneRPCTimeout` 默认 1s（config.go:1748-1753）；`LongPollHWOnlyNotifyDelay` 默认 5ms（build.go:63）；`ChannelIdleTimeout` 默认 0（禁用 idle eviction，需 `WK_CLUSTER_CHANNEL_IDLE_TIMEOUT` 显式开启）；执行模式默认 pooled（mailbox 2048 / turnBudget 16）。

## 发现

### [P1] 1. 全局 sendCoordMu 跨同步网络 RPC 和响应处理（含落盘）被持有——一个不可达 peer 即可让全节点复制发送串行卡顿

- **位置**：`pkg/channel/runtime/backpressure.go:333-407`（关键 337-344、391-399）
- **类别**：并发 / 性能
- **代码**：
  ```go
  func (r *runtime) sendEnvelope(env Envelope) error {
      ...
      r.sendCoordMu.Lock()
      r.sendCoordActive.Add(1)
      defer func() { ... r.sendCoordMu.Unlock() }()
      err := r.sendEnvelopeLocked(env)
  ```
  ```go
  if env.Kind == MessageKindLanePollRequest {
      // Long-poll RPCs can legitimately park for maxWait, so don't keep
      // global send coordination locked across the blocking session.Send.
      r.sendCoordMu.Unlock()
      err := session.Send(env)
      r.sendCoordMu.Lock()
      return err
  }
  if err := session.Send(env); err != nil {   // ← 其余 kind 全程持锁
  ```
- **触发路径**：
  1. 只有 `LanePollRequest` 在 `session.Send` 期间释放了 `sendCoordMu`；`FetchRequest` / `ReconcileProbeRequest` 走 `session.Send` → `pkg/channel/transport/session.go:84-98 / 204-218`，是**同步 RPC**（`context.WithTimeout(Background, rpcTimeout)`，默认 1s），且响应在 `Send` 返回前通过 `adapter.deliver` **同步**回注 `runtime.handleEnvelope(Sync=true)`（session.go:274-298）。
  2. 领导者侧：`processLeaderLongPoll`（replicator.go:138-192）在调度 worker 上逐个 peer 同步发 `ReconcileProbeRequest`。触发序列：某 ISR peer 宕机 → 每次探测 RPC 满打满算挂满 1s 超时，期间**全局** `sendCoordMu` 被持有 → 所有 lane-poll 发送（`dispatchLanePoll` → `sendEnvelope`）、背压重试 drain、deferred sync drain 全部在 `sendCoordMu.Lock()` 上排队，逐 peer 1s 地串行放大（N 个坏 peer = N 秒级全局发送停顿）。
  3. 更糟的一层：响应回注的 `handleEnvelope` → `applyReconcileProbeResponseEnvelope`（backpressure.go:1020-1030）→ `ch.replica.ApplyReconcileProof(context.Background(), ...)`，即**副本 loop 回环等待也发生在持锁状态下**——replica loop 积压（pooled 模式 mailbox 可达 2048 条命令）时，锁持有时间进一步超过 RPC 超时。
  4. 非 long-poll 回退路径下，`processReplication` 的 `FetchRequest` 同理，且它跑在唯一调度 worker 上，还会顺带阻塞所有频道的调度任务。
- **后果**：单个慢/不可达 peer 使全节点所有对外复制发送（含与其他健康 peer 的 lane poll）串行卡顿；leader 收敛期间（正是探测最密集的时段）最易触发，表现为复制整体降级而非单 peer 降级。
- **建议**：把 `session.Send` 的"长操作不放全局锁"原则从 LanePoll 扩展到 Fetch/Probe（或将响应回注改为异步），sendCoordMu 只保护 inflight 记账与队列原子性。

### [P1] 2. LeaderLaneSession.Poll 在持有 (peer,lane) 会话锁的情况下用 context.Background() 阻塞等待副本 loop——一个积压频道冻结同 lane 所有频道的复制唤醒

- **位置**：`pkg/channel/runtime/lane_session.go:206-232`（apply 回调在锁内）+ `pkg/channel/runtime/longpoll.go:326-351`（Background ctx）
- **类别**：并发 / context
- **代码**（lane_session.go，`defer s.mu.Unlock()` 覆盖整个 apply 循环）：
  ```go
  func (s *LeaderLaneSession) Poll(
      cursor []LaneCursorDelta,
      apply func(LaneCursorDelta),
      ...
  ) (LeaderLanePollResult, *lanePollWaiter) {
      s.mu.Lock()
      defer s.mu.Unlock()
      for _, delta := range cursor {
          ...
          if apply != nil {
              apply(delta)
          }
      }
  ```
  （longpoll.go:336-343，apply 的实现）
  ```go
  if applier, ok := ch.replica.(followerCursorApplier); ok {
      _ = applier.ApplyFollowerCursor(context.Background(), core.ReplicaFollowerCursorUpdate{...})
      return
  }
  ```
- **触发路径**：
  1. `ServeLanePoll`（longpoll.go:98-105）把 `r.applyFollowerCursor` 作为 apply 传入 `session.Poll`；follower 的 `cursorDelta` 在**持有该 (peer,lane) 会话 mutex** 时逐条应用。
  2. `ApplyFollowerCursor` → `submitLoopCommand(ctx)`（replica/progress.go:9 → pooled_loop_driver.go:54-78）**同步等待**副本 loop 回执；ctx 是 `context.Background()`，await 只受 `done`/`stop` 影响（pooled_loop_driver.go:101-128），没有超时。
  3. 触发序列：频道 A 的 replica loop 积压（mailbox 2048 深，或正在执行慢 follower apply effect）→ 领导者处理 follower F 对 lane L 的 poll 时，`apply(A)` 在 `session.mu` 内阻塞任意久 → 同一 (peer,lane) 上**所有其他频道**的 `MarkDataReady` / `MarkHWOnlyReady`（来自 `onChannelReady`，跑在 HW-advance 通知的 timer goroutine / append 通知 goroutine / 调度 worker 上）全部卡在 `s.mu.Lock()` → 这些频道的复制唤醒停摆，且阻塞的是本进程关键路径 goroutine。
  4. 同类问题：`selectLaneReadyItem` → `ch.replica.Fetch(ctx, ...)`（longpoll.go:208）也在 `s.mu` 内执行，但它传的是 RPC ctx（有外部 deadline）；唯独 cursor apply 显式丢弃了取消能力。
- **后果**：单频道副本 loop 积压 → 同 lane（LaneCount=8，平均 1/8 频道）的复制进度标记与 HW 唤醒级联冻结；`context.Background()` 使 runtime 层完全失去取消点。
- **建议**：cursor apply 移出 `session.Poll` 的临界区（先在锁内快照、锁外应用），并把上游 RPC ctx 传入 `ApplyFollowerCursor`。

### [P1] 3. 入站 lane 响应路径的激活/应用没有任何 deadline，且 lane 卡死后没有任何重试定时器兜底

- **位置**：`pkg/channel/runtime/backpressure.go:804-859`（关键 828、875、853-858）
- **类别**：context / 并发
- **代码**：
  ```go
  for _, item := range resp.Items {
      ch, ok := r.lookupChannel(item.ChannelKey)
      if !ok {
          var err error
          ch, _, err = r.ensureChannelForIngress(context.Background(), item.ChannelKey, ActivationSourceLaneOpen)
          ...
      }
      ...
      _ = r.applyFetchResponseEnvelope(ch, peer, fetchResp)
  }
  if !reissue {
      return
  }
  r.reissueLanePoll(peer, resp.LaneID)
  ```
  ```go
  if err := ch.replica.ApplyFetch(context.Background(), core.ReplicaApplyFetchRequest{...}); err != nil {
  ```
- **触发路径**：
  1. goroutine 模型（已核实）：lane 响应在 `dispatchLanePollAsync` 每次发车派生的一条独立 goroutine 上同步处理（lane_dispatcher.go:160-167 → transport/session.go:129-183 同步 deliver）。因此**不是**共享 per-peer 派发 goroutine，单个慢频道不直接卡其他 peer——按协调者提示的判据，主触发面降为 lane 级。
  2. 但 lane 级停摆是真实且不可恢复的：`ApplyResponse` 在处理 items **之前**把 lane 标为非 inflight（823 行），而 `reissueLanePoll` 在**全部 items 应用完之后**才执行（855-858 行）。如果 828 行的 `ensureChannelForIngress` 阻塞（其内部 `Sync.ActivateByKey` → singleflight → 权威 metadb 读取，resolver.go:225-275，runtime 传入的 ctx 无 deadline，取消完全依赖下游实现），该 goroutine 挂起 → `reissueLanePoll` 永不执行。
  3. 关键点：lane 停摆后**没有任何兜底定时器**。`scheduleLaneRetry` 只在发送失败时触发（lane_dispatcher.go:190-191）；响应处理中途挂起不算发送失败。也没有任何东西重新 `scheduleLaneDispatch`（除非该 lane 上某个频道恰好发生 meta 变更/append 事件）。结果：挂在该 lane 上的所有频道（约 1/8）复制无限期停止，同时泄漏一条 park 的 goroutine。
  4. 875 行 `ApplyFetch(context.Background())` 同理：等副本 loop 回执期间无取消点；副本 loop 被 `Close` 时靠 `done` 退出，但 loop 活着却积压时只能干等。
- **后果**：激活路径挂起（如权威存储抖动且下游无内部超时）→ 每 (peer,lane) 复制静默停摆、goroutine 泄漏；无自愈路径。
- **建议**：给入站激活/应用传带 deadline 的 ctx（哪怕复用 RPC 超时常量），并在 lane 响应处理失败/超时时也走 `scheduleLaneRetry` 兜底。

### [P2] 4. EnsureChannel 持 shard 写锁跨副本构造与恢复磁盘 I/O——冷频道激活卡住同 shard 全部频道的查找热路径

- **位置**：`pkg/channel/runtime/runtime.go:142-209`（关键 144、161、170、208-209）；佐证 `pkg/channel/replica/replica.go:203`
- **类别**：并发 / 性能
- **代码**：
  ```go
  func (r *runtime) EnsureChannel(meta core.Meta) error {
      shard := r.shardFor(meta.Key)
      shard.mu.Lock()
      ...
      generation, err := r.allocateGeneration(meta.Key)
      ...
      rep, err := r.replicaFactory.New(ChannelConfig{...})
      ...
      shard.channels[meta.Key] = ch
      shard.mu.Unlock()
  ```
- **触发路径**：`replicaFactory.New` → `NewReplica` → `r.recoverFromStores()`（replica.go:203）——加载 Checkpoint、Epoch History、RetentionState 并扫描 message 表恢复 LEO，全部是 Pebble 磁盘读。整个恢复过程持有该 shard 的 `sync.RWMutex` 写锁。触发：follower 收到冷频道首条 lane 响应（backpressure.go:828 激活路径）或 `ServeFetch` ingress 激活（backpressure.go:1033 → activator → `EnsureLocalRuntime` → `EnsureChannel`）→ 该 shard（FNV 64 分片，约 1/64 频道）的所有 `lookupChannel` / `ApplyMeta` / `RemoveChannel` 在磁盘恢复期间阻塞；`ServeFetch` 本身就运行在对端 RPC 服务 goroutine 上，反过来放大复制延迟。批量冷频道涌入（如流量洪峰后的首次访问）时按 shard 聚集成簇阻塞。
- **后果**：激活磁盘 I/O 延迟直接转化为同 shard 频道的读路径抖动；无 panic/数据问题，故 P2。
- **建议**：先在锁外完成 generation 分配与副本构造（key 尚未入 map，无并发冲突），只在登记 `shard.channels[key]` 时短暂持锁。

### [P2] 5. RemoveChannel 在同时持有全局 sendCoordMu 和 shard.mu 的情况下阻塞等待副本 loop 执行 Tombstone

- **位置**：`pkg/channel/runtime/runtime.go:223-246`
- **类别**：并发
- **代码**：
  ```go
  func (r *runtime) RemoveChannel(key core.ChannelKey) error {
      shard := r.shardFor(key)
      r.sendCoordMu.Lock()
      shard.mu.Lock()
      ch, ok := shard.channels[key]
      ...
      if err := ch.replica.Tombstone(); err != nil {
          shard.mu.Unlock()
          r.sendCoordMu.Unlock()
          return err
      }
  ```
- **触发路径**：`Tombstone()` 是一次同步 loop-command 提交并等待回执（与发现 2/1 同一等待语义）。触发：删除/迁移清理频道（`ApplyMeta(Status=Deleted)` → `RemoveChannel`，channel.go:245-246；idle eviction 也走它）时，若目标副本 loop 积压，则全局 `sendCoordMu` + 该 shard 写锁被同时持有直到 loop 轮到这条命令——与发现 1 叠加：此时所有 lane poll 发送在 `sendCoordMu` 上排队，同 shard 查找在 `shard.mu` 上排队。对比同包 `commit_notify.go` / `runtime.go:248-257`（timer 清理与 `replica.Close()` 都刻意放在解锁之后）可见这是被遗漏的一处。
- **后果**：频道删除的代价被放大为全局发送停顿窗口；删除风暴（批量下线频道）时最明显。
- **建议**：`Tombstone()` 移到两把锁之外（先持锁取引用、标记移除并加墓碑，再解锁执行副本状态转换）。

### [P2] 6. 稳态 long-poll follower 永不刷新 lastActiveAt——开启 idle eviction 后会周期性驱逐"正在健康复制"的频道；lane 应用路径完全绕过 beginUse 驱逐护栏

- **位置**：`pkg/channel/runtime/backpressure.go:824-853`（无 touch/beginUse）+ `pkg/channel/runtime/channel.go:167-197` + `pkg/channel/runtime/runtime.go:663-708`
- **类别**：正确性 / 资源
- **代码**：
  ```go
  // handleLanePollResponse：lookupChannel 命中分支没有 touch()，也没有 beginUse
  for _, item := range resp.Items {
      ch, ok := r.lookupChannel(item.ChannelKey)
      if !ok {
          ... ensureChannelForIngress ... // 只有 miss 分支才 touch
      }
      ...
      _ = r.applyFetchResponseEnvelope(ch, peer, fetchResp)
  ```
  ```go
  func (c *channel) tryMarkIdleEvicting(cutoff time.Time) bool {
      ...
      if c.evicting || c.inUse > 0 { return false }
      if c.lastActiveAt.After(cutoff) { return false }
      if c.pending != 0 || c.snapshotBytes > 0 || c.replicationPeers.pending() > 0 { return false }
      c.evicting = true
  ```
- **触发路径**：
  1. FLOW.md 明言 lane long-poll 是"当前唯一复制主路径"。但 `handleLanePollResponse` 对命中的 channel **既不 touch 也不 beginUse**；`lastActiveAt` 只有 `beginUse`（本地 Append/Fetch）、`touch`（fetch/probe 响应处理 backpressure.go:664）会刷新。
  2. 触发：一个纯 follower、无本地客户端读写的频道，稳态下只通过 lane poll 复制（空响应/数据响应都不经过 touch/beginUse）→ `lastActiveAt` 停留在激活时刻 → `WK_CLUSTER_CHANNEL_IDLE_TIMEOUT > 0` 时，idle 扫描判定其空闲。`channelHasRuntimeWork` 只查 scheduler 队列、peerRequests、snapshot waiter——一条**正在 leader 侧 park 着的 lane poll** 在这些结构里完全不可见 → 频道被 Tombstone + `replica.Close()` + 释放 MaxChannels 配额。
  3. 之后 leader 下一条 lane 响应 miss → 重新激活（generation +1、副本重建、membership 重同步）→ 再次空闲 → 再次驱逐：每个追平的 follower 频道以 IdleTimeout 为周期做 tombstone/重建循环。这与 FLOW.md 的"卸载前会避开正在……复制……运行时工作"（runtime 架构要点 Idle Eviction 节）直接矛盾，构成架构/文档分歧。
  4. 同根问题的另一面：正因为该路径不 `beginUse`，`tryMarkIdleEvicting` 与 `applyFetchResponseEnvelope` 之间存在窗口——驱逐侧先 `Tombstone()` 再 `Close`，应用侧随后对已 tombstone/关闭的副本调 `ApplyFetch`，错误被 `_ =`（853 行）静默吞掉。因 leader HW 未变、follower cursor 未推进，下一条 poll 会重取，能自愈，但丢失了一次已接收的数据应用并制造无谓的重传。
- **后果**：开启 idle eviction（非默认）后：追平频道被周期性驱逐重建（generation 膨胀、replica 生命周期抖动、MaxChannels 配额震荡）；伴生 tombstone 竞态下的静默丢应用。默认配置下不触发，故 P2。
- **建议**：lane 响应命中路径加 `touch()`（并在应用期间 `beginUse`），或把"lane inflight"计入 `channelHasRuntimeWork`。

### [P2] 7. peerReferencedByAnyChannel 在全局 sendCoordMu 下对全部频道做全量扫描——每次使 peer 失效的 meta 变更都付 O(频道总数) 代价

- **位置**：`pkg/channel/runtime/runtime.go:406-452`（关键 435-452）；调用点 `runtime.go:245`（RemoveChannel）、`runtime.go:294`（ApplyMeta，持锁区间 286-295）
- **类别**：性能
- **代码**：
  ```go
  func (r *runtime) peerReferencedByAnyChannel(peer core.NodeID) bool {
      ...
      for i := range r.shards {
          shard := &r.shards[i]
          shard.mu.RLock()
          for _, ch := range shard.channels {
              meta := ch.metaSnapshot()
              if r.isReplicationPeerValid(meta, peer) {
                  shard.mu.RUnlock()
                  return true
              }
          }
          shard.mu.RUnlock()
      }
      return false
  }
  ```
- **触发路径**：`ApplyMeta` 在持有全局 `sendCoordMu`（286 行加锁）后调用 `evictInvalidPeerSessions(invalidatedPeers)` → 对每个失效 peer 调 `peerReferencedByAnyChannel` → 遍历 64 个 shard 的全部 channel，对每个取一次 `metaSnapshot()`（atomic load + Meta 结构拷贝，Meta 含多个 slice 字段）。触发：leader 变更/成员变更使某 peer 失效 → 单次 meta 应用付出 O(全节点频道数) 的扫描 + 每频道一次 Meta 深拷贝，全程压住全局发送协调锁。`RemoveChannel` 路径同理（245 行）。频道规模 10 万级时单次扫描毫秒到十毫秒级，且发生在本已是 P1 的全局锁临界区内。
- **后果**：meta 变更热点期（如批量 leader 迁移）放大发现 1 的全局发送停顿；属于"持全局锁做全 map 遍历"的教科书条目。
- **建议**：在 shard map 之外维护 per-peer 引用计数（或倒排索引）回答"该 peer 是否仍被引用"，扫描只在兜底路径保留。
- **附带（同根，量级较小）**：idle eviction 扫描对每个候选频道调 `channelHasRuntimeWork` → `peerRequests.hasChannelWork`（backpressure.go:99-113）线性遍历全部 groups + 全部队列，整体 O(频道数 × groups)，仅在开启 idle eviction 时触发。

### [P3] 8. FLOW.md 宣称的"三级背压"中软背压（BackpressureSoft）完全未实现

- **位置**：`pkg/channel/runtime/types.go:45-48` + `pkg/channel/runtime/backpressure.go:375`；分歧点 `pkg/channel/FLOW.md` 运行时架构要点节
- **类别**：架构/文档
- **代码**：
  ```go
  const (
      BackpressureNone BackpressureLevel = iota
      BackpressureSoft
      BackpressureHard
  )
  ```
  ```go
  if state := session.Backpressure(); state.Level == BackpressureHard {
      r.peerRequests.enqueue(env)
      r.scheduleBackpressureRetry(env.Peer)
      return ErrBackpressured
  }
  ```
- **触发路径**：FLOW.md 写"三级背压：无背压(立即发送) / 软(批量合并) / 硬(排队+重试调度)"。全仓 grep（非测试）`BackpressureSoft` 仅命中定义处；运行时只判 `BackpressureHard`，`state.Level == BackpressureSoft` 时与 None 走完全相同的立即发送路径，"批量合并"分支不存在。任何 transport 把 peer 置为 Soft 级都得不到文档承诺的行为。
- **后果**：文档与实现不一致；Soft 级是死枚举，后续接入方按文档理解会得到错误预期。
- **建议**：要么实现 Soft 级合并，要么修 FLOW.md/枚举为两级。

### [P3] 9. errNotImplemented 死代码（staticcheck U1000）

- **位置**：`pkg/channel/errors.go:31`
- **类别**：死代码
- **代码**：
  ```go
  errNotImplemented = errors.New("channel: not implemented")
  ```
- **触发路径**：无——已按对抗性复核要求 grep 全部引用：包内唯一语义相近点是 `replica/durable_store.go:336` 的"Compatibility-only fallback for stores that have not implemented ApplyFetchStore"，但那是**有真实实现**的回退分支（AppendLeaderBatch + checkpoint），并非应当返回 not-implemented 而静默成功的路径。不存在"应报未实现却静默成功"的缺口。
- **后果**：纯死代码。
- **建议**：删除。

### [P3] 10. durable_codec.go 常量组 SA9004——已验证无线上编码宽度风险

- **位置**：`pkg/channel/durable_codec.go:7-8`
- **类别**：正确性（轻微）/ 可读性
- **代码**：
  ```go
  DurableMessageCodecVersion byte = 1
  DurableMessageHeaderSize        = 45
  ```
- **触发路径**：无实际触发。针对协调者提示的"序列化上下文中无类型常量默认 int 可能改变编码宽度"做了专项核查：`DurableMessageHeaderSize` 的全部 6 处使用（handler/codec.go:73,76,93,164；pkg/db/message/compat.go:1288,1309,1355）均为 `len(payload) < N` 比较与 `pos := N`（int 局部变量），不参与任何 `append`/binary 写入，Go 无类型常量按上下文定值，不存在宽度漂移。真正上线的编码字节是 `DurableMessageCodecVersion`（显式 byte），写死为 1。
- **后果**：仅 staticcheck 噪音与可读性。
- **建议**：补显式 `int` 类型。

### [P3] 11. 每次 leader 本地 append 派生一个 goroutine 做 commit 唤醒

- **位置**：`pkg/channel/runtime/runtime.go:183-194`
- **类别**：性能
- **代码**：
  ```go
  if notifier, ok := rep.(interface{ SetLeaderLocalAppendNotifier(func()) }); ok {
      notifier.SetLeaderLocalAppendNotifier(func() {
          go r.onChannelAppend(meta.Key)
      })
  }
  ```
- **触发路径**：`onChannelAppend`（replicator.go:210-213）只做 `cancelPendingChannelCommit` + `onChannelReady`（查 map、置 ready 标记），耗时微秒级，与 append 本身的落盘/quorum 开销相比可忽略；但它是**每条消息**一次的 goroutine 创建/销毁。`LongPollHWOnlyNotifyDelay` 的 5ms 合并只覆盖 commit-only 路径（`scheduleChannelCommit`），data-ready 唤醒没有合并（虽有 `LongPollDataNotifyDelay` 在 lane_session 内合并 wake，但 goroutine 本身每 append 都建）。
- **后果**：高写入吞吐下纯 goroutine churn（调度压力与 GC 栈分配）；不构成正确性问题。
- **建议**：改为投递到轻量事件队列/复用 lane dispatcher 的唤醒路径。

## 已排除的候选项

- `pkg/channel/runtime/backpressure.go:613-619`（Sync 信封同步回注）—— 看似会在持 `sendCoordMu` 的 RPC 回注里重入加锁造成自死锁，但因为 `handleEnvelope(Sync)` 内所有发送都经 `syncDeliveryActive()` 检查走 `deferSyncEnvelope`/`deferPeerDrain`（684-685、513-522），`drainDeferredSyncWorkLocked` 用的是不重新加锁的 `sendEnvelopeLocked`（482 行），重入是安全的。这正是 defer 机制存在的原因。
- `pkg/channel/runtime/backpressure.go:632-663`（channel==nil 时 inflight 泄漏嫌疑）—— 频道被删时 `RemoveChannel` 先 `tombstones.add`（带该 gen）再 `peerRequests.clearChannel` 释放全部该频道 inflight；响应迟到时 `tombstones.contains(env.ChannelKey, env.Generation)` 命中 knownDrop → 释放。唯一漏网需要响应迟到超过墓碑 TTL（1 分钟，build.go:357-359 附近配置），而 RPC 超时仅 1s，不可达。
- `pkg/channel/runtime/backpressure.go:218-221`（releaseInflightForEnvelope 的 generation 容忍逻辑）—— reservation.generation 与 env.Generation 均 0 时放行是刻意的（重试信封可能无 generation），requestID 匹配已足够防误释放。
- `pkg/channel/runtime/backpressure.go:472-490`（drainDeferredSyncWorkLocked 循环内发新请求）—— 看似乒乓 livelock：deferred fetch 发出 → 同步响应 → 又 defer 新 fetch。但每次 ApplyFetch 都推进 follower LEO，循环受数据收敛约束必然终止，且每轮有真实网络往返与超时，非忙死循环。
- `pkg/channel/runtime/lane_session.go:234-237`（第二个并发 Poll 覆盖 parked waiter）—— 覆盖后旧 waiter 只能等 RPC deadline 返回 TimedOut；但 dispatcher 对同一 (peer,lane) 做 single-flight（lane_dispatcher.go:41-55 processing/queued 去重），正常运行不可能并发 Poll 同一 session。
- `pkg/channel/runtime/lanes.go:217-221`（ApplyResponse default 分支把 pending 清零）—— 疑似 StaleMeta/NotLeader 状态会让 follower lane 永久静默。核查后：当前全部生产代码只产生 OK / NeedReset / Closed（longpoll.go:41,73,152；transport 不合成其他状态），StaleMeta/NotLeader 是无生产者的防御性枚举；且 meta 变更会经 `syncFollowerLaneMembership` 重新置 pending。列为防御代码缺口而非 bug，若未来接入这些状态需配 retry。
- `pkg/channel/runtime/backpressure.go:242-249`（peerRequests.queued 的 per-peer 空队列对象不删除）—— popQueued 只弹元素不删 map 项，条目按"曾发生背压的 peer 数"上界，集群节点数级别，单个空 struct 指针，可忽略。
- `pkg/channel/runtime/session.go:31-44`（runtime 侧 peerSessionCache 与 transport sessionManager 双重缓存）—— 缓存值是 3 字段小结构，上界为对端节点数；仅在 peer 永久下线且从未出现在任何 meta 里时才可能残留，量级可忽略。
- `pkg/channel/runtime/runtime.go:717-767`（Close 不 join dispatchLanePollAsync / handleEnvelope 在途 goroutine）—— 在途 goroutine 的后续动作全部有 `isClosed()` 守卫或落在已关闭副本上返回错误（submitCommand 的 closed/stop 分支），且被 RPC 超时（≤ maxWait+rpcTimeout）有界；`laneDispatcher.finish` 对替换后的新队列返回 false，不会复活 worker。有界、无害。
- `pkg/channel/runtime/runtime.go:530-546`（applyReplicaMeta 吞 ErrLeaseExpired 返回 nil）—— 与 FLOW.md 5.4 节④"Runtime 触发 reconcile"语义一致：lease 过期由权威续租后再应用，本地保持 leader 身份但 append 会被 `appendableLocked` 拒绝，不是静默错误。
- `pkg/channel/runtime/tombstone.go`、`commit_notify.go`、`lane_dispatcher.go`、`scheduler.go` 的 timer/map 生命周期 —— 逐一对过：所有 timer 有 version 防误 stop、map 条目在 channel 移除（RemoveChannel → clearReplicationRetries/clearPendingChannelCommit/clearChannel）与 Close 全量清理；tombstoneManager.dropExpired 同步清理空 key。与侦察结论一致，确为干净参照。
- `pkg/channel/channel.go:257-281`（lockApplyMeta 引用计数锁）—— refs 递减与 delete 全程在 applyMu 内，无竞态；`ApplyMeta` 失败回滚走 `RestoreMeta`，符合 FLOW.md 避坑清单描述。
- gosec G115 × 5（lanes.go:53,68,303、longpoll.go:43）—— `LongPollLaneCount` 来自本地配置（默认 8，config.go:1162-1166 无远端输入），`hasher.Sum32() % uint32(laneCount)` 结果 ≤ laneCount，转 uint16 仅在配置 > 65535 时截断且 leader/follower 两侧一致截断，比较逻辑（longpoll.go:43）两侧同样截断，不产生行为分叉。误报。
- `pkg/channel/runtime/backpressure.go:875 / longpoll.go:336 / 1021` 的 `context.Background()` 中，除已立为发现的 2、3 号外，`applyReconcileProbeResponseEnvelope`（1021）的阻塞后果已并入发现 1（持全局锁）单独计数，不重复立项。

## 本分片整体评价

这部分 runtime 代码的**微结构质量高于平均水平**：timer version 防陈旧唤醒、single-flight dispatcher、引用计数 per-key 锁、墓碑 gen 匹配等都做得严谨，侦察给出的两个参照（runtime.go 关停、commit_notify 合并）经核实确实干净。但它的**全局协调层有一致的系统性弱点**：一把全局 `sendCoordMu` 被反复用于包裹阻塞操作（同步 RPC、副本 loop 回环、Tombstone），加上三处 `context.Background()` 恰好都落在复制的关键回环上，使"单个慢副本/慢 peer 拖慢全节点复制"成为跨发现 1/2/3 的共同根因——这是本分片最需要优先处理的问题，且三者可用同一方向修复（锁只保护记账、取消能力全程透传）。生命周期侧的隐患（shard 锁跨磁盘 I/O、idle eviction 看不见 lane 活动）目前都有配置或默认路径护着，量级 P2。本分片全部为 v1 生产运行路径代码（`internal/app` 直接组装），严重度按实际计。
