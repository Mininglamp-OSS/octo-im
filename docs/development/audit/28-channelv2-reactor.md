# 28-channelv2-reactor — channelv2 多 Reactor 运行时审计报告

范围：`pkg/channelv2/reactor/`（20 个非测试 `.go` 文件，共 4848 行；11 个测试文件仅用于交叉验证意图，不作为审计对象）。已先完整阅读 `pkg/channelv2/FLOW.md`（136 行）。

## 覆盖情况

| 文件 | 行数 | 已读 |
|---|---|---|
| reactor.go | 825 | 是（全文，含事件循环 loop()/handle() 全部分支） |
| replication_runtime.go | 668 | 是 |
| effect.go | 572 | 是 |
| lifecycle.go | 499 | 是 |
| group.go | 343 | 是 |
| scheduler.go | 315 | 是 |
| append_queue.go | 252 | 是 |
| lifecycle_model.go | 204 | 是 |
| replication_state.go | 198 | 是 |
| record_cache.go | 174 | 是 |
| mailbox.go | 146 | 是 |
| metrics.go | 123 | 是 |
| lifecycle_follower.go | 96 | 是 |
| lifecycle_apply.go | 95 | 是 |
| lifecycle_leader.go | 93 | 是 |
| future.go | 83 | 是 |
| runtime_snapshot.go | 73 | 是 |
| event.go | 56 | 是 |
| router.go | 27 | 是 |
| backpressure.go | 6 | 是 |

合计 4848 行，与 WORKER_BRIEF 估计一致；全部 20 个非测试文件均已完整阅读（无摘录/跳读）。另交叉阅读了 `pkg/channelv2/worker/{pool.go,pools.go,task.go}`、`pkg/channelv2/transport/local.go`、`pkg/channelv2/service/append.go`、`pkg/clusterv2/channels/transport.go`、`pkg/clusterv2/net/transport.go` 作为上下文（不在本分片路径内，未在这些文件中落发现）。

## 发现

### [P1]（因 v2 未上生产路径降级，逻辑严重度对应生产环境的 P0/P1）reactor 发起的所有出站 RPC/checkpoint/append-flush 任务始终使用 `context.Background()`，且链路上任何一层都不施加超时，可在共享的、按 Group 全局唯一的 worker 池上无限占用工作线程

- **位置**：
  - `pkg/channelv2/reactor/effect.go:480`（`tryFlushAppend` 提交 `TaskStoreAppend`）
  - `pkg/channelv2/reactor/lifecycle_apply.go:84`（`startLeaderCheckpoint` 提交 `TaskStoreCheckpoint`）
  - `pkg/channelv2/reactor/lifecycle.go:430`（`trySubmitPullHint` 提交 `TaskRPCPullHint`）
  - `pkg/channelv2/reactor/replication_runtime.go:64,89,122,159`（`trySubmitPull`/`trySubmitPendingApply`/`submitAckPayload`/`trySubmitStopCheckpoint` 提交 `TaskRPCPull`/`TaskStoreApply`/`TaskRPCAck`/`TaskStoreCheckpoint`）
- **类别**：(b) context 传播 / (d) 资源无界 / (h) 分布式正确性
- **代码**（effect.go:475-483）：
  ```go
  task := decision.Tasks[0]
  batch.fence = task.Fence
  batch.records = task.StoreAppend.Records
  if err := r.submitStoreAppend(context.Background(), batch.requests[0].req.ChannelID, task); err != nil {
      rc.appendInflight = nil
      r.failAppendBatch(rc, batch, err)
      return
  }
  ```
  以及 replication_runtime.go:60-66：
  ```go
  func (r *Reactor) trySubmitPull(rc *runtimeChannel, now time.Time) bool {
      ...
      if err := r.submitRPCPull(context.Background(), rc.state.Leader, fence, req); err != nil {
  ```
- **触发路径**：
  1. `worker.Pool.run()`（`pkg/channelv2/worker/pool.go:125-137`）执行 `task.Run(p.ctx, p.deps)`，`p.ctx` 是 Pool 自身在 `NewPool` 时创建的长生命周期 ctx（只在 `Pool.Close()` 时取消），不是任何单次提交的 ctx。
  2. `Task.Run` 内的 `taskContext(parent, task)`（`pkg/channelv2/worker/task.go:142-165`）只有在 `task.Done() != nil` 时才会桥接一个可取消的子 ctx；由于本分片所有上述 7 处提交点把 `Task.Context` 字段也设为同一个 `context.Background()`（`Done()==nil`），桥接直接跳过，worker 实际执行时拿到的是一个既无 deadline、也无法被任何上游取消的裸 ctx。
  3. 下游 `transport.Client` 实现（生产路径 `pkg/clusterv2/channels/transport.go:28-68` 的 `TransportClient.Pull/Ack/PullHint/Notify`，经 `pkg/clusterv2/net/transport.go:87-89` 的 `Call` 直接把同一个 ctx 转发给 `c.client.RPCService(ctx, ...)`）在这一层同样不施加任何独立超时；测试用的 `pkg/channelv2/transport/local.go` 同样直接转发 ctx 给 `server.HandlePull` 等。也就是说从 reactor 发起点到最终网络调用，**整条链路没有任何一层会给这些 RPC/落盘调用加超时**。
  4. Worker 池是 **按 Group 全局共享一份**，而不是每个 Reactor 各自一份（`group.go:32-46` 的 `NewPools`；`defaultWorkerPools`，`group.go:321-329`，RPC 池 worker 数固定为 `max(1, cfg.ReactorCount)`）。因此：只要某一个 follower 的对端 leader 节点网络分区/进程假死（`Pull`/`Ack` RPC 永久阻塞不返回，也不会因 ctx 取消而中止），该 RPC 就会永久占用 `channelv2-rpc` 池的一个 worker 槽位。当同时卡住的 RPC 数达到 `ReactorCount`（池的全部 worker 数）时，**该节点上所有其它 channel（哪怕对端完全健康）** 的 Pull/Ack/PullHint 全部会因为 `Pool.Submit` 的非阻塞 `select` 落入 `default: return ch.ErrBackpressured` 分支而被拒绝，造成与故障对端无关的 channel 复制全面停滞，且没有任何机制能让已经卡住的 worker 恢复（没有 ctx 超时，只能等 TCP 层最终超时或进程重启）。
- **后果**：单个慢/分区的对端节点可以耗尽本节点共享的 RPC/StoreApply/StoreCheckpoint/StoreAppend 池，波及所有无关 channel 的复制与写入前进，属于典型的“级联饱和”故障模式；且完全没有超时兜底（v1 对照锚点 `pkg/channel/runtime/backpressure.go:828,875` 至少还有讨论价值，v2 在检查范围内的这 7 处则是彻底没有任何超时/取消手段），这是同类缺陷在 v2 中被**原样重复**、且目前看不到比 v1 更好的处置。
- **建议**：为这 7 处提交点改用带超时的派生 ctx（例如每个任务类型各自一个可配置的 RPC/IO deadline），并保证 `Task.Context` 字段带有非空 `Done()`，让 `taskContext` 的桥接机制真正生效；同时评估是否需要 per-peer 或 per-channel 的池隔离，避免单一慢节点拖垫全局共享池。
- **v2 生产可达性说明**：`pkg/channelv2` 仅可通过未接入任何生产二进制的 `internalv2` 到达（`cmd/wukongim/main.go` 只构建 `internal/app`），AGENTS.md 标注为"实验性"。**不在生产运行路径上**，故本发现从其逻辑严重度（生产环境下应为 P0，因为可造成全局复制停滞且无自愈手段）下调一级标为 P1。

### [P3] `Group.Submit` 中局部变量 `ctx` 被赋值后立即丢弃（SA4006/SA4017），但不构成实际取消传播缺陷

- **位置**：`pkg/channelv2/reactor/group.go:203`
- **类别**：(b) context 传播 / 死代码
- **代码**：
  ```go
  func (g *Group) Submit(ctx context.Context, key ch.ChannelKey, event Event) (*Future, error) {
      if g == nil || g.closed.Load() {
          return nil, ch.ErrClosed
      }
      if ctx == nil {
          ctx = context.Background()
      }
      future := event.Future
      ...
  ```
  `ctx`（含默认化后的值）之后在函数体内再未被读取——`Event.Context` 是独立字段，真正的取消语义完全靠调用方自己传入的 `event.Context` 和调用方自己对 `future.Await(ctx)`/`ctx.Done()` 的处理（见 `pkg/channelv2/service/append.go:47-70`：`AppendBatch` 把同一个 `ctx` 设成 `event.Context`，并自行 `select { case <-future.Done(): ...; case <-ctx.Done(): ... }`）。
- **触发路径**：无法构造——`Submit` 的 `ctx` 形参从未被用来做任何判断或传递，删除它不会改变任何调用方观察到的行为。已排查全部 3 个调用点（`service/append.go:47`、`service/replication.go:14`、`group.go:231`），均已各自正确处理取消。
- **后果**：仅是 API 上误导性的死参数（读者可能误以为 `Submit` 会用它做超时/取消判断），无实际功能性缺陷。
- **建议**：移除该参数或在文档中说明其仅用于向后兼容/未来用途；不建议标为高优先级。
- **说明**：不在生产运行路径上；即使在语义上也非缺陷，故保持 P3。

### [P3] `(*replicationState).markDirty` 的 `now time.Time` 形参在函数体内被读取后即丢弃（SA4006/SA4017），核实后确认不隐藏任何被丢弃的时效性/租约检查

- **位置**：`pkg/channelv2/reactor/replication_state.go:87`（函数体：85-92 行）
- **类别**：死代码/参数设计
- **代码**：
  ```go
  func (s *replicationState) markDirty(now time.Time) {
      if now.IsZero() {
          now = time.Now()
      }
      s.dirty = true
      s.parked = false
      s.nextPullAt = time.Time{}
      s.nextPullAfter = 0
  }
  ```
- **核实过程**：按 WORKER_BRIEF 的具体要求，逐一检查了全部 6 个调用点（`reactor.go:346`、`reactor.go:439`、`replication_runtime.go:206,240,254,289,359`）以及 `replicationState` 结构体全部字段（`replication_state.go:11-80`）。结构体中**不存在**任何 `lastDirtyAt`/`staleSince`/租约类字段——`dirty`（bool）、`parked`（bool）、`nextPullAt`（会被清零）、`nextPullAfter`（会被清零）是 `markDirty` 真正影响的全部状态，均与"当前时间点"无关，只是"立即变为可调度"的布尔/时间清零标记。`.dirty` 唯一的读取点在 `scheduler.go:242`（`if replication.dirty { ... }`，触发立即重新调度）和 `replication_runtime.go:253`（陈旧性判定的其中一个 OR 条件，逻辑上不需要具体时刻，只需要布尔值）。因此这不是"丢弃了应记录的时效性检查"，而是形参本身就是历史遗留、从未真正被消费——**结论：不构成 WORKER_BRIEF 担忧的正确性缺口，只是死参数**。
- **触发路径**：无法构造出因该丢弃导致的错误调度或过期数据被接受的场景。
- **后果**：无功能性后果，仅代码整洁性问题。
- **建议**：移除 `now` 形参并把所有调用点简化为 `markDirty()`，消除误导性签名。

### [P3] 两个 U1000 死函数已确认与线上逻辑重复、并非被弱化的门控

- **位置**：
  - `pkg/channelv2/reactor/lifecycle.go:232`（`(*Reactor).allFollowersCaughtUp`）
  - `pkg/channelv2/reactor/replication_runtime.go:655`（`(*runtimeChannel).canAcceptFollowerStop`）
- **类别**：死代码
- **代码**：
  ```go
  // lifecycle.go:232
  func (r *Reactor) allFollowersCaughtUp(rc *runtimeChannel) bool {
      if rc == nil {
          return false
      }
      r.syncLeaderFollowers(rc)
      return runtimeViewFromChannel(rc, time.Now(), AppendFenceView{}).AllFollowersCaughtUp()
  }
  ```
  ```go
  // replication_runtime.go:655
  func (rc *runtimeChannel) canAcceptFollowerStop() bool {
      if rc == nil {
          return false
      }
      replication := rc.replication
      return !replication.pullInflight &&
          !replication.ackInflight &&
          !replication.pendingAck &&
          replication.pendingPull == nil &&
          !replication.applyBlocked &&
          replication.applyOpID == 0 &&
          !replication.stopping &&
          !replication.checkpointInflight
  }
  ```
- **核实过程**：`grep -rn` 全包（含测试文件）确认两者均**零调用点**。逐一比对了它们各自的"活的等价逻辑"：
  - `allFollowersCaughtUp` 的等价逻辑是 `lifecycle_model.go` 中导出的 `RuntimeView.AllFollowersCaughtUp()`，它已经被正确编织进真正被使用的 `RuntimeView.CanOfferFollowerStop()`（`lifecycle_model.go`），并被 `lifecycle_leader.go` 的 `OnLeaderLifecycleEvent` 状态机在 `LeaderServing→StoppingFollowers` 迁移判定中使用。
  - `canAcceptFollowerStop` 的等价逻辑是 `lifecycle_follower.go` 中的 `RuntimeView.followerStopBlocked()`，已被 `OnFollowerLifecycleEvent` 在 `FollowerLifecycleStopOffered` 分支中使用（`!view.followerStopBlocked() && view.LEO >= event.LeaderLEO && view.HW >= event.LeaderHW`）。逐字段比对发现死函数与活函数存在两点细微逻辑差异（死函数多检查了 `!replication.stopping`；`checkpointInflight` 的组成方式也不完全一致：活版本的 `PendingWorkView.CheckpointInflight` 是 `checkpointInflight || checkpointOpID != 0` 的组合，死版本只看裸 bool）。但由于死函数**零调用点**，这两处差异不可能被执行到，因此不能构成"门控被弱化"的实际缺陷——它单纯是重构后遗留、从未被删除的旧实现。
- **触发路径**：无法构造——两函数完全不可达。
- **后果**：无功能性后果，纯代码整洁性问题（`go vet`/`staticcheck` 已标记）。
- **建议**：删除这两个函数。

## 已排除的候选项

- **`router.go:26` gosec G115**（`int(h.Sum64() % uint64(r.count))`）：`r.count` 在 `NewRouter` 中校验 `> 0`，取模结果严格落在 `[0, r.count)`；`r.count` 来自 `Config.ReactorCount`（经 `defaultConfig` 钳制为 ≥1 的合理小整数，典型值为个位数到几十），不存在远端输入驱动的无界放大路径。**误报**，排除。
- **`record_cache.go:89-90` gosec G115**（`start := int(from - c.baseOffset)`；`out := make([]ch.Record, 0, int(maxOffset-from)+1)`）：两处转换都发生在 `slice()` 内部，调用前已经过 `c.covers(from)` 校验（保证 `from >= c.baseOffset && from <= c.lastOffset()`，故 `from - c.baseOffset` 非负）以及 `maxOffset` 的钳制/校验（`if maxOffset == 0 || maxOffset > last { maxOffset = last }`，且 `if maxOffset < from { return ..., true }` 提前返回），故 `maxOffset - from` 同样非负且上界受缓存自身大小（由 `maxRecords`/`maxBytes` 配置钳制的小窗口）约束，并非远端输入可无界放大的量。**误报**，排除。
- **`group.go:303` gosec G104**（`r.Close()` 未检查返回值）：`(*Reactor).Close()`（`reactor.go:197-201`）的实现是 `r.once.Do(func(){ close(r.stop) }); <-r.done; return nil`——函数体内不存在任何非 nil 返回路径，返回值永远是 `nil`。**误报**（值本身恒为 nil，检查与否不改变任何行为），排除。
- **`context.Background()` 使用于 `reactor.go:495`（`ensureChannel` 里的 `cs.Load(context.Background())`）与 `reactor.go:682`、`group.go:267`、`future.go:74`**：这几处均是"防御性默认值"（nil ctx 兜底）或一次性、非重复的启动期调用（`ensureChannel` 每 channel 仅执行一次，且发生在 mailbox 事件处理的同步路径中，阻塞的是当前 reactor 的单线程 loop 而非共享 worker 池），与 P1 发现中"反复提交到共享池、且完全没有取消手段"的性质不同，未采纳为独立发现。

## 本分片整体评价

FLOW.md 中的 Append Sequence 图（本地/quorum 分支、leader 缓存命中/未命中分支）与 Channel Runtime Lifecycle Model 图（leader 四阶段、follower 三阶段迁移条件）经与 `handleAppend`/`tryFlushAppend`/`OnLeaderLifecycleEvent`/`OnFollowerLifecycleEvent`/`handleRPCPullResult`/`handleFollowerStopControl` 的实际实现逐条核对，**未发现实质性偏离**——文档质量高，可作为后续维护的可靠参照。

针对三个必答问题：（1）**是否重复 v1 缺陷类**：在 apply/commit 错误处理这一维度上，v2 明确**避免**了 v1 类比锚点（`pkg/controller/plane/statemachine.go:617-637` 状态相关错误杀死 Raft apply 循环）的模式——`handleApplyMeta`/`handleStoreAppendResult`/`handleAck` 中的 `decision.Err`/worker 错误全部路由到具体的调用方 `Future`（`event.Future.Complete(Result{Err: ...})`），从不 panic 或 return 出 `loop()`，reactor 的单线程事件循环本身健壮。但在**另一维度**——出站 RPC/checkpoint/append-flush 缺乏超时——v2 原样重复了 v1 的 `context.Background()` 类比缺陷（`pkg/channel/runtime/backpressure.go:828,875`），且由于 worker 池是 Group 级全局共享而非按 reactor 隔离，波及面理论上比 v1 更广（见上文 P1）。（2）**v1 有而 v2 尚未实现的能力**：在本分片 `reactor/` 范围内未见任何 retention/日志保留策略、ISR 式多副本晋升仲裁、或 tombstone/删除标记的处理逻辑——`runtimeChannel`/`replicationState`/lifecycle 状态机只覆盖"单 leader + 若干 follower 的 pull-based 复制 + 幂等 checkpoint 驱逐"，没有看到成员变更（新增/移除 follower）时的目录级协调逻辑（该逻辑若存在应在别的分片如 `machine/`或`store/`中，非本分片可确认范围）。（3）**v2 内部自身死代码**：`allFollowersCaughtUp`、`canAcceptFollowerStop`、`markDirty` 的 `now` 形参、`Group.Submit` 的 `ctx` 形参，均已核实为真正未被消费、且均不掩盖被弱化的安全门控。**再次强调：`pkg/channelv2` 仅通过未接入任何生产二进制的 `internalv2` 可达，是标注为"实验性"的 v0 验证代码**，本报告中的 P1 发现已按此下调一级；若/当该栈被接入生产路径，应在此之前解决超时缺失问题。
