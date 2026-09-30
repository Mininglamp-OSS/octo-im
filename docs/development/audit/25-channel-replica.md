# channel ISR 副本状态机 (pkg/channel/replica)

> 分片路径：`pkg/channel/replica/`（全部非测试 `.go`）。35 个非测试文件 / 7180 行。
> 工具链：`GOTOOLCHAIN=go1.23.4 go vet ./pkg/channel/replica/...` → 0 条。`git status --porcelain` → 无输出（只读审计，未改动任何仓库文件）。

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/channel/replica/progress_pipeline.go | 750 | 是 |
| pkg/channel/replica/durable_store.go | 634 | 是 |
| pkg/channel/replica/append_pipeline.go | 520 | 是 |
| pkg/channel/replica/commands.go | 442 | 是 |
| pkg/channel/replica/lifecycle_pipeline.go | 434 | 是 |
| pkg/channel/replica/follower_apply.go | 398 | 是 |
| pkg/channel/replica/execution_pool.go | 395 | 是 |
| pkg/channel/replica/replica.go | 357 | 是 |
| pkg/channel/replica/reconcile_coordinator.go | 353 | 是 |
| pkg/channel/replica/pooled_loop_driver.go | 342 | 是 |
| pkg/channel/replica/machine.go | 340 | 是（逐函数通读；确认为仅测试可达的 harness，见发现 5） |
| pkg/channel/replica/retention.go | 328 | 是 |
| pkg/channel/replica/snapshot_pipeline.go | 192 | 是 |
| pkg/channel/replica/checkpoint_writer.go | 191 | 是 |
| pkg/channel/replica/meta.go | 157 | 是 |
| pkg/channel/replica/append.go | 135 | 是 |
| pkg/channel/replica/migration.go | 117 | 是 |
| pkg/channel/replica/loop_driver.go | 117 | 是 |
| pkg/channel/replica/fetch_pipeline.go | 109 | 是 |
| pkg/channel/replica/types.go | 108 | 是 |
| pkg/channel/replica/invariant.go | 106 | 是 |
| pkg/channel/replica/epoch_lineage.go | 96 | 是 |
| pkg/channel/replica/promotion_evaluator.go | 82 | 是 |
| pkg/channel/replica/recovery.go | 77 | 是 |
| pkg/channel/replica/durable_lane.go | 59 | 是 |
| pkg/channel/replica/append_pool.go | 51 | 是 |
| pkg/channel/replica/sendtrace_helpers.go | 47 | 是 |
| pkg/channel/replica/promotion_types.go | 42 | 是 |
| pkg/channel/replica/loop.go | 41 | 是 |
| pkg/channel/replica/reconcile.go | 40 | 是 |
| pkg/channel/replica/history.go | 40 | 是 |
| pkg/channel/replica/state.go | 28 | 是 |
| pkg/channel/replica/progress.go | 26 | 是 |
| pkg/channel/replica/replication.go | 13 | 是 |
| pkg/channel/replica/fetch.go | 13 | 是 |
| **合计** | **7180** | |

## 发现

### [P0] 1. 持久化 append 完成事件在 loop mailbox 满时被静默丢弃，导致该 channel 的写入通道**永久性**卡死（且已落盘记录永不发布）

- **位置**：`pkg/channel/replica/append_pipeline.go:243-256`（丢弃错误）；`pkg/channel/replica/pooled_loop_driver.go:228-236`（mailbox 满返回 `ErrNotReady`）；`pkg/channel/replica/append_pipeline.go:71-74` 与 `:108`（卡死点）；`pkg/channel/replica/progress_pipeline.go:429-431`（唯一的清除点）
- **类别**：分布式一致性 / 正确性 / 可用性
- **代码**：
  ```go
  // append_pipeline.go:243 —— 持久化已完成，结果事件回投 loop，错误被丢弃
  doneAt := r.now()
  _ = r.submitLoopResult(context.Background(), machineLeaderAppendCommittedEvent{
      EffectID:   effect.EffectID,
      ...
      BaseOffset: base,
      Err:        err,
  })
  ```
  ```go
  // pooled_loop_driver.go:228 —— mailbox 满时直接失败，无重试、无阻塞
  if d.size >= d.mailboxSize {
      d.pool.observeEnqueue("queue_full")
      return channel.ErrNotReady
  }
  ```
  ```go
  // append_pipeline.go:107 —— 只要 in-flight 没被清掉，后续所有 flush 都被吞掉
  func (r *replica) emitAppendBatchLocked() {
      if len(r.appendInFlightIDs) != 0 || len(r.appendPending) == 0 {
          return
      }
  ```
- **触发路径**：
  1. 生产默认执行模式是 `pooled`（`internal/app/config.go:1129-1131` 把空值补成 `"pooled"`，`internal/app/build.go:295-313` 构造共享 `ExecutionPool`）。
  2. 某个热点 channel 的 leader 发起一批 append，`emitAppendBatchLocked()` 设置 `r.appendInFlightEffectID = effectID` / `r.appendInFlightIDs = activeIDs`，effect 进入共享 `p.append` 队列。
  3. 共享 append worker（`execution_pool.go:152-158`）执行 `runAppendEffect`，`AppendLeaderBatch` **成功落盘**，durable LEO 已推进。
  4. 此刻该 replica 的 pooled mailbox 已被其它事件填满（`d.size >= d.mailboxSize`，默认 2048；或 `p.ready` 已满且 `d.queued==false`）—— 这正是高负载下必然出现的状态，池化调度本身就是为了在过载时排队。
  5. `submitLoopResult` 返回 `ErrNotReady`，被 `_ =` 丢弃。`machineLeaderAppendCommittedEvent` **永远不会**送达 loop。
  6. `takeAppendInFlightResultLocked`（`progress_pipeline.go:392-431`）是**唯一**把 `appendInFlightEffectID`/`appendInFlightIDs` 清零的正常路径；全仓 grep 确认除此之外只有 `failOutstandingAppendWorkLocked`（close / tombstone / BecomeFollower / 快照安装）会清。**没有任何超时看门狗**。
  7. 从此 `len(r.appendInFlightIDs) != 0` 恒真 → `maybeFlushAppendLocked`（`:71-74`）与 `emitAppendBatchLocked`（`:108`）永久早退。
- **后果**：
  - **该 channel 的写入永久中断**：之后所有 `Append()` 都只会堆进 `appendPending`，一直挂到各自 ctx 超时返回 `DeadlineExceeded`，节点不降级、不告警、也不自愈。只有把该 channel 降为 follower、tombstone 或卸载重建才能恢复。
  - **同一 epoch 边界也被永久堵死**：`lifecycle_pipeline.go:139-142` 的 `prepareSameLeaderEpochBoundaryLocked` 检查 `r.appendInFlightEffectID != 0` 就返回 `ErrNotReady`，于是该 channel 的成员变更 / 迁移 cutover（同 leader 抬 epoch）也永久失败；`migration.go:110` 的 drain 判定同理，`FenceAndDrain` 无法收敛。
  - **运行时 LEO 落后于 durable LEO**：步骤 3 的记录已经在日志里，但 `r.state.LEO` 从未推进，调用方收到的是超时错误。进程重启后 `recoverFromStores()` 会把 LEO 抬到 durable LEO，这段"客户端已被告知失败"的尾部变成 provisional tail，reconcile 后可能被提交并投递 —— 产生幽灵/重复消息。
- **建议**：append 完成事件必须保证送达（为结果事件预留 mailbox 配额或改用阻塞/无界的结果通道），并给 in-flight append effect 加超时看门狗，超时后按 `ErrCorruptState` 走 `failDurableAppendRequestsLocked` 清理 in-flight 状态，而不是无限期停留。

### [P1] 2. checkpoint 完成事件同样可被丢弃，`checkpointInFlight` 永久为真，CheckpointHW 冻结

- **位置**：`pkg/channel/replica/execution_pool.go:176-188`、`pkg/channel/replica/checkpoint_writer.go:22-31`、`pkg/channel/replica/checkpoint_writer.go:95-97`
- **类别**：正确性 / 持久化
- **代码**：
  ```go
  // execution_pool.go:176 —— 共享 checkpoint worker，结果事件错误被丢弃
  err := effect.replica.storeCheckpointEffect(context.Background(), effect.effect)
  _ = effect.replica.submitLoopResult(context.Background(), machineCheckpointStoredEvent{
      EffectID:       effect.effect.EffectID,
      ...
      Err:            err,
  })
  ```
  ```go
  // checkpoint_writer.go:95 —— in-flight 未清零就永远不再发起新的 checkpoint
  if !r.checkpointQueued || r.checkpointInFlight {
      return
  }
  ```
- **触发路径**：与发现 1 同源。checkpoint effect 落盘成功后回投 `machineCheckpointStoredEvent`，若此时 replica mailbox 已满（`pooled_loop_driver.go:228`）则事件丢失。`checkpointInFlight`/`pendingCheckpointEffectID` 只在 `applyCheckpointStoredEvent`（`checkpoint_writer.go:135-137`）被清，于是 `emitCheckpointEffectLocked` 永久早退；`applyCheckpointRetryEvent` 也只是再调一次同一个早退的函数，无法自愈。注意发起侧（`checkpoint_writer.go:119-126`）对 submit 失败**有**回滚+重试，唯独结果侧没有 —— 属于"严格版与宽松版并存"的典型缺口。
- **后果**：该 channel 的 `CheckpointHW` 从此不再推进。进程重启时 `recoverFromStores()` 从陈旧 checkpoint 恢复，需要重放大量日志；同时 `CommitReady` 可能被前一次失败置为 `false` 后再也无法恢复（`applyCheckpointStoredEvent` 是恢复 `CommitReady` 的唯一路径），造成该 channel 写入拒绝 `ErrNotReady`。
- **建议**：与发现 1 一起修，结果事件投递必须保证送达，或给 `checkpointInFlight` 加超时释放。

### [P0] 3. `ApplyMeta`（ISR 变更 / 保留边界推进）在持久化 append 飞行中抬 `roleGeneration`，已落盘的记录被丢弃不发布 → 运行时 LEO 永久落后于 durable LEO，后续每次 append 都"写盘成功但返回 ErrCorruptState"

- **位置**：`pkg/channel/replica/lifecycle_pipeline.go:96-98`（抬 roleGeneration）；`pkg/channel/replica/progress_pipeline.go:370-390`（`canPublishDurableAppendLocked`）；`pkg/channel/replica/progress_pipeline.go:268-280`（后续 base 不匹配）；`pkg/channel/replica/durable_store.go:245-250`（base 取自 durable LEO）
- **类别**：分布式一致性 / 数据正确性
- **代码**：
  ```go
  // lifecycle_pipeline.go:93 —— 非"纯租约续期"且非"同 leader 抬 epoch"的 ApplyMeta 通用路径
  previousMeta := r.meta
  r.commitMetaLocked(normalized)
  r.pendingLeaderEpochEffectID = 0
  r.pendingReconcileEffectID = 0
  r.clearRetentionEffectFencesLocked()
  r.roleGeneration++            // ← 不会清 appendInFlightEffectID
  ```
  ```go
  // progress_pipeline.go:371 —— 只有"写栅栏"这一种 roleGeneration 失配被容忍
  func (r *replica) canPublishDurableAppendLocked(ev machineLeaderAppendCommittedEvent) bool {
      ...
      if ev.RoleGeneration == r.roleGeneration {
          return true
      }
      return r.meta.WriteFence.BlocksAppend()
  }
  ```
  ```go
  // durable_store.go:245 —— 落盘 base 永远取自存储的 durable LEO，与运行时 state.LEO 无关
  oldLEO := s.log.LEO()
  base, err := appendLeaderRecords(s.log, records)
  ...
  if base != oldLEO { return oldLEO, oldLEO, fmt.Errorf(...) }
  ```
- **触发路径**：
  1. leader 正常 append：loop 发出 `appendLeaderBatchEffect{RoleGeneration: G}`，`appendInFlightEffectID = E`。
  2. append worker 通过 `validateAppendEffectFenceLocked`（此时 roleGeneration 还是 G），进入 `AppendLeaderBatch` → Pebble 写 + `Sync()`，耗时毫秒级。**这期间不持有 `r.mu`**。
  3. 控制面推来一份新 meta。`pkg/channel/runtime/runtime.go:530-543` 的 `applyReplicaMeta` 走 `rep.ApplyMeta(meta)`。`meta.go:48-73` 的 `sameLeaderLeaseRefresh` 只在**除 LeaseUntil 外全部相等**时才走快路径 —— 因此 **ISR 增删（follower 掉出/追上 ISR）、`MinISR`/`Status`/`Features` 变化、以及 `RetentionThroughSeq` 推进**（保留策略是周期性动作，`internal/runtime/channelretention/worker.go:302,339` 周期推进）都会落到通用路径，执行 `roleGeneration++` → G+1。`applyMetaCommand` **不会**清 `appendInFlightEffectID`（全仓 grep 确认只有 `takeAppendInFlightResultLocked` 与 `failOutstandingAppendWorkLocked` 会清）。
  4. 步骤 2 的落盘成功返回，`machineLeaderAppendCommittedEvent{RoleGeneration: G}` 回到 loop。`takeAppendInFlightResultLocked` 匹配成功（EffectID 仍是 E），但 `canPublishDurableAppendLocked` 因 `G != G+1` 且无写栅栏而返回 **false** → `failDurableAppendRequestsLocked(requests, channel.ErrNotLeader)`，**`r.state.LEO` 不推进**。
  5. 此刻 durable LEO = runtimeLEO + n。下一批 append 的 `AppendLeaderBatch` 从 durable LEO 落盘，返回 `base = runtimeLEO + n`。`applyLeaderAppendCommittedEvent`（`progress_pipeline.go:268-280`）判定 `ev.BaseOffset != r.state.LEO` 且 `ev.BaseOffset + recordCount != r.state.LEO` → 返回 `ErrCorruptState`，请求全部失败，**但记录已经写进日志了**；紧接着 `r.maybeFlushAppendLocked()` 立刻冲下一批，重复同样的动作。
- **后果**：
  - **消息丢失/错误应答**：步骤 4 的记录已持久化，调用方却收到 `ErrNotLeader`。若上游对 `ErrNotLeader` 做重试，同一条消息会被再次写入 → **消息重复**；若不重试则是"已落盘但对外宣告失败"的幽灵消息，重启恢复后 `recoverFromStores()` 会把 LEO 抬到 durable LEO，这段尾部可能被 reconcile 提交并投递。
  - **该 channel 写入永久失效**：步骤 5 之后每次 append 都是"落盘成功 + 返回 ErrCorruptState"，且没有任何路径把 `r.state.LEO` 追平 durable LEO（reconcile 用的是运行时 LEO；只有 `truncateTo != nil` 的 reconcile 截断才能顺带削掉幻影尾巴，而当 ISR 已追平运行时 LEO 时 `truncateTo == nil`）。只有进程重启或角色切换才能恢复。
  - **日志被幻影记录无界污染**：`maybeFlushAppendLocked()` 的立即重冲让失败路径变成紧循环 —— 高负载下每条入站消息都被写盘一次然后报错，磁盘持续增长。
- **建议**：`canPublishDurableAppendLocked` 的容忍条件应改为"channelKey / epoch / leaderEpoch / leader 未变即可发布"，`roleGeneration` 单独变化不应丢弃已同步的 append（现在只对写栅栏开了口子）；或者在 `applyMetaCommand` 抬 `roleGeneration` 前，先把 in-flight append 的 effect 打上"允许跨代发布"的标记。同时 `applyLeaderAppendCommittedEvent` 检测到 `ev.BaseOffset > r.state.LEO` 时应触发一次以 durable LEO 为准的重新对齐（或强制 reconcile 截断），而不是无限重试。

### [P1] 4. `pendingFollowerApplyEffectID` 是唯一没有任何生命周期兜底清除的栅栏，结果命令投递失败即导致该 follower **永久停止复制**

- **位置**：`pkg/channel/replica/follower_apply.go:33-36`（硬闸）、`:176`（置位）、`:296-299`（唯一清除点）、`:231-249`（投递失败）
- **类别**：分布式一致性 / 可用性
- **代码**：
  ```go
  // follower_apply.go:33 —— 只要栅栏没清，所有后续 ApplyFetch 一律 ErrNotReady
  if r.pendingFollowerApplyEffectID != 0 {
      return machineResult{Err: channel.ErrNotReady}
  }
  ```
  ```go
  // follower_apply.go:231 —— 结果命令投递失败时，栅栏留在置位状态
  result := r.submitLoopCommand(context.Background(), machineFollowerApplyResultCommand{
      EffectID: effect.EffectID,
      ...
      Err:      err,
  })
  ...
  return result.Err
  ```
- **触发路径**：
  1. follower 收到 leader 复制批次，`pkg/channel/runtime/backpressure.go:875` 调 `ApplyFetch`。
  2. loop 的 `applyFetchCommand` → `newFollowerApplyEffectLocked` 置 `r.pendingFollowerApplyEffectID = effectID`，把 effect 交回调用方执行。
  3. 调用方 goroutine 执行 `executeFollowerApplyEffect`：落盘（可能成功）后 `submitLoopCommand(context.Background(), machineFollowerApplyResultCommand{...})`。
  4. 若此时该 replica 的 pooled mailbox 已满（`pooled_loop_driver.go:228`）或共享 `p.ready` 已满（`:248-253`），`enqueue` 返回 `ErrNotReady`，命令**根本没进 loop**。
  5. 全仓 grep 确认 `pendingFollowerApplyEffectID` 的**唯一**清零点是 `applyFollowerApplyResultCommand`（`follower_apply.go:299`）。对比其它栅栏：`pendingReconcileEffectID` 在 `ApplyMeta` / `BecomeLeader` / `BecomeFollower` / `Tombstone` / `Close` / 快照安装 六处都会被清（`lifecycle_pipeline.go:100,155,239,380,395,409`、`snapshot_pipeline.go:182`），`pendingRetentionAdopt/TrimEffectID` 每次 `applyRetentionCommand` 都会被覆盖重发 —— 唯独 follower apply 栅栏没有任何兜底。
- **后果**：该 follower 上此 channel 的复制**永久停止**（每次 `ApplyFetch` 都返回 `ErrNotReady`），且角色切换、meta 更新都无法解除。该副本的 LEO/HW 冻结，leader 的 ISR 少一个可用成员；当 ISR 缩到 `MinISR` 以下时整个 channel 拒绝写入（`ErrInsufficientISR`）。若同时落盘已成功（步骤 3 的 `ApplyFollowerBatch` 返回 nil），本地 durable LEO 还会领先运行时 LEO，与发现 3 同样的错位。
- **建议**：让生命周期转换（`applyMetaCommand` / `applyBecomeFollowerCommand` / `applyBecomeLeaderCommand` / `applyCloseCommand`）像清 `pendingReconcileEffectID` 一样清掉 `pendingFollowerApplyEffectID`，并且在 `executeFollowerApplyEffect` 发现结果命令投递失败时走一次带重试的清栅栏路径。

### [P3] 5. `machine.go` + `state.go`（368 行，占本分片 5%）是只被测试触达的第二套状态机实现

- **位置**：`pkg/channel/replica/machine.go:52-340`、`pkg/channel/replica/state.go:1-28`、`pkg/channel/replica/commands.go:22-29`
- **类别**：架构 / 死代码
- **代码**：
  ```go
  // machine.go:55
  func newReplicaMachine(state replicaMachineState) replicaMachine {
      state = state.clone()
      if state.progress == nil {
          state.progress = make(map[channel.NodeID]uint64)
      }
      return replicaMachine{state: state}
  }
  ```
- **触发路径**：全仓 `grep -rn 'newReplicaMachine' --include='*.go'` 在非测试代码中只命中它自己的定义，唯一使用者是 `pkg/channel/replica/machine_test.go`。`machineAppendCommand` 同理 —— 它不在 `applyLoopEvent`（`lifecycle_pipeline.go:15-71`）的 switch 里，生产路径用的是 `machineAppendRequestCommand`。`replicaMachine.applyAppend` / `applyAdvanceHW` / `applyCheckpointStored` 各自复刻了一份 append 准入、HW 推进、checkpoint 发布逻辑。
- **后果**：两套实现会分叉。`machine_test.go` 通过不代表生产 loop 正确 —— 例如本报告发现 3 的 `roleGeneration` 失配问题在 `replicaMachine` 里根本不存在（它没有 `roleGeneration` 栅栏），针对它写的测试永远不会暴露这个 bug。同时 staticcheck 的 U1000 不会报它（测试引用了），属于长期隐藏的维护负担。
- **建议**：把 `machine_test.go` 改写成直接驱动真实 `replica` loop，删除 `machine.go`/`state.go`/`machineAppendCommand`/`publishStateEffect`；否则 FLOW.md 应明确写出"该 harness 与生产逻辑可能分叉，不构成生产行为证据"。

### [P2] 6. `Close()` 不等待持久化 append effect 退出：已授权的落盘可以发生在副本关闭之后

- **位置**：`pkg/channel/replica/append_pipeline.go:374-397`、`pkg/channel/replica/checkpoint_writer.go:9-40`、`pkg/channel/replica/replica.go:297-321`、`pkg/channel/replica/replica.go:208-215`
- **类别**：并发 / 资源生命周期
- **代码**：
  ```go
  // append_pipeline.go:374 —— 外层 worker 起了一个 per-effect 内层 goroutine
  func (r *replica) startAppendEffectWorker() {
      go func() {
          defer close(r.appendWorkerDone)
          for {
              select {
              case effect := <-r.appendEffects:
                  ctx, cancel := context.WithCancel(context.Background())
                  done := make(chan struct{})
                  go func() { defer close(done); r.runAppendEffect(ctx, effect) }()
                  select {
                  case <-done:
                      cancel()
                  case <-r.stopCh:
                      cancel()
                      return          // ← 直接返回，从不 <-done
                  }
  ```
  ```go
  // replica.go:315 —— Close 只 join 外层 worker（其 defer close(appendWorkerDone) 在上面 return 时就触发了）
  if r.appendWorkerDone != nil {
      <-r.appendWorkerDone
  }
  ```
  ```go
  // replica.go:208 —— pooled 模式更彻底：这两个 done 在构造时就关掉了，Close 根本不等任何 effect
  if cfg.Execution.Mode == ExecutionModePooled {
      close(r.appendWorkerDone)
      close(r.checkpointWorkerDone)
  }
  ```
- **触发路径**：
  1. leader 的 append effect 已通过 `validateAppendEffectFenceLocked`（`append_pipeline.go:195-197`），进入 `r.durable.AppendLeaderBatch(ctx, ...)`（Pebble 写 + fsync，毫秒级）。
  2. 此时 `pkg/channel/runtime/runtime.go:233` 的 `RemoveChannel` 调 `Tombstone()`（清空 in-flight 栅栏、把请求以 `ErrTombstoned` 结束），再调 `:257` 的 `ch.replica.Close()`。
  3. dedicated 模式：外层 worker 收到 `<-r.stopCh` 后 `cancel(); return`，`defer close(r.appendWorkerDone)` 立即触发，`Close()` 的 `<-r.appendWorkerDone` 立刻返回 —— 而内层 goroutine 仍在 `AppendLeaderBatch` 里。`cancel()` 只影响 ctx，Pebble 写入不会因此回滚。pooled 模式：`Close()` 对该 effect 根本没有任何等待点（`appendWorkerDone` 构造时已关），effect 可能还排在共享 `p.append` 队列里没被取走。
  4. 落盘完成后 `submitLoopResult` 因 replica 已关闭返回 `ErrNotLeader`，被 `_ =` 丢弃（`append_pipeline.go:244`）。
- **后果**：调用方收到的是 `ErrTombstoned`/`ErrNotLeader`（"这条消息没写成功"），但记录确实进了 durable 日志。同一 channel 被重新装载时（`ChannelIdleTimeout` 卸载后重新激活、或 tombstone TTL 过后重建），`recoverFromStores()` 会把 LEO 抬到含这些记录的 durable LEO，这段"已宣告失败"的尾部随后可能被 reconcile 提交并投递 —— 幽灵消息。同时 `Close()` 返回后仍有 goroutine 在写属主的存储，违反了 AGENTS.md/FLOW.md 所述"`Close` ... waits for loop/append/checkpoint goroutines to stop"（`replica/FLOW.md` 6.2 节）。
- **建议**：`startAppendEffectWorker` 在 `<-r.stopCh` 分支里也必须 `<-done` 之后再 return；pooled 模式需要一个 per-replica 的 in-flight effect 计数，`Close()` 等它归零（或在 `submitAppendEffect` 时登记，效果完成时注销）。`startCheckpointEffectWorker`（`checkpoint_writer.go:26-28`）是同一个形状，一并修。

### [P2] 7. 每次 HW 推进都在持锁状态下复制一份 progress map，而结果只在错误分支被读

- **位置**：`pkg/channel/replica/progress_pipeline.go:505-512` 与 `:511-523`、`:661-667`
- **类别**：性能
- **代码**：
  ```go
  // progress_pipeline.go:503 —— 成功推进分支
  r.state.HW = candidate
  r.scheduleCheckpointLocked(*checkpoint)
  r.publishStateLocked()
  r.notifyReadyWaitersLocked()
  outcome.advanced = true
  outcome.newHW = candidate
  outcome.leo = r.state.LEO
  outcome.notify = r.onLeaderHWAdvance
  outcome.progress = r.snapshotProgressLocked()   // ← 分配 + 全量拷贝
  ```
  ```go
  // progress_pipeline.go:511 —— outcome.progress 只在 err != nil 时被读
  func (r *replica) finishHWAdvanceOutcome(outcome hwAdvanceOutcome) {
      if outcome.err != nil {
          r.appendLogger().Warn("advance HW failed", ..., wklog.Any("progress", outcome.progress), ...)
          return
      }
      if !outcome.advanced { return }
      if logger, ok := r.debugAppendLogger(); ok {
          logger.Debug("HW advanced", ...)   // 不含 progress
      }
      if outcome.notify != nil { outcome.notify() }
  }
  ```
- **触发路径**：`advanceHWLocked()` 被 `applyCursorCommand`（每个 follower cursor ACK）、`applyFetchProgressCommand`（每次 follower fetch）、`applyLeaderAppendCommittedEvent`（每批 leader 提交）、`applyAdvanceHWEvent` 四处调用，都是每消息级热路径。成功分支每次都执行 `snapshotProgressLocked()` —— `make(map[uint64]uint64, len(r.progress))` 加全量拷贝，**在 `r.mu` 写锁内**。全仓 grep 确认 `outcome.progress` 只有 `:520` 一个读点，位于 `outcome.err != nil` 分支，成功路径拷出来的 map 立即被丢弃。
- **后果**：每条消息提交 / 每次 follower ACK 一次 map 堆分配 + 拷贝，占用写锁时间。N 个 channel × M msg/s 的规模下是纯浪费的 GC 压力与锁持有时间。
- **建议**：把 `:509` 这一行删掉（错误分支的 `:488`/`:497` 保留即可），需要诊断时改为在 `finishHWAdvanceOutcome` 的 debug 分支里按需取。

### [P2] 8. cursor / fetch 进度接受任意 `ReplicaID`，`progress` 与 `retentionProgress` 在一个 leader 任期内只增不删

- **位置**：`pkg/channel/replica/progress_pipeline.go:56-59`、`:120-123`、`:650-660`
- **类别**：无界增长 / 健壮性
- **代码**：
  ```go
  // progress_pipeline.go:56 —— 只校验非零，不校验是否属于 Replicas/ISR
  if cmd.ReplicaID == 0 {
      r.mu.Unlock()
      return machineResult{Err: channel.ErrInvalidMeta}
  }
  ```
  ```go
  // progress_pipeline.go:650
  func (r *replica) setReplicaProgressLocked(replicaID channel.NodeID, matchOffset uint64) {
      if r.progress == nil { r.progress = make(map[channel.NodeID]uint64) }
      r.progress[replicaID] = matchOffset
  }
  ```
- **触发路径**：`ApplyFollowerCursor` / `Fetch` 携带的 `ReplicaID` 来自对端复制请求（`pkg/channel/runtime/longpoll.go:326-341`、`:208-215`）。replica 层只拒绝 `0`，任何其它值都会在 `r.progress` 和 `r.retentionProgress` 里建一个条目。`r.progress` 的**唯一**重建点是 `seedLeaderProgressLocked`，只在 `finishBecomeLeaderLocked` 被调（`lifecycle_pipeline.go:237`）；普通 `ApplyMeta` 通用路径**不重建 `progress`**（只在 ISR/epoch/leader 变化时重建 `retentionProgress`）。因此一个 leader 任期内两个 map 单调增长。
- **后果**：内存按"见过的 ReplicaID 数 × channel 数"增长且无淘汰；`snapshotProgressLocked()`（见发现 7）与 `minISRRetentionProgressLocked()` 的开销随之增长。正确性上目前不受影响 —— `reconcileQuorumCandidate` 只遍历 `meta.ISR`，非成员条目不参与 quorum（已核实）。这是一层缺失的纵深防御 + 内存增长面。
- **建议**：`applyCursorCommand` / `applyFetchProgressCommand` 增加 `containsNode(r.meta.Replicas, cmd.ReplicaID)` 校验，非成员直接 `ErrStaleMeta`；并在 `applyMetaCommand` 通用路径按新的 `Replicas` 集合裁剪 `progress`/`retentionProgress`。

### [P2] 9. follower 复制路径在持有 `r.mu` 写锁的情况下对每条记录做 payload 深拷贝，且没有 leader 侧那样的 owned 变体

- **位置**：`pkg/channel/replica/follower_apply.go:104`、`pkg/channel/replica/machine.go:311-318`（`cloneRecords`）、对照 `pkg/channel/replica/append.go:16-18`（`AppendOwned`）
- **类别**：性能
- **代码**：
  ```go
  // follower_apply.go:100 —— applyFetchCommand 全程 r.mu.Lock() + defer Unlock
  effect := r.newFollowerApplyEffectLocked(req, baseLEO, nextLEO, previousHW, nextHW, true)
  effect.Records = cloneRecords(req.Records)
  ```
  ```go
  // machine.go:311 —— 每条记录一次 slice 分配 + payload memcpy
  func cloneRecords(records []channel.Record) []channel.Record {
      out := make([]channel.Record, len(records))
      for i, record := range records {
          out[i] = record
          out[i].Payload = append([]byte(nil), record.Payload...)
      }
      return out
  }
  ```
- **触发路径**：follower 每收到一批复制记录（`pkg/channel/runtime/backpressure.go:875` → `ApplyFetch` → `applyFetchCommand` → `prepareFollowerRecordApplyLocked`）就在 `r.mu` 写锁内跑一遍 `cloneRecords`：`len(records)+1` 次堆分配加上全部 payload 字节的 memcpy。批大小由 fetch 预算决定（`MaxBytes`，生产默认量级为几十 KB ~ MB）。这段时间内该 channel 的 loop 完全串行阻塞 —— `Status()` 之外的所有命令（append、cursor、fetch progress、meta）都要等这把锁。
- **后果**：follower 节点上每字节复制流量都要多走一次锁内 memcpy，直接抬高该 channel loop 的锁持有时间与 GC 压力，在复制追赶（catch-up）场景下最明显。leader 侧已经为此提供了 `AppendOwned`（`append.go:16-18`，"caller has transferred ownership"）来跳过边界拷贝，follower 侧没有对应逃生通道。
- **建议**：把拷贝移到锁外（`applyFetchCommand` 返回 effect 后、`executeFollowerApplyEffect` 里再拷），或者为 follower apply 增加 owned 语义让 `pkg/channel/runtime` 转移所有权（它刚从线路解码出来的 buffer 本来就是独占的）。

### [P3] 10. FLOW.md 声称 `pooled` 是默认执行模式，代码默认是 `dedicated`

- **位置**：`pkg/channel/replica/replica.go:155-157`，对照 `pkg/channel/replica/FLOW.md:5 节`
- **类别**：架构/文档一致性
- **代码**：
  ```go
  // replica.go:155
  if cfg.Execution.Mode == "" {
      cfg.Execution.Mode = ExecutionModeDedicated
  }
  ```
  FLOW.md 第 5 节原文："`pooled` is the default and uses a shared `ExecutionPool`; ... `dedicated` keeps the legacy per-replica loop goroutine ... and remains a rollback mode."
- **触发路径**：任何直接使用 `replica.NewReplica` 而不显式填 `Execution.Mode` 的调用者（包内 `machine.go` 测试装置、外部 SDK 使用者）会拿到 dedicated 模式，与 FLOW.md 描述相反。生产二进制侥幸不受影响：`internal/app/config.go:1129-1131` 在配置校验阶段把空串补成 `"pooled"`，再经 `internal/app/channelmeta.go:268-272` 显式传入。
- **后果**：文档与包级默认值矛盾；包级默认走的是文档里标注为"legacy/rollback"的路径，而该路径含有本报告发现 6的 goroutine 未 join 缺陷。
- **建议**：把包级默认改成 `ExecutionModePooled`（`Pool==nil` 时回落 dedicated），或修正 FLOW.md 说明"包默认 dedicated，应用层默认 pooled"。


## 已排除的候选项

- `pkg/channel/replica/fetch_pipeline.go:99` —— gosec G115（uint64→int）**误报**。`maxVisibleRecords := effect.LeaderLEO - effect.FetchOffset` 的下溢已被 `:92` 的 `effect.FetchOffset > effect.LeaderLEO → ErrCorruptState` 挡住；截断只在 `uint64(len(records)) > maxVisibleRecords` 时发生，即 `maxVisibleRecords < len(records)`（一个 `int`），转换必定不溢出。这是本分片 staticcheck/gosec 的**唯一**一条命中。
- `pkg/channel/replica/append_pipeline.go:191-256`（`runAppendEffect` 在 `Stop`/`Close` 后继续运行）—— 确认 join 缺失，但**后果远小于预期**：`validateAppendEffectFenceLocked`（`:271-276`）会先检查 `effect.EffectID != r.appendInFlightEffectID` 和 `r.closed`，而 `Close`/`Tombstone` 都通过 `failOutstandingAppendWorkLocked` 把 `appendInFlightEffectID` 清零，所以**关闭之后才到达栅栏的 effect 不会写盘**；不存在"往已关闭的 store 写"。残留风险只剩"栅栏通过之后、落盘进行中"这一窄窗口，已单列为发现 6（P2）而非 P1。
- `pkg/channel/replica/loop.go:18-21`、`append_pipeline.go:466-473`、`execution_pool.go:190-216`、`pooled_loop_driver.go:248-262`、`:290-296`（全部 `select { case ch <- x: default: }` 非阻塞发送）—— 逐个核实**发送本身安全**：`cmd.reply` 是容量 1 且 `releasePooledLoopReply` 归还前会清空（`pooled_loop_driver.go:88-96`），且只在确实收到结果时才归还（`awaitPooledLoopCommandResult` 在 `done`/`stop`/`ctx.Done` 分支返回 `reusable=false`），不会出现串台写入；`waiter.ch` 容量 1 且 `req.completed` 保证每个请求只完成一次，`releaseAppendWaiter`（`append_pool.go:39-52`）归还前循环排空。真正的问题不在发送，而在**发送失败后调用方吞掉了错误**（发现 1/2/4）。
- `pkg/channel/replica/append_pool.go` 的 `sync.Pool` 复用与 loop 持有的 `*appendRequest` 指针竞争 —— 已核实**安全且有意设计**：`completeAppendRequestLocked`（`append_pipeline.go:337-346`）把"向 waiter 发送完成"放在所有字段读写之后的最后一步，`completeAndDeleteAppendRequestLocked`（`:349-359`）先抓取 `requestID` 再完成再 delete，两处都有注释说明"successful callers may recycle req immediately"；`notifyReadyWaitersLocked`（`progress_pipeline.go:694-724`）完成一个 waiter 后立刻 `continue`，不再触碰该指针。
- `pkg/channel/replica/reconcile_coordinator.go:14-37`（`reconcileQuorumCandidate` 把缺失的 ISN 成员按 `hw` 计入）—— 看起来像"用默认值凑出 quorum"，但核实为**正确**：缺失成员只能计到已提交的 `hw`，永远不能被计到 leader 的本地尾部；`candidate > leo → ErrCorruptState`，`candidate < hw → ErrCorruptState`，`candidate == hw → no-op`（`:41-53`）。learner（`Replicas - ISR`）的进度完全不参与，与 `replica/FLOW.md` 6.5 节一致。
- `pkg/channel/replica/progress_pipeline.go:650-655`（`setReplicaProgressLocked` 无单调性保护）—— 看起来能让 progress 回退，但逐个核实**全部**调用方都在外层做了保护：`applyCursorCommand:78` `matchOffset <= oldProgress` 直接返回、`applyFetchProgressCommand:166` `matchOffset > oldProgress`、`applyReconcileProofCommand:570` `matchOffset > current`；仅 `applyReconcileDurableTruncateResultLocked`/`applyLeaderReconcileResultCommand` 会**故意**把本地进度降到截断后的 LEO，而截断永不低于 HW（`:517` / `reconcile_coordinator.go:344-347`），`advanceHWLocked` 的 `candidate < HW → ErrCorruptState` 也兜住了。
- `pkg/channel/replica/retention.go:100-105`（trim 的前置条件在获得 `durableMu` 后没有重新校验）—— 看起来是 checkpoint/retention 截断竞态，但核实安全：trim 要求 `throughSeq <= min(CheckpointHW, HW, LEO)`，而 HW/CheckpointHW 在一个进程生命周期内单调不减（`invariant.go:70-77`），LEO 只能因截断下降而截断永不低于 HW，所以条件一旦成立就不会失效；`validateRetentionTrimEffectFence` 在锁内 + 获得 `durableMu` 后各校验一次 channelKey/epoch/roleGeneration/effectID。
- `pkg/channel/replica/follower_apply.go:33-36` 经 **调用方 ctx 取消** 触发永久栅栏 —— 这条触发路径**不成立**：全仓唯一调用者 `pkg/channel/runtime/backpressure.go:875` 传的是 `context.Background()`，永不取消，因此"命令已入 mailbox 但调用方因 ctx 超时放弃执行 effect"的交错在生产里不可达。发现 4 保留的是另一条（结果命令投递失败）触发路径。
- `pkg/channel/replica/durable_lane.go:26-32`（`Unlock` 无匹配 lock 时 `panic`）—— 逐个核实 7 处 `lockDurableMu` 调用点（`append_pipeline.go:201`、`follower_apply.go:207`、`snapshot_pipeline.go:94`、`reconcile_coordinator.go:234`、`checkpoint_writer.go:47`、`retention.go:132,158`、`lifecycle_pipeline.go:310`）都是"加锁成功才解锁"的配对结构，不存在双解锁路径。
- `pkg/channel/replica/progress_pipeline.go:729-739`（`scheduleCheckpointRetry` 每次都起一个 goroutine）—— 不是泄漏：`select { case <-timer.C: ... case <-r.stopCh: }` 且 `defer timer.Stop()`，10ms 内必退出，同一时刻每个 channel 最多一个。
- `pkg/channel/replica/migration.go:26-34`（`FenceAndDrain` 每毫秒 `time.NewTimer`）—— 轮询开销可接受，且 `<-timer.C` 分支不需要 `Stop()`，`ctx.Done` 分支显式 `Stop()`，无 timer 泄漏。
- **控制面 P0 同型检查（协调者交叉提示）**—— 已核实 `pkg/channel/replica` **没有**那个形状。replica 侧的"apply"是 `applyLoopEvent`（`lifecycle_pipeline.go:9-71`），返回的 `machineResult.Err` 只有两种去向：作为命令的同步回复给外部调用方，或在结果事件路径被 `_ = r.applyLoopEvent(event)`（`loop_driver.go:34`、`pooled_loop_driver.go:332`）丢弃。**没有任何调用方把它当成致命错误去 `setError` + 停止 loop**；loop 只在 `stopCh` 关闭时退出。因此状态相关错误不会杀掉 replica loop，更不会跨集群传播。
- `pkg/channel/replica` 被 `pkg/channel/runtime/backpressure.go:828,875,962` 以 `context.Background()` 调用（协调者交叉提示）—— 核实后**在本分片内不构成独立发现**：`submitLoopCommand` 在 pooled 模式下对 mailbox 满是**立即返回 `ErrNotReady`**（`pooled_loop_driver.go:228`）而非阻塞，所以"无 deadline 导致入站 goroutine 永久挂住"不成立。唯一真正会无限期阻塞的是 `lockDurableMu(context.Background())`，而 durable lane 的持有者本身没有泄漏路径（上一条已核实配对完整）。该问题的实体应记在 unit 26 的 `backpressure.go`。
- `pkg/channel/replica/promotion_evaluator.go:26-63` —— 逐行核实 `EvaluateLeaderPromotion` 不会把没追上的候选判为可晋升：`matchOffsets[meta.Leader] = local.LEO` 之后 peer proof 只能**抬高**别人而不能超过 `local.LEO`（`reconcileProofMatchOffset` 内 `decision.matchOffset > leaderLEO → ErrCorruptState`），低于保留地板的 proof 被 `ErrSnapshotRequired`/`ErrStaleMeta` 跳过，缺失成员仍按 `hw` 计入；`candidate < local.HW` 直接判 `candidate_below_hw` 不可领导。与 `replica/FLOW.md` 6.9 节描述一致。
- `pkg/channel/replica/pooled_loop_driver.go:184-197`（`undoLastPushLocked`）—— 看起来像环形队列越界/错删，但核实正确：`pushLocked` → `undoLastPushLocked` 之间全程持 `d.mu`，撤销的必定是刚压入的那条。

## 本分片整体评价

这是本仓库工程质量**最高**的分片之一：单写者 loop + effect 外置 + `durableMu` 单持久化车道 + 每个 effect 五元组栅栏（EffectID / ChannelKey / Epoch / LeaderEpoch / RoleGeneration）的设计非常扎实，`FLOW.md` 294 行几乎逐函数对齐代码，`sync.Pool` 复用与"发送放最后一步"的时序处理有注释且经得起推敲，gosec/staticcheck 在 7180 行里只留下 1 条误报。**但它有一个系统性的架构缺口**：所有 effect 都靠"结果事件必须回到 loop"来释放 in-flight 栅栏，而回投用的 `submitLoopResult` / `submitLoopCommand` 在 pooled mailbox 或共享 `ready` 队列饱和时会返回 `ErrNotReady`，**四个回投点全部用 `_ =` 或直接忽略丢掉了这个错误**（发现 1/2/4）。发起侧都写了失败重试，结果侧一个都没有 —— 这是典型的"严格版与宽松版并存，宽松版在主路径"。

**最该优先处理的一个问题**：发现 1 —— 持久化 append 完成事件丢失会让该 channel 的写入**永久**卡死（in-flight 栅栏无超时看门狗、无生命周期兜底），同时把 durable LEO 永久留在运行时 LEO 之前。它和发现 3（`ApplyMeta` 抬 `roleGeneration` 丢弃已落盘 append）共享同一个终局状态：**运行时 LEO < durable LEO 之后，该 channel 会进入"每条消息都写盘成功然后返回 ErrCorruptState"的紧循环**，既不自愈也不告警。建议的最小修复是给 `applyLeaderAppendCommittedEvent` 加一条以 durable LEO 为准的重新对齐路径，并给 in-flight append/checkpoint/follower-apply 三个栅栏各加超时释放。

本分片是**在生产运行路径上的 v1 代码**（`internal/app/build.go:295-313` 构造共享 `ExecutionPool`，默认 `pooled` 模式），严重度按实际计算，未做未上线降档。

