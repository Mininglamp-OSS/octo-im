# cluster 槽位/迁移/上线/观测/协调

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/cluster/FLOW.md | 614 | 是（先于代码阅读） |
| pkg/cluster/hashslot_migration.go | 826 | 是 |
| pkg/cluster/onboarding.go | 584 | 是 |
| pkg/cluster/slot_manager.go | 502 | 是 |
| pkg/cluster/observation_sync.go | 494 | 是 |
| pkg/cluster/node_health_scheduler.go | 459 | 是（recon 报 clean，本次复核确认 clean） |
| pkg/cluster/managed_slots.go | 430 | 是 |
| pkg/cluster/reconciler.go | 404 | 是 |
| pkg/cluster/observation_hint.go | 256 | 是 |
| pkg/cluster/observation_cache.go | 242 | 是 |
| pkg/cluster/slot_executor.go | 234 | 是 |
| pkg/cluster/slot_log_entries.go | 216 | 是 |
| pkg/cluster/runtime_observation_reporter.go | 214 | 是 |
| pkg/cluster/snapshot_chunks.go | 171 | 是 |
| pkg/cluster/assignment_cache.go | 120 | 是 |
| pkg/cluster/onboarding_executor.go | 111 | 是 |
| pkg/cluster/slot_handler.go | 101 | 是 |
| pkg/cluster/slot_raft_compaction.go | 99 | 是 |
| pkg/cluster/hashslottable.go | 26 | 是（纯 type alias 转发） |
| pkg/cluster/rebalancer.go | 20 | 是（纯函数转发） |
| pkg/cluster/scalein_safety.go | 18 | 是（内容与文件名不符，见 P3-13） |
| pkg/cluster/hashslot/hashslottable.go | 331 | 是 |
| pkg/cluster/hashslot/rebalancer.go | 256 | 是 |
| pkg/cluster/slotmigration/worker.go | 287 | 是 |
| pkg/cluster/slotmigration/progress.go | 45 | 是 |
| pkg/cluster/slotmigration/snapshot.go | 25 | 是 |

分片内 26 个非测试 `.go` 文件全部通读。为确认触发路径另读了非本分片文件（不在其中下发现）：`agent.go`、`cluster.go`（相关函数）、`router.go`、`observer.go`、`config.go`、`codec.go`、`controller_host.go`、`controller_metadata_snapshot.go`、`pkg/slot/fsm/statemachine.go`、`pkg/controller/meta/onboarding_store.go`、`internal/app/build.go`。

**FLOW.md 一致性**：整体结构与代码吻合。两处实质性背离已作为发现记录：P2-5（FLOW.md 5.7 描述的"Delta 阶段 source 继续接受写入 + DeltaForwarder 转发 live write"在代码中不可达）、P2-6（FLOW.md 5.1 ⑦ 称 migrationProgressLoop"仅在有 active migration / pending abort 时推进"，但判定闸门本身每 200ms 做逐 slot Pebble 扫描）。

---

## 发现

### [P1] 1. observation delta 协议没有 assignment 删除 tombstone：Controller 删除 SlotAssignment 后 follower 永久保留它，且生产环境没有任何全量刷新路径能修复

- **位置**：`pkg/cluster/observation_sync.go:307-314`（删除分支不记 tombstone、不抬 historyFloor）、`observation_sync.go:28-42`（响应结构缺 `DeletedAssignments`）、`observation_sync.go:238-295`（apply 侧无删除处理）
- **类别**：分布式一致性
- **代码**：
  ```go
  // replaceAssignmentsLocked —— 删除分支
  for slotID := range s.assignments {
      if _, ok := next[slotID]; ok {
          continue
      }
      s.revisions.Assignments++
      delete(s.assignments, slotID)
      delete(s.assignmentRevisionBySlot, slotID)
  }                       // ← 既没写 deletedAssignmentRevisionBySlot，也没抬 historyFloor.Assignments
  ```
  对比同文件三个兄弟函数：`replaceTasksLocked:335` 写 `s.deletedTaskRevisionBySlot[slotID]`；`replaceRuntimeViewsLocked:378` 写 `s.deletedRuntimeRevisionBySlot[slotID]`；`replaceNodesLocked:356` 抬 `s.historyFloor.Nodes`。只有 assignments 三者皆无。
- **触发路径**（controller leader 不变更，稳态）：
  1. 运维调用 `RemoveSlot(3)`（`internal/access/manager/slot_add_remove.go:60` → `internal/usecase/management/slot_add_remove.go:47` → `operator.go`）。最后一个 hash-slot 迁移 finalize 后 Controller 删除 slot 3 的 Assignment（FLOW.md 5.9 ④）。
  2. leader 侧 `replaceMetadataSnapshot` → `replaceAssignmentsLocked`：slot 3 从 `s.assignments` 和 `s.assignmentRevisionBySlot` 双双删除，`revisions.Assignments` 由 R 增至 R+1。
  3. follower 下一次 `SyncObservationDelta` 携带 `Revisions.Assignments = R`（`agent.go:97`）。leader 侧 `buildObservationDelta`（controller_host.go:282）因 LeaderID/Generation 未变**不会**强制 full sync；`requiresFullSyncLocked(R)` 需要 `R < historyFloor.Assignments`，而该字段恒为 0 → 亦为 false。
  4. `buildDelta:197-202` 遍历 `assignmentRevisionBySlot`——slot 3 的条目已被删除，故响应里关于 slot 3 一个字节都没有，而 `resp.Revisions.Assignments = R+1`。
  5. follower `applyObservationDelta` 只做 upsert（262-267 行），无任何删除机制 → `state.Assignments[3]` 原样留存，并把本地 revision 推进到 R+1，**此后永不再问询 slot 3**。
  6. `agent.go:111-117` 把整个 `observationState.Assignments` 全量灌进 `assignmentCache`；`reconciler.Tick:56-60` 于是把 slot 3 计入 `desiredLocalSlots` → `ensureManagedSlotLocal` 持续维持/重开该 Slot，而第 ⑧ 步"关闭多余 Slot"（`reconciler.go:99-119`）因 slot 3 在 `desiredLocalSlots` 中而永不关闭它。
- **修复路径为何不存在**：唯一能重建全量 assignment 的 `slotAgent.SyncAssignments`（`agent.go:55-68`，走 `RefreshAssignments` 全量后 `SetAssignments`）在生产中**没有调用方**——它只被 `cluster.go:672 observeOnce` 调用，而 `observeOnce` 经 grep 确认仅存在于 `cluster_test.go:2605/2707` 与 `agent_internal_integration_test.go:139`；`startObservationLoop`（cluster.go:495-526）接的七个循环全部走 `SyncObservationDelta`。因此残留只能靠 controller leader 变更（强制 full sync）或节点重启清除。全仓 grep `DeletedAssignment|deletedAssignment` 零命中，确认是结构性缺口而非漏写一行。
- **后果**：`RemoveSlot` 之后被移除的物理 Slot 的 Raft group 在每个副本节点上无限期继续运行（存储、goroutine、tick、内存均不回收），并继续出现在 `API.SlotIDs()` / `PeersForSlot()` 的缓存视图里。与 FLOW.md 5.9 承诺的收敛语义直接冲突。
- **建议**：为 `observationDeltaResponse` 增加与 `DeletedTasks` 同构的 assignment 删除 tombstone，并在 `applyObservationDelta` 中消费；同时把 `historyFloor.Assignments` 作为兜底抬升。

### [P1] 2. runtimeObservationReporter 在解锁发送窗口内丢弃并发的 requestFullSync 与 markClosed，controller failover 期间可令 planner 停摆一分钟

- **位置**：`pkg/cluster/runtime_observation_reporter.go:72-94`（full-sync 分支）、`:105-127`（增量分支）
- **类别**：并发
- **代码**：
  ```go
  if r.needFullSync || r.fullSyncDueLocked(now) {
      report := runtimeObservationReport{ ..., FullSync: true, Views: cloneRuntimeViews(views) }
      r.mu.Unlock()
      if err := r.cfg.send(ctx, report); err != nil {   // 网络 RPC，锁已释放
          return err
      }
      r.mu.Lock()
      r.mirror = runtimeViewsBySlot(views)   // 用 send 前的旧快照整体覆盖
      r.dirtyViews = make(map[uint32]controllermeta.SlotRuntimeView)
      r.closedSlots = make(map[uint32]struct{})   // ← 整体清空，不区分是否已发送
      r.needFullSync = false                       // ← 清掉 send 期间新到的请求
  ```
- **触发路径 (a)——全量同步请求被吞**：
  1. `runtimeObserver` goroutine（`cluster.go:501`，默认 1s）进入 full-sync 分支，解锁后执行 `send`（一次 controller RPC，数十 ms）。
  2. send 期间 controller leader 发生 failover，`controllerClient.onLeaderChange`（`cluster.go:308-311`）在**另一个 goroutine** 调 `r.requestFullSync()` → `needFullSync = true`。
  3. send 返回（这份 FullSync 报给了旧 leader），重新加锁执行 `r.needFullSync = false`，新 leader 的全量同步请求被静默丢弃。
  4. 按 FLOW.md 5.3 `controllerTickOnce` ①：新 leader 在收到覆盖全部 alive 节点、且属于当前 leader term 的 `FullSync=true` 之前处于 warmup，**直接跳过 planner**。该节点的 full sync 已丢，下一次只能等周期兜底 `fullSyncDueLocked`（`ObservationRuntimeFullSyncInterval`，`config.go:40` 默认 **60s**）。
  5. 这 60 秒内整个集群的 Repair / Rebalance 规划完全停止。
- **触发路径 (b)——slot 关闭 tombstone 被吞**：send 期间 `reconciler.Tick:118` → `deleteRuntimePeers`（`cluster.go:1856`）→ `markClosed(slotX)`（运行在 wake/slowSync goroutine）。send 后 `r.mirror = runtimeViewsBySlot(views)`（旧快照，仍含 slotX）叠加 `closedSlots = make(...)` 清空，tombstone 永久丢失；由于 mirror 里 slotX 仍"存活"，后续 tick 的差分比较不会再次发现它已关闭。leader 侧 `observationCache.runtimeViewsByNode` 于是保留僵尸 view 直到 `runtimeViewTTL`（`controller_host.go:140` = 3 × 60s ≈ 3 分钟）淘汰。
  增量分支（117-127 行）此项无此问题：`for slotID := range r.closedSlots` 读的是**当前** map，send 期间新增的 tombstone 会被一并应用到 mirror 后再清空；`dirtyViews` 丢失也可自愈，因为下一 tick 重新快照 runtime 会与 mirror 再次比出差异。**唯一不可自愈的是 `closedSlots`（来自外部事件而非快照）与 `needFullSync`（来自外部事件）**。
- **后果**：(a) controller failover 若撞上发送窗口，planner 最长停摆 60s——故障恢复期恰恰是最需要 Repair 的时刻；(b) leader 观测缓存最长 3 分钟保留已关闭 Slot 的 runtime view，planner / `leaderTransferExecutorEligible` 在此期间基于 stale voters 决策。
- **建议**：发送前把待提交项移入本地变量、发送后只清除"本次确实发送的那些"（增量分支已是此模式）；`needFullSync` 用"发送前取值、发送后按取值清除"的 compare-and-clear。

### [P1] 3. reconcileOrphanedHashSlotMigrationStates 缺少 source-leader 闸门与 fence/ack 安全校验，本地 hash-slot 表落后时会解除仍在进行中的迁移的 source fence

- **位置**：`pkg/cluster/hashslot_migration.go:542-586`，配合 `hashslot_migration.go:764-786` `cleanupHashSlotDeltaOutbox`
- **类别**：分布式一致性
- **代码**：
  ```go
  for _, state := range states {
      if state.SourceSlot == 0 || state.TargetSlot == 0 || state.LastOutboxIndex == 0 { continue }
      ...
      if active, ok := desired[state.HashSlot]; ok && uint64(active.Source) == state.SourceSlot && uint64(active.Target) == state.TargetSlot {
          continue                        // 只有"本地表里存在完全匹配的活跃迁移"才放过
      }
      migration := HashSlotMigration{ HashSlot: state.HashSlot, Source: ..., Target: ... }
      if _, ok := stores[migration.Source]; ok {
          if err := c.completeHashSlotDeltaOutboxCleanup(ctx, migration); err != nil { return err }
          continue
      }
  ```
  `cleanupHashSlotDeltaOutbox` 随后无条件提案 cleanup：
  ```go
  if state.LastOutboxIndex == 0 { return nil }
  return c.proposeHashSlotOutboxCleanup(ctx, migration.Source, migration.HashSlot, migration.Target, state.LastOutboxIndex)
  ```
  source fsm 的 apply（`pkg/slot/fsm/statemachine.go:467`）在 `state.LastOutboxIndex != 0 && state.LastOutboxIndex <= cleanup.ThroughIndex` 时**删除整个 migration state**——`FenceIndex` 随之归零，`isHashSlotFenced`（statemachine.go:406）转为 false，source 重新接受该 hashSlot 的普通写入。
- **触发路径**：`desired` 完全来自本节点的 `c.router.hashSlotTable.Load()`（`hashslot_migration.go:119-128`）。该函数与 `observeHashSlotMigrations` 中其余每一步不同——它**没有** `shouldExecuteHashSlotMigration`（source leader）闸门，因此在 source Slot 的**每个副本**上都会运行，而 `proposeHashSlotOutboxCleanup` → `ProposeWithHashSlot` 会把命令转发给 source leader 执行。只要任一 source 副本的本地表落后于 Controller：
  - `Router.UpdateHashSlotTable`（`router.go:40-46`）是无条件 `Store(table.Clone())`，**没有 version 单调性校验**；`applyHashSlotTablePayload`（`cluster.go:1188-1198`）同样不校验。
  - 生产中唯一刷新该表的活路径是 `agent.HeartbeatOnce`（`cluster.go:495`，2s 周期）；`observationDeltaResponse`（`observation_sync.go:28-42`）不携带 hash-slot 表，`SyncAssignments`（会带表）如发现 1 所述在生产中不可达。于是任何一次由滞后 peer 应答、或乱序到达的 heartbeat 响应都能把表回退成不含该迁移的旧版本。
  - 节点重启同样构成窗口：`newObserverLoop.Start`（`observer.go:36-49`）在进入 ticker 前先立即 `tick` 一次，`migrationObserver` 周期仅 200ms（见发现 6），而 router 表初始为 `defaultHashSlotTable()`（无迁移）；`migrationObserveOnce` 的闸门 `hasActiveMigrationObservationWork` 恰恰把"存在持久化 migration state"当作继续执行的理由（`cluster.go:1995`），正是为这一重启场景设计的。若 source Slot 在首个 heartbeat 应答落地前被 reconciler 打开，随后 ≤200ms 内即触发 cleanup。
  - 表落后期间该迁移在 `desired` 中缺失 → 持久 state 被判为 orphan → fence 被删除；而 Controller 侧迁移仍活跃，后续 `finalizeHashSlotMigration` 会把 hashSlot 切到 target。
- **后果**：source fence 被解除到 finalize 之间落在 source 上的写入，既不在（更早导出的）snapshot 里，也不在（已被删除的）outbox 里，finalize 后该 hashSlot 路由到 target → **这些写入静默丢失**。需 `EnableHashSlotMigration=true`（`config.go`，默认关闭的实验开关）。
- **建议**：cleanup 判定加 source-leader 闸门（与其余步骤一致），并在 state 仍持有未 ack 的 fence（`FenceIndex != 0 && LastAckedIndex < FenceIndex`）时拒绝清理；给 `UpdateHashSlotTable` 加 version 单调性校验。

### [P1] 4. snapshot chunk 组装器按远程声明的 total 无上界分配缓冲区，且 key 全部由线路字段构成 —— 单个畸形帧即可触发致命 OOM

- **位置**：`pkg/cluster/snapshot_chunks.go:87-99`；解码 `pkg/cluster/codec.go:152-183`（不校验 total 上界）；入口 `pkg/cluster/cluster.go:924-940`（无来源校验）
- **类别**：安全 / 资源
- **代码**：
  ```go
  entry := a.pending[key]
  if entry == nil {
      if chunk.total > uint64(math.MaxInt) {
          return assembledRaftSnapshot{}, false, fmt.Errorf("raft snapshot total overflows int: %d", chunk.total)
      }
      entry = &raftSnapshotChunkAssembly{
          message:    append([]byte(nil), chunk.message...),
          total:      chunk.total,
          data:       make([]byte, int(chunk.total)),   // ← 唯一约束是 < MaxInt
          chunks:     make(map[uint64]int),
          lastUpdate: now,
      }
      a.pending[key] = entry
  }
  ```
- **触发路径**：
  1. 解码侧只做**内部自洽**校验：`offset <= total`、`len(data) <= total-offset`、帧长与 msgLen/dataLen 相符（codec.go:179-181）。因此 `total = 1<<40, offset = 0, dataLen = 1` 是一个完全合法的帧。
  2. `handleRaftSnapshotChunkMessage` 对任何到达 `msgTypeRaftSnapshotChunk`（=4）的帧直接解码并 `add()`，**没有** Controller Raft 通道那样的 `To=local && From!=local` 校验（FLOW.md 5.1 ④ 明确 Controller Raft 有此校验，Slot Raft chunk 通道无）。`grep -n "token|Auth|auth|TLS|tls" pkg/cluster/transport.go pkg/cluster/transport_glue.go` 零命中——传输层无鉴权、无 TLS。
  3. 首帧即 `make([]byte, 1<<40)`。Go runtime 对超大 len 抛 `runtime: out of memory` —— 这是**不可 recover 的 fatal error**，非 panic。
  4. 放大：`raftSnapshotChunkKey`（15-22 行）由 `slotID/chunkID/from/to/index/term` 六个线路字段构成，全部由发送方任选。每个不同 `chunkID` 都新建一份 `make([]byte, total)`，且 `pruneExpiredLocked` 只在 `add` 内部按 TTL（`defaultRaftSnapshotChunkTTL` = 2 分钟）清理，无周期回收。攻击者用递增 chunkID 发送 N 个小帧即可占住 N × total 字节。
- **后果**：任何能向集群监听端口发送 TCP 数据的一方（或一个字段损坏／版本不匹配的合法 peer）可使目标节点直接因 OOM 终止；对所有节点重复即全集群宕机。合法发送端的 chunk 上界是 `RaftSnapshotChunkSize`（`config.go:376` 默认 8MiB），接收端却完全不校验这一约束。
- **建议**：解码或 `add` 处校验 `chunk.total` 不超过配置的最大快照尺寸，并对 `a.pending` 的条目数与总字节数设上限。

### [P2] 5. hash-slot 迁移从 Snapshot 阶段起就 fence 住 source，整个 Delta 阶段 / durable outbox / live forwarder / Progress 机制在现有流程中全部不可达，与 FLOW.md 描述的语义相反

- **位置**：`pkg/cluster/hashslot_migration.go:639-658`（fence 先于导出）、`slotmigration/worker.go:217-230`（相位推进）、`slotmigration/progress.go:17-22`、`hashslot_migration.go:53-75`（outgoing 标记条件）
- **类别**：架构 / 文档一致性
- **代码**：
  ```go
  func (c *Cluster) completeHashSlotSnapshot(ctx context.Context, migration slotmigration.Migration) error {
      if _, err := c.currentManagedSlotLeader(migration.Target); err != nil { return ErrSlotNotFound }
      fenced, err := c.ensureHashSlotMigrationFenceApplied(ctx, HashSlotMigration{..., Phase: PhaseSnapshot})
      if err != nil { return err }
      if !fenced { return ErrSlotNotFound }          // fence 是导出快照的硬前置
      snap, sourceApplyIndex, err := c.exportHashSlotSnapshot(ctx, migration.Source, migration.HashSlot)
  ```
- **触发路径**（任一 hash-slot 迁移的正常流程）：
  1. fence 在 worker 仍处 `PhaseSnapshot` 时即被提案并确认（上述代码），`state.FenceIndex != 0` 之后 `isHashSlotFenced`（statemachine.go:406）对该 hashSlot 的**所有非迁移维护命令**返回 fenced，`ApplyBatch:198-206` 直接返回 `ApplyResultHashSlotFenced` 而不执行。
  2. worker 只有在 `MarkSnapshotComplete` 设置 `snapshotAt` 后才 `PhaseSnapshot → PhaseDelta`（worker.go:218-221），因此 **Controller 表进入 PhaseDelta 必然晚于 fence 落盘**。
  3. source 的 runtime outgoing 标记仅在表相位 ∈ {Delta, Switching} 时发布（`deltaMigrationRuntimeForSlot:64-69`），而 `stageMigrationOutbox`（statemachine.go:546-560）要求 `m.migrations[hashSlot].phase ∈ {Delta, Switching}`。
  4. 于是"普通写进入 outbox 并被 live 转发"需要同时满足 `phase ∈ {Delta,Switching}`（⇒ fence 已落盘）与 `FenceIndex == 0`（fence 未落盘）——**逻辑上互斥**。实测唯一会被 live 转发的是 `stageMigrationFence`（statemachine.go:505-530）产出的 fence marker 本身。
  5. 连带地：`Worker.UpdateProgress`（worker.go:110-118）全仓无生产调用方（`hashSlotMigrationWorker` 接口 `hashslot_migration.go:24-31` 根本不含该方法），`Progress` 只在 `MarkSnapshotComplete` 被 `Update(sourceApplyIndex, sourceApplyIndex, ...)` 调用一次 → `DeltaLag` 恒为 0、`StableWindowStart` 立即置位。`PhaseDelta → PhaseSwitching` 的"复制延迟已稳定"判据（worker.go:222-225 + progress.go:17-22）**退化为一个 stableWindow(1s) 定时器**，从不度量 source/target 实际差距。
- **后果**：(a) 迁移中的 hashSlot 从迁移开始（而非切换瞬间）就写不可用，直到 finalize——`AddSlot`/`Rebalance` 会按 `ComputeAddSlotPlan` 搬迁 `hashSlotCount/slotCount` 个 hashSlot（每 source 并发上限 2、全局 4，worker.go:21-22），表现为键空间上的滚动写中断，而 FLOW.md 5.7 与"避坑清单"都把语义描述成"source 继续服务、live write 转发到 target"。(b) FLOW.md、durable outbox replay、`DeltaLag`/`stableThreshold`/`stableWindow` 共同构成的"延迟收敛后再切换"安全叙事实际不存在；真正起作用的只有 fence + outbox drain + ack。维护者若据 FLOW.md 判断迁移期可用性或据 `DeltaLag` 判断切换时机，都会得到错误结论。
- **建议**：要么把 fence 推迟到 Switching 前置（并让 Delta 阶段真正走 outbox + 有序转发），要么更新 FLOW.md 与配置注释，明确迁移为"停写搬迁"，并删除 `Progress`/`UpdateProgress` 死机制。

### [P2] 6. migrationObserver 把 200ms 的 *timeout* 常量当作循环周期，并在零迁移稳态下每 200ms 对每个本地 Slot 做一次 Pebble 扫描

- **位置**：`pkg/cluster/hashslot_migration.go:347-363`（`hashSlotMigrationStores`）；周期来源 `pkg/cluster/config.go:107-108` + `:475-477`；调用 `pkg/cluster/cluster.go:523-526` 与 `:1973-1995`
- **类别**：性能 / 架构
- **代码**：
  ```go
  func (c *Cluster) hashSlotMigrationStores() map[multiraft.SlotID]hashSlotDeltaOutboxStore {
      c.runtimeStateMachinesMu.RLock()
      defer c.runtimeStateMachinesMu.RUnlock()
      stores := make(map[multiraft.SlotID]hashSlotDeltaOutboxStore)   // 每次调用新建 map
      for slotID, sm := range c.runtimeStateMachines {
          store, ok := sm.(hashSlotDeltaOutboxStore)
          if !ok { continue }
          stores[slotID] = store
      }
      return stores
  }
  ```
- **触发路径**：
  1. `c.migrationObserver = newObserverLoop(c.controllerObservationInterval(), ...)`，而 `controllerObservationInterval()`（`readiness.go:40-42`）返回 `Timeouts.ControllerObservation`，其默认值是 `defaultControllerObservationTimeout = 200 * time.Millisecond`（`config.go:28`、`:475-477`）。该字段位于 `Timeouts` 结构中一组超时字段之间，且不像同结构里的 `ObservationSlowSyncInterval` / `PlannerSafetyInterval` 那样带 "Interval" 命名与注释——**一个超时常量被复用成了循环周期**，于是迁移观测循环每秒跑 5 次。
  2. 每次 tick 先过 `hasActiveMigrationObservationWork()`（cluster.go:1973）。零迁移稳态（默认配置即如此，迁移开关默认关闭）下前三个快速判定全为 false，落到第四项 `c.hasPersistedHashSlotMigrationStates(context.Background())`（cluster.go:1993-2000）：对 `hashSlotMigrationStores()` 返回的**每个**状态机执行一次 `ListHashSlotMigrationStates(ctx)`，即一次 Pebble 前缀扫描。
  3. 结果：本节点有 N 个 Slot 时，稳态下每秒 5N 次 Pebble 空扫 + 5 次 map 分配（持 RLock）。N=64 时为 320 次/秒，纯属无效开销，且 `context.Background()` 使这些存储读没有任何 deadline。
- **后果**：稳态常驻 CPU / Pebble iterator 开销与 Slot 数线性相关；FLOW.md 5.1 ⑦ 声称该循环"仅在有 active migration / pending abort 时推进"，但判定闸门自身就是最昂贵的部分，文档与实现不符。
- **建议**：为迁移观测循环引入独立的 `MigrationObserveInterval`（秒级）；对"无持久化 migration state"的结论做粘性缓存，仅在 finalize/abort 后失效。

### [P2] 7. reconciler.Tick 任一阶段出错即整体 return，单个坏 Slot 每轮都阻塞该节点上所有更大 SlotID 的调和

- **位置**：`pkg/cluster/reconciler.go:62-70`、`:88-96`、`:109-114`
- **类别**：健壮性
- **代码**：
  ```go
  for _, assignment := range assignments {
      if !assignmentContainsPeer(assignment.DesiredPeers, uint64(a.cluster.cfg.NodeID)) { continue }
      desiredLocalSlots[assignment.SlotID] = struct{}{}
      _, hasView := viewByGroup[assignment.SlotID]
      if err := a.cluster.ensureManagedSlotLocal(ctx, multiraft.SlotID(assignment.SlotID), assignment.DesiredPeers, hasView, false); err != nil {
          return err            // ← 后续 assignment、loadTasks、关闭多余 Slot、任务执行全部跳过
      }
  }
  ```
- **触发路径**：
  1. `assignments` 来自 `a.cache.Snapshot()`，而 cache 由 `sortedObservationAssignments`（`agent.go:425-435`）按 `SlotID` 升序写入 —— 遍历顺序**每轮都相同且确定**。
  2. 节点上 slot 3 的 `ensureLocal` 持续失败（`cfg.NewStorage` 出错、`storage.InitialState` 出错、磁盘满导致 `OpenSlot` 失败，`slot_manager.go:36-53`、`:66-72`）。
  3. 每次 Tick 都在 slot 3 处 `return err`：`loadTasks`（73 行）、`protectedSourceSlots`（78-98 行）、"关闭多余 Slot"（99-119 行）、任务执行循环（121-204 行）**一次都不会执行**。
  4. 错误向上经 `ApplyAssignments` 返回，在 `syncObservationDeltaOnce`（`cluster.go:1958`）被 `_ =` 丢弃——无日志、无 hook。
- **后果**：单个 Slot 的持续性存储故障使该节点所有其他 Slot 的 Repair / Rebalance / LeaderTransfer 任务永不执行、应关闭的 Slot 永不关闭。故障域从一个 Slot 放大到整个节点，且因错误被吞而不可观测。
- **建议**：按 Slot 收集错误后继续循环，聚合上报；至少让"关闭多余 Slot"与任务执行不被前序 ensureLocal 失败短路。

### [P2] 8. managed-slot RPC 暴露破坏性控制面操作且无任何调用方身份校验

- **位置**：`pkg/cluster/slot_handler.go:64-97`
- **类别**：安全
- **代码**：
  ```go
  case managedSlotRPCChangeConfig:
      err := h.cluster.managedSlots().changeConfigLocal(ctx, multiraft.SlotID(req.SlotID), multiraft.ConfigChange{
          Type:   req.ChangeType,
          NodeID: multiraft.NodeID(req.NodeID),
      })
      return marshalManagedSlotError(err)
  case managedSlotRPCTransferLeader:
      err := h.cluster.managedSlots().transferLeaderLocal(ctx, multiraft.SlotID(req.SlotID), multiraft.NodeID(req.TargetNode))
  ```
- **触发路径**：`rpcServiceManagedSlot`（=20）在 `cluster.go` 注册为公开 RPC handler。传输层无鉴权、无 TLS（同发现 4 的 grep 结论）。任何能连上集群端口的一方可直接构造请求：
  - `change_config` + `RemoveVoter`：Raft 本身只校验"本节点是 leader"（`changeConfigLocal` 在非 leader 时返回 `ErrNotLeader`），**不校验请求者是谁**。对 leader 连续发送 RemoveVoter 可把 3 副本 Slot 削到 1 副本，或移除到不足法定人数 → 该 Slot 永久不可用（需 `RecoverSlot` 人工介入）。
  - `import_snapshot`（85-97 行）：虽校验了本地是 leader，但随后 `importHashSlotSnapshotLocal` 直接把请求体 `req.Snapshot` 作为该 hashSlot 的数据导入 —— 攻击者可用任意字节覆盖该 hashSlot 的元数据。
  - `compact`（73-84 行）：截断目标节点本地 Raft log。
  对比同文件 `managedSlotRPCImportSnapshot` 特意做了 leader 校验，可见作者考虑过越权面，但缺失的是"调用方必须是本集群成员"这一层。
- **后果**：在无鉴权传输之上把 Raft 成员变更与状态机数据导入暴露为任意可调用接口；一次调用即可造成不可自动恢复的 Slot 不可用或数据覆盖。
- **建议**：为集群传输引入对等身份校验（与 `JoinCluster` 的 token 同源即可），并在 managed-slot handler 校验请求来源属于当前 membership。

### [P2] 9. getTask 确认读失败时复用同轮旧任务快照直接执行，可对已被 Controller 替换的任务发起一次真实成员变更

- **位置**：`pkg/cluster/reconciler.go:163-197`
- **类别**：分布式一致性
- **代码**：
  ```go
  freshTask, err := a.getTask(ctx, assignment.SlotID)
  switch {
  case errors.Is(err, controllermeta.ErrNotFound):
      continue
  case err != nil:
      if !controllerReadFallbackAllowed(err) { return err }
      // Reuse the task snapshot we already loaded earlier in this tick when
      // the confirm read only failed transiently. ...
      freshTask = task
  case !sameReconcileTaskIdentity(freshTask, task):
      continue
  }
  task = freshTask
  ...
  execErr := a.cluster.executeReconcileTask(ctx, assignmentTaskState{ ..., task: task })
  ```
- **触发路径**：
  1. Tick 开头 `loadTasks` 取到 slot 5 的任务 T1（Rebalance，SourceNode=A，TargetNode=B）。
  2. Controller 侧随后把 slot 5 的任务改为 T2（例如 onboarding 启动后 `PauseRebalance` + `LockedSlots` 导致原 Rebalance 被替换，FLOW.md 5.3 ④），或 T1 被删除重建为不同 target。
  3. 本节点的 `getTask` 确认读遭遇可降级错误（controller leader 正在切换 / deadline exceeded，`controllerReadFallbackAllowed` 为真）→ 走 fallback 分支，`freshTask = task`（即 T1），**跳过了 `sameReconcileTaskIdentity` 这道唯一的新鲜度闸门**。
  4. `executeReconcileTask` 按 T1 执行 `slot_executor.go:127-163` 的完整序列：AddLearner(B) → waitForCatchUp → PromoteLearner(B) → ensureLeaderMovedOffSource(A) → RemoveVoter(A)。这些都是**已提交的 Raft 成员变更**。
  5. 随后 `reportTaskResult` 上报 T1 的结果，Controller 因 identity 不匹配丢弃（FLOW.md 避坑清单"pendingTaskReport 防重复上报"）——但成员变更已经落盘且无人知晓。
- **后果**：在 controller 读抖动窗口内，节点可依据过期任务对 Slot 副本集做一次真实变更，而 Controller 既不知情也不会记录结果；副本集与 Controller 期望不一致，需下一轮 planner 重新纠偏。注释显示这是为避免"retry 任务永久停滞"而有意放宽的权衡，但放宽的同时丢掉了身份校验。
- **建议**：fallback 复用旧快照时仅允许"上报结果"，不允许执行；或在 fallback 前校验 assignment 的 `ConfigEpoch` / `BalanceVersion` 未变。

### [P2] 10. nextNodeOnboardingJobID 通过全表扫描求 max+1 生成 job ID，并发创建计划会产生同名 job 并静默覆盖已审核的计划

- **位置**：`pkg/cluster/onboarding.go:376-393` 与 `:227-235`
- **类别**：正确性
- **代码**：
  ```go
  func (c *Cluster) nextNodeOnboardingJobID(ctx context.Context, now time.Time) (string, error) {
      jobs, _, _, err := c.controllerMeta.ListOnboardingJobs(ctx, 0, "")
      if err != nil { return "", err }
      prefix := "onboard-" + now.UTC().Format("20060102") + "-"
      maxSuffix := 0
      for _, job := range jobs {
          if !strings.HasPrefix(job.JobID, prefix) { continue }
          suffix, err := strconv.Atoi(strings.TrimPrefix(job.JobID, prefix))
          if err == nil && suffix > maxSuffix { maxSuffix = suffix }
      }
      return fmt.Sprintf("%s%06d", prefix, maxSuffix+1), nil   // 读-改-写，无原子性
  }
  ```
- **触发路径**：
  1. `CreateNodeOnboardingPlan` 经 manager HTTP 入口（`internal/access/manager` → `internal/usecase/management`）到达 controller leader，运行在各自的请求 goroutine 上。两位运维同时创建计划（或界面重复提交）即构成并发。
  2. 两次调用都读到同一批 jobs、算出同一个 `maxSuffix`，返回**相同的 jobID**。
  3. 二者都走 `proposeNodeOnboardingJobUpdate(ctx, NodeOnboardingJobUpdate{Job: &job})`（onboarding.go:232）—— 注意此处 **`ExpectedStatus` 为 nil**。
  4. apply 侧 `pkg/controller/meta/onboarding_store.go:85-109`：`expectedStatus == nil` 跳过 CAS 校验；`job.Status` 为 `Planned`（非 `Running`）跳过"至多一个 running job"校验 → 直接 `writeBatchLocked(writes)` 无条件写入该 jobID。
  5. Raft 串行化后，第二个提案**覆盖**第一个：先提交的那份已审核计划（含 moves、blocked reasons、plan fingerprint）连同其 target 节点一起消失，运维以为自己审核的计划仍在。
  （对照：`startNodeOnboardingJobOnLeader` 走 `ExpectedStatus: &Planned`，确有 CAS 保护；"至多一个 running job"亦由 store 层 97-107 行强制。缺保护的只有 create 路径。）
- **后果**：并发创建扩容计划时一份已审核计划被静默丢弃，运维随后 `start` 的是另一个 target 的计划——与 FLOW.md "POST plan：生成 durable planned job 供审核""start：只启动已审核的 planned job，不重新生成计划" 的审核语义相违背。
- **建议**：create 路径改用"jobID 已存在则拒绝"的条件写（store 层增加 create-if-absent），或用单调序列 / UUID 生成 jobID。

### [P3] 11. observationCache.nodes 被排除在 TTL 淘汰之外，而同结构其余三个 map 均被淘汰

- **位置**：`pkg/cluster/observation_cache.go:172-188`
- **类别**：无界增长
- **代码**：
  ```go
  func (c *observationCache) evictStaleRuntimeViewsLocked() {
      if c == nil || c.runtimeViewTTL <= 0 { return }
      ...
      for nodeID, receivedAt := range c.runtimeReceivedAt {
          if receivedAt.IsZero() || now.Sub(receivedAt) <= c.runtimeViewTTL { continue }
          delete(c.runtimeObservedAt, nodeID)
          delete(c.runtimeReceivedAt, nodeID)
          delete(c.runtimeViewsByNode, nodeID)
      }                                    // ← c.nodes 未被淘汰
  }
  ```
- **触发路径**：`c.nodes` 由 `applyNodeReport`（45-64 行）按 `report.NodeID` 写入，仅在 `reset()`（158-170 行，controller leader 变更时）整体清空。节点被从 membership 移除后，其 `nodeObservation` 条目在 leader 上一直残留到下次 leader 变更；`snapshot()`（122-143 行）每次都把这些陈旧条目一并返回。增长受"历史上曾加入过的节点数"约束（dynamic join 模式下 leader 会拒绝未经 `JoinCluster` 的未知节点），因此是随运维churn 缓慢累积而非远程可驱动的无界增长。这正是审计清单里"有 prune 逻辑的缓存是否所有内部 map 都被覆盖"的典型漏项。
- **后果**：controller leader 上缓慢累积的陈旧节点观测；`snapshot().Nodes` 含已下线节点条目。
- **建议**：在 `evictStaleRuntimeViewsLocked` 中按 `ObservedAt` 一并淘汰 `c.nodes`。

### [P3] 12. assignmentCache 手工枚举字段，静默丢弃 PreferredLeader 与 LeaderTransferCooldownUntil

- **位置**：`pkg/cluster/assignment_cache.go:28-43` 与 `:94-102`
- **类别**：架构（潜在缺陷，当前无消费方）
- **代码**：
  ```go
  cloned = append(cloned, controllermeta.SlotAssignment{
      SlotID:         assignment.SlotID,
      DesiredPeers:   append([]uint64(nil), assignment.DesiredPeers...),
      ConfigEpoch:    assignment.ConfigEpoch,
      BalanceVersion: assignment.BalanceVersion,
  })                       // 6 个字段只搬 4 个
  ```
  `controllermeta.SlotAssignment`（`pkg/controller/meta/types.go:104-117`）另有 `PreferredLeader` 与 `LeaderTransferCooldownUntil`。同仓的 `cloneSlotAssignment`（`controller_metadata_snapshot.go:56-60`）采用正确模式 `out := src` + 深拷贝 slice，天然全字段安全。
- **当前是否可触发**：不可。经 grep 核实两字段的全部读取方（`operator.go:376`、`internal/usecase/management/slots.go:198,217`、`internal/access/manager/slots.go:300`、`tasks.go:135`）都经由 `ListSlotAssignments` / `ListSlotAssignmentsStrict`（`cluster.go:1630-1683`，走 `RefreshAssignments` 或 `controllerMeta.ListAssignments`，均返回完整结构）；`cluster.go:1655` 的 `ListCachedAssignments()` 回退分支需 `controllerClient` 与 `controllerMeta` 同时为 nil，生产不可达。reconciler 与 `internal/app/deliveryrouting.go:799` 只读 `DesiredPeers`。故记为无触发路径的架构隐患而非功能缺陷。
- **后果**：下一个从 `assignmentCache` 读取软 leader 偏好或 transfer 冷却时间的功能会静默拿到零值。
- **建议**：改用 `cloneSlotAssignment` 统一克隆语义。

### [P3] 13. 三处死代码 / 命名失配：scalein_safety.go 并非缩容闸门、slotmigration/snapshot.go 无调用方、DecodeHashSlotTable 不校验相位与 source/target 合法性

- **位置**：`pkg/cluster/scalein_safety.go:1-18`、`pkg/cluster/slotmigration/snapshot.go:11-25`、`pkg/cluster/hashslot/hashslottable.go:310-322`
- **类别**：死代码 / 文档一致性 / 健壮性
- **代码**：
  ```go
  // scalein_safety.go 全文仅此一个函数，不做任何 gating
  func (c *Cluster) ListActiveMigrationsStrict(ctx context.Context) ([]HashSlotMigration, error) {
      if c == nil { return nil, ErrNotStarted }
      if _, err := c.ListSlotAssignmentsStrict(ctx); err != nil { return nil, err }
      table := c.GetHashSlotTable()
      if table == nil { return nil, ErrNotStarted }
      return table.ActiveMigrations(), nil
  }
  ```
  ```go
  // DecodeHashSlotTable —— 相位与 source/target 均不校验
  phase := MigrationPhase(data[offset+2])          // 接受 0..255，合法值仅 0..3
  sourceSlot := multiraft.SlotID(binary.BigEndian.Uint64(data[offset+4 : offset+12]))
  targetSlot := multiraft.SlotID(binary.BigEndian.Uint64(data[offset+12 : offset+20]))
  table.migrations[hashSlot] = HashSlotMigration{ HashSlot: hashSlot, Source: sourceSlot, Target: targetSlot, Phase: phase }
  ```
- **触发路径**：
  - `scalein_safety.go`：文件名暗示存在集中式缩容安全闸门，实际只是管理端的严格活跃迁移查询，不拒绝任何操作。真正的缩容校验分散在 `operator.go` 的 `RemoveSlot`（活跃迁移冲突、不得移除最后一个物理 Slot）与 Controller 提案侧。FLOW.md 5.9 描述了这些校验但从未提及本文件。功能本身正确（严格读不降级、nil 防护齐备），问题纯在命名误导。
  - `slotmigration/snapshot.go`：`grep -rn "slotmigration.ExportHashSlot|slotmigration.ImportHashSlot"` 生产零命中（真实导入导出走 `managed_slots.go:322-386`，且使用请求 ctx）。这两个导出函数是早期残留，内部用 `context.Background()`，若被误用会绕过取消传播。
  - `DecodeHashSlotTable`：相位字节来自线路（heartbeat 响应 / `list_assignments` 携带的表，经 `applyHashSlotTablePayload` 解码）。相位取 200 时，`deltaMigrationRuntimeForSlot:64` 判定非 Delta/Switching → 不发布 outgoing 标记；`advanceSwitchingHashSlotMigrations:264` 要求 `want.Phase == PhaseDelta` → 跳过；本地 worker 停在 Snapshot 直到 `defaultMigrationStallTimeout`（worker.go:23，10 分钟）超时 abort。同时不像 `StartMigration:92` 那样校验 `source != 0 && target != 0 && source != target`。影响可自愈（10 分钟后 abort），故记 P3。
- **后果**：维护者据文件名误判存在缩容闸门；畸形相位使单个迁移停滞 10 分钟后才被回收。
- **建议**：`scalein_safety.go` 重命名或在 FLOW.md 补注；删除 `slotmigration/snapshot.go`；`DecodeHashSlotTable` 对相位与 source/target 做与 `StartMigration` 同级的校验。

### [P3] 14. forwardHashSlotDelta 的 goroutine 无法取消、不被 Stop 等待，且退出条件只有 c.stopped

- **位置**：`pkg/cluster/hashslot_migration.go:77-112`
- **类别**：资源 / 并发
- **代码**：
  ```go
  go c.forwardHashSlotDelta(target, cloned)          // 无 WaitGroup、无 ctx
  ...
  for c != nil && !c.stopped.Load() {                // c 不可能变 nil；唯一出口是 stopped
      ctx, cancel := context.WithTimeout(context.Background(), timeout)
      err := c.proposeHashSlotDelta(ctx, target, cmd.HashSlot, payload)
      cancel()
      if err == nil { return }
      time.Sleep(hashSlotDeltaForwardRetryInterval)  // 100ms
  }
  ```
- **触发路径与规模界定**：这是 gosec G118（`pkg/cluster/hashslot_migration.go:89`）报出的点。按发现 5 的分析，`stageMigrationOutbox` 在现有流程中不可能为普通写产出转发项，故每个迁移实际只会 live 转发一条 fence marker；叠加 `maxConcurrentMigrations = 4`（worker.go:21），并发此类 goroutine 上界约为 4，**不会按写流量堆积**。剩余的真实缺陷是：(a) 循环用 `context.Background()`，不继承任何取消；(b) `Cluster.Stop()` 不 join 这些 goroutine（无 WaitGroup），`stopped` 置位与最后一次 `Load()` 之间存在短暂逃逸窗口，goroutine 可能在 Stop 返回后仍尝试 `ProposeWithHashSlot`，访问正在关闭的 runtime；(c) `c != nil` 是恒真的死条件。FLOW.md 避坑清单已明示"live best-effort、恢复依赖 durable outbox 而非等待 goroutine 发完"，即不 join 是有意设计。
- **后果**：Stop 后短暂的 use-after-close 窗口；目标不可达时最多 4 个 goroutine 以 100ms 周期长期自旋。
- **建议**：传入派生自 Cluster 生命周期的 ctx 并在 Stop 时 cancel + WaitGroup join；去掉恒真的 `c != nil`。

### [P3] 15. onboarding job 分页每次拉全表内存排序，且游标失效时静默返回空页

- **位置**：`pkg/cluster/onboarding.go:292-336`
- **类别**：性能 / 健壮性
- **代码**：
  ```go
  jobs, _, _, err := c.controllerMeta.ListOnboardingJobs(ctx, 0, "")   // 全量
  if err != nil { return nil, "", false, err }
  sortNodeOnboardingJobsForAPI(jobs)
  start := 0
  if cursor != "" {
      cursorTime, cursorID, err := decodeNodeOnboardingCursor(cursor)
      if err != nil { return nil, "", false, ErrInvalidConfig }
      for start < len(jobs) {
          job := jobs[start]
          if job.CreatedAt.Equal(cursorTime) && job.JobID == cursorID { start++; break }
          start++                      // 未命中则一路走到 len(jobs)
      }
  }
  if start >= len(jobs) { return nil, "", false, nil }   // ← 静默空页，无错误
  ```
- **触发路径**：(a) 每个分页请求都从 Pebble 读出**全部** onboarding job 再内存排序 + 线性定位游标；job 无保留策略，列表随时间单调增长，分页语义并未减少 I/O。(b) 游标指向的 job 已被删除或被发现 10 的同名覆盖清掉时，循环扫到尾部，`start >= len(jobs)` 成立，返回 `(nil, "", false, nil)` —— 管理端看到的是"没有更多数据"而非"游标失效"，无法区分翻页结束与游标错误。
- **后果**：job 数量增长后管理端翻页开销线性上升；失效游标表现为静默的空结果页。
- **建议**：store 层支持按 `created_at` 的游标分页；游标未命中时返回明确错误。

---

## 已排除的候选项

### 阶段 0 机械发现核实

`grep 'pkg/cluster/' /tmp/octo-gosec.txt` 命中 73 条，属本分片文件的共 **35 条 G115 + 1 条 G118**。staticcheck 对 `pkg/cluster` **零命中**（`grep -c 'pkg/cluster' /tmp/octo-staticcheck.txt` = 0，119 条全在其他目录），无待核项。

G115 逐条核实，**全部为误报**，理由按组归纳：

- `slot_manager.go:69,74,88,93,105,110,168,241,269,336,339,370,464`（13 条，uint64→uint32）—— `SlotID` 的权威类型是 `controllermeta.SlotAssignment.SlotID uint32`（`pkg/controller/meta/types.go:106`），`multiraft.SlotID` 为 uint64 别名，转换只是在同一 uint32 值域上往返。且 `controllerObservedLeader`（`slot_manager.go:336-338`）本身就带 `uint64(uint32(slotID)) != uint64(slotID)` 的显式上界校验后才转换。无远程可控的无上界输入。
- `reconciler.go:100,103,106,111,116`（5 条）、`slot_executor.go:109,115,188`（3 条）、`managed_slots.go:373`、`onboarding.go:441,442,453`、`assignment_cache.go:115`、`slot_raft_compaction.go:92` —— 同上，均为 uint32 SlotID 经 `multiraft.SlotID` 中转后回到 uint32/map key。
- `slot_handler.go:41`（uint64→int）—— `limit := int(req.Limit)` 紧随 `if req.Limit > maxSlotLogEntryLimit { limit = maxSlotLogEntryLimit }`（42-44 行，uint64 与常量比较）。`req.Limit ∈ (200, 2^64)` 全部被该比较拦截并钳到 200，`limit` 最终恒在 `[0,200]`。误报。
- `slot_log_entries.go:65,133`（int→uint64）—— 65 行 `uint64(opts.Limit)` 已由 `normalizeSlotLogEntriesOptions`（28-36 行）或 handler 的钳制保证非负且 ≤200；133 行为 `len(entry.Data)`，非负。
- `snapshot_chunks.go:108,165`（int→uint64）—— 108 行 `uint64(existingLen)`、165 行 `uint64(existingLen)` 均取自 `len(chunk.data)`，非负。（该文件的真实问题是 `make([]byte, int(chunk.total))` 缺上界，已作为发现 4；gosec 未报该行。）
- `slotmigration/progress.go:31`（uint64→int64）—— 位于 `if sourceApplyIndex > targetApplyIndex` 分支内，差值非负且两侧都是 Raft log index（实际上界为日志长度），不溢出 int64。
- `hashslot/hashslottable.go:254`（int→uint16）—— `uint16(len(migrations))`，迁移按 `map[uint16]` 去重，len ≤ 65536 且实际受 `maxConcurrentMigrations` 远远限制。`:330`（uint32→uint16）—— `crc32 % uint32(hashSlotCount)`，结果 < hashSlotCount ≤ MaxUint16。
- `hashslot_migration.go:89` G118 —— **非误报，已作为发现 14 收录**，但严重度经分析后远低于 gosec 提示（见该条对规模上界的界定）。

非本分片的命中（`cluster.go`、`codec*.go`、`controller_*.go`、`config.go`、`readiness.go`、`operator.go`）按分片纪律跳过。

### 阶段 2 自我驳回（曾入候选、复核后删除）

- **`pendingHashSlotAborts` / `pendingHashSlotDeltaCleanups` 无锁并发访问 → fatal "concurrent map writes"**。曾拟为 P1。驳回：`observeHashSlotMigrations` 的生产调用方经 grep 确认只有 `migrationObserveOnce`（`cluster.go:1970`，单一 `migrationObserver` goroutine）。另两处调用在 `observeOnce`（`cluster.go:683,698`），而 `grep -rn "observeOnce\b"` 显示该函数**仅被测试调用**（`cluster_test.go:2605,2707`、`agent_internal_integration_test.go:139`），`startObservationLoop` 接的七个循环均不含它。`AbortHashSlotMigration` / `finalizeHashSlotMigration` / `completeHashSlotDeltaOutboxCleanup` 亦无 `hashslot_migration.go` 之外的调用方（管理端 `AddSlot`/`RemoveSlot`/`Rebalance` 只到 `operator.go` 的 `StartHashSlotMigration`，不触碰这两个 map）。故两个 map 实为单 goroutine 独占，无竞态。
- **live delta 转发乱序导致 source/target 状态分歧**。曾拟为 P0（多个并发 goroutine 各自独立重试 → target 按到达顺序 apply；target 的幂等是按 `(hashSlot, sourceSlot, sourceIndex)` 集合去重而非序列号，不能纠正乱序）。驳回：见发现 5 的推导——`stageMigrationOutbox` 要求相位 ∈ {Delta,Switching}（⇒ fence 已落盘）而普通写要求 `FenceIndex == 0`，二者互斥，故普通写根本不会进入 live 转发；每个迁移只转发一条 fence marker，不存在两条 delta 竞争。该推导反而成为发现 5。
- **exportHashSlotSnapshot 先读 AppliedIndex 后导出快照的 TOCTOU**（`managed_slots.go:332-341`）。驳回：`completeHashSlotSnapshot`（`hashslot_migration.go:643-650`）在导出前已确认 fence 生效，两次操作之间不可能有普通写 apply；且 `sourceApplyIndex` 事实上未被用于过滤 outbox replay（`replayHashSlotDeltaOutbox:418` 的 fromIndex 传 0）。
- **controller leader 变更后 follower 携旧 generation 的 revision 请求 delta，新 leader 的计数器已归零 → 增量永远为空**。曾拟为 P0。驳回：`controllerHost.buildObservationDelta`（`controller_host.go:280-284`）在 `req.LeaderID != currentLeaderID || req.LeaderGeneration != currentGeneration` 时强制 `ForceFullSync = true`，该路径已被正确封堵。
- **`observationCache.runtimeViewTTL` 未赋值导致 TTL 淘汰完全失效**。驳回：`controller_host.go:140` 赋 `3 * timeouts.ObservationRuntimeFullSyncInterval`，TTL 生效。（但 `c.nodes` 未纳入淘汰，已作为发现 11。）
- **`fullSyncDueLocked` 的 `!r.lastFullSync.IsZero()` 使周期性全量同步永不首次触发**。驳回：`cluster.go:493` 在创建 reporter 后立即 `requestFullSync()`，首个全量同步必然发生并置位 `lastFullSync`，兜底周期随之武装。
- **并发 `StartNodeOnboardingJob` 产生两个 running job → planner 永久 `PauseRebalance`**。驳回：apply 侧 `pkg/controller/meta/onboarding_store.go:97-107` 在 `job.Status == Running` 时强制校验"除自身外无其他 running job"，经 Raft 串行化后不变式成立。`onboarding_executor.go:20` 取 `runningJobs[0]` 因此安全。（create 路径缺失保护是另一回事，已作为发现 10。）
- **`localSlotLogEntries` 每次请求 `cfg.NewStorage(slotID)` 却不 Close → 句柄泄漏**。驳回：`newStorageFactory`（`internal/app/build.go:1617-1621`）返回 `raftDB.ForSlot(...)`，是共享 Pebble 之上的轻量视图，非独立 DB 句柄，无需关闭、不占额外 fd。
- **`ensureLeaderMovedOffSource` / `ensureLeaderOnTarget` 走无 fencing 的 `transferLeadership`（expectedLeader=0），而 LeaderTransfer 任务走严格的 `transferLeadershipFrom`——"宽松版用在主路径"**。驳回：`ensureLeaderMovedOffSource`（`slot_manager.go:355-373`）在循环内每轮重读 `currentLeader` 并仅在 `leaderID == sourceNode` 时才发起转移；即便外层判断与转移之间 leader 已变，效果只是多做一次无害的领导权转移，不破坏安全性。
- **`waitForCatchUp` 用可能来自 stale controller 观测的 leader 的 CommitIndex 作追赶基准**（`slot_manager.go:293-298` + `currentLeader:326-328` 的 `controllerObservedLeader` 回退）。驳回：即使基准偏低导致 PromoteLearner 提前，Raft 的选举安全性（候选人日志必须足够新才能当选）保证已提交条目不丢；后果限于副本短期滞后，无数据丢失触发路径。
- **`reconciler.Tick:40-45` / `:49-54` 吞掉 `listControllerNodes` / `listRuntimeViews` 错误后继续执行**。驳回：`nodeByID` 为空时 `deterministicAliveTaskExecutor:319-334` 的 `ok && node.Status != Alive` 条件使所有 peer 视为存活、退化为最小 NodeID 执行，与正常语义一致；`liveRuntimeViews=false` 时 LeaderTransfer 走 `continue`（148-153 行）跳过本轮且不上报——均为有意的保守降级。
- **scoped reconcile 下 `assignments` 被过滤为空但 `scoped=true`，导致 scope 内的 Slot 被误关闭**（`reconciler.go:30-36` + `:99-108`）。驳回：这正是 RemoveSlot 的预期行为（assignment 已删除 → 关闭该 Slot）；且 `filterAssignmentsByScope:209-211` 在 `len(scope)==0` 时返回原列表，`reconcileScopeFromObservationDelta`（`observation_sync.go:436-441`）在 delta 含 node 变化时返回 nil 强制全量，无误关闭路径。
- **`HashSlotTable` 所有方法无锁**。驳回：实例只经 `router.hashSlotTable`（`atomic.Pointer`）以"读旧引用 / `Store(table.Clone())` 发布新副本"方式流转（`router.go:40-46`），读者持不变快照，写者持独立副本，不存在共享可变实例。
- **`hashslot/rebalancer.go` 的 `ComputeRebalancePlan` 死循环风险**（`:106-123` 的无条件 `for`）。驳回：每轮迭代必从 `owned[donor]` 弹出一项（`popOwnedHashSlot:233-241`），`owned` 有限，耗尽时返回 false 触发 `break`，循环上界为总 hashSlot 数。
- **`tableActiveSlotIDsExcluding` 的 `filtered := slots[:0]` 原地复用底层数组**（`hashslot/rebalancer.go:148-158`）。驳回：`slots` 由 `tableActiveSlotIDs` 内部新建，无外部别名。
- **`node_health_scheduler.go` 整体**（recon 报 clean）。复核确认 clean：timer 回调的 generation + 精确时间戳双重校验（`handleDeadline:112-131`）正确防止过期转换；`beginPending`/`clearPending`（386-408 行）对同一 (node, desired) 去重；`buildNodeStatusTransition`（410-452 行）的状态机迁移条件完备并携带 `ExpectedStatus` 做 CAS；提案在锁外执行。唯一可议的是每次心跳 Stop + 重建两个 `time.AfterFunc`（80-100 行）带来的 timer churn，但该 scheduler 只在 controller leader 运行、节点规模有限，不足以成条。
- **`replaceAssignmentsLocked` 用 `reflect.DeepEqual` 比较含 slice 的结构**（`observation_sync.go:300`）。驳回：DeepEqual 对 slice 做内容比较，语义正确（不会因 `cloneSlotAssignment` 重新分配底层数组而误判变更）。仅有的问题是相比兄弟函数 `replaceTasksLocked:320` 的 `current == task` 多一层反射开销，量级不足以成条。
- **`slot_handler.go` 的 `change_config` / `transfer_leader` 缺少 leader 校验**。驳回：`changeConfigLocal` / `transferLeaderLocal` 在非 leader 时由 Raft 返回 `ErrNotLeader`（`slot_manager.go:147-149`、`:229-231`），leader 语义由 Raft 强制。真正缺失的是**调用方身份**校验，已作为发现 8。
- **`snapshot_chunks.go:114` `overlapsExistingChunk` 的 O(n) 扫描**。驳回：chunk 数约为 `total / RaftSnapshotChunkSize`（8MiB），量级为数十，且条目仅在拼装窗口内存活。

---

## 本分片整体评价

这部分代码的工程质量明显高于该仓库的平均水平：迁移协议具备 fence + durable outbox + ack + cleanup 的完整四件套，Controller 迁移命令强制携带 source/target identity 以防旧命令污染新迁移，LeaderTransfer 有成体系的执行时安全校验并以 deterministic checker 做 fail-closed 上报，nodeHealthScheduler 的 timer generation 双校验写得相当扎实，测试覆盖也很密（`cluster_test.go` 中 50 余处迁移流程用例）。FLOW.md 是难得的活文档，绝大部分描述可与代码逐行对上。

最需要优先处理的是 **发现 1**：observation delta 协议缺少 assignment 删除 tombstone，而生产环境唯一能重建全量 assignment 的 `SyncAssignments` 恰好挂在从未被接入循环的 `observeOnce` 上——两个缺陷叠加使 `RemoveSlot` 的收敛承诺在稳态增量路径上根本不成立，被移除的 Slot 的 Raft group 会在各副本上无限期存活。其次是 **发现 4**（远程无上界 `make([]byte, total)` → 不可 recover 的 fatal OOM，且集群传输层无任何鉴权）与 **发现 2**（controller failover 撞上发送窗口即丢失 full sync，planner 最长停摆 60 秒）。

值得单独提示协调者的是 **发现 5**：源 Slot 自 Snapshot 阶段即被 fence，意味着 hash-slot 迁移实际是"停写搬迁"，而 FLOW.md 与 `Progress`/`DeltaLag`/`stableThreshold` 这套机制共同描绘的"source 持续服务 + live write 转发 + 延迟收敛后切换"在代码中完全不可达（`UpdateProgress` 零调用方，Delta→Switching 退化为 1 秒定时器）。这不仅是文档失配，也说明 `EnableHashSlotMigration` 默认关闭是恰当的——该实验特性的可用性语义尚未成形。相应地，多个迁移类发现（3、5、14）都需要显式开启该开关才能触发，协调者可据此调整排序；而发现 1、2、4、7、8、9、10 在默认配置下均可达。gosec 在 `pkg/cluster` 的 68 条高密度命中经逐条核验全为误报（唯一非误报的 G118 严重度也远低于其提示），该数字不反映真实风险。
