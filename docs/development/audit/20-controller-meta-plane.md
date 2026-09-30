# controller 元数据存储与规划器（pkg/controller/meta + pkg/controller/plane）

## 覆盖情况

以下为分片内**全部**非测试 `.go` 文件（共 15 个，10,973 行），全部逐行通读：

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/controller/meta/types.go | 161 | 是 |
| pkg/controller/meta/codec.go | 801 | 是 |
| pkg/controller/meta/store.go | 879 | 是 |
| pkg/controller/meta/snapshot.go | 287 | 是 |
| pkg/controller/meta/onboarding_types.go | 175 | 是 |
| pkg/controller/meta/onboarding_store.go | 249 | 是 |
| pkg/controller/meta/onboarding_codec.go | 649 | 是 |
| pkg/controller/plane/types.go | 62 | 是 |
| pkg/controller/plane/commands.go | 138 | 是 |
| pkg/controller/plane/controller.go | 167 | 是 |
| pkg/controller/plane/planner.go | 679 | 是 |
| pkg/controller/plane/statemachine.go | 704 | 是 |
| pkg/controller/plane/onboarding_planner.go | 333 | 是 |
| pkg/controller/plane/onboarding_executor.go | 379 | 是 |
| pkg/controller/plane/onboarding_fingerprint.go | 193 | 是 |

前置阅读：`pkg/controller/FLOW.md`（255 行，AGENTS.md 要求，已通读；与代码的分歧见发现 10）。
辅助验证：`go vet`（GOTOOLCHAIN=go1.23.4，两包 0 条）；运行了包内自带 planner benchmark 获取性能数字；跨包调用方（`pkg/cluster/cluster.go`、`controller_host.go`、`operator.go`、`controller_client.go`、`onboarding_executor.go`、`internal/usecase/management/slot_add_remove.go`、`pkg/controller/raft/service.go`）为理解上下文阅读，发现只落在分配路径内。

gosec 阶段 0 结果：meta 23 条 + plane 7 条，共 30 条，逐条核实（结论见「已排除的候选项」末尾的 gosec 清单）。staticcheck：两包 0 条。

## 发现

### [P0] 1. StateMachine.Apply 对 AddSlot/RemoveSlot 返回状态相关业务错误， poison 条目持久复制后 Controller Raft run loop 全集群永久死亡

- **位置**：`pkg/controller/plane/statemachine.go:617-657`（applyAddSlot）、`statemachine.go:659-684`（applyRemoveSlot）、`pkg/controller/raft/service.go:1019-1029`（Apply 错误传播）、`service.go:942-946`（run loop 退出）
- **类别**：分布式一致性 / 正确性
- **代码**：

  applyAddSlot 在 Apply 时段对**状态依赖条件**返回业务错误（不是对请求格式）：

  ```go
  // statemachine.go:626-636 (applyAddSlot)
  if len(table.ActiveMigrations()) > 0 {
      return controllermeta.ErrInvalidArgument
  }
  if len(table.HashSlotsOf(multiraft.SlotID(req.NewSlotID))) > 0 {
      return controllermeta.ErrInvalidArgument
  }
  if _, err := sm.store.GetAssignment(ctx, uint32(req.NewSlotID)); err == nil {
      return controllermeta.ErrInvalidArgument
  } else if !errors.Is(err, controllermeta.ErrNotFound) {
      return err
  }
  ```

  ```go
  // statemachine.go:668-676 (applyRemoveSlot)
  if len(table.ActiveMigrations()) > 0 {
      return controllermeta.ErrInvalidArgument
  }
  if len(table.HashSlotsOf(multiraft.SlotID(req.SlotID))) == 0 {
      return controllermeta.ErrInvalidArgument
  }
  if removingLastAssignedSlot(table, multiraft.SlotID(req.SlotID)) {
      return controllermeta.ErrInvalidArgument
  }
  ```

  错误从 Apply 一路传播出 run loop，且 run goroutine 从不重启：

  ```go
  // raft/service.go:1019-1029 (applyReadyState)
  err = s.cfg.StateMachine.Apply(ctx, cmd)
  ...
      if err != nil {
          return 0, err
      }
  // raft/service.go:942-946 (run)
  for {
      if err := processReady(); err != nil {
          s.setError(err)
          return
      }
  ```

- **触发路径**（三条独立路径均成立）：

  **(a) 并发 AddSlot 竞态。** 管理端 `POST slot add`（`internal/access/manager/slot_add_remove.go:24` → `management.App.AddSlot` → `Cluster.AddSlot` → `submitAddSlot`）没有任何串行化保护——查 `Cluster.AddSlot`（`pkg/cluster/operator.go:196-232`）：读 `c.GetHashSlotTable()`（本地缓存）做 precheck → `nextSlotDefinition` 算出 `maxSlotID+1` → `submitAddSlot` 直接 Propose。两个并发请求都读到同一份缓存表（无活跃迁移、slot 不存在），都通过 precheck，两条 AddSlot 命令都进入 Raft 复制。第二条在 Apply 时 `table.HashSlotsOf(SlotID(newID)) > 0`（第一条已 StartMigration）或 `GetAssignment(...) == nil` 命中 → 返回 `ErrInvalidArgument`。

  **(b) 提交后重试导致重复提案。** `submitAddSlot` 的非 leader 路径走 `retryControllerCommand`（`pkg/cluster/operator.go:590-596`），重试谓词 `controllerCommandRetryAllowed`（`pkg/cluster/cluster.go:1689-1694`）对 `context.DeadlineExceeded` 和 `ErrNotLeader` 重试。而 `failInflightProposalsOnLeaderLoss` 在提案已被 commit 但响应未送达时返回 `ErrNotLeader`（FLOW.md 避坑清单明确记载该行为）。于是超时/leader 切换后的重试会把一条**已 commit 并已 Apply 成功**的 AddSlot 再次提案，重放条目 Apply 时 `GetAssignment(slotID)` 已存在 → `ErrInvalidArgument`。RemoveSlot 同理（其重试同样对 `DeadlineExceeded` 放行）。

  **(c) AddSlot → RemoveSlot 快速连发。** AddSlot commit 后立即 RemoveSlot 同一 slot（UI 连点 / 脚本重试），AddSlot 的 FinalizeMigration 尚未全部完成时 RemoveSlot 的 `HashSlotsOf(slotID) == 0` 或 `removingLastAssignedSlot` 检查在 Apply 时段不成立 → 返回错误。

  错误一旦返回：`processReady` 返回错误 → `run` 中 `s.setError(err); return` → run goroutine 退出（`go s.run(` 全文件仅 `service.go:372` 一处，无任何重启路径）。该 poison 条目已被 Raft 多数持久复制：其它副本重放到同一 entry 时同样退出。controller 重启后从 log 重放同一 entry → 再次死亡，**整个控制面永久无法恢复**（除非手工清理 Controller Raft log）。

- **后果**：控制面全集群崩溃且重启后无法恢复；AddSlot/RemoveSlot 是管理 API（`internal/access/manager` 暴露）的常规操作，触发不需要恶意输入，只需两次并发请求或一次超时重试。
- **建议**：与 FLOW.md 已为 `NodeOnboardingJobUpdate` 建立的先例对齐——状态依赖的冲突在 Apply 时一律按幂等 no-op 处理（记日志、返回 nil），请求有效性检查前移到 leader 提案侧；`Service.run` 增加 Apply 致命错误时只停投票/告警而非静默退出的防护。
- **备注**：协调者已在 `/tmp/octo-im-audit/00-coordinator-verify-unit20.md` 独立复现验证本条（`service.go:942-946` 与 `statemachine.go:626,629,632`），与上述证据一致。

### [P1] 2. Onboarding 启动 move 时用全新 Assignment 覆盖，静默清空 PreferredLeader 与 LeaderTransferCooldownUntil

- **位置**：`pkg/controller/plane/onboarding_executor.go:241-246`（startOnboardingMove）
- **类别**：分布式一致性 / 正确性
- **代码**：

  ```go
  assignment := controllermeta.SlotAssignment{
      SlotID:         current.SlotID,
      DesiredPeers:   sortedPeers(move.DesiredPeersAfter),
      ConfigEpoch:    current.ConfigEpoch + 1,
      BalanceVersion: current.BalanceVersion + 1,
  }
  ```

  对比普通 Rebalance 路径（`pkg/controller/plane/planner.go:225-232`）对同一结构的完整构造：

  ```go
  Assignment: controllermeta.SlotAssignment{
      SlotID:                      assignment.SlotID,
      DesiredPeers:                nextPeers,
      ConfigEpoch:                 assignment.ConfigEpoch + 1,
      BalanceVersion:              assignment.BalanceVersion + 1,
      PreferredLeader:             p.preferredLeaderForPeers(state, assignment, nextPeers),
      LeaderTransferCooldownUntil: assignment.LeaderTransferCooldownUntil,
  },
  ```

- **触发路径**：扩容审批后 `Cluster.advanceNodeOnboardingOnce`（`pkg/cluster/onboarding_executor.go:24-40`）对 running job 调 `NextNodeOnboardingAction`，命中 `OnboardingActionStartMove` 后把 `action.Assignment` 原样放进 `NodeOnboardingJobUpdate.Assignment` 提案；`applyNodeOnboardingJobUpdate` → `GuardedUpsertOnboardingJob`（`onboarding_store.go:62-69`）直接 `encodeGroupAssignment` 写入。`PreferredLeader=0` 通过 `validatePreferredLeaderInDesiredPeers`（0 视为 unset 合法），`LeaderTransferCooldownUntil` 零值同样合法——两个旧值被覆盖无任何报错。
- **后果**：(1) 扩容中 slot 的 leader 意图丢失，`nextPreferenceConvergence`（`planner.go:516-540`）随后按最低负载重选 PreferredLeader 并生成 LeaderTransfer，与 onboarding 执行器自身要做的 leader transfer（`OnboardingActionStartMove` 的 LeaderTransferRequired 分支）互相冲突，产生多余的副本切换；(2) 该 slot 若在 move 前刚经历失败的 leader transfer，durable 失败冷却（`applyLeaderTransferTaskResult` 写入的 `LeaderTransferFailureCooldown`）被抹掉，planner 可立即对不稳定 slot 再次发起转移。
- **建议**：从 `current`（`input.Assignments[move.SlotID]` 已在手）复制 PreferredLeader 与 LeaderTransferCooldownUntil 到新 Assignment。

### [P1] 3. Onboarding job 永不删除：每次 Apply 状态转移持写锁全表扫描解码排序，且全量进入每次 Raft 快照

- **位置**：`pkg/controller/meta/onboarding_store.go:97-109`（GuardedUpsertOnboardingJob）、`onboarding_store.go:193-214`（listOnboardingJobsLocked）、`pkg/controller/meta/snapshot.go:110-118`（snapshot 前缀表）
- **类别**：性能 / 资源泄漏（无界增长）
- **代码**：

  ```go
  // onboarding_store.go:97-109 —— 在 s.mu.Lock() 写锁内调用
  if job.Status == OnboardingJobStatusRunning {
      running, err := s.listRunningOnboardingJobsLocked(ctx)
      if err != nil {
          return false, err
      }
      for _, runningJob := range running {
          if runningJob.JobID != job.JobID {
              return false, nil
          }
      }
  }
  return true, s.writeBatchLocked(writes)
  ```

  ```go
  // onboarding_store.go:193-200 —— 每次全量迭代 + 解码 + 排序
  func (s *Store) listOnboardingJobsLocked(ctx context.Context) ([]NodeOnboardingJob, error) {
      jobs, err := listRecords(ctx, s.db, recordPrefixOnboardingJob, decodeOnboardingJob)
      if err != nil {
          return nil, err
      }
      sortOnboardingJobsByID(jobs)
      return jobs, nil
  }
  ```

  Store 上不存在任何 onboarding job 的删除方法（全包 grep `DeleteOnboardingJob`/`delete.*Onboarding` 均无）；`recordPrefixOnboardingJob('o')` 又在 `collectSnapshotEntriesLocked` 的前缀表里：
  ```go
  // snapshot.go:110-118
  for _, prefix := range []byte{
      recordPrefixMembership,
      recordPrefixHashSlot,
      recordPrefixNode,
      recordPrefixAssignment,
      recordPrefixRuntimeView,
      recordPrefixTask,
      recordPrefixOnboardingJob,
  } {
  ```
- **触发路径**：运维每做一次节点扩容（成功、失败、取消）都永久留下一个 job 记录。job 记录很大（`encodeOnboardingJob` 内嵌完整 Plan.Moves + 执行 Moves + DesiredPeersBefore/After 各两份 + CurrentTask），且此后：(1) 每次 `GuardedUpsertOnboardingJob`（running 状态，即每个 onboarding tick 的 move 推进）都在写锁内把全部历史 job 从 Pebble 逐条读出、完整二进制解码、排序，再过滤——该调用在 Raft Apply 热路径上；(2) 每次 controller Raft log compaction 导出快照时同样全量读取、复制并写进 Raft snapshot（FLOW.md §7 记载 compaction 按 applied 增量触发）。
- **后果**：管理面高频操作下 Apply 延迟与 compaction 体积随历史 job 数线性增长；`'o'` 前缀下的数据只增不减，是典型的无淘汰路径的持久化泄漏。
- **建议**：为终态 job 增加 TTL/条数上限的清理（可并入既有 batch），running-job 检查改为按状态前缀/索引 seek 而非全表解码。

### [P2] 4. Leader 均衡规划最坏 O(N²·A²)，且热循环内每对 (source,target) 重新分配+排序全部 assignment

- **位置**：`pkg/controller/plane/planner.go:443-470`（nextLeaderSkewCorrection）、`planner.go:636-643`（sortedAssignments）、`planner.go:592-616`（resultingLeaderSkew*）
- **类别**：性能
- **代码**：

  ```go
  // planner.go:449-467 —— 三层嵌套，内层每迭代一次重建负载图并全量排序
  for _, source := range overloadedLeaderCandidates(state, loads, minLoad, p.cfg.LeaderSkewThreshold) {
      for _, target := range lowLoadLeaderCandidates(state, loads, maxLoad) {
          if target == source || loads[target] >= loads[source] {
              continue
          }
          for _, assignment := range sortedAssignments(state.Assignments) {
              view := state.Runtime[assignment.SlotID]
              if view.LeaderID != source {
                  continue
              }
              if !p.leaderTransferSafeCandidate(state, assignment, view, target) {
                  continue
              }
              if !resultingLeaderSkewImproves(state, view.LeaderID, target) {
  ```

  ```go
  // planner.go:636-643 —— 每次调用分配全量 slice 并排序
  func sortedAssignments(assignments map[uint32]controllermeta.SlotAssignment) []controllermeta.SlotAssignment {
      out := make([]controllermeta.SlotAssignment, 0, len(assignments))
      for _, assignment := range assignments {
          out = append(out, assignment)
      }
      sort.Slice(out, func(i, j int) bool { return out[i].SlotID < out[j].SlotID })
      return out
  }
  ```

- **触发路径**：扩容使新节点上线（Alive+Active+Data、0 slot）即满足 `minLoad=0`，`maxLoad-minLoad > LeaderSkewThreshold` 恒成立，skew 修正分支每 tick 进入；但该状态下 `leaderTransferSafeCandidate`（`planner.go:585`）因 `containsPeer(view.CurrentVoters, target)`（新节点不在任何 voter 集合里）恒 false——**最大计算量、零产出**，且每个 planner tick（`plannerSafetyInterval` 安全网定时器 + 每次 `plannerWakeOnce`）重复。
- **后果**：每 tick 时间与分配量随 slot 数与节点数平方增长。用包内 benchmark 实测（本条路径被 fail-closed 短路时的基线，即**未含**本条开销的下限）：50,000 slot 时空转 15.9 ms、5.2 MB、51,815 次分配/每次 `NextDecision`；Rebalance 场景同规模 23.8 ms、9.2 MB。真实 leader 路径耗时会显著更高。另注意 `planner_benchmark_test.go:114-121` 的 Runtime 数据从不填 `CurrentVoters`，因此 `leaderTransferObservationsComplete`（`planner.go:542-553`）恒 false，**现有 benchmark 完全没有覆盖 leader placement 路径**。
- **建议**：`sortedAssignments` 提到循环外算一次；按 source 预建 `LeaderID → []SlotAssignment` 索引；skew 修正的整体收益判断用增量负载而非每次重建 `actualLeaderLoads`。

### [P2] 5. applyAddSlot 对 uint64 SlotID 校验只用非零，赋值处不一致截断，跨包 RPC 不校验

- **位置**：`pkg/controller/plane/statemachine.go:618,629,632,638,644,650`；入口 `pkg/cluster/controller_handler.go:416-421`
- **类别**：正确性（整数截断，gosec G115 的真实变体，在 `pkg/controller/plane` 内但 gosec 只标了 uint64→uint32 的子集行）
- **代码**：

  ```go
  // statemachine.go:617-620 —— 唯一的输入校验只查非零
  func (sm *StateMachine) applyAddSlot(ctx context.Context, req AddSlotRequest) error {
      if req.NewSlotID == 0 || req.PreferredLeader == 0 || !containsPeer(req.Peers, req.PreferredLeader) {
          return controllermeta.ErrInvalidArgument
      }
  ```

  同一函数内同一值三种用法混用（63x/64x 行）：
  ```go
  if len(table.HashSlotsOf(multiraft.SlotID(req.NewSlotID))) > 0 { ... }  // :629 全精度 uint64
  if _, err := sm.store.GetAssignment(ctx, uint32(req.NewSlotID)); ...   // :632 截断 uint32
      SlotID: uint32(req.NewSlotID),                                      // :644 截断 uint32
  ```

  跨包入口原样转发 peer 请求（`pkg/cluster/controller_handler.go:416-421`）：
  ```go
  case controllerRPCAddSlot:
      if req.AddSlot == nil { ... }
      command.Kind = slotcontroller.CommandKindAddSlot
      command.AddSlot = req.AddSlot
  ```
- **触发路径**：AddSlot/RemoveSlot 走 controller RPC 可由任意 peer 提交（`controller_client.go` 转发，handler 不做值域检查）。leader 提案侧的 `nextSlotDefinition` 只会产出 `maxSlotID+1`（uint32 来源，天然安全），但协议接受任意外部 `NewSlotID`。`NewSlotID ≥ 2³²` 时：`HashSlotsOf(SlotID(newID))` 用全精度查表必为空、`GetAssignment(uint32(newID))` 用截断值查——若 `newID = 2³² + k` 且 `k` 恰是已存在 slot，两个检查都通过；随后 `ComputeAddSlotPlan(table, SlotID(newID))`/`StartMigration` 用全精度创建 hash-slot 记录，而 `UpsertAssignmentTaskAndSaveHashSlotTable` 写 `SlotID uint32(newID)` = k。hash-slot 表把 slot k 上的 hash slot 迁往 `SlotID(2³²+k)` 这个"不存在的物理 slot"，物理 slot k 的 assignment 被覆盖成新 assignment——hash slot 归属与物理 slot 元数据永久劈叉。`NewSlotID = 2³²`（截断为 0）时，由于 :629 的全精度检查同样通过，最终 `GetAssignment(ctx, 0)`/写入触发 store 的 `ErrInvalidArgument` → 走发现 1 的 P0 全集群死亡路径。
- **后果**：元数据一致性破坏（hash-slot 表与 assignment 表指向不同物理 slot 集合，后续 FinalizeMigration/删除逻辑对不上号）；或经 :632/:644 的 0 值间接触发发现 1。
- **建议**：`applyAddSlot`/`applyRemoveSlot` 入口对 `req.NewSlotID`/`req.SlotID` 加 `> math.MaxUint32` 拒绝；或把 `AddSlotRequest.NewSlotID`/`RemoveSlotRequest.SlotID` 改为 uint32。

### [P2] 6. UpsertNodeAndDeleteRepairTasks 的回滚修复不对称：只回滚 DesiredPeers，不恢复 ConfigEpoch 之外的侧字段，且静默跳过无法归一化的 assignment

- **位置**：`pkg/controller/meta/store.go:284-311`、`store.go:314-342`（restoreRepairAssignment）
- **类别**：正确性（状态恢复不完整）
- **代码**：

  ```go
  // store.go:288-310
  for _, task := range tasks {
      if task.Kind != TaskKindRepair || task.SourceNode != node.NodeID {
          continue
      }
      if assignment, ok := assignmentsByGroup[task.SlotID]; ok {
          if restored, changed := restoreRepairAssignment(assignment, task); changed {
              assignmentsByGroup[task.SlotID] = restored
              if validated, err := normalizeAndValidateAssignmentForPersistence(restored, ErrInvalidArgument); err == nil {
                  restored = validated
              } else {
                  return err
              }
  ```

  ```go
  // store.go:337-341 —— BalanceVersion 不回退（正确），但只动了这两个字段
  restored.DesiredPeers = peers
  if restored.PreferredLeader == task.TargetNode {
      restored.PreferredLeader = 0
  }
  restored.ConfigEpoch++
  return normalizeGroupAssignment(restored), true
  ```

- **触发路径**：Repair 执行中途（AddLearner/CatchUp 任一步），节点恢复心跳 → leader 提案 `OperatorResumeNode` → `applyOperatorRequest`（`statemachine.go:311-313`）调 `UpsertNodeAndDeleteRepairTasks`。restore 把 `TargetNode` 从 DesiredPeers 移除、放回 `SourceNode`、`PreferredLeader` 置 0、`ConfigEpoch++`。若 repair 决策曾按 `preferredLeaderForPeers` 重选过 PreferredLeader（`planner.go:105`，repair 决策总是重算该字段），恢复后的 PreferredLeader 是 0 或旧值，而数据面 slot raft 的实际 voter 集合已部分含 TargetNode——此后 `nextPreferenceConvergence` 会基于错误基线再发起一次 LeaderTransfer。`ConfigEpoch++` 也与数据面观测脱节：Runtime view 的 `ObservedConfigEpoch` 是按数据面收敛计数，`leaderTransferObservationsComplete` 的判断随之失真一拍。
- **后果**：节点恢复后出现一次多余的、可能被目标侧拒绝的 LeaderTransfer 与一轮 config epoch 抖动；不丢数据，但恢复期规划抖动。注：normalize 失败分支返回错误是安全的（batch 整体失败），真正的缺陷是字段级不对称恢复。
- **建议**：restore 时沿用当前 Runtime 观测重算 PreferredLeader，而不是简单置 0；或恢复语义改为"保留 repair 决策、只删任务"二选一，避免半回滚。

### [P2] 7. ExportSnapshot/ImportSnapshot 全库逐记录三重解码 + 逐条 context 检查，全部持 Store 全局锁

- **位置**：`pkg/controller/meta/snapshot.go:107-162`（collectSnapshotEntriesLocked）、`snapshot.go:43-105`（ImportSnapshot）、`pkg/controller/meta/store.go:743-783`（listRecords）
- **类别**：性能 / 健壮性
- **代码**：

  ```go
  // snapshot.go:128-150 —— 每条记录：ValueAndErr + validateSnapshotKey + validateSnapshotValue
  for ok := iter.First(); ok; ok = iter.Next() {
      if err := s.checkContext(ctx); err != nil {
          iter.Close()
          return nil, err
      }
      value, err := iter.ValueAndErr()
      ...
      if err := validateSnapshotKey(iter.Key()); err != nil { ... }
      if err := validateSnapshotValue(iter.Key(), value); err != nil { ... }
  ```

  其中 `validateSnapshotValue` 对每条做**完整结构化解码**（`decodeClusterNode`/`decodeGroupAssignment`/...），随后 `encodeSnapshot` 再原样写回，`decodeSnapshot`（导入端）对每条再做一次 `validateSnapshotKey`+`validateSnapshotValue` 完整解码，`ImportSnapshot` 又一次（`snapshot.go:55-62` 循环）。`Store` 的 `mu` 是单把 RWMutex，`ExportSnapshot` 全程持 RLock，`ImportSnapshot` 全程持 Lock（`snapshot.go:67-68`），且 ImportSnapshot 的 batch 包含对**所有 7 个前缀**的 DeleteRange + 全部 entry 的 Set，单批 commit(pebble.Sync)。
- **触发路径**：Controller Raft log compaction 按 applied 增量触发 `StateMachine.Snapshot` → `ExportSnapshot`（`statemachine.go:148-153`）。每个 snapshot 导出都把全库记录解码 1 遍 + 校验 1 遍；follower 恢复/新节点 join 导入时再解码 2 遍（decodeSnapshot 校验 + ImportSnapshot 校验）。记录量 = 节点数 + slot 数×3（assignment/runtime/task）+ hash 表 + onboarding jobs（见发现 3）。50k slot 时每次 compaction 至少数十万次结构化解码，全部在持锁状态下进行；期间所有 Store 读写（含 Raft Apply 写路径）阻塞。
- **后果**：controller 日志 compaction 期间 Apply 延迟尖刺；导入路径（节点恢复）线性放大 2 倍解码成本。
- **建议**：信任本库自身写入的数据（compaction 导出侧去掉逐条 validateSnapshotValue，或仅在导入侧校验）；DeleteRange+Set 的 batch 按前缀分批提交。

### [P3] 8. plane/controller.go 整个编排层是死代码，且与 FLOW.md 描述相互矛盾

- **位置**：`pkg/controller/plane/controller.go:28-44,46-67`；文档 `pkg/controller/FLOW.md` §3 与 §5.4
- **类别**：架构 / 文档一致性 / 死代码
- **代码**：

  ```go
  // controller.go:46-66 —— 生产中从未被调用
  func (c *Controller) Tick(ctx context.Context) error {
      if c == nil || c.store == nil {
          return controllermeta.ErrClosed
      }
      if c != nil && c.isLeader != nil && !c.isLeader() {
          return nil
      }

      state, err := c.snapshot(ctx)
      ...
      decision, err := c.planner.NextDecision(ctx, state)
  ```

  ```go
  // controller.go:114-134 —— 与 FLOW.md 描述"从 Controller Leader 本地 observation snapshot 取 RuntimeViews"不符
  func (c *Controller) snapshot(ctx context.Context) (PlannerState, error) {
      ...
      views, err := c.store.ListRuntimeViews(ctx)
      ...
      tasks, err := c.store.ListTasks(ctx)
  ```

- **触发路径**：全仓 grep：`NewController`/`ControllerConfig`/`Controller.Tick` 的引用除 `controller_test.go` 外为零。生产调度入口是 `pkg/cluster/cluster.go:740-789 controllerTickOnce`，它自行构造 `PlannerState`（从 `controllerHost.metadataSnapshot()`/`plannerSnapshot()` 取快照，带 warmup fail-closed 门），并不经过 `plane.Controller`。FLOW.md §3 把 `plane/controller.go:40 Tick` 列为"调度入口"，§5.4 整节描述其编排，与实际不符。
- **后果**：死实现里的行为差异（直接读 Store 的 RuntimeView、无 warmup 门、不设 PauseRebalance/LockedSlots）会误导后续维护者按 FLOW.md 理解规划链路；若有人按文档"接线"它，会绕过 onboarding 互斥（`PauseRebalance`/`LockedSlots` 在死实现里永远是 false/nil）。
- **建议**：删除 `plane/controller.go` 并改写 FLOW.md §3/§5.4 指向 `cluster.controllerTickOnce`，或反过来把真实编排移入本包并接通。

### [P3] 9. ControllerMembership 是整套从未被写入的废弃持久化记录类型；meta 包 8 个导出方法零调用方

- **位置**：`pkg/controller/meta/codec.go:531-552`、`store.go:508-587`；`pkg/controller/raft/command_codec.go`（~30 个未用函数，含整套废弃二进制编解码与 `NodeOnboarding` 命令族，属 unit 19 分片，此处作为旁证）
- **类别**：架构 / 死代码
- **代码**：

  ```go
  // codec.go:531-538 —— 编码器存在
  func encodeControllerMembership(membership ControllerMembership) []byte {
      membership.Peers = normalizeUint64Set(membership.Peers)
      data := make([]byte, 0, 16)
      data = append(data, recordVersion)
      data = appendUint64Slice(data, membership.Peers)
      return data
  }
  ```

  ```go
  // store.go:571-587 —— 写方法存在，但全仓无调用方
  func (s *Store) UpsertControllerMembership(ctx context.Context, membership ControllerMembership) error {
      ...
      return s.writeValueLocked(membershipKey(), encodeControllerMembership(membership))
  }
  ```

- **触发路径**：`grep` 全仓（排除测试与 controllerv2）：`UpsertControllerMembership`/`GetControllerMembership`/`DeleteControllerMembership` 0 个调用方；`membershipKey()` 只被这三个方法与 snapshot 校验引用。记录前缀 `'m'` 在 `collectSnapshotEntriesLocked`/`ImportSnapshot` 的前缀表里占位。即：类型、键前缀、编码器、解码器、快照校验、四个 Store 方法俱全，但**没有任何代码路径写入它**，生产库中 `'m'` 记录永远不存在，快照里也永远为空。同类的零调用导出方法还有 `DeleteNode`、`DeleteAssignment`、`DeleteRuntimeView`、`UpsertOnboardingJob`、`UpsertOnboardingJobAssignmentTask`（各 0 个非测试调用方）。`NodeJoinStateRejected` 同样只读不写（8 处读取守卫，无任何写入路径，`applyNodeJoinActivate` 的 default 分支因此不可达，`statemachine.go:286-288`）。
- **后果**：与 `raft/command_codec.go` 的废弃 codec 一起构成"中途迁移被放弃"的证据链；快照格式与 Store API 面积虚增，维护成本。
- **建议**：删除或完成 wiring；`NodeJoinStateRejected` 要么补写入路径（运营拒绝接口）要么删枚举值。

### [P3] 10. FLOW.md 与代码的其他不一致

- **位置**：`pkg/controller/FLOW.md` §5.2 / §5.3 vs `pkg/controller/plane/`
- **类别**：架构 / 文档
- **代码**（文档声明 vs 实际）：

  FLOW.md §5.2："AddSlot: 若已有 hash-slot 迁移、PreferredLeader 为空或不在 Peers 中则拒绝"，§5.3 "`plane/planner.go:93 NextDecision`"、"入口: `plane/controller.go:40 Tick`"。实际行号漂移（`NextDecision` 在 `planner.go:118`，`Tick` 是死代码，见发现 8）不算问题，但下面两处是实质分歧：
  1. §5.4 "③ 若 leader 仍处于 warmup（尚未收到新鲜观测）则跳过本轮规划"——该逻辑只存在于 `cluster.controllerTickOnce`（`cluster.go:747-749`），文档把它记在 `plane.Controller.Tick` 名下；
  2. §5.4 "上层 cluster 可设置 PauseRebalance / LockedSlots"——生产路径确实设置（`cluster.go:756-757`），但被文档点名的 `plane.Controller` 从不设置，二者的编排职责描述颠倒了。
- **触发路径**：无运行时触发；文档误导维护（与发现 8 合并处理即可）。
- **后果**：文档可信度受损；新人按 FLOW.md 找入口会读到死代码。
- **建议**：随发现 8 一并修订。

## 已排除的候选项

- `pkg/controller/meta/store.go:26-40` — 看起来像 `Close()` 后其它方法 use-after-close，实际安全：`Close` 先 `s.mu.Lock()` 置 `s.db=nil` 再解锁后关 db；所有方法先 `ensureOpen()`（RLock 检查）再拿锁，`writeBatchLocked`/`getValueLocked` 内还有 `ensureOpenLocked` 双检。持锁的写不可能撞上置 nil（后者也要 Lock）。残余窗口（ensureOpen 通过后、拿到写锁前 db 被关）内 pebble 操作会返回错误而非 panic。
- `pkg/controller/meta/snapshot.go:22-41` — 看起来像 ExportSnapshot 读到撕裂视图（多前缀多次 NewIter），实际安全：单把 `s.mu` 上所有写入串行化，Export 持 RLock 贯穿 `collectSnapshotEntriesLocked` 全程，写入方全部持 Lock（已核对 store.go 全部写入路径）。
- `pkg/controller/plane/planner.go:381-392` — `replacePeer` 若 source 不在 peers 中会追加 target 使 peer 集变大、若 target 已在会重复；已核对全部三个调用点均有前置守卫：Repair（`firstPeerNeedingRepair` 返回值必来自 `assignment.DesiredPeers`）、Rebalance（`:191` 显式检查 maxNode 在/minNode 不在）、onboarding（`:162` 检查 target 不在）。安全。
- `pkg/cluster/hashslot/hashslottable.go:101` 侧证（调用点在本分片 `statemachine.go:639-641`）— 看起来重复 StartMigration 可重置进行中迁移，实际 `if _, ok := t.migrations[hashSlot]; ok { return }` 幂等，且 `applyAddSlot` 前置要求 `len(table.ActiveMigrations()) == 0`。
- `pkg/controller/plane/statemachine.go:352-356` — 看起来 `applyTaskResult` 对 `advance.Err == nil` 的 Pending 任务无条件删除，可能把"任务刚被 planner 换了新一轮"的任务误删；实际执行器上报按 `task.Attempt` 精确匹配（`:358` Attempt 不匹配静默忽略），且新任务 Attempt 从 0 重新计数与旧任务相同的风险由"planner 只在无任务 slot 上创建任务"排除。另核对 `command_codec.go:1031`：空 Err 文本解码回 nil，不产生假失败。
- `pkg/controller/plane/planner.go:60-63` — `selectBootstrapPeers` 在 `p.cfg.ReplicaN == 0` 时 `len(peers) < 0` 恒 false 会选出空 peers；实际 `pkg/cluster/config.go:188-190` 与 `internal/app/config.go:1063-1067` 均强制 `SlotReplicaN > 0`，不可达。
- `pkg/controller/plane/onboarding_fingerprint.go:191` — `canonicalReflectJSON` 对未支持类型 `panic()`，且 `OnboardingPlanFingerprint` 无 recover；逐个核对 `onboardingFingerprintDocument` 产生的全部动态类型（map/[]uint64/[]any/uint64/uint32/string/bool）均被 `canonicalJSON` 具名分支覆盖，`time.Time` 亦有分支，`ClusterNode` 字段按值展开。当前无可达 panic 路径（若未来 fingerprint 输入加入新类型，此处会变成 Apply 路径上的远程可达 panic——建议改返回 error）。
- `pkg/controller/meta/onboarding_store.go:49-110` — `GuardedUpsertOnboardingJob` 在锁内做磁盘 Get/List 看似持锁 I/O，但 Store 本身就是串行化写模型（所有写都在 Lock 下 batch commit），无额外死锁面；检查与写入在同一临界区内完成，无 TOCTOU 窗口。性能问题已归入发现 3。

**gosec 30 条逐条裁定**（G115 12 条 + G104 11 条 + plane G115 7 条）：

误报（有上界保证）：
- `codec.go:770,798`（uint64→int）— `readUint64Slice`/`readBytes` 在转换前检查 `count/length > uint64(len(rest))`，conv 后的值 ≤ 输入长度；`int(count)` 只用于 make cap。
- `codec.go:786`（uint64→int64）— `readInt64` 的 `int64(binary.BigEndian.Uint64(...))` 是编码端 `appendInt64`（int64→uint64）的精确逆变换，-roundtrip 无损。
- `codec.go:779`（int64→uint64）— 同上，`binary.BigEndian.AppendUint64(dst, uint64(value))` 固定位宽编码，标准做法。
- `codec.go:713`（int→uint32）— `uint32(maxHealthyVoters)`，maxHealthyVoters ≤ len(peers)（副本数，个位数量级），且此转换出现在校验器内，溢出方向是使校验更宽松的假阴性，无远程放大路径。
- `snapshot.go:229-236`（uint64→int ×4）— `keyLen`/`valueLen` 均先检查 `> uint64(len(body))`，转换后 ≤ 剩余缓冲长度。
- `onboarding_codec.go:399` — `count > uint64(len(rest))/14` 保证 `int(count)` ≤ 剩余长度/14。
- `onboarding_codec.go:462` — `moves := make([]NodeOnboardingPlanMove, 0, int(count))`，count 先经 `count > uint64(len(rest))/62` 约束。
- `onboarding_codec.go:639` — `readInt64Values` 的 `int64(binary.BigEndian.Uint64(...))` 固定位宽 roundtrip。
- plane `statemachine.go:578,632,644,650`、`controller.go:159,162,163` — 均 `uint32(multiraft.SlotID)`（uint64→uint32）。`multiraft.SlotID` 的生产来源是 `nextSlotDefinition`（uint32 `maxSlotID+1`，`pkg/cluster/operator.go:361`）与本地 slot manager（uint32 域）。**但跨 RPC 边界 `AddSlotRequest.NewSlotID` 是 uint64 且 handler 不校验，:632/:644/:650 三处截断与 :629 全精度混用构成真实缺陷**——已作为发现 5 单列（这正是"逐一核查后 G115 不是全误报"的那一条）。

G104 误报（11 条）：`store.go:759,765,770` 是 `iter.Close()` 在错误返回路径上的显式调用（G104 不识别"清理型"调用）；`store.go:776`/`snapshot.go:130,136,140,144,153` 同理；`store.go:812` 是 `deleteValueLocked` 内 `closer.Close()`（Get 成功后释放句柄，错误无从产生也无从处理）。全部 iter/closer 在所有分支（含 error）都有 Close，无资源泄漏。

## 本分片整体评价

这两个包的代码风格异常整洁：编码层带版本号与校验、写入全走原子 batch、planner 决策有确定性排序与 fail-closed 姿态、FLOW.md 描述了清晰的防御设计意图（Attempt 匹配、冷却期、onboarding 与 rebalance 互斥）。但从 `raft/` 边界看，"Apply 不返回业务错误"这条在 onboarding 上执行了的纪律没有覆盖 AddSlot/RemoveSlot——结果就是发现 1 这个全仓最严重的问题：管理面两个常规操作可让整个 Controller Raft 组永久死亡且不可自愈（协调者已独立验证）。次优先级是 onboarding 的两处执行缺陷（字段覆盖与永不清理的 job 存储）和扩容状态下 planner 的 O(N²) 空转；planner 的性能基线（50k slot 空转 16ms/5.2MB）本身可接受，但 leader placement 路径零 benchmark 覆盖、规模上限未知。此外本分片确认了与 `pkg/controller/raft/command_codec.go` 同源的"中途迁移被放弃"模式：`plane.Controller` 整个编排层是死代码、`ControllerMembership`/`NodeJoinStateRejected` 是只读不写的废弃结构、meta 包 8 个导出方法零调用方。
