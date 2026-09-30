# slot 状态机与 Multi-Raft（pkg/slot/fsm + pkg/slot/multiraft）

## 覆盖情况

已通读分片内全部非测试 .go 文件（每份均完整读过，非只读签名）：

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/slot/fsm/statemachine.go | 809 | 是 |
| pkg/slot/fsm/command.go | 1883 | 是 |
| pkg/slot/fsm/channel_migration_cmds.go | 445 | 是 |
| pkg/slot/fsm/command_inspection.go | 351 | 是 |
| pkg/slot/fsm/migration_cmds.go | 298 | 是 |
| pkg/slot/fsm/plugin_binding_cmds.go | 101 | 是 |
| pkg/slot/fsm/errors.go | 6 | 是 |
| pkg/slot/multiraft/slot.go | 1019 | 是 |
| pkg/slot/multiraft/api.go | 282 | 是 |
| pkg/slot/multiraft/compaction.go | 169 | 是 |
| pkg/slot/multiraft/types.go | 149 | 是 |
| pkg/slot/multiraft/runtime.go | 132 | 是 |
| pkg/slot/multiraft/ready.go | 102 | 是 |
| pkg/slot/multiraft/scheduler.go | 82 | 是 |
| pkg/slot/multiraft/config.go | 51 | 是 |
| pkg/slot/multiraft/storage_adapter.go | 43 | 是 |
| pkg/slot/fsm 之外注：multiraft/future.go | 35 | 是 |
| pkg/slot/multiraft/logging.go | 15 | 是 |
| pkg/slot/multiraft/errors.go | 14 | 是 |
| pkg/slot/multiraft/doc.go | 2 | 是 |

（fsm 3893 行 + multiraft 2095 行，全部通读；未读任何文件为"否"。）

补充上下文（非本分片，仅为验证调用关系读过）：`pkg/slot/FLOW.md`（全文，作为设计基线）、`pkg/cluster/cluster.go` 1225-1340/542-580/963-1015、`pkg/cluster/slot_manager.go` 40-110、`pkg/cluster/hashslot_migration.go`、`pkg/cluster/managed_slots.go` 320-380、`pkg/cluster/reconciler.go` 90-125、`pkg/db/meta/snapshot.go` 30-200、`pkg/db/meta/table_hashslot_migration.go` 119-150、`pkg/cluster/transport.go` 30-70。

FLOW.md 已按要求先读。一处实质性不一致见发现 2（apply_delta 归属护栏缺失）；其余核对项（ApplyBatch 原子性、ConfChange flush 语义、快照恢复边界、迁移 fence/proof 语义、`markAppliedDeltas` 所在的 delta 流程描述）与代码一致。

## 发现

### [P1] 1. `appliedDelta` 是只写不读、永不清理的无限增长 map（纯内存泄漏）

- **位置**：`pkg/slot/fsm/statemachine.go:787-796`（声明 `statemachine.go:53-54`，写入点 `statemachine.go:281`）
- **类别**：资源泄漏（无界增长，检查类 e）
- **代码**：
  ```go
  func (m *stateMachine) markAppliedDeltas(keys []deltaReplayKey) {
      if m == nil || len(keys) == 0 {
          return
      }
      m.appliedDeltaMu.Lock()
      defer m.appliedDeltaMu.Unlock()
      for _, key := range keys {
          m.appliedDelta[key] = struct{}{}
      }
  }
  ```
- **触发路径**（三点验证均已确认）：
  1. **从不读取**：全仓 `grep -rn 'appliedDelta'`（不含 `_test.go`）只命中声明、构造、`markAppliedDeltas` 写入共 6 处；仅 `state_machine_test.go:1914/2051` 对 `len(raw.appliedDelta)` 做断言。生产代码没有任何读者。
  2. **从不删除/重置**：没有任何 `delete(m.appliedDelta, …)`、没有整体重建；`Restore`（statemachine.go:642-650）导入快照替换了全部 DB 数据后也不清它，`UpdateOwnedHashSlots`/`CloseSlot` 同样不碰。
  3. **真正的去重机制是持久化路径**：`ApplyBatch`（statemachine.go:238-263）先用同批 `pendingDeltaRecords` + `m.db.HasAppliedHashSlotDelta(ctx, …)`（Pebble 点查）判重，再 `wb.MarkAppliedHashSlotDelta(…)` 随 WriteBatch 原子落盘。内存 map 在去重逻辑中不承担任何角色。
  4. **增长驱动**：`ApplyBatch` 每应用一条 `apply_delta` 命令（迁移期源 Slot 把每条 live write 包装转发给目标 Slot，见 `pkg/cluster/hashslot_migration.go:98`），commit 成功后就把一个 `deltaReplayKey{hashSlot, sourceSlot, sourceIndex}` 写进 map。`sourceIndex` 是源 Slot 的 Raft index，严格单调，永不重复。因此目标 Slot FSM 的 map 大小 = 迁移期间该 hash slot 的全部转发写入条数，按每条 ~50B 计，1 万 QPS 的频道迁移 1 小时 ≈ 数 GB 级 resident 内存，且进程不重启永不释放。
- **后果**：hash-slot 迁移（hot migration）期间目标节点内存线性增长直至 OOM；map 本身零功能价值。
- **建议**：直接删除 `appliedDelta`/`appliedDeltaMu`/`markAppliedDeltas` 与 `pendingDeltaKeys`，持久化表已完整覆盖去重语义；若保留需同步删除 `Restore` 时的失效问题。

### [P2] 2. `resolveHashSlot` 对 `apply_delta` 完全不校验归属，`incomingDeltaSlots` 在 apply 路径从未被使用（与 FLOW.md 明文不符）

- **位置**：`pkg/slot/fsm/statemachine.go:362-373`
- **类别**：分布式一致性 / 架构（检查类 h、i）
- **代码**：
  ```go
  func (m *stateMachine) resolveHashSlot(cmd multiraft.Command) (uint16, error) {
      if isApplyDeltaCommandData(cmd.Data) {
          decoded, err := decodeCommand(cmd.Data)
          if err != nil {
              return 0, err
          }
          applyDelta, ok := decoded.(*applyDeltaCmd)
          if !ok || applyDelta.HashSlot != cmd.HashSlot {
              return 0, metadb.ErrInvalidArgument
          }
          return applyDelta.HashSlot, nil   // ← 直接放行，无任何归属校验
      }
  ```
- **触发路径**：
  1. `pkg/slot/FLOW.md` 避坑清单明文规定："目标 Slot 只对这类 `apply_delta` 放开**迁移中的** hash slot，普通命令仍按最终归属校验拒绝"。
  2. 代码里 `incomingDeltaSlots` 的全部使用点只有 4 处：声明（:57）、`UpdateIncomingDeltaHashSlots`（:143）、`runtimeSnapshotHashSlotsLocked` 快照导出（:679）、`addIncomingDeltaHashSlots`（:691）。**apply 路径从不查它**。
  3. 因此任意一个能对该 Slot 提案的节点（Raft quorum 成员即可提案任意字节）可以构造 `applyDelta` 命令携带**任意 hashSlot**（含本 Slot 不拥有、也不在迁入集合中的），FSM 会对该 hashSlot 的 shard 正常执行内层命令写入：`ApplyBatch` 对 `applyDelta` 跳过 fence 检查（`isMigrationMaintenanceCommand` 白名单，statemachine.go:186-191），随后 `decoded.apply(wb, hashSlot)` 落盘。
  4. 数据落在本物理 Slot 的 shard N 前缀下，但路由表把 hash slot N 指向别的 Slot —— 跨 Slot 元数据污染：本节点本地读（proxy 本地读直接读本机 Pebble）会看到其它 Slot 的元数据，且该数据随后续快照继续复制。
  5. 正常代码路径（delta forwarder）总是填迁入中的 hashSlot，所以这是"缺护栏 + 文档与实现不符"，而非现行业务逻辑必然触发；一旦上游 forwarder 出 bug（如 hashSlot 填错）或集群内有节点被攻破/有 bug，错误写入会被 Raft 复制到全部副本，无法靠 FSM 拦截。
- **后果**：跨 hash slot 元数据污染会被复制和快照固化；文档承诺的迁移期隔离护栏实际不存在。
- **建议**：`resolveHashSlot` 的 applyDelta 分支要求 `hashSlot ∈ ownedHashSlots ∪ incomingDeltaSlots`，并修正 FLOW.md 与代码取其一。

### [P2] 3. 领导权在 `enqueueControl` 与 `processControls` 之间丧失时，提案 future 与 Raft entry 错位配对（结果错配 + future 卡死）

- **位置**：`pkg/slot/multiraft/slot.go:210-216`（Propose 后无条件入队）、`slot.go:877-905`（`trackReadyEntries` FIFO 配对）、`pkg/slot/multiraft/runtime.go:96-99`（`processRequests` 先于 `processControls`）
- **类别**：并发 / 分布式一致性（检查类 a、h）
- **代码**：
  ```go
  // slot.go processControls:
  case controlPropose:
      if err := g.rawNode.Propose(action.data); err != nil {
          action.future.resolve(Result{}, err)
          continue
      }
      g.mu.Lock()
      g.submittedProposals = append(g.submittedProposals, action.future)  // Propose 返回 nil ≠ 产生了 entry
      g.mu.Unlock()
  ```
  ```go
  // slot.go trackReadyEntries: FIFO 弹出配对
  g.pendingProposals[entry.Index] = trackedFuture{
      future: g.submittedProposals[0],   // 被静默丢弃的提案占住队首
      term:   entry.Term,
  }
  g.submittedProposals = g.submittedProposals[1:]
  ```
- **触发路径**（逐步）：
  1. Slot 为 leader，`Runtime.Propose` → `enqueueControl` 检查 `g.status.Role == RoleLeader` 通过，action 进入 `g.controls`。
  2. 同一时刻一条更高 term 的 `MsgHeartbeat`/`MsgApp` 已通过 `Runtime.Step` 排进 `g.requests`（入队时 `observeQueuedMessageLocked` 只更新内存 status，rawNode 尚未 Step）。
  3. Worker 下一轮 `processSlot`：`processRequests` **先**执行，`rawNode.Step(高term消息)` 使 rawNode 立即降级为 follower —— 但 `g.controls` 里的提案还在。
  4. `processControls` 执行 `rawNode.Propose(...)`：etcd/raft 对非 leader 的 `MsgProp` 是**静默丢弃、返回 nil**。于是这个永远不会产生 entry 的 future 被 append 进 `submittedProposals`。
  5. 同一轮 `processReady` 的 `ready.CommittedEntries` 里是**上一任期已提交的其它提案的 entry**：`trackReadyEntries` 把队首（被丢弃的 future）配给这些 entry 之一 —— 被丢弃提案的调用方拿到**别人命令的 apply 结果**（Result.Data 是 `ok`/`stale_meta`/GC 计数等）。
  6. 真正产生该 entry 的提案的 future 已被弹出队列、从未进入 `pendingProposals`，其调用方 `Wait(ctx)` 只能靠上层 `ForwardRetryBudget` 超时兜底（`pkg/cluster/cluster.go:996-1015`）；若节点随后重新当选且继续有 entry，错位会顺延到下一轮配对。
  7. 同窗口对 `ChangeConfig` 同样成立（`controlConfigChange` 分支同构）。现有测试 `TestProposeRejectsStaleLeaderAfterHigherTermMessageQueued`（proposal_test.go:150-172）只覆盖了**入队时**的拒绝，未覆盖这个 in-flight 窗口。
- **后果**：提案结果错配（调用方 A 收到命令 B 的结果；依赖 `stale_meta` 判定重试的迁移命令可能把 stale 误读为 ok）+ 提案 future 卡死到领导权再变更才被 `failLeadershipDependentLocked` 清掉。
- **建议**：`trackReadyEntries` 配对前校验 future 与 entry 的对应性（如提案时记录 proposal 本地 index/term），或 Propose 后检查 rawNode 仍是 leader，否则当场 resolve 错误。

### [P2] 4. 每条 FSM apply 命令都做一次迁移状态 Pebble 点查（apply 热路径固定放大）

- **位置**：`pkg/slot/fsm/statemachine.go:198-206`（调用点）、`statemachine.go:391-416`（`isHashSlotFenced`）
- **类别**：性能（检查类 g）
- **代码**：
  ```go
  if !isMigrationMaintenanceCommand(decoded) {
      fenced, err := m.isHashSlotFenced(ctx, hashSlot, pendingMigrationStates)
      ...
  }
  // isHashSlotFenced:
  state, ok := pendingStates[hashSlot]
  if !ok {
      var err error
      state, err = m.db.LoadHashSlotMigrationState(ctx, hashSlot)   // 每条命令一次 Pebble Get
  ```
- **触发路径**：`ApplyBatch` 的批内 `pendingStates` 缓存只覆盖"本批内被迁移维护命令写过"的 hashSlot。正常运行（无迁移）时批内每一条普通命令（CreateChannel、AddSubscribers、UpsertRuntimeMeta……）都各自触发一次 `LoadHashSlotMigrationState` → `meta` 键空间(0x12) 的 Get，绝大多数是 miss。批量 apply 本来是为均摊 fsync，这里引入了与命令数线性相关的额外读放大：100 条/批就是 100 次 Get。迁移态表由本 FSM 的 apply 路径独占写入，完全可以维护一个 apply 内一致的正/负内存镜像（在写 fence 的命令 apply 时更新），消除稳态点查。
- **后果**：所有 Slot 元数据写入的 apply 延迟多一次点查（~µs 级），高吞吐批量下累积可观；属持续税而非悬崖。
- **建议**：为"无迁移态"维护 apply 路径自洽的内存负缓存，或把 fence 态并入批首一次读取。

### [P3] 5. 混合版本集群下未知命令类型会让 Slot fatal（无 capability gate）

- **位置**：`pkg/slot/fsm/command.go:1248-1252`
- **类别**：分布式一致性 / 架构（检查类 h）
- **代码**：
  ```go
  cmdType := data[1]
  decoder, ok := commandDecoders[cmdType]
  if !ok {
      return nil, fmt.Errorf("%w: unknown command type %d", metadb.ErrInvalidArgument, cmdType)
  }
  ```
- **触发路径**：新版本节点当选 Slot leader 并提交了旧版本不认识的 cmdType（如已在计划中的新命令）；旧版本副本 apply 该 entry 时 `decodeCommand` 返回 error → `ApplyBatch` 返回 error → `slot.go:487 g.fail(err)` 置 fatalErr → 该 Slot 在旧节点上永久不可用直至升级。当前所有命令都是 version 1，FLOW.md 只对命令 16 明确了该约束（"stop-the-world 升级"），但这是解码器的结构性属性，适用于**未来任何**新命令类型。版本字节（≠1 拒绝）能挡住整体格式换代，挡不住"同 version 下新增 cmdType"。
- **后果**：滚动升级期间 Slot 级不可用；与 FLOW.md 已认知的命令 16 约束同源但范围更广。
- **建议**：为新增命令引入能力协商/版本 gate（提案前确认全 quorum 支持），或把未知 cmdType 降级为确定性 no-op 结果而非 fatal。

### [P3] 6. 订阅者 UID 列表用 `\x00` 连接编解码，非单射：含 NUL 的 UID 会分裂成多个订阅者

- **位置**：`pkg/slot/fsm/command.go:1860-1883`
- **类别**：正确性
- **代码**：
  ```go
  func encodeStringSet(values []string) []byte {
      ...
      return []byte(strings.Join(sorted, "\x00"))
  }
  func decodeStringSet(data []byte) []string {
      if len(data) == 0 {
          return nil
      }
      return strings.Split(string(data), "\x00")
  }
  ```
- **触发路径**：调用方对 `Store.AddChannelSubscribers`（proxy/store.go:111-119）传入 UIDs `["a\x00b"]`（NUL 字节无任何一层校验：FSM 的 `ValidateSubscriberCommandLimits` 只查数量/字节数，proxy 与 fsm 均不过滤字符集）→ 编码为 `a\x00b` → FSM 解码得 `["a","b"]` → 写入两条主键为 `("a")`、`("b")` 的订阅者行，UID `a\x00b` 的订阅从未生效。跨副本解码确定一致，不会造成 Raft 分叉，但调用方意图与落库结果静默偏离（该"用户"后续权限检查查不到订阅）。`decodeStringSet` 解码发生在提案 commit 之后，proxy 无法提前发现。
- **后果**：特定输入下订阅关系静默错乱（需上游允许 NUL UID 才可达，本分片内未找到 UID 字符集校验）。
- **建议**：`ValidateSubscriberCommandLimits`（或更上游）拒绝含 `\x00` 的 UID，编码改用长度前缀。

### [P3] 7. 死代码：`canonicalizeUint64Set` 与 `wrapMessages`（staticcheck U1000 ×2）

- **位置**：`pkg/slot/fsm/command.go:1816-1832`、`pkg/slot/multiraft/slot.go:902-905`
- **类别**：架构（死代码）
- **代码**：
  ```go
  // command.go:1816
  func canonicalizeUint64Set(values []uint64) []uint64 {
      if len(values) == 0 {
          return nil
      }
      ...
  ```
- **触发路径**：全仓无调用点（`grep -rn` 仅命中定义；`encodeStringSet` 自带同构去重、`wrapMessagesInto` 被实际使用）。无运行路径。
- **后果**：无运行时后果；两处均为 staticcheck 119 条中属于本分片的 2 条 U1000，建议删除。
- **建议**：删除。

### [P3] 8. Slot 关闭/出 fatal 后，已排队的 `controlCompactLog` 永不被处理，`CompactLog` 调用方只能等 ctx

- **位置**：`pkg/slot/multiraft/api.go:230-244`、`pkg/slot/multiraft/slot.go:849-866`（`failPendingLocked` 不清理 `g.controls`）
- **类别**：资源（检查类 a/d）
- **代码**：
  ```go
  // api.go CompactLog
  select {
  case resp := <-req.resp:
      return resp.result, resp.err
  case <-ctx.Done():
      return LogCompactionResult{}, ctx.Err()
  }
  ```
  ```go
  // slot.go failPendingLocked —— 覆盖 submitted/pending proposals/configs，唯独不碰 g.controls
  for index, pending := range g.pendingConfigs {
      pending.future.resolve(Result{}, err)
      delete(g.pendingConfigs, index)
  }
  g.submittedProposals = nil
  g.submittedConfigs = nil
  ```
- **触发路径**：`CompactLog` 入队 control 成功（slot 存活）→ 并发发生 `CloseSlot`/`Runtime.Close`（置 closed）→ Worker `beginProcessing` 因 `admissionErrLocked != nil` 直接返回，`g.controls` 中该 control 永不被消费；`resp` chan 永不收到值（容量 1 的 chan 只防泄漏不防挂起）。调用方若传无 deadline 的 ctx 则永久阻塞。实际运维入口通常带请求 ctx，影响有限。
- **后果**：`CompactLog` 调用方在 Slot 关闭竞态下收不到结果，只能靠 ctx 兜底；control 内存随 slot 一起被丢弃，无泄漏。
- **建议**：`failPendingLocked`/关闭路径对 `g.controls` 里的 `controlCompactLog` 补发错误响应（对 propose/config 类也可顺带统一处理）。

## 已排除的候选项

- **fsm 包全部 42 条 gosec G115 —— 均误报**，逐类核验：
  - `uint64 -> int64`（command.go 36 处 + plugin_binding_cmds.go:88,94）：全部是 `int64(binary.BigEndian.Uint64(value))` 对时间戳/标志位/seq 的**位模式重解释**，解码-编码往返无损，不存在截断；gosec 不区分"截断转换"与"重解释转换"。
  - `int -> uint32`（command.go:1760,1777,1783）：`uint32(len(value))`，即 TLV 字符串长度。受约束：订阅者命令 ≤64KB（`MaxSubscriberCommandUIDBytes`，decode 兜底拒绝）、其余字段来自进程内 marshal 的元数据结构；要溢出需单条 Raft entry >4GB，内存先于转换失败。
  - `int64 -> uint64`（command.go:1771,1789）：`uint64(v)` 为 `binary.AppendUint64` 的位重解释，无损。
  - `int -> uint64`（channel_migration_cmds.go:204）：`uint64(deleted)`，GC 删除行数，非负且受表规模约束。
  - `uint64 -> uint16`（statemachine.go:60）：`uint16(slot)` 物理槽位号，由集群配置（initial slot count 量级为几十~几百）决定上界。
- `pkg/slot/fsm/statemachine.go:713` `_ = forwardDelta(...)` 吞错误 —— 转发失败有持久化兜底：outbox 行已与数据同批落盘（`stageMigrationOutbox` 内 `wb.UpsertHashSlotMigrationOutbox`），目标方 ack/cleanup 命令与 outbox 重放（hashslot_migration.go:423）保证最终送达，best-effort 是设计意图。
- `pkg/slot/fsm/channel_migration_cmds.go:231` `panic(err)` —— `json.Marshal` 对 metadb 纯数据结构体不可能失败（无 chan/func/循环引用），不可远程触发。
- `pkg/slot/fsm/statemachine.go:203-206` fenced 结果跳过 apply —— 设计语义（迁移期源写入确定性拒绝），结果通过 `ApplyResultHashSlotFenced` 回传提案方，非错误被吞。
- `pkg/slot/fsm/statemachine.go:286-300` `applyCommandsIndividuallyAfterStaleCommit` —— stale commit 后逐条重放：出错批次的 WriteBatch 未 Commit 被 defer Close 丢弃，重放是这些命令的首次真实应用，且新 decode 生成新命令对象（`garbageCollectMigrationTasksCmd.deleted` 等可变状态不残留），安全。
- `pkg/slot/multiraft/scheduler.go` 非阻塞 `dispatchLocked`：channel 满时 `s.pending` 暂留，只在下一次 enqueue/begin/requeue 时再派发 —— 存在理论上的派发延迟，但 Ticker 每 `TickInterval` 对全部 Slot `enqueue`（runtime.go:69-78），活性有界，非缺陷。
- `pkg/slot/multiraft/slot.go:675-689` `observeQueuedMessageLocked` 在入队时预改 status.Term/Role —— 只影响观测与 `enqueueControl` 的快速失败，rawNode 状态由 `Step` 权威决定；方向保守（把未来 leader 当 follower 拒绝提案），安全。
- `pkg/slot/multiraft/slot.go:606-616` 降级时 `failLeadershipDependentLocked` 可能 fail 掉"entry 已提交并 apply"的提案 future —— 保守失败，且 `resolveProposal` 的 `(index,term)` 匹配保证不会二次 resolve 错误结果（已配对的 resolution 在 future 被删后落空）。
- `pkg/slot/multiraft/slot.go:886-897` 与 868-881 两个几乎相同的 fail 循环 —— 重复代码，非缺陷。
- `pkg/slot/multiraft` 其余核心（runtime.go / api.go / ready.go / compaction.go / storage_adapter.go / future.go）—— 与侦察结论一致，逐一复核为干净：`r.mu`→`g.mu` 锁序全局一致无反向获取；`CloseSlot` 的 `cond.Wait` 在持有 `g.mu` 下调用且 `finishProcessing` Broadcast，用法正确；`Runtime.Close` 先关 stopCh 再 `wg.Wait` join 全部 worker+ticker，之后才清 slots map；`persistReady` 的 Save→memory 同步顺序保证 crash 后重放一致；`compactLog` 对 `ErrSnapOutOfDate`/`ErrCompacted` 的容忍正确；`shouldCompact` 的 interval 检查无长期漂移问题（fail 后不 recordSnapshot 会下轮重试）。
- FSM apply 确定性三问（检查类 h 专项）：**无** time.Now/rand/floating-point 进入任何 apply 路径（grep 证实 fsm 包非测试代码 0 处 `time.Now`）；仅有的两处 map 迭代（`runtimeSnapshotHashSlotsLocked` 的 `incomingDeltaSlots`、multiraft 的 `currentVotersFromRaftStatus`）之后都紧跟 `normalizeOwnedHashSlots`/`sort.Slice` 排序，输出确定；apply 幂等由单调护栏（runtime meta epoch 单调、`DeletedToSeq`/`ReadSeq` 只前进、CreateUser 已存在跳过）+ 持久化 delta 去重表保证。**未发现**可解释 CI 已知 flaky（pkg/cluster、pkg/controllerv2/raft 四个被跳过测试）的 FSM apply 非确定性源——fsm/multiraft apply 路径是确定性的；flaky 根因应到被跳过测试所在的分片去找。
- `NewStateMachine`（statemachine.go:60）的 `allowLegacyDefault` 老路径（hashSlot==0 回退 legacyHashSlot）—— 只服务于无 hash-slot 表的旧数据兼容，且 `NewStateMachineWithHashSlots` 主路径显式 `allowLegacyDefault=false`，不构成校验旁路。

## 本分片整体评价

这两层是全仓里质量相当高的部分：multiraft 运行时核心（runtime/api/ready/compaction）经逐文件复核没有发现并发、关闭顺序或快照一致性问题，fsm 的 TLV 解码对长度前缀、重复字段、未知命令的处理也明显比仓库平均水平严谨。最需要优先处理的是发现 1：`appliedDelta` 是一个被 Raft 复制写放大驱动、永不回收的纯泄漏，在 hot hash-slot 迁移场景下可直接把目标节点推向 OOM，而它连功能都是多余的（持久化去重表已完整覆盖）。其次是发现 2，FLOW.md 明文承诺的"目标 Slot 只对迁移中的 hash slot 放开 apply_delta"在代码里并不存在——`incomingDeltaSlots` 从未参与 apply 校验，这既是文档/实现背离，也使跨 slot 数据注入完全依赖提案方的自觉。发现 3 的提案 future 错位窗口虽窄，但它是 result 错配这一类难以排查的问题，值得用一次结构化修复（future 与 entry 显式绑定）关掉。
