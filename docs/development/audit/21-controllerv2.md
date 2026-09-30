# 21-controllerv2 — controllerv2 全栈

> **本分片全部为 v2 未上线代码**：`pkg/controllerv2` 及其子包只能经由 `internalv2` 可达，
> 而 `internalv2/app` 没有任何生产 `cmd/*` 二进制 import。协调者已确认零 importer。
> 因此下面所有发现都明确标注"该代码当前不在生产运行路径上"，严重度相应下调一档
> （P0→P1、P1→P2、P2→P3 的降级已经体现在标题里，标题就是降级后的最终严重度）。

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/controllerv2/doc.go | 8 | 是 |
| pkg/controllerv2/runtime.go | 151 | 是 |
| pkg/controllerv2/runtime_start.go | 90 | 是 |
| pkg/controllerv2/runtime_bootstrap.go | 104 | 是 |
| pkg/controllerv2/runtime_refresh.go | 90 | 是 |
| pkg/controllerv2/runtime_sync.go | 52 | 是 |
| pkg/controllerv2/types.go | 177 | 是 |
| pkg/controllerv2/raft/doc.go | 11 | 是 |
| pkg/controllerv2/raft/service.go | 300 | 是 |
| pkg/controllerv2/raft/service_run.go | 151 | 是 |
| pkg/controllerv2/raft/service_recovery.go | 97 | 是 |
| pkg/controllerv2/raft/service_snapshot.go | 56 | 是 |
| pkg/controllerv2/raft/service_helpers.go | 125 | 是 |
| pkg/controllerv2/raft/apply_scheduler.go | 253 | 是 |
| pkg/controllerv2/raft/proposal_tracker.go | 67 | 是 |
| pkg/controllerv2/raft/status.go | 58 | 是 |
| pkg/controllerv2/raft/config.go | 186 | 是 |
| pkg/controllerv2/raft/raftstore/config.go | 41 | 是 |
| pkg/controllerv2/raft/raftstore/store.go | 287 | 是 |
| pkg/controllerv2/raft/raftstore/wal.go | 480 | 是 |
| pkg/controllerv2/raft/raftstore/record.go | 164 | 是 |
| pkg/controllerv2/raft/raftstore/snapshot.go | 95 | 是 |
| pkg/controllerv2/raft/raftstore/metadata.go | 96 | 是 |
| pkg/controllerv2/raft/raftstore/memory.go | 38 | 是 |
| pkg/controllerv2/fsm/doc.go | 7 | 是 |
| pkg/controllerv2/fsm/fsm.go | 208 | 是 |
| pkg/controllerv2/fsm/mutations.go | 79 | 是 |
| pkg/controllerv2/fsm/mutation_guards.go | 111 | 是 |
| pkg/controllerv2/fsm/mutation_handlers.go | 145 | 是 |
| pkg/controllerv2/fsm/mutation_helpers.go | 165 | 是 |
| pkg/controllerv2/state/doc.go | 6 | 是 |
| pkg/controllerv2/state/types.go | 206 | 是 |
| pkg/controllerv2/state/validate.go | 237 | 是 |
| pkg/controllerv2/state/normalize.go | 96 | 是 |
| pkg/controllerv2/state/codec.go | 106 | 是 |
| pkg/controllerv2/state/hashslots.go | 26 | 是 |
| pkg/controllerv2/state/errors.go | 12 | 是 |
| pkg/controllerv2/sync/doc.go | 7 | 是 |
| pkg/controllerv2/sync/server.go | 104 | 是 |
| pkg/controllerv2/sync/client.go | 251 | 是 |
| pkg/controllerv2/sync/contracts.go | 33 | 是 |
| pkg/controllerv2/sync/errors.go | 14 | 是 |
| pkg/controllerv2/planner/doc.go | 7 | 是 |
| pkg/controllerv2/planner/planner.go | 47 | 是 |
| pkg/controllerv2/planner/bootstrap.go | 223 | 是 |
| pkg/controllerv2/server/doc.go | 7 | 是 |
| pkg/controllerv2/server/server.go | 167 | 是 |
| pkg/controllerv2/statefile/doc.go | 7 | 是 |
| pkg/controllerv2/statefile/store.go | 127 | 是 |
| pkg/controllerv2/command/doc.go | 7 | 是 |
| pkg/controllerv2/command/command.go | 75 | 是 |
| pkg/controllerv2/command/codec.go | 45 | 是 |

52 个非测试 `.go` 文件，5702 行，全部通读（含 9 个纯包注释的 `doc.go`）。另读了 `pkg/controllerv2/FLOW.md`、
`pkg/controllerv2/docs/USAGE.md` 作为阶段 1 前置阅读；未找到 `pkg/controllerv2/AGENTS.md`（仓库根 `AGENTS.md`
的分层规则不直接约束这个子树，故未强行套用 a/usecase/runtime 分层检查）。

## 发现

### [P2] 1. apply scheduler 的 enqueue 与"调度器已死亡"之间存在 select 竞态，committed entry 可被静默丢弃、client 请求永久挂起

- **位置**：`pkg/controllerv2/raft/apply_scheduler.go:90-118`，触发点在 `pkg/controllerv2/raft/service_run.go:74-88`
- **类别**：并发 / 资源泄漏
- **代码**：
  ```go
  // apply_scheduler.go
  func (s *applyScheduler) enqueue(ctx context.Context, job toApply) error {
      select {
      case s.jobs <- job:          // 有缓冲(1024)，只要没满就"立即就绪"
          return nil
      case <-ctx.Done():
          return ctx.Err()
      case <-s.ctx.Done():         // 调度器已 cancel 之后也永久就绪
          return s.currentError()
      }
  }
  func (s *applyScheduler) run() {
      defer close(s.done)
      for {
          select {
          case <-s.ctx.Done():
              return
          case job := <-s.jobs:
              if err := s.applyJob(s.ctx, job); err != nil {
                  s.setError(err)
                  s.cancel()        // 调度器自杀，s.jobs 从此再无人消费
                  return
              }
          }
      }
  }
  ```
  ```go
  // service_run.go — processReady()
  trackerMu.Lock()
  tracker.bindAppended(ready.Entries)   // 先把 proposal 绑定到这批 entry 的 index
  trackerMu.Unlock()
  job := toApply{entries: ready.CommittedEntries, snapshot: ready.Snapshot}
  if err := scheduler.enqueue(context.Background(), job); err != nil {
      failAll(err)
      return err
  }
  ```
- **触发路径**：
  1. 某次 `ApplyBatch`（`fsm.StateMachine.ApplyBatch` 内部 `sm.store.Save`）因磁盘写失败/满盘/权限错误返回 error。
     `applyScheduler.run()` 在 `apply_scheduler.go:111-114` 捕获后 `s.setError(err); s.cancel(); return`——
     调度器 goroutine 退出，`s.jobs`（容量 1024 的带缓冲 channel）从此再没有 reader，但**从未被 close**。
  2. Raft 侧的主循环（`service_run.go` 的 `run()`）此刻仍是 leader（`failOnLeaderLoss()` 不会触发，因为节点角色没变），
     继续正常处理下一轮 `Ready()`。它在 `processReady()` 里先执行 `tracker.bindAppended(ready.Entries)`
     把新 committed 的 proposal 绑定进 `tracker.byIndex`，然后调用 `scheduler.enqueue(context.Background(), job)`。
  3. 此时 `enqueue()` 的 `select` 面对两个同时就绪的 case：`s.jobs <- job`（缓冲未满，发送立即成功）与
     `<-s.ctx.Done()`（调度器已经 cancel，永久就绪）。Go 的 `select` 在多个 case 同时就绪时**伪随机**选择，
     所以 `enqueue` 有相当概率走到 `case s.jobs <- job: return nil`，把 error 完全隐藏——调用方看到的是"入队成功"。
  4. `processReady()` 认为这批 entry 已经正常提交给调度器，继续 `rawNode.Advance(ready)`，`run()` 主循环不会
     调用 `s.setRunError`，`doneCh` **不会被 close**。
  5. 这批 committed entry 永远留在已经无人消费的 `s.jobs` 里；绑定到它们的 proposal 停留在
     `tracker.byIndex`，`tracker.complete()`（唯一能给 `proposalTracker` 摘除条目并给 `req.resp` 发信号的路径）
     永远不会被调用。
  6. 调用方阻塞在 `Service.submitProposal`（`service.go:228-237`）的第二个 `select`：`req.resp` 不会来信号，
     `doneCh` 不会 close（因为 `run()` 没有返回），唯一剩下的出口是调用方自己的 `ctx.Done()`。
     但 `pkg/controllerv2/runtime_refresh.go` 的周期 tick 用的是 `context.WithCancel(context.Background())`
     ——没有超时，只有 `Runtime.Stop()` 才会取消——所以这条 proposal 会挂到进程退出为止。
- **后果**：committed 但未 apply 的 Raft 日志条目在内存里静默"消失"（`cluster-state.json` 永久停在旧
  revision，不再前进），对应的 `Propose`/`ProbePropose` 调用方永久 hang（除非自带超时），
  `proposalTracker.byIndex` 里的条目泄漏直到 `Service.Stop()`。`Service.Status().Degraded` 会在**第一次**
  失败时被 `fsm.StateMachine` 正确置位（可被外部监控发现），但这之后所有卡住的具体 proposal 本身没有任何
  自动兜底；这也正是 staticcheck 标出的 `proposalTracker.failFrom`（`raft/proposal_tracker.go:60`）从未被
  调用的根因——本该在"调度器已确认死亡"时按 index 选择性失败排队中的 proposal，但设计上根本没有一处轮询
  `scheduler.currentError()` 的健康检查，`failFrom` 因此成了死代码，真正的缺口比"少写一个调用"更深。
- **建议**：`s.jobs` 满时不要用一个"发送 vs done 竞态"的裸 `select`；调度器 goroutine 退出前应该
  drain 并主动 fail 掉 `s.jobs` 里剩余的 job（或者 `enqueue` 先探测 `s.ctx.Err()`，探测到已死亡就直接
  返回错误而不进入发送 case），同时在 `run()` 主循环里补一个周期性的 `scheduler.currentError()` 检查，
  一旦非 nil 就整体走 `failAll` + 返回，让 `doneCh` 及时 close。

### [P2] 2. `Service.run()` 内部所有磁盘 I/O 都用 `context.Background()`，导致 `Stop()` 在磁盘卡顿时可能无限期挂起

- **位置**：`pkg/controllerv2/raft/service_run.go:41,70,85`；对应的 gosec G118 命中在 `pkg/controllerv2/raft/service.go:124`
- **类别**：并发 / 资源
- **代码**：
  ```go
  scheduler.start(context.Background())
  ...
  if err := store.SaveReady(context.Background(), ready.HardState, ready.Entries, ready.Snapshot); err != nil {
      failAll(err)
      return err
  }
  ...
  if err := scheduler.enqueue(context.Background(), job); err != nil {
  ```
  ```go
  // service.go Stop()
  close(stopCh)
  <-doneCh
  ```
- **触发路径**：`run()` 的 `for` 循环体是"先跑完 `processReady()`，再进 `select` 看 `stopCh`"
  （`service_run.go:101-106`）。如果 `processReady()` 正卡在 `store.SaveReady`（WAL fsync）或
  `scheduler.enqueue` 下游的 `ApplyBatch`/`MarkAppliedBatch`（`statefile.Store.Save` 的
  `tmp.Sync()`/`os.Rename`/`syncDir`）——例如磁盘慢、NFS 挂载卡住、磁盘满导致写入长时间不返回——
  这些调用全部使用 `context.Background()`，没有任何超时或与 `stopCh` 绑定的取消能力。
  此时即便 `Service.Stop()` 已经 `close(stopCh)`，`run()` 也感知不到，只能等这次磁盘调用
  自然返回（成功或出错）才会进入下一轮 `select` 去看 `stopCh`。`Stop()` 阻塞在 `<-doneCh`，
  没有超时兜底，会一直等下去。
- **后果**：不可控的关闭时延；上层（`Runtime.Stop()`）本身也没有给 `r.raft.Stop()` 加超时，
  一次磁盘异常就可能让整个 Runtime 的优雅关闭流程挂死。
- **建议**：把 `Start(ctx)` 传入的 ctx（或从 `stopCh` 派生的 ctx）传给 `SaveReady`/`enqueue`/
  调度器内部的存储调用，使其在收到停止信号时能够尽快返回错误而不是一直阻塞在系统调用上。

### [P3] 3. 周期性 refresh 循环把 `controlTick` 的所有错误直接丢弃，无日志、无告警、无退避

- **位置**：`pkg/controllerv2/runtime_refresh.go:26-32`
- **类别**：错误处理
- **代码**：
  ```go
  for {
      select {
      case <-ctx.Done():
          return
      case <-ticker.C:
          _ = r.controlTick(ctx)
      }
  }
  ```
- **触发路径**：`controlTick`（`runtime_refresh.go:38-60`）内部会调用 `r.raft.Propose`、
  `r.server.TickPlanner`、`r.publishFromState`（进而 `state.ClusterState.Validate()`），
  这些调用在非 `ErrNotLeader` 的情况下把 error 原样 return（例如 `Propose` 命中上面第 1 条
  bug 而 hang/最终失败、或 `TickPlanner` 底层 raft 报错、或 `Validate()` 因状态机产出了非法
  state 而失败）。这个 error 在 `runtime_refresh.go:31` 被 `_ =` 直接丢弃；整个文件没有 import
  任何日志包，没有一处 `log.*` 调用。
- **后果**：bootstrap 卡住、状态发布失败、proposal 持续失败等情况完全没有可观测性，运维只能靠
  `Service.Status().Degraded` 或外部行为异常间接发现，排障成本高。
- **建议**：至少把 `controlTick` 的错误计入一个可观测的计数器/日志，避免长期静默失败。

### [P3] 4. `fsm.StateMachine.ApplyBatch` 把状态机互斥锁跨越磁盘 fsync 持有，阻塞并发的只读快照/状态查询

- **位置**：`pkg/controllerv2/fsm/fsm.go:156-208`（尤其 157-158 与 200）
- **类别**：性能
- **代码**：
  ```go
  func (sm *StateMachine) ApplyBatch(ctx context.Context, entries []AppliedCommand) (BatchApplyResult, error) {
      sm.mu.Lock()
      defer sm.mu.Unlock()
      ...
      if err := sm.store.Save(ctx, next); err != nil {   // CreateTemp+Write+fsync+Rename+syncDir
          sm.degraded = true
          return out, err
      }
      sm.state = next.Clone()
  ```
  `Snapshot`/`IsDegraded` 都需要拿同一把 `sm.mu`（`fsm.go:126-138`）。
- **触发路径**：`pkg/controllerv2/sync/server.go` 的 `GetState`（供 mirror 节点轮询）、
  `Runtime.controlTick` 的 `sm.Snapshot(ctx)`、`Service.Status()` 的 `IsDegraded()` 都会在
  一次批量 commit 正在做 `statefile.Store.Save`（含两次 fsync：临时文件 `Sync()` 和
  `syncDir`）期间被同一把锁挡住，直到这次落盘完成。
- **后果**：慢盘/大批量提交时，mirror 节点的全量同步请求与本地状态查询会出现可观察的尾延迟毛刺；
  由于 apply 是单调度器 goroutine 串行执行的，不会造成死锁，只是延迟叠加。
- **建议**：把 `sm.state` 的发布与 `store.Save` 解耦（例如先落盘、短暂持锁只做指针切换），
  或者用 `RWMutex` + "写时复制"减少与只读路径的互斥窗口。

### [P3] 5. Raft WAL/snapshot/meta 文件与目录使用过宽的本地权限（0644/0755）

- **位置**：`pkg/controllerv2/raft/raftstore/wal.go:216,237,314,49`、`raft/raftstore/snapshot.go:26`、
  `raft/raftstore/metadata.go:54`、`pkg/controllerv2/runtime.go:63`
- **类别**：安全（本地权限加固，非远程可触发）
- **代码**：
  ```go
  // runtime.go:63
  if err := os.MkdirAll(r.cfg.StateDir, 0o755); err != nil {
  ```
  ```go
  // raftstore/wal.go（segment 创建/打开处均为 0o644 / 目录 0o755）
  f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
  ```
- **触发路径**：这是本地路径（由部署方配置的 `StateDir`/`RaftDir`，非远程输入驱动），gosec 的
  G301/G302 命中本质是"权限比 0700/0600 更宽"。在共享主机上，任何本地其他用户都能读取
  （0644）或列出（0755）ControllerV2 的集群拓扑、投票者地址、WAL 内容。
- **后果**：多租户/共享主机场景下的信息泄露面（拓扑、地址、Raft 日志内容对本机其他用户可读），
  不构成远程可触发漏洞。
- **建议**：目录改 `0700`、文件改 `0600`，与该目录下已经做得对的 `statefile`/`raftstore`
  temp+rename 原子写模式保持一致的加固水位。

## 已排除的候选项

- `pkg/controllerv2/state/hashslots.go:22`（`uint16(to)`，G115）——`to` 由
  `BuildInitialHashSlotTable` 内部循环从 `hashSlotCount`（本身是 `uint16`）按 `slotCount` 等分而来，
  数学上不可能超出 `uint16` 范围，且不是远程输入驱动，误报。
- `pkg/controllerv2/raft/raftstore/record.go:49,95,102,122`（`uint32(len(...))`，G115）——
  这些长度都是本地内存中的 Raft entry/record 大小（受 `MaxSizePerMsg`/WAL segment 大小等本地配置
  限制），不是远程输入直接驱动的长度前缀，且实际值远小于 `uint32` 上界，误报。
- `pkg/controllerv2/raft/raftstore/wal.go:299`（`uint64(pos)`，G115）——`pos` 来自 `Seek` 返回的
  `int64`，在本 WAL 实现中语义上永远非负（文件内偏移量），误报。
- `pkg/controllerv2/raft/raftstore/store.go:226,229`（`uint64(entry.Size())`，G115）——
  `proto.Size()` 不会返回负值，误报。
- `pkg/controllerv2/planner/bootstrap.go:178`（G115）——同上模式，本地循环计数转换，无远程输入路径。
- `pkg/controllerv2/raft/raftstore/{wal,snapshot,metadata}.go` 的 G304（"file inclusion via
  variable"）——路径全部来自启动时的本地 `Config.Dir`/`RaftDir`（部署方配置，`raftstore/config.go`
  的 `normalized()` 会做 `filepath.Abs`+`Clean`），不是网络请求参数拼接出来的路径，不构成路径穿越，
  归类为误报；权限过宽的那部分已单独作为发现 5 保留。
- `pkg/controllerv2/statefile/store.go` 的"torn file"疑虑——`Save()` 走的是
  `os.CreateTemp` → `Write` → `Sync()` → `Close()` → `os.Rename`（同目录，原子）→ `syncDir`；
  `Load()` 走 `state.Decode`，后者对 CRC32C checksum 做强制校验，不匹配直接返回
  `ErrChecksumMismatch`，不会静默接受损坏文件。`raftstore/snapshot.go`、`raftstore/metadata.go`
  是同样的 temp+fsync+rename+syncDir 模式。这条协调者给的线索经查证是**假警报**：这部分实现是本
  分片里做得最扎实的一块。
- `pkg/controllerv2/state/`、`pkg/controllerv2/sync/`、`pkg/controllerv2/planner/bootstrap.go` 里
  按 node/slot 建的临时 map（如 `desiredPeerLoad`）——均是每次调用时从 `st.Slots`/`st.Nodes`
  现场重建的局部变量，函数返回后即被 GC，不是长期持有的全局 map，没有无界增长路径。
  `sync/client.go` 的 `tried` map 同理是单次 `SyncOnce` 调用内的局部变量。
  `proposalTracker.byIndex` 本身**不是**这里排除的对象——它在发现 1 描述的场景下确实会无限增长，
  已单独作为真实发现保留。
- `pkg/controllerv2/fsm/mutation_helpers.go` 的 `findNode`/`findAssignment`/`findTaskBySlot`/
  `findTaskByID` 线性扫描——理论上是 O(n)，多次调用可堆成 O(n²)，但 ControllerV2 的
  节点/槽位/任务规模由 `HashSlotCount`/`ReplicaCount` 配置决定，在这个尚未上线、目标场景明确
  是小规模测试栈的实现里找不到能把 n 推到真正有性能意义的量级的路径，且没有远程输入能放大 n，
  写不出具体触发序列，排除。

## 本分片整体评价

`pkg/controllerv2` 是一次刻意做得比 v1 更规整的重写：命令编解码、状态校验、原子文件持久化、
WAL 校验和这几块的工程质量明显高于典型"快速试验"代码（尤其是 `statefile`/`raftstore` 的原子写
路径和 `state.Decode` 的强制 checksum 校验，完全避免了 v1 式的"torn file"风险）。但它并没有
重复 v1 的老问题，而是引入了一个**自己的新问题**：apply 调度器与 Raft 主循环之间用一个"发送 vs
已取消"的裸 `select` 做失败传播，在调度器崩溃后存在真实的竞态窗口，会让 committed entry 静默
卡死、client 请求永久 hang——这是本分片里最值得优先修的一条（发现 1）。

从"控制面职责覆盖"的对比角度看，`pkg/controllerv2`（5702 行）相对 `pkg/controller`（10941 行，
生产在跑）明显是**功能子集**而不是完整替代：`pkg/controllerv2/planner` 只有 `bootstrap.go`
一个文件、一种决策（`BootstrapPlanner`，只产出初始槽位分配），FLOW.md 自己也承认
"V1 only creates bootstrap assignment/task commands"；相对地，v1 的 `pkg/controller/plane`
下有独立的 `onboarding_planner.go`、`onboarding_executor.go`、`onboarding_fingerprint.go`
等一整套上线/扩缩容期间的编排逻辑，`pkg/controller/meta` 也有专门的 onboarding 状态存储
（`onboarding_store.go`/`onboarding_types.go`）。也就是说 v2 目前完全没有对应 v1 的"节点上线/
下线过程中的中间态编排与失败恢复"能力——它只覆盖了"从零启动时一次性分配槽位"这一个最简单的
子场景，节点故障后的重新分配、扩缩容、持续 reconciliation 都不存在。这与 FLOW.md 里明写的
"Do not replace `pkg/controller` in this package" 是一致的：v2 现在只是一个方向验证性的骨架，
不是可以接班的实现。鉴于本分片全部代码经协调者确认零 importer、不在生产运行路径上，所有发现均已
按规则下调一档；即便如此，发现 1（调度器死亡传播竞态）仍建议在 v2 继续往前推进之前优先修掉，
否则会把同一类"失败被静默吞掉"的设计缺陷带进下一阶段。
