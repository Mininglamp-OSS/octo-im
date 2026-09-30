# controller 单组 Raft 服务

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/controller/raft/command_codec.go | 2259 | 是 |
| pkg/controller/raft/service.go | 1912 | 是 |
| pkg/controller/raft/snapshot_chunks.go | 234 | 是 |
| pkg/controller/raft/command_inspection.go | 232 | 是 |
| pkg/controller/raft/status.go | 162 | 是 |
| pkg/controller/raft/config.go | 143 | 是 |
| pkg/controller/raft/transport.go | 127 | 是 |
| pkg/controller/raft/logging.go | 16 | 是 |

（测试文件 service_test.go / service_integration_test.go / transport_test.go / logging_test.go /
command_codec_benchmark_test.go / command_inspection_test.go / config_test.go 按 brief 约定不计入审计范围，
仅作为理解设计意图的参考读物。）

## 发现

### [P0] 1. 任意未认证远端可用一个 `MsgHeartbeatResp` 帧永久杀死目标节点的控制面 Raft 循环（`ErrStepPeerNotFound` 未被过滤）

- **位置**：`pkg/controller/raft/service.go:956-961`
- **类别**：安全 / 分布式一致性 / 正确性
- **代码**：
  ```go
  case msg := <-stepCh:
      if err := rawNode.Step(msg); err != nil && !errors.Is(err, raft.ErrStepLocalMsg) {
          s.setError(err)
          failTracked(pendingQueue, pendingByIndex, err)
          return
      }
  ```
  `go.etcd.io/raft/v3@v3.6.0/rawnode.go:120-125`（依赖库，供参照）：
  ```go
  if IsLocalMsg(m.Type) && !IsLocalMsgTarget(m.From) {
      return ErrStepLocalMsg
  }
  if IsResponseMsg(m.Type) && !IsLocalMsgTarget(m.From) && rn.raft.trk.Progress[m.From] == nil {
      return ErrStepPeerNotFound
  }
  ```
  `util.go:41-50` 的 `isResponseMsg` 包含 `MsgAppResp / MsgVoteResp / MsgHeartbeatResp / MsgUnreachable / MsgReadIndexResp / MsgPreVoteResp`。

- **触发路径**：
  1. `pkg/controller/raft/transport.go:15` 定义 `msgTypeControllerRaft uint8 = 5`，`service.go:402` 通过
     `s.cfg.Server.Handle(msgTypeControllerRaft, s.handleMessage)` 注册到共享的 `transport.Server`。
  2. `pkg/transport/server.go:186-190` 的 `dispatch` 对该 msgType 直接 `h(body)`，`pkg/transport/{server,client,pool}.go`
     中 `grep -i "auth|token|tls"` **零命中** —— 帧没有任何认证或加密。
  3. 攻击者（或任何配置错误、已被移除、尚未加入的进程）向该端口发送一帧
     `raftpb.Message{Type: MsgHeartbeatResp, From: 0xDEADBEEF, To: <目标 nodeID>}`。
  4. `service.go:782-796` 的 `dropInboundRaftMessage` 只拦 `msg.To != localNodeID` / `msg.From == localNodeID` /
     `msg.To == msg.From` 三种情况 —— `From` 是一个**任意其它** ID，全部放过。
  5. `rawNode.Step` 发现 `trk.Progress[0xDEADBEEF] == nil` → 返回 `ErrStepPeerNotFound`；
     它不是 `ErrStepLocalMsg` → `s.setError(err)` + `return`。
  6. `grep -n "go s.run(" service.go` → **只有 `:372` 一处**，无 watchdog / supervisor / 重启逻辑
     （`pkg/cluster/controller_host.go:172-177` 的 `Start` 只在进程启动时调一次）。run goroutine 永久消失。
- **后果**：单帧未认证 UDP/TCP 报文即可让该节点的控制面 Raft 永久停摆 —— 不再选举、不再 apply、不再调度。
  对三个 controller voter 各发一帧即彻底摘除整个集群控制面（Slot bootstrap / repair / rebalance / leader transfer 全停），
  且进程仍在运行、数据面仍在转发消息，因此不会被进程级存活探针发现。**这是不需要任何凭证的控制面 kill switch。**
  同一机制下 `raft.ErrProposalDropped`（leader 正在转移领导权、或本节点已被移出配置时收到转发的 `MsgProp`）
  同样不被过滤，会造成同样的永久死亡。
- **建议**：`Step` 的返回值应按"可忽略的协议级拒绝"处理 —— 至少把 `ErrStepPeerNotFound`、`ErrProposalDropped`
  与 `ErrStepLocalMsg` 一起降级为 warn 日志后 `continue`；同时在 `dropInboundRaftMessage` 里对
  `msg.From` 做一次已知 peer 白名单校验，并给 transport 加认证。

### [P0] 2. run loop 把**所有** apply/decode 错误当作致命且不可恢复，无任何看门狗；committed 业务错误即毒丸

- **位置**：`pkg/controller/raft/service.go:942-947`、`1015-1029`
- **类别**：分布式一致性 / 正确性 / 架构
- **代码**：
  ```go
  for {
      if err := processReady(); err != nil {
          s.setError(err)
          return
      }
  ```
  ```go
  cmd, err := decodeCommand(entry.Data)
  if err != nil {
      return 0, err
  }
  err = s.cfg.StateMachine.Apply(ctx, cmd)
  ...
  if err != nil {
      return 0, err
  }
  ```
- **触发路径 / 错误路径完整枚举**：所有能走到 `s.setError(err); return` 的路径，按"致命是否合理"分类 ——

  | # | 位置 | 错误来源 | 性质 | fatal 是否合理 |
  |---|---|---|---|---|
  | 1 | `service.go:896` | `storageView.persistReady` → raftlog `Save` / MemoryStorage `Append` | 真实存储失败 | **是**（不能丢已承诺的持久化） |
  | 2 | `service.go:914` | `transport.Send` | 见下：实际上**不可达** | — |
  | 3 | `service.go:1015-1018` | `decodeCommand(entry.Data)` | **wire format / 版本歪斜 / 日志损坏** | **否** |
  | 4 | `service.go:1019-1028` | `StateMachine.Apply` | **业务 / 状态相关错误** | **否（根因）** |
  | 5 | `service.go:1032,1039` | ConfChange `cc.Unmarshal` | 日志损坏 | 否（应跳过而非自杀） |
  | 6 | `service.go:1050` | `storage.MarkApplied` | 真实存储失败 | **是** |
  | 7 | `service.go:1061`(经 `applyReadyState:1001`) | `restoreReadySnapshot` → `StateMachine.Restore` | leader 推送的快照不兼容/损坏 | 否（远端可影响） |
  | 8 | `service.go:957` | `rawNode.Step` | 见发现 1，**远端可触发** | **否** |

  其中 #2 实际不可达：`transport.go:44-65` 的 `Send` 对真正的网络发送做了
  `_ = t.client.Send(...)`（注释明写 "transient send failures are not fatal"），
  只有 `ctx.Err()`（这里是 `context.Background()`，永不返回）和 `msg.Marshal()` 会返回错误。
  也就是说：**真正该被容忍的传输错误被容忍了，而绝不该致命的业务错误被当成了致命错误。**

  #4 的具体毒丸序列（与 `pkg/controller/plane/statemachine.go:627,630,633` 联动）：
  1. leader 提案一条 `NodeOnboardingJobUpdate`，`Propose` 成功、entry 被**持久化并 commit**。
  2. `applyReadyState` 调 `StateMachine.Apply`；该命令的状态保护条件此时不成立，返回 `ErrInvalidArgument`。
  3. `return 0, err` → `processReady` `return err` → `setError` → run goroutine 退出。
  4. 注意 `rawNode.Advance(ready)` 在 `service.go:926` 位于 apply **之后**，apply 失败时永不执行 ——
     applied 点没有推进，但 entry 已经在 durable log 里。
  5. 该 entry 是 committed 的，**每个副本**都会 apply 到它、拿到同一个确定性错误、同样自杀。
  6. 重启后 `Start` → `storageView.load` 重放 → 同一 entry → 同样的错误 → 再次自杀。**不可自愈。**
- **后果**：一条被 commit 的命令即可确定性地、永久地摧毁整个控制面集群，且重启无效，只能人工做 raft log 数据手术。
  `FLOW.md` 第 8 节"避坑清单"自己写了 *"Onboarding Apply 冲突必须 no-op ... 都不能从 StateMachine.Apply 返回业务错误"* ——
  说明设计者知道这条不变量，但 raft 侧没有任何防御来兜住违反它的情况。
- **建议**：apply 阶段区分"存储/IO 错误"（可 fatal）与"命令级错误"（必须记日志 + 推进 applied index + 把错误只回给提案者）；
  并为 run goroutine 加 supervisor（退出即重启，带退避与显式健康降级），使 fatal 不等于永久终态。

### [P1] 3. run loop 死亡是**静默**的：`Status` 没有任何字段承载致命错误，死亡与"正常停止"不可区分

- **位置**：`pkg/controller/raft/service.go:1224-1244`、`service.go:649-660`、`pkg/controller/raft/status.go:22-41`
- **类别**：可观测性 / 架构
- **代码**：
  ```go
  func (s *Service) setError(err error) {
      var transport *raftTransport
      s.mu.Lock()
      s.err = err
      if !s.stopping {
          transport = s.transport
          s.started = false
          ...
      }
      s.mu.Unlock()
      ...
      s.recordStoppedStatus()
  }
  ```
  ```go
  func (s *Service) recordStoppedStatus() {
      s.statusMu.Lock()
      compaction := s.status.Compaction
      restore := s.status.Restore
      s.status = Status{
          NodeID:     s.cfg.NodeID,
          Role:       RoleUnknown,
          Compaction: compaction,
          Restore:    restore,
      }
      s.statusMu.Unlock()
  }
  ```
- **触发路径**：
  1. 发现 1 或发现 2 的任一路径触发 `setError(err)`。
  2. `setError` 把 `s.started` 置回 `false` 并把 `stepCh/proposeCh/compactCh/stopCh/doneCh` 全部设为 nil。
  3. **已经在 select 上等待的** `Propose`（`service.go:521-530`）会因 `doneCh` 关闭而返回 `s.currentError()`，
     拿到真实错误 —— 这是唯一一次错误可见的机会。
  4. 之后**所有新的** `Propose` 在 `service.go:491-494` 撞上 `if !s.started { return ErrNotStarted }`，
     拿到的是 `ErrNotStarted` —— 与"服务从未启动"完全相同的错误，真实死因 `s.err` 被彻底掩盖。
  5. `Status()` 返回 `Role=RoleUnknown`、`LeaderID/Term/CommitIndex/AppliedIndex` 全为 0 ——
     `status.go` 的 `Status` 结构体只有 `Compaction.Degraded` 和 `Restore.Failed` 两个失败标志位，
     **没有任何字段表示 run loop 已致命退出**。对于 apply/decode/Step 类死因，`Status` 上留不下一丝痕迹。
  6. `pkg/cluster/controller_handler.go:79-84` 的 `controllerRPCControllerRaftStatus` 管理接口读的就是这个 `Status`，
     因此管理后台看到的也只是"role=unknown"。
- **后果**：控制面死亡后，运维侧观察到的现象是"leader 一直是 0 / role unknown / 提案返回 not started"，
  与"节点刚起来还没选出 leader"或"服务被正常停止"无法区分。没有 readiness 探针会红，没有告警会响。
  一个静默死亡的控制面比一个大声崩溃的控制面危险得多：故障会被当成"选举慢"而被忽略数小时。
- **建议**：在 `Status` 中新增 `Failed bool` / `FailedError string` / `FailedAt time.Time`，由 `setError` 写入；
  `Propose` 在 `s.err != nil` 时返回包装后的真实错误而非 `ErrNotStarted`；并把该状态接到进程 readiness。

### [P0] 4. 单个 ~81 字节未认证快照分片帧即可让进程 `make([]byte, 1<<60)` 崩溃（`chunk.total` 无上界）

- **位置**：`pkg/controller/raft/snapshot_chunks.go:97-109`、`snapshot_chunks.go:204-234`
- **类别**：安全 / 远程可触发 panic / 资源
- **代码**：
  ```go
  entry := a.pending[key]
  if entry == nil {
      if chunk.total > uint64(math.MaxInt) {
          return raftpb.Message{}, false, fmt.Errorf("controller raft snapshot total overflows int: %d", chunk.total)
      }
      entry = &controllerRaftSnapshotChunkAssembly{
          message:    append([]byte(nil), chunk.message...),
          total:      chunk.total,
          data:       make([]byte, int(chunk.total)),   // <-- 唯一上界是 math.MaxInt
          chunks:     make(map[uint64]int),
          lastUpdate: now,
      }
      a.pending[key] = entry
  }
  ```
  `decodeControllerRaftSnapshotChunkBody` 对 `total` 的**唯一**校验（`snapshot_chunks.go:230-232`）：
  ```go
  if chunk.offset > chunk.total || uint64(len(chunk.data)) > chunk.total-chunk.offset {
      return controllerRaftSnapshotChunk{}, fmt.Errorf("... range out of bounds ...")
  }
  ```
  即 `total` 只需 **≥ offset+len(data)**，没有任何"不得超过帧大小 / 不得超过合理快照上限"的检查。

- **触发路径**：
  1. `service.go:403`：`s.cfg.Server.Handle(msgTypeControllerRaftSnapshotChunk, s.handleSnapshotChunkMessage)`
     —— `msgTypeControllerRaftSnapshotChunk = 6`（`transport.go:16`），transport 无认证、无 TLS。
  2. 攻击者发送一帧 body，长度恰好 `72 + 0 + 1 = 73` 字节以上（`message` 需非空，取一个最短合法
     `raftpb.Message` marshal，约 6 字节 → body 约 79-81 字节）：
     `total = 1<<60`、`offset = 0`、`dataLen = 1`、`msgLen = 6`。
  3. `decodeControllerRaftSnapshotChunkBody` 通过：`len(body) == 72+6+1`，`offset(0) <= total`，
     `len(data)=1 <= total-0`。
  4. `service.go:744-756` 的 `handleSnapshotChunkMessage` 直接 `s.assembler.add(chunk)` ——
     **注意 `dropInboundRaftMessage`（`to`/`from` 过滤）只在 `stepMessage` 里，位于组装完成之后**，
     所以连"这帧是不是发给我的"都没检查就先分配了内存。
  5. `add()` 里 `chunk.total = 1<<60 < math.MaxInt`，检查通过 → `make([]byte, 1<<60)`。
     Go runtime `makeslice`：`mem = 1<<60 > maxAlloc`（64 位约 `1<<48`）→ `panicmakeslicelen()`
     → panic `runtime error: makeslice: len out of range`。
  6. 该 handler 由 `pkg/transport/server.go:186-190` 的 `dispatch` **同步**调用（`h(body)`），
     运行在 MuxConn 的 reader goroutine 上。`grep -rn "recover()" pkg/transport/*.go` → **零命中**，
     `serveConn`（`server.go:143-157`）的 defer 只做 cancel/Close，没有 recover。→ **整个进程崩溃。**
  7. 若把 `total` 取成 `1<<40`（1 TiB，仍远小于 `maxAlloc`），则走 `mallocgc` → `runtime: out of memory`
     致命错误 / 被 OOM killer 杀死，同样不可 recover。
- **后果**：任何能连上 controller transport 端口的进程，用**一个 80 字节的报文**即可杀死整个 IM 节点进程
  （不只是控制面，连带数据面、网关、所有连接）。无需凭证、无需握手、可无限重复。
  次级放大：即便 `total` 取 1 GiB 这种"不崩但很贵"的值，`a.pending` 按
  `{chunkID, from, to, index, term}` 五元组作 key，攻击者自由选择 `chunkID` → 每帧一个新 entry，
  每个 entry 立刻 `make` 满 `total` 字节，只有 `pruneExpiredLocked` 的 **2 分钟 TTL**
  （`defaultControllerRaftSnapshotChunkTTL`）才回收 → 1000 帧 × 1 GiB = 1 TiB 常驻分配请求。
- **建议**：`chunk.total` 必须有显式业务上限（例如"不超过配置的最大 controller 快照字节数"），
  并在 `add()` 之前对 `chunk.to == s.cfg.NodeID`、`chunk.from` 属于已知 peer 做校验；
  同时限制 `pending` 的条目数与总驻留字节数，并给 transport handler 加 `recover()` 兜底。

### [P2] 5. 分片发送的所有网络错误被静默丢弃且中途放弃，接收端只能等 2 分钟 TTL 超时

- **位置**：`pkg/controller/raft/transport.go:110-117`
- **类别**：错误处理 / 分布式正确性
- **代码**：
  ```go
  body := encodeControllerRaftSnapshotChunkBody(chunk)
  if len(body) > bodyLimit {
      return nil
  }
  if err := t.client.Send(uint64(msg.To), controllerShardKey, msgTypeControllerRaftSnapshotChunk, body); err != nil {
      return nil
  }
  ```
- **触发路径**：
  1. leader 给落后 follower 推送大快照，`Send`（`transport.go:57-60`）判断
     `len(data) > frameBodyLimit()` 且是 `MsgSnap` → 走 `sendSnapshotChunks`。
  2. 循环发第 k 个分片时连接抖动，`t.client.Send` 返回 error → `return nil`：
     **剩余分片全部不发，且向调用方报告成功**。
  3. 接收端 `a.pending[key]` 里留着一个已 `make` 满 `total` 字节、永远等不到剩余数据的 entry；
     `entry.received < entry.total` → 永不 `Step`。
  4. leader 侧 `rawNode` 认为 `MsgSnap` 已发出，进入 `StateSnapshot`/`PendingSnapshot`，
     要等到 raft 自己的重试超时才会重发。
- **后果**：大快照追赶在网络抖动下会卡住一个完整的重试周期，接收端同时白占 `total` 字节内存最多 2 分钟。
  `len(body) > bodyLimit` 那条 `return nil` 更糟：这是纯粹的编码 bug（分片算得太大），
  却被当成"发送成功"，快照**永远**发不出去且无任何日志。
- **建议**：这两处应返回 error 并至少打一条 warn 日志，让上层知道快照推送失败；
  `len(body) > bodyLimit` 属于内部不变量破坏，应视为编程错误而非静默成功。

### [P2] 6. `MaxCommittedSizePerReady/MaxSizePerMsg` 都设为 `MaxUint64`，一次 Ready 的 apply 可任意长地饿死 `rawNode.Tick()`，且 `time.Ticker` 会丢 tick 导致 raft 逻辑时钟走慢

- **位置**：`pkg/controller/raft/service.go:823-832`、`service.go:889-947`
- **类别**：性能 / 分布式正确性
- **代码**：
  ```go
  return raft.NewRawNode(&raft.Config{
      ID:                       s.cfg.NodeID,
      ElectionTick:             electionTick,     // 10
      HeartbeatTick:            heartbeatTick,    // 1
      Storage:                  storageView.memory,
      Applied:                  appliedIndex,
      MaxSizePerMsg:            math.MaxUint64,
      MaxCommittedSizePerReady: math.MaxUint64,
      MaxInflightMsgs:          256,
  ```
  ```go
  for {
      if err := processReady(); err != nil { ... }   // <-- 这里可能跑很久
      select {
      case <-stopCh: ...
      case <-ticker.C:
          rawNode.Tick()                             // <-- raft 逻辑时钟只在这里推进
  ```
- **触发路径**：
  1. `MaxCommittedSizePerReady = MaxUint64` **显式关闭**了 etcd raft 内建的"单个 Ready 最多带多少已提交字节"
     的分批机制，所以 `ready.CommittedEntries` 可以一次性包含**全部**积压的已提交 entry
     （节点重启后 replay、follower 长时间落后后追赶、或 leader 一次性 commit 一大批）。
  2. `applyReadyState`（`service.go:998-1056`）对每条 entry 同步做
     `decodeCommand` → `StateMachine.Apply`（Pebble 写）→ `OnCommittedCommand` 回调
     （`service.go:1020-1022` → `pkg/cluster/controller_host.go:596` 的 `handleCommittedCommand`，会做快照失效、reload 入队等工作）。
     整个过程在 **`processReady()` 内部**，而 `rawNode.Tick()` 只在其**之后**的 `select` 分支里。
  3. 假设积压 20000 条命令、每条 apply 0.5ms → `processReady` 独占 10 秒。这 10 秒内：
     - `rawNode.Tick()` 一次都不执行 → 作为 leader 不发心跳、作为 follower 不推进选举计时；
     - `ticker` 是 `time.NewTicker`（channel 容量 1），10 秒内产生的 ~100 个 tick **只有 1 个被保留**，
       其余全部丢弃 → 恢复后 `electionElapsed` 只 +1 而不是 +100，**raft 的逻辑时钟永久性地走慢了 99 个 tick**；
     - `stepCh`（容量 1024）期间被塞满，`stepMessage` 的 `default:` 分支静默丢弃后续消息。
  4. 结果：stall 期间没有心跳 → 其它 voter 在 `electionTick=10`（1 秒）后发起选举、抢走领导权；
     stall 结束的节点因为时钟走慢，对"leader 已失联"的判定也被推迟。
- **后果**：控制面在任何批量 apply（重启 replay、大规模 onboarding、follower 追赶）期间会出现
  **秒级心跳空洞与领导权抖动**，且 raft 的选举超时语义被 ticker 丢 tick 破坏，不再等于"10 × 100ms 真实时间"。
  这与 CI 里把 `TestThreeControllerVoters` 及另外三个 raft/cluster 测试标记为 flaky 而 `-skip` 掉
  （`.github/workflows/ci.yml:72-78`）高度吻合：三 voter 场景下 bootstrap/追赶阶段正是批量 apply 最集中的时刻，
  领导权是否抖动取决于机器快慢 —— **这是时序上的真实缺陷，不只是"测试写得敏感"**。
- **建议**：给 `MaxCommittedSizePerReady` 设一个有限值（让 raft 自己分批），并把 raft 逻辑时钟改为
  基于 `time.Since(lastTick)` 补偿式推进（一次 stall 后按真实经过时间调用多次 `Tick()`），
  或把 apply 移出 Ready 处理线程。

### [P2] 7. 超过 64 MB 的**非快照** raft 消息会被静默丢弃并击杀承载整条控制面流量的连接

- **位置**：`pkg/controller/raft/transport.go:57-64`、`pkg/controller/raft/service.go:829`
- **类别**：分布式正确性 / 错误处理
- **代码**：
  ```go
  if len(data) > t.frameBodyLimit() && isControllerRaftSnapshotMessage(msg) {
      if err := t.sendSnapshotChunks(ctx, msg); err != nil {
          return err
      }
      continue
  }
  // Match multiraft transport semantics: transient send failures are not fatal
  // to the local raft loop and are retried by later raft traffic.
  _ = t.client.Send(uint64(msg.To), controllerShardKey, msgTypeControllerRaft, data)
  ```
  ```go
  func isControllerRaftSnapshotMessage(msg raftpb.Message) bool {
      return msg.Type == raftpb.MsgSnap && msg.Snapshot != nil && len(msg.Snapshot.Data) > 0
  }
  ```
- **触发路径**：
  1. `MaxSizePerMsg: math.MaxUint64`（`service.go:829`）→ 一条 `MsgApp` 可以携带 leader 日志里
     **全部**待复制 entry，没有任何字节上限。
  2. 当 Controller log compaction 被显式关闭（`LogCompactionConfig{Enabled:false, EnabledSet:true}`），
     或积压的 `NodeOnboardingJobUpdate` 命令本身很大（每条携带完整 job + plan + moves）时，
     单条 `MsgApp` 的 marshal 结果可以超过 `transport.MaxMessageSize = 64 << 20`（`pkg/transport/errors.go:16`）。
  3. 该消息**不是** `MsgSnap`，所以 `isControllerRaftSnapshotMessage` 为 false → 不走分片，
     直接 `_ = t.client.Send(...)`，**返回值被丢弃、无任何日志**。
  4. 发送侧 `encodeHeader`（`pkg/transport/frame.go:50-55`）只做 `uint32(bodyLen)`，不做上限检查，
     帧被写出去；接收侧 `readFrame`（`frame.go:66-69`）`bodyLen > MaxFrameSize` → 返回 `ErrMsgTooLarge`
     → **reader 报错、整条 MuxConn 被关闭**。
  5. 这条连接是 `controllerShardKey = 1` 上承载**所有** controller raft 帧的连接。
     raft 重试 → 生成同样的巨型 `MsgApp` → 再次击杀连接 → **追赶活锁**。
     只有等 leader 压缩到该 follower 的 `Next` 之前、改走 `MsgSnap` 才能脱困。
- **后果**：落后较多的 controller follower 可能永远追不上，并反复拆掉与 leader 之间的共享连接
  （连带影响同一连接上的其它 msgType）。整个过程零日志、零指标，排查时只能看到"连接反复重连"。
- **建议**：给 `MaxSizePerMsg` 设成小于 `transport.MaxMessageSize` 的有限值（这是 etcd raft 的标准做法），
  并在 `Send` 中对"超限且不可分片"的消息显式返回/记录错误，而不是 `_ =` 丢掉。

### [P3] 8. `command_codec.go` 中约 1100 行是同一 wire format v1 的**第二份完整实现**（30 个 U1000 函数），无任何测试覆盖

- **位置**：`pkg/controller/raft/command_codec.go:107`（`encodeCommandEnvelopeBinary`）、
  `:203`（`decodeCommandEnvelopeBinary`）、`:430`（`commandEnvelopeFieldMask`）、
  `:524-559`（`appendCommandAssignment*`/`appendCommandTask*`）、
  `:736-851`（`appendCommandNodeOnboarding*` 家族）、
  `:989`/`:1144-1250`/`:1371`/`:1756-2108`（对应的 `readTaskAdvance`/`readAssignment*`/`readTask*`/`readNodeOnboarding*` 家族）
- **类别**：架构 / 死代码
- **代码**（两条并行路径的 magic 与 version 完全相同）：
  ```go
  func encodeCommandBinary(cmd slotcontroller.Command) ([]byte, error) {        // 活
      data = append(data, commandEnvelopeBinaryMagic[:]...)
      data = append(data, commandEnvelopeBinaryVersion, byte(cmd.Kind))
      ...
      data = appendCommandMetaAssignment(data, *cmd.Assignment)                  // Meta 版
  ```
  ```go
  func encodeCommandEnvelopeBinary(envelope commandEnvelope) ([]byte, error) {  // 死
      data = append(data, commandEnvelopeBinaryMagic[:]...)                      // 同一 magic
      data = append(data, commandEnvelopeBinaryVersion, byte(envelope.Kind))     // 同一 version
      ...
      data = appendCommandAssignment(data, *envelope.Assignment)                 // Envelope 版
  ```
- **触发路径 / 这意味着什么**（逐条回答"是否可经版本歪斜到达"）：
  1. **唯一的活 dispatcher** 是 `service.go:1434-1441` 的 `decodeCommand`：
     ```go
     if hasCommandEnvelopeBinaryMagic(data) {
         return decodeCommandBinary(data)
     }
     envelope, err := decodeCommandEnvelopeLegacyJSON(data)
     ```
     `grep -rn "decodeCommandEnvelopeBinary" pkg/ internal/` → 只有定义处一条命中。
     **所以死的 binary 路径不可经版本歪斜到达** —— 它没有任何 magic/tag 路由到它，
     brief 里"unused 其实是 untested decode path"的假设在这里**不成立**。
  2. 没有"onboarding 丢失了 binary 支持而悄悄退化"：`encodeCommandBinary:101-103` 用
     `appendCommandMetaNodeOnboardingUpdate`、`decodeCommandBinary:418-421` 用
     `readMetaNodeOnboardingUpdate`，onboarding 命令有**完整的活 binary 支持**。
     死掉的 `appendCommandNodeOnboarding*` / `readNodeOnboarding*` 是同一格式针对中间层
     `nodeOnboardingJobEnvelope`（JSON 结构体）的重复实现。
  3. 真正的成因：编码路径从 `Command → commandEnvelope → binary` 重构成了
     `Command → binary` 直达（`encodeCommand` 只调 `encodeCommandBinary`），
     中间那一跳的整套编解码器被遗留下来。`commandEnvelope` 本身仍是活的
     （`decodeCommandEnvelopeLegacyJSON` 读旧 JSON 日志要用），所以编译器和 staticcheck
     只能报出它的**binary 编解码器**是死的。
- **后果**：两份实现共享同一 magic + 同一 version 且**没有任何区分位**。一旦有人"复活"
  envelope 路径，或在维护时只改了 Meta 版而漏改 Envelope 版（两者已经是逐字段一一对应的手抄关系），
  就会得到一个静默的、只在某些命令类型上发散的 wire 不兼容。而死的那一半
  （`command_codec_benchmark_test.go` / `service_test.go` 都只测 `encodeCommand`/`decodeCommand`）
  **零测试覆盖**，发散不会被任何测试发现。约 1100 行 / 2259 行 ≈ 该文件一半是这种死重复。
- **建议**：直接删除 envelope 的 binary 编解码器（`encodeCommandEnvelopeBinary` /
  `decodeCommandEnvelopeBinary` / `commandEnvelopeFieldMask` 及其 `append*`/`read*` 家族），
  只保留 legacy JSON 解码所需的 `commandEnvelope` 结构与 `commandFromEnvelope`。

### [P3] 9. `FLOW.md` 里 `pkg/controller/raft` 的行号引用全部过期，且缺失快照分片的失败语义说明

- **位置**：`pkg/controller/FLOW.md` 第 5.1 / 7 节 vs `pkg/controller/raft/service.go`
- **类别**：架构与文档一致性
- **代码 / 对照**：

  | FLOW.md 写的 | 实际位置 |
  |---|---|
  | `入口: raft/service.go:214 Propose` | `Propose` 在 `service.go:489` |
  | `→ raft/service.go:304 run` | `run` 在 `service.go:781` |
  | `Bootstrap 触发 ... raft/service.go:170` | 在 `service.go:868-872` |
  | `plane/statemachine.go:47 Apply` | `Apply` 在 `statemachine.go:65` |

- **触发路径**：`AGENTS.md` 规定"有 `FLOW.md` 则必读"，新人按 FLOW.md 的行号跳转会落到完全无关的代码
  （`service.go:214` 落在 `nodeOnboardingPlanEnvelope` 的结构体定义里，`:304` 落在 `Start` 的中段）。
- **后果**：文档的导航价值失效；更实质的是 §7 的 "Controller Raft 大快照传输" 小节只描述了
  happy path（"超过单帧预算时分片，接收端必须完整重组后再 Step"），完全没有提到
  **发送中断后接收端要靠 2 分钟 TTL 才回收**（见发现 5），也没有提到 `chunk.total` 没有上界（见发现 4），
  因此读文档的人不会意识到这条路径上有两个未处理的失败模式。
- **建议**：FLOW.md 改为引用函数名而非行号（行号必然腐烂），并补上快照分片路径的失败语义与容量约束。

### [P3] 10. transport handler 永不注销：`Stop()`/`setError()` 之后快照分片仍继续分配内存，且 `s.assembler` 存在无锁读写竞争

- **位置**：`pkg/controller/raft/service.go:401-403`、`service.go:744-751`、`service.go:412-471`（`Stop`）
- **类别**：并发 / 资源
- **代码**：
  ```go
  s.assembler = newControllerRaftSnapshotAssembler(defaultControllerRaftSnapshotChunkTTL, time.Now)  // 持 s.mu
  s.cfg.Server.Handle(msgTypeControllerRaft, s.handleMessage)
  s.cfg.Server.Handle(msgTypeControllerRaftSnapshotChunk, s.handleSnapshotChunkMessage)
  ```
  ```go
  func (s *Service) handleSnapshotChunkMessage(body []byte) {
      chunk, err := decodeControllerRaftSnapshotChunkBody(body)
      if err != nil {
          return
      }
      if s.assembler == nil {                                        // 无锁读
          s.assembler = newControllerRaftSnapshotAssembler(...)       // 无锁写
      }
      msg, ok, err := s.assembler.add(chunk)
  ```
- **触发路径**：
  1. `pkg/transport/server.go:71-83` 的 `Handle` 只能**覆盖**某个 msgType 的 handler，
     没有任何注销 API；`Stop()`（`service.go:412-471`）和 `setError()` 都不尝试注销。
  2. 因此 `Stop()` 返回后（或 run loop 因发现 1/2 致命退出后），msgType 5/6 的 handler 仍然挂在
     共享的 `transport.Server` 上。
  3. 此后每一个到达的分片帧仍会走完 `decodeControllerRaftSnapshotChunkBody` → `s.assembler.add(chunk)`，
     **仍然执行 `make([]byte, int(chunk.total))` 并在 `a.pending` 里驻留 2 分钟**
     （只有 `stepMessage` 里的 `if !started { return }` 拦住了后续投递，而内存已经分配完了）。
     → 发现 4 的崩溃/耗内存通道在服务停止后依然畅通。
  4. 并发竞争：`Start` 在持 `s.mu` 的情况下写 `s.assembler`（`:401`），而
     `handleSnapshotChunkMessage` 在 MuxConn reader goroutine 上**不持任何锁**读写同一字段（`:749-751`）。
     由于 handler 从第一次 `Start` 之后就永久注册，一次 `Stop()` → `Start()` 重启
     （`Start` 里 `if s.started { if s.stopping { ...continue } }` 的循环结构说明重启是被支持的流程）
     即可与在途的入站分片帧并发 → 对同一指针字段的无同步读写，是 `go test -race` 可检出的数据竞争。
     另外两个并发 reader goroutine 同时看到 `nil` 也会各自 new 一个 assembler 并互相覆盖。
- **后果**：停止后仍可被远端诱导分配内存（放大发现 4 的攻击面，且此时连"服务已停"都不能作为防线）；
  重启路径上存在真实数据竞争。生产 `pkg/cluster` 目前只在 `cluster.go:262` 调一次 `Start`、
  在 `cluster.go:631` 调一次 `Stop`，不做重启，所以竞争在当前生产编排下不可达 —— 故评 P3 而非 P1。
- **建议**：给 `transport.Server` 增加注销能力并在 `Stop`/`setError` 里注销 msgType 5/6；
  `s.assembler` 改为在 `NewService` 里一次性创建且只读（它自身已有 `mu`），或用 `atomic.Pointer` 持有。

## 已排除的候选项

**gosec 命中（15 条，全部为 G115，逐条核实后全部误报）**

- `command_codec.go:1292`、`:1557`、`:1610`、`:1648`、`:1922`、`:1975`、`:2007` —— `int(count)` 用于
  `make([]T, 0, int(count))`。这些 `count` 全部来自 `readItemCount(minItemSize)`
  （`command_codec.go:2109-2124`），它做了**正确的有界检查**：
  ```go
  if minItemSize <= 0 { minItemSize = 1 }
  if count > uint64(len(r.rest)/minItemSize) {
      return 0, errCommandEnvelopeCorrupt
  }
  ```
  即 `count ≤ 剩余缓冲区字节数 / 单项最小字节数`。这与仓库里两个已知正确范式
  （`internal/access/node/delivery_push_codec.go:283 readCollectionLen`（调用点如 `:88`、`:213`）、
  `pkg/cluster/codec_control.go:995`）是**同一个惯用法**。→ 误报。
- `command_codec.go:2134` —— `make([]uint64, 0, int(count))`，`readUint64Slice`（`:2126-2143`）先做
  `if count > uint64(len(r.rest)/8) { return nil, errCommandEnvelopeCorrupt }`。→ 误报。
- `command_codec.go:2153`、`:2154` —— `readString`（`:2145-2156`）先做
  `if length > uint64(len(r.rest)) { return "", errCommandEnvelopeCorrupt }`，
  再 `string(r.rest[:int(length)])` / `r.rest = r.rest[int(length):]`。
  `length` 已被 `len(r.rest)`（本身是 `int`）上界约束，转换不可能溢出。→ 误报。
- `command_codec.go:2190` —— `readInt64` 的 `int64(value)`，是刻意的有符号重解释，
  且配套的 `commandIntFromInt64`（`:2253-2259`）做了 `int64(int(value)) != value` 的往返校验。→ 误报。
- `command_codec.go:876`、`:2120` —— `appendCommandInt64` 的 `uint64(value)` 与 `readItemCount` 里的
  `uint64(len(r.rest)/minItemSize)`，都是编码侧/非负值转换。→ 误报。
- `snapshot_chunks.go:118`、`:175` —— `uint64(existingLen)` / `uint64(len(chunk.message))`，
  源头是 `int` 且非负。→ 误报。

  **结论：本分片 `command_codec.go` 的每一个 decode 函数都已针对"`uint64` 线路长度 → `int` → `make()`/切片"
  这一模式逐一核对，全部满足"先用剩余缓冲区长度做上界、再转换"的正确范式。该文件不存在 sibling unit
  报告的那类 P0 长度前缀缺陷。**

**staticcheck U1000 命中中被排除的**

- `service.go:625 recordRaftStatus` —— 确实未使用，但**控制面并没有因此丢掉 Raft 状态观测**：
  真正的记录路径是 `updateLeader`（`service.go:1246-1255`）→ `recordRaftStatusSnapshot(status)`，
  它在每次 `Tick`、每次 `Step`、每次 `Advance` 之后都被调用。`recordRaftStatus(rawNode)` 只是
  `recordRaftStatusSnapshot(rawNode.Status())` 的一层薄封装。→ 死代码但无功能缺失，不单列为发现。
- `service.go:1193 setCompactControllerLogHookForTest`、`service.go:854 setLifecycleWaitHookForTest`、
  `service.go:1205 setRawNodeBootstrapForTest` —— 测试钩子 setter，注释已明确"must remain nil in production"。
  → 不计为缺陷。

**其它自行驳回的候选项**

- `service.go:906-915`（`ready.Entries` 与 `pendingQueue` 的 FIFO 配对）—— 起初怀疑
  follower 从 leader 复制来的 `EntryNormal` 会与本地 pending 提案错配。复核后驳回：
  `Propose` 分支前置了 `rawNode.Status().RaftState != raft.StateLeader → ErrNotLeader`，
  而 `failInflightProposalsOnLeaderLoss`（`service.go:1903-1912`）在**每次** `Tick`、每次 `Step`、
  每次 `Advance` 之后都会被调用并清空 `pendingQueue` + `pendingByIndex`。
  因此进入 `processReady` 时若本节点已非 leader，队列必为空 → 不可能错配。
- `pendingByIndex` 的条目泄漏 —— 提案被 commit 前若一直没有 quorum，tracker 会留在 map 里。
  但 `CheckQuorum: true`（`service.go:832`）保证无 quorum 的 leader 在一个 election timeout（1s）内下台，
  下台即触发 `failInflightProposalsOnLeaderLoss` 清空。→ 泄漏窗口 ≤1s 的提案量，有界，不成立。
- `Propose` 在 ctx 取消后仍把 tracker 留在队列里 —— `req.resp` 是 `make(chan error, 1)`，
  后续 `tracked.resp <- err` 不会阻塞，值被丢弃。→ 无 goroutine 泄漏。
- `Start`/`Stop`/`setError` 的 `close(stopCh)` 双关闭 —— 逐路径复核：`Stop` 的第二次调用会在
  `s.stopping` 分支上等 `stopDone`；`Start` 的 `stopRequested` 分支关闭的是它自己的局部 `stopCh`；
  `setError` 从不 `close` 任何 channel（只置 nil）。→ 不存在双关闭。
- `transport.Send` 里 `_ = t.client.Send(...)` 吞掉发送错误 —— 这是**刻意**的（注释明写
  "transient send failures are not fatal to the local raft loop and are retried by later raft traffic"），
  且与 raft 的重传语义一致。→ 对普通大小的消息不算缺陷；只有"超限不可分片"那一类才是问题（见发现 7）。
- `command_codec.go` 的 `readInt()` 接受任意 `int64`（含负数）作为 `CurrentMoveIndex` ——
  本包比 `pkg/controller/meta/onboarding_codec.go:176` 的校验更宽松（后者明确检查
  `CurrentMoveIndex < -1 || (!= -1 && >= len(Moves))`）。但消费侧
  `pkg/controller/plane/onboarding_executor.go:218` 做了
  `if job.CurrentMoveIndex >= 0 && job.CurrentMoveIndex < len(job.Moves)` 的边界检查，
  不存在越界索引。→ 宽严不一致属实，但写不出 panic 触发路径，按 brief 要求删除。
- `updateLeader` 每 100ms tick + 每条入站消息都调 `rawNode.Status()` 并在 `peerProgressFromRaft`
  里分配 slice + `slices.SortFunc` —— 3 voter、heartbeat 100ms 下约每秒几十次，
  量级可忽略。→ 不构成性能瓶颈。
- 运行时 conf change 导致"已移除 peer 的响应消息打死 leader" —— `grep -rn "ProposeConfChange"`
  在 `pkg/controller/` 下**零命中**（只有 `service_test.go:575` 手工构造 ConfChange entry），
  说明 controller voter 集合在 bootstrap 后不再变更。因此发现 1 的**内部**触发路径不成立，
  但**外部/配置错误/未认证攻击者**的触发路径成立，发现 1 保留。

## 本分片整体评价

这是一份工程素质明显高于仓库平均水平的代码：wire codec 的每一个长度前缀都有正确的剩余缓冲区上界检查
（是全仓应当被当作范式的那一份），`Start`/`Stop`/`setError` 的生命周期状态机对并发调用、启动中停止、
双关闭都做了细致处理，compaction 与 snapshot restore 的边界（先导入快照再 replay、
`Ready.Snapshot` 先 restore 再标记 applied）都和 `FLOW.md` 描述一致。

但它有一个**结构性的致命设计选择**：run loop 把所有错误一视同仁地当作"致命且终态"——
`s.setError(err); return`，而全文件只有一处 `go s.run(`、上层只在进程启动时调一次 `Start`，
没有任何 watchdog、supervisor 或健康降级。这把三类本应可恢复的错误（committed 命令的业务错误、
命令 decode 失败、以及**远端可控的 `rawNode.Step` 协议级拒绝**）升级成了整个控制面的永久死亡，
而且 `Status` 结构体里连一个承载死因的字段都没有，死亡与"正常停止"在管理接口上完全无法区分。

**最该优先处理的一个问题是发现 1**：因为 `pkg/transport` 没有认证也没有 TLS，
任何能连上 controller 端口的进程用**一帧 `MsgHeartbeatResp`** 就能永久杀死目标节点的控制面 Raft 循环，
对三个 voter 各发一帧即摘除整个集群控制面，而进程仍然活着、数据面仍在转发 —— 存活探针不会报警。
修复成本极低（把 `ErrStepPeerNotFound`/`ErrProposalDropped` 与 `ErrStepLocalMsg` 一起降级为 warn+continue），
收益极高。紧随其后的是发现 4（一个 ~80 字节的未认证分片帧即可让 `make([]byte, 1<<60)` 崩掉整个进程）
和发现 2（一条 committed 命令即可确定性地、重启不可自愈地摧毁整个控制面集群）。




