# 新架构集群全链路（v2）

**注意：本分片全部代码（`pkg/clusterv2/`、`internalv2/`）当前不在生产运行路径上。** `cmd/wukongim/main.go` 只构造 `internal/app`，全仓无任何 `internalv2/app` 的 importer。以下所有发现的严重度已按"不在生产运行路径"下调一档（原始严重度在括号内注明）。

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/clusterv2/api.go | 57 | 是 |
| pkg/clusterv2/errors.go | 20 | 是 |
| pkg/clusterv2/config.go | 188 | 是 |
| pkg/clusterv2/node.go | 203 | 是 |
| pkg/clusterv2/node_lifecycle.go | 151 | 是 |
| pkg/clusterv2/node_loops.go | 56 | 是 |
| pkg/clusterv2/node_snapshot.go | 149 | 是 |
| pkg/clusterv2/node_transport.go | 41 | 是 |
| pkg/clusterv2/node_defaults.go | 159 | 是 |
| pkg/clusterv2/default_slots.go | 87 | 是 |
| pkg/clusterv2/default_slot_leaders.go | 73 | 是 |
| pkg/clusterv2/default_slot_proposer.go | 63 | 是 |
| pkg/clusterv2/snapshot_changes.go | 25 | 是 |
| pkg/clusterv2/internal/lifecycle/group.go | 58 | 是 |
| pkg/clusterv2/internal/clock/clock.go | 15 | 是 |
| pkg/clusterv2/control/controller.go | 62 | 是 |
| pkg/clusterv2/control/runtime.go | 265 | 是 |
| pkg/clusterv2/control/controllerv2.go | 172 | 是 |
| pkg/clusterv2/control/static.go | 134 | 是 |
| pkg/clusterv2/control/snapshot.go | 107 | 是 |
| pkg/clusterv2/control/snapshot_validate.go | 100 | 是 |
| pkg/clusterv2/control/snapshot_clone.go | 20 | 是 |
| pkg/clusterv2/control/transport.go | 152 | 是 |
| pkg/clusterv2/control/codec.go | 86 | 是 |
| pkg/clusterv2/channels/service.go | 165 | 是 |
| pkg/clusterv2/channels/meta.go | 78 | 是 |
| pkg/clusterv2/channels/slot_meta.go | 147 | 是 |
| pkg/clusterv2/channels/placement.go | 38 | 是 |
| pkg/clusterv2/channels/projection.go | 51 | 是 |
| pkg/clusterv2/channels/static_meta.go | 45 | 是 |
| pkg/clusterv2/channels/lifecycle.go | 22 | 是 |
| pkg/clusterv2/channels/transport.go | 179 | 是 |
| pkg/clusterv2/channels/codec.go | 103 | 是 |
| pkg/clusterv2/net/transport.go | 101 | 是 |
| pkg/clusterv2/net/client.go | 15 | 是 |
| pkg/clusterv2/net/server.go | 17 | 是 |
| pkg/clusterv2/net/codec.go | 22 | 是 |
| pkg/clusterv2/net/ids.go | 33 | 是 |
| pkg/clusterv2/net/discovery.go | 77 | 是 |
| pkg/clusterv2/net/local.go | 62 | 是 |
| pkg/clusterv2/net/errors.go | 12 | 是 |
| pkg/clusterv2/propose/service.go | 78 | 是 |
| pkg/clusterv2/propose/forward.go | 52 | 是 |
| pkg/clusterv2/propose/codec.go | 54 | 是 |
| pkg/clusterv2/propose/types.go | 73 | 是 |
| pkg/clusterv2/routing/router.go | 100 | 是 |
| pkg/clusterv2/routing/table.go | 128 | 是 |
| pkg/clusterv2/slots/types.go | 65 | 是 |
| pkg/clusterv2/slots/manager.go | 106 | 是 |
| pkg/clusterv2/slots/reconciler.go | 55 | 是 |
| pkg/clusterv2/slots/runtime.go | 47 | 是 |
| pkg/clusterv2/slots/observer.go | 29 | 是 |
| pkg/clusterv2/observe/loops.go | 59 | 是 |
| pkg/clusterv2/observe/reporter.go | 67 | 是 |
| pkg/clusterv2/observe/snapshot.go | 18 | 是 |
| internalv2/app/app.go | 144 | 是 |
| internalv2/app/config.go | 47 | 是 |
| internalv2/app/lifecycle.go | 75 | 是 |
| internalv2/access/gateway/handler.go | 108 | 是 |
| internalv2/access/gateway/batch.go | 76 | 是 |
| internalv2/access/gateway/mapper.go | 69 | 是 |
| internalv2/access/gateway/error_map.go | 43 | 是 |
| internalv2/usecase/message/app.go | 41 | 是 |
| internalv2/usecase/message/send.go | 226 | 是 |
| internalv2/usecase/message/types.go | 141 | 是 |
| internalv2/usecase/message/ports.go | 52 | 是 |
| internalv2/usecase/message/errors.go | 24 | 是 |
| internalv2/infra/cluster/appender.go | 87 | 是 |
| internalv2/infra/cluster/error_map.go | 33 | 是 |
| internalv2/contracts/messageevents/event.go | 19 | 是 |

7 个 FLOW.md（pkg/clusterv2、internalv2、app、gateway、usecase、infra、contracts）全部通读。

动态验证：`GOTOOLCHAIN=go1.23.4 go test -race ./pkg/clusterv2/... ./internalv2/...` —— **14 个测试在 -race 下触发数据竞争**（详见发现 #1、#2），全部竞争报告指向本分片代码。

---

## 发现

### [P1] 1. Node 控制快照 watch goroutine 无 WaitGroup join：Stop 与 applySnapshot 数据竞争 + use-after-close（原始：P0）

- **位置**：`pkg/clusterv2/node_loops.go:8-25`、`pkg/clusterv2/node_lifecycle.go:88-98`
- **类别**：并发 / 资源泄漏
- **代码**（node_loops.go:8-25 —— 只有 cancel，没有 WG）：
  ```go
  func (n *Node) startWatchLoop() {
      ctx, cancel := context.WithCancel(context.Background())
      n.watchCancel = cancel
      watch := n.control.Watch()
      go func() {
          for {
              select {
              case <-ctx.Done():
                  return
              case ev, ok := <-watch:
                  if !ok {
                      return
                  }
                  _ = n.applySnapshot(ctx, ev.Snapshot)
              }
          }
      }()
  }
  ```
  对照同文件 `startChannelTickLoop`（27-47 行）与 `default_slot_leaders.go:12-42`，两者都正确使用 `n.channelTickWG` / `n.slotLeaderWG` 做 `Add(1)`/`Done()`/`Wait()` join。而 `Stop()`（node_lifecycle.go:88-98）：
  ```go
  n.stopping.Store(true)
  if n.watchCancel != nil {
      n.watchCancel()
  }
  n.stopSlotLeaderLoop()
  n.stopChannelTickLoop()
  var errs []error
  if n.channels != nil {
      if err := n.channels.Close(); err != nil {
  ```
  `watchCancel()` 之后**不等待** watch goroutine 退出，紧接着关闭 `n.channels`；相邻两行对另外两个 loop 却是正确 join 的。
- **触发路径**：① Controller 发布新快照 → watch channel 有事件 → watch goroutine 从 select 醒来开始执行 `n.applySnapshot(ctx, ev.Snapshot)`；② 此时另一 goroutine 调用 `Node.Stop()`：`watchCancel()` 使 ctx.Done 就绪但 goroutine 已进入 applySnapshot；③ Stop 继续执行 `n.channels.Close()` 并把 `n.channels` 置 nil（node_lifecycle.go:100）；④ applySnapshot 内部第 72 行并发读 `n.channels != nil`（该字段无锁保护，且在 `n.mu.Lock()` 临界区内读取）——与 Stop 的裸写构成数据竞争；若 applySnapshot 先读、Stop 后写，则后续 `n.slots.Reconcile`（defaultSlotRuntime）可能与 `defaultSlotRuntime.Close()` 并发执行 → use-after-close。
- **后果**：`go test -race` 实测证实——`-race` 下 14 个 clusterv2 测试失败（`TestNodeStopDiscardsDefaultChannelsForRestart`、`TestNodeInitializesDefaultControllerV2WhenOptionMissing`、`TestNodeStartMarksDefaultChannelsReadyWithoutController` 等），竞争报告精确指向 `node_lifecycle.go:100`（Stop 写 `n.channels`）vs `node_snapshot.go:72`（watch goroutine 读 `n.channels`），以及 `node_snapshot.go:65-66`（读/调 `n.slots`）vs `node_lifecycle.go:135`（Stop 写 `n.slots=nil`）。生产上等价后果是关停时窗口期 use-after-close / 竞态 panic。
- **建议**：给 watch loop 加 `watchWG`，`Stop()` 中 cancel 后 `Wait()` 再关闭 channels/slots（与同文件另外两个 loop 的既有模式对齐）。

### [P1] 2. RaftTransport.Send 每批起一个无上限 goroutine 且吞错（原始：P0/P1 边界）

- **位置**：`pkg/clusterv2/control/transport.go:31-58`
- **类别**：并发 / 资源泄漏 / 可观测性
- **代码**：
  ```go
  func (t *RaftTransport) Send(messages []raftpb.Message) {
      ...
      for nodeID, batch := range byNode {
          payload, err := EncodeRaftBatch(batch)
          if err != nil {
              continue
          }
          go t.sendBatch(nodeID, payload)
      }
  }

  func (t *RaftTransport) sendBatch(nodeID uint64, payload []byte) {
      ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
      err := t.sender.Send(ctx, nodeID, clusternet.RPCControlRaft, payload)
      if err != nil {
          fmt.Printf("control raft send failed %v\n", err)
      }
      cancel()
  }
  ```
- **触发路径**：ControllerV2 Raft 状态机每 tick 产生待发消息并调用 `Send`。当某 voter 节点宕机/网络分区时，`sender.Send` 对该节点的每次投递要等连接池 dial 超时或 ctx 200ms 超时；Raft tick 持续产生新消息 → 每个新 batch 又 `go sendBatch`。多 voter 集群 + 高 tick 频率下 goroutine 数量按"发送速率 × 投递延迟"无界堆积（发送方无队列、无背压、无并发上限）。编码失败分支 `continue` 静默丢弃整批 Raft 消息，会直接造成该 voter 的 Raft 心跳/投票消息丢失。
- **后果**：网络分区时 goroutine 无界增长（内存泄漏）；Raft 消息丢失被完全吞掉且仅用 `fmt.Printf` 输出（绕过任何日志框架），故障不可诊断。
- **建议**：per-peer 串行发送队列 + 有界缓冲；错误走结构化日志。

### [P1] 3. 三节点复制冒烟测试在 -race 下失败：pkg/transport server 并发读写（原始：P0）

- **位置**：竞争点在 `pkg/transport/server.go:149-151`（readLoop goroutine 读 vs serveConn 写），由本分片 `pkg/clusterv2` 三节点测试 `TestClusterV2ThreeNodeDefaultChannelsReplicateQuorumAppend` 触发
- **类别**：并发
- **代码**（transport/server.go:145-152）：
  ```go
  s.wg.Add(1)
  go func() {
      defer s.wg.Done()
      if err := s.handleConn(conn); err != nil {   // :149 读
          ...
      }
  }()                                               // :151 写
  ```
  竞争报告：`serveConn.func1()` 读 `server.go:149` vs `serveConn()` 写 `server.go:151`，经 `MuxConn.readLoop`（conn.go:111）。
- **触发路径**：clusterv2 默认 transport 走 `clusternet.TransportServer`（即 pkg/transport.Server）；三节点 ChannelV2 复制测试建立多条并发 TCP 连接时，`-race` 检测到 serveConn 栈帧上的变量竞争（`-race` 报告的读写点在同一函数栈帧，通常是 err/conn 变量逃逸后并发访问）。
- **后果**：底层 transport 存在真实数据竞争，clusterv2 是它的消费方；v2 一旦上线即继承该竞争（该文件本身属其它分片，此处记录竞争证据与本分片的暴露路径）。
- **建议**：transport server 侧修复 conn/err 生命周期；本分片无义务修，但 v2 上线前必须先修。

### [P1] 4. 消息 ID 分配器跨重启重置：`nodeID << 48` 起点必然复用历史 MessageID（原始：P0）

- **位置**：`internalv2/app/app.go:132-144`
- **类别**：分布式一致性 / 正确性
- **代码**：
  ```go
  type nodeMessageIDs struct {
      next atomic.Uint64
  }

  func newNodeMessageIDs(nodeID uint64) *nodeMessageIDs {
      g := &nodeMessageIDs{}
      g.next.Store(nodeID << 48)
      return g
  }

  func (g *nodeMessageIDs) Next() uint64 {
      return g.next.Add(1)
  }
  ```
- **触发路径**：节点以 NodeID=1 启动，发 100 条消息（ID = 2^48+1 .. 2^48+100），持久化进 channelv2 消息库；进程重启（v2 无任何 ID 高水位持久化，`internalv2` 无磁盘状态）→ `New` 重新 `Store(1<<48)` → 第 1 条新消息 ID = 2^48+1，与历史消息完全相同。MessageID 是 MessageCommitted 事件与 sendack 的全局标识。
- **后果**：MessageID 冲突：客户端去重、下游 conversation/delivery 迁移（FLOW.md 明说这是 contracts/messageevents 的设计目标）都会把新旧消息当成同一条。对照 v1（`internal/runtime/messageid/snowflake.go`）用 bwmarrin/snowflake（时间戳分量保证单调递增不回退），v2 把 v1 已解决的问题重新引入了。
- **建议**：换回 snowflake 或至少持久化高水位。

### [P2] 5. 控制快照去重用 `reflect.DeepEqual` 深比较全量快照（原始：P1）

- **位置**：`pkg/clusterv2/snapshot_changes.go:16-25`
- **类别**：性能
- **代码**：
  ```go
  func snapshotChanges(previous, next control.Snapshot) controlSnapshotChanges {
      previous = previous.Clone()
      next = next.Clone()
      return controlSnapshotChanges{
          nodes:     !reflect.DeepEqual(previous.Nodes, next.Nodes),
          slots:     !reflect.DeepEqual(previous.Slots, next.Slots),
          tasks:     !reflect.DeepEqual(previous.Tasks, next.Tasks),
          hashSlots: !reflect.DeepEqual(previous.HashSlots, next.HashSlots),
      }
  }
  ```
- **触发路径**：每次 Controller 快照事件（每条 Raft 提交都可能发布一次）都执行：2 次全量深拷贝 + 4 次全量 `reflect.DeepEqual`。`Clone()` 本身逐节点/逐槽/逐任务分配切片（snapshot_clone.go），快照含数千 slot × peers 时每次事件 O(集群规模) 分配与比较，全部在 applySnapshot 的 foreground 路径上（watch goroutine 与 Start 都会走）。
- **后果**：大集群下每次控制面更新产生大量 GC 压力； revision 递增但内容未变时仍做全量比较。Revision 单调递增，直接比较 `Revision` + 内容哈希即可。
- **建议**：以 Revision 为主判据，DeepEqual 仅作降级手段。

### [P2] 6. `initialControlSnapshot` 在 `Watch()` channel 上无限阻塞等待，且 Start 失败路径泄漏该等待（原始：P1）

- **位置**：`pkg/clusterv2/node_snapshot.go:23-38`
- **类别**：并发 / context
- **代码**：
  ```go
  watch := n.control.Watch()
  for {
      select {
      case <-ctx.Done():
          return control.Snapshot{}, ctx.Err()
      case event, ok := <-watch:
          if !ok {
              return control.Snapshot{}, ErrNotStarted
          }
          if err := event.Snapshot.Validate(); err == nil {
              return event.Snapshot.Clone(), nil
          } else if !emptyControlSnapshot(event.Snapshot) {
              return control.Snapshot{}, err
          }
      }
  }
  ```
- **触发路径**：LocalSnapshot 返回空快照（首次启动、Raft 未选出 leader）时进入等待循环。`Start` 用的 ctx 是调用方 ctx（`n.control.Start(ctx)` 之后 `initialControlSnapshot(ctx)`）——注意 `Runtime.Start` 内部 `backend.Start` 已完成，后续 watch 事件只在该节点被选中/同步到状态后才到达。若调用方传入的 ctx 无 deadline（生产常见 `context.Background()` 派生），且 ControllerV2 一直选不出 leader（例如 allowBootstrap=false 且所有 voter 都在等对方），`Start` 永久阻塞且无超时兜底——`cfg.Timeouts.Start`（config.go:89，默认 10s）声明了"Start readiness gates 的最大时长"却**从未被使用**。
- **后果**：`Timeouts.Start` 配置项形同虚设；Start 可能永久挂起且无诊断输出。FLOW.md 声称 "wait for a valid initial control snapshot"，但未提及时限语义与配置的矛盾。
- **建议**：用 `context.WithTimeout(ctx, cfg.Timeouts.Start)` 包住 readiness gate。

### [P2] 7. Config 校验放行 Slot 数为 0 的集群且 `uint16(len(voters))` 溢出（gosec G115 核实为真）（原始：P1）

- **位置**：`pkg/clusterv2/config.go:126-131` + `pkg/clusterv2/config.go:184`
- **类别**：正确性
- **代码**：
  ```go
  if c.Slots.ReplicaCount == 0 {
      c.Slots.ReplicaCount = uint16(len(c.Control.Voters))
      if c.Slots.ReplicaCount == 0 {
          c.Slots.ReplicaCount = 1
      }
  }
  ...
  if int(c.Slots.ReplicaCount) > len(c.Control.Voters) {
      return ErrInvalidConfig
  }
  ```
- **触发路径**：用户显式配置 `ReplicaCount > 65535` 个 voter（`len(c.Control.Voters)` 超过 65535）时 `uint16(...)` 截断回绕，截断后的 ReplicaCount 通过 `int(ReplicaCount) > len(voters)` 校验，实际副本数与配置不符 → bootstrap 时 voter 集合与副本数不一致。此外 `validateSlots` 允许 `InitialSlotCount`、`HashSlotCount` 任意值，而 `controllerv2/state/hashslots.go:9` 要求 `slotCount <= hashSlotCount`，clusterv2 自身不预检该不变量，错误延迟到 Controller bootstrap 才暴露。
- **后果**：voter > 65535 的畸形配置下副本数静默错乱；其余为延迟报错（健壮性问题）。
- **建议**：校验阶段加 `len(voters) <= math.MaxUint16` 与 `InitialSlotCount <= HashSlotCount`。

### [P2] 8. `splitSegments` 只合并相邻同频道的 item，跨频道穿插时同频道被拆成多个小批（原始：P2）

- **位置**：`internalv2/usecase/message/send.go:93-104`
- **类别**：性能
- **代码**：
  ```go
  func splitSegments(items []preparedSend) []segment {
      out := make([]segment, 0, len(items))
      for _, item := range items {
          channel := ChannelID{ID: item.cmd.ChannelID, Type: item.cmd.ChannelType}
          if len(out) > 0 && out[len(out)-1].channel == channel {
              out[len(out)-1].items = append(out[len(out)-1].items, item)
              continue
          }
          out = append(out, segment{channel: channel, items: []preparedSend{item}})
      }
      return out
  }
  ```
- **触发路径**：批量 send 交替发往两个频道 `[chA, chB, chA, chB]` → 产生 4 个单条 segment，4 次独立 `AppendBatch`，每次一次 raft propose。注释/命名（"split adjacent sends by canonical channel"，FLOW.md 同样写 adjacent）表明这是有意为之以保持 item 顺序对齐，但同频道 item 间不会重排却完全可以聚合（结果按 index 回填）。
- **后果**：微批场景（gateway micro-batching，FLOW.md 的核心卖点）下聚合度取决于客户端发送顺序，最坏退化为逐条 propose。
- **建议**：按频道分组聚合（结果本来就按 index 回填，不破坏对齐）。

### [P2] 9. `OnSendBatch` 中途返回错误导致已发送消息无 sendack 且丢失全部结果（原始：P1）

- **位置**：`internalv2/access/gateway/batch.go:33-75`
- **类别**：错误处理
- **代码**：
  ```go
  cmd, err := mapSendCommand(ctx, item.Frame)
  if err != nil {
      if errors.Is(err, ErrUnauthenticatedSession) {
          results[i].Reason = message.ReasonAuthFail
          continue
      }
      return err                          // (A) 提前返回：validItems 已就绪但未处理
  }
  ...
  batchResults := batcher.SendBatch(validItems)
  if len(batchResults) != len(validItems) {
      return ErrSendBatchResultCountMismatch   // (B) 已投递的 append 无 sendack
  }
  ...
  for i, item := range items {
      if err := writeSendack(&contexts[i], item.Frame, results[i]); err != nil {
          return err                        // (C) 半途失败，剩余 item 无 sendack
      }
  }
  ```
- **触发路径**：(A) 批中第 1 个 item 映射失败（非 auth 类错误，如 frame 解析异常）→ 已通过 `EnsureChannelMeta` 预热/或后续 item 从未获得结果，整批无 sendack；(B) usecase 返回数量不齐 → 已 append 成功的消息已在 channelv2 持久化，但客户端永远收不到 sendack → 客户端按超时重发 → **消息重复**；(C) 第 i 个 item 的 WriteFrame 失败（连接抖动）→ 后续 item 即使 append 成功也无 sendack，同样重发重复。
- **后果**：消息重复投递（重发路径无幂等键校验——`cmd.ClientMsgNo` 被传递但 usecase 不做去重，`prepare()` 中无 ClientMsgNo 查重逻辑）。SEND 契约上 sendack 缺失即触发客户端重试，这是正确性问题。
- **建议**：C 场景至少记录已成功 item 的日志用于幂等判定；usecase 层按 ClientMsgNo 去重。

### [P2] 10. `SendBatch` 对单个 item 的 ctx 失败静默降级为 `ReasonSystemError` 之外的零值（原始：P2）

- **位置**：`internalv2/usecase/message/send.go:106-117`
- **类别**：错误处理
- **代码**：
  ```go
  func (a *App) appendSegment(segment segment, results []SendBatchItemResult) {
      active := make([]preparedSend, 0, len(segment.items))
      for _, item := range segment.items {
          if err := item.ctx.Err(); err != nil {
              results[item.index] = SendBatchItemResult{Err: err}
              continue
          }
          active = append(active, item)
      }
  ```
- **触发路径**：`Send()`（send.go:12-18）把 `SendBatchItemResult.Err` 原样返回给 `Handler.sendOne`（handler.go:100-103），后者 `reasonForError` 映射 `context.Canceled/DeadlineExceeded → ReasonSystemError`——合理。但 gateway 的批路径 `OnSendBatch`（batch.go:56-62）对 `batchResults[j].Err != nil` 只做 `reasonForError` 后写入 result，Err 本身被丢弃——与单发路径行为一致，无实际问题。**复核后降级为观察项**：真正的问题是 `prepare` 中 `ctx.Err()` 检查（send.go:62-64）发生在 `authorizer.AuthorizeSend` 之前，但 authorize 之后的 append 前还有一次 ctx 检查（send.go:109），两处检查之间如果 ctx 被取消，append 仍会带着已取消的衍生 ctx 进入 `AppendBatch`，由下游 mapAppendError 返回 Canceled——行为正确但多一次无效 raft 提交尝试的窗口。
- **后果**：轻微；无正确性破坏（下游会以 Canceled 失败）。
- **建议**：appendSegment 内追加消息前做最后一次 ctx 检查即可（已有，收紧为逐 item 检查）。

### [P2] 11. `channels.Service.Append` 在本地非 leader 且无 forward client 时静默返回 ErrNotLeader，与 meta 解析顺序造成写放大（原始：P2）

- **位置**：`pkg/clusterv2/channels/service.go:88-105`
- **类别**：正确性 / 性能
- **代码**：
  ```go
  func (s *Service) Append(ctx context.Context, req ch.AppendRequest) (ch.AppendResult, error) {
      meta, ok, err := s.resolveAppendMeta(ctx, req.ChannelID)
      if err != nil {
          return ch.AppendResult{}, err
      }
      if ok {
          if meta.Leader != 0 && meta.Leader != s.localNode {
              if s.forward == nil {
                  return ch.AppendResult{}, ch.ErrNotLeader
              }
              return s.forward.ForwardAppend(ctx, meta.Leader, req)
          }
          if err := s.runtime.ApplyMeta(meta); err != nil {
              return ch.AppendResult{}, err
          }
      }
      return s.runtime.Append(ctx, req)
  }
  ```
- **触发路径**：非 leader 节点每次 Append 都先执行 `resolveAppendMeta`（走 `SlotMetaSource.EnsureChannelMeta` → 读 Slot 元数据存储，可能还走 raft propose 写初始元数据），然后才发现自己不是 leader 需转发。元数据解析（含潜在的元数据 raft 写）发生在转发判定**之前**，非 leader 上的这次 Ensure 是无用功且可能与 leader 上的 Ensure 竞争写同一条 ChannelRuntimeMeta（`UpsertChannelRuntimeMeta` 非幂等提案，两次并发首写会产生两条竞争的初始元数据提案——leader epoch 都为 1 但 replica 集可能不同，后者覆盖前者）。
- **后果**：首写并发时初始 channel placement 不确定（哪个节点先 propose 谁的 replica 集生效）；每次转发都多一次元数据 I/O。
- **建议**：EnsureChannelMeta 的创建分支仅在本地为 Slot/channel leader 时执行，非 leader 直接转发。

### [P2] 12. `ForwardHandler.HandleRPC` 只做 `IsLocalLeader` 检查、无 hashSlot 归属校验，转发 payload 内 hashSlot 与 SlotID 可不一致（原始：P2）

- **位置**：`pkg/clusterv2/propose/forward.go:38-50`
- **类别**：分布式一致性
- **代码**：
  ```go
  func (h *ForwardHandler) HandleRPC(ctx context.Context, payload []byte) ([]byte, error) {
      req, err := DecodeForwardRequest(payload)
      if err != nil {
          return nil, err
      }
      if h == nil || h.slots == nil || !h.slots.IsLocalLeader(req.SlotID) {
          return nil, ErrNotLeader
      }
      if err := h.slots.Propose(ctx, req.SlotID, req.Payload); err != nil {
          return nil, err
      }
      return nil, nil
  }
  ```
- **触发路径**：编码侧 `EncodeForwardRequest`（codec.go:31-42）把 `SlotID` 与 `HashSlot` 独立编码，二者一致性完全由发送方保证。接收侧仅校验 `IsLocalLeader(req.SlotID)`，payload 解码出的 hashSlot（`defaultSlotProposer.Propose` → `propose.DecodePayload`）不与本地 slot 的 owned hash slots 校验（`pkg/slot/fsm` 的 `ownedHashSlots` 只在部分命令 apply 时用）。被攻陷/有 bug 的节点可把任意 hashSlot 的元数据命令提案到错误 slot 的 raft 组，FSM 写入后路由表与元数据存储分叉。
- **后果**：跨节点信任边界内的一致性漏洞（需要节点本身有 bug 或被篡改，故 P2）；正常路径由 `Service.route()` 保证一致。
- **建议**：HandleRPC 解码 hashSlot 后校验其归属 SlotID 再提案。

### [P2] 13. Manager.Ensure 对 `unassigned` map 只增不删（原始：P2）

- **位置**：`pkg/clusterv2/slots/manager.go:53-56, 83-89`
- **类别**：无界增长
- **代码**：
  ```go
  if !containsNode(assignment.DesiredPeers, m.localNode) {
      m.unassigned[assignment.SlotID] = struct{}{}
      return nil
  }
  ...
  func (m *Manager) IsUnassigned(slotID uint32) bool {
      _, ok := m.unassigned[slotID]
      return ok
  }
  ```
- **触发路径**：slot 迁移（v1 非目标但 DesiredPeers 变化即可触发）：slot 曾不在本节点 → 记入 unassigned；后续快照把它迁到本节点 → `Ensure` 走 Open/Bootstrap 分支但**从不 `delete(m.unassigned, ...)`**。`IsUnassigned` 的调用方（全仓 grep 仅 `Manager` 自身与测试）得到过期答案。
- **后果**：map 无界增长有限（slot 数量级），主要是语义缺陷：迁入后 `IsUnassigned` 仍返回 true。
- **建议**：Ensure 成功打开/bootstrap 后 delete。

### [P3] 14. observe 包整体为死代码（v2 内部亦无调用方）

- **位置**：`pkg/clusterv2/observe/loops.go`（59 行）、`reporter.go`（67 行）、`snapshot.go`（18 行）
- **类别**：架构 / 死代码
- **代码**：全仓 grep（含测试）`clusterv2/observe` **零引用**——没有任何非测试文件 import 该包，包内 `Loop`/`Reporter`/`RuntimeSnapshot` 三个导出类型均无外部使用者。
- **触发路径**：不适用（不可达）。报告者链路 `Reporter → control.Controller.ReportNode/ReportSlots`，而 `control.Runtime.ReportNode/ReportSlots`（runtime.go:196-203）本身是"best-effort no-op until ControllerV2 exposes report commands"——即观察链路的接收端也是 stub。
- **后果**：144 行维护负担；语义上它实现了正确的 WaitGroup join 模式（loops.go:26-58），却从未被接上。协调者线索核实：**确认死代码**。
- **建议**：接入 Node lifecycle 或删除；接入前不要继续在其上扩展。

### [P3] 15. `internal/clock` 包死代码 + FLOW.md 未提及

- **位置**：`pkg/clusterv2/internal/clock/clock.go`（15 行）
- **类别**：死代码
- **代码**：全仓 grep `clusterv2/internal/clock` 零引用。
- **触发路径**：不适用。
- **后果**：无。
- **建议**：删除或接入 observe/propose 的定时路径。

### [P3] 16. `RPCControlReportNode`/`RPCControlReportSlots`/`MsgSlotRaft`/`MsgSlotRaftBatch` 服务 ID 已定义未使用

- **位置**：`pkg/clusterv2/net/ids.go:24-27, 5-8`
- **类别**：死代码
- **代码**：
  ```go
  // RPCControlReportNode serves node report requests.
  RPCControlReportNode
  // RPCControlReportSlots serves Slot runtime report requests.
  RPCControlReportSlots
  ```
- **触发路径**：grep 全仓（除定义处）零引用。与发现 #14 的 observe Reporter 呼应：报告 RPC 的编号已预留、发送端已写好（observe.Reporter）、接收端 stub——整条链路三段都各自"完成"但从未连通。
- **后果**：协议号空间被未实现功能占用。
- **建议**：连通或注释预留意图。

### [P3] 17. FLOW.md（pkg/clusterv2）声称 "foreground write paths only read atomic route/channel state"，与 applySnapshot 实际行为部分不符

- **位置**：`pkg/clusterv2/FLOW.md`（"Observe loops are intentionally small and low-frequency; foreground write paths only read atomic route/channel state."）vs `pkg/clusterv2/node_snapshot.go:65-68`
- **类别**：架构/文档
- **代码**（node_snapshot.go）：
  ```go
  if n.slots != nil && (firstSnapshot || changes.slots) {
      if err := n.slots.Reconcile(ctx, snapshot); err != nil {
          return err
      }
  }
  ```
- **触发路径**：FLOW.md 描述基本准确（路由表确实是 atomic.Pointer 替换），但两点未如实说明：① watch goroutine **不 join**（发现 #1），FLOW.md 的 Stop Flow 列出 "stop Controller watch loop" 却未提它有泄漏窗口；② `Timeouts.Start` 在 Start Flow 中被隐含为 readiness gate 上限，实际未实现（发现 #6）。
- **后果**：文档可信度；新增维护者按 FLOW.md 理解 Stop 语义会遗漏发现 #1。
- **建议**：修 #1、#6 后文档即恢复一致。

### [P3] 18. `fmt.Printf` 直接输出替代日志框架（3 处）

- **位置**：`pkg/clusterv2/control/transport.go:55`；`pkg/clusterv2/control/runtime.go` 无；`internalv2` 无——另两处为 `pkg/clusterv2/` 下 grep `fmt.Printf` 仅此一处真实热路径输出（其余发现 #2 已并入）。补充：`pkg/clusterv2/default_slot_leaders.go` 的 leader 刷新循环静默吞掉 `Status()` 错误（observer.go:20 `continue`），无任何日志。
- **类别**：可观测性
- **代码**：见发现 #2 摘录第 8 行。
- **触发路径**：控制面 Raft 消息发送失败。
- **后果**：绕过日志级别控制与采集管道。
- **建议**：接入项目统一日志器。

---

## 架构裁定（应协调者要求）

### 1. v2 目前实际覆盖的功能子集 vs v1

v2 是一个**极薄的写入骨架**，覆盖度大致如下：

| 能力域 | v1 | v2 现状 |
|---|---|---|
| 客户端 SEND → 持久 append → sendack | 完整 | **已覆盖**（且是唯一打通的端到端链路） |
| NoPersist 实时投递 | 完整 | 显式不支持（prepare 返回 ReasonUnsupported） |
| 投递（deliver）、会话/最近会话、CMD、webhook/插件 | 完整 | 零实现，仅有 messageevents DTO 占位 |
| 管理 API、鉴权体系 | 完整 | 零实现（authorizer 默认 allowAll） |
| Slot 多副本自动装配 | 完整 | 单节点限定（`defaultSingleNodeSlotsEnabled` 要求 ReplicaCount==1 且单 voter） |
| hash-slot 迁移/onboarding/drain | 有 | FLOW.md 声明为 Non-Goal |
| ControllerV2 集成 | — | 启动/快照/watch 可用；**节点/槽上报链路是三段各自完成但未连通的 stub**（#14/#16） |

行数量级（协调者数据）佐证：`internal` 83,986 vs `internalv2` 1,185（约 1.4%），`pkg/cluster` 18,388 vs `pkg/clusterv2` 4,540（约 25%）。

### 2. v1 缺陷类别在 v2 中的复现情况（本分片最有价值的结论）

**部分复现，部分改善，方向向好但把关不严：**

- **复现**：
  - **goroutine 生命周期纪律不均衡**——v1 的典型问题是 join 缺失，v2 在 `node_loops.go` 又出现同款（#1），而同文件、同包的另两处是正确的。这说明 v2 没有"必须 join"的强制约定/评审清单，靠作者自觉。
  - **消息 ID 分配**——v1 用 snowflake（全局单调、重启安全），v2 自己写了个 `nodeID<<48` 内存计数器，重启即冲突（#4）。这是 v2 主动放弃了 v1 的正确方案，属于**倒退**。
  - **错误静默吞掉**——`RaftTransport.Send` 吞错 + `fmt.Printf`（#2），`Status()` 错误静默 continue（#18），与 v1 风格一致。
- **改善**：
  - 路由表用 `atomic.Pointer[Table]` 不可变替换 + CAS（routing 包），无锁热路径，优于 v1 的锁表。
  - control.Runtime 的 watch loop、observe 的 Loop 都是正确的 cancel+WaitGroup join 范式。
  - 错误映射集中、typed error 贯穿（errors.go 链），比 v1 的字符串错误好。
  - Item 对齐的 batch 结果契约（SendBatchItemResult）设计干净。
- **把关缺口**：**v2 自身的 CI 从未跑过 `-race`**。`go test -race ./pkg/clusterv2/...` 当场暴露 14 个失败测试，其中最核心的 Start/Stop 生命周期测试全数命中（#1）。一个自诩"集群语义单节点也不 bypass"的重写分支，连自己生命周期测试的竞争检测都没过——这是该分支上线前必须补的门槛。

### 3. v2 内部已死的代码

- `pkg/clusterv2/observe/`（144 行，#14）——整包无任何 importer，且其依赖的 `ReportNode/ReportSlots` 接收端是 stub。
- `pkg/clusterv2/internal/clock/`（15 行，#15）——零引用。
- `net/ids.go` 中 4 个 RPC/Msg 编号未使用（#16）。
- `usecase/message.cloneAppendRequest`（send.go:211-217）——生产路径零调用，仅测试引用（staticcheck 未报因测试引用）。生产路径上 payload 克隆实际发生了**三次**（mapper.cloneBytes → prepare.cloneCommand → appendSegment 再 cloneBytes），再加 infra/cluster 的 `toChannelMessages` 一次——同一 payload 在一条 send 路径上被复制 4 次，属于过度防御性拷贝（性能项，并入 #5 建议）。

---

## 已排除的候选项

- `internalv2/access/gateway/batch.go:45`（gosec G118）— **误报**。`cancels` 收集后由 21-25 行的 `defer` 统一调用：
  ```go
  defer func() {
      for _, cancel := range cancels {
          cancel()
      }
  }()
  ```
  所有路径（含 39 行 early return、53 行 mismatch return、71 行 writeSendack 错误 return）都会触发 defer，无泄漏。注意语义：这批 per-item timeout ctx 在**整批 sendack 写完之前**都活着，是设计所需。
- `pkg/clusterv2/slots/observer.go:22`（gosec G115 uint64→uint32）— 误报。`status.SlotID` 本身源自 `multiraft.SlotID(slotID)`（slots/observer.go 的调用方 `routingSlotStatuses` 传入的就是 uint32），截断不可能发生。
- `pkg/clusterv2/routing/router.go:24`（G115 uint32→uint16）— 误报。`crc32 % uint32(count)` 结果 < count ≤ 65535，数学上无溢出。
- `pkg/clusterv2/channels/projection.go:18`（G115 int64→uint8）— 边界存疑但无法触发：`meta.ChannelType` 来自 metadb，其类型域为协议 channel type（uint8）；若存储被污染为 >255 值则截断，但存储写入侧同源于 uint8，无远程可达路径。
- `pkg/clusterv2/propose/codec.go:39`（G115 int→uint32）— 误报。`len(req.Payload)` 受同函数 32 行非空校验，且 uint32 长度前缀是线路格式约定，payload 上游是本地内存命令，无 4GB 可达输入。
- `pkg/clusterv2/node_snapshot.go:72`（G115 int→uint32）— 误报。`len(snapshot.Slots)` 来自控制面校验后的快照，量级为集群 slot 数。
- `pkg/clusterv2/config.go:127`（G115 int→uint16）— **真问题**，已列入发现 #7。
- `internalv2/access/gateway/mapper.go:54`（G115 uint64→int64）— 边界存疑：`result.MessageID` 是 uint64 消息 ID 写入 `int64` sendack 字段，>2^63 时变负。触发需要 MessageID 超过 int63——`nodeID<<48` 计数器下 NodeID>32768 时起点即超 int63？否：2^48×32768=2^63 恰好边界，NodeID 需 ≥32769 才超。NodeID 是配置值，理论上可达但需要刻意配置，实际影响是 sendack 字段符号翻转；因发现 #4 已使 ID 体系不可用，此项并入 #4 的修复一并解决。
- `pkg/clusterv2/observe/loops.go` — 曾怀疑泄漏，核实为**正确实现**（cancel+WG.Wait），问题只是死代码（#14）。
- `internalv2/app/lifecycle.go` Start 失败回滚 — 曾怀疑 gateway.Start 失败后 cluster.Stop 失败会留下 started=true 的脏状态，核实 32-36 行：rollback 失败时保持 `started=true, clusterStarted=true`，后续 Stop 会重试 cluster.Stop——与 FLOW.md "state remains retryable so a later Stop can clean up" 一致，设计如此。
- `pkg/clusterv2/control/runtime.go` Stop 与 publishState — 曾怀疑 watch goroutine 在 Stop 关 backend 后仍写 snapshot，核实 134-138 行：`watchCancel()` 后 `watchWG.Wait()` 再 `backend.Stop(ctx)`，顺序正确（与 #1 形成鲜明对照，证明 #1 是疏忽而非模式）。
- `pkg/clusterv2/routing/table.go` BuildTable 越界 — 曾怀疑 `HashToSlot[int(hashSlot)]` 在 range 越界时 panic，核实 snapshot_validate.go:66-90 强制 ranges 连续且恰好覆盖 [0, Count)，越界不可达。
- `pkg/clusterv2/propose/service.go:48` — 曾怀疑 `route.Leader == s.localNode || s.slots.IsLocalLeader(...)` 双条件存在脑裂窗口（陈旧路由 leader），核实：IsLocalLeader 是实时 Raft status 查询，第二个条件兜住了陈旧路由；若两者皆假走 forward，远端 ForwardHandler 再查一次——链路自洽。代价是两次 status 查询在热路径（性能可接受）。
- `internalv2/usecase/message/send.go:79-84` MessageID 由 usecase 分配 — 曾怀疑 gateway 与 usecase 双方都分配 ID 导致重复，核实 mapper.go:39 显式 `cmd.MessageID = 0`，唯一入口归 usecase；非 gateway 入口若传非零 ID 则被信任（契约在 types.go:65 注明），无实际冲突。

## 本分片整体评价

这是一段**质量中上、纪律不均**的早期重写代码：接口切分干净（control/routing/slots/propose/channels/net 职责清晰）、typed error 与 item 对齐契约是亮点，routing 的不可变原子表明显优于 v1。但它目前只覆盖 v1 约 1-25% 的功能面（send 骨架），且已经复现了 v1 的两类老毛病——goroutine join 缺失（`node_loops.go`，`-race` 下 14 个测试当场失败）和自造的、重启即冲突的消息 ID 分配器；更值得警惕的是 v2 显式放弃了 v1 已验证的 snowflake 方案。`observe` 报告链路是"三段各自完成、从未连通"的 stub 死代码，说明该分支存在"按清单推进、按连线验证"的开发节奏缺口。最优先的一件事：**把 `-race` 加入 v2 包的 CI 门槛并修掉 watch loop join 缺失**——这是该重写分支在可上线路径上的第一个硬闸门；其次是在迁移交付/会话等下游域之前先替换 MessageID 分配器，否则所有下游迁移都会建立在会重复的 ID 之上。
