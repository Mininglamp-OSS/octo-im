# app 投递路由与投递生命周期

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/app/deliveryrouting.go | 2204 | 是 |
| internal/app/committed_replay.go | 536 | 是 |
| internal/app/delivery_ack_batcher.go | 180 | 是 |
| internal/app/delivery_presence_cache.go | 173 | 是 |
| internal/app/delivery_lifecycle.go | 173 | 是 |
| internal/app/presenceworker.go | 151 | 是 |
| internal/app/presenceauthority.go | 115 | 是 |
| internal/app/user_system_uid_cache.go | 95 | 是 |
| internal/app/committed_events.go | 28 | 是 |

## 发现

### [P0] 1. 毒消息导致**永久崩溃循环**：committed replay 在启动时无条件重放未推进游标的已提交消息，而重放→投递→编码 panic 全链路同步，游标永远无法越过毒消息

- **位置**：`internal/app/committed_replay.go:208-222`（启动即全量扫描）、`internal/app/committed_replay.go:369-379`（先投递，后推进游标）、`internal/app/committed_replay.go:388-393`（`submitMessage` 同步调用 delivery）
- **类别**：正确性 / 可用性 / 远程可触发 panic
- **结论（回答优先问题）**：**是，replay 会重新投递同一条毒消息，并且构成不可自愈的崩溃循环。**

- **代码**（`committed_replay.go:208-222`，启动路径无条件先做一次全量扫描）：
  ```go
  func (r *committedReplayer) run(ctx context.Context, done chan<- struct{}) {
  	defer close(done)

  	r.runFullScanOnceAndLog(ctx)
  	ticker := time.NewTicker(r.cfg.Interval)
  	defer ticker.Stop()
  ```
  `runFullScanOnceAndLog` → `runOnce(ctx, true)` → `replayChannels(ctx, forceFullScan=true)` → 直接 `ListCommittedReplayChannels`（枚举 `engine.ListChannelKeys()` 的**全部**持久化 channel key，`committed_replay.go:279-286`、`:474-491`），不依赖内存 dirty 集合，所以**重启后必然覆盖到毒消息所在的 channel**。

- **代码**（`committed_replay.go:369-379`，投递成功才推进游标）：
  ```go
  			if err := r.submitMessage(ctx, msg); err != nil {
  				return lag, err
  			}
  			lastSeq = msg.MessageSeq
  		}
  		if lastSeq == cursor {
  			return lag, nil
  		}
  		if err := r.cfg.Log.StoreCommittedDispatchCursor(ctx, ch.Key, r.cfg.CursorName, lastSeq); err != nil {
  ```
  `StoreCommittedDispatchCursor` 在 `submitMessage` **之后**才执行。

- **代码**（`committed_replay.go:388-393`，同步提交，无 recover）：
  ```go
  func (r *committedReplayer) submitMessage(ctx context.Context, msg channel.Message) error {
  	if r.cfg.Delivery != nil {
  		if err := r.cfg.Delivery.SubmitCommitted(ctx, deliveryruntime.CommittedEnvelope{Message: msg}); err != nil {
  			return err
  		}
  	}
  ```

- **同步性已逐层核实**（这是"崩溃循环"而非"只崩一次"的关键）：
  - `internal/usecase/delivery/submit.go:10-15` → `a.runtime.Submit(ctx, env)`
  - `internal/runtime/delivery/manager.go:80-82` → `m.shardFor(...).submit(ctx, env)`
  - `internal/runtime/delivery/shard.go:26-39` → `err := act.handleStartDispatch(ctx, env)`（**无 `go`，同一 goroutine**）
  - `internal/runtime/delivery/actor.go:45-62` → `dispatchObserved` → `dispatch` → `resumeResolvable` → `resolvePages` → `applyPush`（`actor.go:327-338`）→ `pushOutsideLock`（只是临时放锁，仍在同一 goroutine）
  - → `internal/app/deliveryrouting.go:1521-1523` `localDeliveryPush.Push` → `pushEnvelope` → `:1565` `conn.Session.WriteFrame(f)`；
    或 `deliveryrouting.go:1890-1901` `deliveryPushItem` → `encodeDeliveryFrame` → `p.codec.EncodeFrame(f, frame.LatestVersion)`（**远程路由分支无需任何本地连接即可触发编码**）
  - `pkg/protocol/codec/encoder.go:136-146` `WriteString` 在 `len(str) > math.MaxInt16` 时 `panic(...)`
  - 全仓非测试 `recover()` 只有 2 处：`internal/access/plugin/handlers_lifecycle.go:48`、`internal/runtime/channelmeta/activate.go:41` —— **都不在这条链上**

- **触发路径**（逐步）：
  1. 攻击者/异常客户端向 `POST /message/send` 提交一条 `client_msg_no` 长度 > 32767 字节的消息（该字段未做长度校验，见单元 04 的 P0）。
  2. 消息被持久化并 commit 到 channel log，`committedSeq` 前进到 `N`。
  3. 同步 fan-out：`buildRealtimeRecvPacket`（`deliveryrouting.go:2156-2184`）把 `ClientMsgNo` 原样拷入 `frame.RecvPacket`；编码时 `WriteString` panic；panic 沿同步调用栈向上，**没有任何 recover**，进程死亡。
  4. 进程重启 → `committedReplayer.Start` → `run` → `runFullScanOnceAndLog` → 枚举全部 channel key → 该 channel 的 `committed` 游标仍停在 `N-1`（第 3 步中 `StoreCommittedDispatchCursor` 从未执行到）。
  5. `LoadCommittedMessages(startSeq=N)` 取回毒消息 → `submitMessage` → 同步投递 → 同一个 panic → 进程再次死亡。
  6. 回到第 4 步。**无限循环，且游标永远不会推进**，因为推进游标的那一行永远执行不到。

- **后果**：
  - 一条未认证 HTTP 请求即可让**承载该 channel 的节点进入不可自愈的崩溃循环**。
  - 该 channel 是持久化的，重启、扩容、换节点都无法摆脱：只要某节点成为该 channel 的 leader（`CommittedReplayState` 在 `committed_replay.go:525` 用 `status.Leader != localNodeID` 过滤，意味着 leader 漂移会把毒药带到新 leader），新 leader 也会崩。
  - 恢复只能靠人工数据手术（直接改写 Pebble 里的 `committed` 游标或删除该消息行），无任何运行时开关可绕过。
  - 严重度高于单纯的"远程可触发崩溃"：这是**持久化的、自我复制的**崩溃。

- **加重因素**：`replayChannel` 的外层 `runOnce` 循环（`committed_replay.go:261-275`）在 `replayChannel` 返回 error 时**直接 `return err` 中止整个 pass**，所以即使毒消息只是让 replay 报错（而非 panic），它后面所有 channel 的 replay 也会被永久堵住——单条消息阻塞全节点的 replay 修复能力。

- **建议**：在编码层把 `WriteString` 的 panic 改成返回 error（或在 fan-out/replay 入口加 recover + 毒消息隔离），并在入口对所有线路格式字符串字段做长度校验；同时给 replay 增加"投递失败计数 → 跳过并告警"的毒消息隔离（poison pill quarantine），让游标能在人工介入前先推进过去。

### [P1] 2. `committed` 投递游标**只由 replay 推进、热路径从不推进**，导致每条消息被重复投递一遍；线上唯一的去重是 256 条的进程内 LRU

- **位置**：`internal/app/committed_replay.go:377`（唯一的 `StoreCommittedDispatchCursor` 调用点）、`internal/app/committed_replay.go:212-221`（30s 周期）
- **类别**：正确性（消息重复）/ 性能
- **代码**（`committed_replay.go:369-381`）：
  ```go
  			if err := r.submitMessage(ctx, msg); err != nil {
  				return lag, err
  			}
  			lastSeq = msg.MessageSeq
  		}
  		if lastSeq == cursor {
  			return lag, nil
  		}
  		if err := r.cfg.Log.StoreCommittedDispatchCursor(ctx, ch.Key, r.cfg.CursorName, lastSeq); err != nil {
  ```
- **核实**：全仓 `grep -rn "StoreCommittedDispatchCursor\|AdvanceCommittedDispatchCursor"`（排除 `_test.go`）只有 `committed_replay.go:338`、`:377` 两个业务调用点，**都在 replayer 内部**。热路径（`asyncCommittedDispatcher.routeCommitted` → `submitLocal` → `deliveryApp.SubmitCommitted`，`deliveryrouting.go:526-620`）成功投递后**不会**推进任何游标。
- **触发路径**：
  1. 消息 seq=N commit，热路径同步 fan-out 成功，客户端收到第一份 RECV。游标仍为 N-1。
  2. 最多 30s 后（`defaultCommittedReplayInterval = 30 * time.Second`，`committed_replay.go:22`）replay pass 命中该 channel（该 channel 已被 `SubmitCommitted` 标 dirty，`committed_replay.go:143-156`）。
  3. `LoadCommittedMessages(startSeq=N)` 取回 seq=N，`submitMessage` 再次提交给同一个 delivery actor。
  4. 是否重复推送完全取决于 actor 的内存去重：`internal/runtime/delivery/actor.go:602-608` `hasSeenMessage` 只查 `inflight` + `completed`，而 `completed` 的容量是 `recentCompletedMessageCap = 256`（`actor.go:9`，`rememberCompleted` 在 `:610-627` 做 FIFO 淘汰）。
  5. 若该 channel 在这 30s 窗口内的消息数 > 256（≈8.5 msg/s，繁忙群聊很常见），窗口前段的 messageID 已被挤出 `completed` → `hasSeenMessage` 返回 false → `dispatch` 重新建 InflightMessage → 重新 resolve → 重新 `applyPush` → **客户端收到第二份同 messageID 的 RECV 帧**。
  6. 同样条件下 actor 也会被 idle 回收（`deliveryruntime` 默认 `IdleTimeout = time.Minute`，`internal/runtime/delivery/manager.go:43-45`；`internal/app/build.go:607-633` 的 `deliveryruntime.Config` **没有设置 IdleTimeout**，用默认值）→ 去重集合整体丢失。
  7. 进程重启后 actor map 为空，游标最多落后 30s，**整段窗口的消息会全量重投**给已重连的客户端。
- **后果**：
  - 重复 RECV 帧（客户端若不按 messageID 去重则消息重复显示）。
  - 更严重的副作用：重投会重新走 resolve → `notifyResolvedUIDPage`（`deliveryrouting.go:1409-1420`，触发 CMD 会话意图路由）和 `notifyOfflineResolved`（`internal/runtime/delivery/shard.go:37`，触发 `pluginReceiveObserver` → **离线推送插件被二次调用**）。离线用户会收到**重复的推送通知**。
  - 恒定 2× 的 fan-out CPU / resolve / 推送开销（正常路径 + replay 路径各一次），且 replay 每 30s 全量重扫 dirty channel。
- **建议**：热路径投递成功后同步（或批量）推进 `committed` 游标，replay 只负责补齐真正落后的窗口；同时让去重键持久化或至少覆盖一个完整的 replay 周期，而不是固定 256 条。

### [P2] 3. delivery tag 拓扑校验在每条群消息上做 O(assignments × slots) 线性扫描，且 tag 缓存未命中时把**整个订阅者列表**无上界读入内存

- **位置**：`internal/app/deliveryrouting.go:808-832`（线性扫描）、`:729-760`（每 UID 算 slot + 每 slot 一次 `LeaderOf`）、`:1320-1341`（无上界收集 UID）
- **类别**：性能
- **代码**（`deliveryrouting.go:813-832`，嵌套线性查找）：
  ```go
  	assignmentBySlot := make(map[uint32]controllermeta.SlotAssignment, len(requiredSlotIDs))
  	for _, assignment := range assignments {
  		if !deliveryTagSlotRequired(assignment.SlotID, requiredSlotIDs) {
  			continue
  		}
  ...
  func deliveryTagSlotRequired(slotID uint32, requiredSlotIDs []uint32) bool {
  	for _, requiredSlotID := range requiredSlotIDs {
  		if slotID == requiredSlotID {
  			return true
  		}
  	}
  ```
  `requiredSlotIDs` 已经是**排序好的** `[]uint32`（`:740` `sort.Slice`），却用线性 `deliveryTagSlotRequired` 而不是二分/map。
- **代码**（`deliveryrouting.go:1320-1341`，无上界累积）：
  ```go
  	uids := make([]string, 0, pageSize)
  	for {
  		page, next, done, err := r.subscribers.NextPage(ctx, snapshot, cursor, pageSize)
  		if err != nil {
  			return nil, err
  		}
  		uids = append(uids, page...)
  ```
- **触发路径**：
  1. 一个群 channel（`ChannelType != ChannelTypePerson`，person 走 `deliveryTagPartitionBypassed` 旁路，`:1112-1116`）收到一条消息。
  2. tag 快路径命中时（`leaderTagFromSnapshot:1243-1262`）仍会调用 `ValidateCurrentDeliveryTagTopology`（`:763-790`）→ `currentDeliveryTagAssignmentBySlot` → `deliveryTagAssignmentBySlot`。设集群配置 `InitialSlotCount = S`、该群订阅者覆盖 `K` 个不同 slot，则每条消息做 `S × K` 次比较。S=1024、K=256 时 ≈ 26 万次比较 **每条消息**。
  3. tag 快路径未命中（TTL 1 分钟，`internal/app/build.go:584-587` `deliverytagruntime.NewManager(Options{TTL: time.Minute})`）→ `collectSnapshotUIDs` 分页读完**全部**订阅者进一个 slice，再 `uniqueStringsForDeliveryTag`（`:1488-1506`）为全量 UID 建一个 map，再 `CurrentDeliveryTagTopology` 对全量 UID 建 slotSet map 并对每个唯一 slot 调一次 `cluster.LeaderOf`。10 万人群 = 每分钟一次 10 万字符串 + 两个 10 万条 map 的分配。
- **后果**：大群场景下 fan-out 延迟与 GC 压力随群规模和 slot 数放大；`LeaderOf` 每 slot 一次调用在拓扑变更期还可能触发 controller 查询放大。
- **建议**：`requiredSlotIDs` 改成 `map[uint32]struct{}` 或对已排序数组做二分；订阅者快照改成流式分区（边读边分桶）而不是先全量落地；拓扑校验结果按 `HashSlotTableVersion` 做短期 memo。

### [P3] 4. delivery tag 的"按 slot 权威分区"实际是**按下标取模的轮转分配**，与 `SlotAuthorityRef` 设计矛盾；分发给 follower 的 RPC 无任何生产调用方（半成品死代码）

- **位置**：`internal/app/deliveryrouting.go:1350-1361`
- **类别**：架构 / 潜在正确性
- **代码**：
  ```go
  func (r tagDeliveryResolver) partitionDeliveryTagUIDs(_ context.Context, uids []string, topology deliverytagruntime.PartitionTopologyVersion) []deliverytagruntime.NodePartition {
  	byNode := make(map[uint64][]string)
  	for index, uid := range uids {
  		nodeID := r.localNodeID
  		if len(topology.SlotAuthorityRefs) > 0 {
  			nodeID = topology.SlotAuthorityRefs[index%len(topology.SlotAuthorityRefs)].LeaderNodeID
  		}
  ```
  分区归属用的是 `index % len(SlotAuthorityRefs)` —— UID 在 slice 中的**位置**，与该 UID 自己的 slot 毫无关系。而 `SlotAuthorityRefs` 恰恰是按 `r.cluster.SlotForKey(uid)` 逐 UID 算出来的（`:729-760`），即上游花了 O(n) 算准了每个 UID 的 slot 归属，下游又扔掉不用。
- **为什么现在没炸**：`deliveryTagRoutableUIDs`（`:1447-1462`）把**所有** partition 的 UID 拼在一起后本地全量展开，所以构建节点上分区是否正确不影响路由；真正会依赖分区语义的 `Manager.StoreFollowerPartition`（`internal/runtime/deliverytag/manager.go:124-151`，只保留 `tag.PartitionForNode(localNodeID)`）唯一入口是 `deliveryTagAuthority.UpdateDeliveryTag`（`deliveryrouting.go:714-723`）这个 RPC 服务端；而 `accessnode.Client.UpdateDeliveryTag`（`internal/access/node/delivery_tag_rpc.go:133`）**全仓没有任何非测试调用方**（`grep -rn '\.UpdateDeliveryTag(' --include=*.go` 只命中服务端分发与该定义本身）。
- **触发路径（潜在）**：一旦有人把 follower 分发接上（这显然是设计意图，见 `NodePartition` 注释 "stores the subscriber UIDs one node should process"），follower 用 `PartitionForNode(localNodeID)` 取到的将是一组与本节点 slot 无关的随机 UID；同一个 UID 可能没有任何节点负责（若它的 slot leader 恰好没被轮转到）→ **该订阅者永久收不到群消息**。
- **后果**：当前为架构不一致 + 死代码（P3）；若 follower 分发被接上则升级为投递丢失（P1）。
- **建议**：分区按 `cluster.SlotForKey(uid)` 映射到对应 `SlotAuthorityRef.LeaderNodeID`；或者删除整套未接线的 follower 分区机制，避免留一个语义错误的半成品。

### [P2] 5. 远程 ack 批处理器**只做 fire-and-forget**：`NotifyAck` 立刻返回 nil，批量 RPC 的错误写进一个**永远没人读**的 channel 被彻底丢弃，上游据此提前删除 ack 绑定

- **位置**：`internal/app/delivery_ack_batcher.go:58-94`（返回 nil）、`:123-137`（错误写入 `waiter.done` 后无人消费）、`internal/app/deliveryrouting.go:2041-2047`（上游据返回值删绑定）
- **类别**：错误处理 / 正确性
- **代码**（`delivery_ack_batcher.go:65-93`）：
  ```go
  	if ctx == nil {
  		ctx = context.Background()
  	}
  	waiter := &deliveryAckWaiter{command: cmd, done: make(chan error, 1)}
  	var ready []*deliveryAckWaiter
  	...
  	if len(ready) > 0 {
  		go n.flushWaiters(nodeID, ready)
  	}
  	return nil
  ```
  ```go
  	err := n.sender.NotifyAckBatch(ctx, nodeID, commands)
  	cancel()
  	for _, waiter := range waiters {
  		waiter.done <- err          // :135 —— 全仓无任何一处 <-waiter.done
  	}
  ```
- **核实**：`deliveryAckWaiter.done` 在本文件外无引用；`grep -n "\.done"` 在本文件内只有 `:42` 声明与 `:135` 写入，**没有任何接收方**。这三处 staticcheck 告警（`:66` SA4006 `ctx` 赋值后未用、`:66` SA4017、`:139` U1000 `cancelWaiter` 未使用）指的正是同一件事：原本设计的"等待 + 取消"路径写了一半就没接上。
- **触发路径**：
  1. 运维打开 `Delivery.AckBatchMaxWait > 0`（`internal/app/build.go:754-763` 才会构造这个 notifier；默认 0 时不构造，所以本问题是**配置开启后**才生效）。
  2. 客户端对一条由远程 owner 节点投递的消息回 RECVACK → `ackRouting.AckRoute`（`deliveryrouting.go:2033`）→ `r.notifier.NotifyAck(ctx, binding.OwnerNodeID, event)`。
  3. `NotifyAck` 把 ack 塞进批次后**立即返回 nil**。
  4. `AckRoute` 认为成功，执行 `r.remoteAcks.RemoveRoute(...)`（`deliveryrouting.go:2046`），**本地 ack 绑定被删除**。
  5. 稍后批次 flush，`NotifyAckBatch` 因网络分区/owner 重启失败 → err 写进 `waiter.done` → 无人读取 → **owner 节点永远不知道这条消息已被确认**。
  6. owner 的 delivery actor 按 `RetryDelays{500ms, 1s, 2s}` 重推该路由 → 客户端**收到重复 RECV**；重试耗尽后走 `expireRoute` → 该消息被记为投递失败。
- **后果**：ack 静默丢失 → 重复推送 + 错误的投递失败统计；`ctx`（含上游超时/取消）被完全忽略；`cancelWaiter` 这条取消路径写好了从未接线。
- **建议**：`NotifyAck` 应 `select { case err := <-waiter.done: ...; case <-ctx.Done(): n.cancelWaiter(...) }` 真正等待批次结果并把错误返回给 `ackRouting`，或者明确改成异步语义并在失败时重试/告警，而不是让上游误以为成功。

### [P2] 6. `Close()` 不等待已派生的 flush goroutine，关闭后仍可能用已停止的 node client 发 RPC

- **位置**：`internal/app/delivery_ack_batcher.go:90-92`、`:103-114`、`:131-133`
- **类别**：并发 / 资源（use-after-close）
- **代码**：
  ```go
  	if len(ready) > 0 {
  		go n.flushWaiters(nodeID, ready)      // :91 gosec G118，无 ctx、无 WaitGroup
  	}
  ...
  func (n *deliveryAckBatchNotifier) Close() {
  	n.mu.Lock()
  	n.closed = true
  	pending := n.detachAllLocked()
  	n.mu.Unlock()
  	for nodeID, waiters := range pending {
  		n.flushWaiters(nodeID, waiters)        // 只 flush 还在 map 里的；已 detach 出去的 goroutine 不管
  	}
  }
  ```
  `flushWaiters` 内部用 `context.WithTimeout(context.Background(), 5*time.Second)`（`:131`），与关闭流程完全解耦。
- **触发路径**：
  1. 高 ack 速率下 `len(batch.waiters) >= n.maxBatch`（默认 64）触发 `detachNodeLocked` + `go n.flushWaiters(...)`；该批次此刻已从 `n.batches` 中删除。
  2. 进程开始关闭，`cleanup` 执行 `ackBatcher.Close()`（`internal/app/build.go:759-762`）。`detachAllLocked` 看不到已 detach 的批次，`Close` 立即返回。
  3. cleanup 栈继续往下关闭 `app.nodeClient` / 网络层。
  4. 步骤 1 的 goroutine 仍在 `n.sender.NotifyAckBatch(ctx, nodeID, commands)` 里（最长 5s），对一个正在/已经关闭的 client 发 RPC。
- **后果**：关闭期 use-after-close（取决于 nodeClient 关闭实现，可能是 error、阻塞，最坏是对已关闭连接池的 panic）；`Close()` 返回不代表静默，违反 `Stop()` 语义。
- **建议**：给 notifier 加 `sync.WaitGroup`，`go n.flushWaiters` 前 `wg.Add(1)`，`Close()` 在置 `closed` 后 `wg.Wait()`；flush 的 context 从一个 notifier 级别的可取消 ctx 派生。

### [P1] 7. 系统 UID 缓存**重启后全部丢失**（无持久化回载），且集群广播遇第一个错误就中止、目标节点列表是启动时的静态配置快照

- **位置**：`internal/app/user_system_uid_cache.go:62-77`（首错中止）、`internal/app/build.go:713-718`（静态 peer 列表）、缺失的启动回载
- **类别**：分布式一致性 / 正确性
- **代码**（`user_system_uid_cache.go:62-77`）：
  ```go
  	for _, nodeID := range uniqueRemoteSystemUIDCacheNodes(u.localNodeID, u.peerNodeIDs) {
  		var err error
  		if add {
  			err = u.remote.AddSystemUIDsToCache(ctx, nodeID, uids)
  		} else {
  			err = u.remote.RemoveSystemUIDsFromCache(ctx, nodeID, uids)
  		}
  		if err != nil {
  			return fmt.Errorf("sync system uid cache to node %d: %w", nodeID, err)
  		}
  	}
  ```
- **核实**：
  - `IsSystemUID`（`internal/usecase/user/legacy.go:152-163`）**只查进程内 map**，没有任何回退到 `SystemUIDStore` 的路径。
  - 全仓（排除 `_test.go`）写入 `systemUIDCache` 的入口只有三处：`internal/access/api/user_legacy.go:104`（本地 HTTP）、`internal/access/node/user_system_uid_rpc.go:35`（peer RPC）、`internal/usecase/user/legacy.go:80`（`AddSystemUIDs` 自身）。**没有任何启动时从 `ListSystemUIDs()` 回载缓存的代码**。
  - `peerNodeIDs` 来自 `controllerPeerIDs(cfg.Cluster.DerivedControllerNodes(), cfg.Cluster.runtimeSeeds())`（`build.go:717`）—— 配置派生的**静态**列表，只含 controller 节点与配置种子。
- **触发路径 A（重启丢失）**：
  1. 运维调系统 UID 新增接口把 `svc_bot` 加为系统 UID；`local.AddSystemUIDs` 持久化到 store 并写入本地 map，再广播给 peer。
  2. 任一节点重启 → `userusecase.New` 建一个**空** `systemUIDCache`（`internal/usecase/user/app.go:57`），store 里的持久化记录**从不被读回**。
  3. 该节点上 `IsSystemUID("svc_bot")` 永久返回 false → 系统账号丢失全部旁路特权（例如 `cfg.Message.UserRateLimitSystemUIDBypass` 限流旁路）。
  4. 结果：**同一个请求打到不同节点会有不同行为**，且没有任何日志/指标提示。
- **触发路径 B（部分广播）**：
  1. 3 个 peer，第 2 个正在重启。
  2. `broadcastSystemUIDCache` 对 peer1 成功、peer2 返回 error → **立即 return**，peer3 永远收不到。
  3. `local.AddSystemUIDs` 已经成功（函数开头就执行了），API 返回错误，但**本地和 peer1 的缓存已经被改了**，没有任何补偿让 peer3 收敛。
- **触发路径 C（动态加入的节点）**：非 controller、非种子的数据节点动态加入后，既不在 `peerNodeIDs` 里（收不到广播），也不做启动回载 → 永远不知道任何系统 UID。
- **后果**：系统账号权限在集群内不一致且不可自愈；重启即静默失效。
- **建议**：`IsSystemUID` 未命中时回退查 `SystemUIDStore`（或启动时 `ListSystemUIDs` 回载 + 周期对账）；广播改成"全部尝试 + 收集错误 + 后台重试"，节点列表改为从集群成员视图动态获取。

### [P2] 8. presence 缓存的失效会被**并发中的回填覆盖**，把已下线会话的路由重新缓存整个 TTL；受害用户既收不到实时消息、也拿不到离线推送

- **位置**：`internal/app/delivery_presence_cache.go:68-99`（读取→锁外 RPC→回填之间无版本保护）、`:147-153`（`invalidateUID` 只 delete）
- **类别**：并发（缓存 stale-write 竞态）/ 正确性
- **代码**（`delivery_presence_cache.go:79-96`）：
  ```go
  	routes, err := c.target.EndpointsByUID(ctx, uid)   // 锁外 I/O
  	if err != nil {
  		return nil, err
  	}
  	cached := clonePresenceRoutes(routes)
  	out := clonePresenceRoutes(cached)
  	expiresAt := now.Add(c.ttl)
  	c.mu.Lock()
  	if len(c.entries)+1 > c.maxEntries {
  		c.entries = make(map[string]deliveryPresenceCacheEntry)
  	}
  	if len(cached) == 0 {
  		delete(c.entries, uid)
  	} else {
  		c.entries[uid] = deliveryPresenceCacheEntry{routes: cached, expiresAt: expiresAt}
  ```
  回填时只检查 `len(cached)`，**不检查这期间是否发生过 `invalidateUID`**（`:147-153` 只是 `delete`，没有代号/版本位）。
- **触发路径**（具体交错）：
  1. 前置：`Delivery.PresenceCacheTTL > 0`（`internal/app/build.go:580-582`，默认 0 不启用，所以这是**配置开启后**的问题）。
  2. 群消息 fan-out，goroutine G1 对 uid `U` 缓存未命中，进入锁外 `c.target.EndpointsByUID(ctx, U)`（可能是跨节点 RPC，见 `presenceauthority.go:47-58`），返回 `U` 在 session `S1` 上在线。
  3. 此刻 `U` 断线：gateway 走 `UnregisterAuthoritative` → `deliveryPresenceCache.UnregisterAuthoritative`（`:52-56`）→ `invalidateUID(U)`（此时 map 里本来就没有 `U` 的条目，delete 是空操作）。
  4. G1 恢复执行，把**步骤 2 的旧数据** `S1` 写进 `c.entries[U]`，`expiresAt = now + TTL`。
  5. 接下来整个 TTL 内，所有 fan-out 对 `U` 都命中这条陈旧缓存 → `routes` 非空。
  6. 因为 `routes` 非空，resolver 不会把 `U` 放进 `offlineUIDs`（`deliveryrouting.go:1000-1002`、`:1385-1387` 都是 `if len(routes) == 0` 才计入）→ `actor.recordOfflineResolvedEvents`（`internal/runtime/delivery/actor.go:442-445`，仅消费 `page.OfflineUIDs`）不产生离线事件 → **`pluginReceiveObserver` 不被调用，离线推送不发**。
  7. 而 `localDeliveryPush.pushEnvelope`（`deliveryrouting.go:1544-1548`）查 `online.Connection(S1)` 查不到 → 路由进 `Dropped` → 被当作"正常丢弃"，不重试、不告警。
- **后果**：TTL 窗口内发给 `U` 的每条消息**既没有实时投递、也没有离线推送**，用户完全静默丢消息，日志里只有 Debug 级的 dropped 计数。
- **加重因素（同文件）**：
  - 跨节点失效缺失：`invalidateUID` 只在**本节点**的 Register/Unregister 上调用；`U` 在节点 B 上上线/下线时，节点 A 的缓存要等 TTL 自然过期。
  - 淘汰策略是"满了就整表清空"（`:87-89`、`:128-130`）而不是 LRU：到 65536 条时一次性丢弃全部缓存 → 随后所有 resolve 集体穿透到 presence 权威节点（惊群）。
  - 完全没有按 TTL 的定期清理：早已过期的条目一直占着内存，直到触发整表清空。
- **建议**：给每个 uid 加失效代号（generation），回填前比对，代号变过就丢弃这次结果；`invalidateUID` 递增代号而不是单纯 delete；淘汰改成 LRU + 定期过期清理；presence 权威侧变更应主动向持缓存节点推失效。

### [P2] 9. presence worker 每 100ms 对**每个有本地在线会话的 slot** 调一次 `LeaderOf`（走 multiraft 全局锁），且心跳失败的 error 被完全丢弃、无日志

- **位置**：`internal/app/presenceworker.go:10`（100ms 常量）、`:85-89`（丢 error）、`:120-138`（每 slot 一次 `leaderOf`）
- **类别**：性能 / 错误处理
- **代码**（`presenceworker.go:85-90`）：
  ```go
  			case <-heartbeatTicker.C:
  				_ = heartbeater.HeartbeatOnce(ctx)
  			case <-leaderTickerCh:
  				if pollPresenceSlotLeaders(observedLeaders, activeSlotIDs, leaderOf) {
  					_ = heartbeater.HeartbeatOnce(ctx)
  				}
  ```
  ```go
  	currentSlots := make(map[uint64]struct{})      // :126 每 100ms 新建一个 map
  	changed := false
  	for _, slotID := range activeSlotIDs() {
  		currentSlots[slotID] = struct{}{}
  		leaderID, err := leaderOf(slotID)
  ```
- **核实调用成本**：`leaderOf` 绑定的是 `app.cluster.LeaderOf`（`internal/app/build.go:594-597`）→ `pkg/cluster/cluster.go:1329` → `pkg/cluster/router.go:56` → `pkg/slot/multiraft/api.go:247-263` `Runtime.Status`，后者 `r.mu.RLock()` 取的是 **multiraft runtime 的全局 RWMutex**，再做 `g.statusSnapshot()`。
- **触发路径**：
  1. `defaultPresenceLeaderPollInterval = 100 * time.Millisecond`（`presenceworker.go:10`），且 `activeSlotIDs`/`leaderOf` 都在 `build.go:567-597` 被赋值，所以这个 100ms 分支**一定启用**。
  2. `activeSlotIDs` = `onlineRegistry.ActiveSlots()` 里所有有本地在线会话的 slot。UID 按 hash 散布，一台承载几万连接的 gateway 上这基本等于**全部 slot**。
  3. 集群配置 `InitialSlotCount = 1024` 时：每秒 10 × 1024 = **10240 次 multiraft 全局 RWMutex RLock + statusSnapshot**，且每秒新建 10 个 1024 容量的 map，永不停止——只为发现 leader 变更。
  4. 这把全局锁的写方（slot 增删、配置变更、迁移）会被这个稳定的读风暴拖慢。
- **心跳错误**：`HeartbeatOnce` 失败（presence 权威节点不可达、slot 无 leader）时 error 被 `_ =` 丢弃且**没有任何日志或指标**。presence 租约续不上 → 本节点全部会话在集群视角变成离线 → 消息改走离线推送，运维完全看不到原因。
- **建议**：leader 变更改为订阅集群拓扑事件而非 100ms 轮询；退一步也应把间隔调到秒级并复用 map；`HeartbeatOnce` 失败必须记日志 + 计数器。

### [P2] 10. 投递运行时维护 tick 每 200ms 为了一个 gauge **逐个加锁遍历全部 delivery actor**

- **位置**：`internal/app/delivery_lifecycle.go:14`（200ms 常量）、`:110-121`（每 tick 调一次）、`:126-131`
- **类别**：性能（持锁遍历热点）
- **代码**（`delivery_lifecycle.go:110-121`）：
  ```go
  		case <-retryTick:
  			_ = l.cfg.Runtime.ProcessRetryTicks(ctx)
  			l.observeMaintenanceSnapshot()
  			retryTick = l.cfg.After(l.cfg.TickInterval)
  		case <-sweepTick:
  			l.cfg.Runtime.SweepIdle()
  			l.observeMaintenanceSnapshot()
  ```
  ```go
  	snapshot := deliveryruntime.MaintenanceSnapshot{
  		InflightRoutes: l.cfg.Runtime.InflightRouteCount(),
  		AckBindings:    l.cfg.Runtime.AckBindingCount(),
  	}
  ```
- **核实**：`AckBindingCount()` 是 O(1)（`internal/runtime/delivery/ackindex.go:204-211`，只返回 `len`）；但 `InflightRouteCount()`（`internal/runtime/delivery/manager.go:126-140`）对**每个 shard** 取 `s.mu`、复制 actor 列表，再对**每个 actor** 取 `act.mu`（`shard.go:123-135` + `actor.go:587-589`）。而 `act.mu` 正是消息 fan-out 主路径持有的锁（`shard.go:30-35` 在 `handleStartDispatch` 全程持有）。
- **触发路径**：`deliveryRuntimeRetryTickInterval = 200 * time.Millisecond`（`delivery_lifecycle.go:14`）→ 每秒 5 次全量遍历。节点上活跃 channel actor 数为 N（idle 1 分钟才回收，繁忙集群 N 可达数万）→ 每秒 5N 次互斥锁获取，且每次都要排在正在做 fan-out 的 actor 后面。
- **后果**：高 channel 基数下这是一条与消息主路径直接争锁的固定开销；`ProcessRetryTicks` 本身是时间轮（只处理到期项，成本合理），浪费全在这个 gauge 上。
- **建议**：`InflightRoutes` 改成 actor 增减时维护的原子计数器；或让快照只跟 30s 的 sweep tick 采集。

### [P2] 11. committed 分片队列溢出会**静默丢弃实时投递**，只有指标、没有日志，靠 30s 后的 replay 补救

- **位置**：`internal/app/deliveryrouting.go:333-348`
- **类别**：错误处理 / 可观测性
- **代码**：
  ```go
  	select {
  	case queue <- committedDispatchItem{ctx: ctx, env: env}:
  		depth = len(queue)
  	default:
  		enqueueResult = "overflow"
  		overflow = true
  		depth = len(queue)
  		fallbackScheduled = d.enqueueConversationFallbackLocked(ctx, env)
  	}
  	d.mu.Unlock()
  	...
  	if overflow && !fallbackScheduled {
  		d.logCommittedRoute(env, "conversation_fallback_dropped", 0, nil)
  	}
  	return nil
  ```
- **触发路径**：
  1. 某 channel 的写入速率超过其所属 shard worker 的处理速率（shard 由 `channelID` hash 决定，`:431-439`，**单个热点群只会落在一个 shard 上**），该 shard 的 1000 深队列打满（`committedDispatchDefaultQueueDepth = 1000`，`:46`）。
  2. `default` 分支命中：`routeCommitted` 永不执行 → 不做 resolve、不做 push → **这条消息的实时投递彻底丢失**。
  3. 只有 conversation 更新被塞进 fallback 队列；`logCommittedRoute` 仅在连 fallback 都没排上时才调用一次，而且 `logCommittedRoute`（`:589-594`）在 `err == nil` 且未开 Debug 时**直接 return`**——所以生产上这条日志也不会打。
  4. `SubmitCommitted` 返回 **nil**，上游认为一切正常。
  5. 用户要等最多 30s，直到 committed replay 把这条消息重投（`committed_replay.go:212-221`）。
- **后果**：热点群在突发流量下会出现最长 30s 的消息投递延迟，调用链上没有任何 Warn 级日志说明原因；排查只能靠 `ObserveCommittedDispatchOverflow` 指标。
- **建议**：溢出时至少打一条带 channelID / messageSeq 的 Warn；并考虑对溢出 envelope 做有界阻塞入队或立即同步投递，而不是完全依赖 30s replay。

### [P3] 12. 一条消息在 fan-out 链路上被复制 5～8 次 payload；`distributedDeliveryPush` 的 codec **每次 Push 都重新 `New()`**（值接收者上的伪懒初始化）

- **位置**：`internal/app/committed_events.go:19-27`、`internal/app/deliveryrouting.go:306`、`:499-506`、`:1601-1604`、`:1903-1918`、`:2177`
- **类别**：性能（热路径分配）
- **代码**（`committed_events.go:19-27`，每个订阅者一次 Clone）：
  ```go
  	var joined error
  	for _, sub := range f.subscribers {
  		if sub == nil {
  			continue
  		}
  		joined = errors.Join(joined, sub.SubmitCommitted(ctx, event.Clone()))
  	}
  ```
  `MessageCommitted.Clone()`（`internal/contracts/messageevents/events.go:19-23`）里 `e.Message.Payload = append([]byte(nil), e.Message.Payload...)` —— 每次 Clone 全量拷 payload。
- **代码**（`deliveryrouting.go:1601-1604`）：
  ```go
  func (p distributedDeliveryPush) Push(ctx context.Context, cmd deliveryruntime.PushCommand) (deliveryruntime.PushResult, error) {
  	if p.codec == nil {
  		p.codec = codec.New()
  	}
  ```
  `p` 是**值接收者**，赋值只影响本次调用的副本；而 `internal/app/build.go:620-632` 构造 `distributedDeliveryPush{...}` 时**没有设置 `codec` 字段**，所以 `p.codec` 恒为 nil，`codec.New()` 在**每一次 Push** 都执行一次，"懒初始化"永远不会生效。
- **逐层核实的 payload 拷贝次数**（一条群消息，3 个 committed 订阅者，见 `build.go:667-677`）：
  1. `committedFanout` 对 `committedDispatcher` / `committedReplayer` / `pluginCommittedRouter` 各 `event.Clone()` → **3 次**。
  2. `asyncCommittedDispatcher.SubmitCommitted:306` 又 `cloned := event.Clone()` → **+1**。
  3. actor `dispatch` → `cloneEnvelope(env)`（`internal/runtime/delivery/actor.go:135`）→ **+1**。
  4. `applyPush` → `cloneEnvelope(msg.Envelope)`（`actor.go:335`）→ **每个 push 批次 +1**。
  5. `buildRealtimeRecvPacket:2177` → `Payload: append([]byte(nil), msg.Payload...)` → **每个收件人帧 +1**。
  6. `deliveryPushItemWithFrame:1915` → `Frame: append([]byte(nil), frameBytes...)` → **每个远程 item +1**。
- **后果**：大 payload 在 fan-out 上产生固定倍数的分配与 GC 压力；`codec.New()` 虽只是 `&WKProto{}`（`pkg/protocol/codec/protocol.go:35-37`），但仍是每条消息一次逃逸分配，且该懒初始化是明确的实现错误。
- **建议**：`committedFanout` 只 Clone 一次并让订阅者承诺只读；`distributedDeliveryPush` 的 codec 在 `build.go` 构造时注入或改成包级单例，删掉值接收者上的伪懒初始化。

### [P3] 13. 本地投递主路径 `submitLocal` 丢弃投递与会话投影的 error 且**零日志**，同模块的严格版 `submitLocalStrict` 就在下面 6 行

- **位置**：`internal/app/deliveryrouting.go:615-637`
- **类别**：错误处理（同模块严格/宽松并存，宽松版用在主路径）
- **代码**：
  ```go
  func (d *asyncCommittedDispatcher) submitLocal(ctx context.Context, env deliveryruntime.CommittedEnvelope) {
  	if d.delivery != nil {
  		_ = d.delivery.SubmitCommitted(ctx, env)
  	}
  	d.submitConversation(ctx, env.Message)
  }

  func (d *asyncCommittedDispatcher) submitLocalStrict(ctx context.Context, env deliveryruntime.CommittedEnvelope) error {
  	if d.delivery == nil {
  		return errMessageScopedDeliveryRequired
  	}
  	if err := d.delivery.SubmitCommitted(ctx, env); err != nil {
  		return err
  	}
  ```
- **触发路径与实际影响（已核实，故降级为 P3）**：`submitLocal` 是 `routeCommitted` 两个主分支（`:527-531` `no_channel_log`、`:545-549` `local_owner`）的唯一投递出口。但顺着调用链核实下去，当前两个被丢弃的 error 都几乎恒为 nil：
  - `d.delivery` = `deliveryusecase.App`（`build.go:651`）→ `Manager.Submit` → `shard.submit` → `handleStartDispatch`；resolve 失败在 `actor.handleResolveFailure`（`internal/runtime/delivery/actor.go:270-287`）内部转成重试并返回 `nil`，push 失败在 `applyPush`（`actor.go:342-344`）转成 `Retryable`，所以 `Submit` 实际上不返回 error。
  - `d.conversation` = `conversationusecase.projector`（`build.go:652`）→ `SubmitCommitted`（`internal/usecase/conversation/projector.go:167-179`）**所有分支都 return nil**，连"待处理队列已满、本次更新被丢弃"（`:174-176` `if !p.enqueuePending(msg) { return nil }`）也返回 nil。
- **后果**：当前不丢数据，但这是一个**零日志的静默通道**：任何未来给 `Submit`/`SubmitCommitted` 增加 error 返回的改动，都会在这条主路径上被无声吞掉；而 `routeCommitted` 其余分支（`:551-566`）都规规矩矩通过 `logCommittedRoute` 上报。
- **建议**：`submitLocal` 至少在 error 非 nil 时调 `d.logCommittedRoute(env, "local_submit_failed", d.localNodeID, err)`，与同函数其它分支保持一致。

### [P3] 14. `internal/FLOW.md` 把 committed replay 描述成"补偿未分发事件"，实际它是**唯一**推进 cursor 的一方，因而会重放全部已分发事件

- **位置**：`internal/FLOW.md:365-369` vs `internal/app/committed_replay.go:377`
- **类别**：架构 / 文档一致性
- **文档原文**（`internal/FLOW.md:365-369`）：
  ```
  committed_replay 后台从已提交 Channel Log 补偿未分发事件
    ├─ 按 committed cursor 扫描 message_seq
    ├─ 重新提交 delivery / conversation；不再运行独立 CMD subscriber-scan projector
    ├─ 普通 CMD intent 由 delivery resolver 的 UID page observer 重新产生
    └─ delivery 接受后批量推进 cursor；active hint 失败只记录告警
  ```
- **实际**：热路径（`asyncCommittedDispatcher.submitLocal`）成功投递后不推进 cursor（全仓只有 `committed_replay.go:338`、`:377` 两个推进点，见发现 2），所以 replay 扫描到的从来不是"未分发事件"，而是**上一轮 pass 以来的全部已分发事件**。文档描述的"补偿"语义与实现的"无条件重放"语义不符，会误导读者以为重复投递不可能发生。
- **建议**：要么改实现（热路径推进 cursor，让 replay 名副其实），要么把文档改成"周期性重放窗口内全部 committed 消息，由 delivery actor 的内存去重抑制重复"，并写明去重容量与失效条件。

## 阶段 2 对抗性复核对发现 1（P0 崩溃循环）的补充限定

重新读了 `internal/FLOW.md:262-280` 的启动顺序与 `committed_replay.go:115-135` 的 `Start`，需要给发现 1 加一个**准确的成立条件**，避免夸大：

- `committed_replay` 在启动序列第 **12** 步启动，`gateway` 在第 **15** 步。但 `Start` 里是 `go r.run(runCtx, r.done)`（`:133`），**全量扫描在后台 goroutine 上跑**，启动流程不等它，所以扫描与 gateway 开放端口是并发的。
- 毒消息要真正 panic，必须在重放它的那一刻**至少解析出一条路由**（发送者自己的路由会被 `isSenderDeliveryRoute` 丢弃，`deliveryrouting.go:1677-1682`）：
  - **多节点集群**：该 channel 的订阅者只要有人在线于**其它**节点，presence 就会解析出 `NodeID != localNodeID` 的路由 → `distributedDeliveryPush.Push`（`:1601`）→ `encodeDeliveryFrame`（`:1898-1901`）→ `EncodeFrame` → `WriteString` panic。**无需本节点有任何连接**，崩溃循环成立。
  - **单节点集群且重放该 channel 的瞬间恰好无人在线**：`resolvePages` 得到空路由 → 不编码 → 不 panic → `StoreCommittedDispatchCursor` 执行成功 → **cursor 越过毒消息，系统自愈**。这是唯一的逃生口，且完全靠运气（取决于全量扫描到该 channel 时是否已有客户端重连）。
- 结论修正：**在生产的多节点集群中崩溃循环成立且不可自愈**；单节点部署有概率在重启窗口内自愈。发现 1 的 P0 定级保持不变（本项目 `AGENTS.md` 明确"单节点部署统一视为单节点集群"，生产形态是多节点）。

## 已排除的候选项

- `internal/app/deliveryrouting.go:1524-1571`（`pushEnvelope` 的 `sharedFrame` / `framesByUID` 帧复用）—— 看起来像"多个收件人共享同一个可变 frame"的正确性问题，但核实后安全：`conn.Session.WriteFrame(f)` 是**同步编码**（`pkg/gateway/session/session.go:155-178` → `pkg/gateway/core/server.go:390-392` → `encodeAndWrite:700-711` 里同步 `adapter.Encode`），编码只读 packet 不改写；且整个循环在同一 goroutine 内串行，不存在并发共享。person channel 用 `framesByUID` 按 UID 分帧也是正确的（同一 UID 的多设备本就该收到同一视角的帧）。
- `internal/app/deliveryrouting.go:1651-1660`（`pushRemoteNodes` 任一节点报错就 `return PushResult{}, err`，丢弃已成功的本地/其它节点结果，进而被 `actor.applyPush:342-344` 整体标成 Retryable 重推）—— 逻辑上确实是"一个远程节点失败导致所有收件人收到重复帧"，但**当前不可达**：`pushRemoteNodeChunk`（`:1760-1825`）把 RPC 错误全部转成 `pushLegacyDeliveryItems` 的结果并返回 `nil`，唯一的 error 出口是 `deliveryPushItems` → `EncodeFrame`，而 RECV 编码在 `frame.LatestVersion` 下只会 panic 不会返回 error（`encodeRecv`/`encodeMessageSeq` 见 `pkg/protocol/codec/recv.go:102-141`、`message_seq.go:16-26`）。属于**潜在陷阱**，一旦编码层按发现 1 的建议改成返回 error，这条立刻变成 P1 的重复投递。
- `internal/app/presenceauthority.go:47-58`、`:61-95`（`EndpointsByUID` / `EndpointsByUIDs` 在 `c.local == nil` 时会空指针解引用 —— `(*presence.App).EndpointsByUID` 直接访问 `a.dir`，`internal/usecase/presence/authority.go:29-37` 无 nil 守卫；而同文件的 `shouldUseLocalLeader:105-114` 是有 `c.local == nil` 守卫的）—— 实际不可达：`build.go:551-565` 构造后立刻 `authorityClient.local = app.presenceApp`，生产路径上 `local` 恒非 nil。守卫不一致属于可读性问题，不构成缺陷。
- gosec G115 `deliveryrouting.go:438` `int(hash.Sum64() % uint64(len(d.shards)))` —— 结果被 `% len(shards)` 上界约束，误报。
- gosec G115 `deliveryrouting.go:735` `uint32(r.cluster.SlotForKey(uid))` —— `multiraft.SlotID` 底层就是 32 位槽号，误报。
- gosec G115 `deliveryrouting.go:1853` `int(resp.AcceptedCount)`（`deliveryPushResponseAcceptedCount`）—— 返回值只用于 Debug 日志字段（`:1798`），恶意 peer 最多让日志里出现一个负数，无业务影响。
- gosec G115 `deliveryrouting.go:1966`、`:1984` `int(resp.AcceptedCount)` —— 两处都有紧邻的上界判断（`:1965` `if resp.AcceptedCount >= uint64(len(routes))` 提前返回、`:1983` `if resp.AcceptedCount > uint64(len(candidates))` 提前返回），切片下标不会越界，误报。
- gosec G115 `deliveryrouting.go:2166` `int64(msg.MessageID)` —— 线路格式 `RecvPacket.MessageID` 本身就是 int64，MessageID 由雪花式生成器产出不会超过 MaxInt64，误报。
- `internal/app/deliveryrouting.go:250-266` `abandonQueuedCommitted`（关闭时清空所有排队的 committed 事件）—— 看起来像关机丢消息，但这是**显式设计**（函数注释 "channel log replay is the durable fallback"，`internal/FLOW.md:365` 亦如此描述），且下次启动的全量 replay 会补齐。不算缺陷。
- `internal/app/committed_replay.go:185-197` / `deliveryrouting.go:220-232` 的 `waitStop` 在 ctx 超时分支里起了一个"后台等 done"的 goroutine —— 该 goroutine 一定会随 `close(done)` 退出，不会泄漏；重复 `StopContext` 由 `stopping`/`done` 双字段保护，不会 double-close。已复核安全。
- `internal/app/delivery_lifecycle.go:107-122` 用 `time.After` 而非 `Ticker` —— 每次分支触发后重新 `After`，旧 timer 已经 fire；只有 ctx 取消时会残留最多两个待 fire 的 timer（200ms / 30s），可忽略，不构成泄漏。

## 本分片整体评价

这是全系统最热的路径，代码整体是有设计意图的（分片队列、actor 去重、tag 分区、replay 补偿、指标齐全），但**关键不变量没有被端到端守住**。最需要优先处理的是发现 1：一条未经长度校验的字符串字段（`client_msg_no`/`msg_key`/`topic`/`stream_no`/`from_uid` 任一 > 32767 字节）会在 `buildRealtimeRecvPacket` 原样进入 RECV 帧，编码层 `panic` 而全仓只有两处与此无关的 `recover()`；由于消息先持久化后 fan-out，且 `committed_replay` 在启动时无条件全量重放、并且"先投递成功再推进 cursor"，多节点集群会进入**持久化的崩溃循环**，只能靠人工数据手术恢复。

第二优先是发现 2：committed cursor 只由 replay 推进，热路径从不推进，意味着每条消息都会在 30s 内被重放一遍，唯一的防线是每个 actor 256 条的内存 LRU —— 繁忙群聊、actor idle 回收、进程重启这三种常见情况下防线都会失效，产生重复 RECV 帧和**重复的离线推送**。这条同时也是 `internal/FLOW.md` 描述与实现不符的根源（发现 14）。

此外有一类共性问题值得协调者统一看：**主路径上的失败被系统性地降级为静默**——`submitLocal` 丢 error 无日志（13）、分片队列溢出只有指标无日志（11）、presence 心跳失败 `_ =` 丢弃（9）、ack 批处理的 RPC 错误写进无人读的 channel（5）、系统 UID 广播首错中止且无补偿（7）。这些单独看严重度不高，合起来的效果是"生产出问题时链路上什么都查不到"。性能侧的问题（3、10、12 与 9 的轮询）都是"为了一个可有可无的东西在每条消息 / 每 100ms 上做全量扫描或全量拷贝"，属于可以低风险优化的部分。
