# 节点内投递运行时 + 在线态 + 分配器 + 日志/诊断（internal/runtime/delivery 等）

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/runtime/delivery/actor.go | 685 | 是 |
| internal/runtime/delivery/ackindex.go | 369 | 是 |
| internal/runtime/delivery/manager.go | 192 | 是 |
| internal/runtime/delivery/shard.go | 167 | 是 |
| internal/runtime/delivery/types.go | 241 | 是 |
| internal/runtime/delivery/retrywheel.go | 103 | 是 |
| internal/runtime/delivery/mailbox.go | 37 | 是 |
| internal/runtime/deliverytag/manager.go | 298 | 是 |
| internal/runtime/deliverytag/cache.go | 56 | 是 |
| internal/runtime/deliverytag/types.go | 104 | 是 |
| internal/runtime/deliverytag/topology.go | 38 | 是 |
| internal/runtime/online/registry.go | 285 | 是 |
| internal/runtime/online/types.go | 79 | 是 |
| internal/runtime/online/delivery.go | 26 | 是 |
| internal/runtime/sequence/allocator.go | 41 | 是 |
| internal/runtime/messageid/snowflake.go | 32 | 是 |
| internal/runtime/channelid/person.go | 46 | 是 |
| internal/runtime/channelid/agent.go | 23 | 是 |
| internal/runtime/channelid/command.go | 27 | 是 |
| internal/runtime/channelid/request_subscribers.go | 64 | 是 |
| internal/runtime/userlimit/limiter.go | 210 | 是 |
| internal/observability/diagnostics/store.go | 351 | 是 |
| internal/observability/diagnostics/index.go | 202 | 是 |
| internal/observability/diagnostics/event.go | 178 | 是 |
| internal/observability/diagnostics/sampler.go | 157 | 是 |
| internal/observability/diagnostics/tracking.go | 257 | 是 |
| internal/observability/diagnostics/sendtrace_adapter.go | 79 | 是 |
| internal/observability/diagnostics/tracectx/context.go | 92 | 是 |
| internal/log/zap.go | 223 | 是 |
| internal/log/config.go | 53 | 是 |
| internal/log/writer.go | 19 | 是 |

共 4734 行非测试代码，全部通读。另为理解上下文跨包阅读了 `internal/app/build.go`（投递装配 575-650、760-800）、`internal/app/delivery_lifecycle.go`（维护循环）、`internal/app/deliveryrouting.go`（tagDeliveryResolver / deliveryTagAuthority / distributedDeliveryPush）、`internal/app/committed_replay.go`、`internal/usecase/delivery/subscriber.go`、`internal/usecase/message/send.go`、`internal/access/api/diagnostics.go`、`internal/access/node/delivery_tag_rpc.go`、`internal/app/lifecycle/resource_stack.go`、`pkg/wklog/field.go`、`bwmarrin/snowflake@v0.3.0` 源码。本分片父包 `internal/FLOW.md` 已读，`internal/runtime/*` 下除 channelmeta/channelplane 外无 FLOW.md，本分片包内无 FLOW.md 需比对。

阶段 0 机械发现共 5 条（gosec 3 条 + staticcheck 2 条），全部在下方核实。

## 发现

### [P1] 1. deliverytag 缓存无淘汰调用方，EphemeralTag 令有界 tagCache 每条消息净增一个条目 —— 无界内存增长

- **位置**：`internal/runtime/deliverytag/cache.go:38-55`、`internal/runtime/deliverytag/manager.go:100-122`、`internal/runtime/deliverytag/manager.go:266-273`
- **类别**：资源泄漏（无界增长，类别 e）
- **代码**：

  cache.go 的淘汰实现：
  ```go
  func (c *tagCache) cleanupExpired(now time.Time, ttl time.Duration) int {
      if ttl <= 0 {
          return 0
      }
      removed := 0
      for key, tag := range c.tags {
          if now.Sub(tag.LastAccess) <= ttl {
              continue
          }
          delete(c.tags, key)
  ```

  manager.go 的构造：`internal/app/build.go:584` 用 `TTL: time.Minute` 构造该 Manager（有界设计成立的前提），但清理入口全仓只有测试调用：
  ```
  $ grep -rn CleanupExpired internal pkg --include='*.go'
  internal/runtime/deliverytag/manager_test.go:246:  removed := manager.CleanupExpired()
  internal/runtime/deliverytag/manager.go:266:       func (m *Manager) CleanupExpired() int {
  ```
  生产唯一写入点 `BuildEphemeralTag`（`internal/app/deliveryrouting.go:1274`，由 `tagDeliveryResolver.BeginResolve` → `leaderTagFromSnapshot` 在 `!source.ReusableTagState` 时对每条 committed 消息调用）：
  ```go
  tag := DeliveryTag{
      Key: m.newTagKey(),
      ...
      LastAccess: now,
  }
  m.cache.tags[tag.Key] = tag.clone()
  ```
- **触发路径**：
  1. 任一 `ChannelTypeTemp`（`internal/usecase/delivery/subscriber.go:155-160` 置 `ReusableTagState=false`）或带临时订阅者的 `ChannelTypeInfo`（同文件 195-208）频道消息被 committed，经 `internal/app/build.go:607` 装配的 resolver 在本节点 `BeginResolve`。
  2. `leaderTagFromSnapshot` 走 `BuildEphemeralTag`，向 `cache.tags` 插入一个新随机 key（16 字节 hex）的 DeliveryTag，其 `Partitions` 深拷贝了全部订阅者 UID 切片。
  3. 没有任何生产代码调用 `CleanupExpired`，`cache.tags` 单调增长；每个条目持有该消息的完整分区订阅者列表。大群/临时频道高频投递下按消息速率持续泄漏。
- **后果**：节点内存随已投递的 ephemeral-tag 消息条数线性增长，不会回收；包含订阅者 UID 数据，放大驻留集合。这是 `AGENTS.md` "节点内有界诊断/运行时"设计意图与实际的直接矛盾——`deliverytag` 明确实现了 TTL/淘汰机制却从未被调度。
- **建议**：把 `CleanupExpired` 挂到节点维护循环（与 delivery `SweepIdle` 同一个 lifecycle tick），或让 `BuildEphemeralTag` 在 put 前对 `cache.tags` 做容量上界 + LRU 淘汰。

### [P1] 2. 可重入窗口内并发 `routeAcked` 会把 actor 的 `pendingRouteCnt` 与 `resolvable` 堆推入永久不一致 —— 消息卡在 inflight、预算被吞

- **位置**：`internal/runtime/delivery/actor.go:289-313`（三个 OutsideLock 方法各自 `a.mu.Unlock()` → I/O → `a.mu.Lock()`）与 `internal/runtime/delivery/shard.go:41-59`（`routeAcked` 是独立入口，等待同一把 `a.mu` 后直接改共享状态）
- **类别**：并发（类别 a：锁外 I/O 的可重入窗口）
- **代码**：

  actor.go:308-313（窗口本身；`beginResolveOutsideLock`/`resolvePageOutsideLock` 同型）：
  ```go
  // pushOutsideLock pushes route batches without holding the actor lock.
  func (a *actor) pushOutsideLock(ctx context.Context, cmd PushCommand) (PushResult, error) {
      a.mu.Unlock()
      result, err := a.shard.manager.push.Push(ctx, cmd)
      a.mu.Lock()
      return result, err
  }
  ```

  shard.go:48-55（窗口期内可并发进入的另一入口）：
  ```go
  act.mu.Lock()
  err := act.handleRouteAck(ctx, RouteAcked{
      MessageID: binding.MessageID,
      Route:     binding.Route,
  })
  events := act.drainExpiredEventsLocked()
  offlineEvents := act.drainOfflineResolvedEventsLocked()
  act.mu.Unlock()
  ```
- **触发路径**（真实交错，Push 是节点间 RPC，窗口为毫秒级；`distributedDeliveryPush.Push` 会调用 `app.nodeClient` 发网络请求，见 `internal/app/build.go:620-632`）：
  1. 消息 M 在 actor A 上 `applyPush` → `pushOutsideLock` 解锁 `a.mu`，阻塞在跨节点 push RPC 上（几十 ms 常态）。此刻 `msg.PendingRouteCnt=2`（route r1、r2），`a.pendingRouteCnt=2`，`a.resolvable` 含下一消息 M'（seq 更小侧已排好堆）。
  2. 远端节点已收到帧并回了 ACK；`Manager.AckRoute`（`manager.go:84-90`，先在 `ackIdx.TakeRoute` 取出 binding）→ `shard.routeAcked` 阻塞等 `a.mu`。
  3. push RPC 返回，actor 重新加锁，处理 `result.Accepted`：r1 被 accept，`scheduleRetry(r1, attempt+1)` 成功 → r1 留在 `msg.Routes`、计数不减。actor 在下一次 `resumeResolvable`/`resolvePageOutsideLock`（`resolvePages` 循环体 151-190 每页都解锁）或另一个 `pushOutsideLock` 中再次放锁。
  4. `routeAcked(r1)` 拿到锁，`finishRoute(r1)`：`msg.PendingRouteCnt 2→1`、`a.pendingRouteCnt 2→1`、`delete(msg.Routes, r1)`、`ackIdx.RemoveRoute`（no-op，重复删除有 `!ok` 保护，安全）。至此逻辑一致——但 r1 的 retry-wheel 条目已调度且 `state.Attempt` 已固化在 wheel entry 里。
  5. retry tick 到期，`handleRetryTick`（actor.go:97-114）查 `msg.Routes[r1]` 不存在 → `return nil`，这条安全。**但第 3 步中若 result 里 r1 是 `Retryable` 而非 `Accepted`**（push 返回限流），`applyPush` 372-386 会 `state.Attempt = attempt` 并 `scheduleRetry`，而并发 ACK 的 `finishRoute` 在窗口两侧执行时会与这段的 `msg.Routes[route] = state` 交错：ACK 分支先 `delete`，push 分支随后用 re-validate `_, ok := msg.Routes[route]; if !ok { continue }` 跳过——这里恰好安全。
  6. **真正失守的路径在第 4 步之后**：`routeAcked` 尾部调用 `a.resumeResolvable(ctx)`（shard.go:73），它把 `a.resolvable` 堆里的 M' 取出、`resumeMessage` → `beginResolveOutsideLock` 再次解锁。若同一轮有两个 entry 路由到同一 actor（大群一条消息 push 多页、每页一次窗口），两个入口在窗口内交替进入 `nextResolvableMessage`：`ResolveInProgress` 标志只在 msg 粒度防重入，**不保护 `a.resolvable` 堆和 `a.pendingRouteCnt` 计数本身**。第二个入口在第一个入口 `resolvePages` 的 `routeBudgetRemaining()`（actor.go:579-585，读 `a.pendingRouteCnt`）判定 `remaining<=0` 返回 `nil` 后，第一个入口完成了页并 `ensureRouteState` 增计数，而第二个入口早已因旧快照返回——本轮没有任何入口再唤起 M'（`handleResolveRetryTick` 只在 wheel 里有 Resolve 条目时才会再触发，而 M' 尚未失败、没有条目）。
  7. 结果：M' 停在 `resolvable` 堆顶，`ResolveBegun=false`，除非该 channel 后续再来一条消息触发 `resumeResolvable`，否则不再被处理。`msg.PendingRouteCnt>0` 使 `isIdle`（actor.go:568-573）永假 → actor 永不被 `sweepIdle` 淘汰；`MaxInflightRoutesPerActor`（默认 4096）被这些僵尸 route 占用。
- **后果**：低概率但真实：消息投递停滞（对该 channel 的后续消息因 seq 堆序仍会处理，但 M' 自身永不上屏直至下一条消息偶然唤醒）、actor 级 inflight 预算慢性泄漏、`InflightRouteCount` 指标虚高。最坏形态是频道静默后该消息永久丢失实时投递（离线兜底依赖 `OfflineResolved` 事件，而事件只在该消息被 resolve 时产生）。
- **建议**：让所有入口在解锁 I/O 前后沿用同一条不变量——重新加锁后先校验"窗口内无其他入口进入过"（如世代计数器），或把 `routeAcked/routeOffline/processRetryTicks` 收敛为投递给 actor 所在 shard 的单一串行队列（`submit` 已经是串行入口，其余三个不是）。死代码 `pushResultStillCurrent`（发现 5）正是当初为此写的校验，恢复接线并扩展到 resolve 路径。

### [P2] 3. `dispatchObserved` 对同 seq 重复提交（跨重启/跨节点 replay）会重复投递：`completed` 集合仅 256 条且进程内有效

- **位置**：`internal/runtime/delivery/actor.go:51-62`、`internal/runtime/delivery/actor.go:602-627`
- **类别**：正确性 / 分布式一致性（类别 h：重放幂等）
- **代码**：
  ```go
  if a.hasSeenMessage(env.MessageID) {
      return nil
  }
  if a.nextDispatchSeq == 0 {
      a.nextDispatchSeq = env.MessageSeq
  }
  switch {
  case env.MessageSeq < a.nextDispatchSeq:
      return a.dispatchLate(ctx, env)
  ```
  与
  ```go
  const recentCompletedMessageCap = 256
  ...
  a.completed[messageID] = struct{}{}
  a.completedOrder = append(a.completedOrder, messageID)
  if len(a.completedOrder) <= recentCompletedMessageCap {
      return
  }
  evict := a.completedOrder[0]
  ```
- **触发路径**：`committedReplayer`（`internal/app/committed_replay.go:364-380`）按 durable cursor 重放。`Submit` 失败路径（如 `submit` 报错返回给 replay，`Manager.Submit` 透传 actor `handleStartDispatch` 错误）会导致 replay 下轮重提交同一 MessageID；此时若该 channel 在两次重放之间已流过 >256 条消息，`completed` 里的 MessageID 已被 FIFO 淘汰，`hasSeenMessage` 返回 false，`dispatch` 重新创建 inflight 并整条重推。异步派发路径（`asyncCommittedDispatcher`，`internal/app/build.go:728`）的 at-least-once 语义依赖这个 256 条的进程内去重窗口。
- **后果**：同一条 committed 消息对在线路由重复 push（客户端按 MessageID+Seq 幂等去重则表现为无害重复帧，不幂等的旧客户端表现为重复通知）；`ackIdx` 中同一 (uid,session,messageID) key 被 `Bind` 覆盖，不产生索引泄漏（`Bind` 是 map 赋值，安全）。
- **建议**：去重窗口改为按 channel seq 判断（`env.MessageSeq <= lastDispatchedSeq` 即丢弃或仅走 late 分支），而不是维护 ID 集合；late 分支里已有 seq 比较逻辑，缺的是完成态的最后 seq 记录。

### [P2] 4. `AckIndex.TakeSession/TakeSessionRoute` 反向索引只按 sessionID 键控，跨 UID 共享 sessionID 时会把其他 UID 的绑定一并删除

- **位置**：`internal/runtime/delivery/ackindex.go:110-159`
- **类别**：正确性（并发/一致性）
- **代码**：
  ```go
  func (i *AckIndex) TakeSessionRoute(uid string, sessionID uint64) []AckBinding {
      ...
      keys := i.reverse[sessionID]
      if keys.len() == 0 {
          return nil
      }
      out := make([]AckBinding, 0, len(keys.len()))
      for _, key := range keys.list(nil) {
          if key.uid != uid {
              continue
          }
  ```
  以及 `removeLocked`（240-254）删除单条后未重写 `i.reverse[binding.SessionID]` 的 `many` map 时依赖 `sessionBindings.remove(key)` 值语义生效——`sessionAckKeys` 是值类型，`remove` 内部对 `s.many` 的 `delete` 作用于共享底层 map，但 `case 1` 分支把 `s.many=nil` 写进了**局部副本**：
  ```go
  func (s *sessionAckKeys) remove(key ackKey) {
      if s.many != nil {
          delete(s.many, key)
          switch len(s.many) {
          case 0:
              s.many = nil
          case 1:
              for remaining := range s.many {
                  s.single = remaining
                  s.singleSet = true
                  s.many = nil
  ```
- **触发路径**：会话 A 有两条绑定（`many` 形态），`removeLocked` 删除其一：`remove` 里 `delete(s.many, key)` 落在共享 map 上生效，但 `case 1` 的收缩（`s.single=...; s.many=nil`）只改了局部副本 `sessionBindings`，且 `removeLocked` 的 246-252 行只在 `len()==0` 时写回。此后 `i.reverse[sid]` 仍是 `many={剩余1条}` 的副本视图？——不：`case 1` 分支里 `s.many = nil` 只影响副本，原 `i.reverse[sid].many` map 仍含 1 条。下次 `addReverseLocked` 走 `add()` 时 `s.many != nil` 成立，继续往共享 map 加——功能仍正确，但 `singleSet` 收缩永远丢失，`findMessage`/`list` 始终走 `many` 慢路径，且 `TakeSession` 中 `delete(i.reverse, sessionID)` 前若条目属于另一 UID 的共享 sessionID（`reverse` 以 sessionID 全局键控，而 `SessionClosed` 的调用方 `Manager.SessionClosed` manager.go:92-100 传的是 `cmd.UID+SessionID`），`TakeSessionRoute` 会把同 sessionID 下其他 UID 的 `entries` 删掉却因 `key.uid != uid` 不返回它们——绑定被静默删除。
  sessionID 在本系统由 gateway 连接生成、全局唯一（`online.MemoryRegistry` 以 sessionID 为全局键），因此跨 UID 碰撞在当前调用图下不可达；但 `TakeSession(sessionID)`（110-129）被暴露为无 UID 过滤版本，一旦新的调用方传入非唯一 ID 即触发。按"暂无已知触发者"定 P2。
- **后果**：若 sessionID 语义变化，出现静默删除他人绑定 → 对方消息的 ACK 找不到 binding，`Manager.AckRoute` 直接 return nil（manager.go:85-88）→ 路由按超时重试直至 expire，产生假 RouteExpired。
- **建议**：reverse 键改为 `(uid, sessionID)` 复合键，或删除 `TakeSession` 无 UID 变体；顺带修复 `remove` 的值语义收缩丢失问题。

### [P2] 5. gosec G115：`manager.go:162` `hasher.Sum32()%uint32(len(m.shards))` 与 `snowflake.go:31` `uint64(g.node.Generate())` —— 均为误报，但 shard 散列在每消息热路径上付出可避免的分配+接口调用

- **位置**：`internal/runtime/delivery/manager.go:155-163`、`internal/runtime/messageid/snowflake.go:30-32`
- **类别**：性能（类别 g）
- **代码**：
  ```go
  hasher := fnv.New32a()
  _, _ = hasher.Write([]byte(key.ChannelID))
  _, _ = hasher.Write([]byte{key.ChannelType})
  return m.shards[hasher.Sum32()%uint32(len(m.shards))]
  ```
  ```go
  func (g *SnowflakeGenerator) Next() uint64 {
      return uint64(g.node.Generate())
  }
  ```
- **触发路径/核实**：
  - `manager.go:162`：`int→uint32` 的被转换值是 `len(m.shards)`，恒正且极小，无溢出。**误报**。但 `shardFor` 每条 committed 消息、每条 ACK 都调用（`Manager.Submit`/`AckRoute`/`SessionClosed`），`fnv.New32a()` 返回接口 + `cannot inline (*Manager).shardFor: function too complex`（本机 `go build -gcflags=-m` 输出），热路径每消息一次堆分配。同包 `userlimit.shardIndex` 用同样模式（`limiter.go:206-210`，256 分片），同样不可内联。建议换 FNV 常量运算内联实现。
  - `snowflake.go:31`：snowflake `ID` 是 int64 位模式（63 bit 符号位恒 0：time(41)|node(10)|step(12)），`bwmarrin/snowflake@v0.3.0` 的 `Generate()` 只在位域内拼装，值恒非负。`uint64()` 转换无损。**误报**。
- **后果**：无正确性影响；shard 散列每消息一次额外分配在 10万 msg/s 量级可测。
- **建议**：内联 FNV-1a 常量实现替换 `fnv.New32a()`。

### [P3] 6. staticcheck U1000 ×2：`(*actor).pushResultStillCurrent`（丢失的再校验守卫）与 `(*tagCache).getTag`

- **位置**：`internal/runtime/delivery/actor.go:315-318`、`internal/runtime/deliverytag/cache.go:22-28`
- **类别**：架构（死代码）
- **代码**：
  ```go
  func (a *actor) pushResultStillCurrent(msg *InflightMessage, route RouteKey) bool {
      _, ok := msg.Routes[route]
      return ok
  }
  ```
  ```go
  func (c *tagCache) getTag(key string) (DeliveryTag, bool) {
      tag, ok := c.tags[key]
      if !ok {
          return DeliveryTag{}, false
      }
      return tag.clone(), true
  }
  ```
- **触发路径**：`git log -S pushResultStillCurrent`：该函数在 `6a2c6553 perf(delivery): run actor io outside locks` 引入时**曾是活跃守卫**（`applyPush` 对 `result.Accepted`/`Retryable` 逐 route 调用），在 `11f6ed42 perf: inline delivery route state` 中被内联展开为 `state, ok := msg.Routes[route]; if !ok { continue }` 后弃用。当前 `applyPush` 的内联版本等价，故**此条不是缺陷**，但它是发现 2 的注脚：作者意识到解锁窗口需要 route 存活性校验并实现过，resolve 路径（`resolvePages`）的对应窗口只有 `a.inflight[msg.MessageID] != msg` 粗校验，没有等价的细粒度守卫。
  `getTag` 则是 `Manager.LookupTag`（manager.go:226-239）手写展开后的孤儿；注意 `Manager.LookupTag` 用 `m.mu.Lock()` 写 `LastAccess`，而 `CurrentRef` 用 `m.mu.RLock()`——前者用写锁做读操作，是恢复 `getTag` 时可一并修正的点。
- **后果**：无运行时影响；`pushResultStillCurrent` 的存在暗示防御不完整的窗口（见发现 2）。
- **建议**：删除 `getTag`；`pushResultStillCurrent` 要么删除要么按发现 2 恢复为完整守卫。

### [P3] 7. gosec G301：`internal/log/zap.go:22` 日志目录 `0o755` —— 配置默认值兜底使其不可达，误报

- **位置**：`internal/log/zap.go:22`
- **类别**：安全（权限）
- **代码**：
  ```go
  if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
      return nil, fmt.Errorf("create log dir: %w", err)
  }
  ```
- **触发路径**：唯一调用方 `internal/app/build.go:79`，`cfg.Log.Dir` 来自应用配置。`internal/app/config.go:1518-1522` 对 Compress/Console 做默认注入，日志目录默认 `./logs`（config.go 默认值链），无任何路径把 Dir 设为含敏感文件的既有目录。755 目录本身可被同组用户读文件名，但日志内容敏感性低（项目日志不含 payload，见 `wklog` 字段集）。**误报**，记录备查。

### [P2] 8. `internal/usecase/message/send.go` 的限流拒绝把 `Decision.Reason` 之外的 `RetryAfter` 丢弃，且 userlimit `MaxBuckets` 打满后对新用户一刀切拒绝（含首条消息）

- **位置**：`internal/runtime/userlimit/limiter.go:118-126`
- **类别**：健壮性（类别 c/f 边界）
- **代码**：
  ```go
  b := shard.buckets[req.UID]
  if b == nil {
      if l.cfg.MaxBuckets > 0 && l.buckets.Load() >= int64(l.cfg.MaxBuckets) {
          return Decision{Allowed: false, RuleName: l.cfg.RuleName, Reason: ReasonBucketLimit, RetryAfter: l.retryAfter(1)}
      }
      b = &bucket{tokens: float64(l.cfg.Burst), lastRefill: now, lastSeen: now}
      shard.buckets[req.UID] = b
      l.buckets.Add(1)
  }
  ```
- **触发路径**：`MaxBuckets` 配置了上界且 EvictIdle 因 IdleTTL 过长清理不及 → 新用户第一次发送直接 `ReasonBucketLimit` 拒绝，返回给客户端的语义与限流（rate_exceeded）混同（send.go:190 透传）。若运维把 `UserRateLimitMaxBuckets` 设小于日活用户数，表现为"新用户永久发不出消息"，且 `RetryAfter` 无意义。
- **后果**：可用性问题，可由配置错误触发；代码本身按设计工作。
- **建议**：`ReasonBucketLimit` 建议在 usecase 层映射为独立的错误码/告警，而不是与 rate_exceeded 同路。

### [P3] 9. `internal/log/config.go:45-52` `withDefaults` 的 `c == (Config{})` 全等比较在显式设置任一字段后同时关闭 Console/Compress 默认 —— 与 app 层默认注入重复且语义脆弱

- **位置**：`internal/log/config.go:44-52`
- **类别**：健壮性
- **代码**：
  ```go
  if !c.Console {
      // Preserve explicit false; default true only when zero-value config is used.
      c.Console = c == (Config{})
  }
  if !c.Compress {
      c.Compress = c == (Config{})
  }
  ```
- **触发路径**：调用方只设置 `Level:"debug"`、其余留零值（`internal/app/build.go:79-88` 是全字段显式传入，当前不触发；但包是导出的，任何只传 Level 的新调用方会得到 Console=false、Compress=false，与注释"zero-value config"的字面语义相反——设置了 Level 的配置不是零值，却走到这里时 `c.Console` 已为 false 且不满足 `c == Config{}`）。实际当前生产不可达，属潜在 API 陷阱。
- **后果**：仅未来调用方受影响。
- **建议**：改用显式 `consoleSet`/`compressSet` 标志（app 层 config.go:1518-1522 已经这么做了，包内这套是残留的第二套机制）。

### [P2] 10. `RetryWheel` 是带互斥的有序切片而非时间轮，`PopDueInto` 每次 tick 全堆扫描归并——但 `processRetryTicks` 持 `retryMu` 串行处理所有到期项，单 shard 内重试风暴会阻塞其他 channel 的 retry tick

- **位置**：`internal/runtime/delivery/shard.go:81-106`、`internal/runtime/delivery/retrywheel.go:36-58`
- **类别**：性能（类别 g）
- **代码**：
  ```go
  func (s *shard) processRetryTicks(ctx context.Context) error {
      s.retryMu.Lock()
      defer s.retryMu.Unlock()
      due := s.wheel.PopDueInto(s.manager.clock.Now(), s.retryScratch[:0])
      ...
      for _, entry := range due {
          act := s.actor(ChannelKey{ChannelID: entry.ChannelID, ChannelType: entry.ChannelType})
          ...
          act.mu.Lock()
          err := act.handleRetryEntry(ctx, entry)
  ```
- **触发路径**：一个 shard（默认 `deliveryShardCountForParallelism(GOMAXPROCS)` 分片，见 build.go）内某大群频道 push 失败产生数千 route retry entry（每 route 一条），tick 到期时循环逐个 `handleRetryTick`——每次含 `applyPush` → `pushOutsideLock` 的**同步网络 I/O**（`distributedDeliveryPush.Push`）。同 shard 的其他 channel 的 retry 延迟被这个串行循环拖长；`ProcessRetryTicks` 在 manager.go:102-109 逐 shard 串行，全部 tick 每 200ms（delivery_lifecycle.go:14）跑一轮，最坏情况一轮超时导致下轮 entry 堆积。
- **后果**：重试路径的尾延迟放大；无正确性问题（wheel 与 actor 锁序固定：`retryMu → s.mu → act.mu`，shard.go:27-31 是 `s.mu → act.mu`，一致，无死锁）。
- **建议**：`handleRetryEntry` 里的 push 已在窗口外执行是好事，但按 actor 分组批量 push、或给 processRetryTicks 加时间预算（本轮只处理 N 条）可切断放大。

### [P3] 11. `internal/runtime/delivery/mailbox.go`（37 行）整个文件是死代码

- **位置**：`internal/runtime/delivery/mailbox.go:1-37`
- **类别**：架构（死代码）
- **代码**：
  ```go
  type mailbox struct {
      limit  int
      events []any
  }

  func (m *mailbox) push(event any) bool {
      if m == nil {
          return false
      }
  ```
- **触发路径**：`grep -rn "newMailbox\|mailbox" internal/runtime/delivery --include='*.go'` 仅命中自身与 `mailbox_test.go`。无任何生产引用（pkg/channel 的 execution_pool mailbox 是另一套同名无关实现）。staticcheck 未报是因为测试文件引用了它。
- **后果**：无；维护噪音。
- **建议**：连同其测试删除。

### [P3] 12. `diagnostics.Store.Query` 空条件查询（无索引键）在 RLock 内全 ring 拷贝 5 万事件，再在锁外逐条过滤——`/debug/diagnostics/events` 仅凭 stage 就能触发

- **位置**：`internal/observability/diagnostics/store.go:126-158`、`internal/observability/diagnostics/index.go:67-90`
- **类别**：性能 / DoS 面（类别 g）
- **代码**：
  ```go
  s.mu.RLock()
  ids := append([]uint64(nil), s.index.lookup(q)...)
  candidates := s.candidateEventsLocked(ids)
  if len(ids) == 0 && q.Stage != "" {
      candidates = s.stageCandidateEventsLocked(q.Stage)
  }
  if len(ids) == 0 && q.Stage == "" {
      candidates = s.retainedCandidateEventsLocked()
  }
  ...
  s.mu.RUnlock()
  ```
  `retainedCandidateEventsLocked`（188-199）遍历整个 ring 并**拷贝每个 Event**（Event 含 20+ 字段、多个 string）。
- **触发路径**：`GET /debug/diagnostics/events?limit=500`（internal/access/api/diagnostics.go:58-70，`DebugAPIEnabled` 开启时注册）不带索引键 → 拷贝 50,000 个 Event（每个含 TraceID/ChannelKey/ClientMsgNo/Error 等 string，数百字节）→ 约 10-30MB 瞬时分配 + O(n) 过滤。诊断 API 虽有 debug 开关且默认关闭（config.go:1382-1383），开启后任何持有 HTTP 访问权的人可反复触发造成 GC 压力。
- **后果**：瞬时分配放大；有界（单次），无泄漏。
- **建议**：无键查询直接在 ring 上流式过滤，或拒绝无键全量查询。

## 已排除的候选项

- `internal/runtime/online/registry.go`（整体）— 侦察认为 bucket 清理正确。复核确认：`removeActiveIndexes`（232-249）在 `bucket.count==0` 时 `delete(r.byGroup, ...)`，`Register` 重注册同 sessionID 时先 `removeActiveIndexes(existing)` 再 `addActiveIndexes`，`digest` 用 XOR 增减对称、`count` 与 `conns` map 始终同步；`MarkClosing` 只从 active 索引摘除、保留 bySession 条目（有界于真实连接数）。`routeFingerprint` 的 `writeString` 每次分配（逃逸分析确认 `([]byte)(s) escapes`），但该函数只在 Register/Unregister 调用，非热路径。**无界增长不成立，确认排除**。
- `internal/runtime/delivery/manager.go:162` — gosec G115（int→uint32），`len()` 恒正极小，误报（发现 5 有详述）。
- `internal/runtime/messageid/snowflake.go:31` — gosec G115（int64→uint64），snowflake 位模式符号位恒 0，转换无损；`MaxNodeID=1023` 上界校验在 NewSnowflakeGenerator:19-21。误报。
- `internal/log/zap.go:22` — gosec G301，目录 0755；唯一调用方走应用默认配置，无敏感目录场景，误报（发现 7）。
- `internal/runtime/delivery/actor.go:315` — U1000 死函数本身无害，且 git 历史证实其守卫语义已被内联等价保留在 `applyPush`（发现 6）；resolve 路径缺少等价守卫是发现 2，不在此重复计。
- `internal/runtime/deliverytag/cache.go:22` — U1000 死函数，无调用方，无正确性影响（发现 6）。
- `internal/runtime/delivery/shard.go:sweepIdle`（108-121）— 在 `s.mu` 下遍历 actors 并对每个加 `act.mu`；锁序 `s.mu → act.mu` 与 `submit`（27-31）一致；`isIdle` 在持 `act.mu` 下判断 `len(a.inflight)>0` 防止淘汰有活儿的 actor；删除发生在解锁后、map 遍历中 delete 安全。**确认安全**。
- `internal/runtime/delivery/ackindex.go` — `sessionAckKeys` 值语义收缩丢失（many→single 不降级）只造成慢路径常驻，无正确性问题；`Take/TakeRoute` 先查后删在同一写锁内原子；`RemoveRoute` 重复删除有 `!ok` 保护（与 actor 的 `finishRoute` 重复调用兼容）。除发现 4 的防御性键控问题外无缺陷。
- `internal/runtime/delivery/retrywheel.go` — 堆序实现正确（siftUp/siftDown 边界、`RetryEntry{}` 置零防泄漏）；`PopDueInto` 复用 `retryScratch`，`clear(due)` 后归还。P2 性能点在发现 10，非正确性问题。
- `internal/runtime/sequence/allocator.go` — 逐行复核：`nextMessageID` 用 `atomic.Int64.Add(1)`，单调且并发安全，溢出到负数在 int64 耗尽前不可达（每 ns 分配 1 个需 292 年）；`NextChannelSequence` 持 mutex，uint32 溢出 wrap 到 0 在 42 亿条/频道内不可达。**但注意**：全仓唯一引用是 `internal/usecase/message/deps.go:16` 的类型别名 `type SequenceAllocator = sequence.Allocator`，`MemoryAllocator` 无任何生产构造点——该包当前实际是死代码（真实消息 seq 由 channel log 分配）。不构成运行时风险，不计发现。
- `internal/runtime/messageid/snowflake.go` — `snowflake.NewNode` 内部校验 node 范围；`Generate()` 持锁、同毫秒 step 递增、step 溢出自旋等到下一毫秒，单调性由库保证；uint64 转换无损。**P0 候选排除**。
- `internal/runtime/channelid/person.go:12-13` — `crc32.ChecksumIEEE([]byte(uid))` 两次分配（逃逸分析确认 escapes），但 Normalize/Encode 只在发送与查询入口调用，非每帧热路径。**排序方向正确性**（leftHash>rightHash 决定拼接序、相等时按字典序打破平局）保证两端独立编码得到同一 channelID，除非 CRC32 碰撞（概率 2^-32，且碰撞导致的是双方各自算出不同 channelID——这是既有协议设计权衡，触发需刻意构造 UID 对，不构成实际缺陷，记录备查）。
- `internal/runtime/channelid/request_subscribers.go:55-57` — FNV-64 对 subscriber 列表哈希出 temp channel ID，`strings.Join(normalized, ",")` 在 UID 含逗号时可碰撞（["a,b"],["a"],["b"]）——但 channelID 只用于内部 temp 频道派生且订阅者列表以 `Subscribers` 字段随请求传递，碰撞只意味着两个不同请求共享 temp channel ID，投递仍按 `Subscribers` 精确列表走（`RequestScopedUIDs`），不产生错投。记录备查，不计发现。
- `internal/runtime/userlimit/limiter.go` — 桶数学正确：`refill` 有 `now.After(lastRefill)` 防钟回拨重复注水、`math.Min` 封顶 Burst；`AllowSend` 持分片锁读写 tokens 原子；`MaxBuckets` 检查与 `l.buckets.Add(1)` 非原子（两个 UID 并发可短暂超 MaxBuckets 1-2 个）， benign。Janitor 的 stop channel 由 `cleanup.Push`（build.go:778-780）保证关闭；`EvictIdle` 计数用 `Add(-evicted)` 与 `AllowSend` 的 `Add(1)` 有微弱竞态（evict 遍历时新 bucket 尚未 lastSeen 更新即被计数），导致 `buckets` 计数可能少计 → MaxBuckets 放松，无泄漏方向。均不构成可触发缺陷。分片锁 256 把、`shardIndex` 不可内联有一次分配（同发现 5 模式），量级太小不计。
- `internal/observability/diagnostics/store.go` — **有界性确认成立**：ring `events` 定长 `Capacity`，`Record` 覆盖前 `delete(s.byID, overwritten.id)`（87-89 行），`byID` 与 ring 同步上界；9 个有界索引各自 `maxEvents`（256）+`maxKeys`（10000）双界，`evictKeys` 覆盖所有索引。`nextID` uint64 溢出不可达。索引查询返回的 id 可能已被 ring 淘汰，`candidateEventsLocked` 的 `byID` 存在性检查兜住。**排除"bound is not real"疑虑**；仅发现 12 的查询放大问题。
- `internal/observability/diagnostics/sampler.go:143-156` — `keepByRate` 的 `(n * 2_654_435_761) % 1_000_000`：n 从 1 递增，uint64 乘法溢出 wrap 使槽位序列在 2^64/模数 后非严格均匀，但对采样率估计影响可忽略（确定性分层采样，实际分布良好）。排除。
- `internal/observability/diagnostics/tracking.go` — 规则数上界 `DefaultMaxTrackingRules=100` + `pruneLocked` 每次 Add/List/Delete 时清理过期，`snapshot` 用 atomic.Value 发布、Keep 无锁。有界且正确。排除。
- `internal/log/zap.go` — 三个 level 分文件 writer + 可选 debug core；lumberjack 轮转参数全部来自配置且有默认；`Sync` 对 stdout 类错误做了 errno 过滤。无发现。
- `internal/observability/diagnostics/tracectx/context.go` — `newTraceID` 的 `rand.Read` 失败回退 `strings.Repeat("0",31)+"1"` 是固定值，理论上所有请求共享同一 traceID——但 crypto/rand 失败在 Linux/macOS 实际不可发生，且回退值可被 `ValidateHeaderTraceID` 通过。理论性排除。
- 九类检查中 (b) context：`resolvePages`/`applyPush` 均透传 ctx 到 resolver/pusher，无 Background 丢弃；diagnostics 的三处 `context.Background()` 是 nil-ctx 兜底，安全。(d) 资源：delivery 包无 Pebble iter/文件句柄；`processRetryTicks` 的 scratch 归还正确。(i) 分层：`internal/runtime/*` 本分片 9 个包 import 检查（grep 见阶段 2 记录）零命中 access/usecase/app/gin/http，**分层约定全部合规**。

## 本分片整体评价

这一分片是全仓质量较高的部分：接口收敛、锁序一致、vet/staticcheck 干净、diagnostics 的有界设计真正落实了上界（ring + 双界索引 + 规则上限），`AGENTS.md` 的 `internal/runtime/*` 分层约定在全部 9 个包中零违规。最需要优先处理的是 **发现 1（deliverytag 缓存无淘汰调用方 + EphemeralTag 每消息净增）**：它不是理论问题——`ChannelTypeTemp`/临时订阅 `ChannelTypeInfo` 是正常运行路径，泄漏速率等于这两类频道的消息速率，且已实现的 `CleanupExpired` 只是没被接线，修复成本极低。其次是 **发现 2**：解锁 I/O 窗口的并发入口问题真实存在，`pushResultStillCurrent` 的死代码状态佐证了作者曾经防护过这个窗口；不过其触发需要跨节点 push 返回与 ACK 到达的毫秒级交错，实际发生率低，定 P1 偏保守。`internal/runtime/sequence` 包（含 `MemoryAllocator`）经逐行核对是无引用死代码，真实的序列分配在 channel log 层，本分片无 P0。
