# app 组合根：配置、装配、生命周期

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/app/build.go | 1720 | 是 |
| internal/app/config.go | 1796 | 是 |
| internal/app/app.go | 269 | 是 |
| internal/app/lifecycle.go | 579 | 是 |
| internal/app/lifecycle_components.go | 460 | 是 |
| internal/app/runtime_summary.go | 117 | 是 |
| internal/app/node_drain_state.go | 114 | 是 |
| internal/app/bootid.go | 18 | 是 |
| internal/app/errors.go | 10 | 是 |
| internal/app/lifecycle/component.go | 13 | 是 |
| internal/app/lifecycle/manager.go | 89 | 是 |
| internal/app/lifecycle/resource_stack.go | 67 | 是 |

跨包只读用于确认触发路径（不计入本分片发现）：`internal/usecase/cmdsync/pending.go`、`internal/usecase/conversation/sync.go`、`internal/runtime/deliverytag/{manager,cache,types}.go`、`internal/runtime/userlimit/limiter.go`、`internal/app/observability.go`、`pkg/gateway/auth.go`、`pkg/gateway/core/server.go`、`pkg/channel/handler/meta.go`、`pkg/cluster/{agent,controller_host,observation_sync}.go`、`internal/access/api/{routes,server}.go`、`cmd/wukongim/{main,config}.go`。

先读了 `internal/FLOW.md`（v2.1，2026-05-16），下文有两条与代码不符的记录。

---

## 发现

### [P0] 1. 客户端网关完全无认证，且配置层把唯一的开关做成了「设为 true 就报错」

- **位置**：`internal/app/config.go:811-813` + `internal/app/build.go:970-978`
- **类别**：安全
- **代码**：

```go
// internal/app/config.go:811
	if c.Gateway.TokenAuthOn {
		return fmt.Errorf("%w: gateway token auth requires verifier hooks", ErrInvalidConfig)
	}
```

```go
// internal/app/build.go:970
	app.gateway, err = gateway.New(gateway.Options{
		Handler:        app.gatewayHandler,
		Authenticator:  gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{TokenAuthOn: cfg.Gateway.TokenAuthOn, NodeID: cfg.Node.ID}),
		Observer:       gatewayObserver,
		DefaultSession: cfg.Gateway.DefaultSession,
		Transport:      cfg.Gateway.Transport,
		Listeners:      cfg.Gateway.Listeners,
		Logger:         app.logger.Named("gateway"),
	})
```

`pkg/gateway/auth.go:60` 是唯一的认证分支，只有 `opts.TokenAuthOn` 为真才会校验 token：

```go
		deviceLevel := frame.DeviceLevelSlave
		if opts.TokenAuthOn && !isVisitor(opts.IsVisitor, connect.UID) {
			if connect.Token == "" || opts.VerifyToken == nil {
				return &AuthResult{Connack: &frame.ConnackPacket{ReasonCode: frame.ReasonAuthFail}}, nil
			}
```

- **触发路径**：
  1. `Gateway.TokenAuthOn` 零值为 false，且 `ApplyDefaultsAndValidate` 在 811 行**拒绝**把它设成 true（`WK_GATEWAY_TOKEN_AUTH_ON=true` → 进程启动即失败）。因此生产上 `TokenAuthOn` 恒为 false。
  2. 组合根构造 `WKProtoAuthOptions` 时只传了 `TokenAuthOn` 和 `NodeID`——`VerifyToken`、`IsBanned`、`IsVisitor` 三个钩子全部为 nil。`internal/app` 按 `AGENTS.md` 是唯一组合根，没有其它地方能注入它们。
  3. 任意客户端 TCP 连到 `tcp-wkproto` 监听器，发一个 `ConnectPacket{UID: "任意受害者 UID", DeviceID: "x", DeviceFlag: APP}`，`auth.go:60` 的 `if` 直接跳过，`auth.go:94` 返回 `ReasonSuccess`。我用仓库自带的单节点集成用例实测确认了这条路径能连上：
     `GOWORK=off GOTOOLCHAIN=go1.23.4 go test -tags=integration -run TestAppStartAcceptsWKProtoConnectionAndStopsCleanly ./internal/app` → `ok 7.792s`，该用例正是「不带任何 token 直接 CONNECT 并断言 `ReasonSuccess`」。
  4. 连上后 `Handler.OnSessionActivate` 把这个 UID 注册进权威在线路由（`presence.Activate`），该 UID 的所有在线消息随后被投递到攻击者的连接；同时攻击者可以用这个 UID 调 `handleSend` 冒名发消息。
- **后果**：任意第三方可以冒充任意用户收发消息 —— 完整的身份伪造 + 消息读取。这不是「默认关闭的可选特性」，而是**配置层主动堵死了打开的路径**，运维无法通过任何配置修复。`FLOW.md:670` 自述「TokenAuth 当前不可用」，说明是已知状态，但代价是整个产品没有客户端认证。
- **建议**：把 verifier 钩子（`VerifyToken` / `IsBanned` / `IsVisitor`）做成组合根可注入的依赖并从 `Config` 驱动，然后把 811 行的硬拒绝改成「TokenAuthOn 为真但未注入 verifier 才报错」；在 verifier 落地前，至少让 `TokenAuthOn=false` 在启动日志里打 WARN 而不是静默通过。

---

### [P1] 2. `deliverytag.Manager` 缓存全仓无人调用淘汰函数，按频道无界增长

- **位置**：`internal/app/build.go:580-583`（构造点，设置了 TTL 却没有任何调度者）
- **类别**：资源泄漏 / 无界增长
- **代码**：

```go
// internal/app/build.go:580
	deliveryTagManager := deliverytagruntime.NewManager(deliverytagruntime.Options{
		LocalNodeID: cfg.Node.ID,
		TTL:         time.Minute,
	})
```

淘汰函数存在但没有生产调用方：

```go
// internal/runtime/deliverytag/manager.go:265
// CleanupExpired removes cold tags and any channel refs that point at them.
func (m *Manager) CleanupExpired() int {
```

```
$ grep -rn "CleanupExpired" --include="*.go" .
internal/runtime/deliverytag/manager_test.go:246:	removed := manager.CleanupExpired()
internal/runtime/deliverytag/manager.go:265:// CleanupExpired removes cold tags ...
internal/runtime/deliverytag/manager.go:266:func (m *Manager) CleanupExpired() int {
internal/runtime/deliverytag/cache.go:38:func (c *tagCache) cleanupExpired(...)
```

缓存里装的是全量订阅者列表：

```go
// internal/runtime/deliverytag/types.go:37
type NodePartition struct {
	NodeID uint64
	UIDs   []string
}
```

- **触发路径**：
  1. `deliveryTagManager` 被注入 `tagDeliveryResolver.tags`（`build.go:604`）和 `deliveryTagAuthority{tags: deliveryTagManager}`（`build.go:~1025`，node RPC `delivery_tag` 的服务端）。
  2. 每次该节点作为 channel leader 投递一条消息，`deliveryrouting.go:1299` 调 `r.tags.BuildLeaderTag(...)`；`manager.go:95` 执行 `m.cache.put(tag)`，把 `tags[tag.Key]` 和 `channelRef[tag.ChannelKey]` 写进两张 map。
  3. `cache.go:17-20` 的 `put` 只写不删。唯一的删除在 `cache.go:38 cleanupExpired`，而它的唯一入口 `CleanupExpired()` 在非测试代码里**零调用方** —— `delivery_runtime` 的 lifecycle tick（FLOW.md 4.4 描述的 "sweep idle actor"）扫的是 delivery actor，不碰 tag cache。
  4. 更糟的是 `manager.go:72-81`：当 `requestRequiresNewMaterialization`（订阅者版本变了 / 拓扑变了）或 `MintFreshKey` 成立时会换一个新 `key`，旧 key 的 `tags[oldKey]` 条目**永远留在 map 里**（`channelRef` 只覆盖一份，`tags` 是累加的）。所以增长维度不止「频道数」，还叠加「每个频道的 tag 换代次数」。
- **后果**：常驻内存随 `Σ(历史投递过的频道 × 订阅者 UID 数 × tag 换代次数)` 线性增长，进程生命周期内无回收。大群（万人群）+ 频繁成员变更的场景下是 GB 级泄漏，最终 OOM。构造时那句 `TTL: time.Minute` 是死配置，会让人误以为有过期机制。
- **建议**：在 `delivery_runtime` 的 lifecycle 周期 tick 里调 `deliveryTagManager.CleanupExpired()`，并给 `tags` map 加条数上限；`TTL` 提升为可配置项。

---

### [P1] 3. `Stop()` 用一个 5s context 串行跑完 17 个组件，预算被前面的 HTTP 优雅关闭吃光后，CMD pending 落盘被跳过

- **位置**：`internal/app/lifecycle.go:41-65`（共享预算）→ `internal/app/lifecycle_components.go:59-111`（停止顺序）
- **类别**：资源泄漏 / 正确性（数据丢失）
- **代码**：

```go
// internal/app/lifecycle.go:50
	a.stopOnce.Do(func() {
		a.started.Store(false)
		a.restoreDiagnosticsSink()
		stopCtx, cancel := context.WithTimeout(context.Background(), apiStopTimeout)
		defer cancel()
		err = errors.Join(
			a.stopLifecycleManager(stopCtx),
			...
```

`apiStopTimeout = 5 * time.Second`（`lifecycle.go:12`）。`Manager.Stop` 把**同一个** ctx 串行传给每个组件：

```go
// internal/app/lifecycle/manager.go:77
func (m *Manager) stopStartedLocked(ctx context.Context) []error {
	started := m.started
	m.started = nil
	var errs []error
	for i := len(started) - 1; i >= 0; i-- {
		if err := started[i].Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}
```

逆序第 1、2 位是两个 HTTP 优雅关闭，会一直阻塞到在途请求结束：

```go
// internal/access/api/server.go:293
func (s *Server) Stop(ctx context.Context) error {
	...
	return httpServer.Shutdown(ctx)
}
```

逆序第 10 位的 CMD updater 在 ctx 已过期时**提前 return，跳过落盘**：

```go
// internal/usecase/cmdsync/pending.go:157
	if wasRunning {
		if cancel != nil { cancel() }
		close(stopCh)
		select {
		case <-doneCh:
		case <-ctx.Done():
			return ctx.Err()      // <- savePendingFile() 永远到不了
		}
	}
	var flushErr error
	if wasRunning { flushErr = u.Flush(ctx) }
	saveErr := u.savePendingFile()
```

- **触发路径**：
  1. 节点收到 SIGTERM，`main.go` 的 defer 调 `App.Stop()`，建立唯一的 5s `stopCtx`。
  2. 逆序停止：`manager` → `api` → `gateway` → `channel_retention` → `channel_migration` → `committed_replay` → `committed_dispatcher` → `delivery_runtime` → `plugin_runtime` → `cmd_conversation_updater` → …（顺序见 `lifecycle_components.go:59-110` 的追加序）。
  3. 只要 `/manager/*` 或 `/api/message/send` 上有一个慢请求在途（durable send 要等 channel log quorum commit；`Gateway.SendTimeout` 默认 20s，`config.go:1755`），`httpServer.Shutdown(stopCtx)` 就会一直等到 5s 预算耗尽。
  4. 轮到 `cmd_conversation_updater` 时 `stopCtx` 已 Done。此时 `flushLoop` 很可能正卡在 `u.Flush(ctx)` 里的 `u.store.UpsertCMDConversationStates(ctx, states)`（一次跨节点 slot leader RPC，`pending.go:333`），`doneCh` 未就绪、`ctx.Done()` 就绪 → `select` **确定性**走 `return ctx.Err()`。
  5. `u.savePendingFile()` 从未执行，内存里未 flush 的 CMD conversation intent 全部丢失。
- **后果**：`FLOW.md:666`（「graceful stop 可恢复未 flush pending」）和 `FLOW.md:633` 的停止契约在带载关停时静默失效。受影响用户的 `/message/sync`（CMD 离线同步）读不到这批会话游标，表现为离线消息「少了一段」。这是**优雅关停下的数据丢失**，不是 kill -9。
  补充：磁盘状态本身是安全的 —— `closeChannelLogDB / closeRaftDB / closeWKDB`（`lifecycle.go:535-578`）不接受 ctx，在 lifecycle manager 之后无条件执行，Pebble 不会被饿死、锁文件不会残留。被饿死的只有接受 ctx 的组件停止逻辑。
- **建议**：每个组件独立分配停止预算（作者在 `lifecycle.go:330` 给 `conversation_active_hints` 派生了 1s 子超时，说明已意识到这个风险，只是没有推广到其余 16 个组件）；同时把 `cmdsync` 的 `savePendingFile()` 移到 `defer`，让它在任何提前返回路径上都执行。

**同时说明 brief 提到的 `docs/development/CODE_QUALITY.md` 三条记录的现状**：第一条（共享 5s 停止预算）**仍然成立**，即本条发现；第三条（`isSenderDeliveryRoute` 重复 `return`）brief 已核实不再存在，我复核同意。该文档属于部分过期状态。

---

### [P1] 4. 三个后台资源只登记在「构建失败清理栈」上，构建成功后被 `Release()`，`Stop()` 从不释放

- **位置**：`internal/app/build.go:256-262`、`internal/app/build.go:755-762`、`internal/app/build.go:778-781`，释放点缺失于 `internal/app/lifecycle.go:41-65`
- **类别**：资源泄漏 / 并发
- **代码**：

```go
// internal/app/build.go:256  —— discovery 地址变更回调
		cancelDiscoveryWatch := dynamicDiscovery.OnAddressChange(func(nodeID uint64, _, _ string) {
			app.dataPlanePool.ClosePeer(nodeID)
		})
		cleanup.Push("data-plane discovery watch", func() error {
			cancelDiscoveryWatch()
			return nil
		})
```

```go
// internal/app/build.go:755  —— delivery ack 批量通知器
		ackBatcher := newDeliveryAckBatchNotifier(app.nodeClient, deliveryAckBatchNotifierOptions{...})
		cleanup.Push("delivery ack batcher", func() error {
			ackBatcher.Close()
			return nil
		})
		deliveryNotifier = ackBatcher
```

```go
// internal/app/build.go:778  —— 限流器 janitor goroutine
		stopLimiter := limiter.StartJanitor(userRateLimitJanitorInterval(cfg.Message.UserRateLimitIdleTTL))
		cleanup.Push("user send rate limiter", func() error {
			stopLimiter()
			return nil
		})
```

构建成功时整个栈被交还：

```go
// internal/app/build.go:~981
	cleanup.Release()
	return app, nil
```

```go
// internal/app/lifecycle/resource_stack.go:59
func (s *ResourceStack) Release() {
	s.mu.Lock(); defer s.mu.Unlock()
	if s.closed { return }
	s.released = true
	s.closers = nil          // <- 所有 closer 被丢弃
}
```

- **触发路径**：
  1. `build()` 成功 → `cleanup.Release()` 把 15 个 closer 全丢掉。其中 12 个在 `Stop()` 里有等价的显式释放（logger/dashboard/diagnostics sink/metadb/raftDB/channelLogDB/dataPlanePool/dataPlaneClient/replicaExecutionPool/isrTransport/isrRuntime/channelLog，见 `lifecycle.go:53-62` 与 `545-569`）。
  2. 剩下这 3 个在全仓**没有任何其它释放点**：

```
$ grep -rn "ackBatcher\|stopLimiter\|cancelDiscoveryWatch" --include="*.go" internal pkg | grep -v _test
internal/app/build.go:256  internal/app/build.go:260
internal/app/build.go:755  internal/app/build.go:760  internal/app/build.go:763
internal/app/build.go:778  internal/app/build.go:780
```

  3. `StartJanitor` 是一个只靠 `stop` channel 退出的 goroutine + ticker：

```go
// internal/runtime/userlimit/limiter.go:151
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C: l.EvictIdle(now)
			case <-stop: return
			}
		}
	}()
	return func() { close(stop) }
```

  `stopLimiter` 无人调用 → 该 goroutine 与 ticker 在 `Stop()` 后继续运行到进程退出。
  4. `ackBatcher` 的 `time.AfterFunc(n.flushDelay, ...)`（`delivery_ack_batcher.go:77`）同理：`Close()` 无人调用，`Stop()` 返回后仍可能触发 `flushNode` → `n.sender.NotifyAckBatch(ctx, ...)`，而 `n.sender` 是 `app.nodeClient`，其底层 `app.cluster` 在 `Stop()` 里已经关掉了 —— 这是一次 **stop 后的 use-after-stop RPC**；同时 batch 里已排队的 ack 被静默丢弃，因为 `Close()`（本该把它们 flush 出去）从未跑。
  5. `cancelDiscoveryWatch` 无人调用 → 回调闭包一直挂在 `DynamicDiscovery` 上，闭包体读的是 `app.dataPlanePool`，而 `closeChannelLogDB()`（`lifecycle.go:562`）会把它置 nil。
- **后果**：`Message.UserRateLimitEnabled=true` 时每个 App 实例泄漏一个 goroutine + ticker；`Delivery.AckBatchMaxWait>0` 时关停期丢失已排队的远端 ack 并对已关闭的集群发 RPC。生产是单 App 进程，影响局限在关停窗口；但 `internal/app` 的集成测试会反复 `New()`，泄漏会在测试进程内累积。核心问题是 **brief 点名的那条规则被违反了：build 构造的组件必须全部登记到关停栈**，这三个只登记在失败路径上。
- **建议**：把这三个 closer 也接进 `App` 的停止序列（或让 `ResourceStack.Release()` 把 closer 转交给 App 而不是直接丢弃），并把「构造即登记关停」做成 build 的强制约定。

---

### [P1] 5. 会话同步的批量 owner 分组「一个频道缺 leader 就整个请求失败」，且该频道可由客户端请求体指定

- **位置**：`internal/app/build.go:1497-1518`
- **类别**：正确性 / 可用性（远程可触发）
- **代码**：

```go
// internal/app/build.go:1507
		metasByKey, err := metas.BatchGetChannelRuntimeMetas(ctx, metaKeys)
		if err != nil {
			return nil, err
		}
		for _, metaKey := range metaKeys {
			meta, ok := metasByKey[metaKey]
			if !ok || meta.Leader == 0 {
				return nil, channel.ErrStaleMeta
			}
			grouped[meta.Leader] = append(grouped[meta.Leader], convKeysByMeta[metaKey])
		}
		return grouped, nil
```

调用方把错误一路抛到 HTTP 层：

```go
// internal/usecase/conversation/sync.go:51
	keys := filterCandidateKeys(candidates, query.ExcludeChannelTypes)
	latestByKey, err := a.facts.LoadLatestMessages(ctx, keys)
	if err != nil {
		return SyncResult{}, err
	}
```

- **触发路径**：
  1. 客户端 `POST /conversation/sync`，请求体带 `last_msg_seqs`（`internal/access/api/conversation_legacy_model.go:16` → `conversation_sync.go:33` → `SyncQuery.LastMsgSeqs`）。
  2. `addOverlayCandidates`（`internal/usecase/conversation/sync.go:117-134`）对 `LastMsgSeqs` 里的**每个** key 建候选；即使 `GetUserConversationState` 返回 `metadb.ErrNotFound`，也照样加入候选集（`overlay = true`）。不存在的频道不会被过滤掉。
  3. `LoadLatestMessages`（`build.go:1338-1360`）先本地探测：`loadLatestConversationMessage` → `cluster.Status(key)`。对本节点没有 runtime 的频道，`pkg/channel/handler/meta.go:132-137` 的 `metaForKey` 返回 `channel.ErrStaleMeta`，于是该 key 进入 `remoteKeys`。**这是多节点集群的常态路径，不是边角情况。**
  4. `loadRemoteLatestMessagesBatch` → `groupConversationKeysByOwner`。`f.metas` 是 `*metastore.Store`，确实实现了 `BatchGetChannelRuntimeMetas`（`pkg/slot/proxy/runtime_meta_rpc.go:118`），所以走的是上面那段批量分支。
  5. 只要有**一个** key 在权威元数据里不存在（客户端瞎填的频道、已删除的频道）或 `Leader == 0`（刚创建未 bootstrap、迁移中、leader repair 未完成），1513 行直接 `return nil, channel.ErrStaleMeta`，整批放弃。
  6. 错误抛回 `App.Sync` → HTTP 500。`LoadRecentMessagesBatch`（`build.go:1467-1490`）共用同一个分组函数，`msg_count>0` 时同样受影响。
- **后果**：单个坏 key 让该用户**整个会话列表拉不出来**（不是少一条，是一条都没有）。既可被客户端无意触发（本地缓存里留着已删除频道的 `last_msg_seqs`，此后会话同步永久 500），也可被主动触发 —— 公开 API 引擎无任何认证，攻击者可以对任意 UID 构造这样的请求。同函数的逐 key 回退分支（`build.go:1520-1529`）行为一致，所以没有降级路径。注意本地探测阶段是容错的（`ErrChannelNotFound` 被吞掉继续），远程分组阶段却是全或无 —— 同一模块内「宽松版」和「严格版」并存，严格版在主路径上。
- **建议**：把「解析不出 owner 的 key」从批次里剔除并继续（与本地分支对 `ErrChannelNotFound` 的处理对齐），让 `Sync` 返回能拿到的会话；确实需要区分时用逐 key 的部分失败结果，而不是整批 error。

---

### [P1] 6. 网关准入标志只由「状态跃迁事件」驱动，`/readyz` 有自愈回退而网关没有 —— 可形成「readyz 说 ready、网关拒绝所有连接」的黑洞节点

- **位置**：`internal/app/node_drain_state.go:93-114` + `internal/app/observability.go:532-561`
- **类别**：正确性 / 可用性
- **代码**：

```go
// internal/app/node_drain_state.go:93
func (a *App) observeNodeStatusChange(nodeID uint64, status controllermeta.NodeStatus) {
	if a == nil { return }
	if a.nodeDrainState != nil {
		a.nodeDrainState.Observe(nodeID, status)
	}
	if nodeID == a.cfg.Node.ID {
		a.updateGatewayAdmissionFromDrainState()
	}
}

func (a *App) updateGatewayAdmissionFromDrainState() {
	if a == nil || a.gateway == nil { return }
	accepting := false
	if a.nodeDrainState != nil {
		accepting = a.nodeDrainState.KnownNotDraining()
	}
	a.gateway.SetAcceptingNewSessions(accepting)
}
```

`/readyz` 的自愈回退**绕过了** `observeNodeStatusChange`：

```go
// internal/app/observability.go:545
func (a *App) refreshLocalNodeDrainState(ctx context.Context) {
	...
	nodes, err := a.cluster.ListNodes(queryCtx)
	if err != nil { return }
	for _, node := range nodes {
		if node.NodeID == a.cfg.Node.ID {
			a.nodeDrainState.Observe(node.NodeID, node.Status)   // <- 直接 Observe，不触发网关准入重算
			return
		}
	}
}
```

准入关掉后连接被直接切断：

```go
// pkg/gateway/core/server.go:360
func (s *Server) onOpen(listener *listenerRuntime, conn transport.Conn) error {
	if listener == nil || conn == nil { return nil }
	if !s.AcceptingNewSessions() {
		_ = conn.Close()
		return nil
	}
```

- **触发路径**：
  1. `build.go:71` 建 `nodeDrainState`（`known=false`），`build.go:980` 在 build 末尾调 `updateGatewayAdmissionFromDrainState()` → `Ready()` 因 `!known` 返回 `(false,"unknown")` → `SetAcceptingNewSessions(false)`。**网关出厂即拒绝所有连接**（`pkg/gateway/core/server.go:182` 的默认 `accepting=true` 被这里覆盖掉）。
  2. 唯一能把它打开的是 `OnNodeStatusChange(localNodeID, *, Alive)`。而该 hook 只在**真实跃迁**时发射，两条路径互斥：
     - controller leader 侧 `pkg/cluster/controller_host.go:696` 要求 `h.LeaderID() == h.localNode`，且只处理 `CommandKindNodeStatusUpdate` / `CommandKindOperatorRequest`（`controller_host.go:635-656`，`NodeJoin` 不产生事件），并且 `change.from == to` 时跳过（`controller_host.go:704`）。
     - agent 侧 `pkg/cluster/agent.go:121` 要求 `!a.cluster.isLocalControllerLeader()`。
  3. 正常单节点冷启动这条链路是通的（我实测 `TestAppStartAcceptsWKProtoConnectionAndStopsCleanly` 通过，7.8s）。但只要这一次事件丢失 —— 典型是 controller leader 在 `NodeStatusUpdate` 提交与 `emitCommittedNodeStatusChanges` 之间发生切换：旧 leader 在 `controller_host.go:696` 因 `LeaderID() != localNode` 短路不发，新 leader 的 agent 侧又因 `isLocalControllerLeader()` 为真被 `agent.go:121` 抑制 —— 该节点的 `known` 永远停在 false。
  4. 此后没有任何周期性重算：`updateGatewayAdmissionFromDrainState` 的调用点全仓只有 build 末尾和 `observeNodeStatusChange` 两处。而 `/readyz` 每次被探测都会走 `localNodeNotDraining` → `refreshLocalNodeDrainState` → `ListNodes` 拿到真实的 Alive → `Observe(...)` → `Ready()` 返回 true → **readyz 报 ready**。
- **后果**：节点对外声称 ready（K8s readiness probe 通过、LB 把它留在后端池里），但 `onOpen` 无条件 `conn.Close()` 掉每一个客户端连接 —— 一个只吸流量不提供服务的黑洞节点，且没有任何指标会指出矛盾（`AcceptingNewSessions` 只出现在 manager 的 runtime summary 里）。方向相反的风险同样存在：`Ready()` 的 `staleAfter=15s`（`node_drain_state.go:11`）在稳态集群下必然过期（稳态不产生状态跃迁），`/readyz` 靠回退自愈，网关准入没有对应机制。
- **建议**：让 `refreshLocalNodeDrainState` 走 `observeNodeStatusChange`（或在其后显式调一次 `updateGatewayAdmissionFromDrainState`），并给网关准入加一个低频的周期性重算，别把它绑死在「一次性跃迁事件」上。

---

### [P1] 7. 不安全默认值清单：公开 API 监听器无任何认证，而 metrics / health-details 默认开

- **位置**：`internal/app/config.go:1349-1362`（默认值）；`internal/app/config.go:817`（manager 认证校验）；`internal/app/build.go:930-947`（注入公开 API）
- **类别**：安全（信息泄露）
- **代码**：

```go
// internal/app/config.go:1349
	if !c.Observability.metricsEnabledSet {
		c.Observability.MetricsEnabled = true
	}
	if !c.Observability.networkEnabledSet {
		c.Observability.NetworkEnabled = true
	}
	if !c.Observability.healthDetailEnabledSet {
		c.Observability.HealthDetailEnabled = true
	}
	if !c.Observability.healthDebugEnabledSet {
		c.Observability.HealthDebugEnabled = false
	}
	if !c.Observability.Diagnostics.enabledSet {
		c.Observability.Diagnostics.Enabled = true
	}
```

```go
// internal/app/build.go:930
			MetricsHandler:           app.metricsHandler(),
			HealthDetailEnabled:      cfg.Observability.HealthDetailEnabled,
			HealthDetails:            app.healthDetailsSnapshot,
			Readyz:                   app.readyzReport,
			DebugEnabled:             cfg.Observability.HealthDebugEnabled,
			...
			DiagnosticsDebugEnabled:  cfg.Observability.Diagnostics.Enabled && cfg.Observability.Diagnostics.DebugAPIEnabled,
```

- **触发路径**：`cmd/wukongim/config.go:995` 把 `API.ListenAddr` 默认成 `0.0.0.0:5001`，所以公开 API 总是开着；该 engine 上除 CORS 外没有任何中间件、没有认证（brief 已核实 `internal/access/api/server.go:190-191`）。逐项核对暴露面如下：

| 配置项 | 默认 | 挂在哪个监听器 | 实际泄露什么 |
|---|---|---|---|
| `Observability.MetricsEnabled` | **true** | 公开 API `0.0.0.0:5001` `GET /metrics`（`routes.go:17-21`） | 全量 Prometheus 指标：在线连接数、各协议会话数、channel 活跃数/上限、delivery resolve/push 速率与失败、append 延迟、集群 controller 调用统计 —— 免费的容量与拓扑侦察面 |
| `Observability.HealthDetailEnabled` | **true** | 公开 API `GET /healthz/details`（`routes.go:11-13`） | **最严重的一项**：`observability.go:405-417` 逐 slot 输出 `leader_id`、`current_peers`（集群内部节点 ID 列表）、`healthy_voters`、`has_quorum`、`applied_epoch`；`observability.go:428-438` 输出 `alive/suspect/dead_nodes`、活跃 controller 任务分类计数、活跃迁移数；外加 `node_id`/`node_name`/`uptime_seconds`/在线连接数/存储健康。等于把整张集群拓扑和 raft 健康状况匿名公开 |
| `Observability.NetworkEnabled` | **true** | 不直接上公开 API（只喂 manager 的 network snapshot） | 本身不构成公开泄露，但打开了每包级别的本地网络观测开销；真正的风险是它随 manager 一起暴露（见下一行） |
| `Observability.Diagnostics.Enabled` | **true** | **不**上公开 API：`DiagnosticsDebugEnabled = Enabled && DebugAPIEnabled`，而 `DebugAPIEnabled` 默认 false（`config.go:1382`） | 仅在内存保留 50000 条诊断事件（`config.go:1364-1365`），经 manager 与 node RPC `diagnostics` 可读。默认值本身是安全的 —— 这一项是清单里唯一的「默认正确」 |
| `Observability.HealthDebugEnabled` | **false** | — | 默认正确 |
| `Manager.AuthOn` | **false**（零值，`config.go` 无任何默认赋值） | manager 监听器（需运维显式设 `WK_MANAGER_LISTEN_ADDR`） | `config.go:817` 的写法是 `if ListenAddr != "" && AuthOn { 校验 JWT/用户 }` —— 也就是说「开了 manager 但没开认证」这个组合**完全不校验、不告警**。此时节点管理、用户/系统 UID 增删、频道业务 CRUD、消息保留策略、插件管理、诊断查询全部无认证开放 |
| `Message.UserRateLimitEnabled` | **false**（零值） | — | 每 UID 发送令牌桶默认关闭。叠加发现 1（任意 UID 可连），开箱状态下没有任何发送速率保护 |

- **后果**：默认部署下，任何能连到 5001 的人可以完整枚举集群节点 ID、每个 slot 的 leader 与副本集、quorum 健康度和运行指标 —— 这正是打发现 1（冒名连接）和横向移动所需要的全部侦察信息。`Manager.AuthOn=false` 被静默接受则意味着运维一旦开了管理端口就等于开了无认证的集群控制面。
- **建议**：`HealthDetailEnabled` 默认改 false，或把 `/healthz/details` 与 `/metrics` 挪到独立的内网监听器；`config.go:817` 增加「`ListenAddr != "" && !AuthOn` 直接拒绝启动（或至少 WARN）」；把这张表固化进部署文档的安全基线。

---

### [P2] 8. `startPlugin` 是复合启动，后半段失败时前半段已启动的插件运行时不会被回滚，之后 `Stop()` 也跳过它

- **位置**：`internal/app/lifecycle.go:253-266` + `internal/app/lifecycle.go:413-428`
- **类别**：资源泄漏
- **代码**：

```go
// internal/app/lifecycle.go:253
func (a *App) startPlugin(ctx context.Context) error {
	if a.startPluginFn != nil { return a.startPluginFn(ctx) }
	if a.pluginRuntime != nil {
		if err := a.pluginRuntime.Start(ctx); err != nil {
			return err
		}
	}
	if a.pluginReceiveObserver == nil { return nil }
	return a.pluginReceiveObserver.Start(ctx)   // <- 这里失败，pluginRuntime 已经起来了
}
```

```go
// internal/app/lifecycle.go:413
func (a *App) stopPlugin(ctx context.Context) error {
	if !a.pluginOn.Swap(false) {
		return nil                               // <- pluginOn 从未置 true，直接 no-op
	}
```

- **触发路径**：
  1. `Plugin.Enable=true`。`pluginLifecycleComponent`（`lifecycle_components.go:298-312`）的 `start` 先调 `a.startPlugin(ctx)`，**成功后才** `a.pluginOn.Store(true)`。
  2. `pluginRuntime.Start(ctx)` 成功 —— 它已经扫描 `Plugin.Dir` 下的 `*.wkp` 并 `exec` 出插件子进程、绑好 unix socket（`FLOW.md:690`）。
  3. `pluginReceiveObserver.Start(ctx)` 失败（例如有界 worker 池初始化失败，或第 9 位启动时 ctx 已被上游组件拖超时）。
  4. `startPlugin` 返回 err → 组件 `start` 提前返回，`pluginOn` 保持 false → `Manager.startWithRollbackContext`（`lifecycle/manager.go:56-62`）只回滚 `m.started` 里的组件，**失败的这个组件不在其中**，不会被 Stop。
  5. `App.Start()` 返回 err。若调用方随后调 `Stop()`，`stopPlugin` 在 414 行被 `pluginOn` 门卫挡住，直接 no-op。
  6. 更糟的是 `cmd/wukongim/main.go:34-41`：`defer application.Stop()` 注册在 `Start()` 判错**之后**，Start 失败时直接 `return err` → `log.Fatal` → `os.Exit(1)`，defer 根本不跑。
- **后果**：插件子进程在父进程退出后被 init 收养，成为孤儿进程；unix socket 文件残留，下次启动 bind 冲突。同一模式适用于任何「一个 lifecycle component 内部串联多个真实启动」的场景，`startPlugin` 是当前唯一一处。
- **建议**：把 `plugin_runtime` 和 `plugin_receive_observer` 拆成两个独立的 lifecycle component（启停顺序天然正确），或在 `startPlugin` 内部对后半段失败做本地补偿（回滚已启动的 `pluginRuntime`）。

---

### [P2] 9. `controllerPeerIDs` 从静态配置推导对端集合，动态 join 模式下会拿到合成的 `MaxUint64` 伪节点 ID

- **位置**：`internal/app/build.go:1291-1300`（合成 ID）+ `internal/app/build.go:717,837`（消费点）
- **类别**：正确性 / 分布式
- **代码**：

```go
// internal/app/build.go:1291
func (c ClusterConfig) runtimeSeeds() []raftcluster.SeedConfig {
	seeds := make([]raftcluster.SeedConfig, 0, len(c.Seeds))
	for i, addr := range c.Seeds {
		seeds = append(seeds, raftcluster.SeedConfig{
			ID:   multiraft.NodeID(^uint64(0) - uint64(i)),   // 18446744073709551615, ...614, ...
			Addr: addr,
		})
	}
	return seeds
}
```

```go
// internal/app/build.go:717
		peerNodeIDs: controllerPeerIDs(cfg.Cluster.DerivedControllerNodes(), cfg.Cluster.runtimeSeeds()),
// internal/app/build.go:837
			ControllerPeerIDs: controllerPeerIDs(cfg.Cluster.DerivedControllerNodes(), cfg.Cluster.runtimeSeeds()),
```

消费端对每个 peer 硬失败：

```go
// internal/app/user_system_uid_cache.go:66
	for _, nodeID := range uniqueRemoteSystemUIDCacheNodes(u.localNodeID, u.peerNodeIDs) {
		var err error
		if add { err = u.remote.AddSystemUIDsToCache(ctx, nodeID, uids) } else { ... }
		if err != nil {
			return fmt.Errorf("sync system uid cache to node %d: %w", nodeID, err)
		}
	}
```

- **触发路径**：
  1. 动态 join 模式（`Cluster.Nodes` 为空、`Cluster.Seeds` 非空，由 `config.go:1030-1031` 判定）。
  2. `DerivedControllerNodes()`（`config.go:672-681`）遍历的是 `c.Nodes`，此时为空 → 返回空切片。
  3. `controllerPeerIDs`（`build.go:1019-1044`）于是只从 seeds 取 ID，拿到的全是 `runtimeSeeds()` 合成的 `^uint64(0)-i` —— 这些是给种子地址寻址用的占位 ID，**不是任何真实节点的 cluster node ID**。
  4. 管理端调 `AddSystemUIDs`：`user_system_uid_cache.go:36-41` 先本地写入成功，再 `broadcastSystemUIDCache` 对节点 `18446744073709551615` 发 RPC → 该 ID 不在 controller 成员表里 → 报错 → 整个 `AddSystemUIDs` 返回失败。
  5. 即使不看合成 ID：`peerNodeIDs` 在 `build()` 时从静态配置快照一次，**此后永不刷新**。动态 join 进来的第 3、4 个节点永远不在这个集合里，拿不到系统 UID 缓存广播。
- **后果**：动态 join 模式下系统 UID 管理接口恒定失败，且「本地已写入 + 广播失败」造成各节点系统 UID 缓存不一致（系统 UID 用于限流 bypass 和权限判定，不一致会导致同一 UID 在不同节点上行为不同）。静态 `Cluster.Nodes` 模式不受影响。
- **建议**：`peerNodeIDs` / `ControllerPeerIDs` 改为运行时从 `cluster.ListNodes` 动态解析，而不是 build 期从静态配置快照；至少在 `controllerPeerIDs` 里过滤掉 seed 合成 ID（它们不是可寻址的成员）。

---

### [P3] 10. FLOW.md 与代码不符：`managed_slots_ready` 超时

- **位置**：`internal/FLOW.md:267` vs `internal/app/lifecycle.go:15`、`internal/app/config.go:1192-1194`
- **类别**：架构 / 文档
- **代码**：

```go
// internal/app/lifecycle.go:11
const (
	apiStopTimeout                  = 5 * time.Second
	activeHintStopTimeout           = time.Second
	defaultDataPlaneDialTimeout     = 5 * time.Second
	defaultManagedSlotsReadyTimeout = 30 * time.Second
)
```

`FLOW.md:267` 写的是「`managed_slots_ready`（等待 Slot 就绪，超时 10s）」，实际默认 30s（`config.go:1192` 在未配置时填入 `defaultManagedSlotsReadyTimeout`，`lifecycle.go:526-532` 使用它）。
- **触发路径**：运维按文档假定冷启动最多阻塞 10s 来设 K8s `startupProbe`/`initialDelaySeconds`，实际多节点冷启动可阻塞到 30s，探针提前判失败并重启 Pod，形成重启循环。
- **后果**：启动编排配置错误。
- **建议**：把 FLOW.md 第 4.1 节改成 30s，并标注可由 `Cluster.ManagedSlotsReadyTimeout` 覆盖。

---

### [P3] 11. `SetCommitCoordinatorExplicitFlags` 静默丢弃 4 个参数中的 3 个

- **位置**：`internal/app/config.go:622-628`
- **类别**：架构 / 可读性
- **代码**：

```go
// SetCommitCoordinatorExplicitFlags records whether durable commit coordinator settings were explicitly configured.
func (c *ClusterConfig) SetCommitCoordinatorExplicitFlags(flushWindowSet, maxRequestsSet, maxRecordsSet, maxBytesSet bool) {
	if c == nil {
		return
	}
	c.commitCoordinatorFlushWindowSet = flushWindowSet
}
```

- **触发路径**：`cmd/wukongim/config.go:912` 认真地传了 4 个 explicit 标志，后 3 个被直接丢掉 —— `ClusterConfig` 里根本没有 `commitCoordinatorMaxRequestsSet` / `MaxRecordsSet` / `MaxBytesSet` 字段（`config.go:581-590` 的字段列表可验证）。
- **后果**：当前无功能损害（`config.go:1153-1160` 对这三项只拒绝负值，0 被合法地当作「不限」透传给 `ConfigureCommitCoordinator`）。但这个签名撒了谎：任何后续想给这三项加「显式设 0 与未设 0 语义不同」的逻辑（这正是本文件其余几十处 `xxxSet` 的通用模式）都会静默失效。
- **建议**：要么补齐三个 `...Set` 字段，要么把签名收窄成只接受 `flushWindowSet`。

---

### [P3] 12. 网关 `IsBanned` 钩子在全仓没有任何实现，是一个不可达的安全能力

- **位置**：`internal/app/build.go:972`（唯一可注入点，未注入）
- **类别**：死代码 / 安全
- **代码**：`pkg/gateway/auth.go:76` 有完整的封禁分支：

```go
		if opts.IsBanned != nil {
			banned, err := opts.IsBanned(connect.UID)
			if err != nil || banned {
				reason := frame.ReasonBan
				if err != nil { reason = frame.ReasonAuthFail }
				return &AuthResult{Connack: &frame.ConnackPacket{ReasonCode: reason}}, nil
			}
		}
```

但全仓只有定义和这一处使用：

```
$ grep -rn "IsBanned" --include="*.go" internal pkg | grep -v _test
pkg/gateway/auth.go:36:	IsBanned    func(uid string) (bool, error)
pkg/gateway/auth.go:76:		if opts.IsBanned != nil {
pkg/gateway/auth.go:77:			banned, err := opts.IsBanned(connect.UID)
```

- **触发路径**：组合根 `build.go:972` 构造 `WKProtoAuthOptions` 时不传 `IsBanned`，`internal/app` 又是唯一组合根，因此该分支恒不执行。
- **后果**：`frame.ReasonBan` 这条协议返回码在服务端永远不会产生。注意这里**不是**「管理端能封禁但网关不生效」—— `pkg/db/meta` 里只有频道级的 `Ban`/`SendBan`/`Disband`（`table_channel.go:17-21`），没有用户级封禁模型，所以是能力缺失而非绕过。但与发现 1 合起来看：网关层既没有认证也没有封禁，是完全开放的。
- **建议**：要么随发现 1 的 verifier 注入一并接上用户级封禁，要么删掉这个从未被接线的钩子，避免它看起来像一道存在的防线。

---

## 已排除的候选项

**阶段 0 机械扫描**：`grep 'internal/app/' /tmp/octo-gosec.txt` 命中 12 条、`/tmp/octo-staticcheck.txt` 命中 23 条，**全部落在本分片之外的文件**（`deliveryrouting.go`、`channelmigration.go`、`observability.go`、`manager_messages.go`、`manager_message_retention.go`、`delivery_ack_batcher.go` 及各 `_test.go`），属于单元 02/03。本分片的 10 个非测试文件 + `lifecycle/` 包在 gosec 与 staticcheck 上**零命中**，无 G115 需要甄别。（`delivery_ack_batcher.go` 的 SA4006/SA4017/U1000 我在发现 4 里只引用了它的生命周期登记面，代码质量本身留给对应单元。）

- `internal/app/build.go:1109-1114` + `config.go:1113` —— `ChannelBootstrapDefaultMinISR` 默认 2，单节点集群 `SlotReplicaN=1`，看起来会让频道永远无法 commit。实际 `internal/runtime/channelmeta/bootstrap.go:89` 有 `clampBootstrapMinISR(b.defaultMinISR, len(replicas))` 按副本数收敛，安全，误报。
- `internal/app/config.go:1627-1645` `storagePathsOverlap` —— 看起来像字符串前缀比较（会把 `/data/raft` 和 `/data/raft-snapshots` 误判为重叠），实际 `storagePathSegments` 先按路径分隔符切段再逐段比较，`raft` != `raft-snapshots` 第二段就返回 false。实现正确。
- `internal/app/config.go:1495-1499` —— `if !Enabled && !Set { =true } else if Enabled && !Set { =true }` 两个分支赋同一个值，等价于单条 `if !Set`。冗余但无行为差异，不构成缺陷。
- `internal/app/build.go:439-447` `repairProbeClient` 未见 `Close()` —— 读 `pkg/channel/transport/probe*.go:28-39` 确认 `ProbeClient` 只借用 `app.dataPlaneClient`，自身不持有连接或 goroutine，无需释放。
- `internal/app/lifecycle.go:53` 的 5s 预算是否会让 Pebble / raft log 落盘被截断 —— 不会。`closeChannelLogDB` / `closeRaftDB` / `closeWKDB`（`lifecycle.go:535-578`）都不接受 ctx，在 `stopLifecycleManager` 之后无条件执行到底，磁盘状态与锁文件安全。发现 3 的后果因此限定在「接受 ctx 的组件停止逻辑」，不含存储层。
- `internal/app/lifecycle.go:41-65` 重复调用 `Stop()` 的双关闭 panic —— `stopOnce sync.Once` + 各 `*On.Swap(false)` 原子门卫 + `ResourceStack` 的 `closed`/`released` 标志三重保护，幂等正确。
- `internal/app/lifecycle_components.go:59-110` 的启停顺序 —— 与 `FLOW.md:262-316` 逐项比对一致（含 `plugin_runtime` 早于 `delivery_runtime` 启动、晚于其停止；`cmd_conversation_updater` 早于 `delivery_runtime` 启动因而晚于其 drain 才保存 pending）。无偏差。
- `internal/app/build.go:256-262` 的 `cancelDiscoveryWatch` 闭包在 `dataPlanePool` 被置 nil 后触发导致 nil panic —— 顺序上 `cluster.Stop()` 先于 `closeChannelLogDB()` 置 nil，集群停后不再产生地址变更事件，写不出确定的交错。该句柄的泄漏本身已并入发现 4，panic 这一分支排除。
- `internal/app/build.go:656` `deliverySubmitDispatcher` 只含 `{committedDispatcher, committedReplayer}` 而 `committedDispatcher`（line 670）额外含 `pluginCommittedRouter` —— 两个 fanout 的订阅者集合不同看似是遗漏，但 `FLOW.md:692` 明确规定「PersistAfter 只在 committed owner 上执行，远端通过 `plugin_committed` RPC 路由」，node RPC `delivery_submit` 路径不该再触发一次插件副作用，否则会重复执行。设计一致，非缺陷。
- 网关准入在单节点冷启动下永久为 false —— 我实测 `TestAppStartAcceptsWKProtoConnectionAndStopsCleanly` 通过（7.79s），说明常规路径下 controller 会提交一次把本节点置 Alive 的 `NodeStatusUpdate` 跃迁。因此「常规启动即黑洞」被排除；保留下来的是发现 6 中「事件丢失后无任何重算路径、且与 `/readyz` 自愈不对称」这个可写出具体交错的收窄版本。

---

## 本分片整体评价

这是**真实生产路径**上的组合根（`cmd/wukongim/main.go:29` → `app.New(cfg)`），不是 v2 未上线代码，严重度按实际计。工程质量整体偏高：`lifecycle.Manager`/`ResourceStack` 的启动回滚与构建失败清理抽象干净，17 个组件的启停顺序与 `FLOW.md` 严格一致，`ApplyDefaultsAndValidate` 近 700 行校验相当细致（每个 tuning 项都区分「未配置」与「显式配零」），gosec/staticcheck 在本分片零命中。问题集中在两类系统性疏漏，而非局部编码错误。

最需要优先处理的是**发现 1：客户端网关没有任何认证，且 `config.go:811` 主动把唯一的开关堵死**——任何人可以冒充任意 UID 收发消息，这让其余所有权限校验形同虚设；发现 7 的 `/healthz/details` 默认无认证公开整张集群拓扑，恰好为它提供了侦察面，两者应当一起修。第二类是**「构造了但没接进关停/调度」的系统性疏漏**：发现 2（`CleanupExpired` 全仓无调用方，按频道无界泄漏订阅者列表）、发现 4（三个 closer 只登记在失败路径上）、发现 8（复合启动的后半段失败不回滚前半段）同属一个根因——组合根缺少「每个构造出的后台资源必须登记到关停栈 / 周期调度器」的强制约定，建议用一条针对 `build.go` 的结构化检查（或 `internal` 边界测试的扩展）把它固化下来。发现 3 的共享 5s 停止预算与发现 6 的网关准入不对称则是两处会在生产压力下才现形的隐患，修复成本都很低。
