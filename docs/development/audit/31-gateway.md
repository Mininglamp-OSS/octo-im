# 客户端网关基础设施（pkg/gateway）

## 覆盖情况

`pkg/gateway/FLOW.md`（AGENTS.md 要求先读）已全文通读。以下为分片内**全部** 39 个非测试 `.go` 文件：

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/gateway/core/server.go | 1664 | 是 |
| pkg/gateway/transport/gnet/conn.go | 697 | 是 |
| pkg/gateway/transport/gnet/group.go | 677 | 是 |
| pkg/gateway/testkit/fake_transport.go | 473 | 是 |
| pkg/gateway/session/session.go | 349 | 是 |
| pkg/gateway/transport/gnet/ws_frame.go | 238 | 是 |
| pkg/gateway/core/idle_tracker.go | 223 | 是 |
| pkg/gateway/protocol/jsonrpc/adapter.go | 179 | 是 |
| pkg/gateway/types/options.go | 165 | 是 |
| pkg/gateway/transport/gnet/actor.go | 164 | 是 |
| pkg/gateway/transport/gnet/ws_handshake.go | 150 | 是 |
| pkg/gateway/auth.go | 147 | 是 |
| pkg/gateway/protocol/wsmux/adapter.go | 136 | 是 |
| pkg/gateway/wkprotoenc/crypto.go | 129 | 是 |
| pkg/gateway/testkit/fake_handler.go | 128 | 是 |
| pkg/gateway/protocol/wkproto/adapter.go | 117 | 是 |
| pkg/gateway/core/registry.go | 111 | 是 |
| pkg/gateway/testkit/fake_protocol.go | 108 | 是 |
| pkg/gateway/gateway.go | 106 | 是 |
| pkg/gateway/core/dispatcher.go | 91 | 是 |
| pkg/gateway/transport/gnet/factory.go | 89 | 是 |
| pkg/gateway/testkit/wkproto_crypto.go | 84 | 是 |
| pkg/gateway/session/manager.go | 75 | 是 |
| pkg/gateway/types/event.go | 60 | 是 |
| pkg/gateway/transport/transport.go | 59 | 是 |
| pkg/gateway/transport/logging.go | 55 | 是 |
| pkg/gateway/transport/gnet/listener.go | 54 | 是 |
| pkg/gateway/types/observer.go | 37 | 是 |
| pkg/gateway/types/errors.go | 34 | 是 |
| pkg/gateway/errors.go | 34 | 是 |
| pkg/gateway/protocol/protocol.go | 25 | 是 |
| pkg/gateway/types/auth.go | 21 | 是 |
| pkg/gateway/options.go | 17 | 是 |
| pkg/gateway/transport/listener.go | 16 | 是 |
| pkg/gateway/binding/builtin.go | 15 | 是 |
| pkg/gateway/binding/binding.go | 15 | 是 |
| pkg/gateway/types/session_values.go | 14 | 是 |
| pkg/gateway/event.go | 14 | 是 |
| pkg/gateway/testkit/fake_session.go | 12 | 是 |

合计 6782 行。为确定 panic 后果，另外通读了 vendored `gnet v2.9.7`（`engine_unix.go`、`eventloop_unix.go`、`connection_unix.go`）与 `golang.org/x/sync@v0.16.0/errgroup`。

---

## 发现

### [P0] 1. WebSocket 小消息的写完成回调会窃取并清空尚未发出的大消息 writev 帧 → 出站消息静默丢失

- **位置**：`pkg/gateway/transport/gnet/conn.go:640-672`（`finishNextOutboundWrite`）、`conn.go:582-593`（`releaseOutboundWriteFrame`）、`conn.go:498-523`（`writeWebSocketVector`）、`conn.go:539-551`（`asyncWriteWithOutboundLimit`）、`conn.go:630-638`（`releaseOutboundWriteCallback`）
- **类别**：正确性 / 资源（缓冲区所有权）
- **代码**：

```go
// conn.go:640-651 —— 回调无法区分自己属于 AsyncWrite 还是 AsyncWritev
func (s *connState) finishNextOutboundWrite(conn gnetv2.Conn, err error) {
	s.outboundMu.Lock()
	var frame *wsWritevFrame
	if conn != nil && len(s.outboundWriteFrames) > 0 {
		frame = s.outboundWriteFrames[0]      // ← 无条件弹出队首 writev 帧
		s.outboundWriteFrames[0] = nil
		...
// conn.go:582-593 —— 回收时把 gnet 仍持有的 iovec 清空
func (s *connState) releaseOutboundWriteFrame(frame *wsWritevFrame) {
	if frame == nil { return }
	frame.bufs[0] = nil
	frame.bufs[1] = nil
	s.outboundMu.Lock()
	s.outboundWriteFrameFree = append(s.outboundWriteFrameFree, frame)
```

`outboundWriteFrames` **只**由 `queueOutboundWriteFrame`（conn.go:517，仅 `writeWebSocketVector` 调用）写入；但 `releaseOutboundWriteCallback` 同时被 `writeWebSocketCompact`（conn.go:493-495，payload < 1024 字节）和普通 `Write`（conn.go:458-460）注册。回调里没有任何信息把它和自己那次写关联起来。

关键点：`writeWebSocketVector` 在 conn.go:518 传给 gnet 的是 `framed.bufs[:]` —— 一个**指向 `wsWritevFrame` 结构体内部数组**的切片。gnet 只把这个 `[][]byte` 存进 hook（`connection_unix.go:513-518`），直到事件循环真正处理该任务时才读取（`asyncWritev` → `c.writev(hook.data)`，`connection_unix.go:273-287`）。

- **触发路径**（默认 `ws-gateway` 监听器 `0.0.0.0:5200`，`MaxOutboundBytes` 默认 1MB 因此限流分支必然启用）：
  1. 投递 goroutine 对同一个 WebSocket 会话先写一条小帧（如 SENDACK / PONG，编码后 < 1024 字节）→ `writeWebSocketCompact` → `beginOutboundWrite(sC)`（`outboundWriteSizes=[sC]`）→ `AsyncWrite(framed, releaseOutboundWriteCallback)`，任务 T1 入 gnet poller 队列。
  2. 紧接着写一条大帧（RECV，编码后 ≥ 1024 字节）→ `writeWebSocketVector` → `acquireOutboundWriteFrame()` 得到 F1 → `buildWSWritevFrame` 填好 `F1.bufs[0]=header, F1.bufs[1]=payload` → `queueOutboundWriteFrame(F1)`（`outboundWriteFrames=[F1]`）→ `AsyncWritev(F1.bufs[:], cb)`，任务 T2 入队。
     （两次写由 `session.writeMu` 串行，步骤 1、2 在同一个投递 goroutine 上连续执行；只要事件循环此时还没排干队列——`Trigger` 需要唤醒事件循环，存在实际延迟——就会进入下面的交错。）
  3. 事件循环处理 T1：写出小帧，`defer` 里调用回调 → `finishNextOutboundWrite(conn, nil)` → `conn != nil && len(outboundWriteFrames) > 0` 成立 → **弹出 F1 并 `releaseOutboundWriteFrame(F1)`**，把 `F1.bufs[0]`、`F1.bufs[1]` 置为 `nil`，F1 进入 free list。
  4. 事件循环处理 T2：`c.writev(hook.data)`，而 `hook.data == F1.bufs[:] == [nil, nil]`。`connection_unix.go:184-189` 算出 `n = 0`，`gio.Writev` 对零长 iovec 成功返回 → **这条 RECV 一个字节都没发出，且不产生任何 error**，`err == nil` 所以上层 `session.WriteFrame` 返回成功。
  5. 级联：F1 已在 free list，下一次 `writeWebSocketVector` 会 `acquireOutboundWriteFrame()` 拿回 F1 并覆写 `F1.header`/`F1.bufs`；若此时 T2 尚未被处理，gnet 读到的是**另一条消息**的 header 与 payload（重复发送 + 原消息丢失）。随后 T2 自己的回调又会弹出新队首帧，把下一条大消息一并清空。
- **后果**：WebSocket 客户端**静默丢失实时消息**，且服务端认为写成功（不会触发重发、不计入任何错误指标）。IM 的出站流量正好是「小 SENDACK/PONG + 大 RECV payload」混合，命中概率高；步骤 5 还会造成帧内容错配（把 A 的消息体配上 B 的帧头发出）。
- **建议**：让回调携带自己那次写的身份（例如用闭包捕获对应的 `*wsWritevFrame` 与 size，或为 compact/普通写注册一个不碰 `outboundWriteFrames` 的独立回调），不要用「弹队首」来猜回调归属。

---

### [P0] 2. WebSocket 入站 payload 未拷贝就交给 actor goroutine，事件循环随即覆写同一数组 → 数据竞争 + 入站消息串包/丢失，并污染已入队的异步 SEND

- **位置**：`pkg/gateway/transport/gnet/conn.go:118-133`（`enqueueDataWithOpcode`，不拷贝）对比 `conn.go:136-152`（`enqueueCopiedData`，拷贝）；`conn.go:279-285`（`appendWSInbound`）、`conn.go:327-331`（`wsInbound` 复位）、`ws_frame.go:123-132`（payload 别名 buf）、`group.go:494` 与 `group.go:539-545`
- **类别**：并发（数据竞争）/ 正确性
- **代码**：

```go
// conn.go:118-133 —— WebSocket 路径直接存入调用方切片，无拷贝
func (s *connState) enqueueDataWithOpcode(opcode byte, data []byte) bool {
	s.mu.Lock()
	...
	s.pendingBytes += len(data)
	s.queue = append(s.queue, connEvent{kind: connEventData, data: data, op: opcode})

// conn.go:279-285 / 327-331 —— 同一个 wsInbound 底层数组被反复复用
func (s *connState) appendWSInbound(data []byte) bool {
	...
	s.wsInbound = append(s.wsInbound, data...)      // 事件循环 goroutine
// nextWSResult():
	if consumed == len(s.wsInbound) {
		s.wsInbound = s.wsInbound[:0]               // len=0，cap 保留 → 下次 append 从 arr[0] 覆写
```

`ws_frame.go:127` 的 `frame.payload = buf[offset : offset+payloadLen]` 直接指向 `s.wsInbound` 的底层数组；`conn.go:393` 把它原样放进 `wsTrafficResult.payload`；`group.go:540` 再原样 `enqueueDataWithOpcode`。TCP 路径明确用了 `enqueueCopiedData`（注释写着 "copies from gnet's transient read buffer"），WebSocket 路径漏掉了这层保护，而 `s.wsInbound` 同样是被复用的临时缓冲。

- **触发路径**（默认 `ws-gateway` 监听器）：
  1. WS 客户端发消息 M1（一条完整 WebSocket 帧）。gnet 事件循环 `OnTraffic` → `appendWSInbound(buf)`，`s.wsInbound = arr[0:N]`。
  2. `nextWSResult()` 解出该帧，`consumed == len(s.wsInbound)` → `s.wsInbound = s.wsInbound[:0]`（len 0，cap 仍为 N）；`result.payload = arr[offset:N]`。
  3. `enqueueDataWithOpcode(op, arr[offset:N])` 把这个切片放入 conn queue，`signal()` 唤醒 actor shard（**另一个 goroutine**）。
  4. actor shard 尚未执行 `handleEvent`（shard 被 GOMAXPROCS 个 goroutine 共享，负载下排队很常见）之前，客户端发出第二条消息 M2（≤ N 字节）。事件循环再次 `OnTraffic` → `appendWSInbound(buf2)` → `append(arr[0:0], buf2...)` → **原地覆写 `arr[0:len(buf2)]`，正好覆盖 M1 的 payload（起始于 arr[offset]，offset 只有 2–14）**。
  5. actor goroutine 随后执行 `runtime.handler.OnData(s.transport, event.data)`，`core.Server.onData` 在 server.go:444 直接用这块内存解码（`len(state.inbound)==0` 分支不拷贝）。
- **后果**：
  - **数据竞争**（事件循环写 / actor 读同一内存，`-race` 可检出）；
  - M1 被解成 M2 的字节 → 要么协议错误踢连接，要么**把同一条消息处理两次**、或解出半 M2 半残留的畸形帧；
  - 更严重的一层：`wkproto`/`wsmux` 适配器都声明 `OwnsDecodedFrames() == true`（`protocol/wkproto/adapter.go:34-36`、`protocol/wsmux/adapter.go:40-47`），于是 `cloneAsyncSendFrame`（`core/server.go:558-569`）走 `cloned.Payload = send.Payload[:len:len]` **不做深拷贝**，异步 SEND 队列里保存的正是指向 `s.wsInbound` 的切片。worker 取出时 payload 早已被后续消息覆写 → **错误的消息体被写入集群并持久化、扇出给所有订阅者**。这直接违反 FLOW.md §4.4 对 `DecodedFrameOwner` 的约定（"Decode 返回的 frame 对象与 payload 字节在返回后保持有效且不会被复用/修改"）。
- **建议**：WebSocket 入站与 TCP 一致，在入队前拷贝 payload（或让 `nextWSResult` 交出所有权后不再复用该数组）；同时把 `OwnsDecodedFrames` 的判定下沉到 transport 能力，而不是只看 protocol 适配器。

---

### [P0] 3. 网关认证在缺少校验钩子时 fail-open，任何客户端可声明任意 UID；且配置层禁止开启 token 认证

- **位置**：`pkg/gateway/auth.go:59-74`、`pkg/gateway/core/server.go:638-644`
- **类别**：安全
- **代码**：

```go
// auth.go:59-74
		deviceLevel := frame.DeviceLevelSlave
		if opts.TokenAuthOn && !isVisitor(opts.IsVisitor, connect.UID) {
			if connect.Token == "" || opts.VerifyToken == nil {
				return &AuthResult{Connack: &frame.ConnackPacket{ReasonCode: frame.ReasonAuthFail}}, nil
			}
			level, err := opts.VerifyToken(connect.UID, connect.DeviceFlag, connect.Token)
			...
		}
		// TokenAuthOn == false 时：直接落到下面，connect.UID 原样采信
// core/server.go:641-644
	result.SessionValues[gatewaytypes.SessionValueDeviceID] = connect.DeviceID
	for key, value := range result.SessionValues {
		state.session.SetValue(key, value)   // SessionValueUID = connect.UID，无任何校验
	}
```

- **触发路径**：
  1. `internal/app/build.go:972` 构造 authenticator 时只传 `TokenAuthOn: cfg.Gateway.TokenAuthOn, NodeID`，`VerifyToken`/`IsVisitor`/`IsBanned` 全为 nil。
  2. `internal/app/config.go:811-813` 把 `Gateway.TokenAuthOn == true` 判为**配置错误**（"gateway token auth requires verifier hooks"），因此运行中的实例 `TokenAuthOn` 必然为 false —— 运维无法通过配置打开。
  3. 攻击者 TCP 连上默认 `tcp-wkproto` 监听器 `0.0.0.0:5100`，发一个 WKProto CONNECT，`UID` 填受害者 uid、`Token` 留空、`ClientKey` 填任意合法 X25519 公钥（`auth.go:109-114` 仅要求 ClientKey 非空）。
  4. `handleAuthFrame` 拿到 `ReasonSuccess` → 写入 `gateway.uid = <受害者 uid>` → 调用 `SessionActivator`（`internal/access/gateway` 在此做 presence activate）→ 写 CONNACK → `setAuthenticated(true)`。
- **后果**：完整身份冒用。攻击者被登记为该 UID 在线，可**接收该用户的实时消息**（投递按 UID 路由到该 session），并以该用户身份发消息（`mapSendCommand` 只读 session 里的 `gateway.uid`）。`IsBanned` 为 nil 意味着封禁同样完全不生效。
- **建议**：认证必须 fail-closed —— `Authenticator` 非空但缺少 `VerifyToken` 时应拒绝连接（或 `NewWKProtoAuthenticator` 在 `TokenAuthOn && VerifyToken == nil` 时构造失败），并补齐 verifier 钩子后解除 config 层的禁止。

---

### [P0] 4. 网关所有 goroutine 都没有 panic 屏障，远程可达的编码 panic 会直接杀死整个进程

- **位置**：`pkg/gateway/transport/gnet/actor.go:52-61`（actor goroutine）、`pkg/gateway/protocol/wkproto/adapter.go:79`、`pkg/gateway/core/server.go:700-711`（`encodeAndWrite`）；panic 源 `pkg/protocol/codec/encoder.go:143-145`
- **类别**：正确性 / 可用性（远程可触发崩溃）
- **代码**：

```go
// transport/gnet/actor.go:52-61 —— 唯一的 defer 是 wg.Done()，无 recover
	p.startOnce.Do(func() {
		for _, shard := range p.shards {
			shard := shard
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				shard.run()        // → processReady → handleEvent → handler.OnData/OnOpen
			}()
		}
	})
// pkg/protocol/codec/encoder.go:142-145 —— 越界不返回 error 而是 panic
	bl := len(str)
	if bl > math.MaxInt16 {
		panic(fmt.Errorf("WriteString: len(str) > math.MaxInt16, len(str) = %d", bl))
	}
```

已核实的事实链：
- `grep -rn "recover()" pkg/gateway/` → **0 处**；4 个 goroutine 启动点（`actor.go:56`、`group.go:316`、`core/server.go:757`、`core/server.go:807`）全部裸奔。
- vendored `gnet v2.9.7` 全仓 `recover()` → **0 处**；事件循环由 `errgroup.Group.Go` 启动（`engine_unix.go:43-47,123`），而 `x/sync@v0.16.0/errgroup/errgroup.go:78-92` 的注释明确说明**故意不 recover** f() 的 panic。
- 因此：gateway handler / 编解码 / WS 帧处理中的任何 panic ⇒ 进程崩溃，不是「只断一条连接」。
- 全仓 `MaxInt16` 上界检查只存在于上面那个 `panic` 自身（`grep -rn MaxInt16 internal/ pkg/` 仅命中 encoder.go），上游无任何长度校验。

- **触发路径**（跨协议放大，所有环节已逐一核实）：
  1. 攻击者连默认 `ws-gateway`（websocket + `wsmux`），首包以 `{` 开头 → `wsmux` 选中 `jsonrpc`（`protocol/wsmux/adapter.go:111-116`）。
  2. 发一个 JSON-RPC `send` 请求，`clientMsgNo` 设为 100KB 字符串（`MaxInboundBytes` 默认 1MB，放得下）。JSON 无 int16 长度前缀，`pkg/protocol/jsonrpc/types.go:604-618` 的 `SendRequest.ToProto()` 原样搬进 `frame.SendPacket.ClientMsgNo`，全链路无长度校验。
  3. 该 `ClientMsgNo` 随消息持久化，投递时经 `internal/app/deliveryrouting.go:2156-2179 buildRealtimeRecvPacket` 原样填入 `RecvPacket.ClientMsgNo`。
  4. 任意一个走 **wkproto**（默认 `tcp-wkproto` :5100）的接收方在线 → `deliveryrouting.go:1565 conn.Session.WriteFrame(f)` → `core/server.go:705 state.listener.adapter.Encode` → `protocol/wkproto/adapter.go:79 a.codec.EncodeFrame(f, version)` → `pkg/protocol/codec/recv.go:117 enc.WriteString(recvPacket.ClientMsgNo)` → **panic**。
  （纯 wkproto 入站走不通：`decoder.Binary()` 用 int16 长度前缀，最大 32767，正好等于 `MaxInt16` 不触发。必须借 JSON-RPC 入口注入。`ChannelID`、`Topic`、`MsgKey`、`FromUID` 同理，`encodeRecv` 对它们都调 `WriteString`。）
- **后果**：一个未授权（见发现 3，任何 UID 都能登入）的客户端发一条畸形消息即可**让整个节点进程崩溃**，并且该消息已被持久化 —— 节点重启后重新投递会再次崩溃（crash loop）。
- **建议**：两处都要补 —— (a) 在 actor goroutine、gnet 回调入口、异步 SEND worker、idle monitor 四个 goroutine 边界加 `recover()`，把 panic 降级为「关闭该连接」；(b) 在网关入站侧对所有会被写回 wire 的字符串字段做上界校验（≤ `MaxInt16`），拒绝而不是让编码层 panic。

---

### [P1] 5. WebSocket 握手完成前的连接没有任何超时，不进入 session 跟踪，也不计入 drain 统计 → slowloris 耗尽 fd

- **位置**：`pkg/gateway/transport/gnet/group.go:441-449`、`pkg/gateway/transport/gnet/ws_handshake.go:28-35`、`pkg/gateway/transport/gnet/factory.go:71-88`
- **类别**：安全 / 资源泄漏
- **代码**：

```go
// group.go:441-449 —— 只有 tcp 在 OnOpen 时入队 open 事件
	if !runtime.admitConn(state) {
		return nil, gnetv2.Close
	}
	c.SetContext(state)
	if runtime.opts.Network == "tcp" {
		state.enqueueOpen()          // websocket 要等到握手成功（group.go:525）才 enqueueOpen
	}
	return nil, gnetv2.None
// ws_handshake.go:29-35 —— 收不到 \r\n\r\n 且未超 8KB 就一直等
	headerEnd := bytes.Index(buf, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		if len(buf) > wsMaxHeaderSize { return nil, failWSHandshake(...), true }
		return nil, nil, false        // 无超时，连接保持
	}
```

- **触发路径**：
  1. 攻击者对默认 `ws-gateway`（`0.0.0.0:5200`）建立 N 条 TCP 连接。
  2. 每条发送 `GET / HTTP/1.1\r\nHost: x\r\n`（**不发**结束的空行），然后保持静默。
  3. `parseWSHandshake` 返回 `complete=false`，`handleWSTraffic` 返回 `gnetv2.None`，`state.enqueueOpen()` 永不执行。
  4. 因此 `connHandler.OnOpen` → `core.Server.onOpen` 永不被调用：没有 `sessionState`、没有 `registerState`、`touchReadActivity` 从未执行、**`idleTracker` 里没有任何条目**，默认 3 分钟 `IdleTimeout` 完全不适用。
  5. `Options.gnetOptions()`（factory.go:71-88）只设置 Multicore / NumEventLoop / ReusePort / 读写 buffer cap —— 没有任何读超时或 `TCPKeepAlive`，gnet 侧也不会回收。
- **后果**：每条恶意连接永久占用一个 fd、一个 `connState`、`listenerRuntime.conns` 的一个 map 条目，可无上限累积至 fd 耗尽，正常客户端无法接入。同时这些连接对 `Gateway.SessionSummary()`（只遍历 `core.states`，server.go:1359-1370）**完全不可见**，节点 drain 安全判断会误认为「已无会话」而继续下线。
- **建议**：在 transport 层给「已 accept 但未完成握手」的连接加一个独立的握手 deadline（或让 websocket 连接在 `OnOpen` 时就进入 idle 跟踪），并把未握手连接纳入 drain 统计。

---

### [P1] 6. `engine.Stop` 失败/超时时 actor pool 永不停止 → GOMAXPROCS 个 goroutine 泄漏，且重启监听器会再叠加一个 pool

- **位置**：`pkg/gateway/transport/gnet/group.go:377-398`、`group.go:299-303`
- **类别**：资源泄漏
- **代码**：

```go
// group.go:377-398
func (g *engineGroup) stopEngine(engine gnetv2.Engine, cycle *engineCycle) error {
	var err error
	if g.stopEngineFn != nil {
		err = g.stopEngineFn(engine, cycle)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = engine.Stop(ctx)
		cancel()
		if cycle != nil {
			if runErr, ok := <-cycle.doneCh; ok && err == nil { err = runErr }
		}
	}
	if err == nil {                                  // ← 只在成功时才停 actor pool
		if actors := g.actors.Swap(nil); actors != nil { actors.stop() }
	}
	return err
}
```

- **触发路径**：
  1. `Gateway.Stop()` → `core.Server.Stop()` → `listener.Stop()` → `engineGroup.stop()` → `stopEngine`。
  2. `engine.Stop(ctx)` 带 5 秒超时。actor shard 上正在执行的业务 handler（`handleEvent` 同步调用 `core.Server.onData` → `internal/access/gateway` → usecase → 集群 RPC / 磁盘）耗时超过 5 秒时，`engine.Stop` 返回 `context.DeadlineExceeded`。
  3. `err != nil` → `actors.stop()` 被跳过，`g.actors` 仍指向旧 pool；`group.go:285` 的 `if err == nil` 也不成立，`g.running` 保持 true、`g.cycle`/`g.routes` 不清理。
  4. `actorPool` 的 GOMAXPROCS 个 shard goroutine（actor.go:56-59）阻塞在 `select { case <-s.stopCh: ... case <-s.wake: }`，因为 `close(p.stopCh)` 只在 `stop()` 里执行 —— **永久泄漏**。
  5. 若之后再次 `listenerHandle.Start()`（`engineGroup.start` 的 late-start / `restartEngine` 分支），`startEngine`（group.go:301-303）会 `newActorPool(0)` + `actors.start()` + `g.actors.Store(actors)`，再叠加一批 goroutine，旧的那批永远拿不到停止信号。
- **后果**：goroutine 泄漏（每次失败 GOMAXPROCS 个）；进程无法干净退出（`Gateway.Stop()` 返回后仍有活跃 actor 可能继续回调已关闭的 handler）；反复重启监听器时线性累积。
- **建议**：`stopEngine` 无论成功与否都要 `g.actors.Swap(nil)` 并 `stop()`（放进 defer），并在失败路径上把 `g.running`/`g.cycle`/`g.routes` 也复位到一致状态。

---

### [P2] 7. 异步 SEND worker 不检查会话是否已关闭，会在 `OnSessionClose` 之后再回调 `OnFrame`

- **位置**：`pkg/gateway/core/server.go:811-830`、`core/server.go:841-879`、`core/server.go:1403-1438`
- **类别**：正确性（生命周期契约）
- **代码**：

```go
// core/server.go:811-830 —— 取出任务后直接 dispatch，无 isClosed() 判断
func (s *Server) runAsyncDispatchWorker(tasks <-chan asyncDispatchTask) {
	defer s.workerWG.Done()
	collector := newAsyncSendBatchCollector(tasks, asyncSendBatchLimitsFromOptions(s.options.DefaultSession))
	for {
		batch, ok := collector.nextBatch()
		if !ok { return }
		if s.dispatchSendBatch(batch) { continue }
		for _, task := range batch {
			recordAsyncDispatchWait(task)
			if err := s.dispatchFrame(task.state, task.replyToken, task.frame); err != nil {
				s.handleHandlerError(task.state, err)
			}
		}
	}
}
```

- **触发路径**：
  1. 客户端连续发 5 个 SEND，`dispatchSendFrameAsync`（server.go:571-581）把它们放进按 ChannelID 分片的有界队列，`onData` 立即返回。
  2. 客户端此刻断开 → gnet `OnClose` → actor → `core.Server.onClose`（server.go:683-698）→ `state.close(CloseReasonPeerClosed, err)`。
  3. `sessionState.close`（server.go:1408-1437）在 `closeOnce.Do` 内依次 cancel `requestContext`、`unregisterState`、`session.Close()`、`adapter.OnClose`、`conn.Close()`，最后 `dispatcher.sessionClose(st)` → 业务 handler 的 **`OnSessionClose`**（`internal/access/gateway` 在此做 presence deactivate）。
  4. 异步 worker 随后才取到那 5 个任务 → `dispatchFrame` → 业务 handler 的 **`OnFrame(SEND)`**，对象是一个已经宣告关闭、presence 已注销、`RequestContext` 已 cancel 的会话。
- **后果**：违反 FLOW.md §6.6 描述的关闭语义（close 之后不应再有帧回调）。实际表现为：`OnSendBatch`/`OnFrame` 拿到已取消的 context 后返回错误 → `handleHandlerError` → `state.close()`（`closeOnce` 已用完，静默空转）→ 产生一批无意义的错误日志与 metrics；更糟的是业务层若在 `OnFrame` 路径上隐式重建/刷新 presence 或会话索引，会在注销之后又写回状态，留下幽灵在线记录。`Stop()` 路径（server.go:323-330 先 `state.close` 再 `asyncDispatch.close()`）放大了这个窗口。
- **建议**：worker 取出任务后先判 `task.state.isClosed()`（batch 路径同样过滤），跳过已关闭会话；`Stop()` 里先关 `asyncDispatch` 并等 worker 退出，再逐个 `state.close`。

---

### [P2] 8. 除 SEND 外的所有业务帧在共享 actor shard 上同步执行，单个慢 handler 会阻塞该 shard 上全部连接

- **位置**：`pkg/gateway/core/server.go:536-546`、`pkg/gateway/transport/gnet/actor.go:31-37`、`actor.go:142-164`、`pkg/gateway/transport/gnet/conn.go:253-265`
- **类别**：性能（队头阻塞）
- **代码**：

```go
// core/server.go:536-546 —— 只有 SendPacket 走异步，其余原地同步调用业务 handler
		s.observeFrameIn(state, f)
		if send, ok := isSendPacket(f); ok {
			s.dispatchSendFrameAsync(state, replyToken, send)
			continue
		}
		if err := s.dispatchFrame(state, replyToken, f); err != nil {
// transport/gnet/actor.go:31-37 —— shard 数量默认就是 GOMAXPROCS
func newActorPool(shards int) *actorPool {
	if shards <= 0 {
		shards = runtime.GOMAXPROCS(0)
		if shards <= 0 { shards = 1 }
	}
```

- **触发路径**：
  1. 连接按 `connID % len(shards)` 固定分配到某个 actor shard（`actor.go:77-82`），shard 数默认 `GOMAXPROCS`（8 核机器 = 8 个 goroutine 服务全部连接）。
  2. `actorShard.drainReady`（actor.go:142-164）在 shard goroutine 上**串行**调用 `state.processReady()` → `handleEvent` → `runtime.handler.OnData` → `core.Server.onData` → `dispatchInboundFrames`。
  3. 客户端发一个 RECVACK → `internal/access/gateway.handleRecvAck` → `messages.RecvAck` → `internal/usecase/message/recvack.go:9` 用 **`context.Background()`（无超时）** 调 `deliveryAck.AckRoute` → `internal/runtime/delivery/manager.go:84-90` → `shardFor(...).routeAcked(ctx, binding)`，即向另一个有界 actor 队列提交。
  4. 该 delivery shard 队列满或其 actor 阻塞时，`routeAcked` 阻塞 → gateway actor shard goroutine 被占住 → 分配到同一 shard 的**所有其它连接**的 open/data/close 事件全部停摆，且因为没有超时可以无限期阻塞。
- **后果**：约 1/GOMAXPROCS 的在线连接同时停止处理入站数据（包括 PING，进而被 idle 判定踢掉）。RECVACK 的量级与消息投递量同阶，是真正的热路径。对比之下 SEND 有最多 1024 个 worker（`maxAsyncDispatchWorkers`，server.go:33），两条路径的并发度差了两个数量级。FLOW.md §10 只把 SEND 移出了事件循环，RecvAck/Ping 仍留在串行路径上。
- **建议**：把 RECVACK（以及任何可能进入 usecase 的非 SEND 帧）也移到有界 worker pool，或至少给这些同步调用加超时；不要让单条连接的业务耗时影响同 shard 的其它连接。

---

### [P2] 9. `idleTracker.touch` 在每次入站读上取同一把全局互斥锁并做堆调整 + 多次 map 写

- **位置**：`pkg/gateway/core/idle_tracker.go:27-48`、`idle_tracker.go:215-223`、`pkg/gateway/core/server.go:437`、`core/server.go:1578-1587`
- **类别**：性能
- **代码**：

```go
// core/idle_tracker.go:27-48 —— 全服务器一把 sync.Mutex
func (t *idleTracker) touch(state *sessionState, now time.Time) {
	...
	t.mu.Lock()
	defer t.mu.Unlock()
	deadline := now.Add(t.timeout)
	if idx, ok := t.indexes[state]; ok && idx >= 0 && idx < len(t.heap) && t.heap[idx].state == state {
		t.heap[idx].deadline = deadline
		t.heap.fix(idx, t.indexes)      // 上/下沉，每次 swap 写两次 map
		return
	}
	t.heap.push(idleDeadline{deadline: deadline, state: state}, t.indexes)
// idle_tracker.go:215-223
func (h idleDeadlineHeap) swap(i, j int, indexes map[*sessionState]int) {
	h[i], h[j] = h[j], h[i]
	if h[i].state != nil { indexes[h[i].state] = i }
	if h[j].state != nil { indexes[h[j].state] = j }
}
```

- **触发路径**：`core.Server.onData` 在 server.go:437 对**每一批**入站数据调用 `state.touchReadActivity()`，后者无条件 `st.server.idleTracker.touch(st, now)`（server.go:1584-1586），没有任何节流。N 条连接、每条 R 条消息/秒 ⇒ 每秒 N×R 次抢同一把 `t.mu`；每次持锁期间做一次 `map[*sessionState]int` 查找 + 一次堆 fix，堆规模 N=100k 时深度约 17，最坏 34 次 map 写，全部在临界区内。多个 gnet 事件循环 + GOMAXPROCS 个 actor shard 会同时争抢这一把锁。
- **后果**：入站吞吐被一把全局锁串行化，且锁内工作量随连接数对数增长（并伴随大量 map 写而非纯数组操作）。这是替换「按 tick 全量扫描」时把成本搬到了每帧路径上；在大连接数下会成为网关的主要串行瓶颈。相关的 `lastReadActivity` 原子写（见发现 11）也在同一路径上白做。
- **建议**：给 `touch` 加节流（例如仅当距上次刷新超过 `IdleTimeout/N` 才真正入堆），或改为按 shard 分片的 tracker（与 actor shard 对齐，消除跨 shard 争用）。

---

### [P2] 10. wkproto 批量解码整批只取一次协议版本，与 CONNECT 同段管道化的后续帧按 LatestVersion 解码

- **位置**：`pkg/gateway/protocol/wkproto/adapter.go:45-59`、`pkg/gateway/core/server.go:444-462`、`core/server.go:515-548`
- **类别**：正确性（协议兼容）
- **代码**：

```go
// protocol/wkproto/adapter.go:43-59 —— version 在循环外取一次，循环内所有帧共用
	frames := make([]frame.Frame, 0, 1)
	consumed := 0
	version := uint8(frame.LatestVersion)
	if sessVersion, ok := sessionVersion(sess, false); ok {
		version = sessVersion
	}
	for consumed < len(in) {
		f, n, err := a.codec.DecodeFrame(in[consumed:], version)
		...
		frames = append(frames, f)
		consumed += n
	}
```

- **触发路径**：
  1. 旧客户端在**一个 TCP 段**里同时发出 `CONNECT(Version=3)` + `SEND`（管道化）。
  2. `core.Server.onData` 一次 `decodeInboundFrames` 把两帧都解出来。此时 session 里还没有 `gateway.protocol_version`，`sessionVersion(sess, false)` 返回 `(LatestVersion, true)`，于是 **SEND 也按 version=6 解码**。
  3. `decodeSend`（`pkg/protocol/codec/send.go:32-33,51`）在 version 2/3/5 处行为不同：version≥5 不再读 `streamNo`，version≥3 才读 `Expire`。按 6 解一个 v3 的 SEND 会跳过实际存在的字段、又多读 4 字节 —— 字段错位。
  4. `handleAuthFrame` 处理 CONNECT 时才把协商结果写进 `gateway.protocol_version`（`core/server.go:641-644`），但那一批的 SEND 早已解错。
  （`decodeConnect` 本身忽略 version（`pkg/protocol/codec/connect.go:8-39`），所以 CONNECT 自身不受影响，问题只出在同批的后续帧。）
- **后果**：旧协议版本客户端若管道化发送，首批业务帧被错位解码 —— 要么 `protocol_error` 踢连接，要么把错位字段当合法值写入集群（`Expire`、`streamNo` 被污染）。
- **建议**：`Decode` 在每次循环迭代重新读取 session 版本，或让 core 在识别到 CONNECT 后中断本批解码、用新版本重新解析剩余字节。

---

### [P3] 11. 热路径上的只写状态：`lastReadActivity` 与 `session.Manager` 写进去从来没人读

- **位置**：`pkg/gateway/core/server.go:104`、`core/server.go:1583`、`core/server.go:44`、`core/server.go:1311`、`core/server.go:1328`
- **类别**：架构（死代码）/ 性能
- **代码**：

```go
// core/server.go:104 + 1578-1586 —— 唯一的读者不存在
	lastReadActivity atomic.Int64
...
func (st *sessionState) touchReadActivity() {
	now := time.Now()
	st.lastReadActivity.Store(now.UnixNano())     // 全仓仅此一处写，零处读
// core/server.go:1303-1329 —— Manager 只有 Add/Remove
	s.sessions.Add(state.session)
	...
	s.sessions.Remove(state.session.ID())
```

- **触发路径**：`grep -rn lastReadActivity pkg/gateway/` 只有声明（:104）与写入（:1583）两处，无任何读取点。`session.Manager` 的 `Get`/`Range`/`Count`（`session/manager.go:26-75`）在全仓非测试代码中**零调用方**；`SessionSummary`（server.go:1359-1370）用的是 `s.states` 而非 `s.sessions`。
- **后果**：每批入站数据多一次无用的原子写（与发现 9 在同一路径）；每条连接建立/关闭多一次 `Manager.mu` 全局互斥锁 + `map[uint64]Session` 条目（在 `Server.mu` 之外的第二把全局锁），纯开销。不是无界泄漏（Remove 会执行），但属于典型「只写不读的 map」。
- **建议**：删掉 `lastReadActivity`；`session.Manager` 要么真正接入（例如让 `SessionSummary` 用它）要么从 `core.Server` 移除。

---

### [P3] 12. `Options.Validate` 从不校验 websocket `Path` 与 `Network` 取值，`ErrListenerWebsocketPath` 是死错误

- **位置**：`pkg/gateway/types/options.go:83-132`、`pkg/gateway/types/errors.go:14`、`pkg/gateway/errors.go:14`
- **类别**：架构（死代码）/ 健壮性
- **代码**：

```go
// types/options.go:104-126 —— 只查空串与重复，不查取值合法性
		if name == "" { return ErrListenerNameEmpty }
		if _, ok := seenNames[name]; ok { return ErrListenerNameDuplicate }
		...
		if network == "" { return ErrListenerNetworkEmpty }
		if transport == "" { return ErrListenerTransportEmpty }
		if protocol == "" { return ErrListenerProtocolEmpty }
	}
// types/errors.go:14 —— 定义了但没人用
	ErrListenerWebsocketPath = errors.New("gateway: websocket listener path is required")
```

- **触发路径**：`grep -rn ErrListenerWebsocketPath .`（排除 docs）只命中 `types/errors.go:14` 定义和 `errors.go:14` 的 re-export，**零个使用点**。`Network` 的合法性要等到 `gnet.Factory.Build`（`transport/gnet/factory.go:52-57`）才检查，于是一个 `Network: "udp"` 的配置能通过 `NewServer`，直到 `Start()` 才失败。
- **后果**：配置错误的失败点被推迟到启动阶段；FLOW.md §4.2 称 "`Options.Validate` 会修剪 listener 字段、校验 listener name/address 不重复" 与实际一致，但 `ErrListenerWebsocketPath` 的存在暗示本应有 path 校验却从未实现。
- **建议**：在 `Validate` 里校验 `Network ∈ {tcp, websocket}`，并按 `ErrListenerWebsocketPath` 的语义补上 websocket path 校验（或删掉该错误）。

---

### [P3] 13. 代码与 FLOW.md 不一致（三处）

- **位置**：`pkg/gateway/FLOW.md` §6.2 / §4.4 / §6.4 对应 `transport/gnet/group.go:446-448`、`protocol/wkproto/adapter.go:34-36`、`core/server.go:1227-1244`
- **类别**：架构 / 文档一致性
- **代码**：

```go
// core/server.go:1227-1244 —— 非 wkproto 协议直接被标记为已认证，没有任何认证环节
func (s *Server) syncSessionProtocol(state *sessionState) error {
	...
	if protocol == "wkproto" && s.options.Authenticator != nil {
		state.setAuthRequired(true)
		return nil
	}
	state.setAuthRequired(false)
	state.setAuthenticated(true)     // jsonrpc 会话：无 CONNECT、无 token、直接放行
	return s.dispatchSessionOpen(state)
}
```

三处不一致（均已逐一核实）：
1. **§6.2 声称 `OnOpen` 对每条连接「② 创建 sessionState」**，但 `group.go:446-448` 只对 `Network == "tcp"` 调 `enqueueOpen()`；websocket 连接在握手成功前（`group.go:525`）根本不会触发 `core.Server.onOpen`，因此没有 sessionState、不在 idle 跟踪内、不在 `SessionSummary` 内（即发现 5）。
2. **§4.4 规定** "协议适配器只有在 `Decode` 返回的 frame 对象与 payload 字节在返回后保持有效且不会被复用/修改时，才可以实现 `DecodedFrameOwner` 并返回 true"。`wkproto` 与 `wsmux` 都无条件返回 true，但在 WebSocket transport 上 payload 指向被复用的 `s.wsInbound`（即发现 2）——该声明对 gnet websocket 路径不成立。
3. **§6.4 只描述了 WKProto CONNECT 认证流程**，而 `syncSessionProtocol` 让 `jsonrpc`（含默认 `ws-gateway` 的 `wsmux` 选中 jsonrpc 的情形）直接 `authenticated = true`。实际后果：`internal/access/gateway.OnFrame`（`frame_router.go:17-28`）对 `ConnectPacket` 落到 `default: return ErrUnsupportedFrame`，而 `CloseOnHandlerError` 默认 true ⇒ **JSON-RPC 客户端一发 connect 请求就被断开，且没有任何 CONNACK**；不发 connect 直接 send/recvack 则被 `mapSendCommand`/`mapRecvAckCommand`（`mapper.go:56-64`）以 `ErrUnauthenticatedSession` 拒绝。唯一能正常工作的是 PING（`handlePing` 不查 UID），未认证会话可靠 ping/pong 无限续命并刷新 idle deadline，同时被计入 `SessionSummary` 影响 drain 判断。
- **后果**：FLOW.md 作为该包的权威流程文档，在连接生命周期、零拷贝安全前提、认证覆盖面三个关键点上与实现不符；其中 2、3 直接掩盖了真实缺陷。JSON-RPC 协议栈实际上处于「有编解码但无认证入口」的半成品状态。
- **建议**：同步更新 FLOW.md；并明确 jsonrpc 的认证方案（要么定义 JSON-RPC connect 的认证路径，要么在 `Validate` 阶段拒绝无认证协议的对外监听器）。

---

### [P3] 14. 805 行测试替身位于生产包树内

- **位置**：`pkg/gateway/testkit/`（`fake_transport.go` 473、`fake_handler.go` 128、`fake_protocol.go` 108、`wkproto_crypto.go` 84、`fake_session.go` 12）
- **类别**：架构（分层）
- **代码**：`testkit/fake_transport.go:151-153` 等处在非 `_test.go` 文件里 `panic("nil fake listener")`、`panic(err)` —— 测试语义的 fail-fast 代码编译进普通包。
- **触发路径**：`grep -rl "gateway/testkit"` 排除 `_test.go` 后，唯一的非测试 importer 是 `test/e2e/suite/wkproto_client.go`（e2e 辅助包）。`cmd/wukongim`、`cmd/wkbench`、`internal/app` 均无非测试引用，因此**当前不在生产运行路径上**（见「已排除的候选项」）。
- **后果**：无运行时风险，但 `pkg/gateway/testkit` 作为可导出包，任何生产代码都可以引入这些含 `panic` 的替身；同时它使 `pkg/gateway/...` 的公开 API 面比实际需要大。
- **建议**：迁到 `internal/` 下的测试支撑包，或改名为 `*_test.go` 形式的共享 testdata 包（`export_test.go` / 独立 `testing` 子模块）。

---

## 已排除的候选项

**侦察阶段提示，已逐一核实为干净/必要成本：**
- `core/server.go:40-105` —— 确认锁粒度正确：`sessionState.inboundMu`（单连接入站缓冲）、`sessionState.metaMu`（单连接元数据，RWMutex）、`Server.mu`（RWMutex 保护 `states` 查找型 map）。锁顺序一致，`close()` 不取 `inboundMu`，无死锁环。
- `core/registry.go:24-97` —— RWMutex 保护，`RegisterTransport`/`RegisterProtocol` 仅在 `gateway.buildRegistry` 启动期调用，`Transport`/`Protocol` 仅在 `NewServer` 调用；运行期零争用。`isNil` 用 reflect 正确处理了 typed-nil 接口。
- `session/manager.go`、`session/session.go` —— nil receiver 守卫齐全，RWMutex 用法正确，`WriteFrame` 的双重检查（closing/closed 原子量 + `writeMu`）正确。（`Manager` 只写不读见发现 11。）
- `transport/gnet/conn.go:136-152 enqueueCopiedData` —— `append([]byte(nil), data...)` 是 gnet buffer 复用契约（`c.Next(-1)` 的返回值仅在回调期间有效）**要求**的必要拷贝，不是缺陷。注意它还先做 pendingBytes 准入再拷贝，顺序正确。
- `transport/gnet/ws_frame.go` —— `decodeWSFrameWithLimit` 边界完整：`maxPayloadBytes` 检查（:105）在读 mask key 与切片之前，127 长度先拒 `> MaxInt64`（:99-101），控制帧强制 final 且 payload ≤ 125（:134-141），mask 强制校验（conn.go:332-339），UTF-8 校验（:362-369、:385-392），分片重组有上界（:351-353）。`buildWSCloseFrame` 在 text 过长时返回 nil，调用点用 `result.closeNow` 兜底（group.go:546-559），降级正确。
- `transport/gnet/ws_handshake.go:148 sha1.Sum([]byte(key + wsGUID))` —— gosec G401 + G505 **误报**：RFC 6455 强制要求用 SHA-1 计算 `Sec-WebSocket-Accept`，且此处不用于任何安全判定。

**机械扫描核实结果：**
- gosec `transport/gnet/actor.go:81` G115 `int(connID%uint64(len(p.shards)))` —— 误报，结果被 `len(p.shards)`（int）上界约束。
- gosec `core/server.go:1182` / `:1194` G115 `int(hash % uint64(shards))` / `int(id % uint64(shards))` —— 误报，同上，结果必 `< shards`。
- gosec `transport/gnet/conn.go:533` G115 `byte(c.state.wsWriteOp.Load())` —— 误报，取值后立刻与 `wsOpcodeText`/`wsOpcodeBinary` 比对，不匹配则回退 `wsOpcodeBinary`（conn.go:533-536），截断值不会被使用。
- gosec `protocol/jsonrpc/adapter.go:18` G101 硬编码凭据 —— 误报，`replyTokenQueueSessionValue = "gateway.jsonrpc.reply_tokens"` 是 session value 的 key 名，因含 "token" 被命中。
- staticcheck 在本分片仅 1 条（`core/async_dispatch_test.go:408` SA1029），属测试文件，按 brief 第 4 条不在范围内。

**自我驳回的候选项：**
- `core/server.go:430-431` `onData` 全程持 `inboundMu` 并在其下同步调用业务 handler —— 看起来是「持锁做 I/O」，但该锁是**单连接**锁，同一连接的 `onData` 已由 actor shard 串行化，唯一竞争者是 `close()`，而 `close()` 不取该锁。无死锁、无额外争用。（真正的队头阻塞问题在 shard 层面，已另列为发现 8。）
- `session/session.go:216-228 LoadOrStoreValue` 对 hot key 是 check-then-act 而非原子 —— 唯一调用方 `protocol/jsonrpc/adapter.go:128` 传的 key 是 `"gateway.jsonrpc.reply_tokens"`，不在 `isHotValueKey` 白名单（session.go:259-275）内，走的是 `sync.Map.LoadOrStore` 原子分支。hot key 分支无生产调用方，写不出触发路径。
- `protocol/jsonrpc/adapter.go` 的 `replyTokenQueue.tokens` 无界增长 —— 核实为收支平衡：`Decode` 每次最多解 1 帧并最多 push 1 个 token（notification 无 id 不 push），`core/server.go:516` 用 `len(frames)` 即 1 调 `TakeReplyTokens`；`OnClose` 还会 `clear()`。非泄漏。
- `core/server.go:640-672` 出站字节记账 `outboundWriteSizes` 弹队首而非对应项 —— 会造成瞬时数值偏斜，但因为是纯计数器且弹出总量守恒，后续回调会自行抵平，写不出「限流永久失效」或「计数永久漂移」的触发序列。（同一函数里对 `outboundWriteFrames` 弹队首是真 bug，已列为发现 1 —— 区别在于那里弹出的是**持有 gnet 仍在引用的内存的对象**，不是数字。）
- `transport/gnet/ws_frame.go:99-127` 在 `maxPayloadBytes <= 0` 时 `offset+payloadLen` 可能整数溢出导致切片越界 panic —— 需要运维显式配置**负数** `MaxInboundBytes`（`NormalizeSessionOptions` 只把 0 替换为默认值，`types/options.go:139-141`），过于牵强，无现实触发者。
- `core/server.go:956-1063 asyncSendBatchCollector` 的 `time.Timer` 未在 worker 退出时显式释放 —— `collect` 里 `defer c.stopTimer()`（:995）保证每轮结束都 Stop，`startTimer` 用的是正确的 Stop-drain-Reset 惯用法（:1038-1051）。无泄漏。
- `pkg/gateway/testkit` 被生产代码引用 —— 核实为**未被引用**：唯一非 `_test.go` importer 是 `test/e2e/suite/wkproto_client.go`（e2e 测试支撑包），三个 `cmd/*` 二进制与 `internal/app` 均无引用。因此只作为分层问题记为 P3（发现 14），而非可运行风险。
- `core.Server.Stop()` 未复位 `started` —— 不是缺陷：`Start()` 先查 `if s.stopped { return ErrGatewayClosed }`（server.go:191-194），停机后重启被正确拒绝。
- `Start()` 失败路径的 listener 泄漏 —— `rollbackStart`/`rollbackRuntimeListeners`（server.go:1373-1385）逆序 Stop，且 `listenerHandle.Stop()` 对未 Start 的 handle 是 no-op（`transport/gnet/listener.go:35-37`），符合 `transport.Listener` 的契约注释。无泄漏。

---

## 本分片整体评价

这是全节点唯一的客户端入口，代码水平整体明显高于一般水准：锁粒度是**按连接**而非全局、`Server.mu` 正确使用 RWMutex、idle 调度用最小堆+索引取代全量扫描、gnet buffer 复用契约在 TCP 路径上被正确遵守、WebSocket 帧解码的边界检查（长度上界、mask 强制、控制帧约束、UTF-8、分片上界）相当完整 —— 侦察阶段标记为「干净」的几处我都逐一复核并确认干净。`FLOW.md` 也是我见过的更认真的一份。

但**恰恰在最要命的两个地方失守，且都集中在 WebSocket 路径**：出站侧 `finishNextOutboundWrite` 用「弹队首」猜测异步写回调的归属，导致小消息的完成回调会把尚未发出的大消息 iovec 清空（发现 1，静默丢消息）；入站侧 WebSocket 忘了做 TCP 路径已经做对的那次拷贝，把指向复用缓冲的切片跨 goroutine 交出去（发现 2，数据竞争 + 串包，并因 `OwnsDecodedFrames` 的零拷贝优化把污染的 payload 写进集群持久化）。两者都在默认 `ws-gateway :5200` 上，都不需要恶意客户端就会自然发生。

**最该优先处理的一个问题是发现 2**（WebSocket 入站零拷贝越界）：它同时是数据竞争、是消息串包、还会让错误的消息体被持久化和扇出 —— 是本分片唯一会污染持久状态的缺陷，而修复只需在 `group.go:540` 入队前拷贝一次 payload，代价与 TCP 路径已付的一致。紧随其后是发现 4 + 3 的组合：认证 fail-open（任何人可冒用任意 UID，且配置层禁止开启 token 校验）叠加「全包零 `recover()` + gnet 零 `recover()` + errgroup 明确不 recover」，使得一个未授权客户端用一条超长 `clientMsgNo` 的 JSON-RPC 消息即可打死整个进程，且该消息已落盘、重启后重放会再次打死 —— 这是可被远程触发的 crash loop。

本分片是 **v1 生产运行路径**（`cmd/wukongim/main.go` → `internal/app` → `pkg/gateway.New`），不是未上线的 v2 栈，因此上述严重度均按实际计算，未做下调。

