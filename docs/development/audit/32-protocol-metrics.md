# 协议编解码 + 指标 + 日志封装（pkg/protocol, pkg/metrics, pkg/wklog, pkg/observability/sendtrace）

## 覆盖情况

分片内全部 53 个非测试 `.go` 文件，共 6774 行，全部通读：

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/protocol/jsonrpc/types.go | 685 | 是 |
| pkg/protocol/jsonrpc/codec.go | 481 | 是 |
| pkg/metrics/dashboard_collector.go | 431 | 是 |
| pkg/protocol/wkprotoenc/crypto.go | 417 | 是 |
| pkg/protocol/codec/protocol.go | 400 | 是 |
| pkg/protocol/frame/common.go | 371 | 是 |
| pkg/metrics/transport.go | 357 | 是 |
| pkg/protocol/codec/decoder.go | 264 | 是 |
| pkg/protocol/codec/encoder.go | 235 | 是 |
| pkg/metrics/channel.go | 224 | 是 |
| pkg/metrics/message.go | 186 | 是 |
| pkg/protocol/codec/recv.go | 170 | 是 |
| pkg/metrics/gateway.go | 164 | 是 |
| pkg/metrics/controller.go | 164 | 是 |
| pkg/metrics/delivery.go | 161 | 是 |
| pkg/wklog/field.go | 159 | 是 |
| pkg/wklog/raft_logger.go | 149 | 是 |
| pkg/protocol/jsonrpc/example_new_fields.go | 146 | 是 |
| pkg/observability/sendtrace/sendtrace.go | 143 | 是 |
| pkg/protocol/codec/send.go | 126 | 是 |
| pkg/protocol/codec/sendack.go | 121 | 是 |
| pkg/protocol/jsonrpc/example_event.go | 118 | 是 |
| pkg/protocol/frame/recv.go | 99 | 是 |
| pkg/metrics/registry.go | 77 | 是 |
| pkg/protocol/codec/connack.go | 73 | 是 |
| pkg/protocol/codec/sub.go | 71 | 是 |
| pkg/protocol/codec/connect.go | 70 | 是 |
| pkg/protocol/codec/suback.go | 66 | 是 |
| pkg/metrics/slot.go | 59 | 是 |
| pkg/metrics/diagnostics.go | 57 | 是 |
| pkg/metrics/storage.go | 51 | 是 |
| pkg/protocol/codec/event.go | 42 | 是 |
| pkg/protocol/frame/send.go | 36 | 是 |
| pkg/protocol/codec/disconnect.go | 36 | 是 |
| pkg/protocol/codec/message_seq.go | 33 | 是 |
| pkg/protocol/codec/common.go | 32 | 是 |
| pkg/protocol/codec/recvack.go | 31 | 是 |
| pkg/wklog/logger.go | 29 | 是 |
| pkg/protocol/frame/setting.go | 29 | 是 |
| pkg/protocol/frame/suback.go | 26 | 是 |
| pkg/protocol/frame/connect.go | 26 | 是 |
| pkg/protocol/frame/event.go | 23 | 是 |
| pkg/protocol/frame/connack.go | 23 | 是 |
| pkg/protocol/frame/sendack.go | 22 | 是 |
| pkg/protocol/frame/recvack.go | 19 | 是 |
| pkg/protocol/frame/disconnect.go | 19 | 是 |
| pkg/protocol/frame/sub.go | 16 | 是 |
| pkg/wklog/nop.go | 15 | 是 |
| pkg/protocol/frame/pong.go | 11 | 是 |
| pkg/protocol/frame/ping.go | 11 | 是 |

pkg/protocol/jsonrpc 下的 README.md / README_zh.md / protocol.md 亦通读（与代码无实质矛盾，jsonrpc 包内无 FLOW.md/AGENTS.md）。跨包调用关系（gateway adapter、internal/app、internal/access/node RPC、gnet transport）为理解触发路径而读，发现均落在本分片路径内。

---

## 发现

### [P0] 1. `WriteString` 的 panic 是全协议栈唯一长度防线，且所有帧编码器都把无校验的字符串送进它 —— C-1 的系统性枚举

- **位置**：`pkg/protocol/codec/encoder.go:137-152`、`pkg/protocol/codec/encoder.go:160-172`
- **类别**：正确性 / 安全（远程可触发 panic）
- **代码**：
  ```go
  // encoder.go:137-145
  func (e *Encoder) WriteString(str string) {
      if len(str) == 0 {
          e.WriteInt16(0)
          return
      }
      bl := len(str)
      if bl > math.MaxInt16 {
          panic(fmt.Errorf("WriteString: len(str) > math.MaxInt16, len(str) = %d", bl))
      }
  ```
  ```go
  // encoder.go:160-169
  func (e *Encoder) WriteBinary(b []byte) {
      if len(b) == 0 {
          e.WriteInt16(0)
      } else {
          bl := len(b)
          if bl > math.MaxInt16 {
              panic(fmt.Errorf("WriteBinary: len(b) > math.MaxInt16, len(b) = %d", bl))
          }
  ```
- **系统性枚举结论**（本分片要求的第 1 项）：`grep -rn 'panic(' pkg/protocol` 的全部非测试命中就是这两处（另有一处 `protocol.go:84` 是注释掉的旧代码）。**整个编解码层只有这 2 个 panic 点，而防御整个栈的字符串长度的就是这一个 `if`**。所有按 int16 前缀写字符串的编码器都走 `WriteString`，共 12 个生产编码器函数、约 30 个调用点：`encodeRecv`（MsgKey/FromUID/ChannelID/ClientMsgNo/Topic/StreamNo，`recv.go:106-137`）、`encodeSend`（`send.go:78-99`）、`encodeSub`、`encodeSuback`、`encodeConnack`、`encodeDisConnect`、`encodeSendack`、`encodeEvent`。`encodeRecv` 本身返回 `error`（`recv.go:102`），`encodeFrameWithWriter` 也返回 `error`（`protocol.go:156`）——错误通道现成，选择 panic 而非返回 error 纯属多余，且这个选择把每一处上游遗漏的长度校验都升级成了进程崩溃。
- **不对称性（C-1 的成因，crux）**：解码侧 `Decoder.Binary()`（`decoder.go:163-178`）按 int16 前缀切片并检查 `d.offset+int(size) > len(d.p)`，且 `MaxRemaingLength = 1MB`（`common.go:6`）+ `PayloadMaxSize = 32767`（`common.go:7`）共同约束解码产物——**任何从线上解码出来的字符串不可能超过 32767 字节，所以"回显已解码值再编码"的路径永远安全**。但 **API 来源**（`POST /message/send` 的 JSON、JSON-RPC 客户端输入）的字段没有这条物理约束，可以直接携带任意长度的字符串进入 frame 对象，再由 `EncodeFrame` 编码——API 世界允许的值超出了线路格式可表达的值域，而两层之间没有任何一道校验。这就是 bug 存在的全部原因。
- **触发路径**：与协调者 C-1 相同——`POST /message/send` 带 >32767 字节的 `client_msg_no`/`from_uid`/`channel_id` → `buildRealtimeRecvPacket`（`internal/app/deliveryrouting.go:2156-2179`）→ `EncodeFrame`（`deliveryrouting.go:1900`）→ `encodeRecv` → `WriteString` → panic → 无 recover → 进程崩溃。补充两点本分片核实的事实：
  1. **WriteBinary panic 目前是潜在的**：生产代码没有任何 `WriteBinary` 方法调用点（grep 全仓只有定义与测试），现行编码器对 payload 一律用无前缀的 `WriteBytes`。但它与 WriteString 是同一复制粘贴模式，任何新增编码器照抄即中。
  2. **崩溃循环的编码侧半边**：消息在投递前已持久化提交；`encodeDeliveryFrame`（`deliveryrouting.go:1898-1901`）在每次投递时从已提交消息重建 RECV 帧再编码。毒消息重启后重放投递会再次走到同一个 panic（重放是否真会再次投递由 Unit 02 确认，但编码侧每次重放都会重编码这一点已核实）。
- **后果**：单个未认证 HTTP 请求即可打挂整个进程；若重放路径重新投递，则重启后崩溃循环。
- **建议**：把 `WriteString`/`WriteBinary` 的 panic 改为返回 error（向上贯穿 `encodeFrameWithWriter` 已有的 error 通道），并在入口层对进入协议帧的全部字符串字段加显式长度上限。

### [P0] 2. JSON-RPC 桥是同一批 frame 对象的第二条无长度校验注入路径；且 `int→uint8` 截断静默改写 Version/ChannelType

- **位置**：`pkg/protocol/jsonrpc/types.go:374-388`（`SendParams.ToProto`）、`types.go:336-354`（`ConnectParams.ToProto`）、`pkg/gateway/protocol/jsonrpc/adapter.go:52-63`（`Decode`→`ToFrame`）
- **类别**：正确性 / 安全
- **代码**：
  ```go
  // types.go:374-388
  func (p SendParams) ToProto() *frame.SendPacket {
  req := &frame.SendPacket{
      Framer:      headerToFramer(p.Header),
      Setting:     p.Setting.ToProto(),
      ClientMsgNo: p.ClientMsgNo,
      ChannelID:   p.ChannelID,
      ChannelType: uint8(p.ChannelType),   // G115 实锤：int -> uint8 截断
      Payload:     p.Payload,
      MsgKey:      p.MsgKey,
      ...
  ```
  ```go
  // types.go:338-341 (ConnectParams.ToProto)
  var version uint8 = uint8(p.Version)   // 256 截断成 0
  if p.Version == 0 {
      version = frame.LatestVersion
  }
  ```
- **触发路径**：gosec 在 types.go 报了 6 处 G115（:338 :348 :380 :445 :454 :611），**这些不是误报**——`Version`、`ChannelType`、`DeviceFlag` 是 JSON 里的任意精度 `int`，直接 `uint8()` 截断：
  1. JSON-RPC 客户端（`jsonrpc` / `wsmux` 协议 adapter，均已在 `pkg/gateway/gateway.go` 注册）发送 `send` 请求，`clientMsgNo`/`channelId`/`msgKey`/`topic`/`streamNo` 任一给 >32767 字节的字符串 → `SendParams.ToProto()` 原样装入 `SendPacket` → 该帧沿投递/转发路径被 `EncodeFrame` 时 → 发现 1 的 panic。**没有对任何一个字段做长度校验**（整个 jsonrpc 包无一处 `len(...)` 上限检查）。Payload 唯一例外：`protocol.go:179` 有 `PayloadMaxSize` 检查并返回 error。
  2. `channelType: 256` → 截断成 0（既非个人也非群组），静默产生语义错误的帧而非拒绝请求；`version: 256` → 截断成 0 → 反而被"0 表示用最新版"的逻辑放大成 `LatestVersion`。
  3. `deviceFlag: 2`（jsonrpc 枚举 `DeviceSys`，types.go:23）→ `frame.DeviceFlag(2)` = **PC**（frame 层 SYSTEM=99，`frame/common.go:243-252`）——JSON-RPC 的"系统设备"在线上变成了"PC 设备"，直接影响同标同账号互踢语义。
- **后果**：与发现 1 同级的远程 panic（多一条客户端协议入口）；外加协议字段静默截断/错映射。
- **建议**：ToProto 转换层对所有 string 字段和 enum 字段做范围/长度校验并返回 error；`uint8(x)` 全部换成显式范围判断。

### [P1] 3. `DashboardCollector.Query` 用未校验的 window/step 做 `make([]bucket, N)` —— 节点间 RPC 路径可远程触发 OOM/panic

- **位置**：`pkg/metrics/dashboard_collector.go:249-270`；喂入点 `internal/access/node/monitor_metrics_rpc.go:32`
- **类别**：资源 / 安全（远程可达的资源耗尽）
- **代码**：
  ```go
  // dashboard_collector.go:249-270
  bucketCount := int(window / step)
  ...
  buckets := make([]bucket, bucketCount)
  ```
  ```go
  // monitor_metrics_rpc.go:24-33 —— 解码自原始 uvarint，无任何范围校验
  req, err := decodeMonitorMetricsRequest(body)
  ...
  result, err := a.monitorMetrics.LocalMonitorMetrics(ctx,
      time.Duration(req.WindowSeconds)*time.Second,
      time.Duration(req.StepSeconds)*time.Second)
  ```
- **触发路径**：HTTP 管理面入口（`internal/access/manager/monitor_metrics.go:95-113`）对 window∈[1m,1h]、step∈[5s,60s]、整除关系做了校验，最大 bucketCount=720，安全。**但节点间 RPC 路径没有任何等价校验**：`decodeMonitorMetricsRequest`（`monitor_metrics_codec.go:26-43`）把两个原始 uvarint 直接转成秒数传进 `Query`。攻击者（需到达集群内部 RPC 面）发送 `WindowSeconds=2^40, StepSeconds=1` → `make([]bucket, 1.1e12)` → 立即 panic（makeslice len out of range）或 OOM kill；`StepSeconds=0` → 除零得 +Inf→int 转换未定义行为路径、负数 step → 负 bucketCount → panic。这条 RPC 由 `managementMonitorMetricsReader`（`internal/app/monitor_metrics.go:24-37`）在用户查询跨节点监控时调用，属于常开的生产路径。
- **后果**：目标节点 panic（无 recover 兜住）或内存耗尽；集群内部可触发。
- **建议**：把 window/step 的范围校验下沉到 `Query` 内部（或 RPC 解码后立即校验），不依赖 HTTP 层。

### [P2] 4. 会话加密密钥由 MD5 派生并截断为 16 个十六进制字符（约 64 bit 有效熵），MsgKey 是无密钥摘要而非 MAC

- **位置**：`pkg/protocol/wkprotoenc/crypto.go:265-268`、`crypto.go:298-317`、`crypto.go:7`（gosec G501/G401 实锤）
- **类别**：安全
- **代码**：
  ```go
  // crypto.go:265-268
  func deriveAESKey(secret []byte) []byte {
      sum := md5.Sum([]byte(base64.StdEncoding.EncodeToString(secret)))
      return []byte(string(hexLower(sum[:])[:16]))
  }
  ```
  ```go
  // crypto.go:309-316 —— MsgKey = MD5(base64(AES-CBC(verity串)))，无 HMAC、无密钥参与摘要
  encryptCBCBlocks(sessionCrypto.block, sessionCrypto.iv, encrypted)
  encoded := getMsgKeyScratch(...)
  base64.StdEncoding.Encode(encoded, encrypted)
  sum := md5.Sum(encoded)
  return hexMD5String(sum), nil
  ```
- **触发路径 / 可利用性评估**（按协调者要求如实定性）：此密钥派生在 `NegotiateServerSession`/`DeriveClientSession` 中被调用，保护开启会话加密（`SessionValueEncryptionEnabled`，经 `pkg/gateway/protocol/wkproto/adapter.go:68` 接线）的客户端会话的 payload（AES-CBC）。
  1. X25519 共享密钥本身 256 bit 高熵，但 MD5→取 16 个 hex 字符后，**AES-128 密钥的实际搜索空间只剩 64 bit**。被动窃听者不知道共享密钥，无法直接离线爆破（需先解 ECDH），所以这不是"远程可直接打穿"的漏洞；但 64 bit 在针对性算力面前已可暴力（≈2^64），且无 KDF 迭代拉伸。上游客户端兼容性可能是保留原因。
  2. `MsgKey`（注释自称"仿中间人篡改"）是**对密文的无密钥 MD5**，不是 HMAC：任何持有会话密钥的一方（本就是通信端点）可对任意篡改后的密文重算 MsgKey，CBC 无认证（malleable），完整性保护形同虚设；它最多防"无会话密钥的第三方篡改"。
- **后果**：会话加密强度被人为压到 64 bit；完整性机制不是 MAC。
- **建议**：HKDF（sha256）从共享密钥派生 AES 密钥与 IV；MsgKey 换成 HMAC-SHA256。

### [P2] 5. `wklog.RaftLogger` 在级别过滤之前就完成渲染与分类 —— 禁用日志也要付 Sprintln + ToLower + 8 组 Contains 的钱

- **位置**：`pkg/wklog/raft_logger.go:31-53, 87-107, 109-140`；接线点 `pkg/controller/raft/logging.go:11`、`pkg/slot/multiraft/logging.go:9`
- **类别**：性能
- **代码**：
  ```go
  // raft_logger.go:31-36
  func (l *RaftLogger) Debug(v ...interface{}) {
      l.log(renderRaftArgs(v...), raftLogLevelDebug)
  }
  // raft_logger.go:87-99
  func (l *RaftLogger) log(msg string, level raftLogLevel) {
      event, effectiveLevel := classifyRaftLog(msg, level)
      ...
  // raft_logger.go:102-107
  func renderRaftArgs(v ...interface{}) string {
      ...
      return strings.TrimSpace(fmt.Sprintln(v...))
  }
  ```
- **触发路径**：每个 raft 组（每个 slot、每个 channel replica）的每条 raft 日志事件——包括被降级为 debug 的心跳/readindex/probe——都先走 `renderRaftArgs`（`fmt.Sprintln` 分配 + TrimSpace）再走 `classifyRaftLog`（`strings.ToLower` 分配 + 最多 8 次 `strings.Contains` 全串扫描），然后才第一次触达真正的 logger。即使 logger 是 `NopLogger`（`raft_logger.go:22-24` 对 nil logger 的默认！）或级别关闭，这些成本照付。`DebugEnabled` 守卫（`wklog/logger.go:21-29`）存在但 RaftLogger 完全没用它——本分片对"logging helper 是否 eager 格式化"的答案：**是，而且是专门的 raft 热路径 helper**。slot 数 × channel 数的 raft 组规模下，这是每事件几百纳秒 + 2-3 次分配的常驻税。
- **后果**：无收益的 CPU/分配开销乘以 raft 组数量；心跳高峰期（"msgheartbeat"逐条降级）开销最集中。
- **建议**：`log()` 先查级别/`DebugEnabled` 再渲染；或让 RaftLogger 直接持有 zap 结构化字段。

### [P2] 6. `EncodeFrame` 每帧新分配 buffer（扇出热路径），池化半途而废的证据在两处

- **位置**：`pkg/protocol/codec/protocol.go:146-153`、`protocol.go:193-195`、`decoder.go:5-32`
- **类别**：性能
- **代码**：
  ```go
  // protocol.go:146-153
  func (l *WKProto) EncodeFrame(f frame.Frame, version uint8) ([]byte, error) {
      buffer := bytes.NewBuffer(make([]byte, 0, encodedFrameSize(f, version)))
      err := l.encodeFrameWithWriter(buffer, f, version)
  ```
  ```go
  // protocol.go:193-195 —— End() 里的池化被注释掉了
  func (e *Encoder) End() {
      // bytebufferpool.Put(e.w)
  }
  ```
- **触发路径**：`encodeDeliveryFrame`（`internal/app/deliveryrouting.go:1900`）对每条投递消息每次调用 `EncodeFrame`；本地扇出路径同样每订阅者一次（`deliveryrouting.go:1553-1565` 的 `WriteFrame` 路径）。每帧一次堆分配 + `bytes.Buffer` 内部 growth copy（虽然 cap 已由 `encodedFrameSize` 精确算出，起步即够大，无二次扩容——这点比典型实现好），但群聊大频道下每秒数万帧的稳态分配压力完全可避免：`bytebufferpool`（recv.go 已在用它做 VerityString）或 `sync.Pool` + `Bytes()` 拷贝出局即可。`decoder.go:5-32` 整段注释掉的 `decOne/decTwo/decFour/decEight/decoderPool` 是同一半途而废努力的另一处化石。现 `Decoder` 是切片索引式，无解码分配，所以死代码本身无运行时成本——P2 归在 EncodeFrame 一侧，注释块按 P3 清理。
- **后果**：扇出热路径每帧一次分配；高扇出场景 GC 压力。
- **建议**：引入 bytebufferpool（Get/Put + `WriteFrame` 写入连接 writer）替代 `EncodeFrame` 返回新切片。

### [P3] 7. Prometheus 指标每消息/每帧走 `WithLabelValues` —— 微基准实测 ~74ns/次 vs 缓存句柄 ~52ns/次，0 分配；如实定性为轻微

- **位置**：`pkg/metrics/gateway.go:129-150`；调用点 `internal/app/observability.go:130,137,144`；同模式 `pkg/metrics/message.go:104-165`、`delivery.go:101-118`、`slot.go:45-52`
- **类别**：性能
- **代码**：
  ```go
  // gateway.go:137-143
  func (m *GatewayMetrics) MessageDelivered(protocol string, bytes int) {
      ...
      m.messagesDeliveredTotal.WithLabelValues(protocol).Inc()
      m.messagesDeliveredBytes.WithLabelValues(protocol).Add(float64(bytes))
  }
  ```
- **实测**（本机 Apple M4，prometheus/client_golang v1.19.1，3M iters，镜像生产调用形态：2 CounterVec + 1 HistogramVec(11 buckets)）：
  ```
  BenchmarkWithLabelValues   73.72 ns/op   0 B/op   0 allocs/op
  BenchmarkCachedHandle      51.67 ns/op   0 B/op   0 allocs/op
  BenchmarkSlotObserveProposal 82.97 ns/op  2 B/op  0 allocs/op  (strconv.FormatUint 每调用)
  ```
- **如实定性**：标签值全是预编译常量字符串（"wkproto"/"RECV" 等，无 `fmt.Sprintf`——已核实 pkg/metrics 零 Sprintf），`WithLabelValues` 命中稳定缓存后约 74ns、**0 分配**。每条消息全部指标调用合计约 150-220ns，相对每消息的编码/投递成本（µs 级）占比 <5%，**不是瓶颈**。`pkg/metrics/transport.go:245-357` 自己已经实现了 sync.Map 句柄缓存（注释明说 "Handle caches avoid repeated Vec label lookups on stable transport hot paths"），说明作者知道这个模式——gateway/message/delivery 三个每消息路径的 vec 没有用它，是不一致而非严重缺陷。`SlotMetrics.ObserveProposal` 的 `strconv.FormatUint` 每调用 2B 分配，量级更小。
- **建议**：顺手统一到 transport.go 已有的句柄缓存模式即可，不值得专项优化。

### [P3] 8. `wkprotoenc` sync.Pool 存入非指针切片（staticcheck SA6002）—— 加密开启时每消息 ~24B 额外分配

- **位置**：`pkg/protocol/wkprotoenc/crypto.go:400-407`
- **类别**：性能
- **代码**：
  ```go
  func putMsgKeyScratch(buf []byte) {
      if cap(buf) == 0 || cap(buf) > msgKeyScratchMaxRetained {
          return
      }
      whole := buf[:cap(buf)]
      clear(whole)
      msgKeyScratchPool.Put(whole[:0])
  }
  ```
- **触发路径**：`msgKeyWithCrypto`（crypto.go:298-317）每次计算 MsgKey 调 Get/Put 各两次；SendPacket 校验（`ValidateSendPacket`）与会话加密投递（`SealRecvPacket`）开启时是**每消息**路径。`Put(whole[:0])` 的切片重塑使切片头逃逸，每次 Put 约 24B 堆分配（SA6002 所指）。池本身把大 buffer 的分配省掉了，这个残余是池化收益的小额漏损。
- **建议**：池改存 `*[N]byte` 指针或包装结构指针。

### [P3] 9. `frame.DeviceFlag` 常量块 iota 链断裂，WEB/PC/SYSTEM 是无类型 int

- **位置**：`pkg/protocol/frame/common.go:243-252`
- **类别**：正确性（潜在）/ 可读性
- **代码**：
  ```go
  const (
      APP DeviceFlag = iota
      WEB = 1
      PC = 2
      SYSTEM = 99
  )
  ```
- **触发路径**：只有 `APP` 有 `DeviceFlag` 类型；WEB/PC/SYSTEM 是无类型常量。当前所有比较/switch 都能靠无类型常量隐式转换编译通过，所以**暂无运行时错误**，但任何新增的 iota 项或把这四个值放进 `[]DeviceFlag` 的代码都会暴露类型缺口；且它与发现 2 的 jsonrpc `DeviceSys=2` 错映射互为掩护。作为发现 2 的伴生整洁性问题记录。
- **建议**：`WEB DeviceFlag = 1` 显式补全类型。

### [P3] 10. 解码健壮性：≥5 字节 continuation 的 remaining-length 静默错切；UNKNOWN 帧类型/解码错误被吞成"帧未到齐"

- **位置**：`pkg/protocol/codec/protocol.go:105-113`、`protocol.go:359-376`
- **类别**：正确性（协议健壮性）
- **代码**：
  ```go
  // protocol.go:106-109 —— decodeFramer 的 error 被无条件吞掉
  framer, remainingLengthLength, err := l.decodeFramer(data)
  if err != nil {
      return nil, 0, nil
  }
  // protocol.go:363-374 —— 5 个以上 0x80 continuation 字节时循环自然退出，
  // 返回部分长度与偏移，字节流从此错位
  for multiplier < 27 {
      ...
      rLength |= uint32(digit&127) << multiplier
      if (digit & 128) == 0 {
          break
      }
  ```
- **触发路径**：客户端（gnet TCP/WS 入口）发送首字节帧类型为 0（UNKNOWN）或带 ≥5 个 continuation 字节的长度前缀：`DecodeFrame` 返回 `nil,0,nil` → adapter `break`（`pkg/gateway/protocol/wkproto/adapter.go:54-56`）→ 剩余字节滞留 `state.inbound` → 每次新数据都重复失败，连接挂死直到 `MaxInboundBytes`（默认 1MB，`pkg/gateway/types/options.go:73`，且 0 值会被强制回填默认，`options.go:139-140`）触发关闭。有界（1MB/连接）、可自愈，故 P3 而非 P2。MQTT 规范此处限 4 字节并视为协议错误应立即断连。
- **建议**：区分"数据不足"与"协议非法"，后者直接关闭连接。

### [P3] 11. 生产包内的化石与死代码

- **位置**：`pkg/protocol/codec/decoder.go:5-32, 211-264`（三段注释掉的池化/reader 式实现）；`pkg/protocol/codec/protocol.go:31`（`WKProto` 内嵌 `sync.RWMutex` 但所有方法从不加锁——纯装饰，兼易误导读者以为有并发保护需求）；`pkg/protocol/jsonrpc/example_new_fields.go`、`example_event.go`（146+118 行 Example 函数带 `fmt.Println`，只在 go test 语义下执行，编译进生产二进制纯属死重）；`pkg/protocol/jsonrpc/codec.go:159-174`（未知 notification method 先置 `msgTypeUnknown`+err 再全部覆盖，死逻辑）
- **类别**：架构 / 可读性
- **触发路径**：无运行时触发；`go vet`/staticcheck 已确认无行为影响（除 SA6002 单列）。
- **建议**：删除注释块与 example 文件；去掉无用 mutex。

---

## 已排除的候选项

- `pkg/gateway/protocol/wkproto/adapter.go:33-36` / `pkg/gateway/core/server.go:558-569` — **"frames backed by immutable input bytes" 的零拷贝声明经查成立**：我沿 gnet `OnTraffic`→`enqueueCopiedData`（`conn.go:136-152`，一次性 copy）→ `state.inbound`（`server.go:470-489`）逐行验证了别名链。关键在于 `state.inbound = state.inbound[consumed:]` 重切片时 len==cap（inbound 始终是完整缓冲的 [0:N] 前缀），因此后续 `append` **必然重分配**，绝不会原地覆写已被异步派发帧的 payload 别名的区域。`cloneAsyncSendFrame` 的 `payload[:len:len]` 三索引切片因此安全。曾在候选名单，排除。
- `pkg/protocol/codec/protocol.go:170-211` 全部裸类型断言 `f.(*frame.XxxPacket)` — 只会收到同 switch 的 `GetFrameType()` 产出的类型，`packetDecodeMap` 的 `f.(frame.Framer)` 同理（decodeFunc 只被 `decodeFramer` 的产物调用）。类型与帧类型常量一一对应，不可达。误报。
- gosec 在 `encoder.go`/`protocol.go`/`common.go` 的 38 条 G115（int/int32/int64/uint64→byte/uint32）— 全部是移位取字节（`byte(i>>8)` 等）的**按设计**写法，或 `int→uint32` 的 size 字段（受 `MaxRemaingLength`/`encodedFrameSize` 上界约束），无远程输入可驱动的溢出。误报。唯一实锤的 G115 在 jsonrpc types.go，已列为发现 2。
- `pkg/protocol/codec/sendack.go:52-54` ClientMsgNo 回显 — 值来自解码过的 SEND 帧（≤32767 硬上界），再编码不越界。安全。
- `pkg/protocol/codec/recv.go:96` `BinaryAll()` payload 别名输入缓冲 — 消费方（delivery push RPC `handleDeliveryPushItem`、投递编码）在使用期内输入 `item.Frame` 不可变。安全。
- `pkg/metrics/transport.go` sync.Map 句柄缓存 — LoadOrStore 竞态下可能重复创建 vec 子项但取值一致，无泄漏（重复句柄不被引用）。安全。
- `pkg/metrics/dashboard_collector.go:98-101` `Stop()` 双关闭 — `internal/app/lifecycle.go:72-77` 用 `sync.Once`（`dashboardCollectorStop.Do`）保护。安全。
- `pkg/observability/sendtrace` — `atomic.Pointer` swap、`Record` 的 nil 检查、`Enabled()` 守卫均正确；热路径调用方（如 `pkg/gateway/core/server.go:1071-1087`）都先 `sendtrace.Enabled()`。无发现。
- `pkg/wklog/field.go` `Field{Value any}` 装箱 — 存在但每字段 1 次小分配且仅在实际写出路径被消费，无独立触发路径，不单列。
- ws 握手 sha1（`pkg/gateway/transport/gnet/ws_handshake.go:148`）— RFC 6455 强制，误报，属 Unit 31 范围，按协调者指示忽略。

---

## 本分片整体评价

编解码层的**解码侧**写得出乎意料地扎实：所有读路径都有界检查、`MaxRemaingLength`/`PayloadMaxSize` 双上限、`message_seq.go` 对 legacy uint32 溢出有显式 error、sendack 双布局兼容解码带完整 trailing-byte 校验、`OwnsDecodedFrames` 的零拷贝声明经我逐行验证成立。**但编码侧是同一个协议的另一个世界**：唯一一道长度防线藏在 `WriteString` 里且以 panic 收场，`encodeRecv` 有现成的 error 返回通道却不用——这直接把入口层每一个遗漏的校验都升格为远程可触发的进程崩溃（发现 1/2，与协调者 C-1 互为完整闭环，jsonrpc 桥是第二条等价注入路径）。最优先要修的就是它：panic 改 error + 入口长度上限，两行设计决策能消除本分片全部 P0。指标层质量好、性能体量经基准实测为轻微；wklog 的 RaftLogger eager 渲染是唯一有真实每事件成本的性能问题。加密侧 64-bit 有效密钥强度建议按兼容窗口规划升级而非立即破坏性变更。
