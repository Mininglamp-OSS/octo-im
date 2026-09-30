# 节点间 RPC 入口与编解码（internal/access/node/）

## 覆盖情况

以下为本分片内**全部** 49 个非测试 `.go` 文件，全部通读（分段读取完整文件内容，非仅签名）：

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/access/node/presence_codec.go | 649 | 是 |
| internal/access/node/client.go | 562 | 是 |
| internal/access/node/plugin_management_codec.go | 511 | 是 |
| internal/access/node/channel_leader_codec.go | 486 | 是 |
| internal/access/node/diagnostics_codec.go | 479 | 是 |
| internal/access/node/channel_append_rpc.go | 425 | 是 |
| internal/access/node/options.go | 357 | 是 |
| internal/access/node/channel_append_codec.go | 346 | 是 |
| internal/access/node/delivery_push_codec.go | 332 | 是 |
| internal/access/node/delivery_tag_codec.go | 330 | 是 |
| internal/access/node/channel_messages_rpc.go | 256 | 是 |
| internal/access/node/plugin_management_rpc.go | 254 | 是 |
| internal/access/node/presence_rpc.go | 235 | 是 |
| internal/access/node/diagnostics_tracking_codec.go | 222 | 是 |
| internal/access/node/conversation_facts_rpc.go | 218 | 是 |
| internal/access/node/connections_rpc.go | 218 | 是 |
| internal/access/node/monitor_metrics_codec.go | 206 | 是 |
| internal/access/node/cmdsync_codec.go | 206 | 是 |
| internal/access/node/channel_message_codec.go | 206 | 是 |
| internal/access/node/plugin_http_forward_codec.go | 205 | 是 |
| internal/access/node/channel_messages_codec.go | 201 | 是 |
| internal/access/node/channel_leader_transfer_rpc.go | 189 | 是 |
| internal/access/node/channel_leader_repair_rpc.go | 189 | 是 |
| internal/access/node/connections_codec.go | 187 | 是 |
| internal/access/node/delivery_tag_rpc.go | 186 | 是 |
| internal/access/node/conversation_facts_codec.go | 186 | 是 |
| internal/access/node/runtime_summary_codec.go | 179 | 是 |
| internal/access/node/delivery_submit_codec.go | 175 | 是 |
| internal/access/node/delivery_control_codec.go | 175 | 是 |
| internal/access/node/cmdsync_rpc.go | 167 | 是 |
| internal/access/node/channel_leader_evaluate_rpc.go | 160 | 是 |
| internal/access/node/delivery_push_rpc.go | 159 | 是 |
| internal/access/node/channel_retention_rpc.go | 151 | 是 |
| internal/access/node/plugin_committed_codec.go | 138 | 是 |
| internal/access/node/channel_retention_codec.go | 124 | 是 |
| internal/access/node/channel_plane_codec.go | 120 | 是 |
| internal/access/node/channel_plane_rpc.go | 102 | 是 |
| internal/access/node/diagnostics_tracking_rpc.go | 92 | 是 |
| internal/access/node/user_system_uid_rpc.go | 81 | 是 |
| internal/access/node/plugin_http_forward_rpc.go | 73 | 是 |
| internal/access/node/monitor_metrics_rpc.go | 71 | 是 |
| internal/access/node/runtime_summary_rpc.go | 65 | 是 |
| internal/access/node/diagnostics_rpc.go | 64 | 是 |
| internal/access/node/plugin_committed_rpc.go | 61 | 是 |
| internal/access/node/user_system_uid_codec.go | 57 | 是 |
| internal/access/node/delivery_rpc.go | 52 | 是 |
| internal/access/node/delivery_submit_rpc.go | 37 | 是 |
| internal/access/node/delivery_ack_rpc.go | 34 | 是 |
| internal/access/node/delivery_offline_rpc.go | 30 | 是 |
| internal/access/node/service_ids.go | 24 | 是 |

（合计 10,232 行，与任务简报一致。另外为理解上下文通读了 `pkg/transport/rpcmux.go`、`pkg/transport/server.go`、`pkg/transport/frame.go`、`pkg/transport/conn.go`、`pkg/metrics/dashboard_collector.go`、`pkg/channel/handler/fetch.go`、`pkg/channel/handler/message_query.go`、`pkg/channel/handler/seq_read.go`、`pkg/db/message/compat.go`、`internal/runtime/channelmeta/repair.go`、`internal/runtime/channelmeta/leader_transfer.go`、`internal/FLOW.md`、`AGENTS.md`，以及 `internal/app/build.go` 中 wiring 部分。）

## 发现

### [P0] 1. monitor metrics RPC：恶意/损坏的 WindowSeconds/StepSeconds uvarint 可远程触发整型除零 panic 或负长度 makeslice panic

- **位置**：`internal/access/node/monitor_metrics_codec.go:26-42`（解码）→ `internal/access/node/monitor_metrics_rpc.go:32`（转换）→ `pkg/metrics/dashboard_collector.go:241-245`（崩溃点）
- **类别**：安全 / 远程输入可达的 panic
- **代码**：

  ```go
  // monitor_metrics_codec.go:26-42 — 无任何上界校验
  window, next, err := readUvarint(body, offset)
  ...
  step, next, err := readUvarint(body, next)
  ...
  return monitorMetricsRequest{WindowSeconds: int(window), StepSeconds: int(step)}, nil
  ```

  ```go
  // monitor_metrics_rpc.go:32 — 直接转 Duration 传下去
  result, err := a.monitorMetrics.LocalMonitorMetrics(ctx, time.Duration(req.WindowSeconds)*time.Second, time.Duration(req.StepSeconds)*time.Second)
  ```

  ```go
  // dashboard_collector.go:241-245 — Query 的崩溃点
  now := time.Now().UTC()
  cutoff := now.Add(-window)
  bucketCount := int(window / step)   // step==0 → 整型除零 panic
  stepSec := step.Seconds()
  ...
  buckets := make([]bucket, bucketCount)  // bucketCount<0 → makeslice panic
  ```

- **触发路径**：任意集群内节点（或任何能建立 TCP 连接到节点 transport 端口的攻击者——见发现 8 的无认证事实）向 `monitorMetricsRPCServiceID (52)` 发送 5 字节 magic `{'W','K','M','Q',1}` + 两个 uvarint，且这些 handler 在 `pkg/transport/server.go:180` 的独立 goroutine 中运行且**没有 recover**：
  1. 发 `StepSeconds=0`（uvarint 0x00）→ `time.Duration(0)*time.Second == 0` → `window/step` 整型除零 panic（已用独立小程序验证：`step=0 divide -> PANIC: runtime error: integer divide by zero`）；
  2. 或发 `WindowSeconds` uvarint = `2^64-1` → `int(window) == -1` → `window = -1s` → `bucketCount = -1` → `make([]bucket, -1)` panic（已验证：`negative bucketCount make -> PANIC: runtime error: makeslice: len out of range`）。
  默认配置下 `Observability.MetricsEnabled` 为 true（`internal/app/config.go:1348-1351`），collector 一定在运行，一定走到 `Query`。
- **后果**：每个恶意 RPC panic 掉一个 goroutine；transport server 不带 recover，goroutine 崩溃会导致整个进程退出（Go 语义）。攻击者可用低成本 UDP 级别的循环请求把整个节点打崩，进而影响所有 raft 组。
- **建议**：解码处对 WindowSeconds/StepSeconds 施加上界（如 ≤ 最大窗口秒数）且要求 StepSeconds > 0，越界直接返回 codec error。

### [P0] 2. delivery ack batch 解码：`make([]RouteAck, 0, int(count))` 的 count 无上界，恶意 uvarint 可触发 makeslice panic；且同文件绕过了共享的 `readCollectionLen` 防线

- **位置**：`internal/access/node/delivery_control_codec.go:41-56`
- **类别**：安全 / 远程输入可达的 panic
- **代码**：

  ```go
  count, next, err := readUvarint(body, offset)
  if err != nil {
      return deliveryAckRequest{}, err
  }
  if count == 0 {
      return deliveryAckRequest{}, fmt.Errorf("access/node: empty delivery ack batch")
  }
  offset = next
  commands := make([]deliveryevents.RouteAck, 0, int(count))
  for i := uint64(0); i < count; i++ {
  ```

- **触发路径**：向 `deliveryAckRPCServiceID (8)` 发送 magic `{'W','K','D','A',2}` + 一个 uvarint count（例如 `2^63`，15 字节请求）+ 任意垃圾。`int(count) == -9223372036854775808`，`make(..., 0, 负 cap)` 直接 panic（已用独立小程序复现：`PANIC: runtime error: makeslice: cap out of range`）。即使 count 值为正但很大（如 2^32），`readUvarint` 之后没有像同目录其它 codec 那样调用 `readCollectionLen`（该函数用 `count > uint64(remaining)` 把 count 钳制到剩余字节数）——delivery ack batch 是全目录唯一手写 count→cap 转换的解码器。后续循环里每个 `readRouteAckEvent` 会失败返回，但在第一轮分配前 panic 已经发生。
- **后果**：同发现 1 —— 无 recover 的 goroutine panic → 进程崩溃，可被远程低成本触发。
- **建议**：改为与本目录其余 codec 一致的模式：先 `readCollectionLen(count, len(body)-offset, "delivery ack batch")` 再分配。

### [P1] 3. presence 权威 RPC：SlotID==0 时不做权威校验直接执行写操作，打破 slot-leader 路由契约

- **位置**：`internal/access/node/presence_rpc.go:196-207`（守卫）+ `internal/access/node/presence_rpc.go:76-92`（register 调用）
- **类别**：分布式一致性 / 安全
- **代码**：

  ```go
  func (a *Adapter) authoritativeRPCStatus(slotID multiraft.SlotID) (string, uint64, bool) {
      if a.cluster == nil || slotID == 0 {
          return "", 0, false        // ← slot 0 视为"不处理"，直接放行到本地执行
      }
      leaderID, err := a.cluster.LeaderOf(slotID)
  ```

  ```go
  func (a *Adapter) handleRegister(ctx context.Context, req presenceRPCRequest) ([]byte, error) {
      if body, handled, err := a.handleAuthoritativeRPC(multiraft.SlotID(req.SlotID)); handled || err != nil {
          return body, err
      }
      result, err := a.presence.RegisterAuthoritative(ctx, ...)  // 本地 directory 直接写入
  ```

- **触发路径**：关键前提已核实——`internal/usecase/presence/authority.go:5-10` 的 `RegisterAuthoritative` **完全没有二次权威校验**，直接 `a.dir.register(cmd.SlotID, cmd.Route, ...)`，所以 `presence_rpc.go` 的这个守卫是唯一防线。
  1. 向任意节点的 `presenceRPCServiceID (5)` 发 `presenceOpRegisterID`，请求里 `SlotID=0`、`Route{UID:"victim", DeviceFlag:APP, DeviceLevel:Master, ...}`。`authoritativeRPCStatus(0)` 命中 `slotID == 0` 短路 → `handled=false` → 接收节点不做 leader 检查直接写本地权威 directory。
  2. `directory.register`（`internal/usecase/presence/directory.go:62-72`）会遍历 `d.byUID["victim"]`，对 `conflicts()` 为真的既有路由执行 `delete(d.byUID[route.UID], existingKey)` 并生成 `close`/`kick_then_close` RouteAction —— 即注入一条伪造的 Master 路由会**把受害者真实路由从权威目录里删掉**。
  3. 后续 `EndpointsByUID("victim")` 返回攻击者伪造的路由，消息被投到不存在的 node/session 而被丢弃，直到受害者网关的 heartbeat/replay（30s lease TTL，`directory.go:36`）把目录修复回来。
  正常调用方（`internal/app/presenceauthority.go:106-115`）只在本地是 leader 时才本地执行、否则走 remote RPC，所以生产路径不会发 SlotID=0；但 RPC 服务端接受任何来源（且 transport 无认证，见发现 8）。另外 `SlotForKey` 在 hash slot table 未就绪时也返回 0（`pkg/cluster/router.go:24-30`），启动竞态窗口内同样把 0 当成"放行"。
- **后果**：可远程删除任意在线用户的权威 presence 路由并替换为伪造路由，造成最长一个 lease 周期（30s）的定向投递丢失；可循环发送以持续拒绝服务。
- **建议**：slotID==0 时应返回明确的 codec 错误（拒绝），而不是放行本地执行。

### [P1] 4. 会话事实批量 RPC：Keys 数量无上界，单个 RPC 可驱动 O(keys × limit) 的存储扫描

- **位置**：`internal/access/node/conversation_facts_rpc.go:58-90`（入口）+ `internal/access/node/conversation_facts_rpc.go:179-205`（每 key 的 Fetch）
- **类别**：性能 / 健壮性
- **代码**：

  ```go
  if len(req.Keys) > 0 {
      entries = make([]conversationFactsEntry, 0, len(req.Keys))
      for _, rawKey := range req.Keys {
          ...
          case conversationFactsOpRecent:
              entry.Messages, err = a.loadRecentConversationFacts(ctx, key, req.Limit, req.MaxBytes)
  ```

- **触发路径**：`LoadRecentConversationMessagesBatch` / `LoadLatestConversationMessages`（`internal/app/build.go:1457,1484`）把一个用户的**全部会话 key** 打包进一个 RPC；服务端 `readConversationFactsChannelKeys`（conversation_facts_codec.go:126-145）只用 `readCollectionLen` 钳制 count ≤ 剩余字节数（64MB frame 上限内可达数十万 key），然后**串行**对每个 key 走 `Status + Fetch`（Pebble 迭代器读 limit 条消息）。一个 10 万会话的账号同步一次 = 10 万次独立存储扫描在一次 RPC 内串行完成；同时这是管理端/同步端可触发的热路径。
- **后果**：节点 RPC worker goroutine 被长时间占用（单 goroutine 串行，transport 每请求一个 goroutine），大量此类请求并发会拖垮节点存储 IO；没有分页/分批上限。
- **建议**：对 Keys 数量设置服务端上限（超出返回错误要求调用方分批），与 `maxDeliverySubmitMessageScopedUIDs=10000` 的既有风格对齐。

### [P1] 5. `handleChannelMessagesRPC`/`handleConversationFactsRPC` 对来自远端的 Limit 只透传不钳制，SyncMessages 走分块循环可拉取任意大的消息集

- **位置**：`internal/access/node/channel_messages_rpc.go:115,133`（直接透传）+ `pkg/channel/handler/seq_read.go:110-160`（分块读取，全量在内存累积）
- **类别**：性能 / 健壮性
- **代码**：

  ```go
  // channel_messages_rpc.go:111-116 — SyncMode 分支
  page, err := channelhandler.SyncMessages(a.channelLogDB, committedHW, channelhandler.SyncMessagesRequest{
      ...
      Limit:           req.Query.Limit,
  ```

  ```go
  // conversation_facts_rpc.go:193-202 — recent 分支
  fromSeq := uint64(1)
  if status.CommittedSeq >= uint64(limit) {
      fromSeq = status.CommittedSeq - uint64(limit) + 1
  }
  fetch, err := cluster.Fetch(ctx, channel.FetchRequest{
      ...
      Limit:     limit,
  ```

- **触发路径**：`Limit` 由 `readNodeInt` 从远端 uvarint 解出，仅做溢出检查不做范围检查。SyncMessages/`loadRangeMsgsFromStore`（seq_read.go:130-160）按 256 条/批循环 append 到一个 slice 直到读满 limit 或扫完区间 —— limit=10^9 时单个 RPC 请求触发全频道线性扫描并把上亿条消息累积在 goroutine 内存里。对比：`maxBytes` 参数确实透传并在 `readRows` 内生效，但 `recent` 分支当 `MaxBytes <= 0` 时 `Fetch` 直接返回 `ErrInvalidFetchBudget`，而调用方恒传 `conversationFetchMaxBytes`，防线不覆盖 Limit 维度。
- **后果**：一个构造的 RPC 即可造成节点内存暴涨/长尾阻塞；没有服务端 Limit cap。
- **建议**：服务端对 Limit 做上限钳制（如 1000），与 `defaultMaxSyncLimit` 风格对齐。

### [P3] 6. `handleChannelRetentionRPC` 用 `==` 比较 sentinel 错误，与同目录其余 handler 的 `errors.Is` 不一致

- **位置**：`internal/access/node/channel_retention_rpc.go:66-73`
- **类别**：正确性（一致性 / 脆弱性）
- **代码**：

  ```go
  meta, err := a.refreshMessageQueryMeta(ctx, req.Request.ChannelID)
  switch {
  case err != nil:
      if err == raftcluster.ErrNoLeader {
          return encodeChannelRetentionResponse(channelRetentionResponse{Status: rpcStatusNoLeader})
      }
      return nil, err
  ```

- **触发路径（对抗性复核结论：当前不可触发）**：我逐层核实了 `refreshMessageQueryMeta` → `channelMetaSync.RefreshChannelMeta`（`internal/app/channelmeta.go:104-109`）→ `Sync.ActivateByID`（`internal/runtime/channelmeta/resolver.go:178-203`）→ `Store.getChannelRuntimeMetaAuthoritative`（`pkg/slot/proxy/runtime_meta_rpc.go:53-71`）→ `pkg/slot/proxy/authoritative_rpc.go:110,123`，**全链路均原样返回裸 `raftcluster.ErrNoLeader`，无一处 `%w` 包装**，所以 `==` 今天工作正常。保留为 P3 是因为这是同目录内唯一的 `==` 写法（`channel_messages_rpc.go:79`、`channel_leader_repair_rpc.go` 均用 `errors.Is`），任何上游加一层 `fmt.Errorf("...: %w", ...)` 就会静默失效。
- **后果**：无当前可触发后果；未来上游包装错误时 retention advance 会把 no-leader 误报为内部错误（客户端侧 `normalizeChannelMessagesRPCError` 的字符串匹配可兜底，但同样脆弱）。
- **建议**：改用 `errors.Is(err, raftcluster.ErrNoLeader)`，与同目录其它 handler 一致。

### [P2] 7. delivery tag codec：SlotID uvarint 直接 `uint32(slotID)` 截断，高 slot 编号静默映射到错误 slot（gosec G115 真问题）

- **位置**：`internal/access/node/delivery_tag_codec.go:238-250`
- **类别**：正确性（整数截断）
- **代码**：

  ```go
  var slotID uint64
  if slotID, offset, err = readUvarint(body, offset); err != nil {
      return deliverytagruntime.PartitionTopologyVersion{}, offset, err
  }
  version.SlotAuthorityRefs[i].SlotID = uint32(slotID)
  ```

- **触发路径**：与同文件同函数中所有其它 uvarint 字段（`LeaderNodeID`、`ConfigEpoch`、`BalanceVersion`，均为 uint64 保留）不同，`SlotID` 是 uint32 但编码端 `appendPartitionTopologyVersion` 用 `appendUvarint(dst, uint64(ref.SlotID))` 写入、解码端直接截断。滚动升级中若未来 slot 编号超过 2^32（当前 hash slot 为 uint16，故当前实际值都在范围内），或一个被入侵/实现错误的 peer 发送 slotID=2^32+1，会被静默截成 1 —— 权威引用指向错误 slot，后续 tag fence 判断用错 slot leader。当前是"防御深度缺失"而非已可达缺陷，故 P2。
- **后果**：版本偏移/恶意 peer 下 slot authority 引用静默错乱。
- **建议**：解码端加 `if slotID > ^uint32(0)` 拒绝（同文件 diagnostics_codec.go:336-339 对 `event.SlotID` 已有此正确模式）。
- （gosec G115 命中核实：`delivery_tag_codec.go:248` 为真问题；`monitor_metrics_codec.go` 各处、`connections_rpc.go:217` 的 `int64(v)` 有 `time.Unix(0, ...)` 前的负值语义正确性但值为 `v > 2^63` 时 `int64` 截断为负时间戳 —— `connectionTimeFromUnixNano`/`readPluginManagementTime` 都做了上界检查，`monitor_metrics_codec.go:101` 的 `time.Unix(0, int64(generatedAt))` 未检查，但只影响响应里一个时间字段显示，为 P3 级别，归入已排除候选备注。）

### [P2] 8. 节点间 transport 完全无认证/加密，RPC handler 层也没有来源校验——上述 P0 发现的攻击面从"集群内"扩大为"能连上端口的任何人"

- **位置**：`pkg/transport/server.go:35-56`（Server 无任何 auth 配置）+ `internal/access/node/options.go:325-351`（所有 handler 只信 body）
- **类别**：安全（架构）
- **代码**：

  ```go
  // pkg/transport/server.go — ServerConfig 只有 ConnConfig 和 Logger，无 TLS/token
  type ServerConfig struct {
      ConnConfig ConnConfig
      Logger     wklog.Logger
  }
  // serveConn: accept 后直接 newMuxConn，无握手
  func (s *Server) serveConn(raw net.Conn) {
      ...
      mc = newMuxConn(raw, dispatch, s.cfg.ConnConfig)
  ```

- **触发路径**：`grep -rn 'tls\.\|Token\|auth\|Secret' pkg/transport/*.go` 零命中（已验证）；accept 的任何 TCP 连接都能发 `MsgTypeRPCRequest` 并到达本分片注册的 25 个 handler。`internal/access/node/` 的 handler（如 `handlePluginManagementRPC` 可远程卸载/重启插件、`handleSystemUIDCacheRPC` 可改系统账号缓存、`handleConnectionsRPC` 可列出全部连接 UID/设备信息）只校验 payload 格式，不校验调用方身份。发现 1/2 的"恶意 peer"因此等于"任何网络可达者"。
- **后果**：把本分片所有输入校验缺陷的威胁模型从"受信任集群成员"升级为"任意网络可达方"；同时 manager 级操作（插件卸载）暴露在节点端口上。
- **建议**：节点 transport 增加 mTLS 或共享密钥握手（handler 层单点校验），属跨分片根因。

### [P3] 9. `handleChannelLeaderRepairRPC`/`TransferRPC`：slot 计算失败时静默退化为 `slotID=0` 路径，跳过入口层权威守卫（纵深防御缺失）

- **位置**：`internal/access/node/channel_leader_repair_rpc.go:54-61`（transfer 同构 `channel_leader_transfer_rpc.go:54-61`）
- **类别**：分布式一致性（纵深防御）
- **代码**：

  ```go
  slotID := multiraft.SlotID(0)
  if a != nil && a.cluster != nil {
      slotID = a.cluster.SlotForKey(req.ChannelID.ID)
  }
  if status, leaderID, handled := a.authoritativeRPCStatus(slotID); handled {
      ...
  }
  // handled=false（slotID==0）→ 直接调用本地 RepairChannelLeaderAuthoritative
  ```

- **触发路径**：hash slot table 未加载（节点刚启动、table 同步延迟）时 `SlotForKey` 返回 0（`pkg/cluster/router.go:24-30` 已核实），`authoritativeRPCStatus` 对 0 返回 `handled=false`（presence_rpc.go:196），于是跳过入口层 leader 检查，直接执行 `RepairChannelLeaderAuthoritative`。该函数先 `GetChannelRuntimeMeta`，再经 `selectLeaderCandidate` 向各副本**逐个发出 evaluate RPC**，最后才调 `UpsertChannelRuntimeMetaIfLocalLeader`。
- **为何不是 P0/P1（对抗性复核结论）**：最终写入被下游二次 fencing 拦住 —— `pkg/slot/proxy/store.go:153-157` → `proposeLocalWithHashSlot` → `pkg/cluster/cluster.go:1041-1047` 的 `router.LeaderOf(slotID)` + `IsLocal(leaderID)`，非 leader（含 slotID=0 查不到 slot）一律返回 `ErrNotLeader`/`ErrSlotNotFound`，**不会产生分裂的 leader 元数据写入**。实际后果只是白跑一轮候选评估 RPC 并返回下游裸错误。
- **后果**：启动/迁移窗口内每个 repair/transfer 请求浪费一轮跨副本 evaluate RPC，并把本应清晰的 `no_group` 状态码降级成下游裸 error；入口层守卫形同虚设，一旦下游 fencing 被重构就会变成真正的分裂写入。
- **建议**：slot 计算结果为 0 时直接返回 `rpcStatusNoSlot` 拒绝，而不是退化为本地执行。

### [P3] 10. 整套 delivery tag RPC 状态/日志助手为死代码，疑似丢失了调用点

- **位置**：`internal/access/node/delivery_tag_rpc.go:167-186`
- **类别**：架构 / 死代码
- **代码**：

  ```go
  func deliveryTagRPCStatus(resp DeliveryTagResponse) string {
      if resp.Status != "" {
          return resp.Status
      }
      return rpcStatusOK
  }

  func deliveryTagRPCResult(resp DeliveryTagResponse) string { ... }

  func logDeliveryTagRPC(logger wklog.Logger, msg string, fields ...wklog.Field) {
      if logger == nil {
          return
      }
      logger.Info(msg, fields...)
  }
  ```

- **触发路径**：staticcheck U1000 全仓扫描命中（`/tmp/octo-staticcheck.txt:3-5`），`grep -rn` 全仓无调用点。这三个函数构成了一个完整的"状态归一 + Info 日志"观测层，唯一合理的解释是 `callDeliveryTagRPC`（delivery_tag_rpc.go:140-166，两跳 redirect 时）原本应在每次跳转时记录状态/结果，现在这个观测点丢了。同类：`delivery_submit_codec.go:16` 的 `deliverySubmitRequestMagic`（注释自述"preserves the original v1 magic for legacy binary payload helpers"，但无引用）。
- **后果**：观测能力缺失（无日志），死代码漂移。
- **建议**：要么在 `callDeliveryTagRPC` 的两跳处接回 `logDeliveryTagRPC`，要么删除。

### [P3] 11. `connections_rpc.go` 对全量在线连接做全表扫描 + 全量排序后编码，每个管理端连接列表请求都是 O(总连接数) 快照

- **位置**：`internal/access/node/connections_rpc.go:63-79`
- **类别**：性能
- **代码**：

  ```go
  slots := a.online.ActiveSlots()
  items := make([]Connection, 0)
  for _, slot := range slots {
      for _, conn := range a.online.ActiveConnectionsBySlot(slot.SlotID) {
          items = append(items, nodeConnection(nodeID, conn))
      }
  }
  sort.Slice(items, func(i, j int) bool {
      ...
  })
  ```

- **触发路径**：manager 页面/同步方每次调用 `Client.Connections(ctx, nodeID)`，服务端都遍历全部 slot × 全部连接、构造完整 DTO（含字符串字段）、排序、再编码。10 万连接的节点上一次 RPC 分配百 MB 级临时数据，且没有分页参数。
- **后果**：管理端刷新大节点连接列表时产生内存尖峰与长尾延迟；无上限。
- **建议**：加分页（limit/offset 或 cursor）参数，或至少对返回条数设上限。

## 已排除的候选项

- `internal/access/node/delivery_push_codec.go` 全文 / `delivery_submit_codec.go` 全文 — 简报提示"疑似未加固"。**核实结论：加固到位**。两者是全目录硬化最好的编解码器：所有 `make([]T, n)` 前都过 `readCollectionLen(count, len(body)-offset)`（同时防 int 溢出与超过剩余字节数），`readBytes` 有 `end < offset || end > len(body)` 双重检查，`decodeDeliveryPushResponseBinary`/`decodeDeliverySubmitRequest` 都有 trailing-bytes 检查。
- `internal/access/node/presence_codec.go`、`plugin_management_codec.go`、`channel_leader_codec.go`、`diagnostics_codec.go`、`diagnostics_tracking_codec.go`、`channel_message_codec.go`、`channel_messages_codec.go`、`cmdsync_codec.go`、`conversation_facts_codec.go`、`runtime_summary_codec.go`、`connections_codec.go`、`plugin_committed_codec.go`、`plugin_http_forward_codec.go`、`channel_plane_codec.go`、`channel_retention_codec.go`、`user_system_uid_codec.go` — 与上述两条逐一对比后确认均达到同一加固标准（`readCollectionLen` + 上界常量 + marker 校验 + trailing bytes）。"兄弟 codec 加固不一致"这一侦察假设**不成立**——不一致恰恰出现在简报没点名的两个地方：`delivery_control_codec.go:48`（发现 2）和 `monitor_metrics_codec.go:26-42`（发现 1）。
- gosec G115 命中 22 条逐一核实：
  - `delivery_tag_codec.go:248`（uint64→uint32 SlotID）— **真问题**，见发现 7。
  - `monitor_metrics_codec.go:21-22,42,74-76,101-104`（int↔uint64 旋转及 `int64(generatedAt)`）— 时间戳字段，`int(window)` 转换产生的真实危害已在发现 1 以 panic 形式呈现（那是长度计算而非 G115 本身）；`time.Unix(0, int64(v))` 溢出只导致错误时间显示，不崩溃、不越界，记为 P3 级噪音，不单列。
  - `connections_rpc.go:217`（`int64(v)`→`time.Unix`）— 同上，`v==0` 有专门分支，溢出只影响显示。
  - `plugin_management_codec.go:323,347`（`int(count)`）— 前一行有 `count > maxPluginManagementInt()` / `count > maxPluginManagementMethods` 上界检查，误报。
  - `plugin_http_forward_codec.go:127,145`（int32↔uint64 status）— `resp.GetStatus()` 来自本侧 proto，编码侧 int32→uint64 再解码回来数值一致（补码往返），仅负 status 显示异常，误报级别。
  - `delivery_push_codec.go:276,288`（`int(count)`、`uint64(len)`）— 276 行前有 `readCollectionLen`，288 行是 append 侧自身长度，误报。
  - `delivery_control_codec.go:48`（`int(count)`）— **真问题**，见发现 2（gosec 只报了溢出，实际还缺 `readCollectionLen` 的剩余字节钳制）。
- `internal/access/node/delivery_push_rpc.go:52-56,118-155` — `frames := make([]frame.Frame, len(items))` 解出的同一个 frame 指针被传进多个 session 的 `WriteFrame`。看起来像共享可变对象的数据竞争，但每个 session 的写入由各自 writer 串行消费（`pkg/transport/writer.go` 的 per-conn 队列），且我**写不出并发交错的触发序列**（无法证明 codec 产物含惰性可变状态）—— 按规则删除，不作为发现。
- `internal/access/node/presence_rpc.go:175-180` `handleApplyAction` 完全没有 leader 检查 — 看起来是无鉴权的远程踢连接原语，但 `internal/usecase/presence/gateway.go:111-135` 有三重守卫：`action.NodeID` 必须匹配本节点、`action.BootID` 必须等于本进程 `gatewayBootID`（进程级随机值）、`conn.UID` 必须与 `action.UID` 一致（不一致记 Error 日志并返回错误）。攻击者需同时猜中 bootID + sessionID + uid，实际安全。
- `internal/access/node/cmdsync_rpc.go:42-58` `cmdSyncOpSync`/`cmdSyncOpSyncAck` 没有 UID owner 校验（只有 `push_intent` 经 `ownerValidatingCMDIntentSink` 校验） — 看起来是"严格版/宽松版并存"，但 `Sync`/`SyncAck` 是读取与 ack 推进，`internal/usecase/cmdsync/app.go:52-62` 对 Limit 做了 `defaultMaxSyncLimit=10000` 钳制，且 owner 校验由客户端侧路由保证；无法构造出导致状态不一致的具体序列。
- `internal/access/node/diagnostics_codec.go:130-137` 的 `query.Limit` 未钳制 — `internal/observability/diagnostics/store.go:124,161-167` 的 `normalizeLimit` 有 `defaultMaxQueryLimit=500` 上界，误报。
- `internal/access/node/cmdsync_codec.go:113-117` 的 `query.Limit` 未钳制 — 同上，`internal/usecase/cmdsync/app.go:58-59` 钳制到 `MaxLimit`，误报。
- `internal/access/node/options.go:325-351` 各 handler 调 `a.presence.X` / `a.online.X` 前不做 nil 检查（而 `connections_rpc.go:64` 和多数 handler 都检查了） — 看起来是 nil interface 方法调用 panic，但 `internal/app/build.go:719-752` 组合根把全部 29 个 Options 依赖注入，生产路径无 nil；仅在直接构造 `Adapter` 的测试里可触发，不计。
- RPC service ID 跨包手工分配（`service_ids.go:23` 注释记录 47/48/49/53 被 `slot/proxy`、`channel/transport` 占用），`RPCMux.Handle` 对重复 ID 直接 `panic`（`pkg/transport/rpcmux.go:24-28`）—— 看起来是启动期 panic 风险，但我枚举了全仓所有注册点（`pkg/cluster` {1,14,20}、`pkg/channel/transport` {30,34,35,48}、`pkg/slot/proxy` {3,4,10,11,12,47,49,53}、本分片 25 个 ID），**确认零冲突**，当前无缺陷。
- `readCollectionLen`（`delivery_push_codec.go:283-293`）只用"剩余字节数"作为集合元素上界，不设元素条数常量上限 — 理论上 64MB frame（`pkg/transport/errors.go:16` `MaxMessageSize`）可放大到约 8-16 倍的堆分配（如 `channel.Message` 约 200B/最小编码 25B）。但这是**有界**放大且 frame 读取本身已消耗同量内存，加上 `pkg/transport` 每请求一 goroutine 无并发上限属跨分片根因，故不作为本分片独立发现。
- 资源类 — 本分片无 Pebble iter、文件句柄、ticker、后台 goroutine（全部 handler 是按请求调用的纯函数，goroutine 由 `pkg/transport` 统一管理并入 `s.wg`）；`grep 'go func\|time.New\|panic(\|context.Background()'` 在本分片非测试代码中**零命中**（仅 4 处 `_ = ctx`）。
- 锁类 — 本分片唯一的锁是 `Client.mu`（`options.go:344-347`）保护两个 capability map，读写均持锁，key 为节点 ID（量级 ≤ 节点数），无泄漏、无锁内阻塞调用、无锁序问题。
- 出站超时 — 所有出站调用走 `cluster.RPCService(ctx, ...)`，全分片无裸 `http.Client`/`http.Get`/`http.Post`（已 grep 验证）；`pkg/transport/conn.go:63-70` 的 `RPC` 在 `ctx.Done()` 与 `readerDone` 上双路 select，caller 的 deadline 能传播到底层；未发现缺 deadline 的出站路径。
- `callAuthoritativeRPC`（`client.go:452-520`）/`AppendToLeader`/`QueryChannelMessages`/`AdvanceChannelRetention` 的 candidates 循环 — `tried` 集合去重 + 候选只来自 `PeersForSlot` 或远端返回的 LeaderID，最坏 O(N) 次 RPC 后退出；LeaderID 指向已试过的节点会被 `tried` 拦截，无无限循环。
- `channel_append_rpc.go:69-104` 服务端二次 `AppendBatch` 重试（refresh meta 后用新 epoch 重试） — 看起来是绕过调用方 epoch fencing，但重试前有 `if refreshed.Leader != 0 && uint64(refreshed.Leader) != a.localNodeID { return not_leader }` 把非 leader 情况挡回，写入仍落在当前真 leader；且重试用相同 MessageID 走幂等路径（`ErrIdempotencyConflict` 已在 `channelAppendItemErrorCode` 映射中），非重复投递风险。
- `internal/FLOW.md:579-600` 的节点间 RPC 表列出 17 项，而 `options.go:325-351` 实际注册 25 项（缺 `channel_leader_transfer`/`runtime_summary`/`monitor_metrics`/`connections`/`connection`/`diagnostics_tracking`/`channel_retention`/`system_uid_cache`）—— 文档缺口属实但遗漏项均为观测/管理类旁路，且 `internal/access/node/` 下无自己的 `FLOW.md`（本仓 22 个 FLOW.md 均在更高层目录，该层级不设 FLOW.md 符合既有约定），不足以单列为发现。
- 分层（AGENTS.md「`internal/access/*` 只做入口协议适配」）— 逐文件核对后认为未构成违规：presence 权威语义在 `internal/usecase/presence`、delivery 在 `internal/runtime/delivery`、leader 修复在 `internal/runtime/channelmeta`、消息查询在 `pkg/channel/handler`；本分片留在入口层的是状态码映射、leader 重定向、错误归一化，属入口适配职责范围内。

## 本分片整体评价

这套节点间 RPC 编解码层（25 个 service、约 10.2k 行）的整体工程质量明显高于典型 Go 项目：16 个编解码器遵循统一的 magic + varint + `readCollectionLen` 防线模式，普遍带上界常量、marker 校验与 trailing-bytes 检查，`delivery_push`/`delivery_submit`/`diagnostics` 三组 codec 的加固是范本级的；本分片非测试代码中 `go func`/`time.New`/`panic(`/`context.Background()` **零命中**，无资源泄漏、无锁问题、出站调用全部带 caller ctx。

最需要优先处理的是**两个漏网的解码器**：`monitor_metrics_codec.go:26-42` 与 `delivery_control_codec.go:41-56` 绕过了本目录自己已经建立的防线约定（前者对 window/step 完全不校验、后者是全目录唯一手写 `int(count)` 而不过 `readCollectionLen` 的地方），使任意方可以用 15 字节的请求把整个进程打崩——`pkg/transport/server.go:180` 的 handler goroutine 没有 recover，而 `pkg/transport` 又完全没有认证/TLS，两者叠加把"恶意 peer"的门槛降到"能连上节点端口"。这两条 P0 加起来的修复量不到 10 行，应当最先合入。

值得注意的是，简报给出的"兄弟 codec 加固不一致"这条侦察线索方向正确但目标错了：被点名怀疑的 `delivery_submit`/`delivery_push` 恰恰是加固最好的，真正的不一致在两个没被点名的文件里——这也说明"逐文件通读 + 横向对比同族实现"确实是这个分片的正确方法。

次优先级是三处权威守卫对 `SlotID==0` 的放行语义（presence register/unregister/heartbeat/replay 与 channel leader repair/transfer）：presence 那条因为 `internal/usecase/presence/authority.go` 没有任何二次校验而可被远程用来删除并替换在线用户的权威路由（P1）；leader repair/transfer 那条则被 `pkg/cluster` 的 raft propose fencing 兜住（降为 P3），但同一个 `slotID == 0 → 放行` 写法应当统一改成"拒绝"。此外若干管理/查询类 RPC（conversation facts 的 Keys 批量、channel messages 的 Limit）把远端参数直通存储层且无上限，是节点级资源放大的主要来源。
