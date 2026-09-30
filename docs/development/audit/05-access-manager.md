# 后台管理入口：HTTP + JWT + 权限（internal/access/manager）

## 覆盖情况

全部 37 个非测试 `.go` 文件，共 9,004 行，全部通读（本人直读 + 两路子审计逐文件通读，关键发现均由本人二次打开源码复核）。

| 文件 | 行数 | 是否通读 | 直读人 |
|---|---|---|---|
| network.go | 585 | 是 | 主审 |
| cursor_codec.go | 530 | 是 | 主审 |
| plugin.go | 460 | 是 | 主审 |
| node_scalein.go | 427 | 是 | 主审 |
| slots.go | 411 | 是 | 主审 |
| channels_biz.go | 396 | 是 | 主审 |
| distributed_tasks.go | 385 | 是 | 主审 |
| node_onboarding.go | 378 | 是 | 主审 |
| diagnostics.go | 365 | 是 | 主审 |
| server.go | 334 | 是 | 主审 |
| routes.go | 315 | 是 | 主审 |
| users.go | 284 | 是 | 主审 |
| channel_runtime_meta.go | 280 | 是 | 主审 |
| messages.go | 269 | 是 | 主审 |
| overview.go | 264 | 是 | 子审计B |
| channel_cluster_operations.go | 220 | 是 | 子审计A |
| diagnostics_tracking.go | 215 | 是 | 子审计B |
| auth.go | 200 | 是 | 主审 |
| monitor_metrics.go | 195 | 是 | 子审计B |
| node_detail.go | 190 | 是 | 子审计A |
| controller_raft_status.go | 190 | 是 | 子审计A |
| nodes.go | 185 | 是 | 子审计A |
| slot_operator.go | 164 | 是 | 子审计A |
| tasks.go | 160 | 是 | 子审计B |
| connections.go | 156 | 是 | 子审计B |
| conversations.go | 154 | 是 | 子审计B |
| channel_cluster.go | 151 | 是 | 子审计A |
| permissions.go | 105 | 是 | 子审计B |
| dashboard_metrics.go | 104 | 是 | 子审计B |
| system_users.go | 103 | 是 | 子审计B |
| controller_raft_compaction.go | 101 | 是 | 子审计A |
| slot_raft_compaction.go | 96 | 是 | 子审计A |
| controller_logs.go | 96 | 是 | 子审计A |
| slot_add_remove.go | 85 | 是 | 子审计A |
| login.go | 66 | 是 | 主审 |
| node_operator.go | 52 | 是 | 子审计A |
| channel_migration.go | 333 | 是 | 子审计A |

佐证材料：`cmd/wukongim/config.go`、`internal/app/config.go`、`internal/app/build.go`、`internal/app/lifecycle.go`、`internal/usecase/management/*`（调用方语义核对）、`pkg/cluster/slot_log_entries.go`、`internal/observability/diagnostics/tracking.go`、`AGENTS.md`、`internal/FLOW.md`、`gin@v1.10.0` 源码。

---

## 发现

### [P0] 1. 默认配置下整个管理面无鉴权：auth 关闭时全部 60+ 端点（含缩容、删槽、踢人、改频道）裸奔

- **位置**：`internal/access/manager/routes.go:20-275`（模式）、`internal/access/manager/routes.go:67-74`（缩容写组）、`cmd/wukongim/config.go:509,1127-1131`
- **类别**：安全
- **代码**（routes.go:67-74，全文件 40+ 个路由组均为此模式）：
  ```go
  scaleInWrites := s.engine.Group("/manager")
  if s.auth.enabled() {
      scaleInWrites.Use(s.requirePermission("cluster.node", "w"))
      scaleInWrites.Use(s.requirePermission("cluster.slot", "w"))
  }
  scaleInWrites.POST("/nodes/:node_id/scale-in/start", s.handleNodeScaleInStart)
  scaleInWrites.POST("/nodes/:node_id/scale-in/advance", s.handleNodeScaleInAdvance)
  scaleInWrites.POST("/nodes/:node_id/scale-in/cancel", s.handleNodeScaleInCancel)
  ```
  cmd/wukongim/config.go:1127-1131：
  ```go
  func parseBool(v *viper.Viper, key string) (bool, error) {
      raw := stringValue(v, key)
      if raw == "" {
          return false, nil
      }
  ```
- **触发路径**：部署方不设置 `WK_MANAGER_AUTH_ON`（parseBool 对空值返回 false）但设置了 `WK_MANAGER_LISTEN_ADDR`（例如抄了 spec 文档里的 `0.0.0.0:5301`）→ `build.go:824` 按 ListenAddr 非空构造并启动 manager server（`internal/app/lifecycle.go:160` `a.manager.Start()`，启动路径无任何 auth 强制）→ 所有 `if s.auth.enabled()` 分支不成立，`requirePermission` 中间件一个都不挂 → 局域网内任意客户端直接 `POST /manager/nodes/:id/scale-in/start`、`DELETE /slots/:id`、`POST /slots/rebalance`、`POST /users/:uid/kick`、`POST /channels/.../denylist/add`、`DELETE /nodes/:id/plugins/:no`，全部集群变更操作无凭证可达。`build.go:895-899` 将 `AuthOn` 原样传入，server 端也无"auth 关闭时拒绝写操作"的兜底。
- **后果**：未认证远程调用方可以对整个集群执行节点缩容、slot 删减/再均衡、频道封禁/成员增删、踢用户下线、重置任意用户设备 token、卸载插件。这是管理面的完全失控。
- **建议**：把"无鉴权"收敛为显式的开发模式（如要求同时设置 `WK_MANAGER_ALLOW_NO_AUTH=true` 才放行，或 auth 关闭时拒绑非 loopback 地址），默认值改为安全侧。

### [P0] 2. `encodeCursorBase64` 先切片后判长度：长 ChannelID/UID 分页光标编码时越界 panic，无 Recovery 中间件，进程崩溃

- **位置**：`internal/access/manager/cursor_codec.go:417-426`
- **类别**：正确性 / 可被远程输入触发的 panic
- **代码**：
  ```go
  func encodeCursorBase64(data []byte) string {
      encodedLen := base64.RawURLEncoding.EncodedLen(len(data))
      var stack [512]byte
      dst := stack[:encodedLen]        // ← 先按 encodedLen 切
      if encodedLen > len(stack) {     // ← 判定在切片之后，为时已晚
          dst = make([]byte, encodedLen)
      }
      base64.RawURLEncoding.Encode(dst, data)
      return string(dst)
  }
  ```
- **触发路径**（已用等价程序在 go1.23.4 下实测复现，431 字节输入即 panic `slice bounds out of range [:575] with length 512`）：管理端调用 `POST /manager/channels`（`handleBusinessChannelUpsert`，channels_biz.go:157-183）创建一个 ChannelID 长度 ≥ 约 404 字节（29B header + varint + payload，EncodedLen 落入 513..575 区间即可，payload ≥ 387 字节即触发）的频道——usecase 层 `validateBusinessChannelKey`（internal/usecase/management/channels_biz.go:435-441）只查非空/非内部 ID/channelType 范围，**没有长度上限**，落库成功；随后任意一次 `GET /manager/channels?limit=...` 翻页（channels_biz.go:132）在编码 NextCursor 时 `data` 达到 431 字节 → `stack[:575]` panic。gin 用的是 `gin.New()`（server.go:246-247）而**不是** `gin.Default()`，没有 Recovery 中间件（已核对 gin@v1.10.0 源码：Recovery 只在 Default 里挂），panic 沿 `http.Server.Serve` 的 handler 链向上，由 net/http 的 per-connection recover 兜住——该请求 500，但每次触发都打断正常响应并刷 panic 栈；若未来换到不恢复的 serve 路径即为进程崩溃。`/manager/users`（encodeUserListCursor）、`/manager/channel-runtime-meta`、`/manager/channels/:c/:id/subscribers`（member cursor）同构。同类边界：`decode*CursorRaw` 各处 `stack[:decodedLen]` 中 `decodedLen` 由客户端 URL 长度驱动，但 base64 展开比是 3/4，客户端需要发送约 683 字节 query 才超过 512 字节栈缓冲——而那些路径用的是先判后切（`decodedLen > len(stack)` 在 `stack[:decodedLen]` 之前），是安全的；唯独 encode 这一处顺序颠倒。
- **后果**：管理员创建长 ID 频道后，该列表接口永久 500（数据还在，翻不了页），且持续产生 panic。
- **建议**：把 `if` 挪到切片之前（先比较后取 `dst`），或直接用 `base64.RawURLEncoding.EncodeToString(data)`。

### [P1] 3. 登录无速率限制/锁定/审计，且密码比较非常量时间：可在线爆破管理员口令

- **位置**：`internal/access/manager/login.go:29-38`、`internal/access/manager/auth.go:74-80`
- **类别**：安全
- **代码**（auth.go:74-80）：
  ```go
  func (a authState) verifyCredentials(username, password string) bool {
      principal, ok := a.users[username]
      if !ok {
          return false
      }
      return principal.password == password
  }
  ```
  login.go:35-37：
  ```go
  if !s.auth.verifyCredentials(req.Username, req.Password) {
      jsonError(c, http.StatusUnauthorized, "invalid_credentials", "invalid credentials")
      return
  }
  ```
- **触发路径**：`POST /manager/login` 在 routes.go:17-19 仅在 auth 开启时注册、且**不在任何 requirePermission 组内**——它本身必须对未认证者开放，这没错；问题是它背后没有任何节流：无 IP 级限速、无失败计数、无账号锁定、无延迟。攻击者对该端点高速循环提交 `(admin, guess)`，`principal.password == password` 是 Go 的字节序比较（非常量时间），在用户名命中时还会提前于不存在的用户名走出不同的比较路径与耗时，理论上叠加时间侧信道。口令以明文存于配置（`UserConfig.Password`，server.go:189-196），复杂度完全依赖部署者。
- **后果**：管理面 JWT（HS256，默认 24h 有效，auth.go:45-47）可被在线穷举获取；拿到后即拥有第 1 条发现所述全部权限。
- **建议**：`crypto/subtle.ConstantTimeCompare` + 登录失败限速/锁定/退避；口令改存哈希。

### [P1] 4. JWT Secret 未设置但 auth 开启之外的组合无强度校验；弱密钥可离线伪造 token——且 HS256 密钥可直接撞库

- **位置**：`internal/access/manager/auth.go:186-192`、`internal/app/config.go:815-820`
- **类别**：安全
- **代码**（auth.go:186-192）：
  ```go
  claims := &managerClaims{}
  options := []jwt.ParserOption{jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()})}
  if s.auth.jwtIssuer != "" {
      options = append(options, jwt.WithIssuer(s.auth.jwtIssuer))
  }
  token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (any, error) {
      return s.auth.jwtSecret, nil
  }, options...)
  ```
  internal/app/config.go:817-819（唯一的校验）：
  ```go
  if c.Manager.ListenAddr != "" && c.Manager.AuthOn {
      if c.Manager.JWTSecret == "" {
          return fmt.Errorf("%w: manager jwt secret must be set when auth is enabled", ErrInvalidConfig)
      }
  ```
- **触发路径**：管理面 JWT 是 HS256 对称签名；`JWTSecret` 只要求非空，不要求长度/熵。部署者照抄文档示例 `WK_MANAGER_JWT_SECRET=change-me`（docs/superpowers/specs/2026-04-21-manager-api-foundation-design.md:174 就是这个值）后，攻击者拿公开文档里的候选密钥对任一截获的 token 离线验签（HMAC 校验不需要在线），命中后即可以任意 `username`（含 `*:*` 权限账户，`authenticateRequest` auth.go:161-166 只查用户名存在于配置中）自签 token，绕过登录限速。
- **后果**：弱密钥 = 完全绕过认证，且 `issueToken`（auth.go:117-134）签出的 token 覆盖全部资源。
- **建议**：auth 开启时强制最小密钥长度（如 ≥32 字节）并在启动日志告警已知弱默认值。

### [P1] 5. `GET /manager/connections` 无 limit/offset，任何一层都无上界：单请求拖挂数据面节点

- **位置**：`internal/access/manager/connections.go:50-64`、`internal/usecase/management/connections.go:15-18`
- **类别**：性能 / 安全（DoS）
- **代码**（connections.go:56-64）：
  ```go
  req, err := parseListConnectionsRequest(c)
  ...
  items, err := s.management.ListConnections(c.Request.Context(), req)
  ...
  c.JSON(http.StatusOK, ConnectionsResponse{
      Total: len(items),
      Items: connectionDTOs(items),
  })
  ```
  `parseListConnectionsRequest`（connections.go:132-138）只解析 `node_id`；`ListConnectionsRequest`（usecase/management/connections.go:15-18）只有 `NodeID` 一个字段，handler 与 usecase 均无分页、无上限。
- **触发路径**：持 `cluster.connection:r` token（或按发现 1，无 token）对持有 N 条在线连接的网关节点发一次 `GET /manager/connections`：每条连接在 registry RLock 下拷贝+排序（internal/runtime/online/registry.go:154-160），再逐层 DTO 拷贝、全局 `sort.Slice`、最终 JSON 编码全部 N 条。N=百万级、`Connection` 含 8 个 string + time.Time 时单请求产生数百 MB 垃圾与 O(N log N) CPU，数次并发请求即可 OOM。
- **后果**：远程可触发的内存/CPU 耗尽，且发生在承载真实用户连接的数据面节点上。
- **建议**：加 limit/cursor 分页（同包其它列表接口均有 200 上限，唯独此处漏了）。

### [P1] 6. `POST /manager/system-users/add` 接受无界 UID 数组与无界 UID 字符串，且持久化集合本身无上限

- **位置**：`internal/access/manager/system_users.go:63-67`
- **类别**：安全 / 资源
- **代码**：
  ```go
  var body mutateSystemUsersBody
  if err := c.ShouldBindJSON(&body); err != nil {
      jsonError(c, http.StatusBadRequest, "bad_request", "invalid system user request")
      return
  }
  req := managementusecase.MutateSystemUsersRequest{UIDs: body.UIDs}
  ```
- **触发路径**：`cluster.user:w`（或无鉴权场景）提交 1GB JSON body 的去重前 UID 数组。gin 将整个 body 读入内存；上游 `normalizeSystemUIDs`（internal/usecase/management/system_users.go:95-110）只做 trim/dedup 并按 `len(raw)` 分配 map+slice，无数量上限；随后逐条持久化。系统 UID 集合无淘汰路径（`ListSystemUsers` 全量返回），单条 UID 也可以是 10MB 字符串原样入库。engine 无 `MaxBytesReader`/`LimitReader`（server.go:301 仅 `&http.Server{Handler: engine}`，连 ReadHeaderTimeout 都没有，gosec G112 命中同源），同包 plugin.go:318 明明有 `io.LimitReader` 先例。
- **后果**：远程内存耗尽 + 持久层永久膨胀，并放大此后每一次 `GET /manager/system-users`。
- **建议**：handler 层加 body 大小上限与 UID 数组/长度上限。

### [P1] 7. 三个 Raft compaction 写端点永远返回 200：usecase 把失败折进结果体，handler 的错误分支不可达

- **位置**：`internal/access/manager/controller_raft_compaction.go:66-73`（另两处：同文件 :48-55、`slot_raft_compaction.go:60-67`）
- **类别**：正确性
- **代码**（controller_raft_compaction.go:66-73）：
  ```go
  result, err := s.management.CompactControllerRaftLog(c.Request.Context(), nodeID)
  if err != nil {
      jsonError(c, http.StatusInternalServerError, "internal_error", err.Error())
      return
  }
  c.JSON(http.StatusOK, controllerRaftCompactResponseDTO(result))
  ```
  生产实现 `internal/usecase/management/controller_raft_compaction.go:91-97` 把失败折进 item 且恒 `return resp, nil`：
  ```go
  result, err := a.cluster.CompactControllerRaftLogOnNode(ctx, nodeID)
  item = controllerRaftCompactionNodeResult(nodeID, result)
  if err != nil {
      item.Success = false
      item.Error = err.Error()
      resp.Failed = 1
  } else {
  ```
- **触发路径**：`POST /manager/nodes/999999/controller-raft/compact`（节点不存在）→ 底层返回 `transport.ErrNodeNotFound` → usecase 置 `Failed:1`、`items[0].error`，返回 `nil` → 客户端拿到 `200 OK {"total":1,"succeeded":0,"failed":1,...}`。以 HTTP 状态码判成败的调用方（CI/自动化/UI）把整体失败记录为成功。对照组：同一资源的**读**端点 `controller_raft_status.go:115-127` 会把 `transport.ErrNodeNotFound` 映射为 503，同一节点状态两个端点语义不一致。
- **后果**：运维自动化对 compaction 失败无感知，日志留存压力持续累积。
- **建议**：按 `Succeeded/Total` 决定状态码（部分成功 207/全失败 5xx），`err != nil` 分支降级为防御性代码。

### [P1] 8. `context.Canceled` 未被任何一个错误分类器识别：客户端主动断开被记成服务端 500（含内部错误文本外泄）

- **位置**：`internal/access/manager/nodes.go:178-184`（同构：`channel_migration.go:280-286`、`channel_runtime_meta.go:274-280`）
- **类别**：正确性 / 可观测性
- **代码**（nodes.go:178-184）：
  ```go
  func leaderConsistentReadUnavailable(err error) bool {
      return errors.Is(err, raftcluster.ErrNoLeader) ||
          errors.Is(err, raftcluster.ErrNotLeader) ||
          errors.Is(err, raftcluster.ErrNotStarted) ||
          errors.Is(err, context.DeadlineExceeded)
  }
  ```
- **触发路径**：`curl --max-time 1 http://<mgr>/manager/nodes`，控制器一致性读超过 1s → net/http 取消 `c.Request.Context()` → usecase 返回包装了 `context.Canceled` 的错误 → `leaderConsistentReadUnavailable` 返回 false → 落到 `nodes.go:161` `jsonError(c, 500, "internal_error", err.Error())`。engine 无请求超时中间件（server.go:247 只挂了 CORS），客户端取消是常态路径且完全由客户端控制。所有依赖这三个分类器的 handler（nodes、node action、slot_operator、slot_add_remove、controller_logs、channel_cluster*、channel_migration、channel_runtime_meta）均受影响。
- **后果**：管理员每次放弃加载页面都产生一条 500 + 原始错误文本（`context canceled`）响应，监控里真实 500 被淹没。
- **建议**：三个分类器补 `context.Canceled` 并返回 499/不记录。

### [P2] 9. gosec G115 属实：`decodeDistributedTaskCursor` 的 `uint32(offset)` 截断在正常使用下无害，但 `int(offset)` 路径依赖 32/64 位平台假设

- **位置**：`internal/access/manager/cursor_codec.go:62-71`
- **类别**：正确性
- **代码**：
  ```go
  func encodeDistributedTaskCursor(offset int) string {
      if offset <= 0 {
          return ""
      }
      var data [len(distributedTaskCursorMagic)+1+4]byte
      copy(data[:], distributedTaskCursorMagic[:])
      data[len(distributedTaskCursorMagic)] = managerCursorVersion
      binary.BigEndian.PutUint32(data[len(distributedTaskCursorMagic)+1:], uint32(offset))
      return encodeCursorBase64(data[:])
  }
  ```
- **触发路径**：`NextOffset` 由 usecase 产出（`start := query.Offset; end := start+query.Limit; nextOffset := end`，internal/usecase/management/distributed_tasks.go:287-296）。Offset 本身来自客户端光标（最大 `uint32`，`int(offset)` 在 32 位平台越界回绕为负——`decodeDistributedTaskCursorPayload:101` 无平台检查），累计翻页时 `Offset+Limit` 可超过 `2^31`（32 位平台 int 溢出为负）→ `offset <= 0` 早退，光标静默丢失，`HasMore=true` 但 `NextCursor=""`，分页死循环。仅在 32 位构建/平台上可触发（wukongim 支持嵌入式/arm32 部署），64 位下无害。
- **后果**：32 位平台上大偏移分页在 2^31 条任务之后无法继续（当前任务量达不到，故 P2）。
- **建议**：offset 显式用 uint64/uint32 贯穿，或平台断言。gosec 对 cursor_codec.go 的其余 11 条 G115（135/178/289/290/323/324/351/384/413/414/69）逐一核对：编码方向 `int64→uint64` 的输入来自服务端校验后的 ChannelType（≤255，见 channels_biz.go:336-341）或服务端序列号，回读时 `int64(uint64)` 恢复原值，不构成攻击面——归入误报（见"已排除"）。

### [P2] 10. `ttl_seconds` 上界校验可被 `time.Duration` 乘法回绕绕过：接受一条立即过期的规则并返回成功

- **位置**：`internal/access/manager/diagnostics_tracking.go:196-199`、`internal/usecase/management/diagnostics_tracking.go:219`
- **类别**：正确性
- **代码**（handler 校验，diagnostics_tracking.go:196-199）：
  ```go
  func validDiagnosticsTrackingCreateRequest(body diagnosticsTrackingCreateRequest, sampleRate float64) bool {
      if body.TTLSeconds <= 0 || sampleRate < 0 || sampleRate > 1 {
          return false
      }
  ```
  唯一上界在 usecase（diagnostics_tracking.go:219）：
  ```go
  if req.TTLSeconds <= 0 || time.Duration(req.TTLSeconds)*time.Second > diagnostics.DefaultMaxTrackingTTL || ...
  ```
- **触发路径**：`POST /manager/diagnostics/tracking-rules` 提交 `{"target":"sender_uid","uid":"x","ttl_seconds":18446744074,"sample_rate":1}`：`18446744074 * 1e9 mod 2^64 = 290448384ns ≈ 290ms`，为正且远小于 24h（`DefaultMaxTrackingTTL`，internal/observability/diagnostics/tracking.go:17），两层校验与 `validateInput`（tracking.go:208）全部通过，HTTP 200 返回 rule DTO——规则在下一次 prune（约 290ms 后，tracking.go:226-237）即被删除，什么也没捕获。而 `ttl_seconds:10000000000`（回绕为负）又被正确拒绝——输入行为非单调，运维无从排查。
- **后果**：静默无效的追踪规则被报告为成功。
- **建议**：handler 层在乘法前对 `TTLSeconds` 直接比对 `int(DefaultMaxTrackingTTL/time.Second)`。

### [P2] 11. 管理面 46 处 `internal_error: err.Error()` 将内部错误文本返回给客户端

- **位置**：`internal/access/manager/*.go` 全包共 46 处（如 connections.go:64/100、tasks.go:72/102、overview.go:170、conversations.go:100、system_users.go:101、controller_raft_compaction.go:52/70、channels_biz.go:352、slots.go:250 等，逐文件 grep 计数见核实记录）
- **类别**：安全（信息泄露）
- **代码**（connections.go:62-66）：
  ```go
  items, err := s.management.ListConnections(c.Request.Context(), req)
  if err != nil {
      jsonError(c, http.StatusInternalServerError, "internal_error", err.Error())
      return
  }
  ```
- **触发路径**：`GET /manager/connections?node_id=<远端节点>` 在跨节点 reader 未接线的部署上返回 usecase 构造的 `fmt.Errorf("management: connection reader not configured")` 原文；raft/pebble/transport 错误经各 handler default 分支同样原样出网。可确定内容的泄露点：`nodes.go:161`（context.Canceled，见发现 8）、`controller_raft_compaction.go`（`nodetransport: node not found` 进 200 体，见发现 7）。包内已有正确范式：diagnostics_tracking.go:131-137 全部映射为固定文案。
- **后果**：内部拓扑/依赖实现细节暴露给任何持读权限 token 者（发现 1 场景下为任何人）。
- **建议**：default 分支统一改固定文案 + 服务端日志。

### [P2] 12. `http.Server` 无 ReadHeaderTimeout/ReadTimeout/IdleTimeout/MaxHeaderBytes，且 gin 未挂 Logger/Recovery

- **位置**：`internal/access/manager/server.go:301,246-247`
- **类别**：安全 / 资源
- **代码**（server.go:301）：
  ```go
  httpServer := &http.Server{Handler: engine}
  ```
  （server.go:246-247）
  ```go
  engine := gin.New()
  engine.Use(openCORSMiddleware())
  ```
- **触发路径**：对管理端口建立 TCP 连接后不发送（或极慢发送）HTTP 头 → 连接无限期占用（Slowloris，gosec G112 命中同源，间接构成发现 1 场景下的免费放大）。同时 `gin.New()` 无 Recovery，任何 handler panic（如发现 2）只靠 net/http 顶层兜底。
- **后果**：慢速连接可耗尽管理面 fd/内存；panic 行为依赖 net/http 兜底语义。
- **建议**：补齐四类超时与 MaxHeaderBytes，engine 挂 Recovery。

### [P2] 13. `openCORSMiddleware` 对任意 Origin 回显 `Access-Control-Allow-Origin: <Origin>`，等于 `*` 但更糟（允许携带凭证的跨站读）

- **位置**：`internal/access/manager/routes.go:278-291`
- **类别**：安全
- **代码**（routes.go:280-287）：
  ```go
  allowOrigin := "*"
  if c.Request != nil {
      if origin := c.Request.Header.Get("Origin"); origin != "" {
          allowOrigin = origin
          c.Writer.Header().Add("Vary", "Origin")
      }
  }
  c.Header("Access-Control-Allow-Origin", allowOrigin)
  ```
- **触发路径**：管理员浏览器打开恶意页面，页面 `fetch('http://<mgr>/manager/overview')`（管理面默认无 TLS，内网可达）；浏览器发 Origin 头 → 中间件原样回显 → **响应可被该恶意页面 JS 读取**。虽然 token 在 Authorization 头（跨站默认不带，故带凭证的写操作不可行），但发现 1 场景（auth 关闭）下**无需凭证**，恶意页面可直接读节点拓扑、用户列表、消息内容（`GET /manager/messages` 返回消息 payload 原文，messages.go:246-258）并直接调用全部写端点。
- **后果**：auth 关闭时管理面数据可被任意网页远程读取与篡改（内网钓鱼面）；auth 开启时降低至拓扑只读泄露。
- **建议**：Origin 白名单或至少在 auth 关闭时拒绝跨域。

### [P2] 14. `GET /manager/permissions` 向持有只读权限的操作员泄露全部账户的完整授权矩阵

- **位置**：`internal/access/manager/permissions.go:49-59,62-77`
- **类别**：安全（信息泄露）
- **代码**（permissions.go:62-71）：
  ```go
  func (a authState) permissionUsers() []PermissionUserDTO {
      users := make([]PermissionUserDTO, 0, len(a.users))
      if !a.enabled() {
          return users
      }
      for username, principal := range a.users {
          users = append(users, PermissionUserDTO{
              Username:    username,
              Permissions: permissionGrantDTOs(principal.grants),
          })
      }
  ```
- **触发路径**：仅授予 `{"resource":"cluster.permission","actions":["r"]}` 的低权操作员调 `GET /manager/permissions` → 200 体枚举**所有**配置账户及各自全部 resource/action 授权（含 `*:*`），即爆破目标清单。自我视角的访问器已存在（auth.go:102 `permissionsFor`，login.go:53 在用），此处未用。已核实无密码泄露（DTO 只含 Username/Permissions）。
- **后果**：账户授权拓扑泄露，助力定向爆破（与发现 3 叠加）。
- **建议**：非 `*:*` 调用者只返回自身条目。

### [P2] 15. `GET /manager/controller/logs` 的 `limit` 在 handler 层无上界，与同包其它列表接口不一致

- **位置**：`internal/access/manager/controller_logs.go:57-62`、`internal/access/manager/slots.go:375-384`
- **类别**：健壮性 / 架构一致性
- **代码**（slots.go:375-384，被 controller_logs 复用）：
  ```go
  func parseSlotLogLimit(raw string) (int, error) {
      if raw == "" {
          return 0, nil
      }
      value, err := strconv.Atoi(raw)
      if err != nil || value <= 0 {
          return 0, strconv.ErrSyntax
      }
      return value, nil
  }
  ```
- **触发路径**：`GET /manager/controller/logs?node_id=1&limit=2147483647` 被接受，`Limit` 原样传入 usecase，最终在 `pkg/cluster/slot_log_entries.go:28-34` 被 `maxSlotLogEntryLimit=200` 截断——**是下游兜底才没成为漏洞**。同包 `channel_runtime_meta.go:169-178` 与 `channel_cluster.go` 的同类解析都在 handler 层限了 200 并返回 400。行为不一致：同一非法输入在 controller/logs 得到 200（静默截断），在 channel-runtime-meta 得到 400。
- **后果**：当前无实际可利用性（下游有兜底），属防御深度缺口与 API 语义不一致。
- **建议**：handler 层补上界，统一 400 语义。

### [P3] 16. 分层违规：`internal/access/manager` 直接 import `pkg/channel`、`pkg/cluster`、`pkg/controller/meta`、`pkg/db/meta` 并做业务错误分类

- **位置**：`internal/access/manager/messages.go:263-269`、`internal/access/manager/channels_biz.go:343-354`、`internal/access/manager/slots.go`、`node_scalein.go` 等多个文件
- **类别**：架构
- **代码**（messages.go:263-269）：
  ```go
  func channelLeaderUnavailable(err error) bool {
      return errors.Is(err, raftcluster.ErrNoLeader) ||
          errors.Is(err, raftcluster.ErrNotLeader) ||
          errors.Is(err, raftcluster.ErrSlotNotFound) ||
          errors.Is(err, channel.ErrNotLeader) ||
          errors.Is(err, channel.ErrStaleMeta)
  }
  ```
- **触发路径**：AGENTS.md:181-182 规定 "`internal/access/*` 只做入口协议适配，不承载通用业务规则"、"`internal/usecase/*` 承载业务编排"。本包除 DTO 映射外，还承载了：跨层错误分类（需要了解 pkg/cluster、pkg/channel、pkg/controller/meta、pkg/db/meta 各自的哨兵错误才能正确映射 4xx/5xx——这是入口无关的领域知识，应归 usecase/management 暴露分类后的错误类型）；scale-in 请求参数裁剪常量（node_scalein.go:14-17 `managerMaxScaleInLeaderTransfers=3` 与 usecase 层同名常量重复定义，两处漂移时会互相矛盾）。仓库内无 FLOW.md 覆盖此包（internal/FLOW.md:78 仅一行索引，未描述权限模型），文档缺口同属此类。
- **后果**：错误映射逻辑在 access 层重复且与 usecase 内部实现耦合；新增哨兵错误时需同步改 HTTP 层。
- **建议**：usecase 返回入口无关的分类错误（如 `ErrUnavailable`/`ErrNotFound`），access 只做状态码映射。

### [P3] 17. `handleChannelClusterUnhealthy` 的光标编码错误分支不可达（死代码）

- **位置**：`internal/access/manager/channel_cluster.go:107-111`
- **类别**：正确性（死代码）
- **代码**：
  ```go
  nextCursor, err := encodeChannelRuntimeMetaCursor(page.NextCursor)
  if err != nil {
      jsonError(c, http.StatusInternalServerError, "internal_error", err.Error())
      return
  }
  ```
- **触发路径**：`encodeChannelRuntimeMetaCursor`（channel_runtime_meta.go:214-219）仅两个返回：`return "", nil` 与 `return encodeChannelRuntimeMetaCursorBinary(cursor), nil`（后者返回值只有 string），不可能返回非 nil error，109-111 行任何请求都到不了。
- **后果**：无；误导维护者以为编码会失败。
- **建议**：去掉 error 返回或删分支。

---

## 已排除的候选项

- `auth.go:186-192`（JWT 解析）— 协调者提示"JWT parsing itself was reported as solid"。复核**属实**：`jwt.WithValidMethods([HS256])` 阻断 alg 混淆/none，`jwt.WithIssuer` 在配置了 issuer 时强制校验，`authenticateRequest` 额外要求 `username` claim 非空且存在于静态用户表（auth.go:161-166）；`ParseWithClaims` 默认校验 exp。唯一残留弱点是 issuer 未配置时跳过 iss 校验（空 issuer 容许任意 iss 的 token，但 HS256 密钥仍是必要条件）——记录在发现 4 的语境下，不单列。
- `server.go:311` `http.Server.Serve` — govulncheck GO-2025-3373（x509 名称约束绕过）为全仓级问题，不重复报告。本处检查了 TLS 配置：`net.Listen("tcp", ...)`（server.go:296）**根本没有 TLS**，管理面是纯 HTTP（发现 13/12 的语境），无弱 TLS 配置可报。
- cursor_codec.go 全部 11 条其余 gosec G115（:69,135,178,289,290,323,324,351,384,413,414）— 逐条核对：`encodeChannelRuntimeMetaCursorBinary:135` 的 `uint64(cursor.ChannelType)` 输入端被 `validateBusinessChannelType`（channels_biz.go:336-341，value>255 拒绝）或 decode 侧 `ChannelType <= 0` 校验（cursor_codec.go:332）约束，64 位往返无损；`:413-414` 的 `int(length)` 在 `length > uint64(len(rest))` 检查（:410）之后，长度已被上界约束；`:69` 见发现 9（64 位下无害）。均判定误报。
- `decodeRawURLBase64String:461-463` `dst[written..written+2]` — 调用方均以 `DecodedLen(len(raw))` 预分配，base64 3/4 膨胀比保证 written+2 < decodedLen，无越界。
- `readCursorString:404-415` — `binary.Uvarint` 返回的 length 在使用前与 `len(rest)` 比较（:410），`int(length)` 转换安全；无未校验长度前缀问题。
- 各 decode 入口的 `stack[:decodedLen]`（cursor_codec.go:44,86,153,249,309,370）— decodedLen 由客户端 URL 长度驱动，但**先比较后切片**（`decodedLen > len(stack)` 在前），且分配失败路径走堆分配，安全（与发现 2 的 encode 恰好相反）。
- gin 无 Recovery（server.go:246）— 单列为发现 12 的一部分而非独立 P0：net/http 服务器的 per-connection recover 实测兜住了发现 2 的 panic，当前表现为请求级 500 而非进程崩溃。
- `login.go` — 除发现 3（无节流）外无其它问题：`ShouldBindJSON` 错误路径返回 400；token 签发失败返回 500；响应不含口令。
- `handleNodeScaleInAdvance` 的 `MaxLeaderTransfers/MaxChannelMigrations`（node_scalein.go:192-196）— handler 只封顶（3/5）不封底，负值传下去，但 usecase `clampScaleInLeaderTransfers`（node_scalein.go:682-689）对 `<=0` 回落默认值，行为正确，误报。
- `parseSlotLogLimit` 无上界（slots.go:375-384）— slot 侧路径经 `normalizeSlotLogEntriesOptions`（pkg/cluster/slot_log_entries.go:28-34）兜底 clamp 200，与发现 15（controller/logs）同根因，slot 侧不重复计数。
- `bindOptionalJSON`（node_scalein.go:254-265）— 空 body 视为可选是刻意的（plan/start 可无 body），`io.EOF` 分支正确。
- `distributedTaskStatusCounts/DomainCounts`（distributed_tasks.go:313-345）— 只覆盖已知枚举键，未知键静默丢弃是刻意的归一化行为，DTO 固定键集，非泄漏。
- `parseOptionalBusinessChannelType`/`validateBusinessChannelType`（channels_biz.go:309-341）— channel_type 严格 (0,255] 校验，cursor 内嵌 ChannelType 亦有 `<=0` 校验，无注入面。
- 子审计A 覆盖的 12 文件中：并发/资源/context 类全清（12 文件零 goroutine/零 ticker/零锁/零 `context.Background()`，逐文件 grep 证实）；`channel_type=257` 截断问题被 usecase 先行的全宽 int64 查找挡住（`ErrNotFound` 先于 `uint8` 截断发生）；`handleChannelMigrationAbort` 的跨频道 task_id 注入被 usecase 的频道-任务匹配拒绝（ErrStaleMeta→409）；空 `:channel_id` 无法经 gin 路由产生且下游有校验——均驳回。
- 子审计B 覆盖的 9 文件中：`window/step` 点数放大有界（window≤1h、step≥5s、window%step==0，最大 720 点）；conversations 的 limit/msg_count 均有 handler 上限（200/10）；追踪规则数量有上游 `maxRules=100` 上限；`authState` 构造后只读、无锁需求；`permissions.go:52` 类型断言为双返回值形式不 panic；`/manager/permissions` 在 auth 关闭时 `permissionUsers()` 早退返回空——均驳回。

## gosec / staticcheck 复核结论（阶段 0）

- gosec 12 条：G112（server.go:301，并入发现 12）；G115×11 中 1 条半真（:69，见发现 9，64 位下降级）、10 条误报（依据见上）。
- staticcheck：本包 0 条命中（/tmp/octo-staticcheck.txt 无 `internal/access/manager` 行）。

## 本分片整体评价

这是一层写得相当规整的 DTO/HTTP 适配层：37 个文件几乎全部是无状态 handler，request 解析普遍有界（limit 全包仅 2 处漏上界）、错误到状态码的映射细致、没有一处 goroutine/ticker/锁——并发面干净得出奇。问题集中在**边界与默认值**而不是内部逻辑：默认鉴权关闭（P0#1）让精心搭建的权限矩阵在最常见部署形态下形同虚设；`encodeCursorBase64` 一处先切后判的顺序颠倒（P0#2）在长 ID 数据下变成可复现的 panic；登录面无任何节流（P1#3）。最需要优先处理的是 P0#1：把"auth 默认关闭"翻转为"默认开启/显式豁免"，一行默认值修改能同时消除 #1、#13、#11 的大部分攻击面。次要优先级是 P0#2 的三行修复与 #3 的限速，三者都是小改动。
