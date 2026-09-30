# 客户端入口：HTTP API + gateway 适配 + 主程序

单元 SLUG: `04-access-api-gateway-cmd`。审计对象为生产运行路径上的对外入口（v1 栈，`internal/app` 组合根接入，非 `internalv2`）。

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/access/api/routes.go | 77 | 是 |
| internal/access/api/server.go | 322 | 是 |
| internal/access/api/cors.go | 35 | 是 |
| internal/access/api/debug.go | 52 | 是 |
| internal/access/api/health.go | 28 | 是 |
| internal/access/api/error_map.go | 42 | 是 |
| internal/access/api/user_token.go | 39 | 是 |
| internal/access/api/user_legacy.go | 132 | 是 |
| internal/access/api/message_send.go | 123 | 是 |
| internal/access/api/message_sync.go | 84 | 是 |
| internal/access/api/channel_messagesync.go | 74 | 是 |
| internal/access/api/channel_management.go | 504 | 是 |
| internal/access/api/conversation_sync.go | 192 | 是 |
| internal/access/api/conversation_legacy_model.go | 182 | 是 |
| internal/access/api/route.go | 123 | 是 |
| internal/access/api/plugin.go | 147 | 是 |
| internal/access/api/diagnostics.go | 92 | 是 |
| internal/access/api/bench.go | 152 | 是 |
| internal/access/api/testdata.go | 81 | 是 |
| internal/access/gateway/handler.go | 173 | 是 |
| internal/access/gateway/frame_router.go | 274 | 是 |
| internal/access/gateway/lifecycle.go | 97 | 是 |
| internal/access/gateway/mapper.go | 105 | 是 |
| internal/access/gateway/encryption.go | 44 | 是 |
| internal/access/gateway/error_map.go | 33 | 是 |
| internal/access/gateway/online_session_adapter.go | 76 | 是 |
| cmd/wukongim/main.go | 55 | 是 |
| cmd/wukongim/config.go | 1185 | 是（分段：1-120、325-560、826-1010、1080-1185 全读，560-826 为重复 parse 调用区，按关键字 grep 定位通读） |

关联上下文（非本分片，仅为闭合证据链而读）：`pkg/gateway/auth.go`、`internal/app/config.go:791-850`、`internal/app/build.go:941-975`、`internal/usecase/user/{token.go,command.go,legacy.go}`、`internal/usecase/message/{permission.go,send.go}`、`internal/usecase/conversation/sync.go`、`internal/usecase/channel/app.go`、`wukongim.conf.example:210-270`。

FLOW.md：`internal/access/`、`cmd/` 下无 FLOW.md（`internal/FLOW.md` 为父层概述，未发现与本分片代码矛盾之处）。

---

## 发现

### [P0] 1. 客户端网关认证在编译期即不可启用：`WK_GATEWAY_TOKEN_AUTH_ON=true` 直接导致启动失败，默认部署任何客户端可冒充任意 UID

- **位置**：`cmd/wukongim/config.go:337`；`internal/app/config.go:811-813`；`internal/app/build.go:972`；`pkg/gateway/auth.go:59-74`；`internal/access/gateway/mapper.go:14-17`
- **类别**：安全
- **代码**：
  ```go
  // internal/app/config.go:811-813 —— 唯一的门是"禁止打开"
  if c.Gateway.TokenAuthOn {
      return fmt.Errorf("%w: gateway token auth requires verifier hooks", ErrInvalidConfig)
  }
  ```
  ```go
  // internal/app/build.go:972 —— 构造认证器时不传任何校验钩子
  Authenticator:  gateway.NewWKProtoAuthenticator(gateway.WKProtoAuthOptions{TokenAuthOn: cfg.Gateway.TokenAuthOn, NodeID: cfg.Node.ID}),
  ```
  ```go
  // pkg/gateway/auth.go:60-64 —— TokenAuthOn 恒为 false，此分支永不为真
  if opts.TokenAuthOn && !isVisitor(opts.IsVisitor, connect.UID) {
      if connect.Token == "" || opts.VerifyToken == nil {
          return &AuthResult{Connack: &frame.ConnackPacket{ReasonCode: frame.ReasonAuthFail}}, nil
      }
  ```
  ```go
  // internal/access/gateway/mapper.go:14-17 —— 会话里的 UID 即发送者身份，而 UID 来自 connect 包自报
  senderUID, _ := ctx.Session.Value(coregateway.SessionValueUID).(string)
  if senderUID == "" {
      return message.SendCommand{}, ErrUnauthenticatedSession
  }
  ```
- **触发路径**：
  1. 运维按注释提示（`wukongim.conf.example:266` `# WK_GATEWAY_TOKEN_AUTH_ON=false`）把该值改为 `true` 想启用 token 校验 → `loadConfig` → `ApplyDefaultsAndValidate`（`internal/app/config.go:811`）返回 `gateway token auth requires verifier hooks` → **进程拒绝启动**。生产二进制中 token 认证是无解的死开关（`internal/app` 全目录只有 `build.go:972` 一处构造 Authenticator，从不注入 `VerifyToken`）。
  2. 保持默认（false）：任何 TCP 客户端连 `0.0.0.0:5100`（`cmd/wukongim/config.go:998-1000` 默认监听）发 CONNECT 帧填 `UID=<受害者uid>` → `auth.go:102-108` 直接把该 UID 写入会话 → 该连接即可以受害者身份收发消息、发 RECVACK 清投递回执（`frame_router.go:247-253`）、并在 presence 注册为受害者在线。
- **后果**：完整的身份冒充。配合本报告第 3 条（`/user/systemuids_add`）还可以把冒充的 UID 升级为系统账号绕过权限检查。这是本单元最重要的结论：**回答协调者的问题——是的，`TokenAuthOn` 为 false（且实际只能是 false）时，客户端网关接受任意 UID 声明，可完全冒充他人收发消息。**
- **建议**：要么给组合根接上真实的 token 校验 usecase 并放开校验报错，要么删掉这个假开关并在文档中明确"网关认证由业务后端负责"的信任模型。

### [P0] 2. 无认证 `/user/token` 可为任意（甚至尚不存在的）用户签发/覆盖 token

- **位置**：`internal/access/api/routes.go:55`；`internal/access/api/user_token.go:18-38`；`internal/usecase/user/token.go:21-33`
- **类别**：安全
- **代码**：
  ```go
  // routes.go:55 —— 无认证平面上的注册
  s.engine.POST("/user/token", s.handleUpdateToken)
  ```
  ```go
  // internal/usecase/user/token.go:21-33 —— 唯一的防线是单个内置系统账号
  if a != nil && a.systemUID != "" && cmd.UID == a.systemUID {
      return errors.New("系统账号不允许更新token！")
  }
  ...
  _, err := a.users.GetUser(ctx, cmd.UID)
  if errors.Is(err, metadb.ErrNotFound) {
      if err := a.users.CreateUser(ctx, metadb.User{UID: cmd.UID}); err != nil && !errors.Is(err, metadb.ErrAlreadyExists) {
  ```
- **触发路径**：`POST /user/token {"uid":"victim","token":"attacker-controlled","device_flag":1}`（无任何认证头）→ 若 victim 不存在则**创建**该用户并写入攻击者指定的 token；若存在则**覆盖**其原 token。唯一受保护的是配置里指定的一个 `SystemUID`（`internal/app/build.go` 未显式配置时该字段为空，此时连这个保护都没有）。`UpdateTokenCommand.Validate`（`internal/usecase/user/command.go:30-40`）只校验非空和 `@#&` 三个字符，无长度上限。
- **后果**：认证凭据完全由匿名调用方决定。当前因发现 1（网关 token 校验恒关闭）此 token 暂不参与网关认证，但上游 WuKongIM 客户端协议与后续启用认证的部署会直接采信它；同时也是把"用户注册"这一管理操作暴露在无认证平面上。
- **建议**：将 `/user/token` 与业务后端之间加共享密钥或移入 manager 面；对 token 设长度/格式上限。

### [P0] 3. 无认证 `/user/systemuids_add` 可把任意 UID 升级为"系统账号"，绕过发送权限检查与（可选）限流

- **位置**：`internal/access/api/routes.go:59-66`；`internal/access/api/user_legacy.go:58-68`；`internal/usecase/message/permission.go:15-19,138-142`；`internal/usecase/message/send.go:195`
- **类别**：安全
- **代码**：
  ```go
  // routes.go:61-66 —— 全部无认证
  s.engine.POST("/user/systemuids_add", s.handleSystemUIDsAdd)
  s.engine.POST("/user/systemuids_remove", s.handleSystemUIDsRemove)
  s.engine.GET("/user/systemuids", s.handleSystemUIDsGet)
  s.engine.POST("/user/systemuids_add_to_cache", s.handleSystemUIDsAddToCache)
  s.engine.POST("/user/systemuids_remove_from_cache", s.handleSystemUIDsRemoveFromCache)
  ```
  ```go
  // internal/usecase/message/permission.go:15-19 —— 系统账号直接放行全部发送权限检查
  return frame.ReasonSuccess, nil
  }
  if a.systemUIDs != nil && a.systemUIDs.IsSystemUID(cmd.FromUID) {
      return frame.ReasonSuccess, nil
  }
  ```
  ```go
  // internal/usecase/message/send.go:195 —— 系统账号同时进入限流决策
  IsSystemUID: a.isSystemUID(cmd.FromUID),
  ```
- **触发路径**：`POST /user/systemuids_add {"uids":["attacker"]}`（无认证）→ UID 持久化为系统账号并进本地缓存 → 此后 attacker 以该 UID 发送的消息跳过 `checkSenderSendPermission`（黑名单/允许名单等全部检查，`permission.go:17-19` 提前 return），接收端对系统账号的拒收检查也被跳过（`permission.go:138-142`）；若 `WK_MESSAGE_USER_RATE_LIMIT_SYSTEM_UID_BYPASS=true` 还绕过用户限流。`uids` 数组无长度上限（见发现 4）。
- **后果**：匿名攻击者一行请求即可为自己铸造特权身份，使频道黑名单、个人频道权限模型全部失效。
- **建议**：系统账号管理必须移出无认证平面（manager 面或共享密钥），缓存版 `*_to_cache` 同理。

### 附：无认证爆炸半径逐路由清单（配合协调者已核实的"无认证中间件"结论）

`internal/access/api/routes.go:45-76` 全部路由无认证，逐条能力如下：

| 路由 | 匿名调用方可做的事 |
|---|---|
| `/healthz`,`/healthz/details`,`/readyz`,`/metrics` | 读健康/指标；`healthDetails`（`WK_HEALTH_DETAIL_ENABLE` 默认 true，示例配置 :256）暴露内部细节 |
| `/route`,`/route/batch` | 读集群网关地址（含 `intranet=1` 时**内网地址**，`route.go:75-83`）；`/route/batch` 回显任意长度 uids 数组 |
| `/user/token` | 发现 2：创建用户+签发/覆盖任意 token |
| `/user/device_quit` | 踢任意 uid 的任意 device 下线（`user_legacy.go:20-34`），DoS 单用户 |
| `/user/onlinestatus` | 枚举任意 uid 列表的在线状态（用户在线情报），数组无上限 |
| `/user/systemuids_*` | 发现 3：铸造/撤销特权身份；`GET /user/systemuids` 枚举现有特权账号 |
| `/channel`,`/channel/info`,`/channel/delete` | 匿名创建/改/删任意频道（`channel_management.go:53-107`），删频道=摧毁群组 |
| `/channel/subscriber_*`,`/tmpchannel/subscriber_set` | 匿名增删任意频道订阅者（把任意人拉进/踢出任意群） |
| `/channel/blacklist_*`,`/whitelist_*`（含 `remove_all`） | 匿名操纵任意频道的黑/白名单；`whitelist_get` 读白名单 |
| `/message/send` | 以任意 `from_uid` 向任意频道发消息（即发现 1 的 HTTP 版本，无需连网关） |
| `/message/sync`,`/channel/messagesync` | 读**任意 uid / 任意频道**的历史消息全文（含 payload，`message_sync.go:42-56`、`channel_messagesync.go:39-48`）——全量消息泄露 |
| `/message/syncack` | 以任意 uid 确认同步位点，破坏其 CMD 同步状态 |
| `/conversations/clearUnread`,`/setUnread`,`/delete`,`/conversation/sync` | 读/改/删**任意用户**的会话与未读状态 |

结论：这不是"个别敏感接口漏了认证"，而是**整台 5001 端口上不存在认证概念**；其中 `/message/sync`（任意用户历史消息）、`/channel/delete`、`/user/token`、`/user/systemuids_add` 四类已构成完整的匿名 takeover 链。

### [P1] 4. 业务 API 全部路由无请求体大小限制、无数组长度上限（仅 bench/plugin 两个旁路有）

- **位置**：`internal/access/api/message_send.go:41`；`internal/access/api/channel_management.go:53-58,313-335`；`internal/access/api/user_legacy.go:36-56`；对比 `internal/access/api/bench.go:126`、`internal/access/api/plugin.go:21`
- **类别**：安全 / 资源
- **代码**：
  ```go
  // message_send.go:41 —— 无 MaxBytesReader，body 无上限
  if err := c.ShouldBindJSON(&req); err != nil {
  // bench.go:125-126 —— 只有 bench 旁路做了限制
  if s.benchMaxPayloadBytes > 0 {
      c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, s.benchMaxPayloadBytes)
  ```
  ```go
  // channel_management.go:29-35 —— Subscribers []string 无任何数量上限
  type channelSubscriberRequest struct {
      ChannelID      string   `json:"channel_id"`
      ChannelType    uint8    `json:"channel_type"`
      Reset          int      `json:"reset"`
      TempSubscriber int      `json:"temp_subscriber"`
      Subscribers    []string `json:"subscribers"`
  }
  ```
- **触发路径**：
  1. `POST /channel/subscriber_add {"channel_id":"x","channel_type":1,"subscribers":[<1000万条>]}` → `ShouldBindJSON` 把整个 body 解析进内存 → `forEachSubscriberChunk`（`internal/usecase/channel/app.go:317-333`）虽按 `subscriberPageLimit` 分块，但整个数组已驻留内存；单请求内存放大无界。
  2. `/user/onlinestatus`、`/route/batch`、`/user/systemuids_*` 同样接受任意长度数组。
  3. `message_send.go` 的 `FromUID/ChannelID/ClientMsgNo/token`（发现 2 的 token 字段）无长度上限——这是协调者已确认 P0 的入口侧（协议侧 panic 属 Unit 32）。
- **后果**：匿名内存放大与下游写放大；一个几 MB 的请求可在存储中造成数百万订阅者写入。
- **建议**：所有业务路由统一挂 `http.MaxBytesReader`；数组字段加上限（如 1 万）；字符串字段加长度上限。

### [P1] 5. `/conversation/sync` 的 `msg_count` 无上限：单次匿名请求触发最多 500 个频道 × 1MB 的近期消息批量拉取

- **位置**：`internal/access/api/conversation_sync.go:39-55`；`internal/usecase/conversation/sync.go:76-95`；`internal/app/build.go:1302,1388`
- **类别**：安全 / 性能
- **代码**：
  ```go
  // conversation_sync.go:39-45 —— limit 有 clamp，msg_count 没有
  limit := req.Limit
  if limit <= 0 {
      limit = s.conversationDefaultLimit
  }
  if s.conversationMaxLimit > 0 && limit > s.conversationMaxLimit {
  ```
  ```go
  // internal/usecase/conversation/sync.go:77-83 —— MsgCount 原样进入批量拉取
  if query.MsgCount > 0 {
      if batchFacts, ok := a.facts.(recentMessageBatchLoader); ok {
          ...
          recentsByKey, err := batchFacts.LoadRecentMessagesBatch(ctx, keys, query.MsgCount)
  ```
- **触发路径**：`POST /conversation/sync {"uid":"u","limit":500,"msg_count":100000}`（无认证）→ `views` 被 clamp 到 500，但对这 500 个 key 逐个调用 `loadRecentConversationMessages`（`internal/app/build.go:1388`），每个频道最多拉取 `conversationFetchMaxBytes = 1 << 20`（`build.go:1302`）字节 → 单请求峰值约 500MB 消息体组装 + 响应序列化。协程级并发即可打满内存/带宽。
- **后果**：匿名内存/带宽放大（单请求 ~500MB 上界），多请求并发可致 OOM。
- **建议**：在入口对 `msg_count` 设上限（如 ≤100），或对单请求 recents 总字节数设预算。

### [P1] 6. `WK_MANAGER_AUTH_ON` 代码默认 `false`，示例配置写 `true`：不照抄示例的部署拿到无认证管理面

- **位置**：`cmd/wukongim/config.go:509`；`cmd/wukongim/config.go:1127-1138`；`wukongim.conf.example:225-226`
- **类别**：安全 / 配置
- **代码**：
  ```go
  // cmd/wukongim/config.go:509
  managerAuthOn, err := parseBool(v, "WK_MANAGER_AUTH_ON")
  // cmd/wukongim/config.go:1127-1133 —— unset 静默取 false，不告警
  func parseBool(v *viper.Viper, key string) (bool, error) {
      raw := stringValue(v, key)
      if raw == "" {
          return false, nil
      }
  ```
  ```
  # wukongim.conf.example:225-226
  WK_MANAGER_LISTEN_ADDR=0.0.0.0:5301
  WK_MANAGER_AUTH_ON=true
  ```
- **触发路径**：部署者按需设置 `WK_MANAGER_LISTEN_ADDR=0.0.0.0:5301` 但不知道还要设 `WK_MANAGER_AUTH_ON`（代码不报错、不告警）→ `internal/app/build.go:824` 判定 manager 启用、`build.go:894` `On: cfg.Manager.AuthOn=false` → 5301 管理面完全无认证（管理面路由细节属 Unit 05）。示例配置与代码默认的不一致到行号确认：**代码默认 false（config.go:509+1129-1131），示例写 true（example:226），二者矛盾。**
- **后果**：只要管理员开了 manager 端口而漏开 auth，管理面即裸奔；且 parseBool 家族对 unset 一律静默取零值，同一模式使以下安全相关 flag 默认 insecure：`WK_GATEWAY_TOKEN_AUTH_ON=false`（且如发现 1 根本无法开）、`WK_MESSAGE_USER_RATE_LIMIT_ENABLED=false`（默认无限流，config.go:529-531）、`WK_MESSAGE_PERSON_WHITELIST_ENABLED=false`（config.go:521-523）、`WK_TEST_MODE=false`。
- **建议**：`WK_MANAGER_AUTH_ON` 在设置了 listen addr 且未显式设置时要么默认 true 要么启动告警；parseBool 可保留但对安全 flag 增加显式 unset 检测。

### [P2] 7. 公开 API 的 `http.Server` 未设 `ReadHeaderTimeout`（gosec G112，真实）

- **位置**：`internal/access/api/server.go:277`
- **类别**：安全 / 资源
- **代码**：
  ```go
  httpServer := &http.Server{Handler: engine}
  ```
- **触发路径**：攻击者与 `0.0.0.0:5001` 建立 TCP 连接后缓慢发送/不发 HTTP 头 → 每个半开连接占住一个 goroutine 与连接，无超时回收 → 大量并发慢连接耗尽 fd/goroutine（Slowloris）。manager/gateway 有各自的超时治理，唯独这条入口没有。
- **后果**：单节点 DoS 窗口；goroutine/fd 泄漏型降级。
- **建议**：补 `ReadHeaderTimeout`（以及可选 Read/Write/IdleTimeout）。

### [P2] 8. `/debug/pprof/*` 与 `/debug/goroutines` 挂在同一无认证公开引擎上，单配置位即可暴露

- **位置**：`internal/access/api/debug.go:26-52`；`internal/access/api/server.go:190-191`
- **类别**：安全
- **代码**：
  ```go
  func (s *Server) registerDebugRoutes() {
      ...
      s.engine.GET("/debug/goroutines", s.handleDebugGoroutines)
      s.engine.GET("/debug/pprof/heap", gin.WrapF(pprof.Index))
      s.engine.GET("/debug/pprof/profile", gin.WrapF(pprof.Profile))
  ```
- **触发路径**：运维把 `WK_HEALTH_DEBUG_ENABLE=true`（`internal/app/build.go:941` 是它唯一的门）→ pprof 全套暴露在 `0.0.0.0:5001` 且无认证 → 匿名拉 `/debug/pprof/heap` 得到堆快照（**堆里含近期消息 payload、UID、token 等运行时数据**），`/debug/pprof/profile?seconds=60` 可钉住 CPU。默认关闭故降为 P2，但开关节与业务 API 同一端口同一信任域，没有独立的内部网络门。
- **后果**：配置翻转即消息内容/凭据泄露 + 性能钉死原语。
- **建议**：pprof 单独监听 localhost 端口或加内部令牌。

### [P3] 9. `openCORSMiddleware` 反射任意 Origin——诚实评估：在本信任模型下基本 moot，但会随发现 8 放大 debug 面

- **位置**：`internal/access/api/cors.go:15-22`
- **类别**：安全
- **代码**：
  ```go
  allowOrigin := "*"
  if c.Request != nil {
      if origin := c.Request.Header.Get("Origin"); origin != "" {
          allowOrigin = origin
  ```
- **触发路径**：任意网页 JS `fetch('http://victim-node:5001/message/sync', {method:'POST', body:...})` → 浏览器允许读取响应（无凭据也无需凭据）。**评估**：该 API 无 cookie、无认证，浏览器同源策略本来就挡不住跨站发请求；反射 Origin 唯一新增的能力是让任意网页**读到**响应，而这些响应对所有有网络可达性的调用方本来就可读。因此**不构成独立漏洞**；但当发现 8 的 debug 开关打开时，它使任意网页可静默拉取受害者内网节点的堆快照/指标（浏览器作为跳板进入内网），这是它唯一的增量风险。
- **建议**：保持现状可接受；若收紧，固定 `*` 或可配置白名单即可，无需 credentialed-CORS 级别的修复。

### [P3] 10. Set 系列成员接口跳过 `ChannelType` 校验，与 Add/Remove 系列行为不一致，可写入 type=0 的孤儿名单

- **位置**：`internal/access/api/channel_management.go:325-335`（对照 `:432-446`）
- **类别**：正确性
- **代码**：
  ```go
  // bindLegacyChannelMemberSet —— 只查 ChannelID，不查 ChannelType
  if strings.TrimSpace(req.ChannelID) == "" {
      writeLegacyJSONError(c, "频道ID不能为空！")
      return false
  }
  ```
  ```go
  // validateChannelMember:432-438 —— Add/Remove 系列要求 type != 0
  if req.ChannelID == "" {
      return "channel_id不能为空！"
  }
  if req.ChannelType == 0 {
      return "频道类型不能为0！"
  }
  ```
- **触发路径**：`POST /channel/blacklist_add {"channel_id":"x","channel_type":0,"uids":["u"]}` → 400"频道类型不能为0！"；`POST /channel/blacklist_set {"channel_id":"x","uids":["u"]}`（无 type）→ 200 成功，下游 `setMemberList`（`internal/usecase/channel/app.go:235-241`，无类型校验）把名单写进 `ChannelType=0` 的命名空间——一个任何正常频道查询都不会读的孤儿 key。同一输入一个端点拒绝、另一个端点接受并落库。
- **后果**：校验语义不一致 + 存储孤儿数据；暂无已知越权后果（因 type=0 名单无人读取），故 P3。
- **建议**：`blacklist_set/whitelist_set` 复用 `validateChannelMember`。

### [P3] 11. 多个 handler 把内部 `err.Error()` 原样返回给客户端

- **位置**：`internal/access/api/message_sync.go:48`；`internal/access/api/channel_messagesync.go:50`；`internal/access/api/user_legacy.go:52` 等
- **类别**：安全（信息泄露）
- **代码**：
  ```go
  result, err := s.cmdSync.Sync(c.Request.Context(), cmdsync.SyncQuery{...})
  if err != nil {
      writeLegacyJSONError(c, err.Error())
  ```
- **触发路径**：任一 usecase/存储层错误（含 Pebble 内部错误文本、节点角色冲突信息）直接进入 HTTP 响应。对比 `message_send.go:100-105` 有 `mapSendError` 白名单——同一文件族里严格版与宽松版并存。
- **后果**：向匿名调用方泄露内部实现细节，辅助后续攻击侦察。
- **建议**：统一走 error-map 白名单，未知错误一律 500 通用文案。

### [P3] 12. staticcheck S1016：`bench.go:52` 用结构体字面量逐字段拷贝同构类型

- **位置**：`internal/access/api/bench.go:50-57`
- **类别**：可读性
- **代码**：
  ```go
  c.JSON(http.StatusOK, benchCapacityTargetResponse{
      Version: "bench/v1",
      Gateway: benchCapacityGatewayResponse{
          TCPAddr: addr.TCPAddr,
          WSAddr:  addr.WSAddr,
          WSSAddr: addr.WSSAddr,
      },
  })
  ```
- **触发路径**：无运行时影响；`LegacyRouteAddresses` 与 `benchCapacityGatewayResponse` 字段完全同构，可直接转换。
- **建议**：`Gateway: benchCapacityGatewayResponse(addr)`。

---

## 已排除的候选项

- `internal/access/api/conversation_legacy_model.go:146` — gosec G115（uint64→int64）。`msg.MessageID` 是 snowflake 正整数（`messageid.MaxNodeID` 校验保证符号位为 0），永不溢出，误报。
- `internal/access/gateway/lifecycle.go:72-93` — gosec G115 × 6（int/int32/int64→uint8）。这些是 `deviceFlagFromValue`/`deviceLevelFromValue` 类型开关的兜底分支；生产路径上会话值只由 `pkg/gateway/auth.go:102-108` 写入，类型恒为 `frame.DeviceFlag`/`frame.DeviceLevel`（uint8 底层），`case frame.DeviceFlag` 恒先命中，int 系分支实际不可达，误报。
- `internal/access/gateway/frame_router.go:157` — `defer cancel()` 在循环内，cancel 延迟到 `OnSendBatch` 返回。看似泄漏，但批大小受 `WK_GATEWAY_DEFAULT_SESSION_ASYNC_SEND_BATCH_MAX_RECORDS=128` 约束且函数每批返回，非泄漏。
- `internal/access/api/conversation_sync.go:39-45` 的 `limit` 与 `internal/usecase/cmdsync/app.go:224-230`、`diagnostics.go:81-92` — 均有 clamp（500/10000/500），无界拉取不成立（`msg_count` 是唯一漏网，见发现 5）。
- `internal/access/api/server.go:286-288` — `go func(){ _ = httpServer.Serve(ln) }()` 吞掉返回值：`Shutdown` 后必然是 `ErrServerClosed`，且 `Stop` 走 `httpServer.Shutdown(ctx)` 等待在途请求，无 goroutine 泄漏。
- `internal/access/api/testdata.go`、`bench.go` 全部路由 — 虽无认证，但分别被 `WK_TEST_MODE` / `WK_BENCH_API_ENABLE`（默认 false）门住，且示例配置对 bench 写明了风险（`wukongim.conf.example:215-216`），属已知设计取舍。
- `internal/access/api/plugin.go` — 已有 10MB `MaxBytesReader`、hop-by-hop 头剥离、状态码范围校验（`plugin.go:92-94`），实现干净。
- `internal/access/gateway/handler.go:120-137` `OnSessionClose` — `SessionClosed` 与 `Deactivate` 双路径关闭，`errors.Join` 聚合，无资源泄漏；`onlineSessionAdapter` 是无状态薄壳。
- staticcheck 余量：本分片仅 1 条（S1016，收为发现 12），无 U1000 死代码。

---

## 本分片整体评价

入口适配层（`internal/access/api`、`internal/access/gateway`）单看代码质量是好的：结构清晰、 nil-receiver 防御一致、错误映射统一、gateway 侧 frame→usecase 映射忠实于 AGENTS.md 的"入口只做适配"分层（未发现业务逻辑下沉违例）。问题几乎全部集中在**信任模型**上：整个 5001 业务 API 没有认证概念（协调者已核实），而本单元把它的爆炸半径落到了行级——`/user/token` 签发凭据、`/user/systemuids_add` 铸造特权身份、`/message/sync` 读取任意用户全量消息，三者叠加构成匿名完整接管链。最严重的单一事实是发现 1：`WK_GATEWAY_TOKEN_AUTH_ON` 是一个**设置了反而无法启动**的死开关，客户端网关协议层认证在当前二进制中根本不存在，任何 TCP 客户端自报 UID 即成为该用户。最需要优先处理的就是把这个信任边界显式化：要么接通认证，要么在配置与文档里强制声明"仅限可信网络 + 业务后端前置"。

REPORT SUMMARY: P0=3 P1=3 P2=2 P3=4
