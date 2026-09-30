# 协调者亲自核实的发现（不依赖 worker 报告）

> 本文件由协调者在 worker 派发受阻期间直接读源码核实。每条都是**我本人打开文件读过**的，
> 行号与摘录可复核。与 worker 报告汇总时，这些条目优先级最高（证据链最完整）。

---

## [P0] C-1. 远程可触发的进程崩溃：超长字符串字段一路直达 `WriteString` 的 `panic()`

**位置（完整链路，全部亲自核实）**

1. `internal/access/api/routes.go:69` —— `/message/send` 注册，**无任何认证中间件**
2. `internal/access/api/server.go:190-191` —— 整个公开 API engine **只有一个中间件**：
   ```go
   engine := gin.New()
   engine.Use(openCORSMiddleware())
   ```
3. `internal/access/api/message_send.go:39-61` —— `handleSendMessage` 只校验**非空**，
   对 `FromUID` / `ChannelID` / `ClientMsgNo` **没有任何长度上限**，且 `c.ShouldBindJSON(&req)`
   没有 `http.MaxBytesReader`，请求体无大小限制：
   ```go
   if err := c.ShouldBindJSON(&req); err != nil { ... }
   if req.FromUID == "" || req.Payload == "" { ... }        // 只判空
   } else if req.ChannelID == "" || req.ChannelType == 0 {  // 只判空
   ```
4. `internal/app/deliveryrouting.go:2156-2179` —— `buildRealtimeRecvPacket` 把这些字段
   **原样搬进** RECV 帧：
   ```go
   ClientMsgNo: msg.ClientMsgNo,
   ...
   FromUID:     msg.FromUID,
   ```
5. `internal/app/deliveryrouting.go:1565` —— 扇出时 `conn.Session.WriteFrame(f)` 触发编码
6. `pkg/protocol/codec/recv.go:107-116` —— `encodeRecv` 对 4 个字段调用 `WriteString`，
   **调用前无任何长度校验**：
   ```go
   enc.WriteString(recvPacket.MsgKey)
   enc.WriteString(recvPacket.FromUID)
   enc.WriteString(recvPacket.ChannelID)
   ...
   enc.WriteString(recvPacket.ClientMsgNo)
   ```
7. `pkg/protocol/codec/encoder.go:141-145` —— **直接 panic**：
   ```go
   bl := len(str)
   if bl > math.MaxInt16 {
       panic(fmt.Errorf("WriteString: len(str) > math.MaxInt16, len(str) = %d", bl))
   }
   ```
   `WriteBinary`（`encoder.go:160-170`）同样 panic。

**为什么没被 recover 兜住**：全仓非测试代码只有 **2 处** `recover()`：
`internal/access/plugin/handlers_lifecycle.go:48` 和 `internal/runtime/channelmeta/activate.go:41`。
**都不在投递/编码路径上**。而 panic 发生在 `deliveryrouting.go:1565` 的投递 goroutine，
不是 gnet 的连接回调，因此 gnet 自身的保护（若有）也够不着。

**没有任何上游兜底**：全仓 `grep MaxInt16` 只命中 `pkg/protocol/codec` 自己；
`internal/usecase/message/` 与 `internal/usecase/channel/` 里**没有任何长度上限校验**。

**触发路径**：`POST /message/send`，`client_msg_no` 给一个 32768 字节以上的字符串
（`from_uid` / `channel_id` 同理）→ 消息被接收并提交 → 扇出给订阅者 → `encodeRecv` → panic → **进程崩溃**。

**关键点：这不需要攻击者。** 一个合法的业务后端只要生成了超长 `client_msg_no`，就能把节点打挂。
这使它成为纯粹的健壮性缺陷，与"API 是否该暴露在公网"的设计意图之争无关。

**待进一步确认（未核实，不得当成结论）**：消息在扇出**之前**已经持久化提交，
因此重启后 `internal/app/committed_replay.go` 的重放路径是否会再次投递同一条消息、
从而形成**崩溃循环**——这一点我没有验证，需要 Unit 02 给出结论。

**建议**：在协议编解码层把 `panic` 改成返回 error（`encodeRecv` 已经返回 `error`，有现成通道），
并在入口层对所有进入协议帧的字符串字段加显式长度上限。

---

## [P1] C-2. 公开业务 API 默认无认证 + 绑定 0.0.0.0，且与示例配置的安全姿态不一致

**位置**
- `internal/access/api/server.go:190-191` —— engine 只挂 `openCORSMiddleware()`，无认证
- `internal/access/api/routes.go:45-76` —— 以下全部**无认证**：
  `/user/token`、`/user/device_quit`、`/user/systemuids_add|remove|add_to_cache|remove_from_cache`、
  `/channel`（upsert）、`/channel/delete`、`/channel/subscriber_*`、`/channel/blacklist_*`、
  `/channel/whitelist_*`、`/message/send`、`/message/sync`、`/conversations/*`
  本文件里唯一的"门"是功能开关（`debugEnabled` / `benchEnabled` / `testMode` /
  `diagnosticsDebugEnabled`），**不是认证**
- `wukongim.conf.example:214` —— `WK_API_LISTEN_ADDR=0.0.0.0:5001`

**设计意图辨析（重要，避免夸大）**：该 API 沿袭上游 WuKongIM 的定位，是给客户自己的
应用后端调用的服务端到服务端接口，**设计上假定运行在可信网络**。所以"无认证"本身
一部分是设计取舍而非纯 bug。但仍有三处实质缺陷：

1. 默认绑 `0.0.0.0` 而非 `127.0.0.1`，且**配置注释里没有任何"须置于可信网络/不要暴露公网"的警告**——
   相比之下同一份示例配置对 bench 路由就明确写了
   `wukongim.conf.example:215-216`：*"Enables unauthenticated /bench/v1/* APIs … Keep false in
   production and shared environments because these routes intentionally skip auth."*
   说明作者对"无认证"是有安全意识的，却唯独没对主业务 API 说明这一假设。
2. `/user/token` 与 `/user/systemuids_*` 能**签发/变更身份与系统用户**，
   把它们放在与读接口相同的无认证平面上，信任边界过宽。
3. 示例配置写 `WK_MANAGER_AUTH_ON=true`（`wukongim.conf.example:226`），
   而代码默认据侦察是 `false` —— **示例配置与代码默认不一致**，
   不按示例部署的节点其后台管理面会处于无认证状态。（代码默认值待 Unit 04/05 核实到行号。）

**建议**：`WK_API_LISTEN_ADDR` 默认改 `127.0.0.1:5001`；在配置注释中写明该 API 的信任假设；
把 `/user/token`、`/user/systemuids_*` 这类身份变更接口与只读接口分开，至少提供可选的共享密钥校验。

---

## [P3] C-3. `docs/development/CODE_QUALITY.md` 已过期

该文件仅 5 行，其中记载的
"`internal/app/deliveryrouting.go:isSenderDeliveryRoute` contains a duplicated return statement"
一条，我已核实**当前代码中不存在**（`deliveryrouting.go:1538` 与 `:1613` 两处调用点均正常）。
文档记录的另两条（`App.Stop()` 共享 5s 停止上下文、`TestSessionLongPollRPCTimeoutUsesRecoveryBackoff`
时序敏感）需由 Unit 01 分别核实。

---

## 工具链与全仓基线（已核实，见 plan 文件详述）

- 本机 Go 1.27.1 下 `go build ./...` **失败**（`cockroachdb/swiss` 引用已删除的 runtime 符号）；
  `GOTOOLCHAIN=go1.23.4` 下 build 与 vet 均通过。
- `go vet ./...` 干净；`staticcheck` 119 条（102 条 U1000）；`gosec` 486 条（G115 约 396 条）；
  `govulncheck` 38 个可达漏洞。
- 全仓 0 条 TODO/FIXME/HACK 注释。
- `internalv2` 及全部 `*v2` 包**未接入任何二进制**：`cmd/wukongim/main.go` 只构造 `internal/app`，
  全仓无 `internalv2/app` 的 importer。
- `pkg/protocol/wkprotoenc/crypto.go:266` —— AES 密钥用 MD5 派生并截断成 16 个十六进制字符
  （约 64 bit 有效密钥），无 KDF 强度。
