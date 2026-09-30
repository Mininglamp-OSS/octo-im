# 协调者抽检：Unit 04 的 P0 —— 网关 token 认证是"双重死开关"，任意客户端可冒充任意用户

> 我对 Unit 04 的头号 P0 做了独立复核。**完全成立，且比 Unit 04 描述的更彻底**：
> 这个认证功能不是"默认关闭"，而是**两条独立的路都堵死了**，在当前代码里根本无法启用。
> 判定 **P0**。

---

## 证据链（三段，全部亲自核实）

### 第 1 段 —— 打开开关 = 节点起不来

`internal/app/config.go:810-812`：
```go
if c.Gateway.TokenAuthOn {
	return fmt.Errorf("%w: gateway token auth requires verifier hooks", ErrInvalidConfig)
}
```
**无条件拒绝**。没有"除非配置了 verifier"的分支，没有任何逃生口。
即：运行中的节点，`TokenAuthOn` **只可能是 false**。

### 第 2 段 —— 校验逻辑整块被这个恒假的开关挡住

`pkg/gateway/auth.go:59-74`：
```go
deviceLevel := frame.DeviceLevelSlave
if opts.TokenAuthOn && !isVisitor(opts.IsVisitor, connect.UID) {   // ← 第 60 行，恒假
	if connect.Token == "" || opts.VerifyToken == nil {
		return &AuthResult{Connack: &frame.ConnackPacket{ReasonCode: frame.ReasonAuthFail}}, nil
	}
	level, err := opts.VerifyToken(connect.UID, connect.DeviceFlag, connect.Token)
	if err != nil {
		return &AuthResult{Connack: &frame.ConnackPacket{ReasonCode: frame.ReasonAuthFail}}, nil
	}
	deviceLevel = level
}
```
由第 1 段，`opts.TokenAuthOn` 在生产中恒为 false → **整个 61-74 块是死代码**。
连接直接带着客户端自报的 `connect.UID` 通过，`deviceLevel` 保持默认的 `DeviceLevelSlave`，
**没有任何身份验证**。

### 第 3 段 —— 就算绕过第 1 段，钩子也从未被赋值（故障封闭，但功能缺失）

全仓 `grep VerifyToken`（非测试代码）**只有三处命中，全在同一个文件**：
```
pkg/gateway/auth.go:35   VerifyToken func(uid string, deviceFlag frame.DeviceFlag, token string) (frame.DeviceLevel, error)   ← 字段声明
pkg/gateway/auth.go:61   if connect.Token == "" || opts.VerifyToken == nil {                                                   ← 判空
pkg/gateway/auth.go:67   level, err := opts.VerifyToken(connect.UID, connect.DeviceFlag, connect.Token)                        ← 调用
```
**没有任何一处赋值。** 声明了、判空了、调用了，就是从来没有人往里塞实现。

所以即便有人删掉第 1 段的配置拒绝，第 61 行也会因 `VerifyToken == nil` 而**拒绝所有连接**。

---

## 准确定性（避免夸大，也避免轻描淡写）

这**不是配置失误**，而是**一个未完成的功能**，而且代码自己承认了 ——
错误信息就写着 "gateway token auth **requires verifier hooks**"。
作者知道钩子没接上，于是用配置校验把开关焊死，**故障封闭**（fail-closed）方向是对的。

但后果是：**生产环境只有一种可能的状态 —— 网关完全不做身份验证。**

**触发路径**：任意 TCP / WebSocket 客户端连到 `0.0.0.0:5100`（wkproto）或 `0.0.0.0:5200`（ws），
发一个 CONNECT 帧，`connect.UID` 填任意目标用户的 UID → 直接被接受为该用户 →
可以该用户身份收发消息。**不需要任何凭据。**

---

## 与其它已确认发现的关系（汇总时应合并为一条"接入层无认证"主线）

| 入口 | 状态 | 证据 |
|---|---|---|
| **网关**（5100 / 5200） | 认证功能**无法启用** | 本文件 |
| **业务 HTTP API**（5001） | engine **只有 CORS 中间件**，零认证 | `00-coordinator-verified.md` C-2（我亲自核实 `server.go:190-191`） |
| **管理 HTTP API**（5301） | `WK_MANAGER_AUTH_ON` 代码默认 **false**（示例配置却写 true） | Unit 05 P0 + Unit 04 已核实到行号 |
| **节点间 RPC** | `pkg/transport` **无认证、无 TLS** | 我亲自核实（grep 零命中） |

**四个入口面，没有一个在默认（或唯一可能的）配置下做身份验证。**
这不是四条孤立的问题，而是一条应当作为报告首要结论的系统性缺口。

Unit 04 另外两条 P0 —— 无认证 `/user/token` 可为任意用户创建/覆盖 token
（`token.go:21-33`）、无认证 `/user/systemuids_add` 铸造系统账号并绕过发送权限与限流
（`permission.go:17-19`）—— 是这条主线在 HTTP 面的具体兑现方式：
**攻击者不仅能冒充，还能持久化地为自己铸造身份。**

---

## 修复方向

短期：把 `VerifyToken` 真正接上（`internal/usecase/user` 已有 token 校验能力），
让 `TokenAuthOn=true` 成为可用配置并**改为默认值**；
在此之前，至少把 `WK_API_LISTEN_ADDR` / gateway listener 默认绑定改为 `127.0.0.1`，
并在 `wukongim.conf.example` 中明确写出"本服务当前不做接入认证，必须部署在可信网络内"。
