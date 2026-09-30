# 协调者抽检：Unit 11 的 P0 —— 插件宿主 RPC 显式丢弃已认证调用者身份

> 我对 Unit 11 的 P0 做了独立复核，**完全成立**。这是本次审计中**证据最干净**的一条安全缺陷：
> 入口层正确取得并传递了身份、接口契约把这个参数命名为 `callerUID`、
> 而实现方用 `_` 把它显式丢掉了。判定 **P0**。

---

## 证据链（三层，全部亲自核实）

### 第 1 层 —— 入口层**正确**取得并传递了已认证身份

`internal/access/plugin/handlers_message.go:12`：
```go
resp, err := s.usecase.SendMessage(ctx, &req, c.Uid())
```
`internal/access/plugin/handlers_message.go:27`：
```go
resp, err := s.usecase.ChannelMessages(ctx, &req, c.Uid())
```
`internal/access/plugin/handlers_conversation.go:12`：
```go
resp, err := s.usecase.ConversationChannels(ctx, &req, c.Uid())
```
三处都传了 `c.Uid()` —— 即**连接上已认证的插件身份**。入口层没有问题。

### 第 2 层 —— 接口契约明确把这个参数命名为 `callerUID`

`internal/access/plugin/server.go:47-52`：
```go
SendMessage(ctx context.Context, req *pluginproto.SendReq, callerUID string) (*pluginproto.SendResp, error)
ChannelMessages(ctx context.Context, req *pluginproto.ChannelMessageBatchReq, callerUID string) (*pluginproto.ChannelMessageBatchResp, error)
ConversationChannels(ctx context.Context, req *pluginproto.ConversationChannelReq, callerUID string) (*pluginproto.ConversationChannelResp, error)
```
参数名就叫 `callerUID`。**设计意图毫无歧义：实现方应当用它做授权。**

### 第 3 层 —— 实现方用 `_` 显式丢弃

`internal/usecase/plugin/host_rpc.go:22`：
```go
func (a *App) SendMessage(ctx context.Context, req *pluginproto.SendReq, _ string) (*pluginproto.SendResp, error) {
```
`internal/usecase/plugin/host_rpc.go:38`：
```go
func (a *App) ChannelMessages(ctx context.Context, req *pluginproto.ChannelMessageBatchReq, _ string) (*pluginproto.ChannelMessageBatchResp, error) {
```
`internal/usecase/plugin/host_rpc.go:113`：
```go
func (a *App) ConversationChannels(ctx context.Context, req *pluginproto.ConversationChannelReq, _ string) (*pluginproto.ConversationChannelResp, error) {
```

**三处全部 `_ string`。** 身份被送到了函数门口，然后被丢进垃圾桶。
这不是"忘了加校验"——参数已经在签名里，作者写的是 `_`。

---

## 三个具体后果（逐个核实）

### ① 任意插件可伪造**任意用户**发消息

`internal/usecase/plugin/mapping.go:17-26`（`sendCommandFromPluginReq`）：
```go
fromUID := strings.TrimSpace(req.GetFromUid())      // ← 发送者取自【请求】
if fromUID == "" {
	fromUID = strings.TrimSpace(defaultSenderUID)
	if fromUID == "" {
		return message.SendCommand{}, ErrDefaultSenderUIDRequired
	}
}
```
发送者 UID 来自 `req.GetFromUid()`，**从不与 `callerUID` 比对**。
→ 任何已连接的插件可以以**任意用户身份**发送消息（冒充管理员、冒充任意真实用户）。

### ② 任意插件可读**任意频道**的全部历史消息

`host_rpc.go:38-50`：遍历 `req.GetChannelMessageReqs()`，对每一项直接
`a.messageReader.SyncMessages(ctx, channelMessageQueryFromPluginReq(item))`，
**没有任何"该插件是否有权读此频道"的检查**。
→ 任何插件可以批量拉取全系统任意频道的完整消息历史。

### ③ 任意插件可读**任意用户**的会话列表

`host_rpc.go:113-125`：
```go
uid := ""
if req != nil {
	uid = strings.TrimSpace(req.GetUid())           // ← uid 取自【请求】
}
if uid == "" {
	return nil, ErrConversationUIDRequired          // ← 唯一校验：非空
}
channels, err := a.conversations.ConversationChannels(ctx, uid, defaultHostConversationChannelsLimit)
```
只校验非空，不校验归属。→ 任何插件可以读任意用户的会话列表。

---

## 与 Unit 11 另一条 P1 的关系

Unit 11 另报了 `/plugin/httpForward` 的 `PluginNo` 冒充问题
（`host_rpc.go:132-176` + `invocation.go:83-111`：`handleHTTPForward` 只在
`PluginNo` 为空时才填入 `c.Uid()`，否则信任调用方自报值）。
那条是**同一根因的另一种形态**：一处把身份丢弃、一处让调用方自报身份覆盖。

汇总时应合并为一条模式级问题：
**插件宿主 RPC 层整体缺失"以已认证调用者身份为准"的授权模型** ——
入口层已经把身份准备好了，usecase 层系统性地没有使用它。

---

## 修复方向

`host_rpc.go` 三个方法把 `_ string` 改回 `callerUID string` 并实际使用：
- `SendMessage`：`req.FromUid` 必须与 `callerUID` 允许代发的范围比对，
  或直接以 `callerUID` 覆盖（视插件是否被授予代发权限）；
- `ChannelMessages`：按 `callerUID` 检查该插件被绑定到哪些频道（`pkg/db/meta` 已有 plugin binding 表）；
- `ConversationChannels`：校验 `req.Uid` 是否在该插件的授权范围内。
`httpForward` 则应无条件以 `c.Uid()` 覆盖 `PluginNo`，而不是仅在为空时填充。
