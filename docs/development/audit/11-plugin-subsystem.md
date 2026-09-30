# 插件子系统全链路 (Plugin Subsystem Full Chain)

审计范围：`internal/usecase/plugin/`（含 `pluginproto/`）、`internal/runtime/plugin/`、`internal/access/plugin/`。
三个目录均为 `internal/*`（v1），在生产二进制真实运行，严重度按实际后果评估，不做 v2 折扣。

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/usecase/plugin/app.go | 405 | 是 |
| internal/usecase/plugin/binding.go | 413 | 是 |
| internal/usecase/plugin/cache.go | 116 | 是 |
| internal/usecase/plugin/config.go | 143 | 是 |
| internal/usecase/plugin/deps.go | 159 | 是 |
| internal/usecase/plugin/host_rpc.go | 293 | 是 |
| internal/usecase/plugin/invocation.go | 125 | 是 |
| internal/usecase/plugin/mapping.go | 168 | 是 |
| internal/usecase/plugin/persist_after.go | 49 | 是 |
| internal/usecase/plugin/receive.go | 153 | 是 |
| internal/usecase/plugin/registry_view.go | 62 | 是 |
| internal/usecase/plugin/send_hook.go | 76 | 是 |
| internal/usecase/plugin/types.go | 225 | 是 |
| internal/usecase/plugin/pluginproto/plugin.go | 231 | 是（手写编解码封装层） |
| internal/usecase/plugin/pluginproto/plugin.pb.go | 2603 | 否（`protoc` 生成物，按 brief 指导只做结构性略读，未逐行通读；未发现手工改动痕迹） |
| internal/runtime/plugin/invoker.go | 68 | 是 |
| internal/runtime/plugin/lifecycle.go | 442 | 是 |
| internal/runtime/plugin/process.go | 146 | 是 |
| internal/runtime/plugin/registry.go | 134 | 是 |
| internal/runtime/plugin/scanner.go | 74 | 是 |
| internal/runtime/plugin/socket.go | 167 | 是 |
| internal/runtime/plugin/store.go | 161 | 是 |
| internal/runtime/plugin/types.go | 55 | 是 |
| internal/runtime/plugin/watcher.go | 231 | 是 |
| internal/access/plugin/codec.go | 48 | 是 |
| internal/access/plugin/handlers_cluster.go | 32 | 是 |
| internal/access/plugin/handlers_conversation.go | 18 | 是 |
| internal/access/plugin/handlers_http.go | 21 | 是 |
| internal/access/plugin/handlers_lifecycle.go | 130 | 是 |
| internal/access/plugin/handlers_message.go | 33 | 是 |
| internal/access/plugin/server.go | 160 | 是 |

三个目录下均无 `FLOW.md`（仅根目录有 `AGENTS.md`，其分层规则已在下文架构检查中比对）。

## 发现

### [P0] 1. 插件宿主 RPC 全线丢弃调用者身份：任意已连接插件可伪造任意用户发消息、读取任意频道全部历史、读取任意用户会话列表

- **位置**：
  - `internal/usecase/plugin/host_rpc.go:22`（`SendMessage`，`callerUID` 参数命名为 `_`，完全不用）
  - `internal/usecase/plugin/host_rpc.go:38`（`ChannelMessages`，同上）
  - `internal/usecase/plugin/host_rpc.go:113`（`ConversationChannels`，同上，取用请求体里的 `uid` 而非 `callerUID`）
  - `internal/usecase/plugin/mapping.go:21-27`（`sendCommandFromPluginReq`，`FromUID` 取自请求体）
  - `internal/access/plugin/handlers_message.go:5-33`、`handlers_conversation.go:5-18`（访问层把已认证的 `c.Uid()` 一路传下去，但 usecase 层直接丢弃）
- **类别**：安全 / 正确性（越权、身份伪造、数据泄露）
- **代码**：
  ```go
  // internal/usecase/plugin/host_rpc.go:21-58
  func (a *App) SendMessage(ctx context.Context, req *pluginproto.SendReq, _ string) (*pluginproto.SendResp, error) {
      ...
      cmd, err := sendCommandFromPluginReq(req, a.defaultSenderUID)
      ...
      result, err := a.messages.Send(ctx, cmd)
      ...
  }

  func (a *App) ChannelMessages(ctx context.Context, req *pluginproto.ChannelMessageBatchReq, _ string) (*pluginproto.ChannelMessageBatchResp, error) {
      ...
      for _, item := range req.GetChannelMessageReqs() {
          page, err := a.messageReader.SyncMessages(ctx, channelMessageQueryFromPluginReq(item))
          ...
      }
  }

  func (a *App) ConversationChannels(ctx context.Context, req *pluginproto.ConversationChannelReq, _ string) (*pluginproto.ConversationChannelResp, error) {
      uid := ""
      if req != nil {
          uid = strings.TrimSpace(req.GetUid())
      }
      ...
      channels, err := a.conversations.ConversationChannels(ctx, uid, defaultHostConversationChannelsLimit)
  }
  ```
  ```go
  // internal/usecase/plugin/mapping.go:17-27
  func sendCommandFromPluginReq(req *pluginproto.SendReq, defaultSenderUID string) (message.SendCommand, error) {
      ...
      fromUID := strings.TrimSpace(req.GetFromUid())
      if fromUID == "" {
          fromUID = strings.TrimSpace(defaultSenderUID)
          ...
      }
      ...
  }
  ```
  访问层已经把认证过的连接身份传下来，但被忽略：
  ```go
  // internal/access/plugin/handlers_message.go:5-18
  func (s *Server) handleSendMessage(c rpcContext) {
      var req pluginproto.SendReq
      if !s.decodeProto(c, &req) { return }
      ...
      resp, err := s.usecase.SendMessage(ctx, &req, c.Uid())
      ...
  }
  ```
- **触发路径**：
  1. 任意插件二进制通过 `/plugin/start` 完成一次性握手，`callerUID == info.GetNo()` 校验通过后，该插件在其 socket 连接上获得自己插件号作为 `c.Uid()`（`app.go:96-101`）。这是设计上唯一的身份边界；此后该连接上发出的所有宿主 RPC 都被认为"来自这个插件"，但下游从未再检查这些 RPC 携带的业务身份字段是否与该连接身份一致。
  2. **消息伪造**：插件在 `/message/send` 请求体里把 `FromUid` 填成任意真实用户 UID（例如从 `ConversationChannels`/其它渠道拿到的活跃用户），调用 `SendMessage`。`sendCommandFromPluginReq` 原样采用该 `FromUid`；`a.messages.Send` 走的是与客户端发送完全相同的生产管道（`internal/app/plugin.go` 里 `pluginMessageSender.Send` → `s.app.messageApp.Send`）。下游 `internal/usecase/message/permission.go:13-42` 的 `checkSendPermission` 只基于 `cmd.FromUID`（即插件伪造出来的身份）去查禁言/频道权限，它验证的是"这个被冒充的 UID 是否被允许发言"，而不是"当前连接是否真的是这个 UID"——所以只要目标用户没被禁言，消息会被正常持久化、投递给频道所有成员，`FromUID` 显示为被冒充的真实用户。
  3. **任意频道全量历史读取**：插件在 `/channel/messages` 请求里填任意 `channelId`/`channelType`/`startMessageSeq`，`ChannelMessages` 直接转发给 `a.messageReader.SyncMessages`，没有任何"该插件/该身份是否有权读这个频道"的检查。插件可以枚举频道号，导出全系统任意频道（包括未订阅、未绑定过的频道）的完整历史消息。
  4. **任意用户会话列表读取**：插件在 `/conversation/channels` 请求里填任意 `uid`（不必是自己），`ConversationChannels` 直接返回该用户最近的会话频道列表——这是对任意用户社交关系/联系人图谱的信息泄露，可作为后续针对性攻击的侦察手段。
- **后果**：数据完整性破坏（可伪造任意用户的聊天消息并正常投递、持久化，且绕过发送频率限制——因为 `checkSendPermission`/`checkUserSendLimit` 都是基于伪造身份计算的，只要伪造成 `IsSystemUID` 判定的系统账号即可完全绕过限流）；全量消息历史泄露；用户隐私信息泄露。三者均只需一个已连接的插件进程即可触发，不需要额外权限，属于远程输入可达。
- **建议**：宿主 RPC 的业务身份字段（`FromUid`/会话查询 `uid`/频道读取范围）必须与 `callerUID`（连接时确定的插件身份）做强制关联校验——例如要求 `FromUid` 必须等于 `callerUID`，或引入插件级 ACL/scoped-binding 表来判定该插件是否有权以该 UID 发送、或读取该频道/该用户数据；至少要在 usecase 层不再对参数里的 `_ string`（callerUID）视而不见。

### [P1] 2. `/plugin/httpForward` 允许任意插件冒充调用其它任意插件的 Route Hook

- **位置**：`internal/usecase/plugin/host_rpc.go:155-165`（`normalizeHTTPForwardRequest`）、`internal/usecase/plugin/host_rpc.go:132-152`（`HTTPForward`）、`internal/usecase/plugin/invocation.go:83-111`（`Route`）
- **类别**：安全（越权/跨插件冒充）
- **代码**：
  ```go
  // internal/usecase/plugin/host_rpc.go:155-165
  func (a *App) normalizeHTTPForwardRequest(req *pluginproto.ForwardHttpReq, callerUID string) (*pluginproto.ForwardHttpReq, error) {
      pluginNo := strings.TrimSpace(req.GetPluginNo())
      if pluginNo == "" {
          pluginNo = strings.TrimSpace(callerUID)
      }
      if pluginNo == "" {
          return nil, ErrPluginNoRequired
      }
      if err := validatePluginNo(pluginNo); err != nil {
          return nil, err
      }
      ...
  }
  ```
  ```go
  // internal/usecase/plugin/host_rpc.go:149-152
  if a == nil {
      return nil, ErrInvokerRequired
  }
  return a.Route(ctx, normalized.GetPluginNo(), normalized.GetRequest())
  ```
  `Route` 只做格式校验，不做归属校验：
  ```go
  // internal/usecase/plugin/invocation.go:83-95
  func (a *App) Route(ctx context.Context, pluginNo string, req *pluginproto.HttpRequest) (*pluginproto.HttpResponse, error) {
      if a.invoker == nil { return nil, ErrInvokerRequired }
      if err := validatePluginNo(pluginNo); err != nil { return nil, err }
      req = clonePluginHTTPRequest(req)
      dropHopByHopHeaders(req.Headers)
      if err := a.validateHTTPForwardRequestSize(req); err != nil { return nil, err }
      data, err := req.Marshal()
      ...
      respData, err := a.invoker.RequestPlugin(ctx, pluginNo, PathRoute, data)
      ...
  }
  ```
- **触发路径**：插件 A（`callerUID == "A"`）在 `/plugin/httpForward` 请求里显式填 `PluginNo: "B"`（另一个已注册插件的编号，可通过 `/cluster/config` 或已知部署清单得知），并附带任意 `Method/Path/Headers/Query/Body`。`normalizeHTTPForwardRequest` 里 `req.GetPluginNo()` 非空，直接采用（不会退回到 `callerUID`），只对 `"B"` 做文件名安全格式校验（`validatePluginNo`），从不检查 A 是否有权代表/调用 B。请求最终经 `a.Route(ctx, "B", ...)` 触发插件 B 的 `/plugin/route` 处理器，B 完全无法区分这是宿主转发的合法请求还是插件 A 伪造的。`ForwardHttpReq.ToNodeId > 0` 时还可经 `internal/access/node/plugin_http_forward_rpc.go`（另一分片文件，此处仅引用调用关系）把这个伪造请求转发到集群任意其它节点上的插件 B。
- **后果**：直接违反"插件不能冒充另一个插件"这一设计层面的信任边界（brief 明确指出的头号检查项）：一个低权限/被攻陷的插件可以携带任意 HTTP 方法/路径/正文，驱动任何其它已安装插件对外暴露的 Route 处理器，具体危害取决于目标插件对该 Hook 的实现（可能包含管理操作、支付回调、第三方 API 代理等）。
- **建议**：在 `normalizeHTTPForwardRequest`/`Route` 中加入插件间调用授权（例如显式白名单/声明式"允许被哪些插件转发调用"的清单，或直接禁止 `PluginNo != callerUID` 的跨插件转发，除非显式配置允许）。

### [P1] 3. `Runtime.Start()` 失败清理路径在 ctx 已取消/超时时可以完全不 Kill 已启动的插件进程，导致进程永久失控且 `Stop()`/`Restart()` 永久性变成静默空操作

- **位置**：`internal/runtime/plugin/lifecycle.go:183-221`（`Start`，尤其 189/203/212 三处丢弃 `stopProcessesLocked` 返回的 error）、`lifecycle.go:224-242`（`Stop`，`if !r.started { return nil }`）、`lifecycle.go:245-256`（`Restart`，同样的 `!r.started` 短路）、`internal/runtime/plugin/process.go:87-108`（`ProcessManager.Stop` 里 ctx 已 Done 时的早退分支）
- **类别**：并发 / 资源泄漏
- **代码**：
  ```go
  // internal/runtime/plugin/lifecycle.go:183-208
  for _, spec := range specs {
      shouldStart, err := r.shouldStartSpecLocked(spec)
      if err != nil {
          if r.watcher != nil && r.hotReload { r.watcher.Stop() }
          r.stopProcessesLocked(ctx)              // <- 189，返回值被丢弃
          if r.socket != nil { r.socket.Stop() }
          return err
      }
      ...
      if err := r.startSpecLocked(ctx, spec); err != nil {
          if r.watcher != nil && r.hotReload { r.watcher.Stop() }
          r.stopProcessesLocked(ctx)              // <- 203/212 同样丢弃
          if r.socket != nil { r.socket.Stop() }
          return err
      }
  }
  r.started = true
  return nil
  ```
  ```go
  // internal/runtime/plugin/lifecycle.go:224-256
  func (r *Runtime) Stop(ctx context.Context) error {
      ...
      if !r.started { return nil }   // Start() 从未成功过 => 永久 no-op
      ...
  }
  func (r *Runtime) Restart(ctx context.Context, pluginNo string) error {
      ...
      if !r.started { return nil }   // 同上
      ...
  }
  ```
  ```go
  // internal/runtime/plugin/process.go:87-108
  func (m *ProcessManager) Stop(ctx context.Context, handle *ProcessHandle, stop StopFunc) error {
      ...
      stopCtx, stopCancel := context.WithCancel(ctx)
      defer stopCancel()
      if stop != nil {
          stopStarted := make(chan struct{})
          go func() {
              close(stopStarted)
              _ = stop(stopCtx, handle.Spec.No)
          }()
          select {
          case <-stopStarted:
          case <-ctx.Done():
              return ctx.Err()          // <- 没有调用 Kill()，也没有等 handle.done
          }
      }
      ...
  }
  ```
  `stopProcessLocked` 在调用 `Stop` 之前就已经把 handle 从 map 里删掉：
  ```go
  // internal/runtime/plugin/lifecycle.go:374-390
  func (r *Runtime) stopProcessLocked(ctx context.Context, no string) error {
      handle := r.handles[no]
      if handle == nil { return nil }
      delete(r.handles, no)     // <- 无论 Stop 是否成功都已经从 handles 里移除
      ...
      if err := r.processes.Stop(ctx, handle, stop); err != nil {
          r.upsertStatus(no, StatusError, true, 0, err.Error())
          return err
      }
      ...
  }
  ```
- **触发路径**：
  1. 插件目录下有 A、B 两个插件二进制；宿主进程启动时以一个带超时/可取消的 `ctx` 调用 `Runtime.Start(ctx)`（应用启动阶段常见的启动预算 context，或启动过程中收到 SIGTERM）。
  2. `scanner` 先返回 A 后返回 B。A 通过 `startSpecLocked` 成功启动，`handleA` 被写入 `r.handles`。
  3. 在处理 B 之前，`ctx` 被取消/到期。`startSpecLocked(ctx, specB)` 内部调用 `ProcessManager.Start(ctx, specB)`，其中 `if err := ctx.Err(); err != nil { return nil, err }`（`process.go:72-74`）立即返回错误。
  4. `Runtime.Start` 走进 199-208 行的失败清理分支：调用 `r.stopProcessesLocked(ctx)` 试图停掉已经启动的 A，但传入的仍是这个**已经 Done** 的 `ctx`。
  5. `stopProcessLocked(ctx, "A")` 先把 A 从 `r.handles` 删除，再调用 `ProcessManager.Stop(ctx, handleA, stopFn)`。进入 `Stop` 时 `ctx.Done()` 已经关闭（时间上早于函数入口），而 `stopStarted` 需要新起的 goroutine 被调度后才能关闭——两者不是同时就绪，`ctx.Done()` 在 select 求值的那一刻就已经 ready，`stopStarted` 还没 ready，Go 的 `select` 会直接走 `case <-ctx.Done(): return ctx.Err()` 分支，**完全没有调用 `handle.cmd.Process.Kill()`，也没有等待 `<-handle.done`**。
  6. `stopProcessLocked` 把这个 `ctx.Err()` 上报为 A 的停止失败（`upsertStatus(..., StatusError, ...)`），`stopProcessesLocked` 用 `errors.Join` 收集后返回；但 `Runtime.Start` 在 189/203/212 三处都**丢弃了这个返回值**（G104），只把 B 的原始错误 `return err` 给上层调用者——调用者看到的只是"启动 B 失败：context canceled"，完全不知道 A 的进程被漏杀。
  7. 结果：A 对应的操作系统进程继续存活，仍持有插件 socket 连接、继续占用内存/CPU/句柄，但 `r.handles` 里已经没有它的记录，`r.started` 也停留在 `false`（因为 `Start()` 提前 return 了，永远没执行到 `r.started = true`）。此后任何对这个 `Runtime` 调用 `Stop(ctx)` 或 `Restart(ctx, "A")` 都会因为 `if !r.started { return nil }` 直接静默返回成功，什么都不做——这个失控进程从此完全脱离运行时管理，唯一还能杀死它的办法是重新一次成功的 `Start()`（此时 `startSpecLocked` 会为同一个插件号再启动一个**新的**进程，与那个仍在运行的旧进程同时存在，二者都可能连上同一个 Unix socket 并各自触发 `/plugin/start` 握手，导致 `Registry` 对同一个 `No` 的观测状态在两个真实进程之间来回翻转）。
- **后果**：插件子进程泄漏（不受运行时管理、不会被优雅停止或 SIGKILL）；`Stop()`/`Restart()` 对整个 Runtime 永久性静默失效（返回 `nil` 但什么都没做），运维层面表现为"重启插件"/"关闭插件运行时"命令成功但无实际效果；后续成功的 `Start()` 可能为同一插件号产生重复进程，造成观测状态抖动和资源双倍占用。
- **建议**：`ProcessManager.Stop` 在 ctx 已取消时也应该尝试 `Kill()` 一次而不是直接放弃；`stopProcessesLocked` 的清理错误在 `Start()` 失败路径中不应被静默丢弃，至少应该 `errors.Join` 进最终返回的 error；`r.started` 的语义应该允许"部分失败"状态下仍可重入 `Stop()` 做清理，而不是把它当成"从未启动过"。

## 已排除的候选项

- `internal/usecase/plugin/host_rpc.go:178-199`（`validateHTTPForwardRequestSize`/`validateHTTPForwardResponseSize`）——已核实：body 限制 10MB（`defaultHTTPForwardMaxBodyBytes`，可配置覆盖）、header+query 总量限制 64KB，且在 `normalizeHTTPForwardRequest` 里对请求方向和 `Route`/`HTTPForward` 回程都强制调用，控制到位，不是漏洞。
- `internal/access/plugin/handlers_lifecycle.go` 的 `recover()`（`safeBody`，全仓仅 2 处非测试 `recover()` 之一）——沿调用链查到 `github.com/WuKongIM/wkrpc@v0.0.0-20250312122115-5e44de72d2c8/model.go:159-161`：`func (c *Context) Body() []byte { return c.req.Body }`，唯一 panic 来源是 `c.req == nil`（区分"关闭事件通知"与"真实请求体读取"两种回调场景），recover 范围精确匹配这一个 nil 解引用点，未发现会吞掉其它类别 panic 的证据。
- `internal/runtime/plugin/lifecycle.go:308-313`（staticcheck SA4006，`removed` "never used"）——读取上下文后确认是有意为之：
  ```go
  if removed, err := removeIfUnderDir(r.dir, spec.Path); err != nil {
      errs = append(errs, err)
  } else if removed {
      // Keep registry state even after local binary removal so managers can show disabled state.
  }
  ```
  空分支配的是注释说明的产品决策（卸载后仍保留 registry 记录以便管理端展示"已禁用"状态），不是遗留死代码或漏洞。
- `internal/runtime/plugin/registry.go:39`（`Registry.Remove`）——通读 `internal/runtime/plugin/*.go`、`internal/usecase/plugin/*.go`、`internal/access/plugin/*.go` 全部非测试代码，确认这个方法在生产路径上确实零调用点（`Uninstall` 走的是 `upsertStatus(..., StatusDisabled, ...)` 而不是 `Remove`）。但由于 `Registry` 的 key 空间是"曾经部署到本节点的插件二进制数量"（由运维投放 `.wkp` 文件决定，不受远程输入驱动，量级通常个位数到几十），且 `lifecycle.go:312` 的注释明确说明这是有意保留展示态，所以不构成可远程触发的无界增长，仅是死代码（已在整体评价里作为 P3 提及，不单独列为发现）。
- gosec G115（`send_hook.go:55`、`mapping.go:38/61/77`、`host_rpc.go:147/292`）——逐一核实：
  - `send_hook.go:55` `int64(cmd.SenderSessionID)`：`SenderSessionID` 是本地连接会话号，非远程输入拼接的序列号，无累积溢出场景。
  - `mapping.go:38` `uint8(req.GetChannelType())`、`mapping.go:61` `uint8(req.GetChannelType())`：`ChannelType` 协议里语义上就是个位数枚举值，即使远程传入超范围整数，截断后果只是"识别成一个不存在/错误的频道类型"，后续查询会自然失败或返回不匹配结果，不构成越界写/内存破坏。
  - `mapping.go:77` `uint32(effectiveChannelMessageLimit(req))`：`effectiveChannelMessageLimit` 内部已经通过 `channelMessageQueryFromPluginReq` 把值 clamp 到 `[1, 10000]`（`defaultHostChannelMessagesLimit`/`maxHostChannelMessagesLimit`），转换前已有上界保证。
  - `host_rpc.go:147` `uint64(normalized.GetToNodeId())`：转换前已经用 `normalized.GetToNodeId() > 0` 判断为正数分支，且 `ToNodeId` 是集群节点 ID（部署规模量级，非攻击者可任意放大的累积序列号）。
  - `host_rpc.go:292` `uint8(item.GetChannelType())`：与上面 `ChannelType` 同理，false positive。
  以上均为误报，不计入发现。
- gosec G301（`app.go:107`、`watcher.go:154`、`store.go:55`、`socket.go:79`、`process.go:69`、`lifecycle.go:324`，全部 `os.MkdirAll(dir, 0o755)`）——这些目录路径均来自节点本地配置（`SandboxDir`/`StateDir`/`SocketPath` 所在目录/`Dir`），插件号在写入路径前已经过 `validatePluginNo` 格式校验，不存在路径穿越；0755 vs 建议的 0750 是通用安全基线检查，属于纵深防御层面的加固建议而非可利用漏洞，故未单独列为发现。
- gosec G304（`store.go:155` `os.Open(dir)`）——`dir` 是 `Store.dir`，构造自节点配置的 `StateDir`，不是外部输入拼接路径，false positive。
- gosec G204（`process.go:75` `exec.Command(spec.Path, ...)`）——`spec.Path` 来自 `scanner.go` 的 `ScanPlugins`，该函数对发现的每个候选路径都做了 `filepath.EvalSymlinks` + `ensurePathUnderDir` 校验，确保解析后路径确实位于配置的插件目录内，不是可远程篡改的输入，false positive。
- `internal/runtime/plugin/watcher.go` 的 `Watcher.Stop()` 与内部 `w.run` goroutine 的关闭协调——确认 `Stop()` 通过 `<-done` 真正 join 了内层 goroutine（不是"只 join 外层"的常见漏型）；`time.AfterFunc` 触发的去抖回调 goroutine 未被 `Stop()` 显式 join，但 `lifecycle.go` 的 `Restart()`/`Stop()` 在拿到 `r.mu` 后都会先检查 `r.started`，一个在关闭后才触发的去抖回调不会绕过这个门槛产生可观测的错误效果，未构成可写出触发路径的独立发现。
- `internal/usecase/plugin/binding.go` 中 `a.bindingMu.RLock()` 跨阻塞的 `a.bindingStore.ListPluginBindingsByUID(ctx, uid)` 调用——确认是有意的 epoch 版本号校验模式（`bindingEpoch`），`RWMutex.RLock()` 允许并发读者，写入方通过递增 epoch 使读方能检测陈旧结果，不是死锁或数据竞争。

## 本分片整体评价

这个分片的工程质量在"防御性编码"层面（长度限制、大小限制、符号链接穿越防护、原子文件写入、noCopy 标记、epoch 版本化缓存）明显偏成熟，但在插件子系统真正的核心职责——**权限边界**——上出现了系统性的缺口：`internal/access/plugin` 的每一个 handler 都正确地把已认证的连接身份 `c.Uid()` 传给了 usecase 层，但 `internal/usecase/plugin/host_rpc.go` 里对应的多个方法把这个参数直接命名为 `_` 丢弃，转而信任请求体里由插件自己填写的身份/UID 字段。这不是孤立的疏漏，而是同一模式在 `SendMessage`、`ChannelMessages`、`ConversationChannels` 三个方法上重复出现，说明当时实现这层 RPC 时压根没有把"插件是半可信外部进程"这条设计前提落到访问控制代码里。最应该优先修复的是发现 1（身份伪造/越权读取），因为它不需要任何特殊条件，任何一个被接入的插件进程即可伪造任意用户发消息、拉取任意频道全部历史，是这个分片里唯一同时满足"remote-input 可达"与"直接数据完整性/机密性破坏"的问题。发现 2（跨插件冒充）和发现 3（Start 失败清理路径下的进程泄漏与 Stop/Restart 永久失效）次之，分别属于同一信任模型缺口的另一面和运行时生命周期管理的边界条件缺陷。
