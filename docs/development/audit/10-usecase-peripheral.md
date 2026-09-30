# 周边业务用例 + 合约（internal/usecase/{channel,user,presence,benchdata,testdata} + internal/contracts）

## 覆盖情况

分片内全部 22 个非测试 .go 文件（共 2591 行），全部逐行通读：

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/usecase/channel/app.go | 398 | 是 |
| internal/usecase/channel/types.go | 123 | 是 |
| internal/usecase/user/app.go | 59 | 是 |
| internal/usecase/user/command.go | 41 | 是 |
| internal/usecase/user/deps.go | 56 | 是 |
| internal/usecase/user/legacy.go | 211 | 是 |
| internal/usecase/user/token.go | 65 | 是 |
| internal/usecase/presence/app.go | 96 | 是 |
| internal/usecase/presence/authority.go | 37 | 是 |
| internal/usecase/presence/deps.go | 20 | 是 |
| internal/usecase/presence/directory.go | 384 | 是 |
| internal/usecase/presence/gateway.go | 242 | 是 |
| internal/usecase/presence/types.go | 82 | 是 |
| internal/usecase/presence/worker.go | 49 | 是 |
| internal/usecase/benchdata/app.go | 250 | 是 |
| internal/usecase/benchdata/types.go | 140 | 是 |
| internal/usecase/testdata/controller_snapshot_jobs.go | 93 | 是 |
| internal/usecase/testdata/slot_snapshot_users.go | 152 | 是 |
| internal/contracts/channelmembers/listid.go | 34 | 是 |
| internal/contracts/deliveryevents/events.go | 16 | 是 |
| internal/contracts/messageevents/events.go | 23 | 是 |
| internal/contracts/messageevents/realtime.go | 20 | 是 |

本分片无 FLOW.md（最近的父级文档是 internal/FLOW.md，已读）。为核实触发路径还交叉阅读了：internal/FLOW.md、AGENTS.md、internal/app/presenceauthority.go、internal/app/user_system_uid_cache.go、internal/app/benchdata_adapter.go、internal/app/build.go（相关段）、internal/access/api/{routes,bench,user_legacy,channel_management}.go、internal/usecase/management/users.go（KickUser 段）、internal/usecase/message/permission.go、internal/runtime/deliverytag/manager.go、pkg/db/meta/{table_device,table_subscriber}.go、pkg/slot/proxy/{store,identity_rpc}.go、pkg/gateway/auth.go、pkg/cluster/onboarding.go。

阶段 0 机械发现：gosec 2 条（legacy.go:210 G115、token.go:14 G101），staticcheck 0 条。G115 为真（见发现 8），G101 为误报（见已排除）。

## 发现

### [P1] 1. DeviceQuit 吞掉每个设备的全部错误并恒返回 nil —— 强制下线静默失败
- **位置**：`internal/usecase/user/legacy.go:21-29`（配合 `legacy.go:166-187`）
- **类别**：错误处理 / 正确性 / 安全
- **代码**：
  ```go
  func (a *App) DeviceQuit(ctx context.Context, cmd DeviceQuitCommand) error {
      if a == nil || a.devices == nil || a.deviceReader == nil {
          return ErrDeviceStoreRequired
      }
      for _, flag := range deviceQuitFlags(cmd.DeviceFlag) {
          _ = a.quitDevice(ctx, cmd.UID, flag)
      }
      return nil
  }
  ```
  而 `quitDevice` 里两处都会真失败：`a.deviceReader.GetDevice`（走 `pkg/slot/proxy` 权威 RPC）与 `a.devices.UpsertDevice`（`proposeWithHashSlot` 一次 Raft Propose，见 `pkg/slot/proxy/store.go:215-219`）。
- **触发路径**：管理端调用 `POST /user/device_quit`（或 management.KickUser，`internal/usecase/management/users.go:291` 依赖此返回值）。请求到达时目标 UID 的 slot 恰好发生 leader 切换 / Raft 不可用 → `GetDevice` 或 `UpsertDevice` 返回错误 → `_ =` 丢弃 → `DeviceQuit` 返回 nil → API 层 `writeLegacyMutationResult` 返回 200 `{"status":200}`；management.KickUser 继续 downstream 并返回 `Changed:true`。
- **后果**：强制下线操作静默失败：存储 token 未清空、本节点会话未被 kick，但调用方拿到成功响应并认为设备已登出。这是一个安全语义的管理动作（"账号在其他设备上登录" 场景的运营端对应物），失败被完全隐藏。
- **建议**：收集每个 flag 的错误（errors.Join）并向上返回；让调用方能感知部分失败。

### [P2] 2. SetAllowlist/SetDenylist 先删后加、非原子 —— 白/黑名单存在"fail-open 窗口"
- **位置**：`internal/usecase/channel/app.go:232-237`（`setMemberList`），配合 `internal/usecase/message/permission.go:94-96,109-112`
- **类别**：并发 / 安全（状态一致性）
- **代码**：
  ```go
  func (a *App) setMemberList(ctx context.Context, kind memberListKind, key ChannelKey, uids []string) error {
      if err := a.removeAllMemberList(ctx, kind, key); err != nil {
          return err
      }
      return a.addMemberList(ctx, kind, key, uids)
  }
  ```
  消息发送权限侧（`internal/usecase/message/permission.go`）：
  ```go
  hasAllowlist, err := a.permissions.HasChannelSubscribers(ctx, allowID, channelType)
  ...
  if !hasAllowlist {
      return frame.ReasonSuccess, nil   // 白名单为空 = 放行所有人
  }
  ```
- **触发路径**：运营对一个大群频道调用 `POST /channel/whitelist_set`（SetAllowlist）。`removeAllSubscribersFor`（app.go:282-305）按 1000 一页逐页删除、每页一次存储写；删除完成到最后一个 UID 加回之间，白名单为空（或部分为空）。窗口期内任何被移出白名单的用户发消息 → `HasChannelSubscribers=false` → `ReasonSuccess` 放行。黑名单同理：`SetDenylist` 的删除窗口内 `ContainsChannelSubscriber(denyID)=false`，被封禁用户可发消息。窗口时长与名单大小成正比（每次删除/添加都是独立存储提交）。
- **后果**：名单替换期间权限控制短暂失效（fail-open），可被在窗口期内的发送绕过。
- **建议**：将"整体替换名单"实现为单次原子提交（storage 层一个 batch 内先删后加），或先加后删的交换语义。

### [P2] 3. 订阅者 mutation version 为"读-改-写"且无 CAS —— 并发 Reset 产生并集且版本号不推进，投递 tag 缓存无法感知第二次变更
- **位置**：`internal/usecase/channel/app.go:376-391`、`app.go:64-72,111-120`
- **类别**：并发 / 分布式一致性
- **代码**：
  ```go
  func (a *App) subscriberMutationVersionFor(ctx context.Context, channelID string, channelType int64) (uint64, error) {
      ...
      channel, err := a.store.GetChannel(ctx, channelID, channelType)
      ...
      if channel.SubscriberMutationVersion == 0 {
          return 1, nil
      }
      return channel.SubscriberMutationVersion + 1, nil
  }
  ```
  存储侧 `pkg/db/meta/table_subscriber.go:154-156` 只做 `if mutationVersion > channel.SubscriberMutationVersion { ... = mutationVersion }`（取 max，不要求等于 current+1）。投递 tag 缓存以版本相等判定新鲜（`internal/runtime/deliverytag/manager.go:198-204`：`request.SubscriberMutationVersion < current` → stale，`>` → newer，`==` → hit）。
- **触发路径**：两个管理员同时（或 API 双击重放）对同一频道各发一次 `subscriber_add` 且 `reset=true`（订阅者集合 A 与 B）。两者都读到 version=N、都传 N+1。交错执行 `removeAll`→`addChunk`：最终订阅者是 A∪B 而非"B 替换 A"（违反 Reset 语义），且存储中 `SubscriberMutationVersion` 只推进一次到 N+1。若 channel leader 在两次写之间已按 A 的结果物化了 delivery tag（SMV=N+1），B 的变更因版本相等被 `lookupLocalPartitionLocked` 判定为 hit → 后续消息按陈旧订阅者集合投递（漏掉新加订阅者 / 发给已移除订阅者），直到下一次版本推进。
- **后果**：Reset 语义被破坏（并集）；并发写后投递 fanout 使用陈旧订阅者分区，消息漏投/错投，自愈依赖后续任意一次 mutation。
- **建议**：把版本分配移进存储的同一把锁内（CAS：期望版本不匹配则拒绝重试），或将 remove-all + add 合并为单次原子 batch。

### [P2] 4. DeviceQuit 只 kick 本节点会话、无 UID-owner 路由 —— 多节点集群中"强制下线"对其它节点上的会话无效
- **位置**：`internal/usecase/user/legacy.go:189-204`（`kickLocalDevice`）+ `internal/app/user_system_uid_cache.go:28-30`（无路由直通）
- **类别**：正确性 / 安全（集群语义缺失）
- **代码**（usecase 只看本地 online registry）：
  ```go
  func (a *App) kickLocalDevice(uid string, flag frame.DeviceFlag, delay time.Duration, reason string) {
      if a == nil || a.online == nil {
          return
      }
      for _, conn := range a.online.ConnectionsByUID(uid) {
  ```
  app 组合根 `clusterUserUsecase.DeviceQuit` 只是 `return u.local.DeviceQuit(ctx, cmd)`，没有像 cmdsync（`SlotForKey(uid)` + 本地/远端路由）那样按 UID owner 或按会话所在节点分发。
- **触发路径**：3 节点集群，用户会话挂在节点 C。管理端向节点 A 调 `POST /user/device_quit`。token 在集群权威存储中被清空（propose 走 slot，OK），但 `kickLocalDevice` 只遍历节点 A 的本地在线表 —— 节点 C 上的活跃会话完全不受影响。且当前网关 token 校验实际未启用（`internal/app/config.go:811` 直接拒绝 `TokenAuthOn=true`，`VerifyToken` 钩子全仓无注册点），所以重连也不受已清空 token 的约束。该设备保持完全可收发。
- **后果**：legacy `/user/device_quit` 在多节点部署下只完成"清 token"一半，"踢下线"一半静默失效（management.KickUser 走 `userRoutes`+`userActions` 才是全集群正确的路径，legacy 路径缺失同等能力）。
- **建议**：为 legacy device_quit 补上与 management.KickUser 相同的跨节点 route action 分发，或在 usecase 合约中明确"仅本节点"并由组合根路由。

### [P2] 5. benchdata 用例对 UID 无命名空间限制 —— 可改写任意真实用户的登录 token
- **位置**：`internal/usecase/benchdata/app.go:59-86`（`UpsertTokens`）、`app.go:192-203`（`validateUserToken`）
- **类别**：安全
- **代码**：
  ```go
  for _, item := range items {
      if err := validateUserToken(item); err != nil {
          return resp, err
      }
  }
  ...
  for _, item := range items {
      if err := a.users.UpdateToken(ctx, item); err != nil {
          return resp, err
      }
  ```
  `validateUserToken` 只检查 uid 非空与 `@#/&` 字符，没有任何 bench 前缀/命名空间约束；`UpdateToken` 直接落到普通用户用例，覆盖任意 UID 的设备 token 并踢掉同名设备的本机会话。
- **触发路径**：运维为压测开启 `cfg.Bench.APIEnabled`（非默认，`internal/app/build.go:918`）后，`/bench/v1/users/tokens` 注册在**完全无鉴权**的 legacy API server 上（`internal/access/api/server.go:191` 仅有 CORS 中间件；bench 路由注册见 `internal/access/api/bench.go:9-15`，无任何 auth 中间件）。任何能访问该 API 端口的人 POST `{"run_id":"x","batch_id":"y","users":[{"uid":"<真实用户>","token":"attacker"}]}` → 改写该用户 token（并用 DeviceLevel=Master 触发本机踢线），完成账号接管前置。
- **后果**：bench 开关一旦打开即把"任意账号 token 覆写"暴露在无鉴权端口上；用例层没有任何防御纵深。
- **建议**：在用例层强制 bench UID 前缀/命名空间校验（拒绝非 bench 命名的 UID），或在 bench 路由上要求独立鉴权。

### [P2] 6. presence directory：每次注册/注销/查询都在全局写锁内全量扫描 leases map
- **位置**：`internal/usecase/presence/directory.go:232-242`（`sweepExpiredLocked`），调用点 `:46-50`（register）、`:96-100`（unregister）、`:112-116`（heartbeat）、`:174-178`（endpointsByUID）、`:201-205`（endpointsByUIDs）
- **类别**：性能
- **代码**：
  ```go
  func (d *directory) sweepExpiredLocked(nowUnix int64) {
      if nowUnix <= 0 {
          return
      }
      for k, lease := range d.leases {
          if lease.LeaseUntilUnix <= 0 || lease.LeaseUntilUnix > nowUnix {
              continue
          }
          d.removeOwnerSetLocked(k)
      }
  }
  ```
- **触发路径**：每个客户端连接成功都走 `Activate → RegisterAuthoritative → dir.register`，每个断开走 `dir.unregister`，`/user/onlinestatus` 批量查询走 `endpointsByUIDs` —— 全部先执行对 `d.leases` 的全 map 遍历。leases 条目数 = 本节点作为 leader 的 slot 数 × 活跃 gateway 数（slotID×gatewayNodeID×bootID），slot 数由 `Cluster.InitialSlotCount` 动态管理，可达数百，gateway 数十个 → 数千条。同时 `endpointsByUID/endpointsByUIDs` 是只读查询却因内嵌 sweep 被迫拿写锁 `d.mu.Lock()`，与注册路径完全串行。
- **后果**：连接建立热路径 O(网关数×slot数) 的持锁扫描；读查询与写互斥，连接风暴时 presence 成为吞吐瓶颈。
- **建议**：用最小堆按 `LeaseUntilUnix` 维护到期序做增量 sweep；查询路径用 RLock + 只读快照。

### [P3] 7. FLOW.md 声称权威路由走 Raft Propose，实际实现是 slot-leader 节点内存 map
- **位置**：`internal/usecase/presence/authority.go:5-10`，对照 `internal/FLOW.md:653` 与 `:450`
- **类别**：架构与文档一致性
- **代码**：
  ```go
  func (a *App) RegisterAuthoritative(ctx context.Context, cmd RegisterAuthoritativeCommand) (RegisterAuthoritativeResult, error) {
      _ = ctx
      return RegisterAuthoritativeResult{
          Actions: a.dir.register(cmd.SlotID, cmd.Route, a.now().Unix()),
      }, nil
  }
  ```
  FLOW.md:653：「**权威路由通过 Raft**: `RegisterAuthoritative` 是一次 Raft Propose，写入 Slot Leader 的状态机」。实际 `a.dir` 是 `newDirectory()` 的纯内存结构（`app.go:67`），leader 节点重启/切换即丢失该 slot 的全部路由。
- **触发路径**：slot leader 宕机 → 新 leader 的 directory 为空 → 在各 gateway 的下一次 10s 心跳 digest mismatch + `ReplayAuthoritative`（`worker.go:37-48`）补回之前，`EndpointsByUID(s)`（被 user.OnlineStatus、presence 查询消费）返回空 → 在线状态/按 presence 分类的离线通知最多错 ~10-30s。有自愈路径，但与文档承诺的"Raft 权威"语义不符。
- **后果**：文档与实现不一致；读者会误以为路由有 Raft 持久性保证。failover 窗口内 presence 查询短暂失真。
- **建议**：更新 FLOW.md 描述当前"内存权威 + 心跳对账"模型，或恢复 Raft 持久化。

### [P3] 8. device_flag 远程输入 int→uint8 截断（gosec G115 为真）；除 -1 外的负值静默映射为垃圾 flag
- **位置**：`internal/usecase/user/legacy.go:206-211`
- **类别**：安全（输入校验）/ 正确性
- **代码**：
  ```go
  func deviceQuitFlags(flag int) []frame.DeviceFlag {
      if flag == -1 {
          return []frame.DeviceFlag{frame.APP, frame.WEB, frame.PC}
      }
      return []frame.DeviceFlag{frame.DeviceFlag(flag)}
  }
  ```
- **触发路径**：`POST /user/device_quit` 请求体 `device_flag` 是任意 JSON int（`internal/access/api/user_legacy.go:11-14` 无范围校验）。`device_flag=256` → 截断为 0（APP）→ 清掉的是 APP 设备的 token 而不是调用方意图的设备；`device_flag=-2` → `DeviceFlag(-2)=254` → `GetDevice` NotFound → 静默 no-op 且返回成功。
- **后果**：按错误设备执行/静默跳过强制下线；管理语义与调用方意图不符（当前 token 校验未启用，实际杀伤有限，故 P3）。
- **建议**：在入口或 `deviceQuitFlags` 校验 flag ∈ {APP, WEB, PC}，其余返回错误。

### [P3] 9. testdata 把 1MB payload 灌入 `RetryOfJobID`（本应是"上一个失败 job 的 ID"引用字段），经 Controller Raft 逐条提交
- **位置**：`internal/usecase/testdata/controller_snapshot_jobs.go:53`
- **类别**：架构（合约误用）/ 性能
- **代码**：
  ```go
  writeCtx, cancel := context.WithTimeout(ctx, controllerSnapshotJobWriteTimeout)
  job, err := a.controllerSnapshotJobs.CreateNodeOnboardingPlan(writeCtx, cmd.TargetNodeID, deterministicPayload(cmd.Seed, payloadID, i, cmd.PayloadBytes))
  cancel()
  ```
  `CreateNodeOnboardingPlan` 的第三参在 `pkg/cluster/onboarding.go:68` 与 `controllermeta.NodeOnboardingJob.RetryOfJobID`（`pkg/controller/meta/onboarding_types.go:43-44`："links a retry job to a previous failed job"）中是 job 引用字段，被原样编码进每条 job 记录（`onboarding_codec.go:28`），无长度校验。
- **触发路径**：TestMode 开启的 e2e 环境 `POST /testdata/e2e/cluster/controller-snapshot-jobs`，`count=1_000_000, payload_bytes=1048576`（命令校验允许此上限，`controller_snapshot_jobs.go:82-86`）→ 串行 100 万次独立 Raft propose，每次携带 ~1MB 的"retry_of_job_id" → controller raft 日志与 meta store 膨胀至 TB 级，且该字段语义被破坏（任何把 RetryOfJobID 当 job 引用解析的消费方都会误读）。
- **后果**：仅 TestMode 可达（`internal/access/api/routes.go:42`），但制造出的集群状态会污染后续 snapshot/恢复路径并违背合约字段语义。
- **建议**：给 testdata 增加独立 payload 字段或专用 dataset 接口，不要复用 RetryOfJobID。

### [P3] 10. benchdata 批量写入逐条串行权威 RPC —— 万级 batch 产生 2 万次往返
- **位置**：`internal/usecase/benchdata/app.go:79-85`（`UpsertTokens`）
- **类别**：性能
- **代码**：
  ```go
  for _, item := range items {
      if err := a.users.UpdateToken(ctx, item); err != nil {
          return resp, err
      }
      resp.Accepted++
  }
  ```
- **触发路径**：默认 `APIMaxBatchSize=10000`（`internal/app/config.go:1757`）的一个 `/bench/v1/users/tokens` 请求 → 每条 `UpdateToken` 内部是 `GetUser`（权威读 RPC）+ `UpsertDevice`（Raft propose）两次跨 slot 往返 → 最多 2 万次串行 RPC，全部占用一条 HTTP 连接与一个 goroutine。
- **后果**：压测准备阶段极慢（万级用户可能分钟级），无并发度、无批量合并；非正确性问题。
- **建议**：批内有限并发或提供批量 token 写入端口。

## 已排除的候选项

- `internal/usecase/presence/authority.go` 全部 `_ = ctx` —— 侦察线索称"stub/no-op 占位"。复核：这些方法体是**真实的内存权威实现**（`a.dir.register/unregister/...` 都做了实际工作），`_ = ctx` 只是把未使用的 ctx 参数显式丢弃（无阻塞 I/O，丢弃无害）。不是未实现的桩，也不是吞错误。真正的文档问题在 FLOW.md 侧，见发现 7。
- `internal/usecase/user/token.go:14` gosec G101 "hardcoded credentials" —— 该行是 `updateTokenKickReason = "账号在其他设备上登录"`（踢线原因文案），不是凭据。误报。
- `internal/usecase/channel/app.go:229,246,250` member-list 变更传死值 `subscriberMutationVersion=1` —— 看起来像版本语义错误。复核：allow/deny/temp 名单存放在独立命名空间 channel（`__wk_internal_memberlist__/...`），其 `SubscriberMutationVersion` 与普通频道订阅者无关，传 1 恒可推进（存储取 max）；无消费者对这些名单做版本对账。实际安全。
- `internal/usecase/presence/directory.go:62-72` 在 `range d.byUID[route.UID]` 迭代中 `delete` —— Go 规范允许迭代中删除当前项，且删除的 key 就是本次迭代项。安全。
- `internal/usecase/presence/gateway.go:61-76` Activate 在 dispatchActions 部分失败后回滚 —— 已 dispatch 的 kick 无法撤销、旧会话被从 directory 删除但可能未被踢。复核：旧会话所在 gateway 的 10s 心跳会因 digest mismatch 触发 `ReplayAuthoritative`（`worker.go:37-48`）把其本地路由补回 directory，且本地 `online` 表从未删除该会话，最终一致。有设计的补偿路径，不单列。
- `internal/usecase/presence/directory.go:311-334` `deleteOwnerRoute` 的兜底全 ownerSet 扫描 —— O(#leases) 但仅在常规键（slotID+nodeID+bootID）未命中时触发（slot 迁移场景），频率极低。不构成实际问题。
- `internal/contracts/messageevents/events.go:19-23` `Clone()` 可能漏拷贝深字段 —— 复核 `pkg/channel/types.go:48-67`：`Message` 中唯一可变引用字段是 `Payload []byte`（已拷贝）；`Framer` 是纯值结构体（`pkg/protocol/frame/common.go:6-17`），`MessageScopedUIDs` 已拷贝。Clone 完整。三个合约包（deliveryevents/messageevents/channelmembers）均有真实消费方（internal/app、internal/usecase/message、internal/usecase/delivery 等），无死合约；未发现"一侧当 count、另一侧当 index"的字段歧义。
- `internal/usecase/benchdata/app.go` 无鉴权即可改数据 —— 入口门控（`cfg.Bench.APIEnabled` 默认 false、`s.benchEnabled`）属 unit 04 的 access/api 分片；本报告发现 5 只就"用例层自身无命名空间限制"立论。
- `internal/usecase/user/legacy.go:177-183` `quitDevice` 把 DeviceLevel 覆写为 Master 且不保留原 DeviceID —— 复核 `pkg/db/meta/table_device.go:68-79`：设备记录值只含 (Token, DeviceLevel)，DeviceID 不在存储记录中，清空 token 时把 level 置 Master 无额外信息损失。排除。
- `internal/usecase/presence/worker.go:10-19` `HeartbeatOnce` 错误 join 后返回 —— 返回值正确；调用方 `internal/app/presenceworker.go:85` 用 `_ =` 丢弃属于 unit 03（internal/app）分片，不在本分片立论。

## 本分片整体评价

这部分"周边用例"代码风格统一、入口无关性遵守良好（未发现 usecase 依赖 access/app 的分层违规），presence directory 的 lease/ownerSet/digest 对账设计有清晰的补偿路径，contracts 包小而完整。最需要优先处理的是 **user.DeviceQuit 的静默失败（发现 1）**：一个安全语义的管理动作在存储层任何抖动下都返回成功，且与发现 4（只踢本节点）叠加后，legacy 强制下线在集群里既可能部分失败也可能全节点失效而无人知晓。其次是 channel 用例的两处非原子性（发现 2 的 fail-open 权限窗口、发现 3 的版本竞态），它们都源于"多步存储提交 + 用例层读改写"缺乏事务边界，建议一并引入存储层原子 batch/CAS 解决。本分片全部为 v1 生产运行路径代码（internalv2 无关）。
