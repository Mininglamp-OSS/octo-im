# 核心业务用例：消息 / 投递 / 会话 / CMD 同步（09-usecase-core-message）

## 覆盖情况

分片路径：`internal/usecase/message/`、`internal/usecase/delivery/`、`internal/usecase/conversation/`、`internal/usecase/cmdsync/`。以下列出分片内**全部**非测试 `.go` 文件（行数以 worktree 实测为准）。

| 文件 | 行数 | 是否通读 |
|---|---|---|
| internal/usecase/message/send.go | 564 | 是 |
| internal/usecase/message/app.go | 175 | 是 |
| internal/usecase/message/permission.go | 173 | 是 |
| internal/usecase/message/permission_cache.go | 140 | 是 |
| internal/usecase/message/sync.go | 135 | 是 |
| internal/usecase/message/command.go | 116 | 是 |
| internal/usecase/message/deps.go | 82 | 是 |
| internal/usecase/message/recvack.go | 22 | 是 |
| internal/usecase/message/result.go | 9 | 是 |
| internal/usecase/delivery/subscriber.go | 452 | 是 |
| internal/usecase/delivery/source.go | 42 | 是 |
| internal/usecase/delivery/submit.go | 32 | 是 |
| internal/usecase/delivery/app.go | 25 | 是 |
| internal/usecase/delivery/types.go | 22 | 是 |
| internal/usecase/delivery/offline.go | 19 | 是 |
| internal/usecase/delivery/ack.go | 19 | 是 |
| internal/usecase/delivery/personcodec.go | 17 | 是 |
| internal/usecase/delivery/deps.go | 13 | 是 |
| internal/usecase/conversation/projector.go | 403 | 是 |
| internal/usecase/conversation/active_hint_cache.go | 501 | 是 |
| internal/usecase/conversation/sync.go | 287 | 是 |
| internal/usecase/conversation/app.go | 61 | 是 |
| internal/usecase/conversation/deps.go | 29 | 是 |
| internal/usecase/conversation/types.go | 69 | 是 |
| internal/usecase/conversation/unread.go | 119 | 是 |
| internal/usecase/conversation/delete.go | 60 | 是 |
| internal/usecase/cmdsync/pending.go | 605 | 是 |
| internal/usecase/cmdsync/app.go | 379 | 是 |
| internal/usecase/cmdsync/pending_file.go | 93 | 是 |
| internal/usecase/cmdsync/records.go | 221 | 是 |
| internal/usecase/cmdsync/intent.go | 127 | 是 |
| internal/usecase/cmdsync/types.go | 80 | 是 |

合计 4,905 行非测试代码，全部逐行通读。测试文件按需查阅用于佐证语义（如 `app_test.go` 佐证 SyncRecordCache 语义），未审计测试质量。

前置阅读：`internal/FLOW.md`（v2.1）已通读；本分片四个包目录下无独立 FLOW.md。跨包上下文（`internal/app/build.go` 组装、`pkg/slot/proxy` 存储实现、`pkg/db/meta` 合并语义、`internal/runtime/deliverytag` tag fence、`internal/access/api` 入口）按两阶段要求阅读，发现仅落在分片内。

## 发现

### [P1] 1. pending CMD 恢复文件损坏被静默吞掉，UID owner 重启后离线 CMD 同步游标永久丢失
- **位置**：`internal/usecase/cmdsync/pending_file.go:67-80`
- **类别**：正确性 / 数据丢失
- **代码**：
  ```go
  if err := json.NewDecoder(file).Decode(&updates); err != nil {
      if errors.Is(err, io.EOF) {
          _ = os.Remove(path)
          return nil
      }
      badPath := path + ".bad"
      _ = os.Remove(badPath)
      if renameErr := os.Rename(path, badPath); renameErr != nil && u.logger != nil {
          u.logger.Warn("rename bad pending CMD conversation updates file failed")
      }
      if u.logger != nil {
          u.logger.Warn("ignore bad pending CMD conversation updates file")
      }
      return nil
  }
  ```
- **触发路径**：(1) 用户 A 的 CMD 会话走 request-scoped / delivery UID observer 路径，intent 进入 `ConversationUpdater.PushIntent`（`pending.go:183`），此时状态仅存内存 pending buffer；(2) flushLoop 周期 flush 因 store 不可用或节点繁忙持续失败（`pending.go:333` 出错只 `return err`，数据仍留在 shard 中）；(3) graceful stop 时 `StopContext`（`pending.go:139`）先 `Flush(ctx)` 失败，随后 `savePendingFile` 把内存 pending 写入 `cmd_conversation_updates.json`——但 `os.OpenFile` + `json.Encode` + `os.Rename` 中间没有 fsync，节点此时断电（进程未被杀、文件系统页缓存丢失）导致 rename 后文件内容截断；(4) 重启时 `loadPendingFile` 解码失败，文件被改名 `.bad`，只打一条 Warn，`Start()` 仍返回 nil，进程照常服务；(5) 这批 pending 对应的 `CMDConversationState` 从未落过 raft（`UpsertCMDConversationStates` 是唯一 durable 路径，全部失败），用户的 `/message/sync` 从此按 `max(ReadSeq, DeletedToSeq)+1` 游标继续读，**被覆盖的那批 CMD 消息对该用户永久不可见**——且与 `internal/FLOW.md:666` 自述的 "graceful stop 可恢复未 flush pending" 直接矛盾。
- **后果**：UID owner 节点断电/磁盘故障场景下，离线用户的 CMD 消息（系统指令类）静默丢失，且唯一痕迹是一条 Warn 日志；损坏文件即便留存 `.bad` 也没有任何 reload/告警消费方。
- **建议**：写 pending 文件后 fsync；解码失败至少升级为 Error 级并暴露到 metrics，`.bad` 文件应支持人工恢复工具，或改为 pending 直写 raft、内存文件仅作加速。

### [P1] 2. 恢复后的 pending 文件在首次 flush 前被"半清空"：durable flush 成功但 savePendingFile 覆盖失败会把已落库的 pending 永久留在磁盘恢复文件中
- **位置**：`internal/usecase/cmdsync/pending.go:459-479` 与 `pending_file.go:20-50`
- **类别**：正确性 / 状态机缺陷
- **代码**：
  ```go
  func (u *ConversationUpdater) saveRestoredPendingFileIfNeeded() error {
      u.mu.Lock()
      dirty := u.restoredFileDirty
      u.mu.Unlock()
      if !dirty {
          return nil
      }
      if err := u.savePendingFile(); err != nil {
          return err
      }
      hasPending := len(u.snapshotUpdates()) > 0
      u.mu.Lock()
      u.restoredFileDirty = hasPending
      u.mu.Unlock()
  ```
- **触发路径**：(1) 重启加载 pending 文件，`restoredFileDirty = true`（`pending.go:91`）；(2) 后台 flushLoop 触发 `Flush`，`UpsertCMDConversationStates` 成功——durable 状态已写入 raft/Pebble；(3) `Flush` 末尾调用 `saveRestoredPendingFileIfNeeded`，此时 `savePendingFile` 写恢复文件**失败**（磁盘满、权限），返回 err，`restoredFileDirty` 保持 true；(4) flushLoop 每秒重试 Flush，此时 `snapshotFlushEntries` 为空、`UpsertCMDConversationStates` 不再被调用，只剩反复尝试写恢复文件；(5) 期间客户端调 `/message/sync` + `/message/syncack`：`SyncAck`（`app.go:199-219`）走 `MarkSynced` 删除 pending 内存条目并推进 durable ReadSeq——但注意 `MarkSynced`（`pending.go:299`）里 `throughSeq < pendingUserLastMsgSeq` 时直接 return，不删除；而恢复文件里的旧 pending 记录 `UserReadSeqs[uid]` 早于本次 syncack 推进的 ReadSeq；(6) 下次重启再加载恢复文件，`putLoadedUpdate`（`pending.go:481`）用**旧的** readSeq 重新生成 intent 推回 pending——`PushIntent` 按 max 合并不会回退，但这个"僵尸 pending"让该 UID 的 CMD 会话永远以 pending overlay 出现在 `/message/sync` 里（`app.go:104` mergePendingViews），直到 `MarkSynced` 的 `throughSeq >= pendingUserLastMsgSeq` 检查通过才清除——由于 LastMsgSeq 是旧值而 durable ReadSeq 已更高，**下一次 syncack 即可清掉**；真正的问题在第 4-5 步之间：恢复文件保存持续失败期间 `restoredFileDirty` 永远为 true，`Flush` 每次都返回 `savePendingFile` 的错误，`flushLoop` 只 Warn，状态滞留且与 durable 状态漂移。
- **后果**：durable 已落库但恢复文件刷不掉时，进程进入"每秒一次失败写盘 + Warn"的稳态；磁盘满期间该节点所有 graceful stop 都会因 `StopContext` 返回 saveErr 而被上层记为停止失败。属于健壮性缺陷而非数据丢失（durable 侧已有数据）。
- **建议**：把 `restoredFileDirty` 语义与"文件内容 == 内存 pending 快照"解耦；savePendingFile 失败不应让 Flush 返回错误（durable 已成功），并限制重试频率。

### [P1] 3. `putLoadedUpdate` 用 `context.Background()` + `_ =` 吞掉 PushIntent 失败：畸形恢复记录被静默丢弃
- **位置**：`internal/usecase/cmdsync/pending.go:481-499`（侦察提示确认属实）
- **类别**：错误处理 / 正确性
- **代码**：
  ```go
  func (u *ConversationUpdater) putLoadedUpdate(update PendingConversationUpdate) {
      for uid, readSeq := range update.UserReadSeqs {
          ...
          intent := ConversationIntent{
              CommandChannelID: update.CommandChannelID,
              ChannelType:      update.ChannelType,
              MessageSeq:       lastMsgSeq,
              ...
          }
          _ = u.PushIntent(context.Background(), intent)
      }
  }
  ```
- **触发路径**：恢复文件里存在一条 `MessageSeq == 0` 或 `ChannelID` 非 `____cmd`（如手工编辑、旧版本格式、部分写损坏但 JSON 语法合法）的 update；`PushIntent` → `normalizeConversationIntent`（`pending.go:359-378`）对 `intent.MessageSeq == 0 || !IsCommandChannel(...)` 返回 `ErrIntentRequired`；错误被 `_ =` 丢弃，**该 UID 的这条 pending 记录既没有进内存 buffer，也没有保留在文件中可被后续修正**（下次 Stop 的 savePendingFile 快照来自内存 shard，该条目已消失）——等价于静默丢弃了一条本应恢复的 CMD 会话状态。逐条丢弃是逐 UID 的：一个畸形 update 可丢弃多个用户的 pending read 游标。
- **后果**：合法 JSON 但语义非法的恢复记录导致对应 UID 的 CMD 会话游标回退丢失，无日志、无指标。
- **建议**：逐条记录 Warn 日志（带 uid/channelID）；`lastMsgSeq` 兜底自 `update.LastMsgSeq` 的逻辑应在构造 intent 前保证非零。

### [P1] 4. conversation 同步 `unread` 计算整数截断 + `LastMsgSeq`/`ReadToMsgSeq` u64→u32 截断，长期运行频道游标错乱
- **位置**：`internal/usecase/conversation/sync.go:155-186`（gosec G115 at sync.go:185/183/157 确认为真问题）
- **类别**：正确性（分布式一致性）
- **代码**：
  ```go
  unread := 0
  if latest.MessageSeq > baseReadedTo {
      unread = int(latest.MessageSeq - baseReadedTo)
  }
  ...
  conversation: SyncConversation{
      ...
      LastMsgSeq:      uint32(latest.MessageSeq),
      ...
      ReadToMsgSeq:    uint32(readedTo),
  ```
- **触发路径**：`channel.Message.MessageSeq` 与 `UserConversationState.ReadSeq` 均为 uint64（`supportsMessageSeqU64` 分支见 `internal/usecase/message/send.go:519-521`，新协议客户端明确支持 64 位 seq）。当某频道累计消息数超过 2^32（高频系统频道/长期运行群），或（更现实的）客户端在同一频道上下文携带超大 `last_msg_seqs` overlay、配合历史导入频道：`uint32(latest.MessageSeq)` 回绕变小，`unread = int(...)` 在 64 位平台不溢出但 `LastMsgSeq` 回绕后**小于**客户端 `ReadToMsgSeq`，客户端据此把会话判为"已读/回退"，造成已读游标错乱与未读数错误；`deletedToSeq` 屏障判断（`sync.go:151` 用完整 u64 比较）与客户端可见的 u32 序列不一致，删除屏障对客户端失效。
- **后果**：seq 超 2^32 的频道上所有客户端会话视图错乱（未读数错误、消息被误判已读）；当前单频道消息量未达阈值时为潜伏缺陷，但这是协议显式支持 u64 seq 后遗留的 32 位线路格式字段。
- **建议**：`SyncConversation` 线路字段升级为 u64（协议已支持 `SupportsMessageSeqU64`），或在此处显式对超过 u32 的频道拒绝同步并返回错误而非回绕。
- （关联排除说明：`message/send.go:510` `Timestamp: int32(now.Unix())` 的 G115 属误报——2038 年前不会溢出，且 frame 协议字段本身是 int32。）

### [P1] 5. projector 的 async worker goroutine 在 Stop 之后仍可被 `scheduleFlush` 重新拉起，并在 app 已关闭 store 后写库
- **位置**：`internal/usecase/conversation/projector.go:199-225` 与 `Stop()`（131-165）
- **类别**：并发 / 生命周期
- **代码**：
  ```go
  func (p *projector) scheduleFlush() {
      p.mu.Lock()
      if p.workerRunning {
          p.mu.Unlock()
          return
      }
      ctx := p.ensureFlushContextLocked()
      p.workerRunning = true
      p.flushWG.Add(1)
      p.mu.Unlock()

      go p.async(func() {
          defer p.flushWG.Done()
          defer func() {
              p.mu.Lock()
              p.workerRunning = false
              hasPending := len(p.pending) > 0
              p.mu.Unlock()
              if hasPending {
                  p.scheduleFlush()
              }
          }()
  ```
- **触发路径**：(1) `Stop()` 将 `running=false`、`flushCtx=nil`、`flushCancel=nil`、close(stopCh)、`<-doneCh` 等待 ticker goroutine 退出（`projector.go:148-164`）；(2) **竞态窗口**：在 `Stop` 取锁之前，一个 `SubmitCommitted` 已通过 `enqueuePending`（拿 mu 入队成功）但尚未调用 `scheduleFlush`；或 worker goroutine 的 defer 分支在 Stop 释放 mu 之后执行 `p.scheduleFlush()`；(3) `scheduleFlush` 不检查 `p.running`，`ensureFlushContextLocked` 在 Stop 清空后又新建 `context.Background()` 派生的 ctx，`go p.async(...)` 起一个**Stop 之后**的新 flush worker；(4) 该 worker 调 `p.Flush(ctx)` → `p.store.SubmitUserConversationActiveHints` → slot proxy → raft/本地 Pebble——app 生命周期里 projector Stop 之后紧跟数据库关闭（`internal/FLOW.md:284-316` 停止序列），写入打在正在关闭的 store 上；(5) `flushWG.Wait()`（Stop 内）与该新 worker 之间无 happens-before：Wait 可能在新 goroutine `flushWG.Add(1)` 之前返回，goroutine 泄漏 + use-after-close。
- **后果**：优雅停机期间 goroutine 泄漏与对已关闭/正在关闭存储的写入；`WaitGroup.Add` 与 `Wait` 并发违反 WaitGroup 文档契约（Add 必须在 Wait 前 or 由持有计数的一方调用）。
- **建议**：`scheduleFlush`/`enqueuePending` 检查 `p.running`；Stop 时把 worker 的 defer 重排感知为 no-op（用 stopCh 或 atomic flag）。

### [P1] 6. delivery subscriber resolver `channelMutationVersion` 把所有错误都当成版本 0：陈旧 tag 被"降级为合法"，订阅者变更后投递仍用旧分区
- **位置**：`internal/usecase/delivery/subscriber.go:430-452`
- **类别**：正确性（分布式一致性）
- **代码**：
  ```go
  func (r *subscriberResolver) channelMutationVersion(ctx context.Context, id channel.ChannelID) uint64 {
      if r == nil || r.metadata == nil {
          return 0
      }
      ch, err := r.metadata.GetChannel(ctx, id.ID, int64(id.Type))
      if err != nil {
          return 0
      }
      return ch.SubscriberMutationVersion
  }
  ```
- **触发路径**：(1) 频道 G 的订阅者刚发生变更（踢人/拉黑），`SubscriberMutationVersion` 升到 N，deliverytag 缓存里的 tag 停留在版本 N-1；(2) 新一条 committed 消息投递，`BeginSnapshotWithRequest` → `resolveStoreBackedVersions`（`subscriber.go:124-132`）调 `channelMutationVersion` 拿当前版本；(3) 此刻 `GetChannel` 因 slot leader 迁移 / raft 读超时 / 网络抖动返回 error，函数吞掉错误返回 0；(4) 版本 0 使 `deliverytag/manager.go:198-207` 的 `request.SubscriberMutationVersion != 0` 分支整个被跳过（0 被视为"未提供"），tag 缓存判定 hit，投递继续用**变更前的订阅者分区**；(5) 被移除的订阅者照样收到消息，新加的订阅者漏收该条——`deleted`/`added` 的投递错误方向双向成立，且没有重试，错误被永久掩盖。
- **后果**：订阅者变更后的一个投递窗口内出现"该收没收 / 不该收收到"，并且元数据读取错误被静默吞掉，无任何日志；这正是 brief 提示的"严格/宽松并存、宽松版在主路径"模式的实例（`commandSourceMutationVersion` 有 `GetChannelForPermission` 严格回退，`channelMutationVersion` 完全宽松）。
- **建议**：错误应上抛中止本次 resolve（走既有 retry 轮），而不是返回 0；0 作为"无版本"哨兵值与真实版本 0 冲突也应处理。

### [P2] 7. `permissionCache` 在锁内 `clear(values)` 全表清空：65536 条目权限缓存按 TTL 满后周期性全量失效，权限读打回存储层
- **位置**：`internal/usecase/message/permission_cache.go:132-140`
- **类别**：性能
- **代码**：
  ```go
  func permissionCachePut[K comparable, V any](c *permissionCache, values map[K]permissionCacheEntry[V], key K, value V, err error, expiresAt time.Time) {
      c.mu.Lock()
      defer c.mu.Unlock()

      if len(values) >= permissionCacheMaxEntries {
          clear(values)
      }
      values[key] = permissionCacheEntry[V]{value: value, err: err, expiresAt: expiresAt}
  }
  ```
- **触发路径**：三个 map（channels/contains/hasAny）共享 `permissionCacheMaxEntries = 65536` 上界。大群场景下 `contains` map 的 key 是 (channelID, channelType, uid) 三元组——一个 10 万成员群 + 一条群消息触发的权限检查会往 `contains` 写入发送者 + 可能的 denylist/allowlist 成员查询；多个大群滚动后 `contains` 达到 65536，下一次 Put 触发 `clear(values)`，**全部**条目（包括高频热频道的）瞬间失效，随后一波并发 send 权限检查全部 miss、集中打到 `PermissionStore`（slot proxy → 可能跨节点 RPC）。该模式随缓存再满周期性重复。
- **后果**：周期性的权限读惊群（thundering herd），恰逢发送高峰时放大存储层延迟；单把 `sync.Mutex`（非分片锁）串行化所有权限缓存读写本身也是热路径瓶颈。
- **建议**：换成 LRU/分片淘汰（按过期时间近似 LRU），或至少随机/按最旧淘汰单条而非 clear 全表；读多写少可改 RWMutex 或分片锁。

### [P2] 8. cmdsync `Sync` 对每个候选频道做一次 `LoadCommandMessages`（含跨节点 RPC），大 UID 无界扇出且循环内无 ctx 取消检查
- **位置**：`internal/usecase/cmdsync/app.go:108-125`
- **类别**：性能
- **代码**：
  ```go
  candidates := make([]syncMessageCandidate, 0, limit)
  for _, candidate := range channels {
      fromSeq := candidate.readSeq + 1
      key := candidate.key
      msgs, err := a.messages.LoadCommandMessages(ctx, key, fromSeq, limit)
      if err != nil {
          return SyncResult{}, err
      }
  ```
- **触发路径**：`ListCMDConversationActive` + `ListPending` 各自上限 `activeScanLimit = 2000`（`app.go:16`），merge 后 channels 可达数千。循环对**每个**频道调一次 `LoadCommandMessages`——`internal/app/cmdsync.go:123-153` 显示远端频道走 `loadRemote`（node RPC）。一个积累了 2000 个 CMD 会话且 command log 多数在远端的 UID 发起 `/message/sync`，会串行发起最多 2000 次跨节点 RPC，全部在一个 HTTP 请求 ctx 内；中途客户端断开，循环仅在底层 RPC 超时才感知。同时每次 `LoadCommandMessages` 都先 `GetChannelRuntimeMeta`，再读 log，单个请求内对同一 command channel 元数据无复用。
- **后果**：单请求 P99 可达数十秒量级，占用 UID owner 节点 RPC 池；恶意/异常客户端可用大 UID 刷此接口制造跨节点流量放大（该 API 无鉴权，见发现 12）。
- **建议**：候选数收敛（先按 activeAt 截断到 limit 相关频道）；循环内检查 `ctx.Err()`；`limit` 语义本可只取 activeAt 最高的前 N 个频道。

### [P2] 9. cmdsync `Sync` 每个 candidate 从 `readSeq+1` 全量拉 `limit` 条消息后才在内存里排序截断：返回结果可以系统性偏向旧频道、且重复拉取浪费
- **位置**：`internal/usecase/cmdsync/app.go:108-132`
- **类别**：性能 / 正确性（顺序性）
- **代码**：
  ```go
  for _, candidate := range channels {
      fromSeq := candidate.readSeq + 1
      ...
      msgs, err := a.messages.LoadCommandMessages(ctx, key, fromSeq, limit)
      ...
  }
  sort.Slice(candidates, func(i, j int) bool {
      return syncMessageLess(candidates[i], candidates[j])
  })
  if len(candidates) > limit {
      candidates = candidates[:limit]
  }
  ```
- **触发路径**：`sortSyncChannelCandidates`（`app.go:288`）按 activeAt 降序排频道，但消息级 `syncMessageLess`（`app.go:300`）按 `Timestamp` **升序**排——最终返回给客户端的是"最旧优先"的 limit 条。若总候选消息 > limit，截断发生在升序排序之后：**永远丢掉最新的消息**（留在后面的恰是 Timestamp 最大的）。用户每次 sync 收到的是最旧一批，syncack 推进 `LastReturnedMsgSeq`（每 key 取 max）后，下次 sync 再从推进点拉取——若积压持续大于 limit，用户永远在追赶最旧消息，最新 CMD 指令可能长期不可见（饥饿）。这与"离线同步应最新优先"的常识相反，且 `FLOW.md:537` 自述"按 Timestamp ... 稳定排序并按 limit 截断"未说明是升序截断语义。
- **后果**：大积压用户持续收不到最新 CMD 消息（消息时效性倒挂）；同时每频道都按全 limit 拉取（哪怕最终只有 1 条入选），跨节点流量放大 `候选数 × limit`。
- **建议**：先按频道 activeAt 限流候选，再按 Timestamp 降序截断，或用堆保留最新 limit 条。

### [P2] 10. `SyncRecordCache.evictOverflowLocked` 是 O(n) 线性扫描逐条淘汰，且 `Replace` 每次 sync 全量 prune 整个 map
- **位置**：`internal/usecase/cmdsync/records.go:92-107, 184-209`
- **类别**：性能
- **代码**：
  ```go
  now := c.now()
  c.mu.Lock()
  defer c.mu.Unlock()
  c.pruneExpiredLocked(now)
  ...
  c.entries[uid] = syncRecordEntry{...}
  c.evictOverflowLocked()
  ```
  与
  ```go
  func (c *SyncRecordCache) pruneExpiredLocked(now time.Time) {
      for uid, entry := range c.entries {
          if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
              delete(c.entries, uid)
          }
      }
  }
  ```
- **触发路径**：每次 `/message/sync` 都持全局 `c.mu` 执行一次全 map prune（`records.go:93`），entries 达 `maxUIDs=4096` 时每次 sync 扫 4096 项；再叠加溢出时 `evictOverflowLocked` 每淘汰一条 O(n) 扫描找最旧。4096 UID 并发 sync 时，这把全局锁上的 O(n) 扫描成为 sync 路径串行点。量级不算致命（4096 很小），但每请求全扫 + 全局互斥锁的模式在 UID 数上调（配置可改）后线性恶化。
- **后果**：sync 热路径上的周期性 O(n) 锁内扫描；高并发 sync 时锁竞争。
- **建议**：惰性 TTL（读写时检查单条）+ 溢出时批量/随机淘汰；或 min-heap。

### [P2] 11. `ConversationUpdater.Flush` 的 `removeFlushedEntries` 以"快照值仍相等"判断可删，与并发 `PushIntent` 存在 ABA 式丢更新窗口
- **位置**：`internal/usecase/cmdsync/pending.go:425-439` 与 `pending.go:306-339`
- **类别**：并发 / 正确性
- **代码**：
  ```go
  func (u *ConversationUpdater) removeFlushedEntries(entries []pendingFlushEntry) {
      for _, entry := range entries {
          key := CommandChannelKey{...}
          shard := u.shardForKey(key)
          shard.mu.Lock()
          update := shard.pendingByChannel[key]
          if update != nil &&
              pendingUserLastMsgSeq(update, entry.state.UID) <= entry.lastMsgSeq &&
              pendingUserActiveAt(update, entry.state.UID) <= entry.state.ActiveAt &&
              update.UserReadSeqs[entry.state.UID] <= entry.state.ReadSeq {
              u.removeUIDLocked(shard, entry.state.UID, key)
          }
          shard.mu.Unlock()
      }
  }
  ```
- **触发路径**：`snapshotFlushEntries`（拿 shard 锁取快照后释放）与 `UpsertCMDConversationStates`（网络/raft 提议，毫秒~秒级）之间存在窗口：flush 批次 500 条（`defaultPendingFlushBatch`）时后面 batch 的写延迟让前面 batch 的快照早已过期。`<=` 比较保证"有更新则不删"的方向是对的（readSeq 只增），所以**新消息不会丢**；但注意 `pendingUserLastMsgSeq(update, uid) <= entry.lastMsgSeq` 用的是 per-UID lastMsgSeq，而 `UpsertCMDConversationStates` 落库的 `CMDConversationState`（`pending.go:395-402`）**不包含 lastMsgSeq**（结构只有 ReadSeq/ActiveAt/UpdatedAt）——durable 侧只有 ReadSeq。此时如果 flush 成功、removeFlushedEntries 删除该 UID，但该 UID 的 `ReadSeq == 0`（普通收件人，`intent.go:82` `readSeqs[uid] = 0`）且后续没有 syncack：**durable 状态里 ReadSeq=0/ActiveAt>0 已写入，pending 已删**——下次 sync 从 durable state 读到该频道（activeAt 使其进入 `ListCMDConversationActive`），`fromSeq = max(0, deletedToSeq)+1 = 1`，行为正确。复核结论：该路径自洽，真正的窗口是 `entry.lastMsgSeq` 比较保护了未被 durable 覆盖的字段，方向正确。**降级为 P2 记录一个残余缺口**：`removeFlushedEntries` 比较成功但 `UpsertCMDConversationStates` 实际写入的是同 shard 其它 UID 的合并结果（mergeCMDConversationState 取 max，见 `pkg/db/meta/table_cmd_conversation.go:214-231`），不存在丢数据——确认安全后此条仅保留观察价值，真正的 P2 在于**每个 flush batch 都要重新获取每条 entry 的 shard 锁**（batch 内条目常集中同 shard），高扇出 UID 场景下 flush 与 PushIntent 锁竞争放大。
- **后果**：flush 热路径锁竞争放大；无数据丢失（已复核）。
- **建议**：removeFlushedEntries 按 shard 分组一次加锁处理。

### [P2] 12. `/message/sync` 与 `/message/syncack` 无任何鉴权与限流：任意调用方可枚举/推进任意 UID 的 CMD 游标
- **位置**：`internal/usecase/cmdsync/app.go:84-96, 161-178`（入口：`internal/access/api/routes.go:70-71`，API 层 `message_sync.go:22-58` 未做调用方身份校验——发现落点在 usecase 层缺 UID 所有权校验）
- **类别**：安全 / 架构
- **代码**：
  ```go
  func (a *App) Sync(ctx context.Context, query SyncQuery) (SyncResult, error) {
      uid := strings.TrimSpace(query.UID)
      if uid == "" {
          return SyncResult{}, ErrUIDRequired
      }
  ```
  `SyncAck` 同样只凭 `cmd.UID` 就推进该 UID 的 `CMDConversationState.ReadSeq`（`app.go:181-193`）。
- **触发路径**：`routes.go:70-71` 的 POST `/message/sync` `/message/syncack` 与 `/user/token` 等同处 engine 根路由，无 auth 中间件（server.go:191 仅 CORS）；任何能访问 API 端口的调用方以任意 `uid` 调 `/message/syncack`，即可把受害者全部 CMD 会话的 ReadSeq 推到 `LastReturnedMsgSeq`（可先 sync 拿到 seq 再 ack），受害者的离线 CMD 消息未读态被抹掉；也可用他人 UID 做 sync 枚举 CMD 内容摘要。这与 `/api/message/send` 的模型一致（系统 API 假定内网），但在零信任部署/端口误暴露时是横向越权。message.Send 有 `FromUID` 权限链（permission.go 全套检查），cmdsync 对 UID 无等价的"调用方==UID"校验，属于用例层防御缺失。
- **后果**：跨用户 CMD 游标篡改与信息枚举（依赖部署边界）；严重度受"API 端口假定内网"缓解，故 P2。
- **建议**：在 access 层注入调用方身份并校验 uid == caller（或要求系统 token），usecase 层保留 UID 参数但加可选 caller 断言。

### [P2] 13. `message.sendDurableSegment` 中 committed 事件提交失败被降级为 Warn，request-scoped 与普通消息的可靠性不对称且依赖注释外语义
- **位置**：`internal/usecase/message/send.go:400-419`
- **类别**：正确性 / 错误处理
- **代码**：
  ```go
  intentSubmitted := a.submitRequestScopedCMDConversationIntent(item.ctx, itemResult.Message, item.cmd.RequestSubscribers)
  if a.dispatcher != nil {
      if err := a.dispatcher.SubmitCommitted(item.ctx, messageevents.MessageCommitted{...}); err != nil {
          ...
          a.sendLogger().Warn("submit committed message failed", fields...)
          if len(item.cmd.RequestSubscribers) > 0 {
              results[item.index] = SendBatchItemResult{Err: err}
              continue
          }
      }
  }
  results[item.index] = SendBatchItemResult{Result: sendResult}
  ```
- **触发路径**：普通（非 request-scoped）消息 append 成功后 `dispatcher.SubmitCommitted` 失败（`internal/FLOW.md:683` 明确：dispatcher 停止中返回内部错误；队列满走 best-effort fallback 不返回错误）。此时发送方拿到**成功**的 SendResult（MessageID/Seq 已分配、log 已 commit），但投递/会话副作用未入队——正确性依赖 `committed_replay` 兜底（FLOW.md:681-684）。在 replay 也异常（其自身 cursor 推进失败、节点恰在 replay 扫描窗口内重启且 replay 无持久 cursor）时，该消息的在线投递丢失，客户端无重试信号（已收到 sendack）。request-scoped 消息则把该错误返回给调用方（可重试），两者不对称是设计选择，但普通路径上 "SendResult 成功 + 投递唯一保障是补偿器" 的窗口没有被任何指标/告警语义覆盖（只 Warn）。
- **后果**：极端时序下（dispatcher 拒收 + replay 补偿缺口）已确认 commit 的消息不投递且发送方认为成功——消息"已读未达"。为 P2 因 replay 是显式设计的安全网。
- **建议**：对 dispatcher 拒收加 Error 级 + metrics 计数，供运维与 replay lag 指标联动告警。

### [P3] 14. `message.App` 大量死字段/死类型：identities、channels、recipients、remote、delivery、online、localBootID 及 CommittedMessageEnvelope / Endpoint / SequenceAllocator 均无生产引用
- **位置**：`internal/usecase/message/app.go:66-92`（字段）、`deps.go:18-39`（类型）、`app.go:152-158`（接口）
- **类别**：架构（死代码）
- **代码**：
  ```go
  type App struct {
      identities             IdentityStore
      channels               ChannelStore
      ...
      recipients             RecipientDirectory
      remote                 RemoteDelivery
      delivery               online.Delivery
      online                 online.Registry
      ...
      localBootID            uint64
  ```
- **触发路径**：grep 全仓（非测试）确认：`a.identities`/`a.channels`/`a.recipients`/`a.remote`/`a.delivery`/`a.online`/`a.localBootID` 除赋值外零引用；`IdentityStore`、`ChannelStore`、`RecipientDirectory`、`RemoteDelivery`、`SequenceAllocator`、`CommittedMessageEnvelope`、`Endpoint` 在分片外无非测试引用（`internal/app/build.go:787-788` 仍注入 IdentityStore/ChannelStore 两个永远不读的依赖）。staticcheck U1000 未报是因为字段通过 struct 字面量赋值被视为使用。
- **后果**：误导后续开发者以为消息用例还有直连身份/收件人/远程投递能力（这些已移交给 channelplane/delivery 运行时）；`Options.Online`/`Delivery`/`Recipients`/`RemoteDelivery` 的注入在 app 组合根产生虚假耦合。
- **建议**：删除死字段、死类型与对应 Options 注入。

### [P3] 15. `SyncRecordCache.Pop` 与 `delivery personcodec.go` 等为只被测试调用的死 API
- **位置**：`internal/usecase/cmdsync/records.go:110-132`；`internal/usecase/delivery/personcodec.go:7-17`
- **类别**：架构（死代码）
- **代码**：
  ```go
  // Pop atomically returns and clears the latest unexpired generation for uid.
  func (c *SyncRecordCache) Pop(uid string) []SyncRecord {
  ```
- **触发路径**：全仓非测试 grep 无 `Pop(` 调用方（生产路径用 `Peek` + `DeleteIfUnchanged`，见 `cmdsync/app.go:170,177,220`——这是"严格版 Pop 与宽松版 Peek/DeleteIfUnchanged 并存、宽松版在主路径"的另一实例，且 Pop 若被未来调用会产生 ack 后记录丢失的双删竞态隐患）；`personcodec.go` 的 `EncodePersonChannel`/`DecodePersonChannel`/`NormalizePersonChannel` 仅是 runtimechannelid 的 re-export，分片外无引用。
- **后果**：死 API；`Pop` 尤其危险——其"原子清空"语义与主路径的 DeleteIfUnchanged 语义冲突，被误用会破坏 syncack 重试（ack 失败重试时记录已被 Pop 清掉，ReadSeq 不推进）。
- **建议**：删除 Pop 与 personcodec re-export；或在 Pop 文档标注"勿与 Peek/DeleteIfUnchanged 混用"。

### [P3] 16. FLOW.md 声称 "CMDConversationUpdater 早于 DeliveryRuntime 启动、停止时在 DeliveryRuntime drain 之后保存 pending"，但组件注册顺序参数与之相反
- **位置**：`internal/FLOW.md:633`（文档）对照 `internal/app/lifecycle_components.go:78`（代码）
- **类别**：架构 / 文档一致性
- **代码**（文档）：
  ```
  - **启动顺序严格**: ... CMDConversationUpdater 早于 DeliveryRuntime 启动，因此停止时会在 DeliveryRuntime drain 之后保存 pending
  ```
- **触发路径**：FLOW.md 4.1 启动序列（265-282 行）列出 `8. cmd_conversation_updater ... 10. delivery_runtime`，与文档一致；但本条"避坑"把停止语义描述为"DeliveryRuntime drain 之后保存 pending"，而停止按逆序是先停 delivery_runtime 再停 cmd_conversation_updater——语义成立但依赖读者自行推导逆序；同时 `internal/app/build.go:417-421` 中 ConversationUpdater 的 `FlushInterval` 复用的是 `cfg.Conversation.ActiveHintFlushInterval`（默认 10s，`config.go:1414-1416`）而非文档叙述的独立 pending flush 配置，配置语义混用且无法独立调整 pending flush 频率（defaultPendingFlushInterval=1s 的常量在生产路径永远不生效）。
- **后果**：运维按文档预期 pending 1 秒级 flush，实际是 10 秒级；崩溃窗口内的 pending 丢失面比文档暗示大 10 倍。
- **建议**：给 pending updater 独立配置项；修正 FLOW.md 的配置映射描述。

### [P3] 17. `conversation.App.Options` 的 `ChannelProbeBatchSize`/`ColdThreshold`/`Deletes` 推断与 `ProjectorOptions.DirtyLimit`/`ColdThreshold` 为死配置
- **位置**：`internal/usecase/conversation/app.go:16-26`、`projector.go:41-44`
- **类别**：架构（死代码 / 文档不一致）
- **代码**：
  ```go
  // Deprecated: retained for app wiring compatibility while durable projection tuning is removed.
  DirtyLimit int
  // Deprecated: retained for app wiring compatibility while durable projection tuning is removed.
  ColdThreshold time.Duration
  ```
- **触发路径**：`ProjectorOptions.DirtyLimit`/`ColdThreshold` 已自注释 Deprecated 且结构体字段不使用（`projector` 结构体无对应字段，`projector.go:50-73`）；`internal/app/build.go:526-527` 仍在注入 `cfg.Conversation.FlushDirtyLimit`/`ColdThreshold`，使 config.go:1406-1412 的默认值逻辑成为纯死配置。`conversation.Options.ChannelProbeBatchSize`、`ColdThreshold`、`Async`（app.go 未存 async 字段——注意 app.go:44 存了 `async` 但 `Sync` 路径从不使用，仅 delete.go:54 使用）同理部分死亡。
- **后果**：配置项可被运维设置但完全无效，无任何报错。
- **建议**：删除 Deprecated 字段与对应 config 默认值，避免"配置了却无效"的运维陷阱。

### [P3] 18. `cmdsync.App.Sync` 在 candidates 截断前对全部消息排序：limit 截断后 records 仍可能引用未返回的消息
- **位置**：`internal/usecase/cmdsync/app.go:134-156`
- **类别**：正确性（轻微）
- **代码**：
  ```go
  result := SyncResult{Messages: make([]channel.Message, 0, len(candidates))}
  recordsByKey := make(map[CommandChannelKey]SyncRecord, len(candidates))
  for _, candidate := range candidates {
      ...
      if msg.MessageSeq > record.LastReturnedMsgSeq {
          record.LastReturnedMsgSeq = msg.MessageSeq
      }
  ```
- **触发路径**：`candidates` 已在 130 行截断到 limit，因此 records 只含返回消息——直接复核后此路径一致。**但** `validSyncRecords`（`app.go:346-355`）在 SyncAck 中过滤掉非 command-channel record 后 `DeleteIfUnchanged(uid, records)` 比较的是**原始 records 切片**（`app.go:220`），若某次 sync 产生过混合 record（历史版本/非 command channel），DeleteIfUnchanged 因切片不等而永不清理，该 UID 的 entry 占位到 TTL（5 分钟）过期——无害但使 TTL 成为唯一清理路径。记录为轻微。
- **后果**：极端混合 record 场景下 record 缓存条目滞留至 TTL。
- **建议**：DeleteIfUnchanged 改为对 validRecords 单独比对或标记删除。

## 已排除的候选项

- `internal/usecase/conversation/sync.go:185`（gosec G115 uint64→uint32）——**部分保留**：LastMsgSeq/ReadToMsgSeq 的 u32 截断升级为发现 4（协议支持 u64 seq，长期累积序列号确可超 2^32，符合 brief 给出的"真问题"判据）；同一行中 `int64(latest.Timestamp)` 与 `unread = int(...)` 在 64 位平台无实际溢出路径，未单独成条。
- `internal/usecase/message/send.go:396,303`（gosec G115 uint64→int64）——**误报**：`MessageID` 来自 Snowflake（63 位以内，符号位保证 0），`int64(msg.MessageID)` 不溢出；无远程输入可使其超过 2^63-1。
- `internal/usecase/message/send.go:510`（G115 int64→int32 Timestamp）——**误报**：`now.Unix()` 当前远小于 2^31，frame 协议字段本身即 int32，2038 年问题属协议级而非本行。
- `internal/usecase/message/recvack.go:12`（G115 int64→uint64）——**误报**：`MessageID` 由服务端生成（Snowflake），客户端回传的 MessageID int64 若为负会被 uint64 转成大数，但 delivery AckIndex.Lookup 只按精确 key 匹配，负值 MessageID 不存在对应 binding，仅 miss 无越界/panic 后果。
- `internal/usecase/cmdsync/pending.go:427,541`、`cmdsync/app.go:254`（G115）——**误报**：`ChannelType` 恒为 0-255 范围内的 frame 常量（`uint8(entry.state.ChannelType)` 上游即 uint8 写入）；`hashCommandChannelKey % uint32(len(u.shards))` 中 len 恒为正小整数。
- `internal/usecase/cmdsync/pending_file.go:26,37`（G301/G302/G304）——**误报**：路径由 `cfg.Node.DataDir` + 固定文件名拼接（`pending.go:17`），dataDir 来自服务端配置而非远程输入；0o755/0o644 是数据目录常规权限，文件内容非凭据。
- `internal/usecase/conversation/sync.go:110`（G115 int64→uint8）——**误报**：`state.ChannelType` 由本系统写入时即为 uint8 值域（conversationKey 上游均为 frame.ChannelType* 常量）。
- `internal/usecase/delivery/subscriber.go` nextStoreBackedPage 的快照 fallback（287-309）——初看疑似"cursor 语义混用导致重复/漏页"，复核后：fallback 仅在 `len(paged)==0 && done && cursor==""` 首页触发一次（`snapshotFallbackTried` 单次护栏），且 `seen` map 全程去重，重复投递被 delivery 侧按 UID 去重覆盖，无实际错误页。
- `internal/usecase/conversation/projector.go:181-197` enqueuePending 满时丢弃——**设计取舍，非缺陷**：active hint 本身是 best-effort（FLOW.md:684 "active hint 是可丢弃的 best-effort 路径"），durable 投影由 slot 状态机承担；队列满丢 hint 只影响排序展示新鲜度。
- `internal/usecase/conversation/active_hint_cache.go:394-405` enforceCapacityLocked 的 O(n) `lowestActiveHintKey` 扫描——容量上限 100000 且仅在超限/新 UID 时触发，单次扫描均摊可接受；`hintCountsByUID` 与 `hints` 的计数在 `deleteHintLocked`/`incrementHintCountLocked` 中保持同步（复核了 SubmitHints 中 `!ok` 才 increment 的路径与 RemoveHints/Flush 删除路径），未发现泄漏 map。**但注意** ListHotUserConversationActive（275-298）对全 map 按 uid 过滤是 O(总 hints)，UID owner 每次会话同步调用（slot/proxy/user_conversation_state_rpc.go:437）——10 万 hints 时每次 sync 全扫，因分片路径内另有 sync.go 的列表已按 limit 截断，此处记为观察项，未达发现门槛（hint 总量受 maxHints=100000 硬上界，单次扫描 <10ms 量级）。
- `internal/usecase/message/send.go:465-479` segmentContext 多 item 用最早 deadline 重建 ctx——单 item 保留原 ctx（`cancel` 为 no-op 无泄漏）；多 item 场景 `context.WithDeadline(context.Background(), earliest)` 丢弃了原 ctx 的 **cancel 信号只保留 deadline**：批量 Send 中某 item ctx 被 cancel 时 segment 不会中止——复核 `internal/app` 与 gateway 无 SendBatch 多 item 生产调用方（Send 单 item 恒走单 item 分支），当前不可达，列为观察项不立项。
- `internal/usecase/message/permission.go` visitors/person 的 `receiver` 推导（131-143）——复核 `DecodePersonChannel` 返回排序后的 (left,right)，`cmd.FromUID == right` 时 receiver=left 的推导覆盖了 from 不在频道内的错误情况（返回 NotInWhitelist 经 allowlist 链），无不一致。
- `internal/usecase/cmdsync/app.go:127-131` 排序前 `candidates` 未预分配正确容量——纯微优化，不立项。
- `internal/usecase/message/recvack.go:9` 用 `context.Background()` 替换调用方 ctx——复核调用方为 gateway RecvackPacket 处理（无请求级超时语义），且 AckRoute 内部有独立超时；丢 cancel 影响有限，未立项。

## 本分片整体评价

这四个包整体呈现出清晰的分层重构成果：依赖边界干净（`usecase/*` 未 import 任何 access/gateway 类型，仅依赖 contracts/runtime/pkg，AGENTS.md 层级约定无违例）、best-effort 路径与 durable 路径的职责划分在注释和代码中基本自洽、并发原语使用规范。最需要优先处理的是 **cmdsync pending 持久化链路（发现 1/2/3）**：它是"graceful stop 可恢复"这一文档承诺的唯一实现，却同时存在无 fsync 的原子写、损坏静默吞掉、畸形记录静默丢弃三个独立缺陷，叠加后使 UID owner 节点异常重启时离线 CMD 游标丢失成为现实路径而非理论风险。其次是投递订阅者解析对元数据读错误吞错降级（发现 6），它把订阅者变更的投递正确性寄希望于存储层永远可用。性能面上，permissionCache 全表清空（发现 7）与 cmdsync 的逐频道跨节点拉取（发现 8/9）是发送与同步两条热路径上最先碰到的天花板。

REPORT 位点：P0=0 P1=6 P2=7 P3=5（按上方编号；P1 含 #1,2,3,4,5,6；P2 含 #7-13；P3 含 #14-18）
