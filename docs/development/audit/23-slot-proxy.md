# slot 分布式存储 / RPC facade (`pkg/slot/proxy/`)

## 覆盖情况

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/slot/proxy/authoritative_rpc.go | 141 | 是 |
| pkg/slot/proxy/channel_migration_codec.go | 480 | 是 |
| pkg/slot/proxy/channel_migration_rpc.go | 491 | 是 |
| pkg/slot/proxy/channel_rpc.go | 637 | 是 |
| pkg/slot/proxy/cmd_conversation_state_codec.go | 292 | 是 |
| pkg/slot/proxy/cmd_conversation_state_rpc.go | 286 | 是 |
| pkg/slot/proxy/hashslot_compat.go | 75 | 是 |
| pkg/slot/proxy/identity_rpc.go | 276 | 是 |
| pkg/slot/proxy/identity_subscriber_codec.go | 406 | 是 |
| pkg/slot/proxy/plugin_binding_codec.go | 334 | 是 |
| pkg/slot/proxy/plugin_binding_rpc.go | 741 | 是 |
| pkg/slot/proxy/runtime_meta_codec.go | 666 | 是 |
| pkg/slot/proxy/runtime_meta_rpc.go | 430 | 是 |
| pkg/slot/proxy/store.go | 226 | 是 |
| pkg/slot/proxy/subscriber_rpc.go | 209 | 是 |
| pkg/slot/proxy/user_conversation_state_codec.go | 507 | 是 |
| pkg/slot/proxy/user_conversation_state_rpc.go | 598 | 是 |

合计 6785 行非测试代码，17 个生产文件。

**通读方法说明（便于协调者判断可信度）**：
RPC / store 类文件（`store.go`、`authoritative_rpc.go`、`hashslot_compat.go`、`identity_rpc.go`、
`subscriber_rpc.go`、`runtime_meta_rpc.go`、`channel_migration_rpc.go`、`plugin_binding_rpc.go`、
`user_conversation_state_rpc.go`、`cmd_conversation_state_rpc.go`、`channel_rpc.go`）逐行通读。
5 个 `*_codec.go` 是高度重复的 append/read 字段对，除逐段通读外，另用穷举 grep 验证了三条
结构不变量，以确保没有遗漏的解码站点：
1. 对**全部 17 个文件**（不只 `*_codec.go`）grep 切片分配 → 定位到 **15 处**线上驱动的集合
   分配，逐一确认都经由 `runtimeMetaCollectionLen`（含起初容易漏掉的 `channel_rpc.go:543/588`）。
2. grep 全部对 body 的直接下标索引 → 4 处，均由 `if offset >= len(body)` 守卫。
3. grep 全部非标准 offset 算术 → 仅 `channel_migration_codec.go:179` 一处，
   已确认其 `end < offset || end > len(body)` 双重守卫正确。
另核实：本包无 `panic(`、无 `context.Background()`、无并发原语（均为 grep 零命中）。

### 阶段 0 机械扫描结果（本分片）

gosec 6 条（全部 G115）：
- `channel_migration_codec.go:282, 291, 300` — uint64 -> uint8
- `channel_migration_codec.go:179` — uint64 -> int
- `runtime_meta_codec.go:609` — uint64 -> int
- `runtime_meta_codec.go:621` — int -> uint64

staticcheck 3 条（非测试）：
- `plugin_binding_rpc.go:34` ST1012
- `runtime_meta_codec.go:335` U1000 `runtimeMetaAppendMetaPtr`
- `runtime_meta_codec.go:362` U1000 `runtimeMetaAppendMetas`

## 发现

（追加中）
### [P1] 1. `userMergeHeap.Less` 用纯字典序比较 UID，与 Pebble 的"长度优先"键序不一致 → 权威用户分页**静默漏项**

- **位置**：`pkg/slot/proxy/identity_rpc.go:250-252`（比较器）配合 `identity_rpc.go:172-215`（k-way merge）
- **类别**：正确性 / 分布式一致性
- **代码**：
  ```go
  // identity_rpc.go:250
  func (h userMergeHeap) Less(i, j int) bool {
  	return h[i].User.UID < h[j].User.UID
  }
  ```
  而底层每个 hash slot 的迭代顺序由 Pebble 主键字节序决定，主键为
  `KeyParts{String(uid)}`（`pkg/db/meta/table_user.go` → `pkg/db/meta/table_key.go:73`），
  `String` 编码是 **uint16 大端长度前缀 + 原始字节**：
  ```go
  // pkg/db/internal/keycodec/codec.go:47-53
  func AppendString(dst []byte, value string) []byte {
  	if len(value) > maxStringLen { panic(...) }
  	dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))
  	return append(dst, value...)
  }
  ```
  所以存储序是 **先按长度、再按字典序**。同包内 `runtime_meta_rpc.go:399` 的
  `channelRuntimeMetaLess` 就正确地实现了长度优先：
  ```go
  // runtime_meta_rpc.go:428-431
  func channelRuntimeMetaLess(left, right metadb.ChannelRuntimeMeta) bool {
  	if len(left.ChannelID) != len(right.ChannelID) {
  		return len(left.ChannelID) < len(right.ChannelID)
  	}
  ```
  两个比较器在同一个包里对同一套键编码给出了**互相矛盾**的排序约定。

- **触发路径**（逐步）：
  1. 某个物理 Slot 拥有两个 hash slot A、B。A 中只有 uid `"b"`，B 中只有 uid `"aa"`。
  2. 管理接口调用 `Store.ScanUsersSlotPage(ctx, slotID, UserCursor{}, 1)`（limit=1）。
  3. `scanUsersSlotPageLocal` 从 A 取到 `"b"`、从 B 取到 `"aa"`，两项都入堆。
  4. `Less` 按字典序判 `"aa" < "b"`，`heap.Pop` 先弹出 `"aa"`；`users=["aa"]`，
     `cursor = UserCursor{UID:"aa"}`；`len(users)==limit` 循环退出。
  5. 返回 `done = queue.Len()==0` → 堆里还有 `"b"`，`done=false`，
     调用方（`internal/usecase/management/users.go:179`）带 `after={"aa"}` 请求下一页。
  6. 下一页 `loadUserMergeItem(A, after={"aa"})` → `ListUsersPage` 在 shard A 上
     seek 到 key `[0x00 0x02 'a' 'a']` 之后。`"b"` 的 key 是 `[0x00 0x01 'b']`，
     **小于** 游标，被 seek 跳过 → A 返回空，B 返回空 → `done=true`。
  7. 用户 `"b"` 从整个分页结果里**永久消失**。

- **后果**：权威用户列表（`internal/usecase/management/users.go:179/218` 的管理 API
  用户清单、`:478` 的 per-slot 存在性探测）会静默丢用户。任何基于该分页做全量枚举的
  上层逻辑（备份、审计、批量迁移、容量统计）都会得到不完整且**无任何错误提示**的结果。
  只要同一物理 Slot 的不同 hash slot 里存在长度不同的 UID（生产必然如此），就会发生。
  触发概率随 UID 长度分布的离散程度上升。

- **建议**：把 `userMergeHeap.Less` 改成与存储序一致的长度优先比较（复用
  `channelRuntimeMetaLess` 的形式），并对所有 k-way merge 比较器统一提取一个
  与 `keycodec.AppendString` 对齐的公共 `keyStringLess` 辅助函数，避免同类分歧再次出现。

### [P1] 2. `pluginBindingMergeHeap.Less` 同样是纯字典序 → 插件绑定 plugin_no 分页**静默漏项**

- **位置**：`pkg/slot/proxy/plugin_binding_rpc.go:714-725`（比较器）
  配合 `plugin_binding_rpc.go:186-233`（跨 Slot × HashSlot 的 k-way merge）
  和 `plugin_binding_rpc.go:388-413`（游标推进）
- **类别**：正确性
- **代码**：
  ```go
  // plugin_binding_rpc.go:714
  func (h pluginBindingMergeHeap) Less(i, j int) bool {
  	if h[i].Binding.PluginNo != h[j].Binding.PluginNo {
  		return h[i].Binding.PluginNo < h[j].Binding.PluginNo
  	}
  	if h[i].Binding.UID != h[j].Binding.UID {
  		return h[i].Binding.UID < h[j].Binding.UID
  	}
  ```
  底层二级索引 `idx_plugin_no_uid` 的键是
  ```go
  // pkg/db/meta/table_plugin_binding.go:66
  return KeyParts{String(binding.PluginNo), String(binding.UID)}, true
  ```
  即 `(uint16 len(PluginNo), PluginNo, uint16 len(UID), UID)` —— 存储序是 **UID 长度优先**。
  由于整个扫描固定在一个 `pluginNo` 前缀内，`PluginNo` 比较恒等，实际排序键只有 UID，
  于是比较器与存储序完全错位。

- **触发路径**（逐步）：
  1. 插件 `p1` 绑定了两个用户：uid `"b"`（落在 hash slot A）和 uid `"aa"`（落在 hash slot B），
     A 和 B 属于同一物理 Slot 0，且 A < B。
  2. `internal/usecase/plugin/binding.go:197` 调用
     `ListPluginBindingsByPluginNo(ctx, "p1", "", 1)`。
  3. 两个 shard 各自 `scanPluginBindingsSlotHashSlot(..., limit=1)` 各取一行入堆。
  4. `Less` 判 `"aa" < "b"` → 先弹出 `"aa"`；`out=["aa"]`，
     `last = {SlotID:0, HashSlot:B, Binding:{p1,"aa"}}`；`len(out)==limit` 退出。
  5. `queue.Len() > 0` → 编码 `last` 为 base64 游标返回给调用方，`hasMore=true`。
  6. 第二页：对 shard A（A < B，同 slot），
     `pluginBindingShardAfterCursor(0, A, cursor)` 返回 `A > B` = **false**，
     于是走到 `shardAfter = after.Binding = {p1,"aa"}`（`plugin_binding_rpc.go:412`）。
  7. shard A 上 `ScanPluginBindingsByPluginNo` seek 到索引键
     `(...,0x0002,'a','a')` 之后。uid `"b"` 的索引键是 `(...,0x0001,'b')`，**小于**游标 → 被跳过。
  8. 绑定 `(p1, "b")` 从分页结果里**永久消失**。

- **后果**：`internal/usecase/plugin/binding.go` 的插件绑定清单会漏用户。插件运维
  （查看某插件绑定了哪些用户、批量解绑、迁移）会基于不完整清单操作。同样无任何错误提示。

- **建议**：与发现 1 合并修复 —— 比较器改为长度优先，且与 `keycodec.AppendString` 的编码
  保持单一事实来源。

### [P2] 3. `ListPluginBindingsByPluginNo` 每页扇出 O(HashSlotCount) 次单行 RPC

- **位置**：`pkg/slot/proxy/plugin_binding_rpc.go:186-204`
- **类别**：性能
- **代码**：
  ```go
  slotIDs := append([]multiraft.SlotID(nil), s.cluster.SlotIDs()...)
  sort.Slice(slotIDs, func(i, j int) bool { return slotIDs[i] < slotIDs[j] })
  queue := make(pluginBindingMergeHeap, 0, len(slotIDs))
  for _, slotID := range slotIDs {
  	hashSlots := append([]uint16(nil), s.cluster.HashSlotsOf(slotID)...)
  	...
  	for _, hashSlot := range hashSlots {
  		item, ok, err := s.loadPluginBindingMergeItem(ctx, slotID, hashSlot, pluginNo, after)
  ```
  而 `loadPluginBindingMergeItem`（`:414`）最终调用
  `s.scanPluginBindingsSlotHashSlot(ctx, slotID, hashSlot, pluginNo, shardAfter, 1)` —— **limit 恒为 1**。
  非本地 Slot 时 `scanPluginBindingsSlotHashSlot`（`:376-386`）走
  `callPluginBindingRPC` → `callAuthoritativeRPC`，即一次完整的跨节点 RPC 往返。

- **触发路径**：
  1. 集群配置 `HashSlotCount`（`internal/app/config.go:1021-1025`，默认等于
     `InitialSlotCount`，生产上通常为数十到数百）。
  2. 管理端调用一次 `ListPluginBindingsByPluginNo(ctx, pluginNo, "", 50)`。
  3. 堆初始化阶段：遍历**全部** SlotID × 其全部 hashSlot，
     每个 (slot, hashSlot) 发一次 RPC，每次只取回 1 行 → HashSlotCount 次 RPC。
  4. 出堆阶段：每弹出一行再补一次单行 RPC → 再 50 次。
  5. 这些调用在同一个 goroutine 内**完全串行**（该包无任何并发原语，已核实）。
  6. 每次 RPC 还要重新走 `callAuthoritativeRPC` 的 peer 遍历与 leader 重定向。

- **后果**：HashSlotCount=128 时，一次 50 条的插件绑定分页要做约 178 次串行跨节点 RPC。
  按 1ms RTT 估算单页 ~180ms，且这是纯 leader 节点负载放大：一次管理查询在整个集群上
  产生 HashSlotCount 量级的 Raft leader 读。绑定数量多时翻页会线性叠加这个代价。

- **建议**：每个 shard 的预取 limit 改为页大小（而非 1），让每个 shard 一次 RPC 返回一批，
  堆内消费完再补；并把同一物理 Slot 的多个 hashSlot 合并成一次 RPC（服务端已有
  `scanPluginBindingsSlotHashSlot` 的 per-hashSlot 原语，可在 responder 侧做本地 merge）。

### [P0] 4. `runtimeMetaCollectionLen` 的元素计数上界用"剩余字节数"而非"剩余字节/单元素最小线上大小" → 64MB 未认证帧可放大成 ~4.8GB 分配，进程 OOM 致死

- **位置**：`pkg/slot/proxy/runtime_meta_codec.go:616-625`（唯一的集合长度守卫，被本包 **全部 15 处**
  `make([]T, n)` 复用）；最严重的调用点 `pkg/slot/proxy/user_conversation_state_codec.go:284-300`
- **类别**：安全 / 远程输入可达的进程崩溃
- **代码**：
  ```go
  // runtime_meta_codec.go:616
  func runtimeMetaCollectionLen(count uint64, remaining int, label string) (int, error) {
  	maxInt := uint64(^uint(0) >> 1)
  	if count > maxInt {
  		return 0, fmt.Errorf("metastore: %s count overflows int", label)
  	}
  	if count > uint64(remaining) {      // ← 上界是"剩余字节数"，等价于假设每元素最小 1 字节
  		return 0, fmt.Errorf("metastore: %s count exceeds remaining bytes", label)
  	}
  	return int(count), nil
  }
  ```
  ```go
  // user_conversation_state_codec.go:284
  func readUserConversationStates(body []byte, offset int) ([]metadb.UserConversationState, int, error) {
  	count, next, err := runtimeMetaReadUvarint(body, offset)
  	...
  	statesLen, err := runtimeMetaCollectionLen(count, len(body)-offset, "user conversation states")
  	...
  	states := make([]metadb.UserConversationState, statesLen)   // ← 先分配，再逐元素解码
  	for i := range states {
  		if states[i], offset, err = readUserConversationState(body, offset); err != nil {
  ```
  但 `UserConversationState` 的线上编码（`user_conversation_state_codec.go:303-311`
  `appendUserConversationState`）有 **7 个字段**，每个字段最小 1 字节 varint，
  所以单元素**最小线上大小是 7 字节**，而内存中 `sizeof(UserConversationState)`
  = 2×string(16) + 5×8 = **72 字节**。守卫按 1 字节/元素放行，实际放大系数是 **72 倍**。

- **触发路径**（逐步，无需任何凭证）：
  1. `pkg/transport/errors.go:15-19` `MaxMessageSize = 64 << 20`，单帧体上限 64MB；
     协调者已核实 `pkg/transport` **无鉴权、无 TLS**（grep 零命中）。
  2. 攻击者向任一节点的集群 transport 端口发一个 64MB 帧，
     service ID = `userConversationStateRPCServiceID`(11)（`store.go:39` 注册）。
  3. 帧体构造：`magic(5) + opID(1) + slotID + hashSlot + uid("") + channelID("") +
     channelType + after-marker(0) + limit` ≈ 14 字节头部，
     接着写入 `states` 的计数 uvarint = `67108840`（5 字节），其余全部填充垃圾字节。
  4. `handleUserConversationStateRPC` → `decodeUserConversationStateRPCRequest`
     （`user_conversation_state_codec.go:51`）逐字段解到 `readUserConversationStates`。
  5. 守卫计算 `remaining = len(body)-offset ≈ 67,108,840`，
     `count(67,108,840) > remaining(67,108,840)` → **false，放行**。
  6. `make([]metadb.UserConversationState, 67108840)`
     = 67,108,840 × 72 B = **4,831,836,480 B ≈ 4.83 GB**，由 runtime 立即清零。
  7. 只有分配完成后循环才会在第一个元素读取时报错 —— 内存已经吃掉了。

- **后果**：单个 64MB 的未认证帧换来 4.8GB 瞬时分配。在可用内存低于约 5GB 的节点上，
  Go runtime 抛 `fatal error: runtime: out of memory` —— 这是 **fatal error 而非 panic，
  `recover()` 无法拦截**，整个进程立刻死亡（协调者已核实 `pkg/transport/server.go:186`
  用裸 `go` 分发且无 `recover()`，所以连 goroutine 级兜底都没有）。
  攻击者可以对集群中每个节点重复此操作 → **未认证远程全集群拒绝服务**。
  即使内存充足，反复发送也会持续制造 GB 级分配压力压垮 GC。
  同一守卫覆盖本包全部 15 个集合解码点，其中请求侧（服务端、未认证可达）包括
  `user_conversation_state_codec.go:294/363/406/449/491`、`channel_rpc.go:543/588`、
  `cmd_conversation_state_codec.go:204/273`、`runtime_meta_codec.go:280`。

- **对照**：仓库内**已有正确实现**可直接借鉴 ——
  `pkg/cluster/codec_control.go:995` 用的是 `count > uint64(len(body)/minWireSize)`，
  即以单元素最小线上大小做除数。本包的守卫少了这个除数。

- **建议**：给 `runtimeMetaCollectionLen` 增加一个 `minWirePerElement` 参数，
  上界改为 `remaining / minWirePerElement`，每个调用点传入该集合元素的真实最小编码长度；
  同时对 RPC 集合再加一个与业务语义匹配的绝对上限（如订阅者命令已有的 1000 条硬上限）。

### [P0] 5. `channel_migration` RPC 的 `propose` op 把**调用方提供的任意 FSM 命令字节**直接提交进 Slot Raft，且 `ChannelID` 为空时**完全跳过 hash slot 归属校验** → 未认证远程可永久损坏元数据 Raft 组

- **位置**：`pkg/slot/proxy/channel_migration_rpc.go:398-416`（handler）；
  注册于 `pkg/slot/proxy/store.go:40`
- **类别**：安全 / 分布式一致性 / 数据丢失
- **代码**：
  ```go
  // channel_migration_rpc.go:398
  case channelMigrationRPCPropose:
  	hashSlot := req.HashSlot
  	if req.ChannelID != "" {                                   // ← ChannelID 为空则整段跳过
  		routedHashSlot, redirected, err := s.resolveChannelMigrationRPCRoute(req.ChannelID, slotID)
  		if err != nil || redirected != nil {
  			return redirected, err
  		}
  		hashSlot = routedHashSlot
  	}
  	if len(req.Command) == 0 {                                 // ← 唯一的 Command 校验：非空
  		return nil, fmt.Errorf("metastore: empty channel migration proposal")
  	}
  	if err := proposeWithHashSlot(ctx, s.cluster, slotID, hashSlot, req.Command); err != nil {
  ```
  `req.Command` 是 `channelMigrationRPCRequest.Command []byte`（`:36`），完全由线上输入控制；
  handler **没有解码它、没有校验命令类型**。
  下游 `Cluster.ProposeWithHashSlotResult`（`pkg/cluster/cluster.go:970-1029`）同样不做任何校验：
  ```go
  payload := encodeProposalPayload(hashSlot, cmd)
  leaderID, err := c.router.LeaderOf(slotID)
  ...
  future, err := c.runtime.Propose(attemptCtx, slotID, payload)
  ```
  校验**只在 apply 阶段**发生，而那时命令已经被 Raft 提交并复制：
  ```go
  // pkg/slot/fsm/statemachine.go:186-197
  if cmd.SlotID != multiraft.SlotID(m.slot) {
  	return nil, metadb.ErrInvalidArgument
  }
  hashSlot, err := m.resolveHashSlot(cmd)
  if err != nil {
  	return nil, fmt.Errorf("%w: resolve hash slot slot=%d ...", err, ...)
  }
  decoded, err := decodeCommand(cmd.Data)
  if err != nil {
  	return nil, err
  }
  ```
  对照：同包的 plugin binding handler **有**归属校验
  （`plugin_binding_rpc.go:310` / `:330`：`if !pluginBindingHashSlotOwnedBySlot(s.cluster, slotID, hashSlot)`），
  说明作者清楚需要这一步，只是迁移 handler 漏了。

- **触发路径 A —— 永久砖化 Slot（无需任何凭证）**：
  1. 协调者已核实 `pkg/transport` **无鉴权、无 TLS**。攻击者直连任一节点的集群 transport 端口。
  2. 发送 service ID `47`（`channelMigrationRPCServiceID`），op = `propose`，
     `SlotID` = 该节点当前 leader 的某个 Slot，**`ChannelID: ""`**，
     `HashSlot: 0xFFFF`（该 Slot 不拥有的 hash slot），`Command: <任意非空字节>`。
  3. `handleAuthoritativeRPC`（`authoritative_rpc.go:119`）只检查"本节点是不是该 Slot 的 leader"——通过。
  4. `req.ChannelID == ""` → `if req.ChannelID != ""` 整块被跳过 →
     `hashSlot = req.HashSlot = 0xFFFF`，**从未与 `HashSlotsOf(slotID)` 比对**。
  5. `proposeWithHashSlot` → Raft **提交**该 entry（Raft 只负责复制不透明字节）。
  6. `applyCommittedEntries` → `ApplyBatch` → `resolveHashSlot` 失败 → 返回 error。
     按 `pkg/slot/FLOW.md` §9："ApplyBatch 原子性 …… 任何一条失败会导致整个 Raft Slot fail。"
  7. 这条 entry 已经在**已提交、已复制**的 Raft 日志里。**每个副本**都会在 apply 时失败；
     进程重启后 replay 再次命中同一条 entry，再次失败。
  8. 该物理 Slot 的元数据 Raft 组**永久无法恢复**。

- **触发路径 B —— 任意元数据写入**：
  同样的入口，把 `ChannelID` 设为该 Slot 内一个真实 channel（让路由校验通过），
  `Command` 填入**任意**合法 FSM 命令的 TLV 编码。`pkg/slot/FLOW.md` §7 列出的 34 种命令
  全部可达，包括 `2: UpsertChannel`、`3: DeleteChannel`、`8/9: Add/RemoveSubscribers`、
  `1: UpsertUser`、`42: BindPluginUser`、`35: CommitChannelLeaderTransfer`、
  `38: ClearChannelWriteFence`、`39: AbortChannelMigration`。
  一个名为"channel migration propose"的 RPC 实际上是**整个集群元数据存储的未认证任意写原语**：
  可以删除任意频道、篡改订阅者、绑定插件到任意用户，或者清除迁移写围栏
  （`ClearChannelWriteFence` 会让正在 drain 的 channel 重新开写 → 与新 leader 形成双写）。

- **触发路径 C —— 畸形命令同样砖化**：
  `ChannelID` 合法但 `Command` 是无法解码的垃圾字节 → 步骤同路径 A，
  在 `decodeCommand`（`statemachine.go:194-197`）处失败 → 同样永久砖化。

- **后果**：
  路径 A/C：单个未认证帧 → 一个物理 Slot 的元数据 Raft 组在**所有副本上**永久失效且无法通过重启恢复，
  哈希到该 Slot 的全部用户 / 频道 / 订阅者 / 会话状态不可写、不可权威读。逐个 Slot 重复即全集群元数据瘫痪。
  路径 B：未认证的集群元数据任意写 —— 完整性彻底失守。

- **建议**：handler 必须在 propose 之前 **解码 `req.Command` 并用白名单校验命令类型**
  （只允许 FLOW.md §7 中 30-41 的迁移命令），并且**无条件**校验
  `hashSlot ∈ HashSlotsOf(slotID)`（复用已有的 `pluginBindingHashSlotOwnedBySlot` 形式），
  去掉 `ChannelID == ""` 的旁路；同时在 `pkg/cluster` 的 propose 入口对 hashSlot 归属做前置校验，
  让非法命令在进入 Raft 日志**之前**被拒绝，而不是在 apply 时砖化 Slot。

### [P0] 6. 四个 RPC handler 直接采信线上 `req.HashSlot`，从不校验其归属 → 写路径同样可永久砖化 Slot，读路径可跨分区寻址

- **位置**（同一模式的四处）：
  - `pkg/slot/proxy/user_conversation_state_rpc.go:340-343`（含 `upsert`/`touch`/`clear`/`hide` 四个**写**op）
  - `pkg/slot/proxy/cmd_conversation_state_rpc.go:192-195`（含 `upsert`/`advance_read` 两个**写**op）
  - `pkg/slot/proxy/subscriber_rpc.go:155-158`（读）
  - `pkg/slot/proxy/channel_rpc.go:116-119`（读）
- **类别**：安全 / 正确性 / 数据丢失
- **代码**：
  ```go
  // user_conversation_state_rpc.go:340
  hashSlot := req.HashSlot
  if hashSlot == 0 {
  	hashSlot = hashSlotForKey(s.cluster, req.UID)
  }
  switch req.Op {
  ...
  case userConversationStateRPCUpsert:
  	cmd := metafsm.EncodeUpsertUserConversationStatesCommand(req.States)
  	if err := proposeWithHashSlot(ctx, s.cluster, slotID, hashSlot, cmd); err != nil {
  ```
  `req.HashSlot` 由 `decodeUserConversationStateRPCRequest`（`user_conversation_state_codec.go:69-77`）
  从线上 uvarint 读出，**只校验了 `<= 0xFFFF`，没有校验它是否属于 `slotID`，
  也没有校验它是否等于 `hashSlotForKey(req.UID)`**。
  对照：同包 `plugin_binding_rpc.go:310` / `:330` 对 hash-slot 寻址的 op 做了
  `if !pluginBindingHashSlotOwnedBySlot(s.cluster, slotID, hashSlot) { return nil, metadb.ErrInvalidArgument }`。

- **触发路径 A —— 永久砖化（与发现 5 同机理，但入口是消息同步热路径服务）**：
  1. 攻击者（无凭证，`pkg/transport` 无鉴权无 TLS）向节点发 service ID `11`
     （`userConversationStateRPCServiceID`，`store.go:39` 注册），op = `upsert`，
     `SlotID` = 该节点 leader 的某 Slot，`HashSlot = 0xFFFF`（该 Slot 不拥有），
     `States` = 一条任意合法状态。
  2. `handleAuthoritativeRPC` 只验 leader 身份 → 通过。
  3. `hashSlot = 0xFFFF`（非 0，所以不走重算分支），
     `proposeWithHashSlot(slotID, 0xFFFF, cmd)` → Raft **提交**。
  4. `pkg/slot/fsm/statemachine.go:189-192` apply 时 `resolveHashSlot` 失败 → 整个 Slot fail，
     且该 entry 已复制到所有副本、重启后 replay 必然重蹈 → **永久不可恢复**。
  5. service ID `49`（cmd conversation state）的 `upsert` / `advance_read` 完全同理。

- **触发路径 B —— 写入错分区导致静默数据丢失（不需要恶意，版本偏斜即可）**：
  1. 攻击者（或一个 hash slot 表版本落后的合法对端）发 `upsert`，
     `HashSlot` = 该 Slot **确实拥有**但 ≠ `hashSlotForKey(uid)` 的另一个 hash slot。
  2. FSM 归属校验通过（该 hash slot 归本 Slot），命令被正常 apply，数据落到**错误的分区**。
  3. 后续任何读取都走 `hashSlotForKey(uid)`（`store.go` 全部 Get 路径），
     永远看不到这条记录 → **写入成功、读取为空，无任何错误**。

- **触发路径 C —— 读路径跨分区寻址**：
  `subscriber_rpc.go:155` / `channel_rpc.go:116` 同样采信线上 `HashSlot`，
  未认证调用方可以用任意 hash slot 值遍历本节点上其它分区的订阅者 / 频道元数据。

- **后果**：A 与发现 5 是同一量级的未认证永久砖化，但入口多了 2 个服务 ID（11、49），
  且这两个是会话同步的**主用写路径**，暴露面更大。
  B 是纯静默数据丢失：会话已读位点 / 删除屏障写到无人读的分区，用户表现为"已读不生效、删除不生效"。
  C 是未认证的元数据枚举。

- **建议**：所有以 `hashSlot` 寻址的 handler 在使用前**无条件**校验
  `hashSlot ∈ HashSlotsOf(slotID)`；对有业务 key 的 op 再强制
  `hashSlot == hashSlotForKey(key)`，不一致直接返回 `stale_meta`
  （该状态码已存在于 `authoritative_rpc.go:19`，正是为 hash slot 表版本偏斜准备的）。

### [P2] 7. `hashSlot == 0` 被当作"未指定"哨兵，但 0 是合法 hash slot → 表版本偏斜时行为不一致

- **位置**：`pkg/slot/proxy/user_conversation_state_rpc.go:341`、
  `cmd_conversation_state_rpc.go:193`、`subscriber_rpc.go:156`、`channel_rpc.go:117`
- **类别**：正确性 / 协议设计
- **代码**：
  ```go
  hashSlot := req.HashSlot
  if hashSlot == 0 {
  	hashSlot = hashSlotForKey(s.cluster, req.UID)
  }
  ```
- **触发路径**：
  1. `HashSlotForKey` 是 `CRC32(key) % HashSlotCount`（`pkg/slot/FLOW.md` §5.1），
     返回值域是 `[0, HashSlotCount)`，**0 是完全合法的 hash slot**，
     约 `1/HashSlotCount` 的 key 会落在它上面。
  2. 调用方 UID 哈希到 0 → 请求编码 `HashSlot: 0`。
  3. responder 把 0 当作"未指定"，改用**自己本地的** hash slot 表重算。
  4. 集群正在 resharding、两端 `HashSlotTableVersion()` 不一致时，
     responder 算出的 hash slot 可能 ≠ 调用方意图的分区。
  5. 而对所有非 0 的 hash slot，调用方的值会被原样采用。
  → 同一份协议对"哈希到 0 的 key"和"其它 key"给出**两种不同的一致性语义**。

- **后果**：迁移窗口内，约 1/HashSlotCount 的用户的会话状态读写可能落到错误分区
  （写入丢失 / 读取为空），而其余用户不受影响 —— 这类"只有少数用户出问题"的缺陷极难定位。
  `Store` 已经暴露了 `HashSlotTableVersion()`（`store.go:56`）但 RPC 请求里**没有携带它**，
  所以两端无法发现版本偏斜。

- **建议**：请求里用独立的 `HasHashSlot` 标记位（或指针）区分"未指定"与"hash slot 0"，
  并把 `HashSlotTableVersion` 带进请求，responder 版本不一致时回 `stale_meta` 让调用方刷新路由表。

### [P1] 8. "权威读"没有 ReadIndex，也没有 leader lease；而 `CheckQuorum` 全仓未启用 → 被分区的旧 leader **无限期**返回陈旧数据，且调用方毫不知情

- **位置**：`pkg/slot/proxy/authoritative_rpc.go:33-38`（本地判定）
  与 `authoritative_rpc.go:119-137`（responder 判定）；
  全包 **35 处** `shouldServeSlotLocally` 调用点均依赖它
- **类别**：分布式一致性
- **代码**：
  ```go
  // authoritative_rpc.go:33
  func (s *Store) shouldServeSlotLocally(slotID multiraft.SlotID) bool {
  	if s.cluster == nil || s.singleLocalPeerSlot(slotID) {
  		return true
  	}
  	leaderID, err := s.cluster.LeaderOf(slotID)
  	return err == nil && s.cluster.IsLocal(leaderID)
  }
  ```
  ```go
  // authoritative_rpc.go:119  —— responder 侧同样只验身份
  func (s *Store) handleAuthoritativeRPC(slotID multiraft.SlotID, encode rpcStatusEncoder) ([]byte, bool, error) {
  	leaderID, err := s.cluster.LeaderOf(slotID)
  	switch {
  	...
  	case !s.cluster.IsLocal(leaderID):
  		body, encodeErr := encode(rpcStatusNotLeader, uint64(leaderID))
  		return body, true, encodeErr
  	default:
  		return nil, false, nil          // ← 直接落到本地 Pebble 读，无任何读屏障
  ```
  `LeaderOf` 只是读本节点 raft 的自我认知，没有 quorum 确认：
  ```go
  // pkg/cluster/router.go:56
  status, err := r.runtime.Status(slotID)
  ...
  return status.LeaderID, nil
  ```
  **关键放大因素**：`pkg/cluster/cluster.go:283-287` 构造 `multiraft.RaftOptions` 时
  **只设了 `ElectionTick` / `HeartbeatTick` / `LogCompaction`**，
  `CheckQuorum` 和 `PreVote` 两个字段被省略 → 均为 `false`
  （全仓 grep `CheckQuorum|PreVote` 在 `pkg/cluster/`、`internal/`、`cmd/` 下**零命中**）。
  etcd/raft 在 `CheckQuorum=false` 时，**leader 失去多数派联系也不会自行 step down**。

- **触发路径**（逐步）：
  1. 某物理 Slot 的 raft 组为 N1(leader)、N2、N3。
  2. N1 与 N2/N3 之间发生网络分区（或 N1 侧网卡/链路故障）。
  3. N2、N3 在更高 term 选出 N3 为新 leader，业务继续在 N3 上提交写入，
     例如 `UpsertChannelRuntimeMeta` 把某 channel 的 `Leader` 改为 N5、`ChannelEpoch` 推到 42。
  4. N1 因为 `CheckQuorum=false` **不会降级**，`runtime.Status(slot).LeaderID` 始终等于 N1。
  5. 打在 N1 上的业务请求调用 `Store.GetChannelRuntimeMeta`（`store.go:161`）
     → `getChannelRuntimeMetaAuthoritative`（`runtime_meta_rpc.go:53-56`）
     → `shouldServeSlotLocally` 返回 **true** → 直接读 N1 本地 Pebble。
  6. 返回分区**之前**的旧 meta（Leader=旧节点、ChannelEpoch=41），
     `error == nil`，调用方无法区分它与真正的权威读。
  7. 该状态**持续整个分区期间**，不存在自动收敛的上界。

- **后果**：`pkg/slot/FLOW.md` §5.2 把这条路径明确标注为
  "权威读取（proxy/store.go:138 GetUser，**需线性一致性**）"，
  但实现上既无 ReadIndex 也无 lease，**该保证不成立**。
  由于 `ChannelRuntimeMeta` 正是承载 channel leader / epoch / write-fence 的记录，
  一次陈旧权威读会让 N1 侧的 `internal/runtime/channelmeta` 把写路由到**已被替换的旧 channel leader**
  → 同一 channel 出现两个自认 leader 的节点，即 channel 级脑裂 ——
  而 epoch/fence 机制本来正是为了防止这件事。
  缓解（据实说明）：写路径的 epoch 单调守卫会拒绝陈旧 epoch 的写入，
  所以持久状态不会被旧 leader 改坏；损害集中在**路由与读判定**上
  （读到已删除的用户/频道、权限判定用旧 `Ban`/`Disband` 标志、迁移决策基于过期 meta）。
  `GetChannelForPermission`（`store.go:98-105`）走同一条路径，
  意味着**鉴权判定**也可能基于分区前的旧 `Ban` / `SendBan` / `AllowStranger` 标志。

- **建议**：权威读改为在 leader 上走 raft ReadIndex（或至少 leader lease + `CheckQuorum=true`）
  再读本地状态机；在补上读屏障之前，至少把 `CheckQuorum` 和 `PreVote` 置为 `true`，
  把陈旧窗口从"无限期"收敛到一个选举超时。

### [P2] 9. 四个 Slot 权威分页的 k-way merge 每取一行就重建一次 Pebble 迭代器（`limit` 恒为 1）

- **位置**：`pkg/slot/proxy/identity_rpc.go:219`、`channel_rpc.go:192`、
  `runtime_meta_rpc.go:346`、`plugin_binding_rpc.go:414`
- **类别**：性能
- **代码**：
  ```go
  // identity_rpc.go:217
  func (s *Store) loadUserMergeItem(ctx context.Context, hashSlot uint16, after metadb.UserCursor) (userMergeItem, bool, error) {
  	users, cursor, done, err := s.db.ForHashSlot(hashSlot).ListUsersPage(ctx, after, 1)
  ```
  ```go
  // channel_rpc.go:191
  func (s *Store) loadChannelMergeItem(ctx context.Context, hashSlot uint16, after metadb.ChannelCursor) (channelMergeItem, bool, error) {
  	channels, cursor, done, err := s.db.ForHashSlot(hashSlot).ListChannelsPage(ctx, after, 1)
  ```
  四处形状完全一致：merge 堆每弹出一个元素，就对该 shard 再发一次 **limit=1** 的分页查询。
  而 `ListChannelRuntimeMetaPage`（`pkg/db/meta/table_runtime_meta.go:218-244`）内部是
  `ScanPrimary(ctx, s, after, limit+1)` —— 每次调用都新建迭代器、seek、读 2 行、关闭。

- **触发路径**：
  1. `internal/usecase/management/channel_runtime_meta.go:144` 或
     `internal/usecase/management/node_scalein_channel.go:73` 请求一页
     （`scaleInChannelScanPageLimit` / `req.Limit`，量级数百）。
  2. `scanChannelRuntimeMetaSlotPageLocal` 先为该物理 Slot 的 H 个 hash slot
     各建一次迭代器做初始化（H 次）。
  3. 随后每产出一行就再建一次迭代器补位（limit 次）。
  4. 一页 500 行、H=4 → **504 次**迭代器 create+seek+close，
     而理想实现只需 4 个迭代器全程复用。

- **后果**：Pebble 迭代器创建不是廉价操作（需获取 read state、构建跨 L0 与各层的 merging iterator）。
  节点缩容扫描（`node_scalein_channel.go`）会翻很多页，这个常数放大直接作用在缩容时长上；
  管理端频道清单同理。属于纯浪费，无功能影响。

- **建议**：每个 shard 预取一批（如 `limit` 行）缓存在 merge item 里，堆内消费完再补一批，
  把每页的迭代器次数从 `H + limit` 降到 `H × ceil(limit/batch)`。

### [P2] 10. `SnapshotChannelSubscribers` 的 RPC 响应完全无界，与 transport 的 64MB 帧上限直接冲突 —— 大频道的投递会永久失败

- **位置**：`pkg/slot/proxy/subscriber_rpc.go:61-78`（调用侧）
  与 `subscriber_rpc.go:177-187`（handler）
- **类别**：健壮性 / 资源
- **代码**：
  ```go
  // subscriber_rpc.go:177
  if req.Snapshot {
  	uids, err := s.db.ForHashSlot(hashSlot).ListSubscribersSnapshot(ctx, req.ChannelID, req.ChannelType)
  	if err != nil {
  		return nil, err
  	}
  	return encodeSubscriberRPCResponse(subscriberRPCResponse{
  		Status: rpcStatusOK,
  		UIDs:   uids,
  		Done:   true,
  	})
  }
  ```
  底层是一个**不设上限**的全量循环：
  ```go
  // pkg/db/meta/table_subscriber.go:83
  func (s *Shard) SnapshotSubscribers(ctx context.Context, channelID string, channelType int64) ([]string, error) {
  	var out []string
  	cursor := ""
  	for {
  		page, next, done, err := s.ListSubscribersPage(ctx, channelID, channelType, cursor, 256)
  		...
  		out = append(out, page...)
  		if done { return out, nil }
  ```
  而 transport 硬上限是 64MB：
  ```go
  // pkg/transport/errors.go:15-19
  MaxMessageSize = 64 << 20 // 64 MB
  MaxFrameSize = MaxMessageSize
  // pkg/transport/frame.go:66-69
  if bodyLen > MaxFrameSize {
  	return 0, nil, nil, fmt.Errorf("%w: %d bytes", ErrMsgTooLarge, bodyLen)
  }
  ```

- **触发路径**：
  1. 某广播频道订阅者持续增长（`AddChannelSubscribers` 每条命令上限 1000 UID，
     但**频道总订阅数无上限**，多次调用即可累积到百万级）。
  2. 该频道所属 Slot 的 leader 在**远端**节点。
  3. 投递热路径 `internal/usecase/delivery/subscriber.go:289` 调用
     `SnapshotChannelSubscribers` → 走 RPC 分支。
  4. responder 把全部 UID 编码进单个响应体。按 UID 平均 20 字节（+1 字节长度前缀）估算，
     约 **300 万订阅者**时响应体越过 64MB。
  5. 帧被 `ErrMsgTooLarge` 拒绝 → 该频道的订阅者快照**每次都失败**，且没有分页退路。

- **后果**：超过阈值后，该频道的消息投递持续失败，且错误是底层的 "message too large"，
  与"订阅者太多"毫无语义关联，排查成本极高。这是一个**随数据量增长自动触发**的定时炸弹。
  即使未越限，responder 每次调用也要在内存里堆起完整的 `[]string`
  （百万级 UID ≈ 数十 MB）并再复制一份进编码缓冲区，并发调用会叠加。
  同一文件里**已经存在**分页原语 `ListSubscribersPage`（`subscriber_rpc.go:41-58`），
  快照 op 却绕开了它。

- **建议**：删除无界 snapshot op，让调用方统一走 `ListChannelSubscribers` 分页；
  或在 responder 侧对快照结果设硬上限，超限返回明确的"请改用分页"错误。

### [P2] 11. 会话状态批量写按 hash slot 拆成多次独立提案，失败时留下**不确定的**部分写

- **位置**：`pkg/slot/proxy/user_conversation_state_rpc.go:69-96`
  （`Touch` / `Hide` / `Clear` 同形状）
- **类别**：正确性 / 错误处理
- **代码**：
  ```go
  // user_conversation_state_rpc.go:75
  for slotID, groups := range grouped {
  	for hashSlot, groupStates := range groups {
  		if s.shouldServeSlotLocally(slotID) {
  			cmd := metafsm.EncodeUpsertUserConversationStatesCommand(groupStates)
  			if err := proposeWithHashSlot(ctx, s.cluster, slotID, hashSlot, cmd); err != nil {
  				return err                       // ← 前面已提交的组不回滚
  			}
  			continue
  		}
  		if _, err := s.callUserConversationStateRPC(ctx, slotID, userConversationStateRPCRequest{...}); err != nil {
  			return err
  		}
  	}
  }
  ```
- **触发路径**：
  1. 客户端一次 syncack 携带跨多个频道的已读位点，
     `groupUserConversationStatesBySlotAndHashSlot` 把它们拆成 K 个 (slot, hashSlot) 组。
  2. 前 j 组提案成功并已 apply，第 j+1 组遇到 leader 切换 / ctx 超时 → `return err`。
  3. **`for ... range` 遍历的是 map**，组的处理顺序每次运行都不同 →
     哪些组被提交是**不确定的**。
  4. 调用方只拿到一个 error，无法得知已应用的范围。若调用方不重试
     （如 HTTP 请求上下文已取消），部分已读位点永久停留在旧值。

- **后果**：用户表现为"标记已读后，部分会话仍显示未读"，且重现不稳定（顺序随机）。
  缓解（据实说明）：这些命令本身是单调/幂等的（upsert 取较大值、
  `HideUserConversations` 只在 `DeletedToSeq` 前进时生效），所以**只要调用方重试就会收敛**；
  真正的风险在于不重试的路径。
  附带问题：`shouldServeSlotLocally(slotID)` 被放在**内层** hashSlot 循环里，
  每个 hash slot 都要重新查一次 raft status，与 slotID 无关地重复求值。

- **建议**：把同一物理 Slot 的多个 hash slot 组合并成一次提案（FSM 的 `ApplyBatch`
  本来就保证同批原子性），并把 `shouldServeSlotLocally` 提到外层循环；
  无法合并时，至少把"已成功应用的组"随 error 一并返回，让调用方能做精确重试。

### [P3] 12. `runtimeMetaAppendMetaPtr` / `runtimeMetaAppendMetas` 是纯死代码（U1000）—— 批量编码能力**并未**丢失

- **位置**：`pkg/slot/proxy/runtime_meta_codec.go:335-340`、`:362-367`
- **类别**：死代码
- **代码**：
  ```go
  // runtime_meta_codec.go:362
  func runtimeMetaAppendMetas(dst []byte, metas []metadb.ChannelRuntimeMeta) []byte {
  	return runtimeMetaAppendMetasWithOptions(dst, metas, runtimeMetaEncodeOptions{
  		includeWriteFence:      true,
  		includeRouteGeneration: true,
  	})
  }
  ```
- **核实结论**：这两个函数只是 `...WithOptions` 变体的"全部选项为 true"薄包装。
  真正的批量编码器 `runtimeMetaAppendMetasWithOptions`（`:369-375`）**仍在生产路径上**，
  被 `encodeRuntimeMetaRPCResponseForVersion`（`:111-114`）调用。
  引入 v2/v3 编解码版本（`includeWriteFence` / `includeRouteGeneration`）后，
  无选项版本失去了调用方但没被删除。
  **批量 runtime-meta 写入能力没有退化成逐条发送** —— 该假设经核实不成立。
- **后果**：仅为维护噪声；两个残留函数把 `includeWriteFence/RouteGeneration` 硬编码为 true，
  若将来有人误用会绕过版本协商，向 v1 老节点发出它无法解码的字段。
- **建议**：删除。

### [P3] 13. 插件绑定的"对端能力探测"靠**匹配错误字符串**实现，回退路径是无声的 O(N) 单行 RPC 退化

- **位置**：`pkg/slot/proxy/plugin_binding_rpc.go:34`（ST1012 命名）、
  `:660-666`（字符串匹配探测）、`:429-457`（退化回退）
- **类别**：健壮性 / 架构
- **代码**：
  ```go
  // plugin_binding_rpc.go:34
  var pluginBindingErrGetInHashSlotUnsupported = fmt.Errorf("metastore: plugin binding get_in_hash_slot unsupported")

  // plugin_binding_rpc.go:660
  func pluginBindingGetInHashSlotUnsupported(err error) bool {
  	if err == nil { return false }
  	msg := err.Error()
  	return strings.Contains(msg, "unknown plugin binding rpc op id") || strings.Contains(msg, "unknown plugin binding rpc op ")
  }
  ```
  对端返回的错误文本来自 `plugin_binding_rpc.go:344`
  的 `fmt.Errorf("metastore: unknown plugin binding rpc op %q", req.Op)`。
  同样的字符串匹配模式也出现在 `runtime_meta_rpc.go:378`
  （`strings.Contains(err.Error(), "invalid runtime meta request codec")`）。

- **触发路径 A（退化无声）**：
  1. 滚动升级期间，目标 Slot 的 leader 还是不认识 `get_in_hash_slot` 的旧版本节点。
  2. `loadPluginBindingMergeItem`（`:398`）捕获该哨兵错误，转入
     `loadPluginBindingMergeItemCompat`（`:429`）。
  3. 该兼容路径从 shard 起点开始，**每次 RPC 只取 1 行**，循环直到找到游标之后的第一行：
     ```go
     for {
     	bindings, cursor, done, err := s.scanPluginBindingsSlotHashSlot(ctx, slotID, hashSlot, pluginNo, shardAfter, 1)
     	...
     	if pluginBindingItemAfterCursor(item, after) { return item, true, nil }
     	...
     	shardAfter = cursor
     }
     ```
  4. 翻到第 P 页时，每个 shard 都要重放大约 P 行 → 单页成本 O(P × HashSlotCount) 次跨节点 RPC，
     整体翻页是 **O(总行数²)**。
  5. 全程**没有任何日志、指标或告警**标明已进入退化模式。

- **触发路径 B（探测失效）**：
  任何人修改 `handlePluginBindingRPC` default 分支的错误文案
  （哪怕只是把 `op` 改成 `op_id`），`strings.Contains` 就不再命中 →
  哨兵不再产生 → 回退不触发 → 升级期间插件绑定分页**直接报错**而不是降级。
  错误文案没有任何测试或类型约束保护它是协议的一部分。

- **后果**：A 是升级窗口内管理接口的严重变慢（无声）；
  B 是把一次无害的文案重构变成协议不兼容。
  `pluginBindingErrGetInHashSlotUnsupported` 的 ST1012 命名违规（应为 `errFoo`）
  本身微不足道，但它标记的正是这条脆弱的能力协商链路。

- **建议**：能力协商改用显式机制 —— 响应里带 op 支持位图，或增加一个独立的
  `unsupported_op` 结构化状态码（`authoritative_rpc.go:12-19` 已有状态码枚举），
  不要依赖错误字符串；同时给退化路径加一条 Warn 日志与计数指标。

### [P3] 14. `pkg/slot/FLOW.md` 与代码不一致（3 处）

- **类别**：架构与文档一致性
1. **handler 数量**：`FLOW.md` §9 写 "`proxy.New` 在构造时调用 `cluster.RPCMux().Handle(...)`
   注册 **7 个** handler"，但 `pkg/slot/proxy/store.go:32-41` 实际注册 **8 个**
   （runtimeMeta / identity / subscriber / channel / userConversationState /
   channelMigration / cmdConversationState / pluginBinding）。
   同一份 FLOW.md 的 §8 表格自己列的也是 8 个，文档内部即自相矛盾。
2. **`ListChannelRuntimeMeta` 的真实代价被低估**：`FLOW.md` §9 写
   "遍历所有 SlotID 发 RPC，N 个 Slot 就是 N 次 RPC，慎用"。
   实际上本地 leader 分支（`runtime_meta_rpc.go:77-83`）和 responder 分支（`:224-231`）
   都调用 `s.db.ListChannelRuntimeMeta(ctx)`，而后者是
   ```go
   // pkg/db/meta/compat.go:607
   iter, err := db.meta.engine.NewIter(engine.Span{}, engine.IterOptions{})
   ```
   —— **对整个元数据 Pebble 库做无界全表扫描**（扫过用户、频道、订阅者、会话状态所有键空间），
   再用 `filterChannelRuntimeMetaBySlot`（`runtime_meta_rpc.go:284-293`）丢掉 (N-1)/N 的结果。
   所以真实代价是 **N 次全库扫描**，不是 N 次廉价 RPC。
3. **该路径实为死代码**：`Store.ListChannelRuntimeMeta` 全仓唯一调用方是
   `internal/runtime/channelmeta/resolver.go:576` 的 `Sync.syncOnce`，
   而 `syncOnce` 全仓只被 `resolver_test.go:1073/1107` 调用 ——
   `Sync.Start()`（`resolver.go:114`）的注释明确写着
   "launches lightweight resolver watchers **without scanning all authoritative metadata**"，
   启动的是 `watchActiveSlotLeaders`。
   即 `runtimeMetaRPCList` op + `Store.ListChannelRuntimeMeta` + `filterChannelRuntimeMetaBySlot`
   这一整条链路**当前不在生产运行路径上**。
- **建议**：更正 §9 的 handler 计数；把 `ListChannelRuntimeMeta` 的告警改写为
  "N 次全库扫描"并标注当前无生产调用方，或直接删除该死链路。

### [P3] 15. 迁移任务枚举字段先截断成 uint8 再做白名单校验 → 同一语义存在多种线上编码

- **位置**：`pkg/slot/proxy/channel_migration_codec.go:277-302`（gosec G115 报在 `:282/:291/:300`）
- **类别**：健壮性 / 编解码
- **代码**：
  ```go
  // channel_migration_codec.go:277
  kind, next, err := runtimeMetaReadUvarint(body, offset)
  if err != nil { ... }
  task.Kind = metadb.ChannelMigrationKind(kind)          // ← uint64 截断成 uint8
  if !isValidChannelMigrationKindForRPC(task.Kind) {     // ← 校验的是截断后的值
  	return metadb.ChannelMigrationTask{}, offset, fmt.Errorf("metastore: invalid channel migration kind %d", kind)
  }
  ```
- **触发路径**：线上写入 `kind = 257`（uvarint 两字节）→ `uint8(257) == 1` → 白名单校验通过
  → 被当作 `kind = 1` 接受。`Status`（`:287`）与 `Phase`（`:296`）三处完全同形。
- **后果**：同一个任务存在无穷多种等价线上编码（`k`、`k+256`、`k+512`…）。
  目前无法据此越权（效果等价于直接发 `kind=1`），所以只是健壮性问题；
  但它与项目自身的编码原则冲突 —— `pkg/slot/FLOW.md` §9 明确要求
  "迁移命令只接受单个 JSON payload TLV，重复 payload、重复 JSON key 或未知 JSON 字段
  视为 `ErrCorruptValue`，**避免歧义编码**"。这里恰恰制造了歧义编码。
  另外错误信息打印的是未截断的 `kind`，与实际被校验的值不一致，会误导排障。
- **建议**：先对 uvarint 原值做上界检查（`if kind > math.MaxUint8`）再转换，
  让非规范编码直接失败。

### [P2] 16. `listUserConversationActiveLocal` 吞掉覆盖层读取错误并返回"看起来完整"的陈旧列表

- **位置**：`pkg/slot/proxy/user_conversation_state_rpc.go:437-440`
- **类别**：错误处理 / 正确性
- **代码**：
  ```go
  // user_conversation_state_rpc.go:437
  hints, err := s.userConversationActiveOverlay.ListHotUserConversationActive(ctx, uid, userConversationActiveOverlayAllHintsLimit)
  if err != nil {
  	return persisted, nil        // ← error 被丢弃，返回"只有持久化部分"的结果且 err == nil
  }
  ```
  对比**同一个函数内**几行之后对另一个错误源的处理 —— 那里是正确传播的：
  ```go
  // user_conversation_state_rpc.go:456-466
  state, err = s.db.ForHashSlot(hashSlot).GetUserConversationState(ctx, uid, hint.ChannelID, hint.ChannelType)
  switch {
  case err == nil:
  case errors.Is(err, metadb.ErrNotFound):
  	state = metadb.UserConversationState{...}
  default:
  	return nil, err              // ← 这里就正确返回了
  }
  ```
  同一模块里"宽松版"和"严格版"并存，而**宽松版用在主路径上**。

- **触发路径**：
  1. `pkg/slot/FLOW.md` §9 说明覆盖层的语义："`Store.ListUserConversationActive`
     在 UID 所属 Slot leader 合并持久化 active index 与 `UserConversationActiveOverlay`
     中的 UID-local 热提示" —— 覆盖层里装的**恰恰是尚未落盘的最新活跃会话**。
  2. 覆盖层读取失败。可能原因：节点优雅停机过程中覆盖层存储已关闭；
     或**请求 ctx 被取消/超时**恰好落在这次覆盖层读取上（`ctx` 是透传的）。
  3. `return persisted, nil` —— 调用方拿到一个 `err == nil` 的结果。
  4. 该结果**缺失的正是最近活跃的那批会话**（还没折叠进持久化 active index 的部分）。

- **后果**：用户拉取会话列表时，**正在聊的那几个会话不出现在列表里**，
  而接口返回成功、无任何错误或日志。这是 IM 最核心的界面之一，
  且故障表现（"会话列表少了最新的几条"）与根因（覆盖层读取失败）毫无关联，排查极难。
  ctx 取消的情形更糟：一个已被取消的请求会拿到一份"成功"的不完整答案。
  注意这**不是**有意的 best-effort 降级 —— 若是，至少应当有日志或指标；
  代码里既无日志也无计数器，`err` 被完全丢弃。

- **建议**：要么向上传播该 error，要么明确定义为 best-effort 降级
  并补上 Warn 日志 + 计数指标，同时在响应里带一个"结果可能不完整"的标记，
  让调用方能够区分。至少不能对 `ctx.Err()` 这类取消错误也静默吞掉。

## 已排除的候选项

**类别 (a) 并发 —— 经核实本分片无并发面，整类跳过**
- 对 17 个生产文件执行
  `grep -rn "go func|sync.Mutex|sync.RWMutex|chan |time.NewTicker|time.NewTimer|sync.Once"`
  → **零命中**。本包是纯 RPC/codec/store 转发逻辑，不启动 goroutine、不持锁、
  不建 channel、不建定时器。所有并发语义都在 `pkg/slot/multiraft` 与 `pkg/cluster`（其它分片）。
  按分片说明跳过 (a)，时间投入到 (c)(f)(g)(h)。

**类别 (b) context —— 干净**
- `grep "context.Background()|context.TODO()"` 在本包非测试文件下 **零命中**。
  所有路径都把上游 `ctx` 一路透传到 `cluster.RPCService` 和 `db.ForHashSlot(...)`，
  未发现丢失取消/超时的地方。无发现。

**类别 (d)(e) 资源 / 无界增长 —— 无发现**
- 本包不直接创建 Pebble 迭代器、快照、batch、文件句柄或网络连接（全部委托给
  `pkg/db/meta` 与 `pkg/cluster`），没有 `Close()`/`Stop()` 生命周期。
- `Store` 只有 3 个字段（`cluster` / `db` / `userConversationActiveOverlay`），
  **不持有任何 map 或缓存**，没有按 channel/user/连接维度增长的状态。不存在泄漏面。

**gosec G115 逐条核实（6 条，全部为误报或已降级为 P3）**
- `channel_migration_codec.go:179` —— `end := offset + int(length)`，`length` 是 uint64。
  **安全**：紧跟 `if end < offset || end > len(body)`。`length ∈ [2^63, 2^64)` 时
  `int(length)` 为负 → `end < offset` 命中；`length ∈ [maxInt/2, 2^63)` 时
  `end` 巨大 → `end > len(body)` 命中。两个边界合起来完整覆盖。**误报。**
- `runtime_meta_codec.go:609`（`maxInt := uint64(^uint(0) >> 1)`）—— 这是**正确**的
  int 上界计算习语本身，不是转换缺陷。**误报。**
- `runtime_meta_codec.go:621`（`count > uint64(remaining)`）—— `remaining` 为负时
  `uint64(remaining)` 会变成巨值从而放空守卫，但全部 15 个调用点传入的都是
  `len(body)-offset`，而 `runtimeMetaReadUvarint`（`:570`）/`runtimeMetaReadString`（`:602`）
  在推进 `offset` 前都做了边界检查，`offset` 永远 ≤ `len(body)` → `remaining ≥ 0` 恒成立。
  **当前安全**（但这个守卫的真正缺陷是除数，已作为发现 4 报出）。
- `channel_migration_codec.go:282 / 291 / 300` —— uint64→uint8 截断。
  不构成越权（截断值仍要过白名单），但制造了歧义编码，**降级为 P3 报出（发现 15）**。

**跨分片 P0 模式（`uint64` 线上长度转 `int` 后用于分配/切片）—— 本包逐点核对结果**
- 协调者通报的七个缺陷站点的模式是"缺少 `count > maxInt` 检查"或"缺少 `end < offset` 溢出检查"。
  本包的 **全部 15 处** `make([]T, n)` 都经由唯一的 `runtimeMetaCollectionLen`
  （`runtime_meta_codec.go:616`），该函数**确实**同时做了 `count > maxInt` 与
  `count > uint64(remaining)` 两个检查 → **本包不存在 `int()` 截断成负数的那个缺陷**。
  逐点确认安全（保护它们的是 `runtimeMetaCollectionLen` 的 maxInt 检查）：
  `runtime_meta_codec.go:280/387/519`、`identity_subscriber_codec.go:246/399`、
  `plugin_binding_codec.go:237`、`cmd_conversation_state_codec.go:204/273`、
  `user_conversation_state_codec.go:294/363/406/449/491`。
  **但这 15 处同时都受发现 4（除数错误）影响** —— 守卫存在、量级错误。
- 两个长度前缀读取器 `runtimeMetaReadString`（`:602-614`）与
  `channelMigrationReadBytes`（`:173-186`）**实现正确**，形式与仓库内的正确参考
  `internal/access/node/delivery_push_codec.go:283` 一致（溢出检查 + 剩余字节检查）。

**已考虑但无法写出触发路径，故删除的候选项**
- `authoritative_rpc.go:73-114` 的 peer 轮询重试用在 **propose**（写）上
  （`callChannelMigrationProposalRPC` → `channelMigrationRPCPropose`）：
  怀疑"RPC 超时但提案实际已提交，重试到另一 peer 造成重复 apply"。
  排除理由：迁移命令在 FSM 侧全部由 epoch / fence / task 阶段守卫
  （`pkg/slot/FLOW.md` §9 "Channel 迁移 fence/task 一致性"），重复 apply 会被判为
  `stale_meta` 而非重复生效；且 `ProposeWithHashSlotResult`（`pkg/cluster/cluster.go:988-993`）
  自身的 `Retry` 只对 `ErrNotLeader` 重试。构造不出导致错误结果的具体序列 → 删除。
- `resolvePluginBindingRPCRoute`（`plugin_binding_rpc.go:346-365`）在
  `LeaderOf` 出错时吞掉原始 error 只回 `no_leader`：
  排除理由：调用方 `callAuthoritativeRPCWithStatuses` 会把它记为
  `lastErr = raftcluster.ErrNoLeader` 并继续轮询，不会被当成成功。属于信息损失，非缺陷。
- `heap.Pop(...).(userMergeItem)` 等 8 处不带 `ok` 的类型断言
  （`identity_rpc.go:200/259` 等）：排除理由：每个堆只有一种具体元素类型被 `Push`，
  类型在本地可证，远程输入无法影响。
- `getChannelForPermissionAuthoritative`（`channel_rpc.go:56`）自身不做
  `shouldServeSlotLocally` 判断：排除理由：唯一调用方 `store.go:98-105` 已在外层做了判断。
- `Store.ListChannelRuntimeMeta` 的 N×全库扫描：性能形态确实很差，
  但唯一调用方 `Sync.syncOnce` 经核实**只被测试调用**，不在生产路径上 →
  不作为性能发现，改为 P3 死代码/文档条目（发现 14）。

## 本分片整体评价

这是**在生产运行路径上**的活代码（`internal/app` → `pkg/cluster` → `pkg/slot/proxy`，
非 v2 未上线栈），是全系统读写分布式元数据的唯一入口。代码风格整体相当整洁：无并发面、
无资源生命周期、context 透传完整、编解码集中复用同一套原语、长度前缀读取器的溢出检查写得正确 ——
在协调者通报的那个跨分片 `int()` 截断 P0 模式上，本包**逐点干净**。

但它在**信任边界**上系统性失守。八个 RPC handler 都建立在"对端是可信集群节点"这个未经验证的
前提上，而 `pkg/transport` 既无鉴权也无 TLS（`server.go:176-185` 收到
`MsgTypeRPCRequest` 就直接 `go` 派发给 RPCMux，中间没有任何校验）。三个 P0 全部由此而来：
`channel_migration` 的 `propose` op 把线上字节当作 FSM 命令直接提交进 Raft（发现 5）；
四个 handler 直接采信线上 `req.HashSlot` 而从不校验归属（发现 6）；
集合长度守卫用"剩余字节数"而非"剩余字节/单元素最小编码"做上界（发现 4）。
前两者的后果是同一个且极其严重：非法命令**先被 Raft 提交、再在 apply 时失败**，
而按 FLOW.md 的 ApplyBatch 原子性约定这会 fail 掉整个 Slot —— 坏 entry 已复制到所有副本，
重启 replay 必然重蹈，**元数据 Raft 组永久砖化**。值得注意的是同包的
`plugin_binding_rpc.go:310` 已经写对了归属校验，说明这是遗漏而非设计选择。

**最该优先处理的一个问题是发现 5**：在 `channelMigrationRPCPropose` 的 handler 里加上
命令类型白名单和无条件的 hash-slot 归属校验。它是本包里唯一一处让**任意未认证字节**
直达 Raft 提案的入口，修复面很小（一个 handler 分支），但堵住的是"永久砖化元数据集群"
与"任意元数据写"两个后果。紧随其后是把 `CheckQuorum` 置为 `true`（发现 8）——
一行配置，就能把陈旧权威读的窗口从"整个分区期间"收敛到一个选举超时。

另需指出：`userMergeHeap.Less`（发现 1）与 `pluginBindingMergeHeap.Less`（发现 2）
用字典序比较线上长度前缀编码的键，而同包的 `channelRuntimeMetaLess` 和 `channelLess`
用的是正确的长度优先序 —— 四个比较器里两对两，分页因此会**静默漏数据**且无任何报错，
这类缺陷在生产中极难被发现。
