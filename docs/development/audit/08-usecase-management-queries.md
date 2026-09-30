# 审计分片报告：08-usecase-management-queries

- **SLUG**: `08-usecase-management-queries`
- **TITLE**: 管理用例：其余聚合查询（internal/usecase/management 非分布式任务/缩容/网络/迁移文件）
- **日期**: 2026-09-30
- **方法**: Phase 0（gosec/staticcheck 全库输出中本分片命中的逐一核实）→ Phase 1（30 个文件全部逐行通读，走查 a–i 九类检查项）→ Phase 2（每个候选重新打开文件读全函数上下文、`grep` 全部调用方、确认代码在 `internal/app` 组合根 → `internal/access/manager` HTTP 入口的真实存活路径、撰写可复现触发路径）
- **约束遵守**: 只读审计，未修改任何仓库文件，未 git add/commit/push。本报告是本任务唯一写入的文件。

---

## 已通读文件清单（Phase 1 覆盖证明）

| 文件 | 行数 | 状态 |
|---|---|---|
| internal/usecase/management/app.go | 469 | 全文通读 |
| internal/usecase/management/channel_cluster.go | 235 | 全文通读 |
| internal/usecase/management/channel_cluster_operations.go | 295 | 全文通读 |
| internal/usecase/management/channels_biz.go | 547 | 全文通读 |
| internal/usecase/management/connections.go | 179 | 全文通读 |
| internal/usecase/management/controller_logs.go | 81 | 全文通读 |
| internal/usecase/management/controller_raft_compaction.go | 117 | 全文通读 |
| internal/usecase/management/controller_raft_status.go | 205 | 全文通读 |
| internal/usecase/management/conversations.go | 144 | 全文通读 |
| internal/usecase/management/dashboard_metrics.go | 59 | 全文通读 |
| internal/usecase/management/diagnostics.go | 393 | 全文通读 |
| internal/usecase/management/diagnostics_tracking.go | 329 | 全文通读 |
| internal/usecase/management/messages.go | 193 | 全文通读 |
| internal/usecase/management/monitor_metrics.go | 398 | 全文通读 |
| internal/usecase/management/node_detail.go | 44 | 全文通读 |
| internal/usecase/management/node_onboarding.go | 373 | 全文通读 |
| internal/usecase/management/node_operator.go | 31 | 全文通读 |
| internal/usecase/management/nodes.go | 365 | 全文通读 |
| internal/usecase/management/overview.go | 340 | 全文通读 |
| internal/usecase/management/plugin.go | 316 | 全文通读 |
| internal/usecase/management/slot_add_remove.go | 69 | 全文通读 |
| internal/usecase/management/slot_detail.go | 104 | 全文通读 |
| internal/usecase/management/slot_logs.go | 101 | 全文通读 |
| internal/usecase/management/slot_operator.go | 48 | 全文通读 |
| internal/usecase/management/slot_raft_compaction.go | 94 | 全文通读 |
| internal/usecase/management/slot_recover_rebalance.go | 108 | 全文通读 |
| internal/usecase/management/slots.go | 266 | 全文通读 |
| internal/usecase/management/system_users.go | 110 | 全文通读 |
| internal/usecase/management/tasks.go | 156 | 全文通读 |
| internal/usecase/management/users.go | 645 | 全文通读 |

合计 30 个文件 / 6814 行，与 WORKER_BRIEF 的文件集描述一致（unit 07 的 distributed_tasks / node_scalein / channel_migration / network / channel_runtime_meta 系列文件未纳入本分片审计范围）。为核实触发路径，另跨包阅读了：`internal/app/runtime_summary.go`、`internal/app/monitor_metrics.go`、`internal/access/node/{runtime_summary_rpc.go, monitor_metrics_rpc.go}`、`internal/access/manager/{server.go, monitor_metrics.go, connections.go, messages.go, channel_cluster_operations.go, channel_runtime_meta.go, nodes.go}`、`pkg/transport/client.go`、`pkg/transport/pool.go`（超时相关行）、`pkg/cluster/cluster.go`（RPCService 委托段）、`pkg/cluster/managed_slots.go`、`pkg/cluster/slot_manager.go`（statusOnNode）、`pkg/slot/proxy/{identity_rpc.go, authoritative_rpc.go}`、`internal/FLOW.md`、`AGENTS.md`。本包无 FLOW.md；已核对 internal/FLOW.md 中 `management.App` 接口与实际代码一致。

---

## Phase 0：工具输出核实

- **staticcheck**（/tmp/octo-staticcheck.txt）：本分片 0 条命中。
- **gosec**（/tmp/octo-gosec.txt）：本分片 13 条 G115（整型溢出转换）。逐条核实结论：
  - **实报 2 条**（同根因，合并为发现 F10）：`messages.go:160` 与 `channel_cluster_operations.go:126` 的 `uint8(req.ChannelType)`/`uint8(channelType)`——`int64` 管理请求参数只有 `<= 0` 下界校验（`internal/usecase/management/messages.go:150`、`internal/access/manager/messages.go:190-197`、`internal/access/manager/channel_runtime_meta.go:180-188` 均无上界），`>255` 的输入静默截断为另一个 channel type。
  - **误报 11 条**：`users.go:200,270,387,617`、`channels_biz.go:400`、`overview.go:242`、`slot_add_remove.go:33`、`slot_recover_rebalance.go:100,101` 均为 `uint32(slotID)`/`uint32(...From/To)` 类转换，值域受物理 slot 数量（集群配置，个位/十位数量级）约束，无溢出可能；`users.go:437` 的 device level 转换值域为 2 个枚举值。均满足"有上界保证"。
- 本分片无 `_ = err` 式吞错、无未检查类型断言（与 brief 预告一致），审计精力按 brief 指引集中在聚合层面（g/b/c 类）。

---

## 发现

### F1 (P1) GetMonitorMetrics 集群视图顺序 fan-out 无每节点超时，慢节点挂起管理请求

- **位置**: internal/usecase/management/monitor_metrics.go:143-162（入口 `GetMonitorMetrics` :98-108）
- **类别**: (b) 上下文/超时 + (g) 性能
- **代码**:
```go
func (a *App) getClusterMonitorMetrics(ctx context.Context, nodes []controllermeta.ClusterNode, window, step time.Duration) (MonitorMetricsResult, error) {
	targets := monitorMetricTargets(nodes)
	results := make([]metrics.QueryResult, 0, len(targets))
	available := make(map[uint64]bool, len(targets))
	for _, node := range targets {
		qr, err := a.queryNodeMonitorMetrics(ctx, node.NodeID, window, step)
		if err != nil {
			continue
		}
		results = append(results, qr)
		available[node.NodeID] = true
	}
```
- **触发路径**: ① 管理端监控页轮询 `GET .../monitor/metrics`（cluster 视图，nodeID=0）→ `internal/access/manager/monitor_metrics.go:116` 传入 `c.Request.Context()`（无 deadline）；② `internal/app/monitor_metrics.go` 直通 → `internal/access/node/monitor_metrics_rpc.go:51` `c.cluster.RPCService(ctx,...)` → `pkg/cluster/cluster.go` → `pkg/transport/client.go:39-75` → `pool.RPC`。逐层核实：该链路除拨号超时（`pkg/transport/pool.go:297` DialTimeout）外**没有任何请求级超时**，完全依赖调用方 ctx；manager HTTP server 也未配置 ReadTimeout/WriteTimeout（`internal/access/manager/server.go:301` `&http.Server{Handler: engine}`）。③ 集群中任一节点 TCP 可达但应用层挂死（如 GC 死循环、进程 freeze 不关连接）→ 该节点 RPC 永不返回，本循环阻塞在其上，HTTP 请求与 handler goroutine 无限期挂起。④ 监控页默认自动刷新，每次刷新再泄漏一个挂起 goroutine。
- **后果**: 管理端监控接口在单节点故障场景下永久挂起并累积泄漏 goroutine；本应为运维提供故障可见性的接口恰好在最需要时不可用。
- **建议**: 对齐同包 diagnostics.go 的正确模式（`diagnostics.go:185-220`：`sem := make(chan struct{}, 8)` + `context.WithTimeout(ctx, managerDiagnosticsNodeTimeout)` 每节点 2s），为 `queryNodeMonitorMetrics` 加每节点 deadline；可顺带将顺序 fan-out 改为有界并发。

### F2 (P1) ListNodes/GetNode 对每个远程节点做顺序 RuntimeSummary RPC，同样无超时

- **位置**: internal/usecase/management/nodes.go:127（`ListNodes` 循环）、nodes.go:282（`managerNode` → `nodeRuntimeSummary`）
- **类别**: (b) 上下文/超时 + (g) 性能（N+1 per-node RPC in loop）
- **代码**:
```go
// nodes.go ListNodes
for _, clusterNode := range clusterNodes {
    nodes = append(nodes, a.managerNode(ctx, clusterNode, controllerLeaderID, slotSummary))
}
// nodes.go:282 managerNode
runtime := a.nodeRuntimeSummary(ctx, clusterNode.NodeID)
```
```go
// internal/app/runtime_summary.go:49-52（远程分支）
summary, err := r.nodeClient.RuntimeSummary(ctx, nodeID)
// internal/access/node/runtime_summary_rpc.go:48
respBody, err := c.cluster.RPCService(ctx, multiraft.NodeID(nodeID), 0, runtimeSummaryRPCServiceID, body)
```
- **触发路径**: 管理端节点列表页轮询 `GET .../nodes` → `internal/access/manager/nodes.go:155` `ListNodes(c.Request.Context())`（无 deadline）→ 对 N 个节点做 N 次顺序 RPC（每次 = 1 个远程 RuntimeSummary RPC，本地节点走内存汇总）。与 F1 同链路核实：RPC 全链路无请求级超时、HTTP server 无超时。任一远程节点挂死 → `ListNodes` 无限期挂起；N 个节点则请求延迟为 N×RTT 的串行累加。
- **后果**: 最基础的节点清单页在单节点故障时永久挂起；每次轮询泄漏一个 goroutine。与 F1 同为管理面可用性缺陷。
- **建议**: 同 F1——每节点 `context.WithTimeout`；`managerNode` 的 RuntimeSummary 可并行获取（有界并发）或由 controller 心跳数据本地近似。

### F3 (P2) listUsersByKeyword 全量扫描所有 slot 的全部用户行，无总扫描上限

- **位置**: internal/usecase/management/users.go:208-244
- **类别**: (g) 性能（full-scan aggregation / DoS-by-dashboard）
- **代码**:
```go
for i := startIndex; i < len(slotIDs); i++ {
    slotID := slotIDs[i]
    ...
    for {
        page, nextCursor, done, err := a.users.ScanUsersSlotPage(ctx, slotID, after, userFilteredScanLimit(limit))
        ...
        for _, user := range page {
            if !strings.Contains(user.UID, keyword) { continue }
            ...
            item, err := a.managerUserListItem(ctx, slotID, user.UID)
```
- **触发路径**: 管理端用户搜索框每次输入（含单个字符）发起 `GET .../users?keyword=a&limit=20` → `listUsersByKeyword` 对 `a.cluster.SlotIDs()` 的**每一个** slot 做**全部页**扫描（`userFilteredScanLimit` 只把单页上限提到 200，对总扫描行数无任何上限），每个匹配行再触发 `managerUserListItem`（1 次 routes 批查 + 4 次 GetDevice，见 F5）。核实 `pkg/slot/proxy/identity_rpc.go:89-103`：非本节点领导的 slot，`ScanUsersSlotPage` 是**跨节点 identity RPC**（顺序、无超时）。100 万用户规模下一个 `keyword=a` 请求即扫描 100 万行并产生数千次跨节点 RPC，全部串行。无每页 deadline、无总扫描预算、无 cache。
- **后果**: 大规模集群上管理端关键字搜索一次请求即构成对全集群元数据存储的全表扫描 + 串行跨节点 RPC 风暴；多个管理端用户同时搜索可拖垮 slot 身份 RPC 通道，波及生产流量。
- **建议**: 限制总扫描行数预算（如 50k 行）并在超限时返回"需更精确关键字"提示；或建立 UID 索引/倒排；每页 RPC 加 deadline。

### F4 (P2) ListBusinessChannels 同样的全 slot 全量扫描模式

- **位置**: internal/usecase/management/channels_biz.go:183-237
- **类别**: (g) 性能（full-scan aggregation）
- **代码**:
```go
for i := startIndex; i < len(slotIDs); i++ {
    ...
    for {
        page, nextCursor, done, err := a.channelBusinessReader.ScanChannelsSlotPage(ctx, slotID, after, businessChannelScanLimit(req.Limit))
        ...
        for _, ch := range page {
            if !businessChannelMatches(ch, req.TypeFilter, keyword) { continue }
```
- **触发路径**: 与 F3 完全同构：`GET .../channels?keyword=x`（`internal/access/manager/channels_biz.go:141`）→ 对所有 slot 的 channel 元数据全量扫描，远程 slot 为顺序跨节点 RPC（`pkg/slot/proxy/identity_rpc.go` 同一 proxy 层），无总扫描上限、无 deadline。频道行数在生产 IM 中远大于用户数（每个会话一条 channel 记录），扫描代价更高。
- **后果**: 同 F3，且规模更大。
- **建议**: 同 F3。

### F5 (P2) 用户列表每行 4 次 GetDevice（远程 slot 时为 4 次串行跨节点 RPC）

- **位置**: internal/usecase/management/users.go:397-414（`userDeviceCounts`，由 `managerUserListItemWithRoutes`:380 调用）
- **类别**: (g) 性能（N+1）
- **代码**:
```go
func (a *App) userDeviceCounts(ctx context.Context, uid string) (int, int, error) {
	deviceCount := 0
	tokenSetCount := 0
	for _, flag := range managerUserDeviceFlags() {
		device, ok, err := a.userDevice(ctx, uid, flag)
		...
```
- **触发路径**: 任一用户列表页（有分页的正常路径 `listUsersUnfiltered` 同样命中）→ 每行调用 `userDeviceCounts`，对 app/web/pc/system 4 个 flag 逐个 `GetDevice`。核实 `pkg/slot/proxy/identity_rpc.go:64-86`：`getDeviceAuthoritative` 对非本节点领导的 slot 是单次跨节点 RPC。`limit=200` 的一页 → 最多 800 次顺序跨节点 RPC，无每调用 deadline。
- **后果**: 用户列表页延迟随页大小线性放大为数百次串行 RPC；slot 领导慢时列表页挂起时长不可控。
- **建议**: 增加 `GetDevicesByUID(uid)` 批量端口（一次取 4 个 flag 或全设备），列表页用聚合查询替代逐行取。

### F6 (P2) GetChannelClusterSummary 无上限全量扫描全部频道运行时元数据

- **位置**: internal/usecase/management/channel_cluster.go:75-125
- **类别**: (g) 性能（full-scan aggregation，每次请求全扫、无 cache、无预算）
- **代码**:
```go
for _, slotID := range slotIDs {
    after := metadb.ChannelRuntimeMetaCursor{}
    for {
        page, nextCursor, done, err := a.channelRuntimeMeta.ScanChannelRuntimeMetaSlotPage(ctx, slotID, after, channelClusterScanPageLimit)
        ...
        for _, meta := range page {
            summary.Total++
            replicaTotal += len(meta.Replicas)
```
- **触发路径**: 管理端频道集群健康页 `GET .../channel-cluster/summary`（`internal/access/manager/channel_cluster.go:61`）→ 对所有 slot 的全部 `ChannelRuntimeMeta` 行全量扫描做计数聚合。活跃频道数十万的生产集群上，每次请求全扫一遍（远程 slot 为顺序跨节点 RPC），且这是 dashboard 汇总数字，无需逐行精确。
- **后果**: 每次页面刷新 = 全集群频道元数据全表扫描；自动刷新时形成持续全扫负载。
- **建议**: 结果按 epoch/version 缓存（短 TTL）；或由 controller 周期性预聚合；每页 RPC 加 deadline。

### F7 (P2) ListConnections 无分页全量 dump，全节点连接一次返回

- **位置**: internal/usecase/management/connections.go:60-87
- **类别**: (g) 性能（unbounded result set）
- **代码**:
```go
	slots := a.online.ActiveSlots()
	items := make([]Connection, 0)
	for _, slot := range slots {
		for _, conn := range a.online.ActiveConnectionsBySlot(slot.SlotID) {
			items = append(items, managerConnection(nodeID, conn))
		}
	}

	sort.Slice(items, func(i, j int) bool {
```
- **触发路径**: `ListConnectionsRequest`（:14-17）只有 `NodeID` 字段，无 Limit/Cursor；HTTP 层 `internal/access/manager/connections.go:132-138` 也无任何分页参数。管理端打开连接列表页 → 本地节点全量连接（大型网关节点 10 万+ 在线连接）一次性物化为 DTO 切片 + 全量排序 + 全量 JSON 序列化。远程分支 `a.connections.NodeConnections(ctx, nodeID)` 同样一次性返回远端全量（且该 RPC 无 deadline，同 F1 链路）。
- **后果**: 大节点上每次打开连接页都是内存/CPU/带宽尖峰；无界响应体可拖垮管理端浏览器与节点。
- **建议**: 加 Limit/Cursor（按 ConnectedAt/SessionID 游标）；远程 RPC 加 deadline。

### F8 (P2) CompactControllerRaftLogs 顺序 fan-out 无每节点超时（手动运维操作）

- **位置**: internal/usecase/management/controller_raft_compaction.go:45-71
- **类别**: (b) 上下文/超时
- **代码**:
```go
	peerIDs := a.controllerPeerIDList()
	for _, peerID := range peerIDs {
		result, err := a.cluster.CompactControllerRaftLogOnNode(ctx, peerID)
		if err != nil {
			...
```
- **触发路径**: 运维在 UI 点击 controller 日志压缩 → 逐个 controller peer 顺序 RPC，ctx 直通无 deadline（同 F1 已核实的链路）。与 F1/F2 区别在于这是低频手动操作，不是轮询端点；但触发场景恰是"某节点已经不健康需要压缩恢复"时，挂在那台不健康节点上最久。
- **后果**: 压缩操作界面永久转圈；运维无法判断是仍在执行还是已挂死。
- **建议**: 每节点 `context.WithTimeout`（压缩是耗时操作，可给更长如 30s）+ 失败项像 diagnostics_tracking.go 一样列入 per-node 结果。

### F9 (P2) ListSlots(NodeID 过滤) 对每个 slot 一次顺序 SlotLogStatusOnNode RPC，无超时

- **位置**: internal/usecase/management/slots.go:128-139
- **类别**: (b) 上下文/超时 + (g) N+1
- **代码**:
```go
		if opts.NodeID != 0 {
			logStatus, err := a.cluster.SlotLogStatusOnNode(ctx, opts.NodeID, slot.SlotID)
			if err != nil {
				return nil, err
			}
			slot.NodeLog = &SlotNodeLogStatus{...}
		}
```
- **触发路径**: 核实 `pkg/cluster/slot_manager.go:444-465`：目标节点非本地时 `SlotLogStatusOnNode` 是一次 RPC。管理端查看某节点的 slot 视图 → 循环对同一目标节点为**每个 slot**发一次顺序 RPC（几十个 slot = 几十次串行 RTT），单次无 deadline，任一次挂起则整个列表挂起。
- **后果**: 节点详情/slot 视图在目标节点异常时永久挂起；健康时延迟也随 slot 数线性放大。
- **建议**: 目标节点应提供批量接口（一次返回该节点全部 slot 的 log watermark）；或至少每 RPC 加 timeout。

### F10 (P3) channelType 仅校验 >0，>255 的管理请求静默截断为错误类型（gosec G115 实报）

- **位置**: internal/usecase/management/messages.go:150-160；同模式 internal/usecase/management/channel_cluster_operations.go:126（及 unit 07 的 channel_runtime_meta.go:337，此处仅作关联说明）
- **类别**: (f) 输入校验 / G115 实报
- **代码**:
```go
// messages.go:150
	if req.ChannelID == "" || req.ChannelType <= 0 || req.Limit <= 0 {
		return ListMessagesResponse{}, metadb.ErrInvalidArgument
	}
	...
		page, err := a.messages.QueryMessages(ctx, MessageQueryRequest{
			ChannelID: channel.ChannelID{
				ID:   req.ChannelID,
				Type: uint8(req.ChannelType),
			},
```
- **触发路径**: 管理端 `GET .../messages?channel_id=c1&channel_type=300`（HTTP 层 `parseMessageChannelType` 仅拒绝 `<=0`）→ `uint8(300) = 44` → 返回的是 type=44 频道的消息而非报错。`GetChannelClusterReplicaDetail`（channel_cluster_operations.go:126）同理。对比：`channels_biz.go:438-440 validateBusinessChannelKey` 已正确做了 `channelType > math.MaxUint8` 上界校验，说明仓库内已有正确范式。
- **后果**: 管理端查询结果与请求参数不符（错类型数据），且无任何报错线索；属低危正确性缺陷（管理面只读、无越权放大）。
- **建议**: 在 usecase 层复制 validateBusinessChannelKey 的上界校验（`channelType > math.MaxUint8 → ErrInvalidArgument`）。

---

## 已排除的候选项

| 候选 | 排除原因 |
|---|---|
| users.go/channels_biz.go 的 cursor KeywordHash 绑定（crc32）疑似安全问题 | 实际是**正确**的防错位设计：游标与产生它的过滤参数哈希绑定，防游标跨过滤条件复用。非缺陷。 |
| diagnostics.go / diagnostics_tracking.go 顺序 fan-out | Phase 2 核实为**正面样板**：每节点 `context.WithTimeout(ctx, managerDiagnosticsNodeTimeout)`（2s）+ 跳过/不可用状态显式入 `Nodes` 结果 + `DiagnosticsStatusPartial`/`DiagnosticsTrackingStatusPartial` 部分结果显式标注。无需修改。 |
| getClusterMonitorMetrics 逐节点 `continue` 吞错（F1 关联项） | 虽跳过失败节点，但 `available` map 传入 `monitorNodesWithAvailability`，节点可用性在响应 DTO 中可见——部分结果非静默。真正缺陷是挂起（F1），吞错本身不成立。 |
| monitorClusterNodes 失败时静默回退本地指标 | 回退后响应 `Scope.View=LocalNode` 而非 Cluster，调用方可区分，非静默降级。 |
| overview.go 聚合 | 仅 4 次 controller-leader RPC + `overviewAnomalyItemLimit=5` 封顶所有异常组，设计良好。 |
| conversations.go ListRecentConversations | `limit+1` 探测截断 + Truncated 标志，边界正确；MsgCount 上游 Sync 内部有约束。 |
| messages.go ListMessages 分页 | BeforeSeq 游标 + HasMore，正确分页（保留的仅是 F10 的类型截断）。 |
| plugin.go / node_onboarding.go / tasks.go / slot_operator.go / slot_detail.go / slot_add_remove.go / slot_recover_rebalance.go / system_users.go / controller_logs.go / controller_raft_status.go / dashboard_metrics.go / slot_logs.go / slot_raft_compaction.go / node_operator.go | Phase 1+2 走查未发现可写触发路径的缺陷：入参校验齐全（plugin.go 的 limit 规范化 50/200、node_onboarding 直通 cluster 严格 API）、错误显式返回、无无界扫描、无 DTO/HTTP 泄漏。 |
| channels_biz.go `businessChannelDetail` 3 次顺序 HasChannelSubscribers | 仅单频道详情路径，3 次本地读，量级可接受，不构成 N+1（列表路径用的是纯内存 `businessChannelListItem`，Phase 2 已核实）。 |
| G115 误报 11 条（users.go:200,270,387,437,617、channels_biz.go:400、overview.go:242、slot_add_remove.go:33、slot_recover_rebalance.go:100,101） | 值域受 slot 总数/device level 枚举约束，有上界保证，按 brief 标准为误报。 |
| 类别 (e) 无逐出缓存 | Phase 1 全量检查：本包所有 map 均为单请求局部变量，无跨请求缓存键控 channel/user，无逐出问题可报。 |
| 类别 (i) 入口耦合 | grep 全包无 gin/net/http 导入（命中仅为 "Plugins"/"origin" 子串），DTO 纯净，符合 AGENTS.md 入口无关要求。 |
| 类别 (h) 并发/锁 | 本包自身无共享可变状态（App 字段在 New 后只读）；fan-out 并发仅存在于 diagnostics.go，且 sem 有界、cancel 显式调用，无泄漏路径。 |

---

## 本分片整体评价

这个包的整体设计水准明显高于典型管理代码：游标哈希绑定过滤参数、diagnostics 的"每节点超时 + 有界并发 + 显式 partial 标注"三件套、overview 的异常组封顶，都是可以当作仓库范式的实现。但它存在一个系统性盲区——**同一批"聚合查询"路径没有复制 diagnostics 的超时纪律**：monitor_metrics、nodes、slots(NodeID)、controller_raft_compaction、connections(远程分支) 全部把调用方 ctx 直通到底层 RPC，而该 RPC 链路（usecase → access/node → pkg/cluster → pkg/transport/pool）除拨号超时外没有任何请求级 deadline，manager HTTP server 又未配置 server 超时，五处形成"单节点挂死 → 管理请求永久挂起 + goroutine 累积"的共同触发路径（F1/F2 因是轮询端点升为 P1）。另一个系统性问题是三类无预算全量扫描（用户/频道关键字搜索、频道健康汇总、连接全量 dump）在多节点部署下还会放大为串行跨节点 RPC 风暴。修复方向高度收敛：为所有 per-node/per-slot/per-page RPC 统一加每调用 deadline（一个包级常量即可），并给三个全量扫描加扫描预算或缓存。
