# 协调者抽检：Unit 19 的 P0-1 —— 一个未认证帧永久静默杀死控制面 Raft

> 我独立复核了 Unit 19 的头号 P0，**完全成立**。
> 就"触发难度"而言，这是整个审计中**最容易触发的 P0**：**一个报文，无需任何凭据，
> 无需并发时序，无需特定集群状态**。且死亡是**静默的**。

---

## 缺陷机理（三环，全部亲自核实，含依赖库源码）

### 环 1 —— etcd raft 对"来自未知 peer 的响应类消息"返回专用错误

`~/go/pkg/mod/go.etcd.io/raft/v3@v3.6.0/rawnode.go:117-127`：
```go
func (rn *RawNode) Step(m pb.Message) error {
	// Ignore unexpected local messages receiving over network.
	if IsLocalMsg(m.Type) && !IsLocalMsgTarget(m.From) {
		return ErrStepLocalMsg
	}
	if IsResponseMsg(m.Type) && !IsLocalMsgTarget(m.From) && rn.raft.trk.Progress[m.From] == nil {
		return ErrStepPeerNotFound          // ← 第 124 行
	}
	return rn.raft.Step(m)
}
```
任何**响应类**消息（`MsgHeartbeatResp` / `MsgAppResp` / `MsgVoteResp` …）
只要其 `From` 不在 `Progress` 跟踪表里，就返回 `ErrStepPeerNotFound`。
**这是 etcd raft 的正常防御行为**，意思是"忽略它"，不是"致命"。

### 环 2 —— 本项目只过滤了两个错误中的一个

`pkg/controller/raft/service.go:956-962`：
```go
case msg := <-stepCh:
	if err := rawNode.Step(msg); err != nil && !errors.Is(err, raft.ErrStepLocalMsg) {
		s.setError(err)
		failTracked(pendingQueue, pendingByIndex, err)
		return                              // ← 运行协程退出
	}
```
`rawnode.go` 只有**两个** Step 错误：`ErrStepLocalMsg`（:121）与 `ErrStepPeerNotFound`（:124）。
本项目**只过滤了前者**。后者直落 `setError` + `return`。

看得出作者知道"Step 会返回可忽略的错误"（专门 `errors.Is` 了一个），**却漏了同一处返回的另一个**。
两个错误在依赖库里是紧邻的两行。

### 环 3 —— 没有重启，且死亡无法观测

- `go s.run(` 在 `service.go` 全文件**只出现一次**（`:372`）—— 无看门狗、无 supervisor、无 restart-on-error（我在复核 Unit 20 时已确认）。
- `pkg/transport` 非测试代码 **auth / TLS 零命中**，`serveConn` 从不识别对端身份 —— 任何能连上端口的一方都能投递 raft 消息。
- **Unit 19 的 P1-3（我采信其结论）**：`Status` 没有任何字段承载这个致命错误，
  **运行协程死亡与"正常停止"在外部完全不可区分**。没有日志告警、没有健康检查反映、没有指标。

---

## 触发路径（一步）

向目标节点的集群端口发送**一个** raft 响应类消息（例如 `MsgHeartbeatResp`），
`From` 字段填一个**不在该集群成员表里的任意节点 ID**。

→ `Step` 返回 `ErrStepPeerNotFound` → 未被过滤 → `setError` + `return`
→ **该节点控制面 Raft 永久停摆**，且外部看不出任何异常。

**不需要**：凭据、并发时序、特定集群状态、多次尝试、大流量。

---

## 严重度定位：与另两条 P0 的比较

| | 触发难度 | 破坏范围 | 是否静默 |
|---|---|---|---|
| **本条（Unit 19 P0-1）** | **一个报文，无凭据** | 单节点控制面（可逐个点杀全集群） | **完全静默** |
| P0-1 毒消息崩溃循环（Unit 02） | 一个未认证 HTTP 请求 | 承载该 channel 的节点，且随 leader 漂移扩散 | 进程崩溃，可见 |
| P0-13 Controller 毒丸（Unit 20） | 两个并发管理请求 | **全集群**控制面同时死亡 | 进程崩溃，可见 |

**本条的独特危险在于"静默"**：另两条会崩进程，运维至少能从重启、日志、告警里看到。
本条只是让 Raft 循环安静地退出 —— 节点还在、端口还听、健康检查可能还返回 OK，
但控制面已经不再前进。这种故障模式极难诊断。

**结论**：`service.go:957` 的 `errors.Is(err, raft.ErrStepLocalMsg)` 应改为同时放行
`ErrStepPeerNotFound`（依赖库的语义是"忽略此消息"）。这是**一行修复**。

---

## 同时确认 Unit 19 的另两条 P0

- **P0-2**：run loop 把**所有** apply/decode 错误都当作致命且不可恢复 ——
  这正是我在复核 Unit 20 时确认的那半部分（`service.go:942-946`）。
  Unit 19 按我的要求把它作为**设计问题**回答了：错误路径里既有真正的存储/传输故障
  （致命尚可辩护），也有业务/状态相关错误（致命是错的），而代码不区分。
- **P0-4**：`chunk.total` 无上界 → 单个约 81 字节的未认证快照分片帧即可让进程
  `make([]byte, 1<<60)` 崩溃。这是 **PAT-2** 在本包的又一个实例
  （与 Unit 15/16/23/24/27 的同类站点合并计数）。

---

## 对总报告的影响

1. **PAT-4 需要扩写**：原表述是"apply 对状态相关条件返回 error 且调用方视为致命"。
   实际更广：**run loop 对一切来源的 error 都视为致命**，而其中一类（`ErrStepPeerNotFound`）
   是依赖库明确用来表达"请忽略"的。根因不只是状态机返回了业务错误，
   **更是 run loop 没有对错误做可恢复性分类**。
2. **新增一条模式**：项目多处只处理了"一组同类错误中的一个"
   （此处漏 `ErrStepPeerNotFound`；Unit 06 的 `handleChannelRetentionRPC` 用 `==` 而非
   `errors.Is` 比较 sentinel；Unit 05 的 `context.Canceled` 未被任何错误分类器识别）。
3. **修复优先级**：`service.go:957` 加一个 `errors.Is` 应进入"第 0 批"，
   与 `pkg/transport/server.go:186` 的 `recover()` 并列 —— 两者都是一行、都封堵未认证远程杀伤。
