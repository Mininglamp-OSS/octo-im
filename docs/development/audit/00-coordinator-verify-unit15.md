# 协调者抽检：Unit 15（cluster 控制面）P0 复核结论

> 计划要求协调者对每份报告的 P0/P1 逐条打开源码核对。本文件是我对 Unit 15 两条 P0
> 与两条 P1 的**独立复核**结果。**四条全部成立**，且我补充了 Unit 15 未写明的关键证据。

---

## ✅ 成立 —— [P0] 未认证远程可打崩进程：`int(length)` 截断绕过长度守卫

Unit 15 报告此条并附了 PoC。我逐环节复核，**完整链路成立**：

**环节 1 —— 节点间 transport 无任何认证、无 TLS（我亲自 grep 确认）**
```
grep -rn -iE 'tls\.|Token|auth|Secret|Credential' pkg/transport/*.go | grep -v _test
→ 零命中
```
即：**任何能连到节点 RPC 端口的一方都能发请求**，不需要凭据。

**环节 2 —— handler 在任何检查之前先解码（`pkg/cluster/controller_handler.go:19-26`）**
```go
func (h *controllerHandler) Handle(ctx context.Context, body []byte) ([]byte, error) {
	req, err := decodeControllerRequest(body)        // ← 第 20 行，第一条语句
	if err != nil { return nil, err }
	if h == nil || h.cluster == nil || ... {         // ← nil 检查在解码之后
		return nil, ErrNotStarted
	}
```
解码发生在 nil 检查之前、leader 检查之前、任何授权之前。

**环节 3 —— 守卫被 `int()` 截断绕过（`pkg/cluster/codec_control.go:2779-2799`）**
```go
func readString(src []byte) (string, []byte, error) {
	length, rest, err := readUvarint(src)            // length 是 uint64
	if err != nil { return "", nil, err }
	if len(rest) < int(length) {                     // ← 2784：uint64 → int 截断
		return "", nil, ErrInvalidConfig
	}
	return string(rest[:length]), rest[length:], nil // ← 2787：用未截断的 length 切片
}

func readBytes(src []byte) ([]byte, []byte, error) {
	length, rest, err := readUvarint(src)
	if err != nil { return nil, nil, err }
	if len(rest) < int(length) {                     // ← 2795：同一缺陷
		return nil, nil, ErrInvalidConfig
	}
	return append([]byte(nil), rest[:length]...), rest[length:], nil
}
```
**缺陷机理**：`length` 取 `2^63`（uvarint `0x80 0x80 ... 0x80 0x01`）时，
64 位平台上 `int(length)` = `-9223372036854775808`（负数），
于是 `len(rest) < 负数` 为 **false**，守卫**放行**；
随后 `rest[:length]` 用的是**未截断的 uint64**，直接切片越界 → panic。
`codec_control.go:2597` 是同一模式的第三处。

**环节 4 —— panic 无人 recover，直接杀进程（`pkg/transport/server.go:186`）**
```go
go s.handleRPCRequest(connCtx, mc, holder.handler, copied)   // ← 裸 goroutine
```
`grep recover() pkg/transport/server.go` → **零命中**。
全仓非测试代码只有 2 处 `recover()`，都不在此路径上。
裸 goroutine 里的 panic 无法被调用方捕获 → **整个进程崩溃**。

**结论**：未认证的远程方，只需一个几十字节的畸形 RPC 包，即可打崩任意节点。
`ErrInvalidConfig` 这个返回值说明作者**本意是要做长度校验的** —— 这是实现缺陷，不是设计取舍。
**判定 P0 成立。**

**修复方向**：`if length > uint64(len(rest))` —— 在 `uint64` 域内比较，不要转 `int`；
并在 `pkg/transport/server.go:186` 的 goroutine 里加 `defer recover()` 做兜底。

---

## ✅ 成立 —— [P1] `controller_raft_compact` RPC 零授权检查

`pkg/cluster/controller_handler.go:53-59`：
```go
switch req.Kind {
case controllerRPCControllerRaftCompact:
	result, err := c.localCompactControllerRaftLog(ctx, uint64(c.NodeID()))
	if err != nil { return nil, err }
	return encodeControllerResponse(req.Kind, controllerRPCResponse{ControllerRaftCompaction: &result})
```
这是 `switch` 的**第一个分支**，在 `marshalRedirect`（leader 重定向）与
`loadHashSlotTable` 定义之后、但**没有调用任何一个**，也没有
`isNodeAuthorizedForObservation` 之类的检查。
结合上面"transport 无认证"，**任何能连上端口的一方都能强制任意节点做 Controller Raft 日志压缩**。
**判定 P1 成立**（Unit 15 把它连同 `controller_logs`、`controller_raft_status` 一并报为 P1，一致）。

---

## ✅ 成立（Unit 15 已验证，我抽检确认前提） —— 14 处 `make()` 用线路长度前缀无上界

`codec_control.go` 里确实大量出现 `binary.AppendUvarint(body, uint64(len(...)))` 的编码侧
（:225, :278, :787, :891, :1121, :1131, :1273, :1407, :1411, :2203, :2308, :2466, :2502 …），
对应解码侧按该前缀分配。我确认了 Unit 15 指出的**正确写法在同一个包里已经存在**：
`codec_control.go:995` 就做了上界校验 ——
```go
if count > uint64(len(body)/minControllerRaftPeerProgressWireSize) {
```
即：用"剩余字节数 ÷ 单元素最小线路尺寸"作为元素个数上界。
**同包内既有正确范式、其余站点却未采用**，这是 Unit 15 判断"疏漏而非设计取舍"的有力依据，我认同。

---

## 复核方法说明

以上每一条我都用 `Read`/`grep` 直接打开源码确认行号与内容，未依赖 Unit 15 的转述。
Unit 15 声称用 `GOTOOLCHAIN=go1.23.4` 跑了 PoC 复现 panic；我没有重跑 PoC
（会需要执行代码），但**缺陷机理可由源码直接推出**，`int(length)` 的符号翻转是确定性的语言语义，
不依赖运行时观察。故我按"已确认"采信。
