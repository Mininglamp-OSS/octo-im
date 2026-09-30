# 协调者抽检：Unit 31 的 P0-2 —— WebSocket 入站 payload 零拷贝跨 goroutine 交付

> 我独立复核了 Unit 31 的入站数据竞争 P0，**完全成立，七环链路全部逐行确认**。
> 这条属于**静默数据损坏**类：不崩溃、不报错，只是消息内容错乱。
> 而且它是 **PAT-8「正确实现就在同一个文件里、偏偏这一处没用」** 的最干净实例。

---

## 完整链路（七环，全部亲自读过）

**环 1 —— 入站缓冲区是复用的**
`pkg/gateway/transport/gnet/conn.go:283`：
```go
s.wsInbound = append(s.wsInbound, data...)
```
`:296`、`:328`：`s.wsInbound = s.wsInbound[:0]` —— **保留同一块底层数组**，只把长度归零。
`:293`、`:330`：`s.wsInbound = append(s.wsInbound[:0], s.wsInbound[consumed:]...)` / `s.wsInbound[consumed:]`。

**环 2 —— 解帧**
`conn.go:312`：`frame, consumed, err := decodeWSFrameWithLimit(s.wsInbound, s.maxPendingBytes)`

**环 3 —— 解帧返回的 payload 是**调用方缓冲区的子切片，不是拷贝**
`pkg/gateway/transport/gnet/ws_frame.go:127`：
```go
frame.payload = buf[offset : offset+payloadLen]      // ← 直接切片，零拷贝
if frame.masked {
	for i := range frame.payload {
		frame.payload[i] ^= frame.maskKey[i%4]       // ← 而且是【原地】解掩码，直接改写调用方缓冲区
	}
}
```

**环 4 —— WebSocket 路径把这个别名切片交给队列**
`pkg/gateway/transport/gnet/group.go:540`：
```go
if !state.enqueueDataWithOpcode(result.opcode, result.payload) {
```

**环 5 —— 该入队函数不拷贝**
`conn.go:118-133`：
```go
func (s *connState) enqueueDataWithOpcode(opcode byte, data []byte) bool {
	...
	s.queue = append(s.queue, connEvent{kind: connEventData, data: data, op: opcode})   // ← :129，存的就是入参切片
	s.mu.Unlock()
	s.signal()                                                                          // ← 唤醒【另一个 goroutine】
	return true
}
```

**环 6 —— 同一文件里就有正确版本，且注释点明了这个危险**
`conn.go:135-152`：
```go
// enqueueCopiedData copies from gnet's transient read buffer only after pending-byte admission succeeds.
func (s *connState) enqueueCopiedData(data []byte) bool {
	...
	payload := append([]byte(nil), data...)          // ← :146，拷贝
	s.pendingBytes += len(payload)
	s.queue = append(s.queue, connEvent{kind: connEventData, data: payload})
```
**TCP 路径用的正是它** —— `group.go:473`：`if !state.enqueueCopiedData(buf) {`

**环 7 —— 事件循环随后覆写那块数组**
`conn.go:328` `s.wsInbound = s.wsInbound[:0]`（底层数组不变）→
下一次读取回到 `conn.go:283` `append(s.wsInbound, data...)` →
**新数据直接写进另一个 goroutine 仍在读的那段内存**。

---

## 判定

**作者知道这个坑** —— 专门写了 `enqueueCopiedData`，注释里明确写着
"copies from gnet's **transient** read buffer"，并在 TCP 路径正确使用了它。
**WebSocket 路径没有用。** 这不是不懂，是漏了一处。

**后果**（两类，都不会报错）：
1. **数据竞争**：事件循环 goroutine 写、连接工作 goroutine 读同一块 `[]byte`，无同步。
2. **静默消息损坏 / 串包**：已入队但尚未处理的帧 payload 被后续帧的字节覆盖。
   若该 payload 来自一条 SEND，被污染的内容会**继续沿投递链路走下去并被持久化**
   —— 即写入集群的消息内容与客户端发送的不一致。

**可达性**：`ws-gateway` 是两个默认监听器之一（`0.0.0.0:5200`，见 `wukongim.conf.example:280`），
且网关无任何认证（我已独立核实，见 `00-coordinator-verify-unit04.md`）。
**任意 WebSocket 客户端在一次 TCP 读里塞入多个 WS 帧即可触发** —— 这是完全正常的客户端行为，
不需要恶意构造。

**修复**：`group.go:540` 改用 `enqueueCopiedData`（需要一个带 opcode 的拷贝版本），
或让 `decodeWSFrameWithLimit` 返回拷贝。注意环 3 的**原地解掩码**也需一并处理 ——
它已经在改写调用方缓冲区，即使入队侧改成拷贝，原地异或本身对 `wsInbound` 的修改仍需确认无害。

---

## 关于 Unit 31 的 P0-1（出站丢消息），我未独立复核

Unit 31 另报 `finishNextOutboundWrite`（`conn.go:640-651`）在**任何**写完成回调里都
`pop outboundWriteFrames[0]`，而只有 `writeWebSocketVector` 会入队该 slice，
于是一个小消息（<1024B）的回调会释放掉尚未发出的大消息 `wsWritevFrame` 并把
`bufs[0]/bufs[1]` 置 nil —— 而那正是 gnet 仍持有的 `[][]byte`，导致 `writev` 发送 0 字节且
`err == nil`（静默丢消息）。

**这条我没有逐行验证**（需要读 gnet 的 writev 回调契约），故在总清单中标注为
"分片 31 报告，协调者未独立复核"。它与 P0-2 是同一文件的对称问题（一进一出），
可信度高，但我不把未亲自核实的东西写成已确认。
