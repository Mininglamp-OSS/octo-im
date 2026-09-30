# 协调者核实：节点 RPC 解码层"集合长度"未走安全辅助函数（修正并扩展 Unit 6）

> Unit 6 报告了其中 1 处并称"是全包唯一一处手写 `int(count)` 而没调用共享
> `readCollectionLen` 的地方"。我复核后确认缺陷成立，但**该判断不完整：实际有 4 处**，
> 另外 2 处在 `plugin_management_codec.go`，Unit 6 未发现。
> 这条应作为**模式级**问题进入总清单。

---

## [P0] N-1. 4 处解码站点绕过安全的 `readCollectionLen`，用线路长度直接 `make()`

### 包内已存在正确范式（这是判定"疏漏而非取舍"的决定性证据）

`internal/access/node/delivery_push_codec.go:283-292`：
```go
func readCollectionLen(count uint64, remaining int, label string) (int, error) {
	maxInt := uint64(^uint(0) >> 1)
	if count > maxInt {
		return 0, fmt.Errorf("access/node: %s count overflows int", label)
	}
	if count > uint64(remaining) {
		return 0, fmt.Errorf("access/node: %s count exceeds remaining bytes", label)
	}
	return int(count), nil
}
```
这个辅助函数做了**两个都必要**的检查：
1. `count > maxInt` —— 阻止 `uint64 → int` 的符号翻转（负数 cap）
2. `count > uint64(remaining)` —— 阻止元素个数远超实际剩余字节数的荒唐分配

**全包 15 个文件正确调用了它。**

### 但有 4 处绕过（我亲自 grep 核实，无遗漏）

| 位置 | 代码 | 备注 |
|---|---|---|
| `internal/access/node/delivery_control_codec.go:48` | `commands := make([]deliveryevents.RouteAck, 0, int(count))` | Unit 6 已报 |
| `internal/access/node/monitor_metrics_codec.go:191` | `series := make([]float64, int(count))` | **Unit 6 未报**；注意是 **len** 而非 cap，切片会被完整分配并清零 |
| `internal/access/node/plugin_management_codec.go:195` | `plugins := make([]pluginusecase.LocalPlugin, int(count))` | **Unit 6 未报**；同为 len |
| `internal/access/node/plugin_management_codec.go:434` | `methods := make([]pluginusecase.Method, int(count))` | **Unit 6 未报**；同为 len |

### 触发路径（以 `delivery_control_codec.go:48` 为例，已逐行核实）

```go
// delivery_control_codec.go:37-56
func decodeDeliveryAckRequest(body []byte) (deliveryAckRequest, error) {
	if hasMagic(body, deliveryAckBatchRequestMagic[:]) {
		offset := len(deliveryAckBatchRequestMagic)
		count, next, err := readUvarint(body, offset)   // count: uint64，完全来自线路
		if err != nil { return deliveryAckRequest{}, err }
		if count == 0 {                                  // ← 唯一的校验：只挡 0
			return deliveryAckRequest{}, fmt.Errorf("access/node: empty delivery ack batch")
		}
		offset = next
		commands := make([]deliveryevents.RouteAck, 0, int(count))   // ← 第 48 行，无上界
```
`count` 只被校验"非 0"，**没有任何上界**，随后直接 `int(count)` 作为 cap：
- `count = 2^63`（uvarint 编码约 10 字节）→ `int(count)` 为**负数** →
  `panic: runtime error: makeslice: cap out of range`
- `count = 2^40`（uvarint 仅 6 字节）→ `int(count)` 为正但巨大 →
  为 `RouteAck` 结构体预留约 TB 级容量 → 立即 OOM

两种情形都只需**十几到几十字节**的请求体。

### 为什么等于崩进程（与 Unit 15 的 P0 共用同一放大器，我已独立核实）

- `pkg/transport` 非测试代码中 **auth / TLS 零命中** —— 任何能连到节点 RPC 端口的一方都能发。
- `pkg/transport/server.go:186` —— `go s.handleRPCRequest(...)` 是**裸 goroutine**，
  该文件 `recover()` 零命中；全仓非测试代码仅 2 处 `recover()`，都不在此路径。
- 故 handler 内的 panic **直接终止整个进程**，不是单连接失败。

**后果**：未认证远程方用几十字节畸形包即可打崩任意节点。**判定 P0。**

**修复方向**：这 4 处全部改为调用现成的 `readCollectionLen(count, len(body)-offset, "<label>")`。
这是一个纯机械修复，不需要新设计 —— 正确函数已经写好并在 15 处使用。

---

## 与其它单元的关系（供汇总去重用）

这条与 Unit 15 的 P0（`pkg/cluster/codec_control.go:2784,2795,2597` 的
`if len(rest) < int(length)` 被 `int()` 截断绕过）是**同一根因的两个实例**：
**`uint64` 线路长度在用于分配或切片之前被转成 `int`，转换本身即是绕过点。**

汇总时应合并为一条模式级问题，列出全部 7 个已确认站点：
- `pkg/cluster/codec_control.go:2597, 2784, 2795`（守卫被截断绕过）
- `internal/access/node/delivery_control_codec.go:48`
- `internal/access/node/monitor_metrics_codec.go:191`
- `internal/access/node/plugin_management_codec.go:195, 434`

并同时指出**两套正确范式都已存在于仓库内**：
- `internal/access/node/delivery_push_codec.go:283`（`readCollectionLen`，15 处在用）
- `pkg/cluster/codec_control.go:995`（`count > uint64(len(body)/minWireSize)`）

这使整条问题的性质明确为**规范未统一执行**，而非缺少方案 —— 也说明 gosec 的 396 条 G115
里确实埋着真问题，只是被大量误报淹没了。
