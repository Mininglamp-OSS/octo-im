# 协调者抽检：Unit 23 的分页归并序缺陷 —— 静默漏数据（已确认）

> Unit 23 报了两条"分页静默漏项"。我独立复核，**完全成立**。
> 这类缺陷没有任何扫描器能发现（不是内存安全、不是并发、不是 lint），
> 也不会产生任何错误或日志 —— **它只是少返回数据**。因此值得单列。

---

## 缺陷：k 路归并用 Go 字符串序，而底层存储键是"长度优先"序

### 事实 1 —— 归并堆用纯字典序（亲自核实）

`pkg/slot/proxy/identity_rpc.go:250-252`：
```go
func (h userMergeHeap) Less(i, j int) bool {
	return h[i].User.UID < h[j].User.UID
}
```
`pkg/slot/proxy/plugin_binding_rpc.go:714+`：
```go
func (h pluginBindingMergeHeap) Less(i, j int) bool {
	if h[i].Binding.PluginNo != h[j].Binding.PluginNo {
		return h[i].Binding.PluginNo < h[j].Binding.PluginNo
	}
	if h[i].Binding.UID != h[j].Binding.UID {
		return h[i].Binding.UID < h[j].Binding.UID
	}
	...
```
两处都是 Go 原生 `string` 的 `<` —— **纯按字节内容比较**。

### 事实 2 —— 存储键是长度前缀在前（亲自核实，这是关键）

`pkg/db/internal/keycodec/codec.go:47-53`：
```go
func AppendString(dst []byte, value string) []byte {
	if len(value) > maxStringLen {
		panic(fmt.Sprintf("keycodec: string key part too long: %d", len(value)))
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))   // ← 长度前缀在前
	return append(dst, value...)
}
```
**大端 uint16 长度前缀写在内容之前。** 所以 Pebble 的字节序 = **(长度, 内容)** 的字典序：
**短字符串一律排在长字符串之前，与内容无关。**

### 事实 3 —— 两种序在最普通的输入上就相反

取两个 UID：`"b"` 与 `"ab"`。

| 比较方式 | 结果 |
|---|---|
| Go `<`（归并堆用的） | `"ab" < "b"` —— 因为 `'a'`(0x61) < `'b'`(0x62) |
| 存储键字节序（Pebble 实际用的） | `"b"` → `00 01 62`；`"ab"` → `00 02 61 62`；`00 01…` < `00 02…` → **`"b" < "ab"`** |

**完全相反。** 不需要构造极端输入 —— 任意两个长度不同且短者首字节较大的 UID 就会触发。

---

## 触发路径与后果

1. 权威用户列表分页（`ListUsers` 类接口）向每个 hash slot 分片发 RPC；
   **每个分片按存储序（长度优先）返回自己的那一段**。
2. 协调节点用 `userMergeHeap` 做 k 路归并 —— 但堆的序是**内容优先**。
3. 归并输出因此**不是任何一致的全序**：堆会先弹出它认为"最小"的元素，
   而该元素在存储序里可能并非最小。
4. 分页游标取自"本页最后一个返回项"。下一页据此 `SeekGE` —— 于是
   **存储序上位于该游标之前、但内容序上排在它之后的行，被整段跳过**。
5. 调用方拿到的是一个**看起来正常、实际少了行**的结果集。没有错误、没有日志、没有告警。

**后果**：
- 权威用户分页静默漏项（`identity_rpc.go`）
- 插件绑定按 plugin_no 分页静默漏项（`plugin_binding_rpc.go`）
- 任何依赖"遍历全部用户/绑定"的运维或迁移动作都会**静默处理不完整**
  —— 例如缩容盘点、权限审计、批量迁移

**为什么危险性高于普通 P1**：这是**静默的正确性缺陷**。
崩溃会被发现，OOM 会被发现，慢查询会被发现。少返回 3 个用户不会被任何人发现，
直到某天有人问"为什么这个用户没被迁移过去"。

---

## 顺带确认：`AppendString` 的 panic 也在存储键路径上

`codec.go:48-50` 在字符串超过 `maxStringLen` 时 **`panic`**。
这与 PAT-3（panic 当错误处理、关键路径无 recover）是同一模式，
且 Unit 29 已独立把它报为该分片的 P0（超长 HTTP 字段 → 键编码 panic → 崩掉 channel leader 节点）。
两条互相印证：**同一个 `AppendString` 既是排序缺陷的根源，也是一个远程 panic 点。**

---

## 建议

归并堆的 `Less` 必须与存储键序严格一致。两种可行方向：
1. 比较时按**编码后的键字节**比较（即先 `keycodec.AppendString` 再 `bytes.Compare`），而不是比较原始字符串；
2. 或改用固定宽度/无长度前缀的键编码（但会影响既有数据格式，成本高）。

方向 1 成本低且可立即验证。另外应为"分片归并 + 分页游标"补一条不变式测试：
**任意分片切分下，全量分页结果必须与单分片全量扫描结果逐行相等** ——
这类缺陷只有这种性质测试能稳定捕获。

`runtimeMetaMergeHeap`（`runtime_meta_rpc.go:399`，走 `channelRuntimeMetaLess`）
与 `channelMergeHeap`（`channel_rpc.go:422`，走 `channelLess`）用的是自定义比较函数，
**需要按同一标准另行核实**（Unit 23 未覆盖这两处，我也未核实 —— 列为待办，不作为已确认发现）。
