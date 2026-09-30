# 27-channel-handler-transport: channel 数据面入口/传输 + channelv2 外围

## 覆盖情况

### 生产路径（`pkg/channel/handler/`，`pkg/channel/transport/`）— 完整严重度

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/channel/handler/meta.go | 219 | 是 |
| pkg/channel/handler/append_idempotency.go | 81 | 是 |
| pkg/channel/handler/key.go | 74 | 是 |
| pkg/channel/handler/apply.go | 10 | 是 |
| pkg/channel/handler/read_window.go | 90 | 是 |
| pkg/channel/handler/fetch.go | 99 | 是 |
| pkg/channel/handler/message_sync.go | 162 | 是 |
| pkg/channel/handler/message_query.go | 180 | 是 |
| pkg/channel/handler/seq_read.go | 201 | 是 |
| pkg/channel/handler/append.go | 352 | 是 |
| pkg/channel/handler/codec.go | 224 | 是 |
| pkg/channel/transport/transport.go | 364 | 是 |
| pkg/channel/transport/session.go | 353 | 是 |
| pkg/channel/transport/longpoll.go | 121 | 是 |
| pkg/channel/transport/probe_client.go | 65 | 是 |
| pkg/channel/transport/sendtrace_helpers.go | 47 | 是 |
| pkg/channel/transport/doc.go | 6 | 是 |
| pkg/channel/transport/codec.go | 522 | 是 |
| pkg/channel/transport/longpoll_codec.go | 338 | 是 |
| pkg/channel/transport/migration_control.go | 208 | 是 |

### channelv2 外围（未上线，`internalv2` 不被任何 `cmd/` 二进制导入）— 降一级严重度

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/channelv2/channel.go | 69 | 是 |
| pkg/channelv2/doc.go | 6 | 是 |
| pkg/channelv2/errors.go | 22 | 是 |
| pkg/channelv2/types.go | 157 | 是 |
| pkg/channelv2/machine/append.go | 333 | 是 |
| pkg/channelv2/machine/channel.go | 175 | 是 |
| pkg/channelv2/machine/invariant.go | 11 | 是 |
| pkg/channelv2/machine/meta.go | 85 | 是 |
| pkg/channelv2/machine/progress.go | 21 | 是 |
| pkg/channelv2/worker/deps.go | 17 | 是 |
| pkg/channelv2/worker/pool.go | 138 | 是 |
| pkg/channelv2/worker/pools.go | 108 | 是 |
| pkg/channelv2/worker/result.go | 70 | 是 |
| pkg/channelv2/worker/task.go | 289 | 是 |
| pkg/channelv2/store/adapter.go | 81 | 是 |
| pkg/channelv2/store/channel_adapter.go | 299 | 是 |
| pkg/channelv2/store/memory.go | 218 | 是 |
| pkg/channelv2/service/append.go | 71 | 是 |
| pkg/channelv2/service/meta.go | 27 | 是 |
| pkg/channelv2/service/replication.go | 106 | 是 |
| pkg/channelv2/service/service.go | 113 | 是 |
| pkg/channelv2/transport/local.go | 192 | 是 |
| pkg/channelv2/transport/types.go | 127 | 是 |
| pkg/channelv2/replication/leader.go | 9 | 是 |
| pkg/channelv2/replication/follower.go | 10 | 是 |
| pkg/channelv2/testkit/cluster.go | 127 | 是（仅供 `_test.go` 调用，无任何非测试调用点，已用 `grep -rln` 验证） |

注：`pkg/channelv2/reactor/`（unit 28）、`pkg/channel/replica/`、`pkg/channel/runtime/` 及 `pkg/channel` 根目录 `*.go`（unit 26）不在本分片范围内；为核实我范围内候选发现是否被上层逻辑吸收/掩盖，我读取了 `pkg/channelv2/reactor/reactor.go`、`effect.go`、`future.go` 作为上下文佐证（未在其中报告发现，也未计入本单元覆盖表）。

## 发现

### [P0] 1. 对端可控长度前缀在校验前直接喂给 `make()`，可远程触发单节点 OOM 崩溃

- **位置**:
  - `pkg/channel/transport/codec.go:184`（`decodeFetchResponse` 的 `count` → `make([]channel.Record, 0, count)`）
  - `pkg/channel/transport/codec.go:217-234`（`readRecord` 的 `payloadLen` → `make([]byte, payloadLen)`）
  - `pkg/channel/transport/codec.go:512-522`（`readChannelKey` 的 `length` → `make([]byte, length)`，被请求/响应双向 4 处调用）
  - `pkg/channel/transport/longpoll_codec.go:115,134`（`decodeLongPollFetchRequest` 的 `membershipCount`/`deltaCount`）
  - `pkg/channel/transport/longpoll_codec.go:280,324`（`decodeLongPollFetchResponse` 的 `itemCount`/`recordCount`，内部再次调用 `readRecord`）
  - `pkg/channel/transport/migration_control.go:179-189`（`readString` 的 `length` → `make([]byte, length)`）
- **类别**: 远程输入可触达的崩溃（未校验长度前缀 → `make()`/索引）
- **代码**:
```go
// pkg/channel/transport/codec.go:217-234
func readRecord(rd *bytes.Reader) (channel.Record, error) {
	var record channel.Record
	var header [36]byte
	if err := readFullBytesReader(rd, header[:]); err != nil {
		return channel.Record{}, err
	}
	record.ID = binary.BigEndian.Uint64(header[0:8])
	record.Index = binary.BigEndian.Uint64(header[8:16])
	record.Epoch = binary.BigEndian.Uint64(header[16:24])
	sizeBytes := int64(binary.BigEndian.Uint64(header[24:32]))
	payloadLen := binary.BigEndian.Uint32(header[32:36])
	record.Payload = make([]byte, payloadLen)
	if err := readFullBytesReader(rd, record.Payload); err != nil {
		return channel.Record{}, err
	}
	...
}

// pkg/channel/transport/codec.go:512-522
func readChannelKey(rd *bytes.Reader) (channel.ChannelKey, error) {
	var length uint32
	if err := binary.Read(rd, binary.BigEndian, &length); err != nil {
		return "", err
	}
	channelKey := make([]byte, length)
	if _, err := io.ReadFull(rd, channelKey); err != nil {
		return "", err
	}
	return channel.ChannelKey(channelKey), nil
}
```
  `longpoll_codec.go` 的 `decodeLongPollFetchResponse` 同样先读 `itemCount`（`uint32`）后立刻 `resp.Items = make([]LongPollItem, 0, itemCount)`，内层再读 `recordCount` 后 `item.Records = make([]channel.Record, 0, recordCount)`，两处都在读取任何一条 item/record 实际内容之前完成分配。
- **触发路径**:
  1. `pkg/channel/transport/transport.go:97-100` 在 `New()` 构造时把 `handleRPC`（→`decodeFetchRequest`/`decodeFetchResponse` 同族代码路径）、`handleLongPollFetchRPC`（→`decodeLongPollFetchRequest`）、`handleReconcileProbeRPC`、`handleMigrationControlDrainRPC`（→`readString`/`readChannelKey`）直接注册进 `opts.RPCMux.Handle(...)`，这是集群内任意对端节点发来的 RPC 帧的统一入口；`session.go:91` 的 `decodeFetchResponse(respBody)` 与 `transport.go:348` 的 `decodeLongPollFetchResponse(respBody)` 则是客户端侧对对端*响应*字节的解码入口。已用 `grep -rn` 核实以上调用链，均未在 `make()` 之前对声明的 `count`/`length`/`payloadLen` 做任何上界比较（对 `pkg/channel/transport/*.go` 全文件 grep `maxRecordCount|maxChannelKeyLen|maxItemCount|length >|count >` 等模式，0 命中）。
  2. `pkg/channel/FLOW.md` 明确写明 steady-state `LongPollFetch` 是"当前唯一复制主路径"，即该 RPC 在每个 leader—follower 对之间持续双向交换；`readChannelKey`/`readRecord` 因此位于集群稳态复制热路径上。
  3. 攻击者/故障对端只需构造一个总大小仍在 `pkg/transport` 64MB 帧上限（`MaxFrameSize = MaxMessageSize = 64<<20`，`pkg/transport/frame.go` 在帧头处校验）以内、但**内部**某个长度/计数字段被篡改为接近 `0xFFFFFFFF` 的小 RPC 请求或响应体（例如一条 5-20 字节的 `FenceAndDrain`/`Fetch` 请求，或一次被中间人篡改/因传输层损坏产生的 `LongPollFetch` 响应）。帧层的 64MB 校验只约束帧总长度，不约束帧体内嵌的业务字段值，因此该字段可以在被读取的字节数远小于其声明值的情况下先行触发分配。
  4. `record.Payload = make([]byte, payloadLen)` 在 `payloadLen` 接近 `2^32-1` 时尝试分配约 4GB；`resp.Records = make([]channel.Record, 0, count)`/`item.Records = make([]channel.Record, 0, recordCount)` 在 `count` 接近 `2^32-1` 时，由于 `channel.Record` 结构体本身包含 `[]byte` 等字段（非零大小），尝试分配的容量可达数十至上百 GB。
- **后果**: Go runtime 对巨型分配请求触发的是 `fatal error: out of memory`（运行时致命错误），不是可恢复的 `panic`，进程内任何 `recover()` 均无法拦截，节点直接崩溃退出。由于该字段位于 leader↔follower 双向复制热路径（`LongPollFetch`）及迁移控制路径（`FenceAndDrain`）上，一次异常/伪造的小请求或响应即可使集群中的 leader 节点或 follower 节点崩溃，且该崩溃在多节点场景下可反复由同一畸形包在不同对端间触发（例如伪造响应打崩发起 `LongPollFetch` 拉取的一方）。
- **建议**: 在每个 `binary.Read(...&count/&length/&payloadLen)` 之后、对应 `make()`/循环之前，加入与"剩余可读字节数"或一个显式协议常量（如现有 `pkg/transport` 帧体上限、或 `PullMaxBytes`/记录条数上限）比较的校验，校验失败直接返回解码错误而不是继续分配；对 `readRecord`/`readChannelKey`/`readString` 这类被多处复用的底层函数，建议在函数入口处基于 `rd.Len()`（`bytes.Reader` 剩余字节数）做一次通用的“声明长度不能超过剩余可读字节数”兜底检查，这样可以一次性修掉全部 8 个调用点，且不需要逐个調用处感知具体业务上限。

## 已排除的候选项

- **`pkg/channel/handler/append_idempotency.go` 全文件（sorted-key 全局排序锁）**：按 `WORKER_BRIEF.md` 提示重点复核。`lockAppendIdempotencyKeys` 先用 `normalizeAppendIdempotencyKeys`/`appendIdempotencyKeyLess` 对本次请求涉及的 key 做全局确定性排序（ChannelID.Type → ChannelID.ID → FromUID → ClientMsgNo），再按该顺序逐个在 `s.mu` 保护下获取/创建引用计数锁并递增 `refs`，随后释放 `s.mu` 才真正 `lock.mu.Lock()`；解锁闭包按相反顺序释放，`refs` 归零时在 `s.mu` 下删除 map 项。由于所有调用者对任意 key 子集都遵循同一全局顺序获取锁，不存在环形等待，且 `refs`/map 的读写全部在同一把 `s.mu` 下完成，不存在数据竞争。**结论：确认无死锁/竞态，非发现。**
- **`pkg/channel/handler/key.go:25,57`（`unsafe.String`/`unsafe.Slice(unsafe.StringData(...))`，2 处 G103）**：`KeyFromChannelID` 中 `buf` 是函数局部构造、返回后从不再被写入的字节切片，`unsafeStringBytes` 的反向转换同理只作用于只读的 `ChannelKey` 字符串。两处零拷贝转换的前提（底层缓冲区转换后不可变）均成立。**结论：设计上安全，非发现。**
- **`pkg/channel/handler/seq_read.go:188`（G115，`uint64 -> int`）**：`batchLimit = int(remainingSpan)` 只在 `if remainingSpan < uint64(batchLimit)` 分支内执行，而 `batchLimit` 初值是常量 `seqReadChunkLimit = 256`，故进入该分支时 `remainingSpan` 已被证明 `< 256`，转换不可能溢出。**结论：误报。**
- **`pkg/channel/handler/codec.go:50,89`（G115，`int32<->uint32` Timestamp 往返）**：`encodeMessage` 写入 `uint32(message.Timestamp)`，`decodeMessageView`/`decodeMessageRecord` 用 `int32(binary.BigEndian.Uint32(...))` 还原，是同一有符号字段的对称编解码往返（有意的补码位重解释），不是数据破坏。**结论：误报。**
- **`pkg/channel/handler/codec.go:183,188`（G115，`int(len(value)) -> uint32`，2 处）**：调用方 `encodedMessageSize` 在生成任何一个可变长字段之前显式校验其长度 `<= int(^uint32(0))`，超限返回 `ErrInvalidArgument`；已跟踪确认这两处 `appendSizedString`/`appendSizedBytes` 在实际调用链中都在该校验之后才被调用。**结论：误报。**
- **`pkg/channel/transport/session.go:115,116,117`、`migration_control.go:172`、`longpoll_codec.go:26,32,55,179,194,238,239`、`codec.go:129,204,205,503`（共 16 处 G115）**：逐一核对后均属于"编码侧本地可信数据"或"内部已受限的数值"两类：session.go 三处是 `time.Duration`/`int64` 到 RPC 超时/长度字段的本地配置值转换；migration_control.go:172 与 codec.go/longpoll_codec.go 的 `int -> uint32` 全部是**编码方向**的 `appendXxxBytes`/`encodedSize` helper（写本地已知长度的 `len(x)`），与本发现中"解码方向未校验长度前缀"的 8 个 `make()` 调用点是不同性质——编码方向溢出只会产生错误的本地帧（在 64MB 帧总长度校验下不可行），不构成远程触发的分配放大。**结论：均为误报/非本发现范畴，已与主发现区分。**
- **`pkg/channel/transport/migration_control.go:191-208`（`normalizeMigrationControlError` 与 GO-2025-3373 x509 可达性）**：按提示专项核查。该函数只对一组已知的 channel 层哨兵错误（`ErrNotLeader`/`ErrStaleMeta`/`ErrWriteFenced`/`ErrNotReady`/`ErrLeaseExpired`/`ErrChannelNotFound`）做 `errors.Is` 或错误文本子串匹配，用于把 RPC 层按纯文本序列化的已知错误还原成 sentinel error；一个真实的 TLS/x509 证书校验失败错误（如 `x509.HostnameError`）的错误文本不可能恰好包含"not leader"/"stale meta"等特征短语，因此不会被误判/掩盖为上述哨兵错误之一，会原样穿透返回。**结论：不构成掩盖真实证书校验失败的问题，非发现。**
- **`pkg/channelv2/store/channel_adapter.go:211,237`（G115，`int32<->uint32` Timestamp 往返）**：与 v1 `handler/codec.go:50/89` 完全相同的对称编解码往返模式（`encodeDBCompatibleMessage`/`decodeDBCompatibleMessage`）。**结论：误报。**
- **`pkg/channelv2/store/channel_adapter.go:274,279`（G115，`appendSizedString`/`appendSizedBytes` 内 `int -> uint32`）**：与 v1 不同，这里的 `encodeDBCompatibleMessage` **没有**在拼接各变长字段前做类似 v1 `encodedMessageSize` 的显式长度上限校验，理论上单个字段长度超过 `2^32-1` 字节时会被截断写入错误的长度前缀，属于结构性差异。但触发它需要客户端向 channelv2 提交单条 `payload`/`ClientMsgNo` 等字段本身超过 4GB 的消息——在已确认 channelv2 当前不在生产运行路径上、且 v0 阶段完全没有对外提供网络可达入口（仅 `service.New` 直接构造、由测试/`testkit.ClusterHarness` 驱动）的前提下，找不到任何可达的远程/客户端输入能把单条消息做到 4GB 量级。按"无法写出具体触发路径即删除"的约束，**不作为发现列出**，仅记录该结构性差异供参考。
- **`pkg/channelv2/machine/meta.go` 的 `ApplyMeta`/`clearAppendState`（`s.InflightAppend = nil` 且清空 `PendingAppends` 而不产生任何 `Reply`）**：初看像是"leader 收到会 fence 当前状态的新 meta 时，静默丢弃仍在等待的 append waiter，客户端 future 永远不会完成"的挂起风险。但读取调用方 `pkg/channelv2/reactor/reactor.go:408-430`（`handleApplyMeta`，unit 28，仅作上下文核实）后确认：只要 `metadataWouldFenceState` 判断为真，reactor 在调用 `rc.state.ApplyMeta(event.Meta)` **之前**就已经调用 `r.failPendingAppendWaiters(rc, ch.ErrStaleMeta)` 把所有等待中的 append waiter 以 `ErrStaleMeta` 结束；`machine.ApplyMeta`/`clearAppendState` 里再次清空的只是此时已经不含任何等待者的空 map。**结论：在实际可达的调用路径下不会导致 waiter 悬挂，非发现。**
- **`pkg/channelv2/service/append.go:60-69`（`AppendBatch` 在 `ctx.Done()` 分支里对同一个 `future` 先尝试异步 cleanup 再兜底 `future.Complete(...)`，是否存在与 reactor 正常完成路径的双重 `Complete` 竞态）**：读取 `pkg/channelv2/reactor/future.go`（unit 28，仅作上下文核实）确认 `Future.Complete` 用 `sync.Once` 包裹，第二次及以后的调用是安全的空操作。**结论：非竞态，非发现。**
- **`pkg/channelv2/replication/`（leader.go 9 行 + follower.go 10 行）**：`AckPlan`/`FollowerPlan` 两个纯 DTO 结构体，用 `grep -rn "replication.AckPlan|replication.FollowerPlan|channelv2/replication"` 检索全仓库后确认没有任何文件（包括 channelv2 自身的 `machine/`、`worker/`、`service/`、`reactor/`）导入或引用这个包。已计入下方"整体评价"作为 dead-stub 观察，但因其本身是无逻辑的空结构体、缺乏任何可触发的运行时后果，不单独列为带触发路径的 P0-P2 发现；作为纯 dead code 记录即可（见整体评价）。
- **`pkg/channelv2/testkit/cluster.go` 的 `Close()`（`select{case <-h.stop: default: close(h.stop)}` 在并发多次调用下存在双重 `close` 的经典竞态）**：`grep -rln "testkit.NewClusterHarness|channelv2/testkit" | grep -v _test.go` 确认该文件只被 `_test.go` 测试代码调用，没有任何非测试生产代码路径引用它。按分片说明"不评审 `_test.go` 本身质量"的边界精神，且此文件仅是测试基建的唯一使用者，即使触发也只影响测试运行而非生产/仿真集群，不构成有意义的生产发现。

## 本分片整体评价

`pkg/channel/handler/` 本身确实干净——两把锁的实现（尤其是 `append_idempotency.go` 的排序锁）经复核是正确的，没有发现新问题。真正的风险集中在 `pkg/channel/transport/`：三个二进制编解码文件（`codec.go`、`longpoll_codec.go`、`migration_control.go`）里共 8 处对端可控的长度/计数前缀在做任何"剩余字节是否足够"校验之前就直接喂给 `make()`，其中两处正好落在 `pkg/channel/FLOW.md` 明确标注为"当前唯一复制主路径"的 `LongPollFetch` 双向报文上——这与该分片最初的排查提示（"每个长度前缀都在 make()/索引前做了边界检查"）恰好相反，属于必须现场复核才能发现的反例，是本分片最重要、且完全在生产路径上的 P0 发现。除此之外 transport/session.go 的出站 RPC 均正确设置了超时（`context.Background()+WithTimeout` 是该异步发送模型下的正确用法），连接/goroutine 生命周期在对端丢失时也未见泄漏。channelv2 外围部分（`channel.go`/`types.go`/`errors.go`/`doc.go`/`machine/`/`worker/`/`store/`/`service/`/`transport/`/`replication/`/`testkit/`，合计约 5,900 行）**当前不在生产运行路径上**（`internalv2` 不被任何 `cmd/` 二进制导入），逐文件通读后没有发现独立于该降级前提之外还需要报告的新问题：`store/channel_adapter.go` 是对 `pkg/db/message`（unit 29）的薄适配层而非重复实现存储，`store/memory.go` 是纯测试替身；`replication/` 包（19 行）内的两个结构体在整个仓库范围内确认没有任何引用，是彻底的死代码；`machine/`、`worker/`、`service/` 内看似可疑的几处（meta 变更丢弃 append waiter、`AppendBatch` 取消路径的重复 complete）经追踪到 `reactor/`（unit 28，仅作上下文读取）后均确认被上层正确处理，不构成本分片范围内可独立成立的发现。
