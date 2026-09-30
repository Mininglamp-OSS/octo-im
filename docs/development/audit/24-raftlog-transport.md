# Raft 日志持久化 + 节点间 transport（`pkg/raftlog`, `pkg/transport`）

## 覆盖情况

### pkg/raftlog（非测试 2819 行 / 17 文件）

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/raftlog/pebble_writer.go | 373 | 是 |
| pkg/raftlog/snapshot_store.go | 358 | 是 |
| pkg/raftlog/pebble_reader.go | 337 | 是 |
| pkg/raftlog/pebble_store.go | 327 | 是 |
| pkg/raftlog/snapshot_codec.go | 247 | 是 |
| pkg/raftlog/pebble_db.go | 244 | 是 |
| pkg/raftlog/meta.go | 215 | 是 |
| pkg/raftlog/snapshot_gc.go | 198 | 是 |
| pkg/raftlog/snapshot_manifest.go | 157 | 是 |
| pkg/raftlog/memory.go | 145 | 是 |
| pkg/raftlog/codec.go | 76 | 是 |
| pkg/raftlog/types.go | 69 | 是 |
| pkg/raftlog/scope.go | 34 | 是 |
| pkg/raftlog/testing_hooks.go | 14 | 是 |
| pkg/raftlog/snapshot_rename_linux.go | 9 | 是 |
| pkg/raftlog/snapshot_rename_darwin.go | 9 | 是 |
| pkg/raftlog/snapshot_rename_unsupported.go | 7 | 是 |

### pkg/transport（非测试 1540 行 / 10 文件）

| 文件 | 行数 | 是否通读 |
|---|---|---|
| pkg/transport/pool.go | 519 | 是 |
| pkg/transport/server.go | 249 | 是 |
| pkg/transport/writer.go | 158 | 是 |
| pkg/transport/conn.go | 144 | 是 |
| pkg/transport/client.go | 126 | 是 |
| pkg/transport/frame.go | 116 | 是 |
| pkg/transport/rpcmux.go | 69 | 是 |
| pkg/transport/pending.go | 69 | 是 |
| pkg/transport/types.go | 62 | 是 |
| pkg/transport/errors.go | 28 | 是 |

两个包都**没有** `FLOW.md`（`find pkg/raftlog pkg/transport -name '*.md'` 无结果）。
`AGENTS.md:137` 对 raftlog 的描述（"存储 controller 和 slot 的分布式日志，但不存储 channel 层的"）与代码一致：
`Scope` 只有 `ScopeSlot`/`ScopeController` 两种（`scope.go:7-10`），无 channel scope。

`pkg/raftlog` **在生产运行路径上**：`internal/app/build.go:129` `openRaftLogDB(...)`，
`build.go:1617-1619` `newStorageFactory` → `raftDB.ForSlot(...)`，注入 `raftcluster.Config.NewStorage`。
`pkg/transport` 同理（见下文 T-0）。二者都不是 v2 未上线代码。

---

## 三个跨单元交付项（优先结论）

### D1. 确认：`pkg/raftlog` 实现了**正确的 group commit**，而 `pkg/db` 没有

**确认成立。** `pebble_writer.go:86-151`：

```go
func (db *DB) runWriteWorker() {
	defer db.workerWG.Done()
	for {
		req, ok := <-db.writeCh
		if !ok { return }
		reqs := []*writeRequest{req}
		closed := false
		for {
			select {
			case next, ok := <-db.writeCh:
				if !ok { closed = true; goto flush }
				reqs = append(reqs, next)
			default:
				goto flush
			}
		}
	flush:
		err := db.flushWriteRequests(reqs)
		for _, req := range reqs { req.done <- err; close(req.done) }
		if closed { return }
	}
}
```

```go
func (db *DB) flushWriteRequests(reqs []*writeRequest) error {
	batch := db.db.NewBatch()
	defer batch.Close()
	...
	if err := batch.Commit(pebble.Sync); err != nil { return err }
```

要素齐全：
1. **单一 write worker goroutine**（`pebble_db.go:98-99`，`Open` 里只起一个，`workerWG.Add(1)`）。
2. **机会性非阻塞 drain**（`:97-108` 的 `select` + `default: goto flush`）——只要 channel 里还有堆积的请求
   就继续收，不等待、不引入延迟。
3. **一个 `pebble.Batch`**（`:123`）承载本轮所有逻辑写（可跨多个 Scope，见 `:126-137` 的
   `stateCache map[Scope]*scopeWriteState`）。
4. **一次 `Commit(pebble.Sync)`**（`:144`）——即一次 fsync 摊销到 N 个逻辑写。
5. 队列深度 1024（`types.go:14` `defaultWriteChSize = 1024`），即单次 fsync 最多可摊销 1024 个 raft 写。

**对 unit 29 的结论**：这个仓库里**已经存在**正确的 group-commit 模式，并且是在同一个团队、同一个
Pebble 版本、同一类 durability 需求（raft 日志持久化）下写出来的。因此 `pkg/db` 逐条逻辑写各自
`Commit(Sync)`、而 `pkg/db/internal/commit/coordinator.go` 这个专门的 group-commit 协调器零非测试
调用方，**不是"缺少一个还没人想到的优化"，而是"正确模式在本仓已实现、已验证、但没有被应用到
`pkg/db`"**。严重度应据此上调。

### D2. 死掉的 snapshot 校验簇 —— **纠正预判：不是 durability 漏洞**

staticcheck 报的 4 条 U1000 我逐条核实，结论是**没有任何一条代表校验缺失**：

| 位置 | 结论 |
|---|---|
| `pebble_reader.go:46` `loadSnapshotManifest` | 只是 `loadSnapshotManifestFrom(ctx, s.db.db)` 的一行 wrapper（`:47`）。被调用的 `loadSnapshotManifestFrom`（`:50`）**是活的**：`loadSnapshotMetaViewFrom:294` 调用它。死的只有 wrapper。|
| `pebble_reader.go:150` `loadMeta` | 同上，是 `loadMetaFrom(s.db.db)` 的 wrapper；`loadMetaFrom` 在 `loadSnapshotMetaViewFrom:287` 活跃使用。|
| `snapshot_codec.go:15` `hasSnapshotManifestMagic` | **magic 校验并没有丢**——它被 inline 在活路径的 `decodeSnapshotManifest:65-69` 里（见下）。这个 helper 是冗余重复实现。|
| `snapshot_store.go:20` `errSnapshotNoOverwriteRenameUnsupported` | **staticcheck 误报**：它被 `snapshot_rename_unsupported.go:6` 使用，而该文件的 build tag 是 `//go:build !darwin && !linux`，staticcheck 在 darwin/linux 上跑自然看不到。|

活路径实际校验什么（`pebble_reader.go:69→81` `loadSnapshot` → `snapshotStore.read`）：

```go
// snapshot_codec.go:63-69
func decodeSnapshotManifest(scope Scope, data []byte) (SnapshotManifest, error) {
	decoder := manifestDecoder{data: data}
	if magic, err := decoder.read(4); err != nil {
		return SnapshotManifest{}, err
	} else if string(magic) != snapshotManifestMagic {
		return SnapshotManifest{}, errors.New("raftstorage: invalid snapshot manifest magic")
	}
```

完整校验链：magic（`:65-69`）→ 每个变长字段都走带边界检查的 `manifestDecoder`（`:189-195`
`d.remaining() < size` 即报 "short snapshot manifest data"）→ `validateSnapshotManifestShape`
（`:153-168`，校验 ChunkSize≠0、TotalSize ≤ maxInt、ChunkCount 必须等于
`manifestChunkCount(TotalSize, ChunkSize)` 重算值、checksumCount == ChunkCount）→ 尾部多余字节报错
（`:144-146`）→ `manifest.Validate(scope)`（`snapshot_manifest.go:57-107`：version、scope kind/ID 必须
与调用方 scope 一致、Index/Term 非零、SnapshotID 必须匹配 `^snap-[0-9a-f]{16}-[0-9a-f]{16}-[0-9a-f]{16}$`
且不含路径分隔符、ChecksumType 必须是 crc32c、WholeChecksum 长度必须 4、每个 chunk checksum 长度必须 4）。

payload 侧（`snapshot_store.go:196-233` `read`）：每个 chunk 文件**实际大小必须精确等于**
`expectedChunkSize`（`:311` `uint64(info.Size()) != expectedSize` → 报错），读完后再读 1 字节确认 EOF
（`:318-322`），每个 chunk 校验 CRC32C（`:214-216`），拼装后再校验总长度（`:219`）和整体 CRC32C（`:222`）。

**结论：截断或损坏的 snapshot 无法被静默加载。** 这条候选发现不成立，不写入"发现"。

### D3. `pkg/transport` 安全姿态 —— **两点全部确认属实**

（a）**零认证、零 TLS。** 我独立复核：

```
$ grep -nE 'tls\.|Token|auth|Secret|Credential' pkg/transport/*.go | grep -v _test
（无输出）
$ grep -rn "crypto/tls\|x509" pkg/transport/
（无输出）
```

`client.go:31` 是裸 `net.Dial("tcp", addr)`（经 `defaultDialer`），`server.go:96` 是裸
`net.Listen("tcp", addr)`。握手阶段（`conn.go`）只交换一个 4 字节 magic + 版本 + nodeID，
**没有任何 challenge/response、共享密钥或证书校验**。任何能连到节点 RPC 端口的人都可以冒充任意
`nodeID`（见下文 T-1）。

（b）**`server.go:186` 的 `go s.handleRPCRequest(...)` 没有任何 `recover()`。** 复核：

```
$ grep -n "recover()" pkg/transport/*.go | grep -v _test
（无输出）  ← 整个包，非测试文件，零个 recover
```

```go
// pkg/transport/server.go:182-188
	case FrameTypeRPCRequest:
		req, err := DecodeRPCRequest(frame.Payload)
		if err != nil {
			return err
		}
		go s.handleRPCRequest(sc, req)
	case FrameTypeRPCResponse:
```

**这两点对兄弟单元是 load-bearing 的**：`internal/access/node/delivery_control_codec.go:48` 的
无上界 `make()` 和 `pkg/cluster/codec_control.go:2784` 的 `int(length)` 绕过，
其 panic 发生在 `handleRPCRequest` 派生的 goroutine 里。Go 里**未被 recover 的 goroutine panic 会
终止整个进程**——没有 per-connection 的隔离边界能拦住它。再加上 (a) 的零认证，这意味着
**任何能路由到节点 RPC 端口的攻击者，无需任何凭证即可远程杀死整个 IM 节点进程**。两个 P0 的
"可远程触发 + 进程级崩溃"定性成立。

---

## 发现

### [P0] 1. `pkg/transport` 无认证 + 读帧先分配后读取 → 500 字节流量远程耗尽节点内存（OOM）

- **位置**：`pkg/transport/frame.go:57-80`（分配点 `:74`）；`pkg/transport/server.go:96-104,125-163`（无认证、无连接数上限）
- **类别**：安全 / 资源（远程可触发）
- **代码**：
  ```go
  // pkg/transport/frame.go:66-79
  	bodyLen := binary.BigEndian.Uint32(hdr[1:5])
  	if bodyLen > MaxFrameSize {                      // MaxFrameSize = 64MB
  		return 0, nil, nil, fmt.Errorf("%w: %d bytes", ErrMsgTooLarge, bodyLen)
  	}
  	if bodyLen == 0 { return msgType, nil, func() {}, nil }

  	buf, rel := frameSlabPool.get(int(bodyLen))      // ← 先按声明长度分配
  	body = buf[:bodyLen]
  	if _, err = io.ReadFull(r, body); err != nil {   // ← 才开始读 body
  ```
  `slabPool.get` 只对 ≤512 / ≤4096 / ≤65536 三档走 `sync.Pool`；超过 65536 走
  `buf := make([]byte, n)`（`frame.go:44`），即**每帧现场分配，不复用**。
- **触发路径**：
  1. 攻击者 TCP 连到集群 RPC 端口（`server.go:97` `net.Listen("tcp", addr)`）。
     `serveConn`（`:143-163`）**没有任何握手/认证**，直接 `newMuxConn(raw, dispatch, ...)`，
     整个包 `grep -nE 'tls\.|Token|auth|Secret|Credential'` 零命中。
  2. 只发 5 字节头：`msgType=0x01`, `bodyLen=0x04000000`（= 67108864，正好等于 MaxFrameSize，不触发上限）。
  3. 服务端 `readFrame` 立刻 `make([]byte, 67108864)` = 64MB，然后阻塞在 `io.ReadFull`。
  4. 攻击者**不再发任何字节**。全包**没有任何 read/write deadline**
     （`grep -rn "SetReadDeadline\|SetWriteDeadline\|SetDeadline" pkg/transport/` 只在 `_test.go` 里命中），
     `acceptLoop` 也**没有连接数上限**（`server.go:125-141`），所以这块 64MB 永久驻留。
  5. 开 100 条连接、每条发 5 字节 → 6.4 GB 常驻堆；开 1000 条 → 64 GB。总攻击流量 5KB。
- **后果**：**无需任何凭证即可远程 OOM 杀死整个 IM 节点进程**。同时死连接永不回收（无 deadline），
  goroutine（readLoop + writer loop 各一个）和三条写队列（`writer.go:40-46`）一并泄漏。
- **建议**：握手阶段加节点身份认证（共享密钥或 mTLS）；读帧改成"按实际读入量增量扩容 / 分块读"或
  至少在分配前设置 read deadline；给 listener 加最大连接数与 per-peer 配额。

### [P0] 2. `Server` 把 RPC handler 派到裸 goroutine 且全包零 `recover()` → 下游任意 panic 直接杀进程

- **位置**：`pkg/transport/server.go:177-194`（派发点 `:186`），`server.go:196-224`
- **类别**：安全 / 健壮性（远程可触发）
- **代码**：
  ```go
  // pkg/transport/server.go:177-186
  	case MsgTypeRPCRequest:
  		holder := s.rpcHandler.Load()
  		if holder == nil || holder.handler == nil { release(); return }
  		copied := append([]byte(nil), body...)
  		release()
  		s.wg.Add(1)
  		go s.handleRPCRequest(connCtx, mc, holder.handler, copied)
  ```
  ```go
  // pkg/transport/server.go:203-211
  func (s *Server) handleRPCRequest(ctx context.Context, mc *MuxConn, handler RPCHandler, body []byte) {
  	defer s.wg.Done()
  	if len(body) < 8 { return }
  	requestID := binary.BigEndian.Uint64(body[0:8])
  	payload := body[8:]
  	respData, err := handler(ctx, payload)   // ← 用户 handler，无 recover 包裹
  ```
  复核：`grep -rn "recover()" pkg/transport/` 在非测试文件中**零命中**；
  `handleRPCNotify`（`:196-201`）同样是裸 goroutine。
- **触发路径**：攻击者（同 P0-1，无需认证）发一个 `MsgTypeRPCRequest` 帧，payload 走
  `RPCMux.HandleRPC`（`rpcmux.go:42-55`）路由到注册的业务 handler
  （`pkg/cluster/transport_glue.go:54-59` 注册了 raft / raftBatch / forward / controller / managedSlot）。
  只要任一业务解码器 panic，该 panic 发生在 `go s.handleRPCRequest` 派生的 goroutine 里，
  **Go 运行时对未 recover 的 goroutine panic 一律终止整个进程**——没有 per-connection 隔离边界。
- **后果**：这是**两个兄弟单元 P0 的放大器**：
  `internal/access/node/delivery_control_codec.go:48` 的无上界 `make()` 和
  `pkg/cluster/codec_control.go:2784` 的 `int(length)` 守卫绕过，正因为此处缺少 recover
  才从"单连接错误"升级为"整进程崩溃"。任何编解码层的边界疏漏都直接等价于远程 DoS。
- **建议**：在 `handleRPCRequest` / `handleRPCNotify` / `dispatch` 的 `default` 分支统一加
  `defer func(){ if r:=recover(); r!=nil { log + 关闭该连接 } }()`，把故障域收敛到单连接。

### [P1] 3. 一个 scope 的持久化元数据损坏会毒化整批 group commit，拖垮同节点全部 slot 的 raft 写入

- **位置**：`pkg/raftlog/pebble_writer.go:122-137`
- **类别**：分布式一致性 / 可用性
- **代码**：
  ```go
  func (db *DB) flushWriteRequests(reqs []*writeRequest) error {
  	batch := db.db.NewBatch()
  	defer batch.Close()

  	stateCache := make(map[Scope]*scopeWriteState, len(reqs))
  	for _, req := range reqs {
  		state, err := db.loadScopeWriteState(stateCache, req.scope)
  		if err != nil {
  			return err                       // ← 整批放弃，所有 scope 一起失败
  		}
  		if req.op != nil {
  			if err := req.op.apply(batch, state, &pebbleStore{db: db, scope: req.scope}); err != nil {
  				return err                   // ← 同上
  			}
  		}
  	}
  ```
  返回的 `err` 被 `runWriteWorker:112-115` 原样发给**本批全部** `req.done`。
- **触发路径**：
  1. 节点上 slot 7 的 Pebble 元数据与 snapshot manifest 不一致（例如崩溃在
     `publishSnapshotAndCommit` 发布目录之后、Pebble 提交之前——`pebble_store.go:263` 的注释
     明确承认这种中间态"留给 retry/GC"）。
     `loadScopeWriteState:167` → `loadSnapshotMetaView` → `validateManifestMetaConsistency`
     （`pebble_reader.go:304-321`）返回 `"raftstorage: snapshot manifest and metadata mismatch"`。
  2. `loadScopeWriteState` 失败时**不写任何缓存**，所以这个错误是**永久性、每次必现**的。
  3. slot 7 的 raft loop 在 `processReady` 里失败后**不 Advance、下一 tick 重放同一个 Ready**
     （`pkg/slot/multiraft/slot.go:326-329`），于是 slot 7 每个 tick 都往 `writeCh` 投一次。
  4. 该节点还有其它 slot + controller 在写。`writeCh` 深度 1024（`types.go:14`），
     worker 的机会性 drain（`pebble_writer.go:97-108`）**必然**把 slot 7 的请求和别人的请求装进同一批。
  5. 每一批只要含 slot 7 → 整批返回 mismatch 错误 → 同批的其它 slot 与 controller
     全部收到与自己无关的错误，各自 `g.failPending(err)`（`slot.go:327`）把**正常的客户端提案全部判失败**。
- **后果**：单个 slot 的元数据损坏被 group commit 放大成**全节点控制面写入雪崩**——
  正确的提案被判失败、commit index 停止推进、slot 无法选主/迁移。
  这是 group commit 正确实现（见 D1）之上的一个隔离性缺陷：批内没有 per-request 故障隔离。
- **建议**：`flushWriteRequests` 改为 per-request 隔离——把失败的 request 从本批剔除并只给它回错误，
  其余请求正常提交；`loadScopeWriteState` 失败的 scope 应被标记熔断而不是无限重试。

### [P1] 4. `DB.Close()` 把 `db.db` 置 nil 却不等待读者 → 关机期任意读路径 nil 解引用崩溃（且是数据竞争）

- **位置**：`pkg/raftlog/pebble_db.go:136-159`（关键行 `:153-157`）
- **类别**：资源 / 并发（use-after-close）
- **代码**：
  ```go
  	db.mu.Unlock()

  	db.workerWG.Wait()   // 只等 write worker
  	db.gcWG.Wait()       // 只等 snapshot GC

  	err := db.db.Close()
  	db.db = nil          // ← 无锁写；读路径全都直接用 s.db.db
  	return err
  ```
  所有读方法都无条件解引用该字段，且**没有任何 closing 检查**：
  `pebble_reader.go:171` `readState := s.db.db.NewSnapshot()`、
  `:229` `s.db.db.Set(...)`、`:233` `s.loadEntriesFrom(s.db.db, lo, hi)`、`:34`、`:105`、`:123`、`:138`、
  `pebble_reader.go:281`。只有写路径 `submitWrite`（`pebble_writer.go:76-80`）检查了 `db.closing`。
- **触发路径**：
  1. `internal/access/manager/controller_raft_status.go:113` 是一个 **HTTP 管理端点**，
     经 `pkg/cluster/controller_raft_status.go:120-133` 在 **HTTP handler goroutine** 上调用
     `storage.InitialState(ctx)` / `FirstIndex` / `LastIndex` / `Term` —— 与 raft loop 完全不同的 goroutine。
     （同类路径还有 `pkg/cluster/slot_log_entries.go:92-96`、`controller_log_entries.go:97-101`。）
  2. 进程收到关机信号。`internal/app/lifecycle.go:53-60`：
     ```go
     stopCtx, cancel := context.WithTimeout(context.Background(), apiStopTimeout)  // 5s
     err = errors.Join(
         a.stopLifecycleManager(stopCtx),
         ..., a.closeRaftDB(), ...)
     ```
     `apiStopTimeout = 5 * time.Second`（`lifecycle.go:12`）。`stopLifecycleManager` **超时即返回错误**
     （`lifecycle.go:81-88`），`errors.Join` 的实参求值继续，`closeRaftDB()` 照样执行。
  3. 于是一个还没退出的 HTTP 请求（或任何慢下游）在 `db.db = nil` 之后执行
     `s.db.db.NewSnapshot()` → **nil 指针解引用 panic，进程带 stack trace 崩溃**。
     即便赶在置 nil 之前进入，`pebble.DB` 已 `Close()`，Pebble 对已关闭 DB 的操作是 panic 而非返回 error。
  4. 此外 `db.db = nil` 是**无同步写**，与上述所有 `s.db.db` 读构成教科书级 data race（`-race` 必报）。
- **后果**：优雅关机变成崩溃退出（退出码非 0、可能中断 Pebble 的正常收尾），
  在 k8s 下表现为关机阶段 CrashLoop 事件，掩盖真实的关机失败原因。
- **建议**：`Close()` 用 `sync.RWMutex` 或 `atomic.Pointer[pebble.DB]` 保护 `db.db`，读路径统一走一个
  `acquireDB() (*pebble.DB, error)` 在 closing 时返回 `ErrClosed`；并在关闭前用引用计数
  等待所有 in-flight 读退出，而不是只等 worker 和 GC。

### [P1] 5. 发现 discovery 失败后，并发的 `Pool.acquire` 可能陷入"拨号→立刻关闭→再拨号"的无退避热循环

- **位置**：`pkg/transport/pool.go:170-280`（关键行 `:240-246` 与 `:274-277`）
- **类别**：资源泄漏 / 并发
- **代码**：
  ```go
  // pool.go:240-247  —— 解析失败时把 set 从 p.nodes 摘除，但【没有】设置 set.evicted
  		addr, resolveErr := p.cfg.Discovery.Resolve(nodeID)
  		if resolveErr != nil {
  			if !set.finishSlotDial(slot, nil, nil, ready) { continue }
  			p.nodes.CompareAndDelete(nodeID, set)
  			return nil, resolveErr
  		}
  ```
  ```go
  // pool.go:271-278
  		if !set.finishSlotDial(slot, mc, nil, ready) { continue }
  		if current, ok := p.nodes.Load(nodeID); !ok || current != set || mc.closed.Load() {
  			mc.Close()
  			continue          // ← 回到 :179 的 for，但 set 永不重取
  		}
  		return mc, nil
  ```
- **触发路径**：
  1. goroutine G1 与 G2 同时对 nodeID=5 调 `acquire`，在 `:174` 各自拿到**同一个** `set` 指针。
  2. G1 的 `Discovery.Resolve(5)` 瞬时失败（节点正在下线 / 配置刚变更），执行
     `p.nodes.CompareAndDelete(5, set)` 后返回。此时 `set` 已不在 map 中，但 `set.evicted` **仍是 false**
     （只有 `evictAndClose:428` 才会置位，这条路径不走它；`getOrCreateNodeSet:372,388` 的摘除也都以
     `evicted==true` 为前提）。
  3. G2 继续循环：`:183` `set.evicted.Load()` 为 false → **不会重新取 set**；
     `:192` slot.conn 为 nil → 走到 `:255` 拨号成功 → `:271` `finishSlotDial` 把 mc 存进 `slot.conn`
     → `:274` `p.nodes.Load(5)` 要么 `!ok`、要么是别的 goroutine 新建的 set → `current != set`
     → `mc.Close(); continue`。
  4. 下一圈：`:192` `slot.conn.Load()` 拿到刚关掉的 mc，`mc.closed` 为 true → `:200-202` `clearClosedConn`
     置空 → 再拨号 → 再关闭 → **无限循环，全程没有任何 sleep/backoff**
     （`poolDialCooldown` 只在 `waiterOrCooldown:488` 对 **dialErr** 生效，这里拨号是成功的）。
  5. 退出条件只有 `:180` 的 `ctx.Err()`。但 `Pool.Send`（`pool.go:73-75`）用的是
     `context.Background()`，**永不取消**；`pkg/cluster` 的 raft 发送走的就是 `Client.Send` → `Pool.Send`。
- **后果**：一个 goroutine 100% 占满一核，并对目标节点持续发起完整 TCP 握手后立刻关闭——
  对自己是 fd 抖动 + CPU 打满，对对端是连接风暴；且该 goroutine 永远不返回，raft 发送路径永久卡住。
- **建议**：`:245` 这条摘除路径也要 `set.evicted.Store(true)`（或统一走 `evictAndClose`），
  让 `:183` 能感知并重取 set；同时给 `:274` 的重试加计数上限与指数退避。

### [P1] 6. `snapshotLifecycleMu` 是全局锁，却被 GC 全键空间扫描和快照发布的 fsync 长时间持有

- **位置**：`pkg/raftlog/snapshot_gc.go:16-69`（`:24-25` 加锁，`:43-45` 全量迭代）；
  `pkg/raftlog/pebble_store.go:251-285`（`:254` 加锁 … `:280` 才提交）
- **类别**：性能 / 并发
- **代码**：
  ```go
  // snapshot_gc.go:24-38
  	db.snapshotLifecycleMu.Lock()
  	defer db.snapshotLifecycleMu.Unlock()
  	...
  	live, err := db.liveSnapshotPathsLocked()
  	return db.collectSnapshotGarbageLocked(ctx, live, time.Now())
  ```
  ```go
  // snapshot_gc.go:43-56  —— 上界是整个 format-version 前缀，即【全库所有 key】
  	iter, err := db.db.NewIter(&pebble.IterOptions{
  		LowerBound: []byte{currentFormatVersion},
  		UpperBound: nextPrefix([]byte{currentFormatVersion}),
  	})
  	...
  	for valid := iter.First(); valid; valid = iter.Next() {
  		key := iter.Key()
  		if !isSnapshotManifestKey(key) { continue }
  ```
  ```go
  // pebble_store.go:254-280
  	db.snapshotLifecycleMu.Lock()
  	...
  	if err := db.snapshotStore.publishFinal(staged); err != nil { ... }   // rename + fsync 目录
  	...
  	err := db.submitWrite(req)                                            // 排队 + 一次 fsync commit
  ```
- **触发路径**：
  1. `Save` 每成功写一次快照就无条件启动一次 GC：`pebble_store.go:170-172`
     `if err == nil { s.db.startSnapshotGC() }`。多个 slot 各自周期性 compact → GC 被高频触发。
  2. 每次 GC 持有 `snapshotLifecycleMu`，迭代**全库每一个 key**（包括所有 slot 的所有 raft entry）
     只为挑出 11 字节的 manifest key（`isSnapshotManifestKey:71-73`），
     然后对每个 scope 目录做 `os.ReadDir`，并对过期目录做 `os.RemoveAll`（可能是 GB 级快照目录）——
     **全部在锁内**。
  3. 同一把锁被**每一次快照读**获取：`pebble_reader.go:85-86`
     `s.db.snapshotLifecycleMu.Lock(); defer ...Unlock()`（`loadSnapshotManifestAndRegisterActive`）。
  4. 结果：任一 slot 的 GC 扫描期间，**所有** scope 的 `Snapshot()` 读（follower 追赶时要用）
     和所有快照发布全部串行阻塞。
- **后果**：快照相关操作在大日志节点上退化为 O(全库 key 数) × 全局串行；follower 通过快照追赶被拖慢，
  延长不可用窗口。`publishSnapshotAndCommit` 还把 fsync 和写队列等待也圈进了同一把全局锁。
  （我逐条验证过**不构成死锁**：write worker 路径 `flushWriteRequests` → `loadScopeWriteState` →
  `loadSnapshotMetaView` 从不获取 `snapshotLifecycleMu`。）
- **建议**：GC 的 manifest 扫描改成按 scope 前缀的定向迭代（key 结构 `codec.go:28-33` 已支持）
  并移到锁外，锁只保护 `activeSnapshotPaths` 的读写；`os.RemoveAll` 一律在锁外执行。

### [P1] 7. `db.stateCache` 与本批工作副本共享 entries 底层数组，提交失败会留下被污染的缓存

- **位置**：`pkg/raftlog/pebble_writer.go:147-149,158-164,196-213` + `pkg/raftlog/meta.go:60-84`
- **类别**：正确性 / 数据一致性
- **代码**：
  ```go
  // pebble_writer.go:158-164
  	if cached, ok := db.stateCache[scope]; ok {
  		// Save operations replace the entries slice copy-on-write, so cached
  		// entry payloads can be retained without deep-copying every append.
  		state := cloneScopeWriteState(cached, false)   // ← cloneEntries = false
  ```
  ```go
  // pebble_writer.go:209-211
  	} else {
  		cloned.entries = state.entries                 // ← 直接共享底层数组
  	}
  ```
  ```go
  // meta.go:68-76  —— 注释说的 copy-on-write 并不成立：这里是【就地】覆写
  	oldLen := len(existing)
  	result := existing[:cut]
  	for _, entry := range incoming {
  		result = append(result, cloneEntry(entry))     // cut < oldLen 时写进共享数组
  	}
  	if len(result) < oldLen {
  		clearEntries(existing[len(result):oldLen])     // 把共享数组尾部清零
  	}
  ```
  （`cloneScopeWriteState` 的 `cloneEntries=true` 分支在整个非测试代码里**无任何调用方**：
  `pebble_writer.go:148` 和 `:161` 两处都传 `false`。）
- **触发路径**：
  1. scope S 的缓存 entries 是 `[5,6,7,8]`（len=4，底层数组 A）。
  2. 新 leader 上任，发来冲突覆盖：`saveOp` 携带 entries 从 index 6 开始、只有 2 条 `[6',7']`
     （raft 正常的冲突截断，`persistReady` → `Save` → `submitWrite`）。
  3. `saveOp.apply:281` → `replaceEntriesFromIndex(state.entries, 6, [6',7'])`：
     `cut=1` → `result = A[:1]` → append 两次**直接写 A[1]、A[2]** → `len(result)=3 < oldLen=4`
     → `clearEntries(A[3:4])` 把 A[3] 清成零值 Entry。
     此刻 `db.stateCache[S].entries` 仍是 `A[0:4]`，内容已变成 `[5, 6', 7', {Index:0,Term:0}]`。
  4. `batch.Commit(pebble.Sync)`（`:144`）**失败**（磁盘满 / EIO / Pebble 后台错误）→ 直接 `return err`，
     `:147-149` 的缓存回写被跳过，**被污染的 `db.stateCache[S]` 原样保留**。
  5. 该 scope 下一次写入成功时，`loadScopeWriteState:158` 命中这份脏缓存，
     `updateLogMeta`（`meta.go:202-212`）用 `entries[len(entries)-1].Index` 算 LastIndex
     → **`meta.LastIndex = 0`**，随后被 `store.setMeta`（`pebble_writer.go:298`）持久化。
  6. 重启：`pkg/slot/multiraft/ready.go:36` 的 `if last >= first && last > 0` 判定为假
     → **一条 entry 都不加载进 raft MemoryStorage**；而 `HardState.Commit` 仍是真实的提交位点，
     `memory.SetHardState`（`ready.go:49`）之后 etcd raft 在 `commitTo` 处
     panic `"tocommit(N) is out of range [lastIndex(0)]. Was the raft log corrupted, truncated, or lost?"`。
- **后果**：一次可恢复的磁盘写错误被放大成**该 raft group 的持久化元数据损坏**，
  重启后要么 panic 要么丢失已提交日志。注释里宣称的 "copy-on-write" 与实现不符。
- **建议**：提交失败时必须显式作废该批涉及的所有 scope 缓存（`delete(db.stateCache, scope)`），
  或在 `loadScopeWriteState` 命中缓存时对 entries 做 `cloneEntries=true` 的深拷贝。

### [P2] 8. 发送侧从不校验 `MaxMessageSize`，超限消息由对端撕连接，连带打断该连接上全部在途 raft 消息与 RPC

- **位置**：`pkg/transport/frame.go:50-55,83-92`；`pkg/transport/errors.go:10,15-19`
- **类别**：健壮性 / 正确性
- **代码**：
  ```go
  // frame.go:50-55  —— 只做截断，不做上限校验
  func encodeHeader(msgType uint8, bodyLen int) [HeaderSize]byte {
  	var hdr [HeaderSize]byte
  	hdr[0] = msgType
  	binary.BigEndian.PutUint32(hdr[1:], uint32(bodyLen))
  	return hdr
  }
  ```
  `grep -rn "ErrMsgTooLarge" pkg/transport/` 非测试命中只有 `errors.go:10`（定义）和
  `frame.go:68`（**读侧**）——发送侧 `MuxConn.Send`/`priorityWriter.enqueue`/`writeFrame`/
  `appendWriteFrame` 全都不校验。
- **触发路径**：业务侧（如 `pkg/cluster` 的 `msgTypeRaftBatch` 批量 raft 消息、forward 大消息）
  构造一个 body > 64MB 的消息 → `MuxConn.Send` 返回 nil（"成功"）→
  `priorityWriter.loop:106-110` 把它和**同批最多 63 条其它消息**一起 `bufs.WriteTo(conn)` →
  对端 `readFrame:67-69` 判定 `bodyLen > MaxFrameSize` 返回 `ErrMsgTooLarge`
  → 对端 `readLoop:91-97` 直接 return → 连接关闭 → 本端 `failAllPending:137-143`
  把该连接上**所有**在途 RPC 判失败。
- **后果**：一条超大消息导致整条复用连接被拆，连带影响该连接上所有 slot 的 raft 心跳/日志复制与所有 RPC；
  且调用方拿到的是 nil（误以为发送成功），真正的原因 `ErrMsgTooLarge` 永远不会返回给它。
- **建议**：在 `MuxConn.Send` / `priorityWriter.enqueue` 入口校验 `len(body) > MaxMessageSize` 并返回
  `ErrMsgTooLarge`，让调用方自己分片。

### [P2] 9. `acceptLoop` 在非停止态的 `Accept()` 错误上裸 `continue`，无退避 → fd 耗尽时 CPU 打满

- **位置**：`pkg/transport/server.go:125-141`
- **类别**：健壮性 / 性能
- **代码**：
  ```go
  	for {
  		raw, err := s.listener.Accept()
  		if err != nil {
  			select {
  			case <-s.stopCh:
  				return
  			default:
  				continue          // ← 无 sleep、无退避、无错误分类
  			}
  		}
  ```
- **触发路径**：进程 fd 用尽（正是 P0-1 那种连接不回收的场景，或仅仅是并发连接数上升）→
  `Accept` 持续返回 `EMFILE: too many open files` → 该 goroutine 以满速空转，
  每秒数百万次 `accept(2)` 系统调用。
- **后果**：在最需要资源的时刻额外吃满一个核，并用系统调用风暴挤占其它 goroutine，加速雪崩。
- **建议**：区分临时错误并做指数退避（`net/http.Server.Serve` 的 5ms→1s 做法）；不可恢复错误则退出并上报。

### [P2] 10. `submitWrite` 在持有全局 `db.mu` 的情况下向 `writeCh` 阻塞发送

- **位置**：`pkg/raftlog/pebble_writer.go:75-84`
- **类别**：性能 / 并发
- **代码**：
  ```go
  func (db *DB) submitWrite(req *writeRequest) error {
  	db.mu.Lock()
  	if db.closing {
  		db.mu.Unlock()
  		return writeNotEnqueuedError{cause: errDBClosing}
  	}
  	db.writeCh <- req      // ← 队列满时在持锁状态下阻塞
  	db.mu.Unlock()
  	return <-req.done
  }
  ```
- **触发路径**：写入速率超过单 worker 的 fsync 吞吐 → `writeCh`（深度 1024，`types.go:14`）填满 →
  某个 slot 的 raft loop 持有 `db.mu` 阻塞在发送上 → 其余所有 slot 的 `submitWrite`、
  `Close()`（`pebble_db.go:141`）、`startSnapshotGC()`（`pebble_db.go:179`）全部堵在 `db.mu` 上。
- **后果**：背压从"队列排队"退化为"全局串行 + lock convoy"，写放大时尾延迟急剧恶化；关机也要等到队列排空。
  （验证过**不构成死锁**：worker 回写的 `req.done` 在三个构造点
  `pebble_store.go:166,169,186` 都是 `make(chan error, 1)` 有缓冲，worker 不会被回写阻塞。）
- **建议**：closing 检查改成 `atomic.Bool` + `select { case db.writeCh <- req: case <-db.closedCh: }`，
  不要在持锁状态下做 channel 发送。

### [P2] 11. `Pool.Close()` 没有终态，关停后的 `Send` 会重新建立连接并永久泄漏

- **位置**：`pkg/transport/pool.go:112-117,403-412`
- **类别**：资源泄漏
- **代码**：
  ```go
  func (p *Pool) Close() {
  	p.nodes.Range(func(_, value any) bool {
  		value.(*nodeConnSet).close()   // ← 只关连接
  		return true
  	})
  }
  ```
  ```go
  func (s *nodeConnSet) close() {
  	for i := range s.slots {
  		if mc := s.slots[i].conn.Load(); mc != nil {
  			mc.Close()                 // ← 不置 evicted、不清 slot.conn、不从 p.nodes 摘除
  		}
  	}
  }
  ```
  对比同文件的 `evictAndClose:423-437`（会 `s.evicted.Swap(true)` + `closeAndClear()`）——
  `Close()` 走的是弱化版。`Pool` 结构体（`:28-33`）也**没有任何 closed/stopped 字段**。
- **触发路径**：
  1. `Client.Stop()`（`client.go:77-81`）→ `pool.Close()`。
  2. 关机竞态下仍有在途 goroutine 调 `Client.Send` → `Pool.Send` → `acquire`。
  3. `acquire:183` `set.evicted` 仍是 false → `:192` `slot.conn.Load()` 拿到刚关闭的 mc、
     `mc.closed` 为 true → `:200-202` `clearClosedConn` 清空 → `:255` **重新拨号建立新 TCP 连接**
     → `:271` 存回 slot → 返回给调用方。
  4. 这条新连接**再也没有人关它**（`Close()` 已经执行完，`stopOnce` 之类的保护不存在），
     其 readLoop + writer loop 两个 goroutine 一并常驻。
- **后果**：关机不干净——残留 TCP 连接与 goroutine；在测试/嵌入式复用场景（每次重启 Pool）下会累积。
  与 P1-5 叠加时更糟：关停后的 `acquire` 有机会落进那个热循环。
- **建议**：给 `Pool` 加 `closed atomic.Bool`，`Close()` 走 `evictAndClose` 并在 `acquire` 入口检查，
  关停后直接返回 `ErrStopped`。

### [P3] 12. 死代码：4 处 U1000 中 3 处为真死代码，1 处为 staticcheck 误报

- **位置**：`pkg/raftlog/pebble_reader.go:46`、`pkg/raftlog/pebble_reader.go:150`、
  `pkg/raftlog/snapshot_codec.go:15`；另 `pkg/raftlog/scope.go:25-33`
- **类别**：架构 / 死代码
- **代码**：
  ```go
  // snapshot_codec.go:14-17  —— 与 decodeSnapshotManifest:65-69 的内联校验重复实现
  // hasSnapshotManifestMagic reports whether data is an external snapshot manifest candidate.
  func hasSnapshotManifestMagic(data []byte) bool {
  	return len(data) >= len(snapshotManifestMagic) && string(data[:len(snapshotManifestMagic)]) == snapshotManifestMagic
  }
  ```
- **触发路径**：不涉及运行时。`loadSnapshotManifest:46` 和 `loadMeta:150` 是各自 `*From` 版本的
  一行 wrapper，`*From` 版本活跃在用（`loadSnapshotMetaViewFrom:287,294`），wrapper 零调用方。
  `Scope.String()`（`scope.go:25`）在整个仓库零调用方
  （`grep -rn "\.String()" pkg/raftlog/` 只命中定义本身）——它构造的 `"slot/%d"` 字符串
  **不参与任何 key 或路径构造**（key 走 `codec.go:21-26` 的二进制编码，
  快照路径走 `snapshot_store.go:235-244` 的 `scopeDir`）。
- **后果**：无运行时影响；`hasSnapshotManifestMagic` 会让读者误以为存在独立的 magic 探测层。
- **建议**：删除三处死函数；`Scope.String()` 若保留应注明仅用于日志。

### [P3] 13. 快照 scope 目录以 0755 创建（gosec G301）

- **位置**：`pkg/raftlog/snapshot_store.go:112-117`
- **类别**：安全（轻微）
- **代码**：
  ```go
  	if err := os.MkdirAll(filepath.Dir(staged.tmpDir), 0o755); err != nil {
  		return err
  	}
  	if err := os.Mkdir(staged.tmpDir, 0o700); err != nil {
  ```
- **触发路径**：同机的非特权用户可以 `ls` 快照根目录下的 `slot-N` / `controller-N` 目录，
  枚举出每个 raft group 的 snapshot ID（其中编码了 index 和 term：
  `snap-%016x-%016x-%s`，`snapshot_store.go:80`）。
- **后果**：仅元信息泄露（各 raft group 的日志进度）。**负载内容不泄露**——
  `staged.tmpDir` 是 0700（`:115`），chunk 文件是 0600（`:281`），rename 后 finalDir 继承 0700。
- **建议**：父目录改 0750。

---

## 已排除的候选项

### 关键的"看起来像 P0，实际安全"三条

- **`pkg/raftlog/pebble_store.go:222-249` `prepareAndWriteSnapshot` 的 `for { ... continue }`** ——
  初看像"finalDir 已存在就永远 `continue`"的无限循环 + 每圈泄漏一个 tmp 目录（崩溃在
  publishFinal 之后、Pebble 提交之前正好制造出"finalDir 存在但 manifest 不存在"的状态）。
  **实际安全**：`prepare` 生成的 `snapshotID` 含 8 字节随机 nonce
  （`snapshot_store.go:72-80`，`snap-%016x-%016x-%s`，nonce 来自 `crypto/rand`），
  每圈的 `finalDir` 都不同，第二圈必然命中 `os.IsNotExist` 而退出；
  且 `:228` 的 `continue` 发生在 `prepare` 之后、`write` 之前，而 `prepare` 的文档与实现都是
  "builds the manifest and filesystem paths **without creating files**"，所以也不泄漏目录。
  同理 `snapshot_store.go:170-193` 的 `stage` 也安全。

- **D2 假设的"截断/损坏快照可被静默加载"** —— 不成立，见上文 D2 全节。
  活路径有 magic + 边界解码器 + shape 校验 + 尾部字节校验 + `Validate(scope)` +
  每 chunk 文件大小精确匹配 + 每 chunk CRC32C + 总长度 + 整体 CRC32C，共 8 道关卡。

- **`pebble_reader.go:202-214` `ensureMeta` → `persistMeta` 的 read-modify-write 竞态** ——
  `currentMeta` 从 Pebble snapshot（`:171`）读，却用 `s.db.db.Set(..., pebble.Sync)`（`:229`）
  **绕过 write worker 直接写**，理论上可以用一份陈旧的 `{LastIndex:0, SnapshotIndex:0}`
  覆盖 worker 刚提交的正确 meta，从而制造出发现 #3 所需的永久性 manifest/meta 不一致。
  **但我写不出成立的触发序列，故排除**：`persistMeta` 只在 `currentMeta` 返回 `fromDisk==false`
  （磁盘上没有 meta key）时才执行，而每个 scope 的 meta key 在**单线程的 bootstrap 阶段**
  就已落盘——slot 走 `pkg/cluster/slot_manager.go:50` 的 `InitialState`，
  controller 走 `pkg/controller/raft/service.go:1261`，都早于各自 raft loop 启动；
  此后 `saveOp.apply` 每次都经 `store.setMeta`（`pebble_writer.go:298`）维护它。
  并发读者（HTTP 管理端点）到达时 `fromDisk` 恒为 true，`persistMeta` 不会被调用。
  —— 但这是一个**只靠调用顺序维持的隐式不变量**，没有任何断言保护，建议后续加固。

### raftlog 的 21 条 gosec 命中：逐条核实，**全部为误报**

| 位置 | 转换 | 上界保证 |
|---|---|---|
| `snapshot_codec.go:171` | `uint16(len(data))`（`appendBytes16`） | 四个调用点全部预检查：`:28` 校验 SnapshotID / ChecksumType / WholeChecksum ≤ MaxUint16，`:34-38` 校验每个 chunk checksum ≤ MaxUint16 |
| `snapshot_codec.go:176` | `uint32(len(data))`（`appendBytes32`） | `:31` 预检查 `len(confState) > math.MaxUint32` |
| `snapshot_codec.go:55` | `uint32(len(ChunkChecksums))` | 同 `:31` 预检查 |
| `snapshot_codec.go:50` / `:103` | `CreatedAtUnixNano` int64↔uint64 往返 | 纯往返；该字段**只写不读**——GC 判过期用的是文件系统 `info.ModTime()`（`snapshot_gc.go:131`），不是它 |
| `snapshot_codec.go:157` / `snapshot_manifest.go:79` / `snapshot_store.go:200` | `uint64(maxInt())` | `maxInt()` = `int(^uint(0)>>1)`，恒正 |
| `snapshot_codec.go:243` | `uint32(maxInt())` | 64 位上恒等于 `0xFFFFFFFF`，该判断是死分支；但真正的守卫是紧随其后的 `d.read(int(size))`（`:189-192` 检查 `d.remaining() < size`），有效 |
| `snapshot_store.go:311` | `uint64(info.Size())` | **同一行**就有 `info.Size() < 0` 前置判断 |
| `snapshot_store.go:314` | `make([]byte, int(expectedSize))` | 紧接 `:311` 的 `uint64(info.Size()) != expectedSize` 相等校验——分配量恒等于真实文件大小；且 `read:200` 已先校验 `TotalSize > uint64(maxInt())` |
| `snapshot_store.go:145` | `data[int(offset):int(offset+chunkLen)]` | `offset < totalSize == len(data)`，`chunkLen` 被 `:142-144` clamp 到 remaining |
| `pebble_store.go:84,87` / `memory.go:52,55` | `uint64(entry.Size())` | protobuf 生成的 `Size()` 返回非负 int |
| `types.go:40` | `uint32(len(confState))` | ConfState 是 raft voter 集合的编码，量级为集群节点数×8 字节；且 `Unmarshal:57` 有 `len(data[44:]) != int(confStateSize)` 的严格相等校验，任何截断/超长都会被拒 |

**没有一条满足"无上界保证 **且** 由远程输入驱动 / 长期累积的序列号"** 的真问题标准。
特别说明：manifest 的所有字段都是**本地**根据 raft snapshot metadata 计算的
（`snapshot_store.go:85-96`），不是线路输入。

### raftlog 的 3 条 gosec G304（`snapshot_store.go:281,301,330`）—— 误报

路径全部由 `scopeDir(scope)`（`:235-244`，scope 只有 slot/controller 两种枚举）+
经 `validateSnapshotID`（`snapshot_manifest.go:118-132`）校验过的 SnapshotID +
`chunkFileName(i)` = `fmt.Sprintf("chunk-%06d", i)` 拼成。
`validateSnapshotID` 显式拒绝 `""` / `.` / `..` / 绝对路径 /
`filepath.Base(id) != id` / 含 `/` 或 `\`，并强制匹配
`^snap-[0-9a-f]{16}-[0-9a-f]{16}-[0-9a-f]{16}$`。无路径穿越可能。

### transport 的 3 条 gosec

- `pool.go:178` / `:188` `int(shardKey % uint64(len(set.slots)))` —— 结果恒 `< len(slots)`（一个 int），误报。
- `frame.go:53` `uint32(bodyLen)` —— 溢出本身需要单条 >4GiB 消息，实际不可达；
  但**缺失的发送侧 MaxMessageSize 校验**是真问题，已作为发现 #8 单列。

### 其它已核实为安全 / 不构成发现的点

- **`fsync 先于 ack`** —— 成立。`flushWriteRequests:144` 的 `batch.Commit(pebble.Sync)` 返回之后，
  `runWriteWorker:112-115` 才把结果写回 `req.done`；`submitWrite:83` 阻塞在 `<-req.done`。
  调用方拿到 nil 时数据确已落盘。
- **日志截断与并发读/快照安装的竞态** —— 安全。`saveOp.apply` 把 manifest 的 `Set`、
  entries 的 `DeleteRange` 和 meta 的 `Set` 放进**同一个 Pebble batch**（`:242-298`），
  Pebble batch 提交是原子的，读者要么看到截断前要么看到截断后。
  读侧 `loadSnapshotMetaView:281` 用 `db.db.NewSnapshot()` 取一致性视图，
  并有 `validateManifestMetaConsistency` 交叉校验。
- **Pebble 资源关闭** —— 全部路径（含 error 分支）都有 `defer`：
  `pebble_writer.go:124` `defer batch.Close()`、`pebble_reader.go:172,282` `defer readState.Close()`、
  `pebble_reader.go:253` `defer iter.Close()`、`snapshot_gc.go:50` `defer iter.Close()`、
  `pebble_db.go:123` `defer closer.Close()`、`pebble_reader.go:146` `defer closer.Close()`、
  `snapshot_store.go:305` `defer file.Close()`。`fsyncDir:329-340` 和
  `writeSyncedFile:280-298` 手工管理 close 但都在 error 分支上关了。未发现泄漏。
- **`pebbleStore.Entries` / `Term` 不返回 `raft.ErrCompacted` / `ErrUnavailable`** ——
  违反 etcd raft 的 `Storage` 契约，但**不在 etcd raft 的调用路径上**：
  `pkg/slot/multiraft/storage_adapter.go:8-11` 显示真正喂给 raft 库的是
  `raft.MemoryStorage`，raftlog 只在 `ready.go:10-58` 的 bootstrap 和
  `compaction.go:155-169` 的 `storageSnapshotBoundary` 被调。
  bootstrap 的 `maxSizePerMsg(0)` 返回 `math.MaxUint64`（`slot.go:986-991`），
  所以 `Entries` 的 maxSize 早停分支不会触发、不会截断日志；
  `storageSnapshotBoundary` 返回的 term 在唯一调用点 `compaction.go:126` 被 `_` 丢弃。
  无可构造的错误后果。
- **`prepareAndWriteSnapshot:234-240` 在 `for` 循环体内 `defer`** —— 由于上面分析的
  "第二圈必然退出"，最多注册一次 defer，不会累积。
- **`publishSnapshotAndCommit` 持 `snapshotLifecycleMu` 调 `submitWrite` 会否死锁** ——
  不会。write worker 的全部路径（`flushWriteRequests` → `loadScopeWriteState` →
  `loadSnapshotMetaView` → `saveOp.apply` / `markAppliedOp.apply` → `setMeta`）
  从不获取 `snapshotLifecycleMu`，无环。仅构成发现 #6 的长持锁问题。
- **`MuxConn.handleRPCResponse:134` 的 `ch <- resp`** —— 不会阻塞：`ch` 在 `RPC:52`
  是 `make(chan rpcResponse, 1)`，且 `pendingMap.LoadAndDelete`（`pending.go:40-49`）
  保证同一 reqID 只被投递一次，伪造/重复的响应帧不会二次发送。
- **`server.go:165-194` 把池化 slab buffer 传给 handler** —— RPC 两条分支（`:173`、`:183`）
  在 `release()` 前都做了 `append([]byte(nil), body...)` 深拷贝，
  `default` 分支（`:188-192`）是同步调用 `h(body)` 后才 `release()`，
  符合 `types.go:14-16` 对 `MessageHandler` 的契约声明。包内无 use-after-free。
- **`errSnapshotNoOverwriteRenameUnsupported`（staticcheck U1000）** ——
  staticcheck **误报**：它被 `snapshot_rename_unsupported.go:6` 使用，
  该文件 build tag 为 `//go:build !darwin && !linux`。
- **`scope.go:28-32` 与 `snapshot_store.go:80,238,240,251,255` 的 `fmt.Sprintf`** ——
  侦察判断"非热路径"**部分成立**：`snapshot_store.go` 的五处确为**每快照一次**
  （prepare / scopeDir / nonce / chunkFileName），不在 per-entry 路径上，不构成性能问题；
  `scope.go` 的三处则根本零调用方（见发现 #12），连"非热路径"都谈不上。**按指示不作为发现计入**。
- **`priorityWriter.loop:116-119` 写失败后 return，队列中剩余消息静默丢弃** ——
  实际影响有限：`:117` 先 `pw.conn.Close()`，这会让对端与本端的 `readLoop`（`conn.go:90`）
  立刻出错返回，`serveConn:153-157` 的 defer 随即 `mc.Close()`，
  池侧则由 `acquire:192` 的 `mc.closed.Load()` 检出并重连。
  丢失窗口是亚毫秒级且 raft 本身会重传，写不出稳定的数据丢失序列，排除。
- **`p.nodes` sync.Map 无淘汰** —— 只按**集群节点数**增长（不是按连接/用户/channel），
  且 `ClosePeer`（`pool.go:120-130`）在地址变更时由
  `pkg/cluster/transport_glue.go:89-97` 的 `OnAddressChange` 触发清理。不构成无界增长。

---

## 本分片整体评价

`pkg/raftlog` 的工程质量明显高于本仓平均水平：group commit 实现正确（D1）、快照有
manifest + 分块 CRC + 整体 CRC + no-overwrite rename 的完整持久化协议、
snapshot ID 有严格的路径注入防护、Pebble 资源在所有 error 分支都正确关闭、
21 条 gosec 命中经逐条核实**全部是误报**。它的问题不在"忘了校验"，
而在**故障隔离与生命周期**：group commit 批内没有 per-request 隔离（#3），
提交失败会留下被就地覆写污染的 entries 缓存（#7），`Close()` 只等后台 worker 不等读者（#4）。
这三条都属于"正常路径完美、异常路径失守"。

`pkg/transport` 则是本分片真正危险的一半，也是**全仓安全姿态的单点**：
**零认证、零 TLS、零读写 deadline、零连接数上限、零 `recover()`**。
其中读帧"先按声明长度分配、再读 body"（#1）让攻击者用 5 字节头换 64MB 堆，
是**无凭证远程 OOM**；而裸 goroutine 派发无 recover（#2）把兄弟单元里
`internal/access/node/delivery_control_codec.go:48` 与 `pkg/cluster/codec_control.go:2784`
两个编解码缺陷从"单连接错误"放大成"整进程崩溃"——这两条也正是协调者要我交叉确认的载荷。

**最该优先处理的一条**：`pkg/transport` 的节点间认证 + 读帧分配策略（#1）。
它同时是 #2 两个兄弟 P0 的**前置条件**——只要 RPC 端口对不可信网络可达，
上述所有解码层缺陷都是无凭证远程 DoS；补上认证与连接配额，三个 P0 的可达性会一起下降。
其次是 `pkg/raftlog` 的 #7（脏缓存导致 `meta.LastIndex=0` → 重启时 etcd raft
`tocommit out of range` panic），它是本分片唯一能造成**已提交日志丢失**的路径。

两个包都是 v1 生产代码（`internal/app/build.go:129` / `:240` 组合根直接构造），
**不属于未上线的 v2 栈**，严重度按实际计算。
