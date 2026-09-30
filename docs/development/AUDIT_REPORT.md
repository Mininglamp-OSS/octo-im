# octo-im 全量代码审计报告：设计缺陷问题清单

> **审计范围**：`internal/` `internalv2/` `pkg/` `cmd/` 全部生产源码（约 199,000 行非测试 Go）+ 架构与文档一致性
> **审计方式**：32 个互不重叠的分片并行只读审计，每条发现须有 `file:line` 证据 + 可复现触发路径，并经对抗性复核
> **不含**：测试代码与测试策略、CI / Docker / 部署配置
> **判定标准**：严格验证、宁缺毋滥 —— 写不出具体触发路径的候选项一律丢弃并公示

---

## 一、执行摘要

本次审计在约 20 万行生产代码中确认了 **29 条 P0（致命）**、**约 78 条 P1（严重）**、
约 105 条 P2 与 86 条 P3 —— 合计约 **298 条**带 `file:line` 证据与触发路径的发现。
逐分片明细见 `docs/development/audit/`（32 份分片报告 + 9 份协调者独立复核记录，共约 100 万字节）。

**最重要的四条结论：**

### 0. 最易触发的致命缺陷只需**一个报文**，而且是静默的

`pkg/controller/raft/service.go:957` 只过滤了 `raft.ErrStepLocalMsg`，
漏掉了依赖库中**紧邻两行声明**的 `ErrStepPeerNotFound`（`rawnode.go:121` vs `:124`）——
两者语义都是"忽略此消息"。于是任何人向集群端口发**一帧** raft 响应消息
（`From` 填任意非成员节点 ID），即可让该节点控制面 Raft 循环永久退出。
`go s.run(` 全文件只有一处，无看门狗；`Status` 也没有任何字段承载这个致命错误 ——
**节点还在、端口还听、健康检查可能仍为绿，而控制面已经停止前进。**
修复是一行。详见 P0-1。

### 1. 四个入口面在默认（或唯一可能的）配置下全部无身份验证

| 入口 | 端口 | 状态 |
|---|---|---|
| 客户端网关 | `0.0.0.0:5100` / `:5200` | 认证功能**无法启用**（开启即启动失败，且钩子从未实现） |
| 业务 HTTP API | `0.0.0.0:5001` | engine **只挂了一个 CORS 中间件**，零认证 |
| 后台管理 API | `0.0.0.0:5301` | `WK_MANAGER_AUTH_ON` 代码默认 **false**（示例配置却写 `true`） |
| 节点间 RPC | 集群端口 | `pkg/transport` **无认证、无 TLS**（grep 零命中） |

这不是四条孤立的配置问题，而是一条系统性缺口：**没有任何一个入口在默认状态下验证调用方身份**。
其余多条 P0 的严重度都由此放大 —— 原本"集群内部可触发"的缺陷，实际是"能连上端口的任何人都可触发"。

### 2. 一条未认证 HTTP 请求可让节点进入**不可自愈的永久崩溃循环**

这是本次审计最严重的单条问题（详见 P0-1）。链路完整、每一环都已逐行核实：
消息先持久化提交、再同步扇出编码 panic、而推进游标的代码行永远执行不到，
重启后全量重放必然再次命中同一条毒消息。**且 leader 漂移会把毒药带给新 leader。**

### 3. 缺陷的主因不是"不会写"，而是"正确写法已存在却未统一执行"

这是本报告最值得决策层注意的一点。在多个高危问题上，**仓库里已经有正确实现**：

| 问题 | 仓库内已有的正确范式 | 未采用的站点 |
|---|---|---|
| 线路长度无上界 → 远程 OOM/panic | `internal/access/node/delivery_push_codec.go:283` `readCollectionLen`（15 处正确使用）；`pkg/cluster/codec_control.go:995` | 至少 7 处 |
| 每次写都 fsync | `pkg/raftlog/pebble_writer.go:86-151`（教科书式 group commit） | `pkg/db` 全部 17+ 写入点；且 `pkg/db/internal/commit/coordinator.go` 是**专为此写的、零调用方**的批量提交器 |
| 关停不 join 后台 goroutine | `internal/runtime/channelmeta/resolver.go` 的 `Stop()`（锁内取 cancel/done、解锁、等待、`refreshWG.Wait()`） | `pkg/channel/replica`、`pkg/cluster/observer`、`pkg/clusterv2/node_loops`、`pkg/cluster/controller_host` |
| Raft apply 出错不可致命 | `pkg/controller/FLOW.md` 的"避坑清单"已为 `NodeOnboardingJobUpdate` 定下 no-op 规范 | `applyAddSlot` / `applyRemoveSlot` 未照做 → P0-13 |

**因此修复成本远低于问题数量所暗示的**：多数高危项是把已有范式推广到遗漏站点，而非重新设计。

---

## 二、审计方法与覆盖

### 覆盖规模

| 区域 | 非测试 Go |
|---|---|
| `pkg/` | 111,698 |
| `internal/` | 83,986 |
| `cmd/` | 2,108 |
| `internalv2/` | 1,185 |
| **合计** | **≈ 199,000** |

切分为 32 个单元（2.6k–10.8k 行/单元），每单元一个 agent 在隔离 git worktree 中只读审计，
要求逐文件通读并在报告开头列出覆盖清单。

### 工具基线（协调者统一执行，供全部分片复用）

- **本机默认工具链编译失败**：Go 1.27.1 下 `go build ./...` 失败 ——
  `github.com/cockroachdb/swiss`（Pebble 间接依赖）引用了已被移除的 runtime 内部符号
  （`hashFn` / `fastrand64` / `getRuntimeHasher`）。必须 `GOTOOLCHAIN=go1.23.4`。
  **这本身是一条供应链风险**：项目被锁死在旧工具链上，无法随 Go 版本前进。
- `go vet ./...`：**全仓干净**（说明 vet 级别的低垂果实已无，缺陷需语义分析）
- `staticcheck ./...`：119 条（102 条 U1000 未使用代码）
- `gosec ./...`：486 条（G115 整数转换约 396 条，**绝大多数是误报，但其中埋着多条真 P0**）
- `govulncheck ./...`：**38 个可达漏洞**
- 全仓 **0 条** `TODO`/`FIXME`/`HACK`/`XXX` 注释（1631 个 `.go` 文件）——
  已知问题没有以注释形式留存，只能靠审计挖出

### 质量控制

1. 每个 worker 必须做两阶段：普查 + **对抗性复核**（重读完整函数与全部调用方，三问不过关即丢弃）
2. 每份报告必须公示「已排除的候选项」及排除理由
3. **协调者对高危条目逐条打开源码独立复核**。本次共独立核实 6 组，其中：
   - 确认成立：单元 04 / 11 / 15 / 20 的 P0，以及单元 02 的崩溃循环链路
   - **修正扩展**：单元 06 称"全包唯一一处"未走安全辅助函数，实测为 **4 处**
   - **协调者自行发现**：`pkg/db/meta` 三条（含一条任何扫描器都未报的问题，见 PAT-5）
4. 部分 worker 主动**驳回了协调者给出的线索**，并给出证据 —— 这些记录同样保留：
   - 单元 24 驳回"快照校验死代码 = durability 漏洞"：实测 live 路径有 8 道校验闸门，
     4 个 U1000 分别是活函数的包装、内联的冗余、以及一个 build-tag 误报 → **不是缺陷**
   - 单元 07 驳回"迁移缺写栅栏"：实测 `WriteFenceToken` + `WriteFenceVersion` + epoch
     在 Raft apply 内 CAS，是真栅栏而非 TOCTOU → **不是缺陷**
   - 单元 28 驳回"丢弃的 `now` 是被删掉的租约检查"：实测无此字段 → 降为 P3
   - 单元 12 驳回"`replicaReplaceCaughtUp` 死代码 = 缺少晋升闸门"：实测
     `EvaluateFinalTargetProof`（`proof.go:7-61`）是**更强**的闸门 → 不是缺陷
   - 单元 30 驳回"`compat.go` 1881 行是第二套实现"：实测是**真正的薄转发层**
     （`DB`/`ShardStore`/`WriteBatch` → `MetaDB`/`Shard`/`Batch`），在生产路径上，
     实质逻辑只集中在 5 处 → **架构重复假设不成立**
   - 单元 03 驳回"诊断面泄露 `from_uid`"：逐路径核实 `redactEvent` 在唯一的事件追加语句上被调用，
     无绕过路径 → **项目遵守了自己的设计规则**；同时指出单元 10 的 benchdata 越权
     在默认配置下**不可达**（`Bench.APIEnabled` 默认 false 双重保护）→ 降级为配置误开风险

### 协调者主动求解的关键问题（结果一并公示）

审计中最有价值的中间结论，往往是"**某条主线在这里不成立**"：

| 问题 | 结论 | 依据 |
|---|---|---|
| 迁移写栅栏是否真的能防"所有权已转移仍写入"？ | **成立，不是缺陷** | 激活缓存**拒绝缓存**任何带栅栏的 meta（`cache.go:68-71,116-119,149-152`）；`invalidateIfWriteFenceChanged` 在每次 `ApplyAuthoritativeMeta` 触发；`runtimeGuardFromMeta` 在 Raft apply 内 CAS 栅栏 token+version+epoch+leader；另有 `requireChannelMigrationCutoverProof` 校验 `DrainedLeaderNode`/`DrainedLeaderEpoch`（单元 07 + 12 双向确认） |
| 快照校验死代码是否意味着损坏快照可被静默加载？ | **不成立** | live 路径有 8 道闸门（magic → 有界解码器 → shape → 尾部字节 → `Validate(scope)` → 逐 chunk 精确文件大小 → 逐 chunk CRC32C → 总大小 + 整包 CRC32C）。4 个 U1000 分别是活函数的包装、内联的冗余、以及一个 build-tag 误报（单元 24） |
| 毒消息是否会被 replay 重新投递？ | **会，且构成崩溃循环** | 见 P0-1（单元 02 + 协调者） |

### 跨分片矛盾的裁决（协调者职责）

汇总时出现一处**表面矛盾**，必须裁决而不是并列呈现：

- 分片 07 与 12 各自独立得出：**channel 迁移的写栅栏是健全的**。
- 分片 03 得出：迁移安全相关字段被组合根硬编码为零值，**可能丢已提交消息**。

**裁决：两者都成立 —— 它们说的是两套不同机制。** 我打开源码逐点核实：

1. **写栅栏**（防"所有权已转移仍写入"）确实健全：
   `internal/runtime/channelmigration/replica_replace.go:225-232` 与
   `leader_transfer.go:166-173` 都从 `drain.*` 正确填充 `CutoverProof` 的 7 个字段，
   `pkg/db/meta/compat.go:1392-1399` 落库，`:1509` 的
   `requireChannelMigrationCutoverProof` 做校验。
2. **但探测报告适配器丢字段**（影响"追赶期能否发现日志分叉"）：
   `internal/app/channelmigration.go:205-220`（`channelMigrationProbeReportFromResponse`）
   把 `EpochHistory` 硬编码为 `nil`、`TruncateTo` 为 `nil`、`SnapshotRequired` 为 `false`，
   而同一个结构体里其余 12 个字段都从 `resp.*` 如实映射 —— 这 3 个是**唯一**被写死的。

**结论**：栅栏拦住了**陈旧写入**，但基于 epoch history 的**分叉检测**与精确截断点
因适配器丢字段而失效，`SnapshotRequired` 恒假意味着永不请求快照、只能走更粗的回退路径。
→ 真实风险不是"写到了错误的 leader"，而是"**副本带着分叉日志被晋升**"。
两条发现互补，**分片 03 这条不应被"栅栏健全"的结论掩盖**。

---

## 三、模式级问题（跨包根因，优先按此修复）

### PAT-1 — 接入层系统性无认证

见「执行摘要 1」。具体条目：P0-2、P0-3、P0-4、P0-5、P0-18。

补充证据：`wukongim.conf.example:215-216` 对 bench 路由明确警告
*"these routes intentionally skip auth … Keep false in production"*，
说明作者具备安全意识，**却唯独没有对主业务 API 与网关说明同样的信任假设**。

### PAT-2 — `uint64` 线路长度在用于分配或切片前被转成 `int`/`uint32`

转换本身就是绕过点。已确认站点：

| 位置 | 形态 |
|---|---|
| `pkg/cluster/codec_control.go:2597, 2784, 2795` | `if len(rest) < int(length)`，`length=2^63` 时 `int()` 为负 → 守卫放行 → 用未截断值切片 |
| `internal/access/node/delivery_control_codec.go:48` | `make([]RouteAck, 0, int(count))` 无上界 |
| `internal/access/node/monitor_metrics_codec.go:191` | `make([]float64, int(count))`（作为 **len**，完整分配并清零） |
| `internal/access/node/plugin_management_codec.go:195, 434` | 同上 |
| `pkg/slot/proxy` `runtimeMetaCollectionLen` | 上界用"剩余字节数"而非"剩余字节 ÷ 单元素最小线上尺寸" → 64MB 帧放大 ~4.8GB |
| `pkg/transport` 读帧 | 先按头部长度分配、后读取 → 5 字节头换 64MB 堆 |
| `pkg/cluster` snapshot chunk | 96 字节报文触发任意大小分配 |
| `pkg/channel/transport` | 8 处长度前缀直接进 `make()` |

**正确范式仓库内已有两套**（见执行摘要 3）。这是规范未统一执行，不是缺少方案。

### PAT-3 — panic 被用作错误处理，而关键路径无 `recover()`

全仓非测试代码**只有 2 处** `recover()`：
`internal/access/plugin/handlers_lifecycle.go:48`、`internal/runtime/channelmeta/activate.go:41`。
**投递、编码、节点 RPC 派发路径上一处都没有。**

而 `pkg/transport/server.go:186` 以**裸 goroutine** 派发 RPC handler
（`go s.handleRPCRequest(...)`，该文件零 `recover()`），
`pkg/protocol/codec/encoder.go:141-145` 在字符串超过 `math.MaxInt16` 时直接 `panic()`
（`encodeRecv` 本身返回 `error`，有现成的错误通道却不用）。

→ 任何 PAT-2 的分配失败、任何超长字段，都不是"请求失败"，而是**整个进程死亡**。

### PAT-4 — run loop 不对错误做"可恢复性"分类，一切 error 皆致命

`pkg/controller/raft/service.go:942-946`：`processReady()` 返回 error → `setError` → `return`，
运行协程退出；`go s.run(` 全文件只出现一次（`:372`），**无看门狗、无 supervisor、无 restart**。

分片 19 逐条枚举了**全部 8 条**能走到 `setError; return` 的路径，结论是**一个精确的倒置**：

| 路径 | 性质 | 当前行为 | 应有行为 |
|---|---|---|---|
| `persistReady`（`:896`）、`MarkApplied`（`:1050`） | 真存储故障 | 致命 | 致命尚可辩护 |
| `decodeCommand`、`StateMachine.Apply`、`cc.Unmarshal`、`StateMachine.Restore`、`rawNode.Step` | 业务 / 状态 / 对端输入 | **致命** | **绝不应致命** |
| `transport.Send`（`:914`） | 瞬时网络 | 实际**不可达** —— `transport.go:62` 写着 `_ = t.client.Send(...)` 并注释"transient send failures are not fatal" | 不致命（已是） |

> **该容忍的被容忍了，绝不该致命的反而致命。** 所以"apply 出错即致命"不是一种设计取舍，而就是根因。

三个具体后果：
1. **业务错误变毒丸** —— `pkg/controller/plane/statemachine.go:626,629,632` 对"已有活跃迁移 / 槽位已存在 /
   assignment 已存在"这类 **apply 时刻的集群状态**返回 `ErrInvalidArgument`。Raft apply 确定性
   → 所有副本同时死亡 → 条目已持久化 → 重启重放再死（**P0-2**）。
   且重启也救不了：`rawNode.Advance` 永不执行，applied 点永不前移。
2. **依赖库的"请忽略"被当成致命** —— `rawNode.Step` 只过滤了 `ErrStepLocalMsg`，漏了紧邻两行声明的
   `ErrStepPeerNotFound`（**P0-1**，见下）。
3. **死亡完全静默** —— `setError` 会把 `s.started = false`，于是此后每次 `Propose` 返回
   `ErrNotStarted`，与"从未启动"完全一致；`status.go:22-41` 有 `Compaction.Degraded` 与
   `Restore.Failed`，但**没有任何字段承载 run loop 的致命退出**；`recordStoppedStatus` 把 Role 重置为
   `unknown`、索引全部归零，而管理 RPC（`controller_handler.go:79`）读的正是它。
   **运维看到的是"leader=0 / role=unknown / propose 说未启动"—— 与一次缓慢的选举无法区分。**

项目自己在 `pkg/controller/FLOW.md` 已为 `NodeOnboardingJobUpdate` 定下 no-op 规范，但未推广到其余 apply 分支。

### PAT-5 — 缓存与状态 map 普遍无淘汰

| 位置 | 增长维度 |
|---|---|
| `pkg/db/meta` `channelCache`（协调者自查） | 每个曾被读取的 channel，无 cap / TTL / LRU；`channelCacheSize()` 仅测试调用 |
| `internal/runtime/channelmeta/cache.go:285` `shard.generations` | 每个曾被 invalidate 的 channel key；**该缓存有完整的 TTL + cap + LRU 机制，但三个回收函数整齐地遗漏了第三张 map** —— 比"没有淘汰"更隐蔽 |
| `pkg/slot/fsm/statemachine.go:794` `appliedDelta` | 每条迁移 delta；**只写不读**、永不清理（真正的去重靠持久化路径） |
| `internal/runtime/deliverytag` tagCache | 每条临时频道消息净增一条；`CleanupExpired` **无任何生产调用方** |
| `pkg/controller/meta` onboarding jobs | 永不删除，且每次状态转移持写锁全表扫描解码排序，并进入每次 Raft 快照 |
| `pkg/clusterv2` `Manager.Ensure` 的 `unassigned` | 只增不删 |

### PAT-6 — 扇出 RPC 普遍无单节点超时

`internal/usecase/management` 的监控指标聚合、节点列表、任务中心、缩容状态读取，
全链路无 deadline。最尖锐的一例（单元 07）：**缩容状态查询的 RPC 目标正是"正在被缩掉的那个节点"**
—— 最可能保持 TCP 连接却不回包的节点。一个僵死节点可永久挂住整个缩容控制面。

### PAT-7 — 关停不 join 后台 goroutine / 非幂等 Stop

`pkg/channel/replica/append_pipeline.go:382-391`（内层 goroutine 从不 join，
而 `replica.go:320-321` 只 join 外层）、`pkg/cluster/observer.go:53-67` 与 `:115-129`
（check-then-`close()` 无锁无 `sync.Once` → 并发 Stop 双关闭 panic）、
`pkg/cluster/cluster.go:592-625`（Stop 非幂等，无锁置 nil 8 个 observer 字段）、
`pkg/clusterv2/node_loops.go:8-25`（watch loop 无 WaitGroup，而**同文件的兄弟函数正确使用了** `channelTickWG`）、
`pkg/cluster/controller_host.go:588, 827`（两个 goroutine 不被跟踪，Stop 后直接 `meta.Close()`）、
`pkg/raftlog` `DB.Close()` 置 `db.db=nil` 却不等读者。

**正确范式**：`internal/runtime/channelmeta/resolver.go` 的 `Stop()`。

### PAT-8 — 大量"写好了但没接线"的正确实现

这是本仓库最反常的特征，也是理解其质量状况的钥匙：

| 死代码 | 规模 | 含义 |
|---|---|---|
| `pkg/db/internal/commit/coordinator.go` | 专用 group commit 批量提交器 | 零调用方；`ConfigureCommitCoordinator` 是**只写不读的 setter**，生产代码调用了它却毫无效果 |
| `pkg/controller/plane/controller.go` | 167 行编排层 | 生产用 `cluster.controllerTickOnce`；FLOW.md 却把它列为**唯一调度入口**，且死实现与自身文档在三处矛盾 |
| `pkg/controller/raft/command_codec.go` | 约 30 个函数 | 整套废弃的二进制编解码路径 + 完整 `NodeOnboarding` 家族 |
| `pkg/controller/meta` `ControllerMembership` | 类型 + `'m'` 键前缀 + 编解码 + 快照校验 + 4 个 Store 方法 | **从未被写入**，该记录在任何部署中都不存在 |
| `internal/runtime/channelplane/resolver.go` | 整个 Resolver（singleflight + 路由缓存 + invalidation serial） | 生产死代码，FLOW.md 列为核心组件 |
| `pkg/clusterv2/observe` | 整包 | v2 内部亦无调用方 |
| `pkg/db/internal/cache/tiny.go` | 有界缓存 | 零非测试导入方 |
| `internal/runtime/delivery/mailbox.go` | 37 行 | 整文件死代码 |

### PAT-9 — "看起来分片了，实际被一把全局锁串行化"

两个独立分片各自发现同一形态，且都出现在**读多写少的热路径**上：

| 位置 | 表象 | 实际 |
|---|---|---|
| `pkg/db/meta/db.go:13`（协调者自查） | 有 `shards` / `shardLocks` 两张按 hash slot 分片的 map | 但 `mu` 是**一把 `sync.Mutex`（非 RWMutex）**，同时保护 `shards`、`shardLocks`、`channelCache`、`testLocked`。`GetChannel` → `cachedChannel` 走的是**排他锁**，于是全节点所有频道元数据读取串行化 |
| `internal/runtime/channelmeta` 激活缓存（分片 12） | 有 16 个分片 | `RunSingleflight` / 全部 store / `Invalidate` 串行在单把全局 `c.mu`，且**持锁遍历全部 16 个分片** —— 与 `FLOW.md` 的明确承诺相反 |

**共同教训**：分片结构被写出来了，但保护它的锁没有跟着分片，于是分片带来的并行度被完全抵消 ——
既付出了分片的复杂度，又没得到它的好处。审查这类代码时应把"锁的粒度是否与数据结构的粒度一致"
作为固定检查项。

补充：`pkg/db/meta` 那把锁还额外背着一个**纯测试用途**的字段
（`testLocked`，`db.go:18`），维护它要在 `lockHashSlots` 里多做 2N 次全局锁往返，
而 `testLockedOrder()` 有一个真实生产调用方（`batch.go:276`）——
于是**批量写会直接加剧频道读的争用**，两条本该无关的路径被这把锁耦合。

### PAT-10 — 一组同类错误只处理了其中一个

反复出现的疏漏形态：作者显然知道"这里会返回可忽略的错误"（专门做了 `errors.Is`），
却漏掉了同一处返回的兄弟错误。

| 位置 | 处理了 | 漏了 |
|---|---|---|
| `pkg/controller/raft/service.go:957` | `raft.ErrStepLocalMsg` | **`raft.ErrStepPeerNotFound`** —— 依赖库中紧邻两行声明（`rawnode.go:121` vs `:124`），语义都是"忽略此消息" → **P0-1** |
| `internal/access/node`（分片 06） | 同目录其余 handler 用 `errors.Is` | `handleChannelRetentionRPC` 用 `==` 比较 sentinel |
| `internal/access/manager`（分片 05） | 各类业务错误 | **`context.Canceled` 未被任何错误分类器识别** → 客户端主动断开被记成服务端 500，并外泄内部错误文本 |


---

## 四、P0 清单（致命）

> **排序依据：触发难度**，而不是编号或分片顺序。对决策而言"需要什么条件才能被触发"
> 比"破坏多大"更重要 —— 一个需要精确并发时序的缺陷和一个"发一个报文就行"的缺陷，
> 不应该排在同一档。
>
> 标 **✅** 的条目由协调者独立打开源码逐行复核确认（含依赖库源码）。
> 标 **○** 的条目来自分片报告、协调者未独立复核（据实标注，不夸大）。

### 第 1 档：一个报文 / 一次请求，无需凭据、无需时序、无需特定状态

| # | 问题 | 位置 | 备注 |
|---|---|---|---|
| **P0-1** ✅ | **一帧未认证 raft 响应消息永久静默杀死控制面 Raft** —— `Step` 只过滤了 `ErrStepLocalMsg`，漏了依赖库紧邻两行声明的 `ErrStepPeerNotFound`（`rawnode.go:121` vs `:124`，语义都是"忽略"）。`go s.run(` 全文件仅一处，无重启；且 `Status` 无字段承载该致命错误 → **死亡与"缓慢选举"无法区分** | `pkg/controller/raft/service.go:957` | 分片 19。**整个审计中最易触发者**：一个 `MsgHeartbeatResp`，`From` 填任意非成员 ID |
| **P0-2** ✅ | **毒消息导致不可自愈的永久崩溃循环** —— 消息先持久化提交，再同步扇出编码 panic，而推进游标的那一行在 panic 之后、永不执行；重启后全量扫描必然重新载入同一条消息 | `internal/app/committed_replay.go:369-377`＋`internal/runtime/delivery/shard.go:32`（无 `go`，全同步）＋`pkg/protocol/codec/encoder.go:141-145`（`panic`） | 分片 02＋32＋04＋协调者。**leader 漂移会把毒药带给新 leader**；恢复需人工改写 Pebble |
| **P0-3** ✅ | **`readString`/`readBytes` 长度守卫被 `uint64→int` 截断绕过** —— `length=2^63` 时 `int()` 为负，`len(rest) < 负数` 为假，守卫放行，随后用**未截断值**切片 | `pkg/cluster/codec_control.go:2784, 2795, 2597` | 分片 15＋协调者。同文件 `:995` 就有正确写法 |
| **P0-4** ○ | 未认证快照分片帧 `make([]byte, int(chunk.total))`，`total` 无上界 → 约 81 字节帧触发 `make([]byte, 1<<60)`。**分配发生在 `dropInboundRaftMessage` 之前**，连 To/From 过滤都不生效 | `pkg/cluster/snapshot_chunks.go:105` | 分片 19＋16＋17 三个分片独立命中 |
| **P0-5** ○ | `pkg/transport` 读帧"先按头部长度分配、后读取" → 约 5KB 流量远程耗尽节点内存 | `pkg/transport`（帧读取路径） | 分片 24 |
| **P0-6** ✅ | **4 处解码站点绕过包内现成的 `readCollectionLen`**，用线路 count 直接 `make()` | `internal/access/node/delivery_control_codec.go:48`、`monitor_metrics_codec.go:191`、`plugin_management_codec.go:195, 434` | 分片 06 报 1 处，**协调者复核为 4 处**；`delivery_push_codec.go:283` 的正确版本在 15 个文件中被正确使用 |
| **P0-7** ○ | monitor metrics RPC 的 window/step 无校验 → 远程整型除零 / 负长度 `makeslice` panic | `internal/access/node/monitor_metrics_codec.go` → `pkg/metrics/dashboard_collector.go` | 分片 06＋32 |
| **P0-8** ○ | `runtimeMetaCollectionLen` 上界取"剩余字节数"而非"剩余字节 ÷ 单元素最小线上尺寸" → 64MB 帧放大约 4.8GB | `pkg/slot/proxy` | 分片 23 |
| **P0-9** ○ | channel 数据面 transport 8 处无界长度前缀直接进 `make()`，经稳态 `LongPollFetch` 复制 RPC 可达 | `pkg/channel/transport/{codec,longpoll_codec,migration_control}.go` | 分片 27 |
| **P0-10** ○ | `keycodec.AppendString` 对超长 key 部件 `panic` → 一次 HTTP 请求即可崩掉 channel leader 节点 | `pkg/db/internal/keycodec/codec.go:48-50` | 分片 29 |
| **P0-11** ✅ | **`pkg/transport/server.go:186` 裸 goroutine 派发 handler + 全包零 `recover()`** | `pkg/transport/server.go:186` | 分片 24＋协调者。**这是 P0-3~P0-10 全部从"请求失败"升级为"进程死亡"的根本放大器** |
| **P0-11b** ✅ | **第二条持久化崩溃循环：超长 token 毒化整个 slot 复制组** —— **键**路径有显式长度守卫（`table_key.go:70-72` 返回 `ErrInvalidArgument`），而**值**路径 `appendValueString`（`tx_helpers.go:51-53`）是**裸转发**、不返回 error、只能 panic；且 `User`/`Device` 对 `Token` **无任何长度校验**（grep 零命中）。→ 一个 >64KiB token 经**无认证**的 `POST /user/token` 提交进 Raft 日志，在**每个副本**的 apply goroutine 里 panic，重启后重放 | `pkg/db/meta/tx_helpers.go:51-53`＋`pkg/db/internal/keycodec/codec.go:48-50`＋`internal/access/api/user_token.go` | 分片 30＋协调者。与 P0-2 同类但**破坏面更大**（整个 slot 复制组而非单 channel），且入口正是 P0-20 那个无认证 token 接口 |
| **P0-11c** ○ | 快照解码 `keyLen+valueLen` uint64 溢出绕过长度校验 → 远程 raft 快照触发切片越界 panic；`entryCount` 无上界即用于 `make` 预分配 → 远程 OOM。且 `ImportHashSlotSnapshot` 写入对端提供的 KV **不做任何值级校验**，而 `rowcodec.Unwrap` 让值自身的 `flags` 字节决定是否校验 checksum → 解析 bug 升级为**持久化投毒** | `pkg/db/meta/snapshot.go`、`decodeUint64Slice` | 分片 30（3 条 P0 合并列出）。注：**流式**快照解析器是死代码，活路径全量物化每个 entry，OOM 面更大 |

### 第 2 档：无需凭据，但需要一次普通的客户端行为或两次并发请求

| # | 问题 | 位置 | 备注 |
|---|---|---|---|
| **P0-12** ✅ | **WebSocket 入站 payload 零拷贝跨 goroutine 交付** —— 解帧返回调用方缓冲区的子切片（还原地解掩码），入队不拷贝，事件循环随后覆写同一数组 → 数据竞争 + **静默消息内容损坏并被持久化**。同文件的 `enqueueCopiedData` 注释点明了这个危险，TCP 路径正确使用了它，WebSocket 路径没有 | `pkg/gateway/transport/gnet/group.go:540` vs `:473`；`conn.go:118-152`；`ws_frame.go:127` | 分片 31＋协调者。触发只需客户端在一次 TCP 读里塞多个 WS 帧（正常行为） |
| **P0-13** ○ | WebSocket 小消息写完成回调窃取并清空尚未发出的大消息 `writev` 帧 → **出站消息静默丢失**（`writev` 发 0 字节且 `err == nil`） | `pkg/gateway/transport/gnet/conn.go:640-651` | 分片 31。协调者未独立复核（需 gnet writev 回调契约） |
| **P0-14** ✅ | **Controller Raft 中毒条目 → 全集群控制面同时永久死亡** —— apply 对"已有活跃迁移 / 槽位已存在 / assignment 已存在"等**状态相关**条件返回业务 error，而 run loop 视一切 error 为致命 | `pkg/controller/plane/statemachine.go:626, 629, 632` ＋ `pkg/controller/raft/service.go:942-946` | 分片 20＋19＋协调者。两个并发 add-slot 即可（链路无互斥、无幂等键）；项目自身 FLOW.md 已为另一条路径定下 no-op 规范却未推广 |
| **P0-15** ○ | `channel_migration` RPC 的 `propose` op 把**调用方任意 FSM 命令字节**直接提交进 Slot Raft，且 `ChannelID` 为空时跳过 hash slot 归属校验 | `pkg/slot/proxy` | 分片 23 |
| **P0-16** ○ | 四个 RPC handler 直接采信线上 `req.HashSlot`、从不校验归属 → 写路径可永久砖化 Slot，读路径跨分区寻址 | `pkg/slot/proxy` | 分片 23 |
| **P0-17** ○ | `encodeCursorBase64` 先切片后判长度（堆回退分支不可达）→ 长 ChannelID/UID 分页光标编码越界 panic；`gin.New()` 未挂 Recovery | `internal/access/manager/cursor_codec.go:420-421` | 分片 05（已用 go1.23.4 实测复现） |

### 第 3 档：认证缺口（无需触发条件 —— 这是持续存在的状态）

| # | 问题 | 位置 | 备注 |
|---|---|---|---|
| **P0-18** ✅ | **网关 token 认证是"双重死开关"** —— 配置层**无条件拒绝** `TokenAuthOn=true`（开启即启动失败），校验块被该恒假开关挡住成为死代码，且 `VerifyToken` 在全仓非测试代码中**只有声明/判空/调用三处、从无赋值**。→ 生产环境唯一可能状态：**网关完全不验证身份**，任意客户端 CONNECT 填任意 UID 即被接受为该用户 | `internal/app/config.go:810-812`＋`pkg/gateway/auth.go:35,60,61,67` | 分片 04＋31＋01＋协调者 |
| **P0-19** ✅ | **业务 HTTP API engine 只挂一个 CORS 中间件、零认证**，默认绑 `0.0.0.0:5001`；`/user/token`、`/user/systemuids_*`、`/channel*`、`/message/*`、`/conversations/*` 全部无鉴权 | `internal/access/api/server.go:190-191`＋`routes.go:45-76` | 协调者亲自核实 |
| **P0-20** ○ | 无认证 `/user/token` 可为任意（甚至尚不存在的）用户签发/覆盖 token | `internal/access/api/user_token.go:21-33` | 分片 04 |
| **P0-21** ○ | 无认证 `/user/systemuids_add` 可把任意 UID 升为系统账号，绕过发送权限检查与限流 | `internal/access/api` ＋ `internal/usecase/.../permission.go:17-19` | 分片 04 |
| **P0-22** ○ | **管理面默认无鉴权**：`WK_MANAGER_AUTH_ON` 代码默认 `false`（示例配置却写 `true`），60+ 端点裸奔，含缩容 / 删槽 / 踢人 / 改频道。最尖锐：`POST /manager/users/:uid/token/reset` 采信调用方提供的 token 并回显 → 未认证接管任意 IM 账号 | `internal/access/manager/routes.go`（~40 处 `s.auth.enabled()`）＋`cmd/wukongim/config.go:509` | 分片 05＋04。项目自身设计文档写明 auth 应默认开启 |
| **P0-23** ✅ | **插件宿主 RPC 全线丢弃已认证调用者身份** —— 入口层传了 `c.Uid()`、接口参数就叫 `callerUID`，实现却写 `_ string`。→ 任意插件可冒充任意用户发消息、读任意频道全部历史、读任意用户会话列表 | `internal/usecase/plugin/host_rpc.go:22, 38, 113` | 分片 11＋协调者 |
| **P0-24** ○ | 网关所有 goroutine 无 panic 屏障（gnet 的 errgroup 明确不 recover）→ 远程可达的编码 panic 杀死整进程 | `pkg/gateway`（4 处 goroutine 启动点） | 分片 31 |

### 第 4 档：需要特定并发交错或运行状态

| # | 问题 | 位置 | 备注 |
|---|---|---|---|
| **P0-25** ○ | 持久化 append 完成事件在 loop mailbox 满时被静默丢弃 → 该 channel 写入通道**永久卡死**，已落盘记录永不发布 | `pkg/channel/replica` | 分片 25 |
| **P0-26** ○ | `ApplyMeta`（ISR 变更 / 保留推进）在 append 飞行中抬 `roleGeneration` → 已落盘记录被丢弃不发布，运行时 LEO 永久落后 durable LEO，此后每次 append "写盘成功但返回 `ErrCorruptState`" | `pkg/channel/replica` | 分片 25 |
| **P0-27** ○ | run loop 把**所有** apply/decode 错误当作致命且终态，无 watchdog（8 条路径中 5 条本不该致命，唯一"可容忍"的那条反而不可达）—— 见 PAT-4 | `pkg/controller/raft/service.go` | 分片 19 |
| **P0-28** ○ | JSON-RPC 桥是同一批 frame 对象的**第二条**无长度校验注入路径，且 `int→uint8` 截断静默改写 Version/ChannelType | `pkg/protocol/jsonrpc/types.go` | 分片 32 |
| **P0-29** ○ | `WriteString` 的 panic 是全协议栈**唯一**长度防线，而所有帧编码器都把未校验字符串送进它（12 个编码器约 30 个调用点，`encodeRecv` 本身返回 `error`、有现成错误通道却不用） | `pkg/protocol/codec/encoder.go:141-145`、`recv.go:107-116` 等 | 分片 32＋协调者 |

---

## 五、P1 清单（严重）

共约 78 条，按主题归类。完整细节见各分片报告。

### 5.1 静默数据丢失 / 静默不一致（**优先级应高于普通 P1**）

这类缺陷不崩溃、不报错、不告警，只是**少数据或错数据** —— 最难发现，发现时往往已积累很久。

- ✅ **分页归并序与存储键序不一致 → 静默漏行**：`pkg/slot/proxy` 的 `userMergeHeap.Less`（`identity_rpc.go:251`）
  与 `pluginBindingMergeHeap.Less`（`plugin_binding_rpc.go:714+`）用 Go 字符串 `<`（内容优先），
  而 `keycodec.AppendString`（`pkg/db/internal/keycodec/codec.go:51`）写**大端 uint16 长度前缀在内容之前**，
  使 Pebble 键序为长度优先。两者在最普通输入上就相反（`"b"` vs `"ab"`）。
  分片归并 + 游标分页 → **存储序在游标之前、内容序在其之后的行被整段跳过**。
  影响权威用户分页、插件绑定分页，以及任何"遍历全部用户/绑定"的运维动作（缩容盘点、权限审计、批量迁移）。

  **根因已由分片 30 从存储侧独立确认，且比代理侧更清楚**：同一个 `pkg/db/meta` 包里
  **并存两个相反的比较器** —— `keycodec.AppendString` 是长度优先，而
  `normalizeSubscriberUIDs` 用 `sort.Strings`（内容优先）排序，
  **文档注释却承诺"UID 升序"**，且导出给调用方的游标是**裸字符串**。
  → 这不是代理层单点笔误，而是**存储层的排序契约本身自相矛盾并对外泄漏**。
  （分片 23＋30＋协调者。`runtimeMetaMergeHeap`/`channelMergeHeap` 用自定义比较函数，**尚未核实，列为待办**。）
- **"权威读"无 ReadIndex、无 leader lease，且 `CheckQuorum` 全仓未启用** →
  被分区的旧 leader **无限期**返回陈旧数据，调用方毫不知情（分片 23）
- **`committed` 投递游标只由 replay 推进、热路径从不推进** → 每条消息被重复投递一遍；
  线上唯一去重是 **256 条进程内 LRU**（分片 02＋13）
- **observation delta 无 assignment 删除 tombstone** → `RemoveSlot` 后 follower 缓存永远残留，
  旧 Slot 副本永不关闭；而唯一的全量修复路径 `SyncAssignments` 挂在**从未接入循环的 `observeOnce`** 上
  （分片 17＋16 两个分片独立命中同一处死代码）
- `cloneSlotAssignment` 丢失 `PreferredLeader` / `LeaderTransferCooldownUntil`（分片 17＋20 独立命中）
- `ListSlotAssignments` 只读降级路径把**陈旧** HashSlotTable 装回 Router，
  且 `Router.UpdateHashSlotTable`（`router.go:40-46`）**完全不比较 version**（分片 16＋17）
- conversation 同步 unread 整数截断 + `LastMsgSeq`/`ReadToMsgSeq` u64→u32 截断（分片 09）
- `DeviceQuit` 吞掉每个设备的全部错误并恒返回 nil → **强制下线静默失败**（分片 10＋01）
- delivery subscriber resolver 把**所有**错误当成版本 0 → 陈旧 tag 被"降级为合法"，
  订阅者变更后仍按旧分区投递（分片 09）
- `reconcileOrphanedHashSlotMigrationStates` 缺 source-leader 闸门与 fence 校验 →
  本地 hash-slot 表落后时会解除**进行中**迁移的 fence → **写丢失**（分片 17，需迁移开关开启）

### 5.2 永久卡死 / 无操作员逃生口

- `pendingFollowerApplyEffectID` 是唯一无生命周期兜底清除的栅栏 → 命令投递失败即该 follower **永久停止复制**（分片 25）
- 卡在 verify / clear_fence 阶段的 channel 迁移任务**无法 abort** → 永久堵死该 channel 后续迁移与相关节点缩容（分片 07）
- 迁移任务无重试上限：drain 目标不可达时写栅栏被无限期反复重建（约 98% 占空比），该 channel 长期不可写（分片 12）
- `Runtime.Start()` 失败清理在 ctx 已取消时可完全不 Kill 已启动插件进程，
  且此后 `Stop()`/`Restart()` **永久变成静默空操作**（分片 11）
- run loop 死亡**静默**：`setError` 置 `started=false` → 此后 `Propose` 返回 `ErrNotStarted`，
  与"从未启动"一致；`Status` 无致命错误字段，管理 RPC 读到的是 `leader=0 / role=unknown`（分片 19）
- 网关准入只由状态跃迁事件驱动，`/readyz` 有自愈回退而网关没有 →
  可形成"readyz 说 ready、网关拒绝所有连接"的黑洞节点（分片 01）

### 5.3 资源泄漏与无界增长
见 **PAT-5**、**PAT-7** 全部条目。补充：`deliverytag` 缓存的 `CleanupExpired` **全仓零调用方**，
而 `BuildEphemeralTag` 每条临时频道消息净增一条（分片 13＋01 独立命中）。

### 5.4 无超时扇出
见 **PAT-6**。最尖锐：**缩容状态查询的 RPC 目标正是"正在被缩掉的那个节点"**（分片 07）。

### 5.5 性能瓶颈（热路径）
- `pkg/db` 全部 17 处写路径逐请求 `Commit(true)`，**Group-Commit 引擎建成却零调用**，
  且 `ConfigureCommitCoordinator` 是**只写不读的 setter** —— 生产代码调用了它却毫无效果（分片 29）
- `ReadReverse` 把"最近 N 条消息"退化成从 seq 1 的**全量正序扫描 + 全量物化**（分片 29）
- LEO 冷恢复是每频道 O(全部历史) 正向扫描，持 `appendMu` 阻塞首个 append（分片 29）
- `pkg/db/meta` 单把**非 RWMutex** 全局锁串行化全节点频道元数据读（协调者自查，见 PAT-9）
- 激活缓存 16 分片被单把全局 `c.mu` 串行化，且**持锁遍历全部 16 分片**（分片 12，见 PAT-9）
- 全局 `sendCoordMu` 跨同步网络 RPC 与响应处理（含落盘）被持有 →
  一个不可达 peer 即可让全节点复制发送串行卡顿（分片 26）
- 稳态下每 200ms 对每个本地 Slot 的整个 meta keyspace 做一次全量 Pebble 迭代（分片 16）
- 缩容排空 O(N²)：每次 `/advance` 做 3 次全量 channel 元数据表扫描，只为创建至多 1 个迁移任务（分片 07）
- Leader 均衡规划最坏 O(N²·A²)，热循环内每对 (source,target) 重新分配并排序全部 assignment；
  实测 50k slots 下**一次空转 tick 耗 15.9ms / 5.2MB / 51,815 次分配**（分片 20）
- Onboarding job 永不删除：每次状态转移持写锁全表扫描解码排序，且全量进入每次 Raft 快照（分片 20）

---

## 六、架构层面结论

### 6.1 v1 / v2 双架构：v2 完全未接入生产

已核实：`cmd/wukongim/main.go` 只构造 `internal/app`；全仓 **零个** `internalv2/app` 的 importer；
`pkg/channelv2` / `pkg/clusterv2` / `pkg/controllerv2` 仅经 `internalv2` 可达。
规模差距：`internal` 83,986 行 vs `internalv2` 1,185 行；`pkg/channel` 17,454 vs `channelv2` 7,729。
`internalv2` 共 13 个 commit（`internal/app` 196 个）——**在积极开发但远未可用**。

**建议**：v2 覆盖的功能子集目前仅到 SEND→持久化→sendack 骨架，无投递、无会话、无管理、无插件。
在其达到可替换性之前，v2 的缺陷不应与 v1 竞争修复资源；但**两个问题需立即处理**：
（a）v2 的消息 ID 分配器丢弃了 v1 已有的正确方案（snowflake，重启安全），任何下游域迁移到 v2 都会建立在必然冲突的 ID 上；
（b）v2 的 `-race` 未纳入 CI —— 分片 18 实测 `go test -race ./pkg/clusterv2/... ./internalv2/...` **14 个测试失败**，
竞态报告直指 `node_lifecycle.go:100` 与 `node_snapshot.go:72`。

### 6.2 文档一致性

**`AGENTS.md` 目录结构已漂移**（逐项核实）：
- 文档中有、仓库中**不存在**：`ui/`、`learn_project/`
- 仓库中有、文档**未记录**：`internal/access/plugin`、
  `internal/runtime/{channelmigration,channelplane,channelretention,deliverytag,plugin}`、
  `internal/usecase/{plugin,testdata}`、`pkg/db/inspect`、
  `internal/bench/{capacity,workload,metrics,report,wkproto}`

**`FLOW.md` 与代码不符**（22 个 FLOW.md 中至少 8 处）：
`pkg/controller/FLOW.md` 把死代码 `plane/controller.go` 列为唯一调度入口且描述与其自身实现三处矛盾；
`internal/runtime/channelplane/FLOW.md` 把死代码 Resolver 列为核心组件；
`pkg/cluster/FLOW.md` 的 Controller RPC 清单缺 3 项 —— **且正是无授权检查的那 3 项**；
`pkg/slot/FLOW.md` 明文的 `incomingDeltaSlots` 归属校验在 apply 路径从未被使用。

**`docs/development/CODE_QUALITY.md`** 仅 5 行且已过期：其中"`isSenderDeliveryRoute` 有重复 return"
一条经核实**当前代码中不存在**。

### 6.3 依赖与供应链

- **38 个可达漏洞**（govulncheck）：`GO-2025-3563`（net/http chunked 请求走私）、
  `GO-2025-3503`（`golang.org/x/net@v0.25.0` 代理绕过，已修版本 v0.36.0）、
  `GO-2025-3420`（跨域重定向泄露敏感头）、`GO-2025-3373`（x509 名称约束绕过，
  经 `internal/access/manager/server.go:311` 可达）
- **工具链锁死**：`cockroachdb/swiss` 使项目无法在 Go 1.24+ 编译
- **协议加密弱点**：`pkg/protocol/wkprotoenc/crypto.go:266` 用 MD5 派生 AES 密钥并截断为
  16 个十六进制字符（约 64 bit 有效熵，无 KDF）；`:315` 另一处 MD5 用于摘要

### 6.4 CI 跳过的"flaky 测试"有真实根因（分片 19 的意外收获）

`.github/workflows/ci.yml` 明确 skip 了 4 个 raft/cluster 测试，注释称其
"timing-sensitive / flaky on shared CI runners"。分片 19 为 `TestThreeControllerVoters`
找到了**真实的时序缺陷**，而非测试自身的问题：

`pkg/controller/raft/service.go:830` 设置 `MaxCommittedSizePerReady: math.MaxUint64`，
**显式关闭了 etcd raft 的 Ready 批量限制** —— 一个 Ready 可以携带全部已提交积压。
而 apply 在 `processReady()` 内部执行，`rawNode.Tick()` 只在其返回后的 `select` 中执行。
于是一次多秒的 apply 突发期间**心跳为零**；又因 `time.NewTicker` 的 channel 容量为 1，
约 100 次被丢弃的 tick 只让 `electionElapsed` 前进 1 ——
**raft 的逻辑时钟永久性地丢失时间**。

三副本集群的 bootstrap / 追赶阶段正是批量 apply 最集中的时刻，
因此 leader 更替表现为"依机器速度而定"的不确定行为。

**结论**：这个测试不该被 skip，它一直在正确地报告一个真实缺陷。
把 `-race` 与这 4 个测试重新纳入 CI，应与代码修复同批进行。

---

## 七、修复优先级建议

**第 0 批（立刻，每条都是一行到数行，封堵未认证远程杀伤）**
1. **`pkg/controller/raft/service.go:957` 的 `errors.Is` 加上 `raft.ErrStepPeerNotFound`** ——
   一行，封堵 P0-1（当前最易触发的致命缺陷）
2. **`pkg/transport/server.go:186` 的 goroutine 加 `defer recover()`** ——
   一行，把 P0-3 ~ P0-10 共 8 条从"进程死亡"降为"请求失败"
3. `pkg/protocol/codec` 的 `WriteString`/`WriteBinary` 改为返回 error（`encodeRecv` 已有 error 通道）
4. 入口层对所有进入协议帧的字符串字段加长度上限 + `http.MaxBytesReader`
5. PAT-2 全部站点改调已有的 `readCollectionLen` / 同类上界判断（纯机械修复，正确函数已在 15 处使用）
6. `applyAddSlot`/`applyRemoveSlot` 的状态相关分支改为幂等 no-op（照抄 FLOW.md 已定的规范）
7. `pkg/gateway/transport/gnet/group.go:540` 改用带 opcode 的拷贝入队（同文件已有 `enqueueCopiedData`）

**第 1 批（认证缺口）**
6. 接上 `VerifyToken`，让 `TokenAuthOn` 可用并改为默认开启
7. `WK_MANAGER_AUTH_ON` 默认改 true，与示例配置对齐
8. `internal/usecase/plugin/host_rpc.go` 三处 `_ string` 改回使用 `callerUID`
9. 默认监听地址由 `0.0.0.0` 改 `127.0.0.1`，并在配置注释中写明信任假设

**第 2 批（数据正确性）**
10. `pkg/slot/proxy` 两处归并堆的比较函数改为与 Pebble 键序一致
11. 权威读引入 ReadIndex 或 leader lease，启用 `CheckQuorum`
12. committed 游标推进 + 毒消息隔离（poison pill quarantine）

**第 3 批（资源与性能）**：PAT-5 全部缓存加上限；PAT-6 全部扇出加超时；
接上 `pkg/db` 的 group commit coordinator；`ReadReverse` 改反向迭代。

**持续**：把 `-race` 纳入 CI（当前不跑，且 v2 实测 14 个测试失败）；引入 `staticcheck`/`gosec` 门禁；
修正 `AGENTS.md` 与 8 处 `FLOW.md`；清理 PAT-8 的死代码。

---

## 八、分片报告索引

全部 32 份原始报告位于 `docs/development/audit/`，另有 7 份协调者独立复核记录（`00-coordinator-*.md`）。
