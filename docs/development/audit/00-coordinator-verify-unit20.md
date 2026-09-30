# 协调者抽检：Unit 20 的 P0 —— Controller Raft 中毒条目导致全集群控制面永久死亡

> 这是目前整个审计中最严重的一条。我对 Unit 20 的核心论断做了**独立逐行复核，完全成立**。
> 判定 **P0**，且应列为总清单第一条。

---

## 缺陷机理（两个半部分，我都亲自核实）

### 半部分 A —— apply 出错 = Raft 运行协程永久退出，无任何重启

`pkg/controller/raft/service.go:942-946`：
```go
for {
	if err := processReady(); err != nil {
		s.setError(err)
		return              // ← 运行协程直接退出
	}
```

`pkg/controller/raft/service.go:1027-1029`（`applyReadyState` 内，apply 已提交条目）：
```go
		if err != nil {
			return 0, err   // ← apply 错误向上传播到 processReady
		}
```

**无重启**：全文件 `go s.run(` **只出现一次**，在 `service.go:372`。
`s.err` 只在 `:399`（启动时）被清空、在 `:607-608` 被读取，没有任何看门狗或重建逻辑。

→ **任何一个已提交条目在 apply 阶段返回 error，该节点的 Controller Raft 就此永久停摆。**

### 半部分 B —— `applyAddSlot` 对**状态相关**条件返回 error（而非仅对畸形输入）

`pkg/controller/plane/statemachine.go:617-637`：
```go
func (sm *StateMachine) applyAddSlot(ctx context.Context, req AddSlotRequest) error {
	if req.NewSlotID == 0 || req.PreferredLeader == 0 || !containsPeer(req.Peers, req.PreferredLeader) {
		return controllermeta.ErrInvalidArgument      // ← 618：这条是输入校验，合理
	}

	table, err := sm.store.LoadHashSlotTable(ctx)
	if err != nil { return err }
	if len(table.ActiveMigrations()) > 0 {
		return controllermeta.ErrInvalidArgument      // ← 626：状态相关！
	}
	if len(table.HashSlotsOf(multiraft.SlotID(req.NewSlotID))) > 0 {
		return controllermeta.ErrInvalidArgument      // ← 629：状态相关！
	}
	if _, err := sm.store.GetAssignment(ctx, uint32(req.NewSlotID)); err == nil {
		return controllermeta.ErrInvalidArgument      // ← 632：状态相关！
	} else if !errors.Is(err, controllermeta.ErrNotFound) {
		return err                                    // ← 635：存储错误
	}
```

**关键区别**：第 618 行是对**请求内容**的校验 —— 这种错误本该在 propose 之前就挡掉。
而 626 / 629 / 632 三行检查的是**apply 时刻的集群状态**，此时条目**已经提交并复制到所有副本**。

---

## 为什么是"中毒条目"而不是"某个节点挂了"

Raft apply 是**确定性**的：所有副本对同一条已提交条目、在同一状态上执行，
必然得到**同一个** `ErrInvalidArgument`。于是：

1. 条目提交 → 复制到全部 controller 副本
2. 每个副本 apply → 命中 626/629/632 之一 → 返回 error
3. 每个副本 `processReady` 返回 error → `setError` → `return` → 运行协程退出
4. **全集群控制面同时死亡**
5. 条目已持久化在 Raft 日志中 → **重启后重放，再次死亡** → 不可自愈

---

## 触发可达性（Unit 20 给了三条，我认为第一条最现实）

**并发 AddSlot**：两个并发的管理面 add-slot 请求
（`internal/access/manager/slot_add_remove.go:24` → usecase → `Cluster.AddSlot`），
链路上无互斥、无幂等键，两者都能通过 leader 侧基于同一份缓存 `GetHashSlotTable()` 的前置检查，
于是两条 AddSlot 都被 propose 并提交；第二条 apply 时命中第 632 行（assignment 已存在）→ 全集群死亡。

**严重性放大（已由 Unit 05 核实）**：`WK_MANAGER_AUTH_ON` 代码默认 **false**，
管理面全部端点（含 add-slot）在默认配置下**无需认证**。
即：**任何能访问管理端口的一方，发两个并发 HTTP 请求，就能永久摧毁整个集群的控制面。**

---

## 项目自己已经知道这个坑，但只修了一个路径

Unit 20 指出 `pkg/controller/FLOW.md` 的"避坑清单"专门就 `NodeOnboardingJobUpdate`
警告过这一危险，并把那条路径改成了 no-op —— **但 AddSlot / RemoveSlot 没有同样处理**。
`controller_host.go:101,152` 把原始状态机直接传入，没有任何错误过滤包装层。
这说明这是**已知模式下的遗漏**，不是无人意识到的问题，判定 P0 有充分依据。

---

## 顺带确认 Unit 20 的 P2（同一函数内 uint64/uint32 不一致）

同一段代码里：
- `:629` 用 `multiraft.SlotID(req.NewSlotID)` —— 完整 uint64
- `:632` `uint32(req.NewSlotID)`、`:644` `SlotID: uint32(req.NewSlotID)`、`:650` 同样截断

而 `:618` 只校验 `NewSlotID != 0`，**没有上界**。
`NewSlotID = 2^32` 时 uint32 侧截断为 0，而 uint64 侧仍是 2^32 —— 槽位身份在
hash-slot 表与 assignment 存储之间**被劈成两个不同的值**。
这与 `pkg/cluster/codec_control.go` 和 `internal/access/node/*_codec.go` 的
`uint64 → int/uint32` 问题**同属一个模式级根因**，汇总时应合并。

---

## 修复方向

apply 阶段对**状态相关**条件必须是幂等 no-op（记录并跳过），**绝不能返回 error**；
error 只应保留给真正的存储故障。同时 `service.go:942` 的 `processReady` 失败处理应区分
"apply 业务错误"（跳过并继续）与"存储/传输故障"（可重试），而不是一律终止运行协程。
FLOW.md 既然已为 `NodeOnboardingJobUpdate` 定下 no-op 规范，应把该规范推广到全部 apply 分支。
