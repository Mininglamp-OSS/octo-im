# Slot apply 背压修复与验证

## 基线与来源

PR #50 的目标为 `Mininglamp-OSS/octo-im:v2.2.5-20260422-fix1`，基线是同名 tag
`02cf8cd19877e307a31cc13412155579a36bf0a4`。
实现分支为 `Ranwanglc/octo-im:fix/slot-apply-backpressure-impl`。

最初移植 [an9xyz 的修复分支](https://github.com/an9xyz/octo-im/tree/acf5c5273e2706ee54469182ca59799396b40872)
在 2026-09-16 04:06:09 UTC 至 2026-09-17 04:06:09 UTC 内的 9 个提交；对方也基于同一 tag，
使用 `cherry-pick -x` 保留作者和来源。PR 另加回归测试及本文档，并补充修复审查指出的异步提案路由缺陷。
因此最终代码不能再描述为与来源分支完全相同。

| 来源修复 | 改动 |
| --- | --- |
| `01752642` | Apply、AppendLogs、TruncateLogTo 按 Raft slot 加锁，避免物理分片上的无关 slot 相互阻塞。 |
| `7f96d913` | AddSubscribers / RemoveSubscribers 使用同步 group commit。 |
| `bc41bf90` | 移除订阅者、添加/设置黑名单时异步提交会话清理，避免逐用户等待业务 Apply。 |
| `cf0f8acd` | CommitWait 保存局部等待 channel，避免 worker release 引发数据竞态。 |
| `acf5c527` | Raft 日志/applied index 使用 group commit；slot 存储先停 worker，再关 Pebble。 |

同时保留来源测试 `52c6bb5b`、`8cf0ab6e`、`6cdaa498`、`25158f3b`。

## 本次补充：修复异步清理的假成功

HTTP 请求路由到 channel slot leader，但删除会话按 uid 路由到 user slot，两者 leader 可以不同。
旧 `Slot.Propose` 只向本地 Raft 发提案；follower/learner/candidate 忽略提案后返回 nil，导致
HTTP 200 并不代表清理已进入 Raft 日志。原同步 `ProposeUntilApplied` 的转发能力没有保留到异步路径。

补充修复统一了非等待 Apply 的单条/批量提案入口：非 leader 通过已有 `SendPropose` 通道转发，
等待 leader 的接纳响应。实际执行 Step 时如果已经不是 leader，则返回 `ErrNotLeader`，避免入口
检查之后发生角色变化又静默成功。没有 leader、停止、取消或转发超时都会返回错误；超时/停止时清理
转发 waiter。没有本地 Raft 实例仍明确报错，本次没有新增非副本节点的 slot RPC 路由。

这些改动没有新增 wire/storage 格式，没有自动重放超时提案。既有异步已读位置更新也使用这个共享入口。

## API 语义与范围

请求/响应格式和配置项不变。订阅关系变更仍等待 Apply；会话清理成功返回只表示 **leader 已接纳提案**，
不表示已获得多数副本持久化确认，也不表示业务数据库已完成删除。响应丢失或超时可能发生在接纳之后，
错误不能一概理解为“没有执行”，不能跨越后续重新入群等操作盲目重放旧删除。

**本 PR 暂不解决多节点高并发下仍出现的请求超时和会话清理积压。** 本次修复的是多节点异步提案
静默丢弃却返回成功的正确性 blocker，不将该回归排除在修复范围之外；小规模三节点正确性检查也不是
多节点压力问题已经解决的证明。

本 PR 同样不保证单节点持续高压下无超时。会话扫描、Pebble 范围删除和清理速率控制仍需后续优化。
关闭 worker 的先后顺序调整也不等于完整实现全局优雅排空；业务 DB 的关闭/排空和共享 batch
错误处理等非 blocking 审查项暂未扩展修改。leader 接纳后故障、跨 slot 的业务时序也不在此承诺完整解决。

## 自动化验证

Go 1.25.0，Linux amd64。新回归覆盖：

- follower/learner 经真实 RaftGroup 事件循环转发，覆盖 Propose、ProposeTimeout、ProposeBatch、
  ProposeBatchTimeout；leader 的 Apply 被阻塞时提案仍可返回，但实际日志内容必须正确到达存储 Apply。
- 本地和转发请求在最终 Step 前发生 leader 变化时返回错误。
- 未初始化（没有角色处理器）及 follower/candidate/learner 拒绝本地提案且不增加日志；leader 正向对照确实追加日志。
- 原 tag 的单节点隐式 leader 初始化可正常提案，不因配置 Role 仍为 Unknown 而被误拒绝。
- 无 leader、已经取消的 context、转发超时、停止及空批次；超时 waiter 不残留。
- 原来仅检查“忽略 follower/candidate 提案”的测试更新为明确要求错误。

```sh
go build -buildvcs=false -o wukongim-pr .
go test -race -count=3 -timeout=90s ./pkg/raft/raftgroup ./pkg/raft/raft \
  -run 'Test(AsyncProposal|LocalProposal|Follower_Propose|Candidate_Propose)'
go test -count=1 -timeout=30s -p=2 ./...
```

定向回归及三轮 race 检查通过。用 Go overlay 将提案入口和 Node.Step 换回 `9e1e3ae3` 的旧实现，
同一组转发/角色测试失败，能检出原 blocker。既有 slot 隔离、同步 group commit、CommitWait 和
重启测试仍保留；干净关闭再打开不等于验证掉电/崩溃持久性。

本机运行中，`pkg/raft/raft`、`pkg/raft/raftgroup`、`pkg/cluster/slot`、
`pkg/cluster/store` 和 `test/e2e` 通过。以上为本机确认通过的验证范围，不代表全量测试已通过；
全量验证结论待独立环境确认。

最终二进制的单节点回归：8 并发、50 人/群、16 群，64/64 请求成功，移除 p50 15.4 ms；
另验证三类清理操作的 36 条真实会话，停止前与重启后均无残留。这是小规模回归，不是高负载容量承诺。

## 三节点正确性对照

使用未注入延迟的本地三节点，64 slots、3 副本、8 个业务及 slot DB 分片，每进程 GOMAXPROCS=2。
24 个用户、3 个群分别覆盖移除订阅者、黑名单新增、黑名单设置。先通过直接读取数据库的接口确认
72 条会话存在于全部三个副本，再调用清理；确保 channel leader 与 user leader 不同的用户实际存在。

旧 head `9e1e3ae3` 的六个创建/清理 HTTP 请求均返回 200，但每个副本残留 48 条会话，对应 48 个
跨 leader 的清理目标。这进一步说明旧压力实验只检查 HTTP 和 slot 追平是不充分的。

修复后核验三个副本的会话均删除，并在正常停止/重启后复查。此场景验证异步清理路由与落库正确性，
没有据此声明高并发、多节点故障切换或崩溃持久性问题已全部解决。

## 历史单节点压力结果（补充修复前的 9e1e3ae3）

这些数字来自同一台共享开发机的一个 WuKongIM 进程、64 slots、单副本、GOMAXPROCS=2，
不是本次新提交的容量标定。各群共享同一批合成用户，客户端读取超时 45 秒。

- 8 并发、50 人/群、16 群，create→remove→add→remove：原 tag 与 PR 均 64/64 HTTP 成功，
  移除 p50 从 472 ms 降至 13 ms；最终数据库和重启查询均确认无残留。
- PR 的 16 并发、100 人/群、256 群：1,024 次 HTTP 全部成功，移除 p50 331 ms。
- 32 并发、1,000 人/群、64 群，create→remove：原 tag 创建 64/64 成功、移除 64 次均超过
  客户端 45 秒，但后台最终完成；PR 两轮分别创建 35/64、41/64 成功，移除 27/35、28/41 成功，
  失败请求包含服务端 Apply 超时。PR 降到 8 并发仍有 13/64 创建失败。
- 成功移除的最终会话均删除；失败请求仍可能保留会话，不能算作业务完成。

早期三节点压力测试的 HTTP 成功率和耗时受静默丢提案影响，不能再作为完整清理工作量下的性能证据。
修复后也不能沿用这些旧数字宣称多节点超时已解决。
