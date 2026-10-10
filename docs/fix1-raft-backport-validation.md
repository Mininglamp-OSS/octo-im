# fix1 的 Raft、消息与会话恢复回迁

## 范围与冲突处理

目标是 **branch** `v2.2.5-20260422-fix1` 的 `44034be2c1d1fd7149084aaf0fd45cdf158c80e4`，
不是同名 tag `02cf8cd1`。来源为 `fix/raft-orphan-learner-bugs` 的
`153a5f7c0581bbd903a37a4183401ebcc4a54cef`。

按依赖顺序用 `cherry-pick -x` 回迁以下提交；前六项为 merge commit，使用 `-m 1`。

| 原 PR | 来源提交 | 功能 |
| --- | --- | --- |
| #40 | `11874978` | 权威频道消息边界、槽 Apply 读取屏障 |
| #41 | `92997907` | 按本地成员身份确定 Raft 角色 |
| #42 | `c90c5dcb` | 持久化 term/vote |
| #46 | `4834ff63` | 消息重试幂等、canonical ID/sequence、持久化 ACK、proposal RPC v2 |
| #47 | `acf08f7e` | 完整会话同步、路由刷新和有预算的重试 |
| #43 | `4ce16cac` | 投票资格、learner 晋升、成员恢复、Apply 重试 |
| #48 | `974cb382` | 发送转发、队列与 peer 重连恢复 |
| #49 | `aa83b898` | 从物理 socket owner 恢复用户逻辑连接 |
| #53 | `f00056b0` | 隔离连接统计使用的 slice，修复竞态 |
| #55 | `153a5f7c` | 后台配置同步、无发送选主/补副本、忙槽条件写入和 orphan expansion marker 恢复 |

目标已有的 #50 保留；来源 #51 是相同背压修复的移植，因此跳过。没有纳入未合并的其他 PR。
严格会话读取、配置同步和发送恢复作为完整依赖组回迁。

实际 cherry-pick 冲突有两处：

- `pkg/wkdb/wukongdb.go`：目标已经保存局部 `waitC`，仅补上来源的解释注释。
- `pkg/raft/raft/node_step.go`：同时保留来源的成员/选举检查，以及目标的未初始化、
  follower、candidate、learner 明确拒绝本地提案。更新隐式 leader 测试的过期注释。

回迁后的生产代码与来源 `153a5f7c` 完全一致。保留目标自己的 `slot-apply-backpressure.md`；
本次另加可复用的真实进程兼容性脚本，补强选主测试初始化屏障并修正 Go E2E 的端口预留。没有改 Go 依赖。

## 验证结果

2026-10-10，Linux amd64、Go 1.25.0、`GOMAXPROCS=4`。每次运行使用独立数据目录；
Go 包间串行，独立运行使用网络 namespace 隔离监听端口。没有复用测试缓存。

| 检查 | 结果与覆盖 |
| --- | --- |
| 编译 | 目标基线、来源 #55 前基线及回迁后的 server 均成功；目标和回迁的所有 Go 测试包编译成功 |
| 来源功能回归 | 来源改动涉及的 75 个测试文件、21 个包、361 个顶层测试全部通过，含成员恢复、持久化投票、消息幂等、会话读取、session 恢复、忙槽配置写入 |
| 目标 #50 回归 | 8 个测试文件、5 个包、86 个顶层测试，`-race -count=3` 全部通过，共 258 次顶层执行 |
| 来源定向竞态 | 18 个包的 178 个测试各三轮通过；Raft 原语 122 个测试、owner/条件提案 5 个测试分别各三轮通过。扩展到旧异步 Raft 集成测试时仍有基线竞态，见下文 |
| 选主测试补强 | 完成初始化后的停机、继续写入、旧节点重启追赶三轮通过；保留全部 ID、sequence、payload 断言 |
| 原有 Go E2E | 修正端口预留后，完整包三次独立启动均通过，覆盖 HTTP 会话和 WebSocket CONNECT/PING |
| 三进程扩容 | 加入后不再发送，12 个频道全部补齐三副本，再停止实际频道 leader；完整读取和无发送恢复通过 |
| 新建三进程 | 12 个频道三副本、完整读取及停机后的无发送恢复通过 |
| 目标旧数据升级 | 用真实目标 branch `44034be2` 创建数据，停机保留 DB 换回迁二进制，再扩容、停机恢复；通过 |
| 故障数据修复 | 用 `f00056b0` 制造配置落后并确认三个入口均为 503，保留 DB 重启回迁二进制，后台恢复及随后停机恢复通过 |
| 原 socket 恢复 | 四进程，socket owner 不在用户槽的三副本中。杀掉用户槽 leader 后，同一 socket 收到消息、响应 PING；重复发送返回相同 canonical ID/sequence |
| 目标清理行为对照 | 目标与回迁二进制分别通过单节点和三节点清理、订阅变更及重启检查，详情如下 |

四种配置恢复场景均由 `test/e2e/channel_config_reconcile.py` 执行：
每种收敛后从三个入口各完整读取 30 次，共 360 次；停机恢复后另外共 88 次完整读取。
校验频道集合、消息 ID/sequence/payload、三副本、配置版本及迁移标记。

新增 `test/e2e/backport_compatibility.py` 只依赖 Python 标准库：

- 清理：24 个用户、3 个频道，每个副本先确认 72 条会话；分别执行移除订阅者、黑名单新增、
  黑名单设置，检查全部副本清空。三节点回迁实测包含 44 个跨 channel/user slot leader 的清理目标。
- 订阅变更：8 并发、16 群、每群 50 人，create → remove → add → remove 的 64 次请求全部成功。
  目标基线和回迁的单节点、三节点四种组合均通过，正常重启后再次直接检查全部副本无残留。
- session：物理 owner 不属于用户槽副本集；停机后新 leader 与物理 owner 仍不同，整个检查不重连客户端。

这些是功能回归和小规模组合检查，耗时不作为生产容量结论；正常重启检查也不等同于掉电持久性测试。

## 全量与扩展检查的限制

**全量测试未通过，不将本 PR 的验证称为全绿。** 已执行两端全量测试及来源整包对照。
初次全量每包 60 秒，来源新增集成用例使 cluster 包超过这一预算，因此补跑 180 秒整包，
提交前再以 120 秒每包执行 `go test ./...`；随后修正该轮发现的 Go E2E 端口重复并单独复测完整 E2E 包。

| 项目 | 实测归因 |
| --- | --- |
| `internal/server` | 两端同包内重复监听 `:5172` 导致 panic；网络 namespace 隔离后仍可复现 |
| `internal/track` | 两端 `TestMessageString` 位图期望不符 |
| `internal/user/event` | 两端原有 `TestUserEventPool_AddConnectEvent` 等待超时；新增连接/session/统计定向测试通过 |
| `pkg/wkdb` | 两端相同的 cache stats、conversation cache、device search、truncate、PluginUser 断言失败 |
| `pkg/wknet` | 两端 `TestEngine` 超时 |
| `pkg/wkserver` | 目标初次全量 `TestReconnect` 失败；回迁初次通过、后续也复现相同失败，保留为基线不稳定项 |
| `pkg/cluster/cluster` | 两端已有批量发送/延迟/背压断言问题；目标的未启动 client 还会 panic。回迁和未修改来源整包均复现 `wkdb.collectMetrics` 的全局 trace 空指针 |
| 扩展 Raft `-race` | `TestElection`、`TestElection2`、`TestPropose`、`TestLogConflict1/2`、`TestProposeUntilApplied` 的异步读写竞争，在未修改目标分支的对应测试复现；raftgroup 的 Election/Propose 同样复现 |
| Go E2E 端口重复（已修正） | 最后一次全量的旧测试分配器把 TCP 和 WebSocket 都分到 `33467`，启动日志明确记录 bind 冲突；改为同时预留四个监听端口，完整包三次独立启动复测通过 |

一次回迁整包运行中，`TestChannelReplicationAfterLeaderConfigChange` 在停机后的槽换主等待超时。
旧测试仅等 leader 非零，可能在初始化的槽均衡尚未完成时停机。单独三轮以及来源的六轮带日志诊断均通过，
这些通过记录不能抹去原始失败。本次为该测试增加初始化前置检查：三个节点的槽分布完成均衡、
没有 learner/迁移标记、各节点发布的配置和 Raft 运行角色一致，然后继续执行原来的故障恢复断言。
补强后单独三轮和提交前全量中的该用例均通过。该用例覆盖完成初始化后的故障恢复；未据此证明所有初始化中断时序。

未扩大生产代码范围去修复上述既有测试设施和通用并发问题。

## 复现入口

输出目录放在有足够空间的文件系统；每个场景的输出目录必须尚不存在。
`backport_compatibility.py` 默认在退出后删除本轮 DB，日志和摘要保留，`--keep-data` 可保留 DB。

```sh
go build -mod=readonly -o /path/to/bin/backport main.go

python3 test/e2e/backport_compatibility.py --binary /path/to/bin/backport \
  --scenario cleanup --nodes 1 --output /path/to/results/cleanup-single
python3 test/e2e/backport_compatibility.py --binary /path/to/bin/backport \
  --scenario cleanup --nodes 3 --output /path/to/results/cleanup-three
python3 test/e2e/backport_compatibility.py --binary /path/to/bin/backport \
  --scenario sessions --output /path/to/results/sessions

python3 test/e2e/channel_config_reconcile.py --binary /path/to/bin/backport \
  --scenario expand --no-post-expansion-send --failover --output /path/to/results/expand
python3 test/e2e/channel_config_reconcile.py --binary /path/to/bin/backport \
  --scenario fresh --failover --output /path/to/results/fresh
python3 test/e2e/channel_config_reconcile.py --binary /path/to/bin/backport \
  --scenario upgrade --legacy-binary /path/to/bin/44034be2 --failover --output /path/to/results/upgrade
python3 test/e2e/channel_config_reconcile.py --binary /path/to/bin/backport \
  --scenario repair --legacy-binary /path/to/bin/f00056b0 --failover --output /path/to/results/repair

go test -mod=readonly -race -count=3 -timeout=180s \
  ./pkg/cluster/slot ./pkg/cluster/store ./pkg/raft/raft ./pkg/raft/raftgroup ./pkg/wkdb \
  -run 'Test(AsyncProposal|LocalProposal|Follower_Propose|Candidate_Propose|BatchCommitWaitConcurrentCompletion|AddSubscribersUsesChannelGroupCommit|RemoveSubscribersUsesChannelGroupCommit|GroupedSlotWrites|Slot|DeleteConversationAsync)'
go test -mod=readonly -count=3 -timeout=120s ./pkg/cluster/cluster \
  -run '^TestChannelReplicationAfterLeaderConfigChange$'
go test -mod=readonly -p=1 -count=1 -timeout=120s ./...
```

361 项来源回归的选择方式为：比较目标 `44034be2` 与来源 `153a5f7c` 的 `*_test.go` 文件，
收集这些文件中的顶层 `Test*`，按对应包执行；没有跳过其中的失败来得到通过数量。
额外的初始化和端口测试设施修正另行复测。
本地证据目录 `tests/artifacts/raft-backport-20261010`（仓库之外）保存二进制 SHA256、
选中的测试清单、完整 `go test -json` 日志、基线对照和各真实进程场景的 `summary.json`。

## 发布边界

沿用来源的升级约束：旧 `/rpc/channel/propose` 被拒绝，消息提案使用 `/rpc/channel/propose/v2`；
发布前停写、排空并升级全部 IM 节点，再恢复写入，不能按透明滚动升级处理。
完整 session 恢复也需要所有参与节点支持 snapshot v2。详见
[消息幂等升级步骤](message-retry-idempotency.md#im-tier-upgrade-runbook) 和
[session 兼容性](user-slot-session-recovery.md#compatibility-and-scope)。

继续保留目标 #50 的成功语义：订阅变更等待 Apply，异步会话清理仅等待 leader 接纳。
消息持久化成功 ACK 使用来源的 committed + durable-applied + canonical identity 条件；
HTTP `/message/send` 仍是入队回执。此次没有把这些不同完成点改成跨槽事务。
