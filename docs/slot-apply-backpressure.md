# Slot apply 背压优化：迁移到 orphan-learner 修复分支

频繁建群和增删订阅者会产生大量 slot 日志、订阅者写入和用户会话清理。
本次迁移缩小存储锁范围、合并同步落盘，并让订阅者移除接口在会话清理提案被 leader 接纳后返回，减少逐用户等待 Apply 的延迟。

## 来源与基线

- 目标分支：`Mininglamp-OSS/octo-im:fix/raft-orphan-learner-bugs`，基线 `aa83b8988a7420b319fa3f3cbcea14a0b555b33d`。
- 来源：[PR #50](https://github.com/Mininglamp-OSS/octo-im/pull/50)，head `93c6331ad2a60af26902ff541bb12d2e010296a3`，其基线为 `02cf8cd19877e307a31cc13412155579a36bf0a4`。
- #50 包含 an9xyz 的背压优化，以及后续 `35ac01ae80d4839ed5072423cc926318dcc3364a` 的异步提案路由修复。本次按最终净改动适配到目标分支，保留来源归属。
- 目标分支已通过 #44 修复 `Batch.CommitWait` 完成通知竞争，因此保留目标 `pkg/wkdb/wukongdb.go`，补入其并发回归测试。
- `Node.Step` 保留目标分支的选举成员发现、非 voter 防护和持久化状态逻辑，叠加非 leader 提案拒绝。slot 存储保留 `SaveHardState` 和 HardState 恢复。目标分支已有的 Apply 重试、消息及会话恢复实现继续保留。

## 行为变化

1. `AppendLogs`、`Apply`、`TruncateLogTo` 按逻辑 slot 互斥，同一物理 Pebble 分片上的其他 slot 可继续处理。
2. 订阅者增删、slot 日志和 applied index 使用现有 BatchDB 的同步 group commit；返回前仍等待对应存储提交。关闭 slot 存储时先停止 batch worker，再关闭 Pebble。
3. `/channel/subscriber_remove`、`/channel/blacklist_add`、`/channel/blacklist_set` 使用 `DeleteConversationAsync` 提交用户会话清理。
4. 异步单条/批量提案在本地不是 leader 时转发到 slot leader；实际 Step 时 follower、candidate、learner 和未初始化节点拒绝提案。未知 leader、取消、停止及转发超时返回错误，并释放转发 waiter。

HTTP 请求/响应、Raft 消息和磁盘格式不变，无新增配置项。

## 异步清理的边界

清理成功仅表示 **leader 已接纳提案**，不表示多数派已持久化，也不表示业务数据库已删除。
channel slot leader 与 user slot leader 不同时，也必须将清理提交到实际 user slot leader，不能在 follower 上忽略后返回成功。
没有本地 slot Raft 实例的节点仍明确报错，本次未扩展非副本节点路由。

超时可能发生在提案已接纳之后，调用方不能据此假定没有副作用，本实现也不自动跨越后续业务操作重放清理。
leader 接纳后、提交前崩溃，以及移除/重新加入之间更强的业务顺序保证，需要另行设计。

**本次暂不解决多节点高并发下的请求超时和后台清理积压。**
单节点也受 CPU、磁盘、群大小和会话数据量限制；按用户扫描会话、Pebble 范围删除、清理限流、完整关闭排空和共享 batch 错误处理等后续优化不在迁移范围内。

## 验证方法

对本次组合代码运行包测试、定向 race 检查和本地真实节点实验。
单元测试覆盖不同 slot 并行/同 slot 互斥、group commit、存储重开、异步清理命令、follower/learner 转发、阻塞 Apply、角色变化、未知 leader、取消/超时/停止及隐式 leader 初始化；同时运行目标分支已有的选举、成员、HardState 和 Apply 重试测试。

主要命令（Go 1.25.0）：

```sh
go build -buildvcs=false -o wukongim .
go test -count=1 -timeout=120s ./pkg/raft/raft ./pkg/raft/raftgroup ./pkg/cluster/slot ./pkg/cluster/store
go test -race -count=3 -timeout=120s ./pkg/raft/raft ./pkg/raft/raftgroup ./pkg/cluster/slot ./pkg/cluster/store ./pkg/wkdb \
  -run 'Test(AsyncProposal|LocalProposal|ElectionMembership|Membership|LearnerPromotion|SingleVoter|HardState|VoteSurvives|LeaderReadWaits|ApplyRetry|ApplyBackoff|BatchCommitWaitConcurrentCompletion|AddSubscribersUsesChannelGroupCommit|RemoveSubscribersUsesChannelGroupCommit|GroupedSlotWrites|Slot|DeleteConversationAsync)'
go test -race -count=3 -timeout=120s ./pkg/cluster/slot
go test -race -count=3 -timeout=120s ./pkg/cluster/node/clusterconfig \
  -run 'Test(DurableVote|ApplyRetry|ApplyMalformedCommand)'
go vet ./internal/api ./pkg/raft/raft ./pkg/raft/raftgroup ./pkg/cluster/slot ./pkg/cluster/store ./pkg/wkdb
```

执行了 `go test -count=1 -timeout=60s -p=2 ./...`；全量检查未全部通过，确认通过的范围见本 PR 描述。

#50 描述中的约一小时测试使用的是来源分支代码，仅作为历史参考，不作为本次组合代码的长时间测试证据。

## 本次三节点清理验证（2026-09-17）

三个真实节点，64 slots、三副本，每节点 `GOMAXPROCS=2`。24 用户 × 3 群，共 72 条会话预先确认写入每个副本；覆盖移除订阅者、黑名单新增/设置，包含 48 个 channel/user slot leader 不同的组合。6/6 业务请求成功，三个副本均清零，正常停止/重启后仍清零，全部进程正常退出。此项仅验证跨 leader 清理正确性。

## 本次单节点阶梯验证（2026-09-17）

共享开发机，16 个逻辑 CPU、约 30 GiB RAM，单个真实 WuKongIM 进程，`GOMAXPROCS=16`，64 slots、单副本，业务/slot DB 各 8 分片。100 人/群，群间共享用户；每个 worker 串行执行“创建 → 移除 → 重新添加 → 再次移除”，首次错误即结束该流程，不自动重试，客户端读超时 45 秒。

8、32、64 并发各发起负载 30 秒，并等待已开始的流程结束；同轮保留数据，每档结束后排空并核验，再升压。结果如下（5,280 次负载请求，不含准备及审计请求）：

| 并发 | 成功 / 负载请求数 | 创建 P95 | 移除 P95 |
| ---: | ---: | ---: | ---: |
| 8 | 2188 / 2188 | 0.314 秒 | 0.138 秒 |
| 32 | 1564 / 1564 | 2.195 秒 | 0.573 秒 |
| 64 | 1528 / 1528 | 4.318 秒 | 0.930 秒 |

全部 5,280 次负载请求返回 HTTP 200，未观察到服务端错误响应或客户端超时，完成 1,320 个四步流程。三个阶段均排空；最终检查全部负载用户会话和 1,320 个成功移除群的订阅者，正常停止/重启前后均无残留，12 条保留对照会话均存在，进程退出码均为 0。另用预存的 36 条会话覆盖移除订阅者、黑名单新增/设置三类清理。

这是组合代码的一轮短时回归，未覆盖千人群或长时间持续压力，不作为生产容量或无超时承诺。
