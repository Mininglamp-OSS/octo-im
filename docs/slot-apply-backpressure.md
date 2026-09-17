# Slot apply 背压修复与验证

## 基线与来源

PR 目标分支为 `Ranwanglc/octo-im:fix/slot-apply-backpressure`，从
`v2.2.5-20260422-fix1`（`02cf8cd19877e307a31cc13412155579a36bf0a4`）创建。
实现分支为 `fix/slot-apply-backpressure-impl`。

移植 [an9xyz/octo-im 的修复分支](https://github.com/an9xyz/octo-im/tree/acf5c5273e2706ee54469182ca59799396b40872)
在 2026-09-16 04:06:09 UTC 至 2026-09-17 04:06:09 UTC 内的全部 9 个提交。
对方也基于同一 tag；使用 `cherry-pick -x` 保留作者和原始提交信息。
业务代码与来源分支 `acf5c527` 一致，另增加并发完成通知与重启持久性测试。

| 原始修复提交 | 问题与改动 |
| --- | --- |
| `01752642` | Apply、AppendLogs、TruncateLogTo 改用每个 Raft slot 的锁，避免同一 Pebble 分片中的无关 slot 互相阻塞；同一 slot 仍串行执行。 |
| `7f96d913` | AddSubscribers / RemoveSubscribers 通过 BatchDB 合并提交，并等待同步落盘完成。 |
| `bc41bf90` | 移除订阅者、添加/设置黑名单时，通过 Raft 提交会话清理，避免 HTTP 路径逐用户等待业务 Apply。 |
| `cf0f8acd` | CommitWait 保存局部等待 channel，避免 worker 释放 Batch 时清空字段导致数据竞态或丢失等待目标。 |
| `acf5c527` | AppendLogs / SetAppliedIndex 使用合并提交；关闭 slot 存储时先停止 BatchDB worker，再关闭 Pebble。 |

同时移植测试提交 `52c6bb5b`、`8cf0ab6e`、`6cdaa498`、`25158f3b`。

## API 语义与范围

API 请求/响应格式和配置项不变。订阅关系变更仍等待 Apply；这些 API 的会话清理成功返回只表示
Raft 接受了提案，不能理解为会话已经删除或已经获得多数副本的持久化确认。
显式同步删除会话的方法仍保留。

这些改动减少共享锁阻塞、同步落盘次数和请求中的串行等待。它们没有改变
`pkg/wkdb/conversation.go` 中昂贵的会话迭代器与范围删除，也没有加入清理速率控制。
因此不保证持续高负载下没有超时。关闭顺序修复也不等于完整实现全局请求和 Raft worker 的优雅排空。

## 自动化验证

本地 Go 版本为 1.25.0，Linux amd64。运行命令：

```sh
go build -buildvcs=false -o wukongim-pr .
go test -count=1 -timeout=1m ./pkg/cluster/slot ./pkg/cluster/store
go test -race -count=3 -timeout=3m \
  -run 'Test(BatchCommitWaitConcurrentCompletion|.*Subscriber.*|DeleteConversationAsync.*|Apply.*|RaftMetadataWritesUseGroupCommit|GroupedSlotWritesSurviveReopen)$' \
  ./pkg/wkdb ./pkg/cluster/slot ./pkg/cluster/store
go test -count=1 -timeout=30s -p=2 ./...
```

构建、slot/store 包测试及三轮上述 race 测试通过。构建关闭 VCS stamping 是为适配本地 worktree
环境的 VCS 状态读取错误，不影响编译的业务代码。

- 将 `TestBatchCommitWaitConcurrentCompletion` 通过 Go overlay 加载到原始 tag 后，race detector
  检出 `CommitWait` 读取 `waitC` 与 `Batch.release` 清空字段之间的竞态；修复后每轮 1,600 次提交全部完成，数据逐条读取一致。
- `TestGroupedSlotWritesSurviveReopen` 验证 16 个 slot 并发写入同一物理分片，关闭并重新打开数据库后，日志内容、applied index 和 term start index 均完整。
- 移植的测试验证跨 slot 隔离、同 slot 串行、订阅者/元数据等待 group commit，以及异步清理的提案内容和错误返回。
- 原始 tag 加载跨 slot 隔离测试后，第二个 slot 无法进入 Apply，测试失败；修复分支通过。

**全量测试未通过。** 首次使用每包 5 分钟超时，确认事件测试一直等待；最终代码复核改为每包
30 秒超时。修复相关包通过，但仍有以下基线或环境问题：

| 测试范围 | 失败及原始 tag 对照 |
| --- | --- |
| `pkg/wkdb` | 原始 tag 也存在 8 个相同失败：TestCacheStats、TestUpdateConversationsInCacheWithNewConversation、TestDeviceCacheStats、TestPermissionCacheStats、TestSearchDevices、TestTruncateLogTo、TestAddPluginUsers、TestGetPluginUsers。 |
| `internal/track`、`internal/user/event` | 原始 tag 同样有 TestMessageString 断言失败、TestUserEventPool_AddConnectEvent 超时。 |
| `pkg/cluster/cluster` | 原始 tag 同样有 TestAdaptiveSendQueue_Shrinking 失败及 TestSendBatchOptimization 空指针。 |
| `internal/server`、`pkg/wknet` | 原始 tag 同样发生监听端口占用。 |
| `pkg/wkserver`、`pkg/wkserver/test` | 请求、重连或监听测试失败，受固定端口与测试时序影响；原始 tag 单独运行 wkserver 也出现 TestServerRoute 空指针，具体失败不完全相同。 |
| `pkg/raft/raft` | TestPropose 间歇性缺失日志并越界；原始 tag 重复运行也复现。该包及其依赖没有本 PR 的代码改动。 |

这些结果不能作为全量回归通过的依据；本 PR 没有扩展修改上述无关问题。

## 三节点验证

使用本 PR 源码直接构建的二进制，SHA-256：
`cd5037fc226c33d1e8348b233faeb0e978e76a2ba3b1ed3897da42f3cfc32cc2`。
3 个独立本地进程、64 slots、3 副本、8 个业务库分片和 8 个 slot 库分片，每进程
`GOMAXPROCS=2`。没有注入延迟、网络故障或磁盘限速；HTTP 客户端 read timeout 为 45 秒。
测试在共享开发机执行，不作为生产容量指标。

| 场景 | 建群 | 加订阅 | 移除订阅 |
| --- | --- | --- | --- |
| 并发 8，16 个群，每群 50 个相同用户；建群→移除→添加→移除 | 16/16 成功 | 16/16 成功 | 32/32 成功，p50 190 ms，最大 345 ms |
| 新集群，并发 32，64 个群，每群 1,000 个相同用户；建群→移除 | 37/64 成功 | — | 37/37 成功，p50 515 ms，最大 2,076 ms |

高压场景仍有 27 次建群返回 HTTP 400，日志包含 `context deadline exceeded` 和
`propose batch until applied timeout`；没有 45 秒客户端超时。建群失败后不会继续提交对应群的
移除请求，不能用两种场景的成功请求数量直接推算吞吐提升。

两个集群均在请求结束后确认 192 个 slot 副本的 applied index、local last index、leader last index
一致，全部节点正常退出（退出码均为 0）。
