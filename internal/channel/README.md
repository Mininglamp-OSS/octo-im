
## 描述

频道事件

## 成员版本与分发重试

成员变更通过 `TagManager.UpdateChannelTag` 原子发布新版本和频道映射。
分发按连续的 TagKey 分段处理（例如 A/B/A），固定本次任务的收件人集合，
查询旧版本失败时不会改用当前成员集合。

跨节点使用 `/wk/forward/distribution/v1`，同时传递该目标节点的收件人快照。
接收节点先保留快照、登记去重凭据并入队，再返回成功；成功响应表示已接收任务，
不代表客户端已经 RECVACK。任务持有快照，因此旧 Tag 被缓存淘汰不影响已接收任务。
发送节点记录本地和各远端的进度，重试只处理未确认部分。频道队列优先重试失败批次，
避免后续批次越过它。

当前边界（进程内固定值，无新增配置项）：

| 资源/操作 | 上限或期限 |
| --- | --- |
| 分发失败重试 | 最多 12 次，30 秒；退避 50ms 至 800ms |
| 单次跨节点请求 | 最多 750ms，同时受任务期限限制 |
| 单个快照请求体 | 32 MiB |
| 接收端去重凭据 | 每节点最多 65,536 条，保留 60 秒 |
| 接收端持有快照的预估占用 | 每节点 64 MiB；任务完成或终止后释放 |
| 历史 Tag 缓存 | 全局最多 4,096 个退役 key，另受原有缓存过期机制限制 |

凭据或快照容量不足时整批拒绝接收，由发送方有限重试；不淘汰有效去重凭据腾位置。
重试和去重不持久化，进程重启、超过去重窗口的迟到请求以及重试耗尽不具备可靠投递保证。
上述 64 MiB 为快照任务预留的估算上限，不是整个消息队列或进程的内存上限。
失败日志包含 messageId、tagKey、channelId/channelType、本机、源/目标节点及 attempt。

只有远端明确返回路由不存在时才使用旧协议；超时等结果不明确的请求继续走去重路由。
混合版本集群中旧节点仍保留旧协议的行为，需要所有接收节点升级后才能获得完整修复。
消息事件的旧编码和客户端 HTTP/WebSocket 接口保持兼容。

回归验证：

```sh
go test -race -count=20 ./internal/manager ./internal/channel/handler ./internal/channel/event ./internal/eventbus
go test -race -tags=integration -count=3 ./internal/channel/handler -run '^TestDistributionThreeNodeSlowPeerAndVersionEviction$'
```

三节点测试使用独立进程、真实 wkserver TCP RPC 和频道队列，注入慢节点/超时并触发
4,102 次成员变更，检查旧版本淘汰后的固定成员、退订/新增成员和重复投递。
测试以接收端 Pusher 入队为观测点，不覆盖完整数据库持久化、WebSocket、RECVACK 或
issue #84 的 8 万在线 SUB5 压测；后者仍须在对应集群环境验收。
