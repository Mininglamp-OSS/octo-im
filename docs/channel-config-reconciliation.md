# 扩容后的频道配置同步与会话 503

## 问题与修复范围

在 `f00056b083d54511141bdc442e61281a11ba3c2c` 上，单节点已有频道扩容到三节点后，槽 owner 可能迁走，频道 leader 仍留在原节点。下一次发送触发 `joinNewRepliceIfNeed`，learner 配置成功保存到槽数据库，但已有频道 leader 的快速提案路径不会重新加载配置，导致 DB version=4、runtime version=3。

会话读取要求两者的 leader、term、version 一致，因此 `/conversation/sync` 持续返回 503。并行读取中一个检查失败会取消其他 RPC，日志中的 `conversationConfig/v2 ... context canceled` 容易掩盖真正的版本落后。相关路由在所有节点均有注册。

`v2.2.5-20260422-fix1`（`02cf8cd19877e307a31cc13412155579a36bf0a4`）同样可能保留这两个不同版本，但没有新分支的严格读取检查。修复恢复完整会话的可用性，同时保留检查；故障、选举和收敛期间仍允许暂态 503。

追加审计发现，只有配置同步还不够：leader 离线时，没有新发送就不会选主；单节点扩容后，槽 owner 与频道 leader 同节点的频道可能一直走发送快速路径，保持单副本。本次将这两项维护接入后台恢复。

## 配置如何收敛

- 存储成功回调只提交频道标识，不执行网络请求，不等待 Raft。worker 再经过槽 applied 屏障读取当前配置，不把回调时间当成整个槽已应用完成。
- 当前槽 owner 在分发前维护已有配置：leader 离线时向当前在线投票副本查询日志，获得当前投票成员的多数派后选择日志最新的副本。learner、非成员和重复成员不计入多数派；日志相同按节点 ID 确定选择，空日志也可选主。新 term 高于配置及所有有效回复中的日志、持久化和运行态 term，溢出或不足多数派时不修改元数据。
- leader 可用后，若成员数低于目标且没有进行中的 learner/迁移，则按现有规则选择已加入且允许投票的在线节点作为 learner。包括槽 owner 与频道 leader 同节点的频道；追赶、晋升及中断后重试沿用原机制。选主失败时不会先保存一半补副本决策。
- 当前槽 owner 分发配置；频道 leader 上的读取提示也可直接触发本地核对。每次重试重新查询当前 owner、配置和目标 leader。
- 内部 `/rpc/channel/reconcileConfig/v1` 接收标识和观察到的版本，重新读取权威元数据，返回 `applied`、`dormant`、`superseded` 或 `retry`。不支持该协议、错误身份、过期版本均不能当成同步成功。
- 运行态比较和 `ConfChange` 在同一个 Raft owner 操作中完成。拒绝旧版本和相同非零版本的不同内容；比较前按 Raft 规则规范化 term 和非成员角色，保留较高的持久化 term。相同配置不重置副本同步状态。新建实例在 owner 上先初始化配置，成功后才注册；取消初始化不会留下零配置实例，也不对已经生效的配置做错误回滚。
- 配置 term 落后于运行态时，仅保留本地较高 term 不足以恢复读取。频道 leader 通过 `/rpc/channel/configTransition/v1` 请求 slot leader，在保留权威 leader 和成员的前提下提交高于观察到 term 的新配置，然后重新同步；读取期间仍要求精确一致。较高 term 中的旧投票不会带入新 term。迁移及 hard-state 持久化失败仍可暂时不可读，单副本恢复标记保留到真正完成 ResumeReplication。
- 所有生产配置写入都在 slot owner 校验被替换的配置版本和 term，再取得与普通写入共用的提案锁，固定已经进入日志的末尾，等待这一固定前缀应用。持锁期间普通写入暂缓进入日志，Raft owner、复制和应用继续运行，因此不需要槽自然空闲，也不会追赶持续增长的 committed index。前缀应用后在 owner 外重新读取并校验配置，在 owner 上再次核对 leader、term 和槽配置后原子接受提案，随即释放锁，再等待本条应用。锁获取与前缀等待继承调用方预算；取消、校验失败或换主都会释放锁。异步角色变更回调保留原始版本，不能拿最新配置版本包装旧内容。该过程使用原有日志命令格式，未改历史日志回放语义。
- 前台获取配置如果在版本校验时输给后台维护，会在原有 5 秒配置预算内重新读取并重新判断；不把输掉的选主结果套到新版本上，也不把这次消息提案前的竞争直接返回给首条发送。预算不随重试重置，旧回调和底层配置写入仍拒绝过期版本。
- 配置应用后的单副本恢复如果暂未 ready，会在调用方原有预算内每 10 毫秒重试；等待发生在 Raft owner 外，允许 Ready 重试持久化。预算耗尽仍返回超时并保留恢复标记，不把未落盘的状态认证为可读，也不因第一次暂态未 ready 就让发送或已提交配置的回调终止。普通休眠频道保持休眠。有 learner 或迁移任务的休眠频道，由配置指定的 leader 自动唤醒以完成迁移。读取发现版本落后、term/leader 不一致或暂未 ready 时只提交提示并继续返回可重试错误；幂等应用避免重复提示重置复制进度。
- 不改变正常发送的快速路径，不增加逐条发送必需的远程配置查询，不改变存储格式。

## 有界维护与恢复

每节点 4 个 worker，最多保留 4096 个不同频道，同频道只有一个在途任务。成功存储的新通知增加代次，重复读取提示不增加代次；旧请求成功不能清除处理期间到达的新写入通知。每次处理预算 2 秒，选主日志 RPC 和等待频道唤醒锁都继承取消与超时；日志探测最多使用剩余预算的一半（上限 4 秒），给配置提交与同步留出时间。只有探测自身超时才保留已收到的回复，父任务取消或超时仍终止操作；部分回复仍须满足当前投票多数派。失败从 100 毫秒退避到 2 秒，另加最多 25% 抖动。满队列下失败任务让出位置，即使处理期间又收到通知也不阻止退让；持久化数据库负责重新发现。

数据库是恢复依据。启动和集群配置改变会触发扫描；每 100 毫秒最多取 128 条配置，当前节点只调度自己拥有的槽。一轮扫描结束后等待 30 秒再开始下一轮，以发现游标之前的变化。队列满时触发扫描，游标不越过未入队的行。满队列中的失败任务会让出空间，由后续完整扫描重新发现，避免不可达频道挡住后面的健康频道。

因此，单次通知丢失、队列溢出和重启均不要求再次发送或手工调用频道 `/start`。全库覆盖时间随数据量、RPC 和磁盘延迟变化，不能把小规模测试时间当成生产环境恢复上界。

每 30 秒记录结构化维护统计：`pending`、队列内 `oldest`、`completed`、`retries`、`overflow`。持续重试按频道限频告警；配置冲突包含 incoming/runtime 信息，远程应用拒绝在目标节点的 debug 日志中记录。这些是日志统计，尚未添加独立的 Prometheus 指标或大规模扫描基准。

关闭时先取消维护 worker，再停止其依赖服务。

## PR #55 第一轮 blocking 修复验证

将初始化取消、持久化 term=5 / 配置 term=4 两个探针放到修复前的 `845b79ac`，分别复现了取消后仍注册实例、低 term 被永久拒绝。修补后的同一探针通过。新增回归还覆盖：hard-state 写入失败后保留并恢复单副本尾部、旧回调拒绝、配置读取到提案之间插入竞争写入、slot 任期变化拒绝旧决策、跨节点 term 修复、过期 RPC、唤醒锁取消，以及拥塞时持续提示不阻止失败任务退让。

通知、扫描和读取提示的测试分别隔离其他恢复来源，并先排空创建配置时的旧任务。配置初始化/恢复与条件提案的定向 `-race` 测试连续 10 次通过，队列定向 `-race` 连续 10 次通过。完整 channel、store、raft、raftgroup、slot 和 internal/api 包检查通过。真实三进程扩容、tag 数据升级扩容、故障基线数据重启恢复各通过 90 次完整会话读取。

全量 Go 测试仍未通过，存在下文列出的基线断言失败、固定端口冲突及超时；本轮全量运行还触发了 `wkdb.collectMetrics` 访问全局 trace 的空指针，已在未修改的 `f00056b0` 单独运行完整 cluster 包复现。不把全量运行描述为通过。

## 验证方法

`test/e2e/channel_config_reconcile.py` 启动独立的三个服务器进程，用 64 个槽、12 个群频道和真实 HTTP 请求验证。脚本会校验频道集合、消息 ID、序号、payload、配置版本、learner 和迁移标记；每种成功场景在收敛后从每个入口连续读取 30 次。扩容场景强制验证槽 owner 与频道 leader 分离。正常新建三节点时，两者可以相同。所有 12 个频道都必须达到三副本，已删除同节点频道的验收例外。

`--no-post-expansion-send` 在节点加入后不再发送，直接要求后台补齐副本。`--failover` 选择实际持有频道 leader 的节点并停止，等待槽换主后，全程不发送、不手工唤醒，要求两个存活入口恢复完整读取，并核对受影响频道的 leader、term、version 已变化且成员未被缩减。收敛窗口默认 45 秒，失败以非零状态退出，不用控制发送掩盖只读失败。

```sh
go build -o /tmp/octo-im-fixed main.go
python3 test/e2e/channel_config_reconcile.py --binary /tmp/octo-im-fixed \
  --scenario expand --no-post-expansion-send --failover --output /tmp/reconcile-expand
python3 test/e2e/channel_config_reconcile.py --binary /tmp/octo-im-fixed \
  --scenario fresh --failover --output /tmp/reconcile-fresh
python3 test/e2e/channel_config_reconcile.py --binary /tmp/octo-im-fixed \
  --scenario upgrade --legacy-binary /path/to/v2.2.5-20260422-fix1 \
  --output /tmp/reconcile-upgrade
python3 test/e2e/channel_config_reconcile.py --binary /tmp/octo-im-fixed \
  --scenario repair --legacy-binary /path/to/f00056b0 \
  --output /tmp/reconcile-repair
```

输出目录必须不存在。只使用 Python 标准库。每轮生成 `events.jsonl`、节点日志和 `summary.json`，默认在子进程退出后删除本轮数据库，使用 `--keep-data` 可保留。三进程数据约占数 GB，不建议在容量较小的 tmpfs 上并行运行多轮。

`upgrade` 先用旧 tag 创建和发送消息，保留数据库换成补丁，再扩容发送。`repair` 先用故障基线制造版本落后，并断言三个入口均为 503，然后保留全部数据重启为补丁，验证自动恢复。`expand --expect-stale` 配合故障二进制可单独运行反向对照。

新增 Go 测试覆盖：成功写入才通知、队列代次与溢出、重试和取消、扫描继续推进、并发提示、相同配置重复 100 次保留同步进度、旧版本和同版本冲突、正常/迁移休眠频道、读侧提示、分页边界、跨节点 RPC 以及目标 leader 更新。

```sh
go test ./pkg/cluster/channel ./pkg/cluster/cluster ./pkg/cluster/store ./pkg/wkdb \
  -run 'Test(Config|ChannelConfigReconcileRPC|ChannelClusterConfigRecoveryPagination|Conversation)' -count=1 -timeout=90s
go test -race ./pkg/cluster/cluster \
  -run '^TestConfig(Queue|Scan|Workers|ReadHint)' -count=1 -timeout=60s
```

2026-09-22 本地验证：扩容、旧 tag 数据升级扩容、故障数据重启恢复、新建三节点四种场景通过；每种场景 90 次完整读取。配置应用、存储通知、队列和分页的定向竞态测试通过。

已执行 `go test ./... -count=1 -timeout=300s` 及后续 60 秒限时全量检查，**全量未通过**。以下失败也在未修改的 `f00056b0` 中复现：

| 包 | 基线失败 |
| --- | --- |
| `internal/server`、`pkg/wknet` | 测试监听端口占用 |
| `internal/track` | `TestMessageString` 轨迹位图断言 |
| `internal/user/event` | `TestUserEventPool_AddConnectEvent` 超时 |
| `pkg/cluster/cluster` | `TestSendBatchOptimization`、`TestProcessBatchLatency`、`TestImprovedNode_BackpressureStrategies` |
| `pkg/wkdb` | 缓存统计、设备查询、截断日志、PluginUser 断言等既有失败 |
| `pkg/wkserver` | `TestReconnect` |

最后一轮并行全量运行还出现 `pkg/wkserver/test.TestSendAndRecv` 超时；该包在补丁和基线分别单独复测均通过（约 2.1 秒），保留为全量运行不稳定项，不计作完整测试通过。

完整集群初始化的 `-race` 检查也会报告基线已有的 clusterconfig/event/Raft 并发访问，不能声明整个集群已无竞态。本次新增维护与应用原语的定向测试通过；完整 channel、store 和 API 包检查通过。

## 2026-09-23 追加维护验证

在原 PR head `d70f1f49` 上执行强化后的同一 E2E 脚本：扩容场景在 45 秒内仍有同节点频道只有一个副本；新建三节点的停机场景在槽换主后的 45 秒窗口内持续 503。两项断言都以失败退出，确认新验收能够检出旧缺陷。

追加修复后，单节点扩容（加入后不再发送）、新建三节点、旧 tag 数据升级扩容、故障数据保留并重启四种场景均通过。每种场景先要求所有频道三副本，再执行 90 次完整会话读取，最后停止实际频道 leader 并验证无新发送的恢复；恢复后两个存活入口再完整读取 10 轮。四轮停机实测在槽换主后约 0.4–1.3 秒恢复，该数字只描述这组小规模本地测试。

新增 Go 回归覆盖当前投票多数派、拒绝 learner/非成员/重复票、空日志选主、日志新鲜度优先于当前任期、任期上界、读取休眠副本的持久化任期、失败选主不改元数据，以及真实 TCP 请求在 peer 未回复时可取消。配置、会话与跨节点 RPC 定向测试通过；选主纯逻辑、队列与扫描的定向 `-race` 连续 10 次通过。

全量 `go test ./... -count=1 -timeout=60s` 仍存在上文列出的基线问题；全量结果与原 PR head 的对照单独记录，不能称为全绿。本轮没有验证生产容量、混合版本自动选主或网络分区的所有时序，也没有重跑历史五轮业务套件。

## 追加评审修复（慢副本与暂态持久化失败）

`f2801628` 的两处 blocking 已用前后对照固定：三个真实 TCP peer 中一个接受日志请求后不回复，原先探测会耗尽 worker 预算，修复后可用其余两个投票回复完成选主并在同一预算内提交配置；父任务主动取消仍及时退出，不足多数派不写配置。四种前台入口（wake、SwitchConfig、两条发送入口）在旧实现中都立即返回暂未 ready，修复后等待临时持久化恢复并完成；真实 `onSaveChannelConfig` 回调覆盖了元数据已提交后的恢复。持续持久化失败的取消测试仍确认实例和恢复意图保留。

原有控制器选主协议只查询副本状态，不通过查询本身持久化投票或阻断旧 leader；故障检测误判旧 leader 时，配置传播前的旧写入窗口是现有协议的残余风险。本 PR 沿用该协议，不宣称覆盖所有网络分区时序。背景维护与前台 GetOrCreate 的并发由配置版本和槽状态的条件提案仲裁，未依赖前台的频道锁。旧 peer 的日志查询不报告持久化 term，完整恢复仍要求参与节点升级。

## 持续槽写入下的配置提交

评审实测发现，旧条件提案要求 `AppliedIndex == CommittedIndex == LastLogIndex`，每 10 毫秒采样会在持续普通写入下耗尽配置预算。本地真实单槽回归在 `1d2a3241` 上复现：空闲时通过，1、2、4 个持续写入者的场景均出现 5 秒超时。修复后相同负载可以完成配置提交；扩展回归连续 3 轮通过，同时检查配置更新、旧版本拒绝、原有 5 秒预算内首条发送和 2 秒预算内后台 leader 恢复，普通写入仍持续推进。

提案原语的定向 `-race` 连续 10 次通过，覆盖待应用前缀完成后再校验、过期决策拒绝、校验期间任期变化拒绝、取消等待后释放提案锁，以及无关日志推进不再拒绝仍有效的决策。这个方案在前缀应用期间短暂串行化同槽的新提案，等待上限仍受调用方预算约束；小规模回归不作为生产容量结论。

## 部署边界

参与节点全部升级后才具备完整的自动同步能力。滚动期间旧 peer 不支持新内部 RPC 时会继续重试，不会误报成功。公网 API 和数据库格式不变；生产发布前仍需在业务容量下验证维护开销，并对现场账号检查完整会话和迁移状态。本 PR 不包含镜像发布或生产部署。
