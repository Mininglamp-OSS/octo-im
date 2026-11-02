# 节点故障后的扩容恢复

外部通过正常加入入口新增节点时，集群优先补齐已有副本组的健康副本，再进行 slot 均衡。目标副本数沿用现有配置；健康三副本扩为四个节点时，已有频道仍保留三个消息副本。

## 成员变更

以 A、B、C 三节点中 C 故障、新增 D 为例：

1. 心跳超时标记 C 离线并触发必要的 leader 选举。离线状态本身不改变多数派。
2. D 先同步配置日志。配置组通过原多数派分别提交 C 退出和 D 晋升，经历 `ABC + learner D → AB + learner D → ABD`。
3. slot 和已有频道按实际健康副本检查缺口，复用现有 learner 同步。D 持久化已提交前缀后，故障 follower 的退出和 D 晋升分步提交；中间配置的多数派和 D 都确认安装后才完成切换。健康 follower 的迁出则先将 D 加为正式副本，再确认多数派并移除迁出节点，避免心跳尚未发现另一个故障时降低可用副本数。存活 leader 在补副本期间继续提供服务。
4. 正常 slot 扩容先迁移 follower，再由已有 leader 均衡器在正式副本之间转移领导权。

slot 迁移任务保存在配置日志中，频道任务保存在已有频道元数据中。后台扫描恢复任务；失败的 learner 可以取消并重新选择可用容量。条件 slot 更新和频道版本校验拒绝被新任务、选举或其他配置取代的旧回调。

D 先完成加入、C 随后才被心跳判离线时，后台仍会按现有健康容量继续完成成员清理。健康容量不足时保留原投票集合。

原配置多数派不可用时，不通过缩小在线节点统计来恢复投票。该流程不提供丢失多数派后的强制恢复。

## 旧节点恢复和资源回收

C 仍是成员时按正常重连恢复。C 已退出时，通过内部成员探测复用正常加入入口；成员列表为空时可用原 bootstrap 地址发现当前集群。加入响应中的已提交成员配置让旧进程先被动同步，避免旧任期和旧投票身份阻塞重新接入。已有成员的探测不修改成员、地址或角色，也不能抢在 bootstrap 初始化前创建成员配置。

已移除节点的 Raft 网络消息在处理任期前被拒绝。只有已提交的成员配置删除对应连接和队列；种子地址或加入响应中的局部节点列表不触发删除。重建 Server 时重新绑定实例自带的 transport，避免复用已停止实例的发送队列。

新增两个内部配置日志命令和一个内部探测路由；加入响应扩展可以读取旧响应。执行这些成员变更前应完成所有投票节点的版本升级，不支持用旧二进制应用新增命令。

## 本地验证

```sh
go test ./pkg/raft/... ./pkg/cluster/node/... ./pkg/cluster/slot ./pkg/cluster/channel ./pkg/cluster/store -count=1
go test ./pkg/wkdb -run '^TestChannelConfigCloneDoesNotShareMembership$' -count=1
go test ./pkg/cluster/cluster -run '^(TestChannelRetirement.*|TestExpansionRecovery.*|TestCommittedRemoval.*|TestClusterJoinResponse.*|TestMembershipProbe.*|TestChannelExpansion.*|TestSlotTransition.*|TestChannelReplicationAfterLeaderConfigChange|TestConfigOrphanMigration.*|TestSeededEmptyDiskRejoinCompletesRecoveryOverTCP)$' -count=1 -timeout=5m
```

集成用例使用真实 TCP 和独立 Pebble 数据库，覆盖普通节点和配置 leader 故障后新增 D、C 在 D 晋升前恢复、C 在数据同步期间恢复、learner/控制节点重启、配置或数据 learner 崩溃后新增另一节点、C 用原磁盘或空磁盘重新接入，以及加入早于故障心跳超时、恢复节点重新当选 leader、正常重连、空目录 seed 重入、旧回调和资源清理。

另有独立进程 SIGKILL 验证：64 个 slot、64 个已有频道、64 个用户，初始 512 条消息；恢复后继续发送并停机核对每个正式副本的消息 ID、序号、内容、频道元数据和用户设备数据。HTTP `/message/send` 返回表示请求进入异步发送队列，因此第二次故障的注入点以实际持久化完成为准。

健康扩容回归包括有数据的 2→3 自动补第三份历史副本，以及 3→4 的 slot 分配、消息写入和配置副本数上限。手动迁移频道 leader 和黑名单跨 slot 清理是独立问题，不在此变更中修复。
