# 节点故障后的扩容恢复

外部通过正常加入入口新增节点时，集群优先补齐已有副本组的健康副本，再进行 slot 均衡。目标副本数沿用现有配置；健康三副本扩为四个节点时，已有频道仍保留三个消息副本。

## 成员变更

以 A、B、C 三节点中 C 故障、新增 D 为例：

1. 心跳超时标记 C 离线并触发必要的 leader 选举。离线状态本身不改变多数派。
2. D 先同步配置日志。配置组通过原多数派分别提交 C 退出和 D 晋升，经历 `ABC + learner D → AB + learner D → ABD`。
3. slot 和已有频道按实际健康副本检查缺口，复用现有 learner 同步。D 持久化已提交前缀后，故障 follower 的退出和 D 晋升分步提交；中间配置的多数派和 D 都确认安装后才完成切换。健康 follower 的迁出则先将 D 加为正式副本，再确认多数派并移除迁出节点，避免心跳尚未发现另一个故障时降低可用副本数。存活 leader 在补副本期间继续提供服务。
4. 正常 slot 扩容先迁移 follower，再由已有 leader 均衡器在正式副本之间转移领导权。单副本 slot 没有 follower 时，使用原有 learner 追齐后交接 leader 的路径；发起异步交接前暂停旧 leader 的提案，恢复与否由已应用的正式配置决定，交接取消后清理旧暂停状态。保存配置的成功或失败回调不能单独恢复写入，避免交接窗口内产生目标尚未持有的已提交写入。

slot 迁移任务保存在配置日志中，频道任务保存在已有频道元数据中。后台扫描恢复任务；失败的 learner 可以取消并重新选择可用容量。条件 slot 更新和频道版本校验拒绝被新任务、选举或其他配置取代的旧回调。

slot 配置保存请求同时校验发起时观察到的配置版本和该 slot 的持久化代次。代次由配置日志应用时赋值，仅在该 slot 的实际分配改变时推进；无关节点信息或其他 slot 更新不会使原任务失效。取消后对同一目标重新发起迁移会获得新代次，旧异步保存任务在提交前被拒绝；若取消和重建发生在条件提案排队期间，最终应用时的代次比较仍会拒绝旧提案。

slot 和频道均在完成回调时重新检查迁出节点的健康状态。C 在 D 同步期间恢复时，保留 C，经历 `ABC + learner D → ABCD → ABD`；四个正式副本只是迁移中间态，目标副本数仍为三。D 和中间配置多数派确认后才能移出 C。若 D 在中间态故障，原正式副本仍在线并满足原多数派及目标容量时，取消 D 并保留迁出节点。

若中间态发生选举，无论原迁出节点 C、迁入节点 D 或其他正式副本当选，都保留新 leader，优先安排故障旧 leader 退出，并选择一个健康 follower 进行同步确认。迁入节点成为 leader 后不等待它向自己同步；选举及完成回调不能直接清空任务并留下超额副本。已出现超额正式副本但迁移标记丢失时，后台也按配置目标副本数重建收尾任务。规划不直接删除投票成员，原配置多数派、已安装配置和落盘前缀仍必须确认；不足多数派时保持现状。普通领导权转移不受此规则影响。进程重启后，任务由现有提示及最长 30 秒一轮的后台扫描重新发现。

D 先完成加入、C 随后才被心跳判离线时，后台仍会按现有健康容量继续完成成员清理。健康容量不足时保留原投票集合。

原配置多数派不可用时，不通过缩小在线节点统计来恢复投票。该流程不提供丢失多数派后的强制恢复。

## 旧节点恢复和资源回收

C 仍是成员时按正常重连恢复。C 已退出时，先通过只读成员探测确认自身缺席，再自动调用正常加入入口；成员列表为空时可用原 bootstrap 地址发现当前集群。探测本身不提交成员变更，已有 learner 继续原任务，正式成员不因探测重新进入恢复。正式加入响应中的准确提交边界及成员配置让旧进程先被动同步，避免旧任期和旧投票身份阻塞重新接入；探测快照不能作为本次加入成功的证明。探测也不能抢在 bootstrap 初始化前创建成员配置。新增 seed 节点若在应用首条配置日志前因超时被清退，仍通过原 seed 发现自身缺席并正式加入重试。

新版客户端固定使用 `/rpc/cluster/join/probe/v2`；新版服务端的 v1、v2 探测入口均只读。旧服务不支持 v2 时返回错误或超时，不回退到可能修改成员的 v1 探测或普通加入。服务端与承担自动重入的客户端需要配套升级，作为 seed 或加入转发入口的节点也须保留完整的加入边界和配置响应；旧客户端不会因收到缺席快照而自动发起正式加入。这项分离保留内部集群网络的既有信任边界，普通 join 的节点身份认证不由本变更提供。

原 slot leader 在心跳超时前以空目录和 seed 重入时，即使存在未完成的迁移或另一个新节点正在 Joining，也要保留该任务并完成恢复选举及 Joined 发布，避免迁移与启动恢复互相等待。已经提交的配置 voter 通过现有 NodeJoining 命令获得本次恢复边界，不重新降为配置 learner，也不覆盖另一节点的成员迁移；应用该边界时恢复本地投票集合。若并发退役使恢复命令成为空操作，返回重试。

已有 slot 副本的恢复优先于尚未分配的新节点。普通新节点等待旧迁移后再均衡；清理其他节点的失败任务不能算作自身分配完成。发布 Joined 前检查条件 slot 更新已经落地，被并发更新取代的计划留到下一轮重新计算，恢复选举的 Candidate 计划也适用此检查。

bootstrap 节点重新加入时，地址依次取有效的显式 ServerAddr、已提交的自身 ClusterAddr、InitNodes 中的自身地址。因此原来依靠 InitNodes 启动、ServerAddr 为空或旧 wildcard 的节点可以使用原配置返回；所有候选均无效时仍由原地址校验拒绝，不把监听地址当作广播地址。

已移除节点的 Raft 网络消息在处理任期前被拒绝。只有已提交的成员配置删除对应连接和队列；种子地址或加入响应中的局部节点列表不触发删除。重建 Server 时重新绑定实例自带的 transport，避免复用已停止实例的发送队列。

本地 slot 移除在 Raft 事件循环上完成；以相同编号重新加载前，在循环外等待旧实例已接收的落盘、读日志、截断和 Apply 任务结束，再读取存储状态。所有本地异步完成事件和 slot 配置事件绑定原实例，消费时丢弃已退役实例的事件；旧 Apply 也不能唤醒新实例的等待者。角色配置保存在异步任务前由事件循环快照，SaveConfig 不进入单 slot 的存储等待，以免成员变更回调等待自身。停机先停止接收工作并等待已接收 worker 完成，再关闭存储。

旧节点重启或 leader 切换后，先确认日志前缀，再允许心跳推进提交；leader 只将完成前缀确认后的持久化同步位置计入多数派和晋升判断。异步日志读取保留发起时任期，旧任期结果不能作为新 leader 的同步响应。一批历史数据跨多个任期时，按任期边界分批持久化，确保每个任期起点和实际尾部任期一致；slot 日志与对应任期起点在同一批次提交。

前缀确认比较同一索引处的实际日志任期。多次分叉时，每轮截断只推进校验，不能直接认定已经匹配；截断后恢复实际保留尾部的任期并再次查询，直到匹配，包括共同前缀为空的情况。复用现有网络格式，缺失任期的长分叉最坏逐条回退。截断请求使用不进入网络编码的本地编号；跨任期完成时协调实际磁盘结果和内存队列，随后向当前 leader 重新校验。截断及失败重试期间保持写入屏障，迟到且已无进展的截断响应只触发重新校验。

截断不能越过已提交前缀，存储拒绝截断时必须返回错误，不能让内存位置继续回退。slot Apply 在锁内核对日志身份、内容及连续性，拒绝读取后已被替换的旧数据；已应用范围不重复覆盖。启动发现 applied 超过日志尾部时返回错误，不能通过降低 applied 标记掩盖已经落盘的状态。该保护防止新分歧，不自动重建旧版本已经产生的错误状态。

已合入的地址变更配置日志命令保留编号 11；本变更的成员移除、条件 slot 更新命令分别使用编号 12、13，避免与已有持久化日志冲突。新增两个内部配置日志命令和版本化内部探测路由；加入响应扩展可以读取旧响应。执行这些成员变更前应完成所有投票节点的版本升级，不支持用旧二进制应用新增命令。

slot 代次保护使用配置命令版本 3，覆盖全量配置、slot 更新、携带 slot 的 Joined、迁移、状态改变及条件更新；命令编号不变。旧版本 1 的历史日志保留原应用语义，缺少代次的旧快照与完整历史回放保持一致，之后由新版实际分配变更建立代次。启用这项保护前必须升级所有配置写入节点，不能依赖旧写入端执行新版的代次校验。

新加入使用版本 1 的 Join/Joining 表达扩展或晋升，故障成员移除由命令 12 单独提交，并在应用时复查在线状态。版本 2 的历史 Join/Joining 仍按原替换语义回放，旧快照中保留的非投票成员元数据不因升级而自动删除；该身份返回时仍需经过 learner 同步。版本 3 的 slot 代次与 Join 版本独立。learner 接任 leader 和已有 follower 接任 leader 都严格增加一次任期，普通 learner 晋升为 follower 保持任期。

## 本地验证

```sh
go test ./pkg/raft/... ./pkg/cluster/node/... ./pkg/cluster/slot ./pkg/cluster/channel ./pkg/cluster/store -count=1
go test -race ./pkg/raft/raft ./pkg/raft/raftgroup ./pkg/cluster/slot -run '^(TestRestartedReplica.*|TestLeaderDoesNotCountUnmatchedSyncPosition|TestSyncErrorDoesNotValidateLocalPrefix|TestUncheckedSyncCannotPromoteLearner|TestPrefixAlignment.*|TestMixedTermCatchup.*|TestAsyncLogReadRetainsRequestTerm|TestSlotRestartRepairsConflictingChannelMetadata|TestSlotRejects.*)$' -count=3 -timeout=90s
go test ./pkg/wkdb -run '^TestChannelConfigCloneDoesNotShareMembership$' -count=1
go test -race ./pkg/raft/raft ./pkg/raft/raftgroup ./pkg/raft/types ./pkg/cluster/slot -run '^Test(SlotMultiTermPrefixRecovery|SlotOldTermTruncate.*|OldTruncate.*|Truncate.*|StandaloneTruncate.*|PendingTruncate.*|DelayedTruncate.*|Prefix.*|EmptyLeader.*|LeaderHandoff.*|LogPrefix.*)$' -count=3 -timeout=3m
go test ./pkg/cluster/cluster -run '^Test(SeededEmptyLeaderRejoin(DuringSlotMigrationOverTCP|WhileNextNodeJoiningOverTCP)|JoiningNodeReceivesSlotsAfterDeadLearnerCleanupOverTCP|ExpansionRecoveryBootstrapRejoinAdvertisedAddress|SingleReplicaExpansionPreservesDataOverTCP)$' -count=1 -timeout=180s
go test -race ./pkg/raft/raftgroup ./pkg/cluster/slot -run '^(TestRetired.*|TestStorageDrain.*|TestConfigurationCallback.*|TestSlotRecreation.*|TestSlotStop.*)$' -count=5 -timeout=90s
go test ./pkg/cluster/cluster -run '^(TestChannelRetirement.*|TestSlotRetirement.*|TestChannelRecoveredSource.*|TestChannelFollowerMigration.*|TestExpansionRecovery.*|TestCommittedRemoval.*|TestClusterJoinResponse.*|TestMembershipProbe.*|TestChannelExpansion.*|TestSlotTransition.*|TestChannelReplicationAfterLeaderConfigChange|TestConfigOrphanMigration.*|TestSeededEmptyDiskRejoinCompletesRecoveryOverTCP)$' -count=1 -timeout=12m
```

集成用例使用真实 TCP 和独立 Pebble 数据库，覆盖普通节点和配置 leader 故障后新增 D、C 在 D 晋升前恢复、C 在数据同步期间恢复、learner/控制节点重启、配置或数据 learner 崩溃后新增另一节点、C 用原磁盘或空磁盘重新接入，以及加入早于故障心跳超时、恢复节点重新当选 leader、正常重连、空目录 seed 重入、旧回调和资源清理。

频道交错故障回归覆盖 `C 故障 → D 同步 → C 恢复 → B 频道不可达或整机停止 → D 完成`，验证 B 不恢复时仍能提交消息和读取历史；另覆盖四副本中间态的目标节点重启、元数据 owner 重启、消息 leader 故障后原迁出节点当选，以及目标节点故障取消。

slot 生命周期回归暂停真实 Pebble 写入，经过正式配置入口移除再加入相同 slot，确认新实例等待旧任务结束、加载完整历史并继续存储后续日志。另覆盖已入队和延迟到达的旧回调、旧实例销毁、各类存储任务排空、worker 池拒绝、配置回调重入，以及存储和重新加载等待期间停机。

日志分歧回归构造返回节点在同一序号保存不同的未提交频道配置，先送达新 leader 心跳，再放开前缀校验和同步。验证旧配置不会先被应用，分歧尾部能修复，随后写入能提交，停机重新打开物理数据库后 leader、任期和配置版本一致。另覆盖 follower/learner、未确认同步不能晋升、同步错误、跨任期历史分批落盘、异步读取任期和 Apply 读取后的尾部替换。

多任期回归为每条日志写入不同频道，避免后续正确写入掩盖错误应用；比较完整保留日志与关闭重开后的物理元数据。延迟真实 Pebble 截断直到 leader 换届，验证旧回调不能推进提交且同步可以继续；另覆盖存储错误重试、迟到重复响应、空前缀和空 leader。迁移中空盘重入用例在 learner 仍暂停时验证重入、选举及写入，也覆盖下一节点已经 Joining 的组合；learner 崩溃清理后检查新节点真正获得 slot 及历史数据。确定性配置 Raft 交错用例覆盖恢复命令前抢先退役、分配或恢复选举计划被条件更新跳过，验证不会提前确认成功。单副本扩容核对迁移前后的持久化数据。

新增选举回归以真实 TCP 和独立 Pebble 构造只有 A 与指定后继副本持有最新日志的四副本中间态，分别强制 C、D 当选，另覆盖选举后迁移标记丢失。频道与 slot 均验证收敛到三个健康副本，再关闭另一个节点并提交后续写入、核对数据。

另有独立进程 SIGKILL 验证：64 个 slot、64 个已有频道、64 个用户，初始 512 条消息；恢复后继续发送并停机核对每个正式副本的消息 ID、序号、内容、频道元数据和用户设备数据。HTTP `/message/send` 返回表示请求进入异步发送队列，因此第二次故障的注入点以实际持久化完成为准。

健康扩容回归包括有数据的 2→3 自动补第三份历史副本，以及 3→4 的 slot 分配、消息写入和配置副本数上限。手动迁移频道 leader 和黑名单跨 slot 清理是独立问题，不在此变更中修复。
