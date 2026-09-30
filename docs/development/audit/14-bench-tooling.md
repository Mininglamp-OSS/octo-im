# 分片 14 审计报告：压测工具链 bench + CLI

范围：`internal/bench/**`（workload/worker/devsim/coordinator/capacity/model/report/metrics/planner/wkproto/target/config）、`cmd/wkbench/`、`cmd/wkdb/`。共 10774 行非测试 Go 代码，全部通读。

FLOW.md：本单元目录下未找到独立的 FLOW.md（`internal/bench` 及子目录均无该文件），故未发现文档分叉问题；架构说明以 `AGENTS.md`（仓库根/相关目录）及代码注释为准。

## 覆盖情况

| 文件 | 行数 | 读取状态 |
|---|---|---|
| internal/bench/workload/connections.go | 472 | 全文读取 |
| internal/bench/workload/group.go | 864 | 全文读取 |
| internal/bench/workload/person.go | 929 | 全文读取 |
| internal/bench/workload/scheduler.go | 131 | 全文读取 |
| internal/bench/workload/session_error.go | 91 | 全文读取 |
| internal/bench/worker/group_runner.go | 311 | 全文读取 |
| internal/bench/worker/person_runner.go | 706 | 全文读取 |
| internal/bench/worker/server.go | 429 | 全文读取 |
| internal/bench/worker/state.go | 265 | 全文读取 |
| internal/bench/devsim/config.go | 489 | 全文读取 |
| internal/bench/devsim/runner.go | 446 | 全文读取 |
| internal/bench/devsim/server.go | 97 | 全文读取 |
| internal/bench/devsim/status.go | 204 | 全文读取 |
| internal/bench/coordinator/preflight.go | 164 | 全文读取 |
| internal/bench/coordinator/run.go | 762 | 全文读取 |
| internal/bench/capacity/config.go | 145 | 全文读取 |
| internal/bench/capacity/discover.go | 100 | 全文读取 |
| internal/bench/capacity/result.go | 114 | 全文读取 |
| internal/bench/capacity/runner.go | 170 | 全文读取 |
| internal/bench/capacity/scenario.go | 163 | 全文读取 |
| internal/bench/capacity/search.go | 143 | 全文读取 |
| internal/bench/model/config.go | 344 | 全文读取 |
| internal/bench/model/bench_api.go | 141 | 全文读取 |
| internal/bench/model/plan.go | 64 | 全文读取 |
| internal/bench/model/rate.go | 48 | 全文读取 |
| internal/bench/report/report.go | 577 | 全文读取 |
| internal/bench/metrics/metrics.go | 450 | 全文读取 |
| internal/bench/planner/planner.go | 396 | 全文读取 |
| internal/bench/wkproto/client.go | 325 | 全文读取 |
| internal/bench/target/client.go | 186 | 全文读取 |
| internal/bench/config/config.go | 180 | 全文读取 |
| cmd/wkbench/main.go | 415 | 全文读取 |
| cmd/wkdb/main.go | 128 | 全文读取 |
| cmd/wkdb/output.go | 159 | 全文读取 |
| cmd/wkdb/config.go | 166 | 全文读取 |

合计 10774 行，全部读取完毕。`pkg/db/inspect`（被 `cmd/wkdb` 调用）及 `internalv2` 明确不在本分片范围，未审计。

## 发现

### [P1-1] capacity search 的报告目录只按 QPS 取整命名，二分收敛/小步进场景会覆盖前一次尝试的报告

- 位置：`internal/bench/capacity/scenario.go:157-163`（`attemptReportDir`），触发机制在 `internal/bench/capacity/search.go:77-138`（`Search`）
- 类别：(f) 性能/正确性 — capacity 子系统的核心承诺是"每次尝试的报告可追溯"，目录键碰撞会导致该承诺被静默打破
- 代码：
```go
func attemptReportDir(root string, attempt Attempt) string {
	root = strings.TrimSpace(root)
	if root == "" {
		return ""
	}
	return filepath.Join(root, "attempts", fmt.Sprintf("%06.0f-qps", attempt.OfferedQPS))
}
```
  `Search()` 中的二分收敛循环（search.go:116-136）：
```go
for ... {
    offered := (lastPassQPS + firstFailQPS) / 2
    ...
    if (firstFailQPS-lastPassQPS)/lastPassQPS <= cfg.BinarySearchMinDeltaRatio {
        break
    }
}
```
- 触发路径：`attemptReportDir` 仅用 `attempt.OfferedQPS` 四舍五入到整数（`%06.0f`）作为目录名，**没有使用 `attempt.Index`**。`capacity/config.go` 的 `Validate()`（:81-130）只要求 `StepFactor > 1` 且 `StartQPS > 0`，未对二者设下限。当以较低的 `--start-qps`（例如默认场景下不指定、只用较小值，如 10~20）配合默认 `BinarySearchMinDeltaRatio=0.05` 做二分收敛时，`(lastPassQPS+firstFailQPS)/2` 在连续几轮迭代中会反复得到相差小于 1 的浮点 QPS 值（例如 12.3 与 12.7 都四舍五入到 "000012-qps"）；或者 ramp 阶段使用接近 1 的 `--step-factor`（只要求 `>1`，如 1.01）时，相邻 `offered *= StepFactor` 的结果在取整后大量重复。这些"不同尝试、相同目录"的场景会导致 `report.WriteDir()`（`internal/bench/report/report.go:209-256`）对同一目录重复执行 `os.WriteFile`/`writeJSON`/`writeYAML`，后一次尝试的 `report.json`、`summary.md`、`metrics/worker-1s.jsonl` 等文件把前一次尝试的完全覆盖。
- 后果：`capacity send` 产出的 `--report-dir/attempts/NNNNNN-qps/` 目录本应一一对应每次尝试，但在上述参数组合下会静默丢失历史尝试数据，用户根据目录回溯某次尝试的真实指标时可能拿到的是后一次尝试覆盖后的数据，而不会有任何报错或警告（`WriteDir` 内部无目录已存在检测）。这正好落在本分片校准要求中"capacity 是否真正计算/持久化了它声称计算的东西"这一最高优先级问题上。
- 建议：`attemptReportDir` 使用 `attempt.Index` 生成目录名（或在 QPS 之外附加序号，如 `fmt.Sprintf("%06d-%06.0f-qps", attempt.Index, attempt.OfferedQPS)`），从根本上消除碰撞可能性；同时可在 `Config.Validate()` 中对 `StepFactor`/`BinarySearchMinDeltaRatio` 设置更保守的下限作为纵深防御。

### [P2-1] bench-api / worker 控制令牌无脱敏标记，以明文形式落盘（0o644）且允许经 HTTP 明文传输

- 位置：
  - `internal/bench/model/config.go:44`（`BenchAPIConfig.Token`）、`:64`（`Worker.ControlToken`）—— 字段无 `omitempty`/自定义 `MarshalJSON`/脱敏
  - `internal/bench/devsim/config.go:75`（`TargetConfig.BenchAPIToken`，来自 YAML 明文配置文件）
  - `internal/bench/worker/state.go:229-241`（`persistAssignment`）：
    ```go
    func (s *State) persistAssignment(a Assignment) error {
        if s.workDir == "" {
            return nil
        }
        if err := os.MkdirAll(s.workDir, 0o755); err != nil {
            return err
        }
        data, err := json.MarshalIndent(a, "", "  ")
        ...
        return os.WriteFile(filepath.Join(s.workDir, "current-run.json"), data, 0o644)
    }
    ```
    `Assignment`（worker/state.go 前部）内嵌 `Target model.Target`，其中包含 `BenchAPI.Token`。
  - `internal/bench/report/report.go` 的 `WriteDir()`（:209-256）把 `rep.Target`（同样含 `BenchAPI.Token`）写入 `target.yaml` 与 `report.json`，权限同样是 `0o644`。
  - `internal/bench/config/config.go:148-156`（`validateHTTPURL`）：
    ```go
    func validateHTTPURL(raw string) error {
        u, err := url.Parse(raw)
        if err != nil || u.Scheme == "" || u.Host == "" {
            return fmt.Errorf("must be an absolute http URL")
        }
        if u.Scheme != "http" && u.Scheme != "https" {
            return fmt.Errorf("must use http or https")
        }
        return nil
    }
    ```
    显式允许 `http` scheme，未对明文令牌传输给出任何警告。
- 类别：(i) 架构/运维安全（不属于核心 CVE 范围，但属于本分片自身设计问题）
- 触发路径：运维人员按文档配置 `target.bench_api.token`/`worker.control_token` 并以 `http://` 地址运行 `wkbench worker`/`wkbench run`；worker 进程会把包含该 token 的完整 `Assignment`（通过 `model.Target`）以 `0o644` 权限写入 `<work-dir>/current-run.json`，协调端同样把 `Target` 写入报告目录下的 `target.yaml`/`report.json`（也是 `0o644`）。这些文件在多用户主机、共享 NFS 报告存储或事后归档打包（如 CI artifact 上传）时可被非预期用户读取；同时 token 在 `http://` 明文连接上传输，可被链路上的中间设备嗅探。bench-api token 授予对目标节点插入任意 UID/Token/Channel 的能力（见 `model/bench_api.go` 的 `BatchTokensRequest`/`BatchChannelsRequest`），因此泄露后果并非纯粹的观测性数据泄露。
- 后果：控制平面凭证以明文、世界可读权限持久化并可经明文通道传输，一旦运行环境不是完全隔离的单用户机器（压测通常由 CI/共享跳板机触发），存在凭证泄露继而被用来污染目标环境数据的风险。
- 建议：为 `Token`/`ControlToken`/`BenchAPIToken` 字段增加序列化时脱敏（自定义 `MarshalJSON`/`MarshalYAML` 输出为 `"***"` 或省略），或在写报告/持久化 assignment 前对 `Target`/`Worker` 做深拷贝并清空敏感字段；文件权限收紧为 `0o600`；`validateHTTPURL` 在配置了 `token` 但地址为 `http` 时至少输出一条警告日志。

### [P3-1] 控制平面 HTTP 服务器缺少读超时，存在慢速攻击面（gosec 已提示，按运维工具校准为低优先级）

- 位置：
  - `cmd/wkbench/main.go:388`：
    ```go
    if err := http.ListenAndServe(cfg.listen, worker.NewServer(cfg.server)); err != nil {
    ```
  - `internal/bench/devsim/server.go:26`：
    ```go
    s.server = &http.Server{Addr: listen, Handler: s.mux}
    ```
- 类别：(g) 性能/健壮性（gosec G114 / G112 命中，均属真实命中，非误报）
- 触发路径：`wkbench worker`（对外暴露 control API，:388 用 `http.ListenAndServe` 裸启动，无 `ReadHeaderTimeout`/`ReadTimeout`）与开发态长跑模拟器的状态 HTTP 服务器（devsim/server.go:26 构造的 `*http.Server` 同样未设置任何超时字段），面对慢速发送请求头的连接（Slowloris 类模式）会长期占用连接资源。由于这两个服务默认监听在 `127.0.0.1`，且是压测工具的控制面而非生产服务路径，风险可控，故按校准要求定为 P3。
- 后果：在 worker 控制地址被暴露到非受信网络（例如误配置监听 `0.0.0.0`）的场景下，可被少量连接耗尽 worker 的可用 goroutine/文件描述符，影响压测任务本身的可靠性；不影响被压测的 WuKongIM 目标集群。
- 建议：为两处 `http.Server` 显式设置 `ReadHeaderTimeout`（如 5s）与合理的 `ReadTimeout`/`WriteTimeout`。

### [P3-2] 死代码：两处未被调用的辅助函数

- 位置：
  - `internal/bench/capacity/runner.go:165-170`：
    ```go
    func timestampedReportDir(root string, now time.Time) string {
        if root == "" {
            return ""
        }
        return filepath.Join(root, fmt.Sprintf("%s-send", now.Format("20060102-150405")))
    }
    ```
  - `internal/bench/workload/person.go`（`startAutoRecvAck`，已被 `startAutoRecvAckWithOptions` 取代，原函数无调用点）
- 类别：(i) 架构/代码卫生（staticcheck U1000 命中，已核实确无调用点）
- 触发路径：无运行时触发路径——纯静态死代码，不影响行为，仅增加维护负担与阅读干扰。
- 后果：轻微；无功能性影响。
- 建议：删除两个未使用函数，或如需保留兼容性接口应在注释中说明保留原因。

### [P3-3] AGENTS.md 未覆盖本分片全部子包

- 位置：仓库根 `AGENTS.md`（架构说明文档）与 `internal/bench` 目录树对照
- 类别：(i) 架构/文档一致性
- 触发路径：无运行时触发；属于文档与代码结构漂移。核实 `AGENTS.md` 中关于 bench 工具链的目录说明未列出 `capacity`、`workload`、`metrics`、`report`、`wkproto` 等已经承担核心职责的子包（这些包合计约 4700 行，占本分片 43%）。
- 后果：新贡献者按文档理解 bench 工具链架构时会遗漏这几个关键子系统，增加上手/审计成本。
- 建议：更新 AGENTS.md 补齐这些子包的一句话职责说明。

## 已排除的候选项

1. **`workload/group.go` `smallGroupMemberIndexes` 的 `MemberBase==0` 覆盖分支与 `worker/group_runner.go` `memberIndexesForGroupChannel` 是否存在成员索引分配分叉** —— 通过完整阅读 `planner/planner.go` 的 `profileIdentityRanges`/`Build` 逻辑证明：
   - 当 `MemberReusePolicy == "allowed"` 时，prepare 路径（group.go）与 traffic 路径（group_runner.go）都直接调用同一个 `DeterministicGroupMemberIndexes(runID, profile/shard 名称, channelIndex, MemberRange, count)`，参数完全一致，不存在分叉可能。
   - 当为 `"disallowed"`（不复用）时，`smallGroupMemberIndexes` 里 `if cfg.MemberBase == 0 { memberStart = channelIndex * cfg.MembersPerChannel }` 这条覆盖分支，仅在 `MemberRange.Start==0`（即 `MemberBase==0`）时触发；而根据 planner 的构造公式 `MemberRange.Start = IdentityRange.Members.Start + ChannelRange.Start*Members.Count`，在 `Members.Count>0` 且 `IdentityRange.Members.Start>=0` 的前提下，`MemberRange.Start==0` 只能发生于 `ChannelRange.Start==0` 同时成立的情形。而在该情形下，覆盖公式 `channelIndex*count` 与非覆盖公式 `MemberBase + (channelIndex-ChannelRange.Start)*count = 0 + (channelIndex-0)*count` 代数上完全等价。**结论：该覆盖分支在所有可达状态下均不改变结果，不是 bug，予以排除。**

2. **`metrics/metrics.go` 中 worker 聚合百分位取"各 worker 本地百分位最大值"而非"全局合并样本重新计算"** —— `HistogramSummary` 类型的文档注释与 `mergeHistogram` 函数内注释均明确说明这是有意为之的近似算法（"aggregate percentiles are the largest worker-local percentiles rather than global distribution percentiles"），属于已知声明的设计选择而非静默错误，不满足本次审计"静默错误百分位"这一发现标准，予以排除。

3. **`coordinator/preflight.go` 的 `checkWorker` 潜在资源无界读取** —— 核实 `p.http.Do(req)` 受 `workerInfoTimeout=10s` 默认超时约束，`io.ReadAll(io.LimitReader(resp.Body, 512))`（:135）已用 512 字节的 `LimitReader` 限界，两处均已正确加界，不构成发现。

4. **`worker/group_runner.go` 的 `groupShardOwnsChannel`（:216-225，`return shard.ChannelRange.Len() > 0 && shard.MemberRange.Start == 0`）作为 split 模式下 `ChannelOwners` 缺失条目时的兜底所有权判断，是否可能与 planner 产生的真实归属冲突** —— 核实 `planner.go` 对 group profile 在所有路径下都会填充 `ChannelOwners` map，该兜底分支在正常调用路径上不可达，属于防御性代码，未发现可触发的输入使其被执行，因缺乏具体触发路径而排除（未作为 P3 死代码列出，因为技术上仍可能通过未来的调用方式触达，不能证明为纯死代码）。

5. **`capacity/result.go` 的 `Result.ExitCode()` 只区分 `StatusPassed`/非 `StatusPassed` 两种返回值，是否会把"内部错误"错误地映射为"未找到稳定 QPS"（`ExitNoStableAttempt`）** —— 核实 `capacity/search.go` 中 `Status` 类型只定义了 `StatusPassed`/`StatusFailed` 两个枚举值，且预检失败、worker 失败等异常路径在 `cmd/wkbench/main.go` 中是在调用 `Search`/`RunAttempt` 返回 `error` 时提前 return 对应的 `ExitPreflightFailed`/`ExitWorkerFailed` 等码，并不经过 `Result.ExitCode()`；只有 `Search` 正常跑完（无 error）才会调用 `result.Status.ExitCode()`（对应 `cmd/wkbench/main.go:250`）。因此该二值判断在实际调用路径上没有歧义，予以排除。

6. **`devsim/config.go` 的 `personVerifyMode`（sampled→full）与 group 分支直接使用原始 `VerifyRecv` 字符串的不对称处理** —— 核实这是有意设计：person 频道固定只有 2 个参与者，"sampled" 验证在语义上等价于 "full"；group 频道成员数可配置且可能很大，需要保留真实的采样模式。不构成缺陷。

## 本分片整体评价

该分片代码整体质量较高：黑盒设计边界清晰（未发现越界 import WuKongIM 服务端内部包的情况）、确定性分片/规划算法（planner.go）经过严格的数学交叉验证未发现分叉、错误处理和资源释放（`ConnectionManager.Close`、`report.WriteDir` 的错误传播、`coordinator/run.go` 的 `errors.Join`/`Unwrap() []error` 处理）覆盖全面、上下文超时在协调端出站 HTTP 调用（`target/client.go`、`coordinator/preflight.go`）与 worker 心跳循环上均有合理边界。核心的量化正确性问题集中在 `capacity/` 子系统一处真实的报告目录碰撞逻辑缺陷（P1-1），符合校准要求中"capacity 是否真正计算/持久化了它声称的东西"的最高优先级关切；另发现一处控制面凭证持久化/传输的安全卫生问题（P2-1）。其余发现均为运维工具的低优先级健壮性/文档问题（P3），未发现 P0 级别问题（无数据丢失/崩溃/远程可触发的严重缺陷）。作为压测/运维工具链，这一风险分布与其"非核心服务路径"的定位相符。
