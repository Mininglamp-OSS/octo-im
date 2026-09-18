# Validation on 2026-09-18

Base: PR #50, `93c6331ad2a60af26902ff541bb12d2e010296a3`.
The tested binary SHA-256 and unrounded observations are in
[smoke-20260918.json](smoke-20260918.json).

## Three actual nodes, finite workload

All five scenarios passed using client concurrency **2**, 12 groups per burst,
8 users per group, 8 slots with 3 replicas, and 2 recovery workers per node.
The final run also checked blacklist removal restores a still-subscribed user's
conversation. There was no browser, Docker workload, message send load, or
unbounded producer. These are new smoke scenarios, not the earlier PR #51
five-round benchmark.

| Scenario | Drain observed after burst/restart | Final conversation consistency |
| --- | ---: | --- |
| Create 12 groups | 4.910 s | Passed at 4.920 s |
| Stable request replay, conflicting ID, blacklist unblock | Completed | Passed |
| Remove then rejoin before old work drains | 4.049 s | Passed at 4.061 s |
| Kill/restart all three owned nodes with 108 pending units | 6.435 s | Passed at 6.439 s |
| Remove all subscribers | 2.415 s | Passed at 2.418 s |

Create admission p95 was **0.229 s**. Foreground read probes succeeded while
cleanup drained, but a first successful probe is not a sustained recovery SLA.
The workload did not saturate storage or reproduce production traffic; recovery
times above must not be extrapolated to arbitrary backlog sizes.

All three child nodes and the harness shared one cgroup: MemoryMax 4 GiB,
MemoryHigh 3 GiB, SwapMax 0, CPUQuota 200%, TasksMax 512. Peak cgroup memory was
**127,221,760 bytes (121.3 MiB)**. `high`, `max`, `oom`, and `oom_kill` events were
all zero. All child processes were stopped after the run.

Preliminary harness runs exposed two setup issues (cluster API metadata readiness
and an overly long Unix socket path) and a missing-channel read-floor case, all
resolved before this result. A preliminary `/tmp` run consumed memory through
Raft WAL preallocation because `/tmp` is tmpfs; it recorded no OOM, and testing
was stopped and moved to disk. The harness now rejects tmpfs/ramfs output paths.

## Go verification

Successful build:

```sh
go build -buildvcs=false -p=2 -o /tmp/im-subscriber-recovery-server .
```

Focused storage, Store, Raft-backoff and API tests passed, including the race
detector:

```sh
go test -race -p=2 -timeout=120s \
  ./pkg/wkdb ./pkg/cluster/store ./pkg/raft/raft ./internal/api \
  -run 'Test(SubscriberRecovery|ConversationLifecycle|ApplyRetry|ApplyBackoff)' -count=1
```

The full suite was run with `go test -p=2 -timeout=120s ./...` under memory/CPU
limits with disk-backed `TMPDIR`. **The full suite is not green.** The failing
packages were rerun in an isolated checkout of the unmodified PR #50 base with
the same limits. Six packages reproduced the same failure classes:

| Package | Reproduced on PR #50 base |
| --- | --- |
| `internal/server` | WebSocket listener address already in use |
| `internal/track` | `TestMessageString` expected path bit string mismatch |
| `internal/user/event` | `TestUserEventPool_AddConnectEvent` timed out at 120 s |
| `pkg/cluster/cluster` | Queue shrinking assertions; `TestSendBatchOptimization` nil pointer |
| `pkg/wkdb` | Cache stats/new-conversation cache assertion, device search, log truncation, plugin user tests |
| `pkg/wknet` | TCP listener address already in use |

`pkg/wkserver/TestReconnect` failed in full runs but passed both the
baseline package run and an isolated rerun of this branch. Its timing-sensitive
failure is recorded rather than counted as a clean full-suite pass. The changed
Store and Raft packages passed their complete package suites, and all new recovery
tests passed. Unrelated baseline failures are not fixed in this PR.

Local raw logs and node data are retained in
`/home/mlamp/Documents/workspace-cli-codex/subscriber-recovery-validation-20260918/`
(`final-smoke/`, `go-test-final-all.log`, `go-test-baseline.log`, and focused test
logs). Run commands and the harness are committed so the checks can be repeated
without access to that local directory.
