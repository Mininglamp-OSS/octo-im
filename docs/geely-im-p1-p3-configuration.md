# Cluster P1–P3 Stability, Readiness, Log Consistency, and Bounded Catch-Up Configuration

This document describes the configuration options, probe endpoints, diagnostic endpoints, and compatibility/rollout rules introduced on top of `v2.2.5-20261021-fix1` (`21cd8f8860eba7804f04039190ec8d92d620cf0b`) for Issue #114.

---

## 1. Configuration Options Reference

All options below are loaded at process startup via Viper (`wk.yaml` or `WK_*` environment variables) and validated in `Options.Check()` before storage or network services start.

| YAML Key | Environment Variable | Type / Unit | Valid Range | Old Default (`21cd8f88`) | New Default | Reload Mode | Small-Resource (`1C1G`) Value | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `db.memTableSize` | `WK_DB_MEMTABLESIZE` | `int` (bytes) | `> 0` | `16777216` (`16 MiB`) | `16777216` (`16 MiB`) | Startup only | `8388608` (`8 MiB`) | Per-shard Pebble MemTable size for `wukongimdb`. |
| `db.slotMemTableSize` | `WK_DB_SLOTMEMTABLESIZE` | `int` (bytes) | `> 0` | Hardcoded `134217728` (`128 MiB`) in `slot/storage_pebble.go` | `16777216` (`16 MiB`, inherits `db.memTableSize`) | Startup only | `8388608` (`8 MiB`) | Per-shard Pebble MemTable size for slot `logdb`, wired from `Options` into `slot.NewServer`. Eliminates the `8 * 128 MiB * 4 = 4.0 GiB` stop-writes memory ceiling. |
| `db.blockCacheSize` | `WK_DB_BLOCKCACHESIZE` | `int64` (bytes) | `>= 0` (`0` disables shared cache) | Independent unshared Pebble default per DB | `16777216` (`16 MiB`) | Startup only | `16777216` (`16 MiB`) | Shared `pebble.Cache` size across all local `wukongimdb` and `slot` `logdb` Pebble shards. |
| `db.shardNum` | `WK_DB_SHARDNUM` | `int` (shards) | `> 0` (immutable once data dir is non-empty) | `8` | `8` | Startup only (immutable on existing data dir) | `8` (keep topology identical) | Number of `wukongimdb` Pebble shards. Validated at startup by `ValidatePebbleShardDir` against existing `000..NNN` directories. |
| `db.slotShardNum` | `WK_DB_SLOTSHARDNUM` | `int` (shards) | `> 0` (immutable once data dir is non-empty) | `8` (not wired to `slot.NewServer`) | `8` (wired to `slot.NewServer`) | Startup only (immutable on existing data dir) | `8` (keep topology identical) | Number of slot `logdb` Pebble shards. Validated at startup by `ValidatePebbleShardDir`. |
| `db.maxMessagesPerBatch` | `WK_DB_MAXMESSAGESPERBATCH` | `int` (messages) | `> 0` | Unbounded (entire `AppendMessages` slice in 1 `Batch`) | `256` | Startup only | `128` – `256` | Maximum messages written per Pebble `Batch` sub-chunk in `wkdb.AppendMessages`. |
| `db.maxBatchBytes` | `WK_DB_MAXBATCHBYTES` | `int` (bytes) | `> 0` | Unbounded | `2097152` (`2 MiB`) | Startup only | `1048576` (`1 MiB`) | Maximum estimated bytes per Pebble `Batch` sub-chunk in `wkdb.AppendMessages`. |
| `cluster.maxLogSizePerBatch` | `WK_CLUSTER_MAXLOGSIZEPERBATCH` | `uint64` (bytes) | `> 0` | `10485760` (`10 MiB`) | `2097152` (`2 MiB`) | Startup only | `524288` (`512 KiB`) | Maximum bytes returned per `GetLogs` catch-up or apply batch in `raftgroup`, `slot`, and `channel`. |
| `cluster.maxLogCountPerBatch` | `WK_CLUSTER_MAXLOGCOUNTPERBATCH` | `int` (entries) | `> 0` | Unbounded (`0`) | `500` | Startup only | `200` | Maximum log entries returned per `GetLogs` batch in `raftgroup`, `slot`, and `channel`. |
| `cluster.learnerToFollowerMaxApplyLag` | `WK_CLUSTER_LEARNERTOFOLLOWERMAXAPPLYLAG` | `uint64` (entries) | `>= 0` | Unchecked (`0`, only `Index`/`StoredIndex` checked) | `8` | Startup only | `8` | Maximum allowed `LastLogIndex - AppliedIndex` lag on a replica when promoting `Learner -> Follower`, `Learner -> Leader`, or `Follower -> Leader`. |
| `cluster.maxReadyApplyLag` | `WK_CLUSTER_MAXREADYAPPLYLAG` | `uint64` (entries) | `> 0` | Unchecked (`0`) | `64` | Startup only | `64` | Maximum ongoing `CommittedIndex - AppliedIndex` lag allowed on local slots for `/readyz` and `/health` after initial bootstrap completes. |
| `cluster.maxApplyStallTicks` | `WK_CLUSTER_MAXAPPLYSTALLTICKS` | `int` (ticks) | `> 0` | Unchecked (`0`) | `50` | Startup only | `50` | Maximum consecutive Raft ticks a local slot may remain in `applying == true` without `ApplyResp` before `/readyz` reports unready (`503`). |
| `cluster.maxSlotApplyWait` | `WK_CLUSTER_MAXSLOTAPPLYWAIT` | `duration` | `>= 0` | Unbounded (consumed full request context) | `500ms` | Startup only | `500ms` | Maximum time a local slot leader waits for `AppliedIndex >= CommittedIndex` before failing fast with `ErrConversationReadRetry`. |
| `cluster.handoffDrainTimeoutTick` | `WK_CLUSTER_HANDOFFDRAINTIMEOUTTICK` | `int` (ticks) | `> 0` | Unbounded (stalled handoff blocked writes forever) | `100` | Startup only | `50` – `100` | Maximum ticks a leader remains paused (`stopPropose == true`) waiting for a target replica to finish pre-publication handoff before aborting the drain pause and resuming local service. |
| `cluster.maxConcurrentChannelMigrations` | `WK_CLUSTER_MAXCONCURRENTCHANNELMIGRATIONS` | `int` (channels) | `> 0` | Unbounded | `8` | Startup only | `4` – `8` | Maximum concurrent background channel Learner catch-up migrations scheduled by `channelConfigReconciler`. |
| `cluster.rejectConnBeforeReady` | `WK_CLUSTER_REJECTCONNBEFOREREADY` | `bool` | `true` / `false` | `false` | `true` | Startup only (requires process/container restart) | `true` | Rejects external client TCP/WebSocket connections in `Server.onConnect` until `CheckLocalServiceReady()` succeeds. Changing `WK_CLUSTER_REJECTCONNBEFOREREADY=false` requires a process/container restart. |
| `cluster.channelLeaderAutoBalance` | `WK_CLUSTER_CHANNELLEADERAUTOBALANCE` | `bool` | `true` / `false` | `false` | `false` | Startup only | `false` | Reserved toggle for background active channel leader auto-rebalancing (disabled by default for conservative rollout). |

---

## 2. Health, Readiness, and Diagnostic HTTP Endpoints

| Endpoint | Purpose | Success Status | Failure Status | Behavior |
| :--- | :--- | :--- | :--- | :--- |
| `GET /healthz` | Kubernetes `livenessProbe` | `200 OK` | `503 Service Unavailable` | Checks only local process liveness (`CheckLocalProcessHealth`). Never fails due to Raft catch-up, peer unavailability, or local storage/apply readiness degradation, preventing kubelet restart loops during recovery. |
| `GET /readyz` | Kubernetes `readinessProbe` | `200 OK` | `503 Service Unavailable` | Checks `CheckLocalServiceReady()` reversibly both during bootstrap and after `bootstrapReady` is reached: local storage directories (`DataDir`, `db/wukongimdb`, `cluster`) and Pebble shards (`wukongDB.CheckWriteHealth()` and `slotServer.CheckWriteHealth()`) must be writable, local node must be `Online` and `NodeStatusJoined`, not a `Learner` on any local slot, all local slots must have reached `BootstrapCommittedIndex` and initial `CommittedIndex`, ongoing slot apply lag (`CommittedIndex - AppliedIndex <= MaxReadyApplyLag`), slot/channel apply & store failures (`ApplyFailureCount == 0`, `StoreFailureCount == 0`), and slot/channel apply stall ticks (`ApplyingTicks <= MaxApplyStallTicks`) must be within bounds. If any post-bootstrap check fails, `bootstrapReady` resets to `false` (`503`) and automatically recovers to `200 OK` once repaired. |
| `GET /health` | Legacy readiness endpoint | `200 OK` | `500 Internal Server Error` | Enforces `CheckLocalServiceReady()` plus cluster-wide slot status check for backward compatibility before `readinessProbe` is switched to `/readyz`. |
| `GET /cluster/slots/:id/digest?from=<idx>&to=<idx>` | Read-only slot log digest verifier | `200 OK` | `400 Bad Request` | Computes a deterministic SHA-256 rolling digest over `(Index, Term, CRC32(Data), Data)` for slot `:id` in `[from, to]` (defaults to the last 1,000 committed entries) to verify that converged replicas have identical underlying log contents without log divergence. |

---

## 3. Compatibility and Rollout Notes

1. **Wire Compatibility**:
   - `SyncReq` carries `AppliedIndex` (encoded as `appliedIndex + 1`) using optional flag bit `1 << 13` (`0x2000`). Legacy `21cd8f88` nodes ignore unknown flag bits or omit the field (`AppliedIndex == 0`), in which case the leader falls back to `StoredIndex` gating without deadlocking mixed-version clusters.
   - Internal fields `Event.TruncateID`, `Event.RoleChangeID`, and `Event.StoreEpoch` are local in-memory fields only and are not serialized on the network wire.
   - `clusterconfig` uses only existing `CMDType` values (`1..10`) and rejects unknown `CMDType` values in `handleCmd` with an explicit error instead of silently advancing state machine versions.
2. **Storage Compatibility**:
   - No Pebble format version or key-value schema changes are introduced.
   - `ValidatePebbleShardDir` prevents accidental startup with a changed `db.shardNum` or `db.slotShardNum` against an existing non-empty data directory.

---

## 4. Internal Peer Token Forwarding and `:5001` / `:5300` Authorization Contract

1. **Fail-Closed Internal Peer Token Forwarding & Redirect Refusal (`service.TrustedPeerNodeHeaders` / `service.TrustedPeerURLHeaders`)**:
   - When `options.G.ManagerToken` (`WK_MANAGERTOKEN`) is configured, internal peer HTTP requests (`internal/api/conversation_sync_read.go`, `internal/api/conversation_latest.go`, `internal/api/user.go`, `internal/api/message.go`, `internal/manager/manager_systemaccount.go`, `internal/plugin/rpc_conversation.go`, `internal/plugin/rpc_plugin.go`, `internal/plugin/api.go`, `pkg/wkhttp/http.go`, `pkg/network/network.go`) attach `token: options.G.ManagerToken` only after fail-closed verification via `service.IsTrustedClusterPeer(node)` / `service.IsTrustedClusterPeerURL(url)` that the target node is registered in `service.Cluster` with a matching `ApiServerAddr`. If `service.Cluster` is nil or panics, trust verification fails closed (`false`).
   - All internal peer HTTP clients set `CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }`, refusing HTTP `301/302/307/308` redirects so custom `token` headers can never leak across redirects.
2. **Strict `:5001` (`apiServer`) vs `:5300` (`managerServer`) Boundary, Invalid-JWT Rejection, and RBAC Enforcement**:
   - `:5001` (`apiServer`) strictly requires `token == options.G.ManagerToken` on all non-health endpoints whenever `WK_MANAGERTOKEN` is non-empty (bare JWTs without `ManagerToken` are rejected with `401 Unauthorized`).
   - On both `:5001` and `:5300`, if an explicit `Authorization` header/query is present, JWT parsing and signature/expiration validation run first: an invalid or expired JWT is immediately rejected with `401 Unauthorized` (never falling back or escalating to `options.G.ManagerUID` even when a gateway `token` header is present).
   - Mutating routes (`POST /cluster/slots/:id/migrate`, `POST /cluster/slots/:id/election`, `POST /cluster/channels/:channel_id/:channel_type/migrate`, `POST /cluster/channels/:channel_id/:channel_type/start`, `POST /cluster/channels/:channel_id/:channel_type/stop`, `POST /conn/remove`, `POST /conn/kick`, `POST /tag/remove`, `POST /pluginconfig/:plugin`, `POST /plugin/bind`, `POST /plugin/unbind`, `POST /plugin/uninstall`, and non-GET `ANY /plugins/:plugin/*path`) enforce `Auth.HasPermissionWithContext(..., auth.ActionWrite)` so read-only `guest` (`[*:r]`) JWTs are rejected with `403 Forbidden`.
   - Cross-node `:5300 -> :5001` forwards (`c.Forward`, `c.ForwardWithBody`, `network.Get`, `network.Post`) to verified cluster peers automatically attach `token: options.G.ManagerToken` via `TrustedPeerURLHeaders` while preserving the caller's `Authorization` header.

---

## 5. Bounded Memory Claims, Synchronous HTTP Persistence Admission, and Verification Boundaries

1. **Memory Threshold Definitions & Historical vs Current Measurements (`1 vCPU / 1 GiB / 0 Swap`)**:
   - Under Project Charter `XIN-2835`:
     - **Container hard limit**: **`1 GiB` (`1,024 MiB`, `0 Swap`)** (`--memory=1g --memory-swap=1g`).
     - **Soft warning threshold**: **`512 MiB`**.
     - **`GOMEMLIMIT` / proposed `< 75%` peak RSS criterion**: **`768 MiB`** (runtime soft GC target and proposed acceptance threshold pending Owner Decision `D2`, **not** the container hard cap).
   - In historical Round 1 (`17b1a5d4`) 3-node container black-box QA tests (`XIN-2851`, executed by `Grace-mini` / `Grace-mini2`):
     - `TC-BASE-002` / `TC-BASE-003` measured leader `node1` at `162.03 / 217.47 / 262.55 MiB`, `node2` at `160.53 / 163.62 / 196.79 MiB`, and joining `node3` at `487.46 / 602.04 / 743.63 MiB` across the `100k / 500k / 1m` tiers (`0` OOM kills).
     - `TC-BASE-001` measured joining `node3` peak RSS at **`806.4 MiB`** during `1m`-message catch-up (`0` OOM kills within the `1 GiB` container hard limit, exceeding the `512 MiB` soft warning and the `768 MiB` `GOMEMLIMIT` / proposed 75% criterion pending Owner Decision `D2`).
   - **Claim Boundary**: This demonstrates bounded survival (`0` OOM kills within the `1 GiB` container hard limit) under the tested `100k/500k/1m` catch-up workloads; it is not an unconditional proof that all sender/receiver OOMs or memory leaks are eliminated under arbitrary topologies, and the historical production `12 GiB` OOM root cause remains `UNCONFIRMED` pending production heap/goroutine pprof capture.
2. **Synchronous HTTP Persistence Admission & Non-Blocking Poller Retention (`POST /message/send` and `POST /message/sendbatch`)**:
   - When `NoPersist == 0`, `POST /message/send` and `POST /message/sendbatch` (`internal/api/message.go`) persist messages through `service.Store.AppendMessages` (grouping by `fakeChannelId` with bounded concurrency in `sendbatch`) before returning `HTTP 200 OK` (or reporting failed subscribers in `fail_uids`), guaranteeing `1:1` reconciliation between `HTTP 200`-accepted messages and persisted messages in `wukongimdb` with unique `message_id` and `client_msg_no` accounting.
   - `channelHandler.isTimeout()` (`internal/channel/event/handler.go`) never evicts a channel handler while `processing` is active or `pending.eventQueue` is non-empty, `advanceEvents` retains `EventChannelOnSend` and `EventChannelDistribute` batches in `pending.eventQueue` across transient `slotLeaderId == 0` handoffs, and `sendToNode` (`internal/channel/handler/base.go`) is non-blocking while `forwardGate` preserves shared poller worker isolation.
3. **Fault-Injection and Topology Boundaries**:
   - Deterministic kernel network fault injection (`tc netem` / `iptables` requiring `CAP_NET_ADMIN`), cgroup blkio throttling (`/sys/fs/cgroup/io.max` unsupported on macOS OrbStack VirtioFS bind mounts), and 5-node `S4` topology execution (`R=2` `quorum=2` mathematical boundary when 2 replicas fail simultaneously, pending Owner decision `D1`) remain explicit external environment/governance boundaries.


