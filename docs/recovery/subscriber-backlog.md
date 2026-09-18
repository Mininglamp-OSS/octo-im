# Subscriber backlog recovery (dependent on PR #50)

This change addresses frequent group/subzone subscriber mutations that previously
waited for all user conversation writes, or submitted cleanup without a durable
continuation. It is based on PR #50 at `93c6331a`. It does not include PR #51 or
claim to repair all Raft election, message delivery, or storage failures.

## Enablement and API contract

The feature is **off by default**. Upgrade every IM node to this version before
enabling it. Old nodes do not understand the new replicated commands. Pause
subscriber mutations and let legacy in-flight conversation writes finish during
the switch; then enable the following configuration on every node:

```yaml
subscriberRecovery:
  enabled: true
  workers: 2       # hard maximum 2 per node, shared by all channels/slots
  maxPending: 1024 # outstanding effects + finalization units per slot/DB partition
  interval: 50ms   # minimum delay between worker scan steps; must be >= 10ms
  timeout: 5s     # budget for a page, including target apply and checkpoint
```

Keep the feature enabled after accepting managed operations. Disabling workers
stops recovery; sending new legacy mutations into managed memberships bypasses
source intent generation. Downgrading a populated cluster is not supported by
this PR. This is a coordinated rollout, not a mixed-version feature switch.

With the feature enabled, the following non-personal channel endpoints use the
durable operation path: `/channel`, subscriber add/remove/remove-all, blacklist
add/set/remove/remove-all, and `/channel/delete` (disband plus subscriber cleanup).
Temporary subscribers retain their existing unsupported response. Live channels
still do not create recent conversations. Unblocking a still-subscribed member (including entries removed by blacklist
replacement) schedules fresh conversation initialization. It does not rejoin a
user who already left. This prevents a denylist tombstone from suppressing future
conversation writes after unblock. Message purging is not
implemented here; the existing `DeleteChannelAndClearMessages` is a no-op.

Supply a stable `operation_id` in the JSON body, or an `Idempotency-Key` header.
If both are present they must match. Retry an uncertain request with the same ID
and business input. IDs are scoped by channel ID and channel type. The same ID
with different input returns HTTP 409. Member order and duplicate UIDs do not
change request identity. An omitted ID is generated and returned, but a client
that loses that response cannot safely identify the original operation.

Example:

```json
{"channel_id":"group-1","channel_type":2,"operation_id":"leave-20260918-1","subscribers":["u1","u2"]}
```

HTTP 200 now acknowledges **durable source mutation and pending work**, not full
conversation synchronization. Existing `status: 200` is retained and `data`
contains the receipt. A typical state is `pending`, with `total`, `completed`,
`version`, `created_at`, `attempts`, `last_error`, and `next_attempt` fields.
Receipt timestamps are Unix nanoseconds.
`complete` means the target pages and tag invalidation have been acknowledged.
The operation may have been superseded by a newer leave/rejoin; completion means
its effects were applied or correctly skipped by lifecycle version checks.

Poll `POST /channel/subscriber_operation` with:

```json
{"channel_id":"group-1","channel_type":2,"operation_id":"leave-20260918-1"}
```

The status request routes to the current source slot leader. HTTP 404 means the
receipt is not visible there; an earlier timed-out proposal may still apply.
HTTP 503 with state `unknown` is also an uncertain outcome, never a guaranteed
rollback. Continue polling/retrying the original ID. HTTP 429 `backlog_full` is a
durable rejection with **no member mutation**; retrying that ID can be admitted
after capacity becomes available. HTTP 429 `subscriber admission busy` is a
request rejected before proposal. Other rejected receipts explain invalid
business state or an oversized reset. Invalid operation input returns HTTP 422.

## Persistence, ordering, and resource bounds

The source slot applies membership changes, an optional channel record, lifecycle
intents, an idempotency receipt, and a pending job in one Pebble sync batch.
Admission is in that same batch. Ordinary capacity rejection is a receipt, not a
Raft Apply error that would block the committed log. A source apply watermark
prevents replay from regenerating already completed work.

Workers scan durable pending records under current slot leadership. There is no
goroutine per group, in-memory task queue, or unbounded retry map. Each worker
loads one job at a time, rotates DB shards and records, and processes at most 16
effects per page. Targets are routed by **user slot**. Checkpoints compare the
exact source job version and previous cursor. Failed pages retain their previous
cursor and retry schedule; backoff starts at 250ms and caps at 32s. A failed
record does not hold the scan cursor and prevent other records from progressing.

Each target stores a lifecycle version for `(uid, channel, type)`. A real rejoin
gets a new canonical conversation ID; retained members keep their generation.
Old cleanup cannot remove a newer join and old add work cannot resurrect a later
departure. Normal conversation writes are fenced before legacy ID rewriting.
The first managed operation cleans duplicate rows from the old implementation;
adopting an existing member preserves its conversation read/unread state.

Conversation data and lifecycle commit atomically in the user DB. The reverse
channel/user relation is in another physical DB. Its `RelationDone` continuation
is persisted and retried before target acknowledgement. Tag invalidation targets
the current **slot** leaders of the ordinary and CMD channels. It is serialized
with authoritative tag construction and always rebuilds from current membership,
never from a stale job's add/remove delta.

The old Apply path regrouped conversation writes across intervening deletes when
a batch had at least ten logs. Apply now preserves command order; only contiguous
channel-config saves are grouped. Storage errors propagate to Raft without
panicking, and Apply retries use tick-paced backoff rather than a tight loop.
The small backoff implementation is adapted from `fix/raft-orphan-learner-bugs`
commit `80f5699b`; unrelated Raft changes are not imported.

Bounds:

- 4 concurrent subscriber HTTP handlers per API instance, including forwarding.
- 1 MiB HTTP body and replicated source operation; at most 4,096 supplied members
  and at most 4,096 unique members in a reset union, with UID/channel/operation ID length limits.
- Pending capacity is **per source slot and physical channel DB partition**,
  not a node-wide quota. With the default 1,024 units, a single operation needing
  more than 1,024 units is rejected even when that partition is empty. Increase
  deliberately, or split large additions/removals into bounded requests. A large
  reset/remove-all that exceeds the limit must be replaced by explicit batches.
- 1–2 workers per node, 16 effects per page, bounded proposal deadlines and retry
  schedules. At most six workers run in a three-node cluster configured with two
  per node, even when client concurrency is two.
- Completed receipts and lifecycle tombstones are retained. This preserves old
  request identity and leave/rejoin fencing but is **not a disk retention policy**.
  Storage grows with distinct operations/memberships. Monitor disk capacity;
  automatic tombstone/receipt GC requires a separate replay/retention contract.

The first migration cleanup can still scan a user's existing conversation rows;
large historical tables take time. Bounded scheduling does not guarantee a fixed
recovery time. Under sustained arrival faster than drain, partitions reach their
admission limit. Disk repair, available target leaders, and a functioning Raft
quorum remain prerequisites for recovery. Non-API code that directly mutates
subscribers must use `SubmitSubscriberOperation` to obtain these guarantees;
legacy migration/import entry points are not retroactively repaired.

## Observe recovery

`GET /channel/subscriber_recovery` reports local worker page/failure counters,
last successful progress time, last error, and **durable pending units owned by
this node's current source slots**, broken down by physical DB partition. Query
all nodes to inspect cluster backlog. During leadership changes, these are
individual reads, not a cluster-wide atomic snapshot. Runtime counters reset on
restart; receipts, pending progress, and retry schedules survive.

Measure separately after stopping a finite burst:

1. Request responsiveness, with foreground probes and latency/error rate.
2. Backlog drain, requiring pending units zero and every accepted receipt complete.
3. Business consistency, checking actual member/conversation state against final
   desired membership after old requests, removes, resets, and rejoins.

Do not treat an HTTP 200, one successful probe, or zero observed queue length
alone as proof of all three. No ETA is emitted when drain rate is unknown.

## Verification and safe local smoke test

Storage and Store tests cover atomic admission, durable reopen/checkpoints,
replay after completion, stable request IDs, out-of-order leave/rejoin, cache
fencing, retained legacy state, interrupted reverse relations, retry fairness,
global worker bounds, lost replies after apply, and mixed Apply ordering.

`test/recovery/subscriber_smoke.py` runs five finite scenarios against three
actual local nodes: create burst, stable retries/conflicts, leave/rejoin, abrupt
restart with pending reset work, and remove-all drain/consistency. Each burst is
12 groups × 8 members at client concurrency **2**. Its results are smoke-test
observations, not the previous PR #51 five-round benchmark or a production SLA.

Run the binary and script inside a cgroup with a hard memory/CPU limit and swap
disabled. The output directory **must be on disk**, not `/tmp` when `/tmp` is
tmpfs: Raft WAL preallocation consumes real memory there. Example on Linux:

```sh
go build -o ./bin/subscriber-recovery .
systemd-run --wait --pipe --property=User=root \
  --property=MemoryMax=4G --property=MemoryHigh=3G \
  --property=MemorySwapMax=0 --property=CPUQuota=200% \
  --property=TasksMax=512 --property=RuntimeMaxSec=600 \
  --working-directory="$PWD" \
  python3 test/recovery/subscriber_smoke.py \
    --binary ./bin/subscriber-recovery --output ./recovery-validation
```

Use an unused port range (default 24101–24133). The script checks the memory
cgroup and filesystem, launches only three owned child processes, and terminates
them on exit. Reports include per-request raw timings, drain/consistency
observations, memory peak, and cgroup OOM events. Its first-successful-probe metric
does not claim sustained request recovery after an induced overload.
