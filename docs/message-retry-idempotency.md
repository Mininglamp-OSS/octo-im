# Message retry idempotency

Persisted sends use `(channel ID, channel type, sender UID, client_msg_no)` as
an idempotency key. Empty client numbers retain append behavior. A retry reuses
the original message ID and sequence; reusing a key with different immutable
content fails. Existing duplicate history is not rewritten.

Admission snapshots buffered entries on the channel Raft owner, resolves
stored entries outside the shared event loop, then checks the owner instance,
term, configuration and log revision before allocating any sequence. Concurrent
appends, replacements or truncation invalidate the snapshot and retry admission
within the caller's deadline. Store completion cannot hide an entry: the
snapshot already contains every not-yet-stored entry at its boundary.

A database hit alone never produces a success ACK: the original entry must be
committed and durably applied. Canonical payload verification also runs outside
the owner, followed by a final leader/configuration/commit/apply fence. A normal
append cannot modify that committed prefix. Truncating below the durable applied
marker is rejected, rather than discarding evidence of acknowledged messages.

Legacy stores need no format migration. The existing retry secondary index is
ordered by client hash followed by channel and sequence; lookups now seek only
the requested channel. Sender-scoped admission examines at most 1,024 candidate
rows and fails with `ErrMessageRetryLookupLimit` if the bucket is larger and no
match has been found. This is an explicit failure, never a not-found result that
could create a duplicate. Stale index entries left by historical truncation are
verified against current rows and count toward this limit. Large same-number
legacy buckets require index maintenance/a dedicated scope index before they
can serve a previously unseen sender; repeatedly resending cannot repair this
limit. This revision chooses bounded lookup, not a new index migration.

Channel apply persists the existing applied-index field. Legacy channels start
at zero and regain confirmation through replication (or the single-voter quorum
rule). Recovery reads at most 1,000 logs and the configured `MaxLogSizePerBatch`
(default 10 MiB) per apply request, advancing the durable marker after each
successful batch. One log may exceed the byte target. Recovery still takes time
proportional to history, but never allocates the whole history in one batch.

The channel proposal RPC is `/rpc/channel/propose/v2`. Its response preserves
request correlation and canonical identity. Old RPC and generic channel proposal
forwarding are rejected; there is no legacy fallback that could silently weaken
idempotency. HTTP `/message/send` returns an enqueue receipt, not a committed
canonical identity. WebSocket success ACKs and later dispatch use the canonical
identity; plugin/webhook/dispatch side effects remain at least once. Outages can
exceed a client's three-second ACK budget even when retries are idempotent.

## IM-tier upgrade runbook

1. Schedule an IM write outage; stop admitting sends at the gateway/API and let
   in-flight requests drain. Keep all persistent volumes and take the normal
   backup. Clients must retain retry keys across reconnects.
2. Stop every old IM process, including nodes eligible to become channel leader.
   Deploy the same new binary to all IM nodes. Do not mix old and v2 proposal
   implementations behind live write traffic.
3. Start a voting quorum, then all remaining nodes. Verify membership, one
   control-plane leader and slot readiness. Resume writes only after every
   reachable IM peer supports v2 and real cross-node test sends return canonical
   IDs/sequences. Warm representative legacy channels and watch recovery markers,
   memory, disk latency and lookup-limit errors before opening full traffic.
4. Rollback also requires quiescing writes and switching the entire IM tier.
   Preserve data and retry records; old binaries lose the new idempotency and
   durable-ACK guarantees. Do not claim a rolling, zero-downtime cutover.

This PR does not repair live connection ownership after user-slot migration or
Octo's HTTP 503-to-400 proxy mapping. Those remain separate availability concerns;
see the review validation report for the R3/R4 evidence and its limits.
