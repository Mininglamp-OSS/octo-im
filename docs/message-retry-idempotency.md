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

Legacy stores need no format migration. Both existing secondary indexes (sender
and client message number) include channel/sequence suffixes. Admission seeks
through their intersection in one Pebble snapshot, loading only common primary
keys and verifying the complete retry key. A large same-number bucket shared by
other senders cannot permanently reject valid sends. Stale rows are checked
against the snapshot's current messages; no fixed candidate-count cap is used.
The caller's context bounds pathological intersections, and cancellation is
returned as an error, never as a missing key. DB I/O remains outside the Raft
owner. One in-flight Pebble operation cannot be interrupted by the context.

Channel apply persists the existing applied-index field. Legacy channels start
at zero and regain confirmation through replication (or the single-voter quorum
rule). Message state is already materialized by `AppendLogs`, so a Raft-confirmed
apply range durably advances its boundary without reading or re-marshaling any
historical payloads. It never seeds commitment from the stored tail. Generic
state machines still read bounded batches and run their payload apply callbacks.
An active channel with a non-empty tail but no confirmed commit boundary returns
a retryable conversation-boundary error until confirmation, rather than zero.

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
   memory and disk latency before opening full traffic.
4. Rollback also requires quiescing writes and switching the entire IM tier.
   Preserve data and retry records; old binaries lose the new idempotency and
   durable-ACK guarantees. Do not claim a rolling, zero-downtime cutover.

This PR does not repair live connection ownership after user-slot migration or
Octo's HTTP 503-to-400 proxy mapping. Those remain separate availability concerns;
see the review validation report for the R3/R4 evidence and its limits.
