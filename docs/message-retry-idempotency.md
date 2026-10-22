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
idempotency. HTTP `/message/send` acknowledges persistence (see below).
WebSocket success ACKs and later dispatch use the canonical identity;
plugin/webhook/dispatch side effects remain at least once. Outages can exceed a
client's three-second ACK budget even when retries are idempotent.

## HTTP send acknowledgement

`/message/send` and `/message/sendbatch` answer only after the channel slot
leader reports the persistence result of the message. The accepting node
registers a waiter keyed by the message ID of each submission attempt and
stamps its node ID into the event's existing `SourceNodeId` field. After
`persist()` the leader reports the result back (in process, or with cluster
message `3003`). The wait is bounded by `cluster.reqTimeout`. Timeout,
durability and Sync/NoSync settings are unchanged.

| Result | `/message/send` response |
| --- | --- |
| Persisted | HTTP 200 with canonical `message_id`, `message_seq` and `client_msg_no` |
| Definitely rejected (permission, conflicting retry key, other non-retryable store error) | HTTP 400, unchanged error shape |
| Definitely not persisted (no reporting leader, or every attempt failed before a proposal) | HTTP 503, `data.outcome = "not_persisted"` |
| Possibly persisted (deadline after proposal, lost report, shutdown) | HTTP 504, `data.outcome = "unknown"` |

Error responses include `data.client_msg_no`, including a server-generated
one, so the caller can retry idempotently. A missing report is never treated as
success.

The server makes at most three attempts within the budget. It replays only
after every earlier attempt has reported a retryable result. Attempts share the
`client_msg_no`. If an earlier attempt commits late, the replay becomes a
duplicate of it and returns its canonical identity. No second durable message
is created. If any attempt may have committed, a later rejection still
returns 504.

`/message/sendbatch` waits for all subscribers, with at most 16 in flight. If
it has only definite failures, it keeps the existing HTTP 200 `fail_uids` and
`reason` response. If any subscriber's outcome is unknown, it returns HTTP 504,
with `unknown_uids` listing those subscribers. NoPersist messages are not
stored, so they are still acknowledged once queued. Plugin RPC sends keep the
asynchronous path.

Mixed versions: a leader that does not serve
`/wk/channel/sendresult/capability/v1` cannot report, so a newer API node
returns 503 without submitting. An older API node leaves `SourceNodeId` zero,
and a newer leader then sends no report. The whole-tier runbook below still
applies.

Before submitting through a remote leader, the API node needs positive evidence
that the leader reports results. It probes the capability route with a 250 ms
attempt, then a 1 s attempt, each limited so at least half of the remaining
send budget is left for the send. Concurrent sends to the same leader share one
probe.

| Capability state | Send behaviour |
| --- | --- |
| Leader answered OK within 30 s | Submit without probing |
| Leader answered OK 30 s to 5 min ago | Submit; one send refreshes the probe and no send waits for it. A refresh without an answer does not extend the 5 min limit |
| Leader answered with a non-OK status within 5 s | HTTP 503, log `reason=capability_unsupported` |
| No answer to either attempt (timeout, connection not authenticated, no connection) | HTTP 503 for this send only, log `reason=capability_inconclusive`. Nothing is cached; the next send probes again |

Each of these 503 responses is returned before any attempt is submitted, so
the message was not persisted. A leader with retained positive evidence that
stops reporting, for example after a rollback, yields 504 for at most 5 min,
never 200. The HTTP response shape is unchanged; the reason appears only in the
`API send not persisted` warning (fields `reason`, `leader`, `cached`, `stale`,
`probe_err_class`) and in the metrics `api_send_not_persisted_total{reason}`,
`sendresult_capability_probe_total{result}`,
`sendresult_capability_probe_latency` and
`sendresult_capability_stale_serve_total`. Other 503 reasons are
`leader_unknown`, `attempts_unavailable` (every attempt reported a retryable
failure), `budget_exhausted` and `registry_closed` (shutdown).

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
