# Message retry idempotency

Persisted sends use `(channel ID, channel type, sender UID, client_msg_no)` as
an idempotency key. Empty client numbers retain append behavior. A retry reuses
the original message ID and sequence; reusing a key with different immutable
content fails. Existing duplicate history is not rewritten.

Lookup, buffered-log inspection, batch coalescing and sequence allocation run
on the channel Raft owner. A database hit alone never produces a success ACK:
the original entry must be committed and durably applied, with its identity
rechecked before returning. Leadership/config changes abort a pending attempt.
Channel apply now persists the existing applied-index field. Legacy stores
without that marker start conservatively at zero and regain confirmation via
replication (or the normal single-voter quorum rule), not by trusting the tail.

The channel proposal RPC is `/rpc/channel/propose/v2`. Its response preserves
both the request correlation ID and canonical message ID. Old RPC and generic
channel proposal forwarding are rejected, and new callers reject missing
canonical fields. **Upgrade all IM nodes together**: mixed-version writes may
fail, and an old leader cannot enforce this guarantee. No database migration
is required; rolling back loses the new commit-boundary/idempotency guarantees.

WebSocket success ACKs, persisted history and subsequent dispatch use canonical
identity. Dispatch and external plugin/webhook side effects remain at least
once: a retry may be the only delivery opportunity after commit-before-crash.
This is not exactly-once external delivery. HTTP `/message/send` still returns
an enqueue receipt before persistence; its immediate ID is not a canonical
commit receipt. Retries do not guarantee a three-second ACK during an outage.

Validation includes real Pebble stores, buffered/concurrent/batched retries,
content conflicts, missing quorum, apply failure, restart with and without an
applied marker, leadership changes, real TCP forwarding, and handler mapping.
Two existing races exposed on this path are also addressed: batch completion
channel release and the background truncation check's mutable Raft index read.
