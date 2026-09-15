# Recovering inter-node send admission

A peer reconnect, stale user/channel leader, or rejected event worker previously consumed accepted events without either completing the operation or returning a useful SENDACK. This change retains queued transport batches until the peer can accept them, and moves user/channel forwarding onto acknowledged, versioned admission RPCs.

## Contract

- `/wk/forward/user/v1` and `/wk/forward/channel/v1` acknowledge whole-batch event admission, **not persistence or recipient delivery**. Routes validate the target before enqueueing.
- An envelope carries one five-second deadline and at most four routing hops. Each sender makes at most six requests (750 ms per request), re-resolving authority between attempts. Distribution/webhook events retain their fixed destination.
- Only persistent SENDs with nonempty `client_msg_no`, ACKs and ping/pong frames can be retried after an ambiguous request. Other events stop on ambiguity. A SEND without confirmed admission receives `ReasonNodeNotMatch` with its original client key/sequence and no invented canonical message ID.
- Persistence preserves the distinction between temporary leader/quorum/transport unavailability (`ReasonNodeNotMatch`) and permanent errors such as idempotency content conflicts. PR46 remains the authority for durable deduplication and canonical message identity.
- Worker admission happens before dequeuing, so saturation preserves queued user/channel events. The lower transport queue is bounded by count/bytes; failed batches remain charged to backpressure until reconnect or shutdown. Shutdown releases retained memory and records drops. No durable queue/outbox is introduced.

## Compatibility and dependencies

Based on `fix/raft-orphan-learner-bugs` at `4ce16cac8153a8cc49cdf2a200e13dcf455e9d71` (the exact base used is recorded in git; PR43/46/47 are merged). Deploy the versioned forwarding implementation to all IM nodes before enabling client retries. Unsupported RPC paths fail explicitly; there is no fallback to unacknowledged forwarding. The separate session-recovery change appends session identity fields; complete encoded-descriptor comparison preserves those fields when both PRs are combined.

The design borrows bounded rerouting and committed idempotency principles from WuKongIM upstream main `111bdee669e402f2c6dd0c285fbea315ea77007d`; it is adapted to this fork's event queues, not a direct backport. An admission success is insufficient to guarantee a live push after process failure; history reconciliation remains necessary.

## Verification

Regression tests cover former leaders, unknown routes, bounded attempts, ambiguous replay eligibility, fixed distribution destinations, worker rejection, authenticated descriptor reuse, concurrent queue resize/close, unavailable-peer backpressure, and recovery over a real TCP connection. New logic is checked with `go test -race` and repeated runs.

The full `go test -p 1 -timeout 30s ./...` suite is also run before pushing. It is not green: current repository/environment failures include the fixed WS listener, track bitmask expectation, a user-event mock timeout, an unstarted client in the batch-send test, database assertions, and network/reconnect tests. An expanded real-cluster RPC race run also reports existing Raft/config/client races; the same function families reproduce on the unchanged baseline. These are not reported as passing tests. This PR does not claim a completed five-round failover experiment.
