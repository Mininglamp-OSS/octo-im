# Recovering inter-node send admission

A peer reconnect, stale user/channel leader, or rejected event worker previously consumed accepted events without either completing the operation or returning a useful SENDACK. This change retains queued transport batches until the peer can accept them, and moves user/channel forwarding onto acknowledged, versioned admission RPCs.

## Contract

- `/wk/forward/user/v1` and `/wk/forward/channel/v1` acknowledge whole-batch event admission, **not persistence or recipient delivery**. Routes validate the target before enqueueing.
- An envelope carries the remaining part of one five-second budget and at most four routing hops. Each sender makes at most six requests (750 ms per request), re-resolving authority between attempts. A relative budget avoids rejecting valid requests because two nodes' wall clocks differ. Distribution/webhook events retain their fixed destination.
- Retries happen only after an explicit pre-admission retry response. A missing response is outcome-ambiguous and is never replayed, even for a persistent SEND: replaying the full pre-persistence pipeline could repeat permission or plugin side effects. Ambiguous SENDs receive the non-retryable `ReasonSystemError`; definite admission failures receive `ReasonNodeNotMatch`. Post-commit distribution failures never emit a second contradictory SENDACK.
- Persistence preserves the distinction between temporary leader/quorum/transport unavailability (`ReasonNodeNotMatch`) and permanent errors such as idempotency content conflicts. PR46 remains the authority for durable deduplication and canonical message identity.
- A persistence result marked `Duplicate` still returns the canonical success ACK, but skips repeated plugin, webhook, and distribution side effects.
- Worker admission happens before dequeuing, so saturation preserves queued user/channel events. The lower transport queue is bounded by count/bytes; failed batches remain charged to backpressure until reconnect or shutdown. Shutdown releases retained memory and records drops. No durable queue/outbox is introduced.

## Compatibility and dependencies

Based on `fix/raft-orphan-learner-bugs` at `4ce16cac8153a8cc49cdf2a200e13dcf455e9d71` (the exact base used is recorded in git; PR43/46/47 are merged). Before the first side-effecting v1 request, a node probes a read-only capability route. Peers without that route receive the legacy fire-and-forget event during a rolling upgrade; once a v1 request has an ambiguous outcome, the sender never guesses by falling back. The separate session-recovery change appends session identity fields; complete encoded-descriptor comparison preserves those fields while a mismatched cached descriptor still has its activity refreshed.

The design borrows bounded rerouting and committed idempotency principles from WuKongIM upstream main `111bdee669e402f2c6dd0c285fbea315ea77007d`; it is adapted to this fork's event queues, not a direct backport. An admission success is insufficient to guarantee a live push after process failure; history reconciliation remains necessary.

## Verification

Regression tests cover former leaders, unknown routes, bounded attempts, no replay after an ambiguous request, fixed distribution destinations, worker rejection, authenticated descriptor reuse, concurrent queue resize/close, unavailable-peer backpressure, and recovery over a real TCP connection. New logic is checked with `go test -race` and repeated runs.

The full `go test -p 1 -timeout 30s ./...` suite is also run before pushing. It is not green: current repository/environment failures include the fixed WS listener, track bitmask expectation, a user-event mock timeout, an unstarted client in the batch-send test, database assertions, and network/reconnect tests. An expanded real-cluster RPC race run also reports existing Raft/config/client races; the same function families reproduce on the unchanged baseline. These are not reported as passing tests. This PR does not claim a completed five-round failover experiment.
