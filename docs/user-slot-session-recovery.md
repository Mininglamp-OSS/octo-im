# Recovering physical sessions after user-slot movement

A user-slot leader could change while an authenticated WebSocket stayed open on another node. The new leader had no logical connection entry, so a committed message could miss the recipient's live socket. This change gives physical socket owners a registry independent of user-slot ownership, and lets the new leader reconstruct its logical view.

## Contract

- Each physical owner has a random boot identity; each prepared socket has a random session identity. Only successful authentication of that exact live socket publishes an authenticated snapshot. A serialized `Auth` flag alone is not accepted as evidence.
- `/wk/presence/snapshot/v1` returns authenticated sessions for at most 128 UIDs. Recovery reads current online owners with at most four concurrent requests, checks the current cluster version/UID authority again, and publishes only a complete snapshot. Missing replies are unknown presence, not offline evidence.
- Recovery has a three-second budget, up to three attempts and a bounded per-node recovery gate. Positive readiness is cached for five seconds (and rechecked if the logical view was evicted); negative snapshots are not cached. The ready cache is capped at 32,768 UIDs.
- Owners periodically touch each UID's current leader. Touches carry only UIDs; the receiver pulls fresh snapshots, so replayed touches cannot authenticate a dead socket. Snapshot requests never acquire the recovery gate.
- Close invalidates an in-flight snapshot generation. Connection lookup/removal and physical writes compare owner boot and session identities. Logical removal no longer deletes the physical connection manager's entry by numeric ID; only the socket close path owns that cleanup.
- Distribution attempts recovery before deciding whether a recipient is offline. Exhausted recovery skips that live-distribution attempt without falsely reporting offline. There is no durable delivery outbox: live delivery remains best effort and history reconciliation is required after interruption.

## Compatibility and scope

Based on `fix/raft-orphan-learner-bugs` at `4ce16cac8153a8cc49cdf2a200e13dcf455e9d71`, with PR43/46/47 already merged. Deploy this implementation across all IM nodes: older nodes cannot provide physical-session proof. Session fields are appended to the internal connection encoding; mixed old/new behavior is not a supported recovery guarantee. No public client packet change is required.

The separate forwarding-recovery PR retains/reroutes events; this PR reconstructs connection authority. Both are needed to cover the original failure chain. The architecture borrows the separation of physical connections and recoverable logical state from upstream main `111bdee669e402f2c6dd0c285fbea315ea77007d`, adapted to this fork. Large-cluster recovery throughput has not been benchmarked.

## Verification

Repeated race-enabled tests cover recovery of an unchanged socket, close during an in-flight snapshot, missing owners, forged identity, stale authority, socket ID reuse, owner restart, logical-view eviction, physical-registry indexing, and stale close/removal without touching the physical connection manager. Server build and the full Go suite are run separately; full-suite failures are disclosed in the PR and are not a clean test result. There is no claim that the five-round WebSocket/failover experiment has been rerun.
