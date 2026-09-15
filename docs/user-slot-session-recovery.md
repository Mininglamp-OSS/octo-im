# Recovering physical sessions after user-slot movement

A user-slot leader could change while an authenticated WebSocket stayed open on another node. The new leader had no logical connection entry, so a committed message could miss the recipient's live socket. This change gives physical socket owners a registry independent of user-slot ownership, and lets the new leader reconstruct its logical view.

## Contract

- Each physical owner has a random boot identity; each prepared socket has a random session identity. Only successful authentication of that exact live socket publishes an authenticated snapshot. A serialized `Auth` flag alone is not accepted as evidence.
- `/wk/presence/snapshot/v1` returns authenticated session identity and routing metadata for at most 128 UIDs. AES keys and IVs never leave the physical socket owner through this route. A recovered descriptor without crypto is finalized from the live local socket context immediately before an encrypted client write.
- Recovery reads current online owners with at most four concurrent requests and checks cluster version, UID authority and snapshot generation again before publishing. Successful owner replies may restore known-live sessions during a partial failure, but only a complete read may evict stale sessions or prove a UID offline.
- Recovery has a three-second budget, up to three attempts and an eight-slot node-local concurrency bound. Warm UIDs bypass that bound. Positive readiness is cached for five seconds, complete negative snapshots for one second, and an evicted logical view invalidates the positive fast path. The 32,768-UID ready cache evicts expired and least-recently-used entries instead of clearing globally.
- Owners periodically touch each UID's current leader. Touches carry only UIDs; the receiver pulls fresh snapshots, so replayed touches cannot authenticate a dead socket. Snapshot requests never acquire the recovery gate.
- Close invalidates an in-flight snapshot generation. Connection lookup/removal and physical writes compare owner boot and session identities. Logical removal no longer deletes the physical connection manager's entry by numeric ID; only the socket close path owns that cleanup.
- Distribution attempts recovery before deciding whether a recipient is offline. If recovery is incomplete, it still delivers to every session already known online and withholds only offline classification for UIDs whose presence is unknown. There is no durable delivery outbox: live delivery remains best effort and history reconciliation is required after interruption.

## Compatibility and scope

Based on `fix/raft-orphan-learner-bugs` at `4ce16cac8153a8cc49cdf2a200e13dcf455e9d71`, with PR43/46/47 already merged. Session fields are appended to the internal connection encoding. During a rolling upgrade, identity-less descriptors are accepted only when they match the physical socket owner's already-prepared local session; a complete but mismatched identity is rejected with an authentication-failure CONNACK. Older nodes cannot serve recovery snapshots, so failover recovery becomes complete only after all IM nodes are upgraded. No public client packet change is required.

The cluster transport does not expose an authenticated caller identity to route handlers. Both presence routes therefore rely on the same private, trusted cluster-network boundary as the other internal routes. Deployments must prevent untrusted clients from reaching the cluster port. Request bodies are capped at 64 KiB before JSON decoding, and snapshots deliberately exclude client session crypto even inside that boundary.

The separate forwarding-recovery PR retains/reroutes events; this PR reconstructs connection authority. Both are needed to cover the original failure chain. The architecture borrows the separation of physical connections and recoverable logical state from upstream main `111bdee669e402f2c6dd0c285fbea315ea77007d`, adapted to this fork. Large-cluster recovery throughput has not been benchmarked.

## Verification

Repeated race-enabled tests cover recovery of an unchanged socket, close during an in-flight snapshot, missing owners, forged identity, stale authority, socket ID reuse, owner restart, logical-view eviction, physical-registry indexing, and stale close/removal without touching the physical connection manager. Server build and the full Go suite are run separately; full-suite failures are disclosed in the PR and are not a clean test result. There is no claim that the five-round WebSocket/failover experiment has been rerun.
