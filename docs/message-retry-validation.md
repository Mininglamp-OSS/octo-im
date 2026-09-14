# PR46 review validation and failure attribution

The revised write path moves both retry resolution and canonical verification
outside the shared Raft loop. Admission revalidates the snapshot's instance,
term, config and log generation; ACKs still require commitment and durable apply.
A slow lookup or final verification does not delay another channel's owner read.
Tests also change leadership while each disk read is blocked and confirm no
stale success ACK. Concurrent store completion reuses the committed identity.

A 20,000-message legacy-channel regression now asserts that applying the
Raft-confirmed prefix performs no history payload reads and at most two durable
marker updates (recovery plus the new send). Generic state machines retain
bounded payload apply batches. Sender/client-number lookup intersects both
existing indexes in one snapshot, so the 1,100-sender same-number fixture can
find its last sender or confirm a new sender is absent without a candidate cap.
The caller deadline bounds pathological stale-index scans. The applied prefix
cannot be truncated. Both PR42's HardState and PR46's applied-index restore are
retained after the base merge.

Configuration installation intentionally retains two ordered owner operations.
The intervening Ready persists a newly selected term before replication resumes.
Collapsing those steps made a real conversation/slot-write integration test
observe committed=0 immediately after wake; the final revision retains the
barrier and that integration passes. No disk I/O was added to owner callbacks.

## Historical round 3

The recorded five-round run used combined commit
`08e2bc2e45e4a473de6231af26c76d495a5b87e8`. Two sends missed the original three-second
ACK window. The SDK emitted the same client numbers again on reconnect roughly
26–31 seconds later. The first logical message retained sequence 28 and the
second was later committed at sequence 38. Final participant-specific history
checks found all 36 logical messages exactly once. A separate 21-frame replay
returned one canonical ID/sequence in all 21 ACKs.

The same outage/reconnect-and-retry mechanism was already observed on feature
base `11874978e4f88c266b597631f4cc6ed959ce5f68` and on that base plus PR41; those
versions persisted retries twice. PR46 removes duplicate persistence, not the
network outage or a promise to ACK within three seconds. The history ordering
of a message first committed after reconnect is distinct from duplicating an
already committed message. The separate sync failure was an explicit IM 503
mapped to Octo 400; PR47 now paces retries but does not change that proxy.

## Historical round 4

All 34 messages received success ACKs and were persisted once, in order. There
were 13 failed realtime assertions. Reinspection of the actual run's metadata
and WebSocket recordings shows:

| Recipient | User slot | Original leader | New leader first observed (UTC) | First missing traffic |
| --- | --- | --- | --- | --- |
| user02 | 6 | 1001 | 1004, 03:36:12.575 | JOIN-NODE4 DM08, started 03:36:12.104 |
| user03 | 15 | 1003 | 1005, 03:36:26.435 | JOIN-NODE5 group01, started 03:36:29.046 |

The first user02 failure straddles the sampled migration boundary. Both users
continued normal PING/PONG exchanges at 03:37:05 and 03:38:05. Their original
connections stayed up until the explicit reconnect at 03:38:44; reconnect
completed at 03:38:47 and subsequent delivery probes passed.

The connection-registration defect is independently reproduced using the real
user EventPool, forward-event codec and PING handler on current base `c90c5dc`
and on the revised 41+42+43+46+47 combination: the old user leader has one
authenticated connection; after migration the new leader receives authenticated
PINGs but still has zero; explicit registration restores one. The relevant
user handler/event, tag and distribution code is byte-identical to `1187497`.
This diagnostic intentionally FAILS its desired-availability assertion on both
versions and is not counted as a passing release test.

These facts isolate a pre-existing mechanism consistent with both runs; the
original R4 run lacks in-memory connection-table snapshots. They do not prove
that every historical miss has one exclusive cause, do not make R4 pass, and do
not close the end-to-end delivery acceptance gate. Connection ownership during
user-slot migration remains a separate repair, and a new instrumented R4 run is
still needed before claiming that acceptance gate is green.

Raw HTTP/WebSocket/container evidence remains in restricted local artifacts,
not in this repository. Source manifests, retry frame timestamps, participant
history checks, slot changes, heartbeat bytes and diagnostic logs are indexed
in the local `tests/artifacts/open-pr-review-20260914/REPAIRS.md` report.
