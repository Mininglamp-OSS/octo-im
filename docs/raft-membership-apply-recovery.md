# Membership apply failure recovery

Failed apply responses release the in-flight request without advancing the
applied index. Retry scheduling belongs to the Raft node, on its owner loop:

- The first seven failures delay the next attempt by 1, 2, 4, 8, 16, 32 and 64
  ticks. Incoming messages and repeated Ready calls cannot consume this delay.
- The eighth failure ends the retry burst and emits an ERROR alarm beginning
  `raft apply retry limit reached`. Further recovery probes run at most once
  every 256 ticks (38.4 seconds with clusterconfig's default 150ms tick).
  Each failed probe repeats the alarm, including applied/committed indexes.
- Successful apply resets the failure streak. Elections, term changes and role
  changes do not reset it. Heartbeats, replication and configuration events
  continue while apply is cooling down.

Operators should inspect the accompanying storage/config error, restore disk
space or repair the failed storage service, and watch applied index progress.
The slow probes recover automatically after a transient failure; reaching the
retry limit does not permanently latch apply or terminate healthy channel/slot
work. A malformed committed command is returned as an error and is never skipped
or acknowledged; persistent decode failures require investigation of the log.

Clusterconfig retains separate in-process progress for saving the snapshot,
delivering membership and notifying listeners. A batch retry does not repeat
commands whose version is already incorporated, successful snapshot writes or
completed notifications. Membership is delivered only after saving succeeds;
listeners run only after pending membership delivery succeeds. A later failure
writing the batch's durable applied marker can therefore retry just that marker.

On restart, the saved application snapshot restores the command version and
`initRaft` restores membership. Cluster/slot startup also initializes nodes and
slots from that snapshot, so replay need not repeat listener notifications for
already-saved entries. This does not change the snapshot file format or its
existing persistence mechanism.

Tests cover permanent errors and recovery through the real apply worker/event
loop, tick pacing across role changes, heartbeat progress during cooldown,
closed-file persistence failure, delivery failure after save, completed batch
prefix replay, stale-tolerant membership delivery and malformed commands.
