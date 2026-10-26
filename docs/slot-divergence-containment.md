# Slot divergence containment

A slot Raft replica *diverges* when its applied state no longer matches its own log. Before this change, a follower asked to truncate below its applied index skipped the truncation but reported success. Later appends then overwrote applied entries, and the replica kept serving reads, voting and (if elected) acknowledging writes.

Containment does two things:

- it stops new silent divergence from being created;
- it fences a replica as soon as divergence is detected.

It does **not** repair state that has already diverged, and it does not make any replica provably healthy.

## Detection

| Signal | Where | Result |
|---|---|---|
| Truncation below the applied index | `TruncateLogTo` returns `ErrTruncateBelowApplied`. Nothing is removed and the newer term start indexes are kept | quarantine |
| Conflicting append at or below the applied index | `AppendLogs` compares `(Term, Id, Data)`. An equal replay is skipped without a write. Any other value rejects the whole batch before writing (`ErrAppendBelowApplied`), including the term start index, which is written in the same batch | quarantine |
| `applied > last` at open | `GetState` reports it and no longer rewinds the applied index | opens quarantined |
| Durable marker present at open | `GetState` | opens quarantined |
| Same-term replacement above the applied index | `AppendLogs` | counted only (`slot_same_term_conflict_total`) |

Timeouts, stalls, transport errors, storage read errors and apply errors **never** quarantine a replica. They keep their existing errors and HTTP statuses.

Divergence that left `applied == last` and no pending truncation cannot be detected. "Not quarantined" does not mean "clean".

## What quarantine does

Quarantine applies to **one slot replica on one node**. The process and every other slot keep running.

- **In-memory fence first.** The fence goes up before the durable marker is written: a synced single-key write in the slot log storage, key header `0x06 0x06`. Every read check that starts after the fence is refused.
- **Raft.** The replica never proposes, leads, votes, replicates, stores, applies, syncs, or certifies a read again for the life of the process. A leader steps down, and its pending proposals are settled by the proposal fence. A configuration change that names it leader is refused.
- **Proposals** for the slot that enter this node fail before admission with `ErrSlotDiverged`. They are reported as not admitted.
- **Reads.** Every read of a slot-owned row on this node fails with `ErrSlotDiverged` and is discarded. The error never equals or wraps a not-found error. The read is checked before and after (check, read, re-check). The table below lists the owner slot of each row; the read and write paths take it from the same helper (`pkg/cluster/store/slot_owner.go`).

  | Rows | Owner slot |
  |---|---|
  | subscribers, channel info, denylist, allowlist, message events, channel cluster config | the slot of the channel id |
  | users, devices | the slot of the uid |
  | conversations | the uid slot **and**, for a non-person row, the channel slot. A list read fails as a whole if any row's slot is fenced |
  | system UIDs, testers, plugin users | slot 0 |

- **Slot election.** The replica reports `diverged=1` in the slot election extension and is dropped from candidacy and quorum. A fail-stopped replica does not answer at all.
- **Channel election.** The node returns an error for channel election info of channels in the slot, so it does not take part.

## Costs (by design)

These are deliberate and fail closed. None of them is a false success.

- **Quarantined slot leader.**
  - Every channel-leader lookup (`LeaderOfChannel`, `LeaderIdOfChannel`, `LeaderOfChannelForRead`) for channels in that slot fails **cluster-wide** until a new slot leader is elected. Each call loads the channel config from the slot leader.
  - Affected paths: message send and sync routing, CMD subscriber distribution, plugin routes, and new channel raft starts for that slot.
  - Channel rafts already running keep serving with the config they loaded.
  - The slot has no writer. Nothing elects a new leader automatically; see P2.
- **Quarantined follower.** The slot loses one replica, and that node takes no part in channel elections for the slot. Channel failover then needs a quorum of the other replicas and fails closed without one.
- **Slot 0.** On the affected node, system-UID, tester and plugin-user reads fail for every user and channel.
- **Leader role switches are disabled for slot Raft.** Leadership moves only by election, so leadership is not rebalanced automatically. A slot migration whose source is the current leader no longer completes by hand-off and stays pending while its target keeps catching up. Completing it needs an operator election (P2), and a learner target cannot be elected directly.
- **Rolling upgrade is unsupported.** With strict parsing, the default, a node that predates the extension counts as unknown in slot elections, so slots that depend on it cannot elect. Upgrade the whole tier.

## Metrics

| Metric | Meaning |
|---|---|
| `slot_diverged{slot,node}` | 1 = quarantined, 2 = fail-stopped (gauge; healthy slots are not reported) |
| `slot_truncate_refused_total{node}` | truncations refused below the applied index |
| `slot_append_overlap_conflict_total{node}` | appends refused at or below the applied index |
| `slot_same_term_conflict_total{node}` | same-term replacements above the applied index (observation only) |
| `slot_diverged_persist_failed{node}` | markers that could not be written (fail-stop) |
| `slot_loginfo_ext_invalid_total` | malformed slot election extensions received |

Every transition logs `slot replica fenced` (slot server) and `replica quarantined` (Raft node). The log includes the slot, node, cause, applied index and last index.

## Runbook

**P1. Identify.** Alert on `slot_diverged > 0` or `slot_diverged_persist_failed > 0`. Note the slot, the node, and whether that node is the slot leader in the cluster config (`GET /cluster/allslot`).

**P2. Quarantined slot leader.**
- The existing admin call `POST /cluster/slots/:id/election` sets the slot to Candidate. The election then excludes the quarantined replica and elects from the healthy ones with a higher term.
- This is an operator decision; the change does not automate it. If no healthy quorum exists, no leader is elected. Requests keep failing; they do not hang.
- Forcing a leader or accepting data loss is **not** a supported step.

**P3. Fail-stopped replica (marker not persisted).**
- **Do not restart the node** until storage is healthy and the incident is handled.
- After a restart the replica comes back uncontained, because there is no marker. It stays that way until its next refused truncation (exposure A4).
- Check disk space and storage errors first.

**P4. Never clear a marker by hand, and never downgrade with a quarantined replica.** Older binaries ignore the marker. Resolve quarantined replicas before any downgrade. Rebuilding or wiping a replica needs a separate, approved procedure; none is provided here.

**Upgrade gate.**
- Stop every node.
- On each node, run `wk slot-scan --config <file>`, or pass `--dataDir` and `--slotCount`.
- The scan is read-only and fails on a running node. It prints every slot replica that would open quarantined and exits non-zero if there is any.
- Do not upgrade while any slot would open quarantined on a quorum of its replicas.
- A clean scan does not prove a replica is healthy (see Detection).

**Strict extension override.** `cluster.slotDivergeStrictExtension: false` makes slot elections accept responders without the extension. It is for emergencies in a mixed-version cluster only, and it gives up the guarantee that a diverged replica is excluded.

## Unfenced reads (by design)

These are allow-listed and enforced by `TestRawDBReadsAreFenced`. They make no completeness or authority claim and must not feed correctness decisions.

- **Admin scans across all slots:**
  - `SearchChannels`, `SearchConversation`, `SearchUser`, `SearchDevice`, `SearchChannelClusterConfig`
  - global counters (`GetTotal*`)
- **Reconciler key discovery:** `GetChannelClusterConfigs`. Every discovered key is reloaded through the fenced loader before it is acted on.
- **Channel Raft state, not slot-owned:**
  - messages, channel last message sequence, channel applied index, term start indexes, Raft hard state
  - conversation local users
- **Node-local tables:**
  - the plugin table (`Store.DB()`, plugin server only)
  - the notify queue

## Not addressed

- Already-divergent retained data, including channel configs loaded before detection.
- The trigger that let a stale leader commit during an election.
- Stale reads from a replaced but unaware healthy leader.
- Restart exposure after a failed marker write (A4).
- Ambiguous (timed-out) send outcomes.
