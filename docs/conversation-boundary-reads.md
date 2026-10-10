# Conversation boundary reads on the v2.2.5 branch

`clearUnread`, `setUnread`, and `delete` update conversations at the UID slot
leader. Their message boundary is read separately from the channel leader.

The metadata read captures a slot committed index and waits, within the request
budget, until that fixed index has been applied. Later unrelated writes do not
invalidate the read. Leadership changes still fail the read, and the caller
and serving node recheck the channel configuration around the boundary read.

For an active channel, the returned sequence is bounded by the committed index
in the post-read Raft snapshot. A message appended to disk but still waiting for
replication acknowledgements cannot advance the conversation boundary beyond
that index. A commit completed during the read may be included.

## Legacy recovery limitation

Active channel recovery now loads the persisted applied marker; it no longer
uses the local message tail as evidence of commitment. Apply maintains that
marker only after Raft confirms a range. Because channel message state was
already written by `AppendLogs`, the marker can advance without replaying the
history payloads. A missing legacy marker starts at zero and replication (or
the single-voter quorum rule) must establish commitment again. A non-empty
active channel whose committed bound is still zero returns a retryable error.

Dormant reads still use the designated owner's durable tail without a runtime
or a commit-marker check. Consequently, a dormant-channel read can return a
crash-residue uncommitted suffix; maintaining the marker for active channels
does not change that legacy dormant-read behavior. This remains a separate
compatibility limitation, not a guarantee of committed dormant history.

Dormant channels remain readable without being created or woken. This preserves
the v2.2.5 behavior and the above residual risk; it is not a claim of the stronger
committed-history guarantee provided by the refactored main branch. Missing
metadata returns an empty boundary without creating a channel. Dormant channels
with either migration marker still set are rejected. A residual learner entry
without migration markers does not block a dormant read: that learner may be
offline or decommissioned, so reading cannot depend on its eventual promotion.

These local role/configuration checks are not a quorum ReadIndex or a leader
lease. They do not guarantee linearizability across network partitions.

## Rolling upgrades and errors

The configuration and boundary RPCs use version 2 paths and responses. Version 1
conversation-read peers, including the earlier experimental implementation that
returned unbounded tails, are rejected. Existing unrelated RPCs are unchanged.
An upgraded caller does not fall back to the older boundary protocol. The full
fix requires upgrading participating nodes; requests served by old UID owners
retain the old behavior until those nodes are upgraded.

Each operation has one request budget and at most one route-refresh retry.
Unresolved readiness, transport, or ownership failures return HTTP 503 with
`{"msg":"retry required","status":503}`. Internal errors retain the channel,
attempt and available peer/slot details for diagnosis; remote serving failures
are also logged at debug level. Internal error text is not returned to API users.
