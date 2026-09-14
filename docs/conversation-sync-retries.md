# Conversation synchronization during leader changes

Public `/conversation/syncMessages` resolves authoritative channel routes and
fans out to the actual leaders, returning the original plain-array schema.
A valid never-created channel returns an empty entry. Repeated channel inputs
are coalesced; empty channel IDs or channel type zero return HTTP 400. The same
normalization is used by sync/syncByChannels/message-sync orchestration; duplicate
cursors select the smallest lower bound in both directions (zero is unbounded).
Ordering is applied after the DB read; it does not turn the cursor into an upper
bound. Server-built conversation read/deleted barriers remain applied before
these requests are constructed.

Routing and each pre/post-read fence use at most 16 concurrent workers, and peer
fan-out is also bounded to 16. One end-to-end deadline covers routing, network
calls and paced retries. Its budget is `cluster.reqTimeout * ceil(channels/64)`
after deduplication (at least one unit), so unpaged histories are not forced into
the same budget as one channel. A shorter caller deadline always wins; the
internal peer's budget is also clamped to this size-based server allowance.
Retries use a 25 ms initial delay, exponential backoff capped at 250 ms plus up to 25% jitter, at
most 64 attempts. Each attempt may use the full remaining budget. Only early
failures retry; a timeout/cancellation before completion ends the request.
Once every worker and both fences have returned success and coverage validates,
a deadline racing with final assembly does not discard that complete result.
Malformed/wrong-version/incomplete peer responses and fixed HTTP 4xx failures
(except 429) fail immediately; elections, transport errors and 5xx retain paced
retry. Every retry resolves routes; a channel observed to exist cannot disappear into an empty result on retry.
Both routing and serving require a normal configuration with a voting leader.
Local serving fences leader/configuration state before and after payload reads,
without performing extra discarded message-sequence reads.

Success must cover every requested channel. Missing, duplicate, wrong-channel,
malformed peer responses fail the whole request. Storage batch results
are checked before they can be converted to empty entries. Retries exhausted at
the deadline return sanitized HTTP 503 (`retry required`); causes are logged on
the server. This preserves complete-result semantics rather than returning a
success with unread channels omitted. Clients should retry temporary 503s within
their own budget. Octo's separate proxy must preserve this status to make that
contract useful end to end. Its current 503-to-400 mapping is an outstanding
acceptance item in [issue #45](https://github.com/Mininglamp-OSS/octo-im/issues/45);
this PR does not claim that Octo clients already receive retryable 503s.

Peers use `/conversation/syncMessages/v2`, an endpoint that only serves locally
led channels, with `{version: 2, channels: [...]}` responses and a positive `budget_ms` integer in
requests. The older nanosecond `budget` field is still accepted during transition;
`budget_ms` takes precedence and is capped by the size-based server budget. v2
rejects missing metadata because its caller already resolved an existing route.
Both routes are registered on the existing public API listener; “peer” describes
intended use, not authentication or network isolation. Public v1 remains routable.
Peer requests carry `peer_read: true`: if v2 is hidden by an intermediary, an
upgraded v1 handler serves that fallback locally rather than recursively routing
it. Positive `budget_ms`/legacy `budget` values are honored on v1 too. Old peers
ignore the new marker and already serve v1 locally.

During a rolling upgrade, only HTTP 404 from the v2 route permits one fallback
request to the same peer's legacy `/conversation/syncMessages` route, under the
same remaining deadline. Its plain array must cover exactly the requested
channels; missing, duplicate or unexpected channels fail closed. A v2 error,
malformed payload or wrong version never enables fallback. Legacy serving lacks
the new local role/config fences, so that stronger guarantee is available after
all peers are upgraded; complete channel coverage remains mandatory throughout.
If PR46 is deployed too, its proposal RPC still requires the coordinated upgrade
procedure in the message retry runbook.

The existing DB interface cannot interrupt an already-running local read.
Context is checked between DB operations and no detached timeout workers are
created, so network/routing work respects the deadline while a stalled disk can
extend one in-flight local operation. These fences do not introduce quorum
ReadIndex or promise linearizable message history.
