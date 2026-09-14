# Conversation synchronization during leader changes

Recent-message synchronization resolves channel routes using authoritative
metadata reads. It has one deadline and at most two attempts; every retry
reloads all routes and reserves time after a slow first peer. Local serving
checks the channel leader/configuration before and after reading messages.

A successful batch must explicitly cover every requested channel, including
valid empty channels. Missing, duplicate, wrong-channel, malformed and legacy
peer responses fail the whole request. API callers receive a sanitized HTTP
503 and can retry; route errors no longer become a successful omitted group.
The existing public response schema is unchanged.

Internal peers use `/conversation/syncMessages/v2`, with a versioned envelope
and remaining deadline budget. All nodes must support it for cross-node sync;
there is no fallback to the old endpoint that could hide partial results. The
old leaf endpoint remains available with local role validation. These checks
do not introduce a quorum ReadIndex or promise linearizable message history.

Tests cover route/leader changes, slow peers, cancellation, incomplete batches,
legacy peers, unused channels and HTTP error propagation. The existing cluster
integration suite still has unrelated baseline races; targeted API race tests
are reported separately from the full repository test result.
