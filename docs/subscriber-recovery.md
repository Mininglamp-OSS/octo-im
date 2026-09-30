# Subscriber recovery durability

`subscriberRecovery.asyncTarget.enabled` is disabled by default. When disabled,
subscriber mutation endpoints keep their synchronous completion behavior. When
enabled, the endpoint and response body are unchanged, but a successful 2xx
means that source membership, revision, idempotency receipt, complete recovery
work, and latest intent are durable and that routing/tag invalidation has
completed. Target conversation projection may still be draining. Its progress
is available from the existing operation-status endpoint.

Turning the flag off affects new requests only. Work already accepted under the
asynchronous boundary continues to drain from durable source state.

## Physical-shard group-commit decision

A recovery-only coordinator was prototyped and tested against direct concurrent
`Commit(Sync)` with Pebble's existing 20ms WAL sync coalescing. The coordinator
was removed because it reduced throughput under realistic eight-shard fan-out,
even after allowing following cohorts to accumulate while Sync commits were in
flight. The production path therefore remains direct `Commit(Sync)` and no
inert group-commit configuration key is exposed.

Reproduction command used during the decision:

```sh
go test ./pkg/wkdb -run '^$' -bench BenchmarkRecoveryCommit -benchtime=2s -count=3
```

On Apple M4/darwin-arm64 with 160 parallel workers, eight physical shards and
eight 200-byte mutations per fragment, direct Sync measured 169–199 us/op while
the pipelined coordinator measured 206–251 us/op. The earlier single-owner
coordinator was slower still. This is a local microbenchmark rather than a
production claim; it is sufficient to reject shipping the regressive optional
path. Source, target projection, checkpoint and Raft durability remain Sync.
