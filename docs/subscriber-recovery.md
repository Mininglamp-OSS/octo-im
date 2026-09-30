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

`subscriberRecovery.groupCommit.enabled` is also disabled by default. It groups
only the first, synchronous target projection stage by physical WKDB shard. It
does not change source, target, checkpoint, or Raft durability and does not
provide a cross-shard transaction. The defaults and supported ranges are:

| setting | default | range |
| --- | ---: | ---: |
| `window` | `2ms` | `250us`–`5ms` |
| `maxCount` | `32` | `1`–`128` |
| `maxBytes` | `1MiB` | `64KiB`–`4MiB` |
| `oldestAge` | `5ms` | `1ms`–`10ms`, at least `window` |
| `queueHard` | `256` | at least `maxCount` |

Invalid enabled configurations stop database startup. Runtime status reports
queue depth, in-flight commits, physical commit count, fragment count, and
encoded mutation bytes. Disabling group commit restores direct `Commit(Sync)`
for newly applied entries.
