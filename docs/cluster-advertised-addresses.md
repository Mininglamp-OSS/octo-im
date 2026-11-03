# Cluster listener and advertised addresses

`cluster.addr` is the local listening endpoint. It may bind all interfaces, for
example `tcp://0.0.0.0:11110`.

`cluster.serverAddr` is the address sent in join requests and stored in replicated
membership as `Node.ClusterAddr`. It must name the interface or host other nodes
can reach, not an unspecified address. These settings are already separate; a
wildcard listener does not require advertising the wildcard address.

```yaml
cluster:
  nodeId: 1002
  addr: "tcp://0.0.0.0:11110"
  serverAddr: "10.0.0.12:11110"
  seed: "1001@10.0.0.11:11110"
```

Use each node's own reachable endpoint in `cluster.initNodes` too. Loopback
addresses are suitable only when the nodes share a host/network namespace.

## Validation and compatibility

Advertised addresses must be `host:port`, optionally prefixed by `tcp://`, with a
decimal TCP port in `1..65535` and no whitespace. IPv6 literals use brackets.
Unspecified IP addresses are rejected, including scoped forms such as
`[::%eth0]:11110` and IPv4-mapped unspecified addresses. Valid IPv6 scope zones
are preserved, but the operator remains responsible for interface names and
reachability on every peer. Validation does not perform DNS or connectivity
checks.

A standalone node may bootstrap with an empty advertised address. After an
operator supplies `cluster.serverAddr` and restarts, reconciliation proposes a
replicated update only if the persisted local address is missing. It does not
replace an existing nonempty record. The optional empty bootstrap value is not
valid for a joining node.

Legacy deployments whose `initNodes` already contain valid endpoints may retain
an unused bind-all `serverAddr` without an unconditional options-parsing failure.
A joining deployment that previously supplied only a wildcard `serverAddr` must
set it to a reachable address; it may keep the wildcard `cluster.addr`. This is
an intentional admission restriction, not automatic address discovery or a
promise of compatibility with invalid historical membership.

Upgrade all members before relying on the new replicated address-backfill
command. Repair of already-persisted invalid nonempty addresses and migration
between nonempty endpoints are outside the automatic backfill path.

## Read ownership

Node getters and `GetClusterConfig` return snapshots, not mutable handles into
live membership. Retaining a result does not observe a later backfill; reacquire
a snapshot to see the new value. Membership updates must use the proposal/apply
path rather than modifying a returned object. This does not assert that the
entire Raft implementation or the separate slot getters are race-free.
