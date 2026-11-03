package types

import "github.com/WuKongIM/WuKongIM/pkg/wkutil"

// HasExcessReplicas distinguishes an installed add-first set from an ordinary
// leadership transfer. Capacity comes from metadata, never the online count.
func HasExcessReplicas(replicas, learners []uint64, capacity uint32) bool {
	return capacity > 0 && len(learners) == 0 && len(replicas) > int(capacity)
}

// PlanReplicaRetirement repairs the endpoints of an installed excess set after
// election, or reconstructs a task lost by an older completion. It only plans:
// a follower's durable-prefix and installed-majority acknowledgements must still
// authorize removal. The new leader is never removed or asked to sync to itself.
func PlanReplicaRetirement(replicas, learners []uint64, capacity uint32, leader, from, to uint64, online map[uint64]bool) (uint64, uint64, bool) {
	if !HasExcessReplicas(replicas, learners, capacity) || !online[leader] || !wkutil.ArrayContainsUint64(replicas, leader) {
		return 0, 0, false
	}
	if (from != 0 || to != 0) && (from == to || !wkutil.ArrayContainsUint64(replicas, from) || !wkutil.ArrayContainsUint64(replicas, to)) {
		return 0, 0, false
	}
	live := 0
	for _, id := range replicas {
		if online[id] {
			live++
		}
	}
	if live < len(replicas)/2+1 {
		return 0, 0, false
	}
	retire := from
	if retire == leader {
		retire = 0
	}
	// Prefer an unavailable voter over retiring a healthy recovered source.
	for _, id := range replicas {
		if id != leader && !online[id] {
			retire = id
			break
		}
	}
	if retire == 0 {
		for _, id := range replicas {
			if id != leader && id != to {
				retire = id
				break
			}
		}
	}
	confirm := to
	if confirm == 0 || confirm == leader || confirm == retire || !online[confirm] {
		confirm = 0
		for _, id := range replicas {
			if id != leader && id != retire && online[id] {
				confirm = id
				break
			}
		}
	}
	return retire, confirm, retire != 0 && confirm != 0 && (retire != from || confirm != to)
}
