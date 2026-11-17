package cluster

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// validSlotConfigTransition fences slot role-change callbacks (Learner->Follower,
// Learner->Leader, Follower->Leader) against stale terms, candidate elections,
// and superseded migration targets.
func validSlotConfigTransition(current *types.Slot, next rafttype.Config) bool {
	if current == nil {
		return false
	}
	if current.Status == types.SlotStatus_SlotStatusCandidate {
		return false
	}
	if current.Term > 0 && next.Term < current.Term {
		return false
	}
	if next.Leader == 0 || !wkutil.ArrayContainsUint64(next.Replicas, next.Leader) || wkutil.ArrayContainsUint64(next.Learners, next.Leader) {
		return false
	}
	for _, learnerID := range next.Learners {
		if wkutil.ArrayContainsUint64(next.Replicas, learnerID) {
			return false
		}
	}
	if current.Term > 0 && next.Term == current.Term && current.Leader != 0 && next.Leader != current.Leader {
		if current.MigrateFrom != current.Leader || current.MigrateTo != next.Leader || next.MigrateFrom != 0 || next.MigrateTo != 0 {
			return false
		}
	}
	if current.MigrateTo != 0 && next.MigrateFrom == 0 && next.MigrateTo == 0 {
		if !wkutil.ArrayContainsUint64(next.Replicas, current.MigrateTo) || wkutil.ArrayContainsUint64(next.Learners, current.MigrateTo) {
			return false
		}
	}
	return true
}
