package cluster

import (
	"slices"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

func validSlotConfigTransition(current *types.Slot, next rafttype.Config, capacity uint32) bool {
	if types.HasExcessReplicas(current.Replicas, current.Learners, capacity) && current.MigrateFrom != 0 &&
		(current.MigrateFrom == current.Leader || current.MigrateTo == current.Leader) {
		return false // maintenance must retarget retirement before any completion
	}
	expected := current.Clone()
	if expected.MigrateTo != 0 && expected.MigrateFrom != 0 {
		if wkutil.ArrayContainsUint64(expected.Learners, expected.MigrateTo) {
			expected.Learners = wkutil.RemoveUint64(expected.Learners, expected.MigrateTo)
			if !wkutil.ArrayContainsUint64(expected.Replicas, expected.MigrateTo) {
				expected.Replicas = append(expected.Replicas, expected.MigrateTo)
			}
			if expected.MigrateFrom != expected.MigrateTo {
				expected.Replicas = wkutil.RemoveUint64(expected.Replicas, expected.MigrateFrom)
			}
		} else if expected.MigrateFrom == expected.Leader && wkutil.ArrayContainsUint64(expected.Replicas, expected.MigrateTo) {
			expected.Term++
		} else if wkutil.ArrayContainsUint64(expected.Replicas, expected.MigrateFrom) && wkutil.ArrayContainsUint64(expected.Replicas, expected.MigrateTo) {
			expected.Replicas = wkutil.RemoveUint64(expected.Replicas, expected.MigrateFrom)
		} else {
			return false
		}
		if expected.MigrateFrom == expected.Leader {
			expected.Leader = expected.MigrateTo
		}
		expected.MigrateFrom, expected.MigrateTo = 0, 0
	} else if expected.MigrateFrom == 0 && expected.MigrateTo == 0 {
		// Existing orphan learners can complete after their predecessor did.
		for _, id := range current.Learners {
			if !wkutil.ArrayContainsUint64(next.Learners, id) && wkutil.ArrayContainsUint64(next.Replicas, id) {
				expected.Learners = wkutil.RemoveUint64(expected.Learners, id)
				if !wkutil.ArrayContainsUint64(expected.Replicas, id) {
					expected.Replicas = append(expected.Replicas, id)
				}
				break
			}
		}
	}
	same := func(a, b []uint64) bool {
		a, b = slices.Clone(a), slices.Clone(b)
		slices.Sort(a)
		slices.Sort(b)
		return slices.Equal(a, b)
	}
	return next.Term == expected.Term && next.Leader == expected.Leader &&
		next.MigrateFrom == expected.MigrateFrom && next.MigrateTo == expected.MigrateTo &&
		same(next.Replicas, expected.Replicas) && same(next.Learners, expected.Learners)
}
