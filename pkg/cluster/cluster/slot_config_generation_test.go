package cluster

import (
	"math"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestSlotCompletionRequiresOriginatingAssignmentGeneration(t *testing.T) {
	for _, tt := range []struct {
		name       string
		generation uint64
		observed   uint64
		valid      bool
	}{
		{"before_cancel_and_restart", 10, 8, false},
		{"unversioned_request_after_change", 10, 0, false},
		{"current_assignment", 10, 10, true},
		{"observed_after_unrelated_controller_update", 10, 12, true},
		{"legacy_assignment", 0, 0, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			current := &types.Slot{Id: 1, Leader: 1, Term: 7, Replicas: []uint64{1}, Learners: []uint64{4},
				MigrateFrom: 1, MigrateTo: 4, Generation: tt.generation}
			completion := rafttype.Config{Leader: 4, Term: 8, Replicas: []uint64{4}, Version: tt.observed}
			require.Equal(t, tt.valid, validSlotConfigTransition(current, completion, 1))
		})
	}
}

func TestSlotCompletionRequiresExactLeaderTerm(t *testing.T) {
	for _, tt := range []struct {
		name           string
		from           uint64
		term, nextTerm uint32
		valid          bool
	}{
		{"leader must advance term", 1, 7, 7, false},
		{"leader advances exactly once", 1, 7, 8, true},
		{"leader cannot skip terms", 1, 7, 9, false},
		{"leader term cannot wrap", 1, math.MaxUint32, 0, false},
		{"follower keeps term", 2, 7, 7, true},
		{"follower cannot advance term", 2, 7, 8, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			replicas := []uint64{1}
			nextReplicas := []uint64{4}
			leader := uint64(4)
			if tt.from == 2 {
				replicas = []uint64{1, 2}
				nextReplicas = []uint64{1, 4}
				leader = 1
			}
			current := &types.Slot{Id: 1, Leader: 1, Term: tt.term, Replicas: replicas, Learners: []uint64{4}, MigrateFrom: tt.from, MigrateTo: 4, Generation: 10}
			completion := rafttype.Config{Leader: leader, Term: tt.nextTerm, Replicas: nextReplicas, Version: 10}
			require.Equal(t, tt.valid, validSlotConfigTransition(current, completion, uint32(len(replicas))))
		})
	}
}
