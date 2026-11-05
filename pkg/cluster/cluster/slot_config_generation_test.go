package cluster

import (
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
			completion := rafttype.Config{Leader: 4, Term: 7, Replicas: []uint64{4}, Version: tt.observed}
			require.Equal(t, tt.valid, validSlotConfigTransition(current, completion, 1))
		})
	}
}
