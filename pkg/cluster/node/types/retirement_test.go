package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanReplicaRetirementAfterElection(t *testing.T) {
	for _, tc := range []struct {
		name             string
		leader, from, to uint64
		capacity         uint32
		offline          []uint64
		learners         []uint64
		wantFrom, wantTo uint64
		changed          bool
	}{
		{"destination-elected", 4, 3, 4, 3, []uint64{1}, nil, 1, 2, true},
		{"source-elected", 3, 3, 4, 3, []uint64{1}, nil, 1, 4, true},
		{"another-follower-elected", 2, 3, 4, 3, []uint64{1}, nil, 1, 4, true},
		{"leader-survives-another-failure", 1, 3, 4, 3, []uint64{2}, nil, 2, 4, true},
		{"lost-task", 4, 0, 0, 3, []uint64{1}, nil, 1, 2, true},
		{"lost-task-all-healthy", 4, 0, 0, 3, nil, nil, 1, 2, true},
		{"already-retargeted", 4, 1, 2, 3, []uint64{1}, nil, 0, 0, false},
		{"no-installed-quorum", 4, 3, 4, 3, []uint64{1, 2}, nil, 0, 0, false},
		{"leader-unavailable", 1, 3, 4, 3, []uint64{1}, nil, 0, 0, false},
		{"normal-four-replica-transfer", 3, 3, 4, 4, []uint64{1}, nil, 0, 0, false},
		{"unknown-capacity", 4, 3, 4, 0, []uint64{1}, nil, 0, 0, false},
		{"learner-still-catching-up", 1, 3, 4, 3, nil, []uint64{4}, 0, 0, false},
		{"partial-task", 4, 3, 0, 3, []uint64{1}, nil, 0, 0, false},
		{"source-not-a-voter", 4, 5, 2, 3, []uint64{1}, nil, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			online := map[uint64]bool{1: true, 2: true, 3: true, 4: true}
			for _, id := range tc.offline {
				online[id] = false
			}
			replicas := []uint64{1, 2, 3, 4}
			from, to, changed := PlanReplicaRetirement(replicas, tc.learners, tc.capacity, tc.leader, tc.from, tc.to, online)
			require.Equal(t, tc.changed, changed)
			if changed {
				require.Equal(t, tc.wantFrom, from)
				require.Equal(t, tc.wantTo, to)
			}
			require.Equal(t, []uint64{1, 2, 3, 4}, replicas, "planning cannot remove a vote")
		})
	}
}
