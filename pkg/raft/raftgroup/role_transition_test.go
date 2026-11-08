package raftgroup

import (
	"math"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestLearnerPromotionUsesNewTermOnlyWhenChangingLeader(t *testing.T) {
	for _, source := range []uint64{1, 2} {
		n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithNodeId(1)))
		cfg := types.Config{Term: 7, Version: 9, Leader: 1, Role: types.RoleLeader, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, MigrateFrom: source, MigrateTo: 4}
		require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
		rg := New(NewOptions())
		next, err := rg.learnTo(n, 4)
		require.NoError(t, err)
		if source == 1 {
			require.Equal(t, uint32(8), next.Term)
			require.Equal(t, uint64(4), next.Leader)
		} else {
			require.Equal(t, uint32(7), next.Term)
			require.Equal(t, uint64(1), next.Leader)
		}
		require.Equal(t, uint64(9), next.Version, "authority still CASes the original version")
		require.NotContains(t, next.Replicas, source)
		require.Contains(t, next.Replicas, uint64(4))
		require.Empty(t, next.Learners)
		require.Equal(t, cfg, n.Config(), "transition must not mutate live owner state")
	}
}

func TestLeadershipPromotionRejectsTermOverflow(t *testing.T) {
	n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithNodeId(1)))
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{Term: math.MaxUint32, Version: 9, Leader: 1, Role: types.RoleLeader, Replicas: []uint64{1, 2}, Learners: []uint64{4}, MigrateFrom: 1, MigrateTo: 4}}))
	rg := New(NewOptions())
	_, err := rg.learnTo(n, 4)
	require.Error(t, err)
	_, err = rg.followerToLeader(n, 2)
	require.Error(t, err)
}
