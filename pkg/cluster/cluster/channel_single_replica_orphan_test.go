package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestSingleReplicaOrphanMarkerCleanup(t *testing.T) {
	for _, target := range []uint64{2, 3, 99} {
		cfg := orphanMigrationConfig(target)
		cfg.ReplicaMaxCount = 1
		before := cfg.Clone()
		changed, err := (&Server{}).joinNewRepliceIfNeed(&cfg)
		require.NoError(t, err)
		require.True(t, changed)
		before.MigrateFrom, before.MigrateTo = 0, 0
		require.Equal(t, before, cfg, "cleanup must not change membership, term, leader or target capacity")
		changed, err = (&Server{}).joinNewRepliceIfNeed(&cfg)
		require.NoError(t, err)
		require.False(t, changed, "a clean config must not create repeated writes")
	}
}

func TestSingleReplicaOrphanMarkerPreservesTransitions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		from, to uint64
		learners []uint64
	}{
		{"clean", 0, 0, nil},
		{"source_only", 2, 0, nil},
		{"target_only", 0, 2, nil},
		{"transfer", 1, 2, nil},
		{"target_is_replica", 1, 1, nil},
		{"real_learner", 2, 2, []uint64{2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := wkdb.ChannelClusterConfig{ReplicaMaxCount: 1, Replicas: []uint64{1}, LeaderId: 1,
				MigrateFrom: tc.from, MigrateTo: tc.to, Learners: tc.learners}
			before := cfg.Clone()
			changed, err := (&Server{}).joinNewRepliceIfNeed(&cfg)
			require.NoError(t, err)
			require.False(t, changed)
			require.Equal(t, before, cfg)
		})
	}
}

func TestSingleReplicaOrphanMarkerPersistsThroughOwnerFence(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 1, 1)
	cfg := orphanMigrationConfig(99)
	cfg.ReplicaMaxCount = 1
	owner, stored := saveOrphanMigrationConfig(t, ctx, servers, cfg)
	repaired, err := owner.maintainChannelConfig(ctx, stored)
	require.NoError(t, err)
	require.Zero(t, repaired.MigrateFrom)
	require.Zero(t, repaired.MigrateTo)
	require.Greater(t, repaired.ConfVersion, stored.ConfVersion)
	current, err := owner.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.True(t, repaired.Equal(current), "persisted channel configuration must match the repaired state")
	require.Equal(t, repaired.ConfVersion, current.ConfVersion)
	require.Equal(t, []uint64{1}, current.Replicas)
	require.Equal(t, uint16(1), current.ReplicaMaxCount)
	_, err = owner.saveChannelConfig(ctx, stored)
	require.ErrorIs(t, err, raft.ErrConfigVersionStale, "a stale snapshot must not restore the orphan marker")
}
