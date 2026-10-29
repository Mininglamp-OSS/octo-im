package cluster

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestOrphanMigrationCleanupAtReplicaTarget(t *testing.T) {
	for _, replicas := range [][]uint64{{1}, {1, 2, 3}} {
		cfg := wkdb.ChannelClusterConfig{ReplicaMaxCount: uint16(len(replicas)),
			Replicas: replicas, MigrateFrom: 99, MigrateTo: 99}
		changed, err := (&Server{}).joinNewRepliceIfNeed(&cfg)
		require.NoError(t, err)
		require.True(t, changed)
		require.Zero(t, cfg.MigrateFrom)
		require.Zero(t, cfg.MigrateTo)
		require.Equal(t, uint16(len(replicas)), cfg.ReplicaMaxCount)
		require.Equal(t, replicas, cfg.Replicas)
	}
}

func TestSingleReplicaOrphanCleanupPersistsWithoutSend(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 3, 1)
	cfg := orphanMigrationConfig(99)
	cfg.ReplicaMaxCount = 1
	_, before := saveOrphanMigrationConfig(t, ctx, servers, cfg)
	for _, peer := range servers {
		peer.initChannelConfigReconciler()
		peer.configReconciler.start()
	}
	require.Eventually(t, func() bool {
		for _, peer := range servers {
			current, err := peer.db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
			if err != nil || current.ConfVersion <= before.ConfVersion || current.MigrateFrom != 0 ||
				current.MigrateTo != 0 || current.ReplicaMaxCount != 1 || len(current.Replicas) != 1 ||
				current.Replicas[0] != 1 || len(current.Learners) != 0 {
				return false
			}
		}
		return true
	}, 6*time.Second, 20*time.Millisecond)
}
