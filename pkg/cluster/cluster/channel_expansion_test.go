package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestChannelExpansionRepairsFullButUnavailableReplicas(t *testing.T) {
	cluster := &types.Config{}
	for _, id := range []uint64{1, 2, 3, 4} {
		cluster.Nodes = append(cluster.Nodes, &types.Node{Id: id, Online: id != 3, AllowVote: true, Status: types.NodeStatus_NodeStatusJoined})
	}
	initial := wkdb.ChannelClusterConfig{LeaderId: 1, Replicas: []uint64{1, 2, 3}, ReplicaMaxCount: 3}
	cfg := initial.Clone()
	require.True(t, planChannelExpansion(&cfg, cluster))
	require.Equal(t, initial.Replicas, cfg.Replicas)
	require.Equal(t, []uint64{4}, cfg.Learners)
	require.Equal(t, uint64(3), cfg.MigrateFrom)
	require.Equal(t, uint64(4), cfg.MigrateTo)
	require.Equal(t, initial.LeaderId, cfg.LeaderId)
	before := cfg.Clone()
	require.False(t, planChannelExpansion(&cfg, cluster))
	require.Equal(t, before, cfg)
	cluster.Nodes[2].Online = true
	require.False(t, planChannelExpansion(&cfg, cluster), "returning C does not overwrite an admitted transition")
	cluster.Nodes[3].Online = false
	require.True(t, planChannelExpansion(&cfg, cluster))
	require.Empty(t, cfg.Learners)
	require.Zero(t, cfg.MigrateTo)
	require.False(t, planChannelExpansion(&cfg, cluster), "three restored live replicas already meet the target")
}

func TestChannelExpansionPreservesHealthyThreeAndRequiresOldQuorum(t *testing.T) {
	cluster := &types.Config{}
	for _, id := range []uint64{1, 2, 3, 4} {
		cluster.Nodes = append(cluster.Nodes, &types.Node{Id: id, Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusJoined})
	}
	cfg := wkdb.ChannelClusterConfig{LeaderId: 1, Replicas: []uint64{1, 2, 3}, ReplicaMaxCount: 3}
	before := cfg.Clone()
	require.False(t, planChannelExpansion(&cfg, cluster))
	cluster.Nodes[1].Online, cluster.Nodes[2].Online = false, false
	require.False(t, planChannelExpansion(&cfg, cluster))
	require.Equal(t, before, cfg)
}

func TestSlotTransitionRejectsDelayedLearnerCompletion(t *testing.T) {
	current := &types.Slot{Id: 1, Leader: 1, Term: 2, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, MigrateFrom: 3, MigrateTo: 4}
	next := rafttype.Config{Leader: 1, Term: 2, Replicas: []uint64{1, 2, 4}}
	require.True(t, validSlotConfigTransition(current, next, 3))
	current.Learners, current.MigrateTo = []uint64{5}, 5
	require.False(t, validSlotConfigTransition(current, next, 3), "old D callback cannot overwrite a newer E migration")
	current.Learners, current.MigrateTo, current.Leader, current.Term = []uint64{4}, 4, 2, 3
	require.False(t, validSlotConfigTransition(current, next, 3), "a previous leader cannot restore its term/role")
}

func TestChannelRetirementDoesNotMutatePriorCachedMembership(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	cfg := wkdb.ChannelClusterConfig{ChannelId: "cached-retirement", ChannelType: 2, LeaderId: 1, Term: 2, ReplicaMaxCount: 3, Replicas: []uint64{1, 3, 2}, Learners: []uint64{4}, MigrateFrom: 3, MigrateTo: 4}
	version, err := s.store.SaveChannelClusterConfig(cfg)
	require.NoError(t, err)
	prior, err := s.db.GetChannelClusterConfig(cfg.ChannelId, 2)
	require.NoError(t, err)
	next, err := s.saveChannelConfigTransitionLocal(ctx, cfg.ChannelId, 2, rafttype.Config{Version: version, Term: 2, Leader: 1, Replicas: []uint64{1, 2, 4}})
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, next.Replicas)
	require.Equal(t, []uint64{4}, next.Learners)
	require.Equal(t, []uint64{1, 3, 2}, prior.Replicas, "an uncommitted transition must not alter an earlier cache snapshot")
}

func TestChannelRetirementKeepsHealthySourceUntilAddFirstCompletion(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	// Node 1 is a healthy source, while node 2 remains the message leader.
	cfg := wkdb.ChannelClusterConfig{ChannelId: "healthy-retirement", ChannelType: 2, LeaderId: 2, Term: 2,
		ReplicaMaxCount: 3, Replicas: []uint64{2, 1, 3}, Learners: []uint64{4}, MigrateFrom: 1, MigrateTo: 4}
	version, err := s.store.SaveChannelClusterConfig(cfg)
	require.NoError(t, err)
	prior, err := s.db.GetChannelClusterConfig(cfg.ChannelId, 2)
	require.NoError(t, err)
	completed := rafttype.Config{Version: version, Term: 2, Leader: 2, Replicas: []uint64{2, 3, 4}}
	stage, err := s.saveChannelConfigTransitionLocal(ctx, cfg.ChannelId, 2, completed)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{1, 2, 3, 4}, stage.Replicas)
	require.Empty(t, stage.Learners)
	require.Equal(t, uint64(1), stage.MigrateFrom)
	require.Equal(t, uint64(4), stage.MigrateTo)
	require.Equal(t, uint64(2), stage.LeaderId)
	require.Equal(t, uint16(3), stage.ReplicaMaxCount)
	require.Equal(t, []uint64{2, 1, 3}, prior.Replicas, "staging must not mutate an earlier snapshot")
	_, err = s.saveChannelConfigTransitionLocal(ctx, cfg.ChannelId, 2, completed)
	require.ErrorIs(t, err, raft.ErrConfigVersionStale, "an old completion cannot skip the installed-majority stage")
	completed.Version = stage.ConfVersion
	final, err := s.saveChannelConfigTransitionLocal(ctx, cfg.ChannelId, 2, completed)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{2, 3, 4}, final.Replicas)
	require.Empty(t, final.Learners)
	require.Zero(t, final.MigrateFrom)
	require.Zero(t, final.MigrateTo)
	require.Greater(t, final.ConfVersion, stage.ConfVersion)
	require.ElementsMatch(t, []uint64{1, 2, 3, 4}, stage.Replicas)
}

func TestChannelExpansionCancelsDeadFormalTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*wkdb.ChannelClusterConfig, *types.Config)
		cancel bool
	}{
		{"old-voters-healthy", func(*wkdb.ChannelClusterConfig, *types.Config) {}, true},
		{"missing-old-majority", func(_ *wkdb.ChannelClusterConfig, c *types.Config) { c.Nodes[1].Online = false }, false},
		{"source-unavailable", func(_ *wkdb.ChannelClusterConfig, c *types.Config) { c.Nodes[2].Online = false }, false},
		{"source-elected-leader", func(c *wkdb.ChannelClusterConfig, _ *types.Config) { c.MigrateFrom = c.LeaderId }, true},
		{"leader-transfer", func(c *wkdb.ChannelClusterConfig, _ *types.Config) { c.MigrateFrom, c.ReplicaMaxCount = c.LeaderId, 4 }, false},
		{"source-not-a-voter", func(c *wkdb.ChannelClusterConfig, _ *types.Config) { c.MigrateFrom = 9 }, false},
		{"destination-healthy", func(_ *wkdb.ChannelClusterConfig, c *types.Config) { c.Nodes[3].Online = true }, false},
		{"capacity-still-needed", func(c *wkdb.ChannelClusterConfig, _ *types.Config) { c.ReplicaMaxCount = 4 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &types.Config{}
			for _, id := range []uint64{1, 2, 3, 4} {
				cluster.Nodes = append(cluster.Nodes, &types.Node{Id: id, Online: id != 4, AllowVote: true, Status: types.NodeStatus_NodeStatusJoined})
			}
			cfg := wkdb.ChannelClusterConfig{LeaderId: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3, 4}, MigrateFrom: 3, MigrateTo: 4}
			tc.mutate(&cfg, cluster)
			before := cfg.Clone()
			require.Equal(t, tc.cancel, planChannelExpansion(&cfg, cluster))
			if tc.cancel {
				require.Equal(t, []uint64{1, 2, 3}, cfg.Replicas)
				require.Zero(t, cfg.MigrateFrom)
				require.Zero(t, cfg.MigrateTo)
				require.False(t, planChannelExpansion(&cfg, cluster))
			} else {
				require.True(t, before.Equal(cfg), "an active or unsafe-to-cancel transition must be preserved")
			}
		})
	}
}

func TestChannelExpansionRetargetsSourceElectedLeader(t *testing.T) {
	cluster := &types.Config{}
	for _, id := range []uint64{1, 2, 3, 4} {
		cluster.Nodes = append(cluster.Nodes, &types.Node{Id: id, Online: id != 1, AllowVote: true, Status: types.NodeStatus_NodeStatusJoined})
	}
	cfg := wkdb.ChannelClusterConfig{LeaderId: 3, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3, 4}, MigrateFrom: 3, MigrateTo: 4}
	before := cfg.Clone()
	cluster.Nodes[1].Online = false
	require.False(t, planChannelExpansion(&cfg, cluster), "do not rewrite a task after losing its old majority")
	require.True(t, before.Equal(cfg))
	cluster.Nodes[1].Online = true
	require.True(t, planChannelExpansion(&cfg, cluster))
	require.Equal(t, uint64(1), cfg.MigrateFrom, "retire the failed old leader and retain the recovered new leader")
	require.Equal(t, uint64(3), cfg.LeaderId)
	require.Equal(t, uint64(4), cfg.MigrateTo)
	require.ElementsMatch(t, before.Replicas, cfg.Replicas)
	require.False(t, planChannelExpansion(&cfg, cluster), "the retargeted follower task must remain stable")
	cfg = before.Clone()
	cfg.ReplicaMaxCount = 4
	require.False(t, planChannelExpansion(&cfg, cluster), "a normal voter leadership transfer must retain its intent")
	cfg.ReplicaMaxCount = 0
	require.False(t, planChannelExpansion(&cfg, cluster), "unknown target capacity cannot identify an add-first stage")
}

func TestChannelRetirementRejectsLeaderTransferFromAddFirstStage(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	cfg := wkdb.ChannelClusterConfig{ChannelId: "source-elected-leader", ChannelType: 2, LeaderId: 2, Term: 3,
		ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3, 4}, MigrateFrom: 2, MigrateTo: 4}
	version, err := s.store.SaveChannelClusterConfig(cfg)
	require.NoError(t, err)
	_, err = s.saveChannelConfigTransitionLocal(ctx, cfg.ChannelId, 2,
		rafttype.Config{Version: version, Term: 4, Leader: 4, Replicas: []uint64{1, 2, 3, 4}})
	require.ErrorIs(t, err, ErrConversationReadRetry, "a changed source role cannot silently complete with an extra voter")
	stored, err := s.db.GetChannelClusterConfig(cfg.ChannelId, 2)
	require.NoError(t, err)
	cfg.ConfVersion = version
	require.True(t, cfg.Equal(stored), "retain the durable task until maintenance retargets its follower")
}
