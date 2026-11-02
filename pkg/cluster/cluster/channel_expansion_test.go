package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
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
	require.True(t, validSlotConfigTransition(current, next))
	current.Learners, current.MigrateTo = []uint64{5}, 5
	require.False(t, validSlotConfigTransition(current, next), "old D callback cannot overwrite a newer E migration")
	current.Learners, current.MigrateTo, current.Leader, current.Term = []uint64{4}, 4, 2, 3
	require.False(t, validSlotConfigTransition(current, next), "a previous leader cannot restore its term/role")
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
