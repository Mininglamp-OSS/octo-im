package cluster

import (
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestFullChannelReplacesOfflineReplicaWithoutChangingVotersEarly(t *testing.T) {
	s, _ := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	cluster := s.cfgServer.GetClusterConfig().Clone()
	cluster.Nodes = []*pb.Node{
		{Id: 1, Online: true, AllowVote: true, Status: pb.NodeStatus_NodeStatusJoined},
		{Id: 2, Online: false, AllowVote: false, OfflineCount: 1, Status: pb.NodeStatus_NodeStatusJoined},
		{Id: 3, Online: true, AllowVote: true, Status: pb.NodeStatus_NodeStatusJoined},
		{Id: 4, Online: true, AllowVote: true, Status: pb.NodeStatus_NodeStatusJoined},
	}
	// Publish through the existing single voter; no further proposals are needed
	// after installing this fixture's three-voter membership.
	require.NoError(t, s.cfgServer.ProposeConfig(cluster))
	cfg := wkdb.ChannelClusterConfig{ChannelId: "offline-replacement", ChannelType: 2, LeaderId: 1, Term: 3, ConfVersion: 7, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}
	changed, err := s.joinNewRepliceIfNeed(&cfg)
	require.NoError(t, err)
	require.True(t, changed, "three configured IDs do not imply three live replicas")
	require.ElementsMatch(t, []uint64{1, 2, 3}, cfg.Replicas)
	require.Equal(t, []uint64{4}, cfg.Learners)
	require.Equal(t, uint64(2), cfg.MigrateFrom)
	require.Equal(t, uint64(4), cfg.MigrateTo)
	require.Equal(t, uint64(1), cfg.LeaderId)
	require.Equal(t, uint16(3), cfg.ReplicaMaxCount)
	for _, tc := range []struct {
		name string
		cfg  wkdb.ChannelClusterConfig
	}{
		{"healthy", wkdb.ChannelClusterConfig{LeaderId: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 3, 4}}},
		{"no old quorum", wkdb.ChannelClusterConfig{LeaderId: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 5}}},
		{"single replica policy", wkdb.ChannelClusterConfig{LeaderId: 1, ReplicaMaxCount: 1, Replicas: []uint64{1}}},
		{"busy learner", wkdb.ChannelClusterConfig{LeaderId: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}}},
		{"busy migration", wkdb.ChannelClusterConfig{LeaderId: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}, MigrateFrom: 1, MigrateTo: 3}},
		{"partial migration", wkdb.ChannelClusterConfig{LeaderId: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}, MigrateFrom: 2}},
		{"no spare", wkdb.ChannelClusterConfig{LeaderId: 1, ReplicaMaxCount: 4, Replicas: []uint64{1, 2, 3, 4}}},
		{"failed leader needs election", wkdb.ChannelClusterConfig{LeaderId: 2, ReplicaMaxCount: 3, Replicas: []uint64{2, 3, 4}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.cfg.Clone()
			changed, err := s.joinNewRepliceIfNeed(&tc.cfg)
			require.NoError(t, err)
			require.False(t, changed)
			require.Equal(t, before, tc.cfg)
		})
	}
}
