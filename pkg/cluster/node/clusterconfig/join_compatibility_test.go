package clusterconfig

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func compatibilityJoinConfig() *pb.Config {
	cfg := removalTestConfig()
	cfg.Nodes[1].Online, cfg.Nodes[2].Online = false, true
	cfg.Nodes[1].OfflineCount = 1
	// A version-2 admission could already be in flight when the node upgrades.
	cfg.MigrateFrom = 2
	return cfg
}

func compatibilityRemoval() *CMD {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data, 2)
	binary.BigEndian.PutUint64(data[8:], 4)
	return NewCMD(CMDTypeNodeRemove, data)
}

func compatibilityPromotion(id uint64) *CMD {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, id)
	return NewCMD(CMDTypeNodeJoining, data)
}

func TestJoinPromotionPreservesMemberReturningAroundRemoval(t *testing.T) {
	for _, when := range []string{"before_removal_planned", "before_removal_applied", "after_removal_applied"} {
		t.Run(when, func(t *testing.T) {
			s := New(NewOptions(WithConfigPath(filepath.Join(t.TempDir(), "config.json"))))
			t.Cleanup(func() { require.NoError(t, s.config.cfgFile.Close()) })
			s.config.cfg = compatibilityJoinConfig()
			removal := compatibilityRemoval()
			if when == "before_removal_planned" {
				s.config.updateNodeOnlineStatus(2, true)
				require.Zero(t, offlineVoterForJoin(s.Snapshot(), 4))
			} else {
				require.Equal(t, uint64(2), offlineVoterForJoin(s.Snapshot(), 4))
				if when == "before_removal_applied" {
					s.config.updateNodeOnlineStatus(2, true)
				}
				require.NoError(t, s.handleNodeRemove(removal))
			}
			if when == "after_removal_applied" {
				require.Nil(t, s.Node(2))
				_, err := s.ProposeJoinWithVersion(&pb.Node{Id: 2, ClusterAddr: "127.0.0.1:1002", Online: true, AllowVote: true})
				require.ErrorIs(t, err, ErrMembershipBusy, "a removed identity must wait for D's committed admission")
				require.Equal(t, []uint64{4}, s.Snapshot().Learners)
			}
			promotion := compatibilityPromotion(4)
			require.Equal(t, uint16(1), promotion.version, "new promotion must not replay version-2 implicit retirement")
			require.NoError(t, s.handleNodeJoining(promotion))
			if when != "after_removal_applied" {
				require.True(t, s.Node(2).Online)
				require.True(t, s.Node(2).AllowVote, "the old MigrateFrom cannot cancel a recovered member's vote")
				require.ElementsMatch(t, []uint64{1, 2, 3, 4}, s.configToRaftConfig(s.config).Replicas)
			} else {
				require.ElementsMatch(t, []uint64{1, 3, 4}, s.configToRaftConfig(s.config).Replicas)
			}
			require.Empty(t, s.Snapshot().Learners)
			require.Zero(t, s.Snapshot().MigrateTo)
		})
	}
}

func TestLegacyReplacementSnapshotRetainsAndReadmitsOldIdentity(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.json")
	s := New(NewOptions(WithConfigPath(configPath)))
	cfg := compatibilityJoinConfig()
	cfg.SlotCount = s.config.cfg.SlotCount
	cfg.SlotReplicaCount = s.config.cfg.SlotReplicaCount
	cfg.ChannelReplicaCount = s.config.cfg.ChannelReplicaCount
	cfg.Nodes = cfg.Nodes[:3]
	cfg.Learners, cfg.MigrateFrom, cfg.MigrateTo = nil, 0, 0
	s.config.cfg = cfg
	joinData, err := (&pb.Node{Id: 4, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusWillJoin}).Marshal()
	require.NoError(t, err)
	require.NoError(t, s.handleCmd(replacementTestCommand(t, CMDTypeNodeJoin, joinData)))
	require.NoError(t, s.handleCmd(replacementTestCommand(t, CMDTypeNodeJoining, compatibilityPromotion(4).Data)))
	joinedData, err := EncodeNodeJoined(4, nil)
	require.NoError(t, err)
	require.NoError(t, s.handleCmd(NewCMD(CMDTypeNodeJoined, joinedData)))
	before := s.Snapshot()
	require.False(t, s.Node(2).AllowVote)
	require.NoError(t, s.config.saveConfig())
	require.NoError(t, s.config.cfgFile.Close())

	reloaded := New(NewOptions(WithConfigPath(configPath)))
	t.Cleanup(func() { require.NoError(t, reloaded.config.cfgFile.Close()) })
	require.Equal(t, before, reloaded.Snapshot(), "upgrade must not reinterpret a version-2 snapshot")
	require.ElementsMatch(t, []uint64{1, 3, 4}, reloaded.configToRaftConfig(reloaded.config).Replicas)
	require.Zero(t, offlineVoterWithSpareCapacity(reloaded.Snapshot()), "historical nonvoting metadata is not a new voter retirement")
	returnData, err := (&pb.Node{Id: 2, ClusterAddr: "127.0.0.1:2002", AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusWillJoin}).Marshal()
	require.NoError(t, err)
	join := NewCMD(CMDTypeNodeJoin, returnData)
	require.Equal(t, uint16(1), join.version)
	require.NoError(t, reloaded.handleCmd(join))
	require.Equal(t, []uint64{2}, reloaded.Snapshot().Learners)
	require.ElementsMatch(t, []uint64{1, 3, 4}, reloaded.configToRaftConfig(reloaded.config).Replicas, "reusing the identity does not skip learner catch-up")
	require.NoError(t, reloaded.handleCmd(compatibilityPromotion(2)))
	require.ElementsMatch(t, []uint64{1, 2, 3, 4}, reloaded.configToRaftConfig(reloaded.config).Replicas)
	require.True(t, reloaded.Node(2).AllowVote)
	require.True(t, reloaded.Node(2).Online)
}
