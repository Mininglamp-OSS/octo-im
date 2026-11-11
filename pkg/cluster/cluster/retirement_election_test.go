package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestChannelRetirementDestinationElectionResumes(t *testing.T) {
	cfg := wkdb.ChannelClusterConfig{LeaderId: 1, Term: 2, ReplicaMaxCount: 3,
		Replicas: []uint64{1, 2, 3, 4}, MigrateFrom: 3, MigrateTo: 4}
	changed, err := electChannelLeader(&cfg, map[uint64]*ChannelLastLogInfoResponse{
		2: {Term: 2, LogTerm: 2, LogIndex: 10},
		3: {Term: 2, LogTerm: 2, LogIndex: 10},
		4: {Term: 2, LogTerm: 2, LogIndex: 11},
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, uint64(4), cfg.LeaderId)
	cluster := &types.Config{}
	for _, id := range []uint64{1, 2, 3, 4} {
		cluster.Nodes = append(cluster.Nodes, &types.Node{Id: id, Online: id != 1, AllowVote: true, Status: types.NodeStatus_NodeStatusJoined})
	}
	require.True(t, planChannelExpansion(&cfg, cluster), "the elected destination must retain a finishable retirement task")
	require.Equal(t, uint64(1), cfg.MigrateFrom, "retire the failed former leader")
	require.Contains(t, []uint64{2, 3}, cfg.MigrateTo, "a follower must acknowledge; the leader cannot sync to itself")
	require.Len(t, cfg.Replicas, 4, "planning must not bypass installed-majority confirmation")
}

func TestSlotRetirementElectionKeepsTask(t *testing.T) {
	for _, leader := range []uint64{3, 4} {
		t.Run(map[uint64]string{3: "source-leader", 4: "destination-leader"}[leader], func(t *testing.T) {
			slot := &types.Slot{Id: 7, Leader: 1, Term: 2, Status: types.SlotStatus_SlotStatusCandidate,
				Replicas: []uint64{1, 2, 3, 4}, MigrateFrom: 3, MigrateTo: 4}
			next, err := slotElectionConfig(slot, leader, nil, 3)
			require.NoError(t, err)
			require.NotZero(t, next.MigrateFrom, "an election must not erase a pending replica retirement")
			require.NotZero(t, next.MigrateTo)
			require.Len(t, next.Replicas, 4)
			require.Equal(t, leader, next.Leader)
			require.False(t, validSlotConfigTransition(next, rafttypes.Config{Leader: 4, Term: next.Term + 1, Replicas: next.Replicas}, 3), "a leader transfer cannot erase an excess replica task")
		})
	}
}
