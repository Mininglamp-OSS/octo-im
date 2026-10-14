package cluster

import (
	"context"
	"math"
	"testing"

	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func TestConfigElectionRequiresCurrentVoterQuorum(t *testing.T) {
	tests := []struct {
		name     string
		replicas []uint64
		learners []uint64
		infos    map[uint64]*ChannelLastLogInfoResponse
		leader   uint64
	}{
		{"majority", []uint64{1, 2, 3}, nil, map[uint64]*ChannelLastLogInfoResponse{2: {LogTerm: 2, LogIndex: 4}, 3: {LogTerm: 2, LogIndex: 5}}, 3},
		{"minority", []uint64{1, 2, 3}, nil, map[uint64]*ChannelLastLogInfoResponse{2: {LogTerm: 2, LogIndex: 4}}, 0},
		{"learner is not a vote", []uint64{1, 2, 3}, []uint64{4}, map[uint64]*ChannelLastLogInfoResponse{2: {}, 4: {LogIndex: 99}}, 0},
		{"nonmember is not a vote", []uint64{1, 2, 3}, nil, map[uint64]*ChannelLastLogInfoResponse{2: {}, 9: {}}, 0},
		{"duplicate voter", []uint64{1, 2, 2, 3}, nil, map[uint64]*ChannelLastLogInfoResponse{2: {}}, 0},
		{"actual single voter despite desired three", []uint64{2}, nil, map[uint64]*ChannelLastLogInfoResponse{2: {}}, 2},
		{"empty logs choose deterministically", []uint64{1, 2, 3}, nil, map[uint64]*ChannelLastLogInfoResponse{2: {}, 3: {}}, 2},
		{"no voters", nil, nil, nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := wkdb.ChannelClusterConfig{LeaderId: 1, Term: 3, ReplicaMaxCount: 3, Replicas: tt.replicas, Learners: tt.learners}
			before := cfg.Clone()
			changed, err := electChannelLeader(&cfg, tt.infos)
			if tt.leader == 0 {
				require.ErrorIs(t, err, ErrConversationReadRetry)
				require.False(t, changed)
				require.Equal(t, before, cfg, "failed elections must not publish partial changes")
				return
			}
			require.NoError(t, err)
			require.True(t, changed)
			require.Equal(t, tt.leader, cfg.LeaderId)
			require.Equal(t, uint32(4), cfg.Term)
		})
	}
}

func TestConfigElectionPreservesFreshestLogAndFencesAllTerms(t *testing.T) {
	// A high current term with an older log must not exclude the newer log.
	infos := map[uint64]*ChannelLastLogInfoResponse{
		2: {Term: 20, LogTerm: 2, LogIndex: 100},
		3: {Term: 4, LogTerm: 3, LogIndex: 5},
	}
	for i := 0; i < 100; i++ {
		cfg := wkdb.ChannelClusterConfig{LeaderId: 1, Term: 3, Replicas: []uint64{1, 2, 3}}
		changed, err := electChannelLeader(&cfg, infos)
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, uint64(3), cfg.LeaderId)
		require.Equal(t, uint32(21), cfg.Term)
	}
	cfg := wkdb.ChannelClusterConfig{LeaderId: 1, Term: math.MaxUint32, Replicas: []uint64{1, 2, 3}}
	before := cfg.Clone()
	changed, err := electChannelLeader(&cfg, infos)
	require.ErrorIs(t, err, ErrConversationReadRetry)
	require.False(t, changed)
	require.Equal(t, before, cfg)
}

func TestConfigElectionReportsDurableTermWithoutRuntime(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("analysis-channel", 2)
	require.NoError(t, err)
	require.NoError(t, s.db.AppendMessages(cfg.ChannelId, cfg.ChannelType, []wkdb.Message{consistencyMessage(1)}))
	require.NoError(t, s.db.SaveRaftHardState(wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType), rafttype.HardState{Term: 17, Vote: 2}))
	info, err := s.getChannelLastLogInfo(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint32(17), info.Term)
	require.Equal(t, uint32(1), info.LogTerm)
	require.Equal(t, uint64(1), info.LogIndex)
	state, err := s.channelServer.ReadLeaderState(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.False(t, state.Exists, "log probes must not wake dormant replicas")
}

func TestConfigMaintenanceFailedElectionLeavesMetadataUnchanged(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	s.configReconciler.stop()
	cfg := consistencyConfig()
	cfg.ReplicaMaxCount, cfg.Replicas, cfg.LeaderId = 3, []uint64{1, 2, 3}, 2
	version, err := s.store.SaveChannelClusterConfig(cfg)
	require.NoError(t, err)
	cfg.ConfVersion = version
	current, err := s.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	_, err = s.maintainChannelConfig(ctx, current)
	require.ErrorIs(t, err, ErrConversationReadRetry)
	after, err := s.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, current, after)
}

func TestConfigElectionCancelledBeforePeerAccess(t *testing.T) {
	s := &Server{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.getChannelLastLogInfo(ctx, "unavailable", 2)
	require.ErrorIs(t, err, context.Canceled)
	_, err = s.maintainChannelConfig(ctx, consistencyConfig())
	require.ErrorIs(t, err, context.Canceled)
}
