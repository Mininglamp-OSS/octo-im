package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestRejectedTransitionRefreshesCanonicalRuntime(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 1)
	s := servers[0]
	cfg := wkdb.ChannelClusterConfig{ChannelId: "stale-promotion", ChannelType: 2, LeaderId: 1, Term: 1, ConfVersion: 1, Replicas: []uint64{1}, ReplicaMaxCount: 1}
	require.NoError(t, s.db.SaveChannelClusterConfig(cfg))
	require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))
	canonical := cfg.Clone()
	canonical.ConfVersion = 2
	require.NoError(t, s.db.SaveChannelClusterConfig(canonical))
	err := s.onSaveChannelConfig(cfg.ChannelId, cfg.ChannelType, rafttype.Config{Version: 1, Term: 1, Leader: 1, Replicas: []uint64{1, 99}})
	require.ErrorIs(t, err, raft.ErrConfigVersionStale)
	state, err := s.channelServer.ReadLeaderState(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.ConfigVersion)
	saved, err := s.db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(2), saved.ConfVersion)
	require.Equal(t, []uint64{1}, saved.Replicas, "never rebase the rejected decision onto the new version")
}

func TestRejectedTransitionReturnsOriginalErrorWhenRefreshApplyFails(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 1)
	s := servers[0]
	runtime := wkdb.ChannelClusterConfig{
		ChannelId: "stale-promotion-refresh-conflict", ChannelType: 2, LeaderId: 1,
		Term: 1, ConfVersion: 2, Replicas: []uint64{1}, ReplicaMaxCount: 1,
	}
	require.NoError(t, s.db.SaveChannelClusterConfig(runtime))
	require.NoError(t, s.channelServer.WakeLeaderIfNeed(runtime))

	// The authority and runtime disagree about the term at the same version,
	// so the refresh can read a newer version than the callback but cannot install it.
	canonical := runtime.Clone()
	canonical.Term++
	require.NoError(t, s.db.SaveChannelClusterConfig(canonical))
	require.ErrorIs(t, s.channelServer.SwitchConfigContext(ctx, runtime.ChannelId, runtime.ChannelType, canonical), channel.ErrConfigConflict)

	stale := rafttype.Config{Version: 1, Term: 1, Leader: 1, Replicas: []uint64{1, 99}}
	_, transitionErr := s.saveChannelConfigTransition(ctx, runtime.ChannelId, runtime.ChannelType, stale)
	require.ErrorIs(t, transitionErr, raft.ErrConfigVersionStale)
	require.Equal(t, transitionErr, s.onSaveChannelConfig(runtime.ChannelId, runtime.ChannelType, stale))
}
