package cluster

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/stretchr/testify/require"
)

// This is an injected notification failure test, separate from the real
// no-fault expansion runs. Metadata commits, but the new leader rejects the
// first notification: the old leader must still demote and reconcile safely.
func TestHandoffNotificationFailureCannotReopenOldLeader(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 2)
	old, next := servers[0], servers[1]
	var id string
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("handoff-%d", i)
		if old.cfgServer.SlotLeaderId(old.getSlotId(candidate)) == 1 {
			id = candidate
			break
		}
	}
	require.NotEmpty(t, id)
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 1, Term: 1, Replicas: []uint64{1}}
	version, err := old.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	cfg.ConfVersion = version
	require.NoError(t, old.channelServer.WakeLeaderIfNeed(cfg))
	var allow atomic.Bool
	next.netServer.Route("/rpc/channel/switchConfig", func(c *wkserver.Context) {
		if !allow.Load() {
			c.WriteErr(errors.New("injected notification failure"))
			return
		}
		next.rpcServer.handleChannelSwitchConfig(c)
	})
	transition := rafttype.Config{Version: cfg.ConfVersion, Term: 2, Leader: 2, Replicas: []uint64{2}}
	require.Error(t, old.onSaveChannelConfig(id, 2, transition))
	persisted, err := old.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), persisted.LeaderId)
	require.Equal(t, uint32(2), persisted.Term)
	state, err := old.channelServer.ReadLeaderState(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.LeaderID)
	enqueued := false
	require.Error(t, old.channelServer.AdmitMessage(ctx, id, 2, 42, 1, func(func()) { enqueued = true }))
	require.False(t, enqueued)
	allow.Store(true)
	// Repeating the old callback cannot overwrite the already committed config.
	_ = old.onSaveChannelConfig(id, 2, transition)
	current, err := old.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, persisted.ConfVersion, current.ConfVersion)
	require.Equal(t, uint64(2), current.LeaderId)
	require.NoError(t, next.channelServer.WakeLeaderIfNeed(current))
	var done func()
	require.NoError(t, next.channelServer.AdmitMessage(ctx, id, 2, 43, 1, func(f func()) { done = f }))
	done()
}

func TestChannelCallbackKeepsRetirementPendingAfterLocalInstall(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	owner := servers[0]
	var id string
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("callback-retirement-%d", i)
		if owner.cfgServer.SlotLeaderId(owner.getSlotId(candidate)) == 1 {
			id = candidate
			break
		}
	}
	require.NotEmpty(t, id)
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 1, Term: 1,
		ReplicaMaxCount: 2, Replicas: []uint64{1, 2}, Learners: []uint64{3}, MigrateFrom: 2, MigrateTo: 3}
	version, err := owner.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	cfg.ConfVersion = version
	require.NoError(t, owner.channelServer.WakeLeaderIfNeed(cfg))
	completion := rafttype.Config{Version: version, Term: 1, Leader: 1, Replicas: []uint64{1, 3}}
	require.ErrorIs(t, owner.onSaveChannelConfig(id, 2, completion), clusterconfig.ErrMembershipBusy)
	stage, err := owner.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{1, 2, 3}, stage.Replicas, "healthy source remains until the intermediate voting set is confirmed")
	require.Empty(t, stage.Learners)
	require.Equal(t, uint64(2), stage.MigrateFrom)
	require.Equal(t, uint64(3), stage.MigrateTo)
	state, err := owner.channelServer.ReadLeaderState(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, stage.ConfVersion, state.ConfigVersion, "the intermediate authority is installed locally even while retirement remains pending")
	require.Equal(t, uint64(1), state.LeaderID)
	// A delayed callback must neither finish the intermediate task nor rewrite
	// its version, even when the callback reconciles the already saved authority.
	require.Error(t, owner.onSaveChannelConfig(id, 2, completion))
	current, err := owner.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, stage.ConfVersion, current.ConfVersion)
	require.Equal(t, stage.Replicas, current.Replicas)
	require.Equal(t, stage.MigrateTo, current.MigrateTo)
}
