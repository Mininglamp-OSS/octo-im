package cluster

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

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
