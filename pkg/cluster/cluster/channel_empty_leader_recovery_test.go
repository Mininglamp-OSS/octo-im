package cluster

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/stretchr/testify/require"
)

func TestEmptyRejoinedChannelLeaderRecoversFromSurvivingReplica(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	owner, empty := servers[0], servers[1]
	for _, mode := range []string{"activation", "background", "empty-control", "missing-voter", "nonempty-control"} {
		t.Run(mode, func(t *testing.T) {
			id := fmt.Sprintf("empty-leader-%s", mode)
			for owner.cfgServer.SlotLeaderId(owner.getSlotId(id)) != 1 {
				id += "x"
			}
			cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 2,
				Term: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}
			version, err := owner.saveChannelConfig(ctx, cfg)
			require.NoError(t, err)
			cfg.ConfVersion = version
			if mode != "empty-control" {
				message := consistencyMessage(1)
				message.ChannelID = id
				require.NoError(t, owner.db.AppendMessages(id, 2, []wkdb.Message{message}))
			}
			if mode == "missing-voter" {
				servers[2].Route("/rpc/channel/lastLogInfo", func(c *wkserver.Context) { c.WriteErr(ErrConversationReadRetry) })
			}
			if mode == "nonempty-control" {
				message := consistencyMessage(1)
				message.ChannelID = id
				require.NoError(t, empty.db.AppendMessages(id, 2, []wkdb.Message{message}))
			}
			if mode == "activation" {
				err = empty.channelServer.WakeLeaderIfNeed(cfg)
			} else {
				_, err = empty.reconcileLocalChannelConfig(ctx, cfg)
			}
			if mode == "missing-voter" {
				require.ErrorIs(t, err, ErrConversationReadRetry)
				stored, loadErr := owner.loadConversationConfig(ctx, id, 2)
				require.NoError(t, loadErr)
				require.Equal(t, cfg.ConfVersion, stored.ConfVersion, "partial probes cannot authorize an empty leader")
			} else if mode == "empty-control" || mode == "nonempty-control" {
				require.NoError(t, err)
				stored, loadErr := owner.loadConversationConfig(ctx, id, 2)
				require.NoError(t, loadErr)
				require.Equal(t, uint64(2), stored.LeaderId)
				require.Equal(t, cfg.ConfVersion, stored.ConfVersion)
			} else {
				require.ErrorIs(t, err, raft.ErrConfigVersionStale)
				stored, loadErr := owner.loadConversationConfig(ctx, id, 2)
				require.NoError(t, loadErr)
				require.Equal(t, uint64(1), stored.LeaderId, "the surviving data-bearing voter must lead")
				require.Greater(t, stored.Term, cfg.Term)
				require.Greater(t, stored.ConfVersion, cfg.ConfVersion)
			}
			require.False(t, empty.channelServer.ExistChannel(id, 2), "an empty returning leader must not accept new writes before recovery")
		})
	}
}
