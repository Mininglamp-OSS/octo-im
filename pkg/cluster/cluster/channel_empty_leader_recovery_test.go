package cluster

import (
	"context"
	"errors"
	"fmt"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/stretchr/testify/require"
)

func TestDormantChannelLeaderRestoresRebuiltReplicasWithoutSending(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	leader := servers[0]
	id := "dormant-surviving-leader"
	for leader.cfgServer.SlotLeaderId(leader.getSlotId(id)) != 1 {
		id += "x"
	}
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 1,
		Term: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}
	version, err := saveRecoveryTestConfig(t, leader, ctx, cfg)
	require.NoError(t, err)
	cfg.ConfVersion = version
	var history []wkdb.Message
	for seq := uint32(1); seq <= 4; seq++ {
		m := consistencyMessage(seq)
		m.ChannelID = id
		history = append(history, m)
	}
	require.NoError(t, leader.db.AppendMessages(id, 2, history))
	require.False(t, leader.channelServer.ExistChannel(id, 2), "the surviving leader starts dormant")
	_, err = leader.reconcileLocalChannelConfig(ctx, cfg)
	require.NoError(t, err)
	require.True(t, leader.channelServer.ExistChannel(id, 2), "background reconciliation must wake a data-bearing dormant leader to repair empty replicas")
	require.Eventually(t, func() bool {
		for _, s := range servers {
			messages, err := s.db.LoadLastMsgs(id, 2, 100)
			if err != nil || len(messages) != len(history) {
				return false
			}
			for i, message := range messages {
				if message.MessageID != history[i].MessageID || message.MessageSeq != history[i].MessageSeq || string(message.Payload) != string(history[i].Payload) {
					return false
				}
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond, "all actual replicas must restore old messages without another send")
	stored, err := loadRecoveryTestConfig(t, leader, ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, cfg.ConfVersion, stored.ConfVersion, "catch-up must not churn metadata")
}

func TestDormantChannelLeaderKeepsCaughtUpReplicasIdle(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	leader := servers[0]
	id := "dormant-caught-up-leader"
	for leader.cfgServer.SlotLeaderId(leader.getSlotId(id)) != 1 {
		id += "x"
	}
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 1,
		Term: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}
	version, err := saveRecoveryTestConfig(t, leader, ctx, cfg)
	require.NoError(t, err)
	cfg.ConfVersion = version
	message := consistencyMessage(1)
	message.ChannelID = id
	for _, s := range servers {
		require.NoError(t, s.db.AppendMessages(id, 2, []wkdb.Message{message}))
	}
	_, err = leader.reconcileLocalChannelConfig(ctx, cfg)
	require.NoError(t, err)
	for _, s := range servers {
		require.False(t, s.channelServer.ExistChannel(id, 2), "caught-up dormant replicas must stay dormant")
	}
	stored, err := loadRecoveryTestConfig(t, leader, ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, cfg.ConfVersion, stored.ConfVersion)
}

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
			version, err := saveRecoveryTestConfig(t, owner, ctx, cfg)
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
				stored, loadErr := loadRecoveryTestConfig(t, owner, ctx, id, 2)
				require.NoError(t, loadErr)
				require.Equal(t, cfg.ConfVersion, stored.ConfVersion, "partial probes cannot authorize an empty leader")
			} else if mode == "empty-control" || mode == "nonempty-control" {
				require.NoError(t, err)
				stored, loadErr := loadRecoveryTestConfig(t, owner, ctx, id, 2)
				require.NoError(t, loadErr)
				require.Equal(t, uint64(2), stored.LeaderId)
				require.Equal(t, cfg.ConfVersion, stored.ConfVersion)
			} else {
				require.ErrorIs(t, err, raft.ErrConfigVersionStale)
				stored, loadErr := loadRecoveryTestConfig(t, owner, ctx, id, 2)
				require.NoError(t, loadErr)
				require.Equal(t, uint64(1), stored.LeaderId, "the surviving data-bearing voter must lead")
				require.Greater(t, stored.Term, cfg.Term)
				require.Greater(t, stored.ConfVersion, cfg.ConfVersion)
			}
			require.False(t, empty.channelServer.ExistChannel(id, 2), "an empty returning leader must not accept new writes before recovery")
		})
	}
}

// Recovery is reconciled asynchronously in production. Retry only documented
// transient leadership/apply barriers while setting up and observing fixtures;
// the recovery decision and message-history assertions remain single and strict.
func retryRecoveryTestBarrier(t *testing.T, fn func() error) error {
	t.Helper()
	var err error
	require.Eventually(t, func() bool {
		err = fn()
		return err == nil || !errors.Is(err, ErrConversationReadRetry) && !errors.Is(err, rafttype.ErrNotLeader)
	}, 3*time.Second, 10*time.Millisecond)
	return err
}

func saveRecoveryTestConfig(t *testing.T, s *Server, ctx context.Context, cfg wkdb.ChannelClusterConfig) (uint64, error) {
	t.Helper()
	var version uint64
	err := retryRecoveryTestBarrier(t, func() error { var err error; version, err = s.saveChannelConfig(ctx, cfg); return err })
	return version, err
}

func loadRecoveryTestConfig(t *testing.T, s *Server, ctx context.Context, id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
	t.Helper()
	var cfg wkdb.ChannelClusterConfig
	err := retryRecoveryTestBarrier(t, func() error { var err error; cfg, err = s.loadConversationConfig(ctx, id, typ); return err })
	return cfg, err
}
