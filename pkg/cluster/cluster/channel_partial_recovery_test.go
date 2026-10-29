package cluster

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestDormantChannelLeaderRestoresPartiallyLaggingReplicas(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	leader := servers[0]
	id := "dormant-partial-recovery"
	for leader.cfgServer.SlotLeaderId(leader.getSlotId(id)) != 1 {
		id += "x"
	}
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 1, Term: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}
	version, err := saveRecoveryTestConfig(t, leader, ctx, cfg)
	require.NoError(t, err)
	cfg.ConfVersion = version
	var history []wkdb.Message
	for seq := uint32(1); seq <= 4; seq++ {
		m := consistencyMessage(seq)
		m.ChannelID = id
		history = append(history, m)
	}
	for i, s := range servers {
		n := len(history)
		if i > 0 {
			n = 2
		}
		require.NoError(t, s.db.AppendMessages(id, 2, history[:n]))
	}
	wake, err := leader.recoverEmptyChannelLeader(ctx, cfg)
	require.NoError(t, err)
	require.True(t, wake, "term=1/index=4 must wake for nonempty replicas at index=2")
	// The production background path must restore actual messages, not merely
	// return the right decision. Metadata publication can transiently need retry.
	require.Eventually(t, func() bool { _, err := leader.reconcileLocalChannelConfig(ctx, cfg); return err == nil }, 3*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		for _, s := range servers {
			got, err := s.db.LoadLastMsgs(id, 2, 100)
			if err != nil || len(got) != len(history) {
				return false
			}
			for i, m := range got {
				if m.MessageID != history[i].MessageID || m.MessageSeq != history[i].MessageSeq || m.Term != history[i].Term || string(m.Payload) != string(history[i].Payload) {
					return false
				}
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	stored, err := loadRecoveryTestConfig(t, leader, ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, version, stored.ConfVersion, "repair must not rewrite channel authority")
}

func TestDormantChannelLeaderDoesNotTreatHighTermAsLag(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "high-term-caught-up", ChannelType: 2, LeaderId: 1, Term: 100, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}
	for _, s := range servers {
		var history []wkdb.Message
		for seq := uint32(1); seq <= 2; seq++ {
			m := consistencyMessage(seq)
			m.ChannelID = cfg.ChannelId
			m.Term = 100
			history = append(history, m)
		}
		require.NoError(t, s.db.AppendMessages(cfg.ChannelId, 2, history))
	}
	wake, err := servers[0].recoverEmptyChannelLeader(ctx, cfg)
	require.NoError(t, err)
	require.False(t, wake, "term=100 must not make caught-up replicas at index=2 appear behind")
}
