package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	clustertype "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func orphanMigrationConfig(target uint64) wkdb.ChannelClusterConfig {
	id := fmt.Sprintf("orphan-migration-%d", target)
	return wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 1, Term: 1,
		ReplicaMaxCount: 3, Replicas: []uint64{1}, MigrateFrom: target, MigrateTo: target,
		Status: wkdb.ChannelClusterStatusNormal}
}

// WaitAllSlotReady only checks that a leader exists. Setup also needs the
// published slot ownership and the actual Raft roles to agree on every peer.
func readyOrphanMigrationOwner(ctx context.Context, servers []*Server, id string) *Server {
	var leader uint64
	var owner *Server
	for _, peer := range servers {
		if len(peer.cfgServer.AllowVoteAndJoinedOnlineNodes()) != len(servers) {
			return nil
		}
		slotID := peer.getSlotId(id)
		slot := peer.cfgServer.Slot(slotID)
		if slot == nil || slot.Leader == 0 || len(slot.Replicas) != len(servers) ||
			len(slot.Learners) != 0 || slot.MigrateFrom != 0 || slot.MigrateTo != 0 ||
			slot.Status != clustertype.SlotStatus_SlotStatusNormal {
			return nil
		}
		if leader == 0 {
			leader = slot.Leader
		}
		state, err := peer.slotServer.ReadLeaderState(ctx, slotID)
		if err != nil || slot.Leader != leader || !state.Exists || state.LeaderID != leader || state.Term != slot.Term {
			return nil
		}
		if peer.opts.ConfigOptions.NodeId == leader {
			if !state.Ready {
				return nil
			}
			owner = peer
		}
	}
	return owner
}

func saveOrphanMigrationConfig(t *testing.T, ctx context.Context, servers []*Server, cfg wkdb.ChannelClusterConfig) (*Server, wkdb.ChannelClusterConfig) {
	t.Helper()
	var owner *Server
	retryable := func(err error) bool {
		return errors.Is(err, ErrConversationReadRetry) || errors.Is(err, rafttype.ErrNotLeader) ||
			errors.Is(err, raft.ErrConfigVersionStale) || errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
	}
	require.Eventually(t, func() bool {
		attemptCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		owner = readyOrphanMigrationOwner(attemptCtx, servers, cfg.ChannelId)
		if owner == nil {
			return false
		}
		// A previous attempt may have committed before losing its response.
		// Reload through the current owner's applied barrier before re-proposing.
		current, err := owner.loadConversationConfigLocal(attemptCtx, cfg.ChannelId, cfg.ChannelType)
		if retryable(err) {
			return false
		}
		if !errors.Is(err, wkdb.ErrNotFound) {
			require.NoError(t, err)
		}
		if !wkdb.IsEmptyChannelClusterConfig(current) {
			expected := cfg.Clone()
			expected.Id, expected.ConfVersion = current.Id, current.ConfVersion
			require.True(t, expected.Equal(current), "setup must retain the intended orphan state")
			cfg = current
			return true
		}
		version, err := owner.saveChannelConfig(attemptCtx, cfg)
		if retryable(err) {
			return false
		}
		require.NoError(t, err)
		cfg.ConfVersion = version
		return true
	}, 5*time.Second, 20*time.Millisecond, "seed orphan metadata through its current ready slot owner")
	return owner, cfg
}

func TestConfigOrphanMigrationRecoversWithoutSend(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	messageLeader := servers[0]
	var configs []wkdb.ChannelClusterConfig
	// Cover both reported targets and a target no longer in the cluster. Repair
	// must choose eligible nodes, not blindly restore the old target as a learner.
	for _, target := range []uint64{2, 3, 99} {
		_, cfg := saveOrphanMigrationConfig(t, ctx, servers, orphanMigrationConfig(target))
		msg := consistencyMessage(1)
		msg.ChannelID = cfg.ChannelId
		require.NoError(t, messageLeader.db.AppendMessages(cfg.ChannelId, cfg.ChannelType, []wkdb.Message{msg}))
		configs = append(configs, cfg)
	}
	// Discover persisted interrupted expansion through the real background scan;
	// there is no foreground send, read hint, or manual marker cleanup.
	// Every server runs maintenance in production. A rebalance must not leave
	// the new slot owner without a worker, even though the message leader is 1.
	for _, peer := range servers {
		peer.initChannelConfigReconciler()
		peer.configReconciler.start()
	}
	require.Eventually(t, func() bool {
		for _, cfg := range configs {
			current, err := messageLeader.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
			if err != nil || len(current.Replicas) != 3 || len(current.Learners) != 0 ||
				current.MigrateFrom != 0 || current.MigrateTo != 0 {
				return false
			}
		}
		return true
	}, 6*time.Second, 50*time.Millisecond)
	for _, cfg := range configs {
		current, err := messageLeader.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
		require.NoError(t, err)
		require.ElementsMatch(t, []uint64{1, 2, 3}, current.Replicas)
		require.Greater(t, current.ConfVersion, cfg.ConfVersion)
		for _, peer := range servers {
			messages, err := peer.db.LoadLastMsgs(cfg.ChannelId, cfg.ChannelType, 10)
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.Equal(t, int64(1), messages[0].MessageID)
			require.Equal(t, uint32(1), messages[0].MessageSeq)
			require.Equal(t, []byte("diagnostic"), messages[0].Payload)
		}
	}
}

func TestConfigOrphanMigrationPreservesOtherTransitions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas []uint64
		learners []uint64
		from, to uint64
	}{
		{"active expansion", []uint64{1}, []uint64{2}, 2, 2},
		{"other learner pending", []uint64{1}, []uint64{3}, 2, 2},
		{"orphan learner", []uint64{1}, []uint64{2}, 0, 0},
		{"leader transfer", []uint64{1, 2}, nil, 1, 2},
		{"replica replacement", []uint64{1}, nil, 1, 2},
		{"different absent endpoints", []uint64{1}, nil, 2, 3},
		{"source only", []uint64{1}, nil, 2, 0},
		{"target only", []uint64{1}, nil, 0, 2},
		{"target already a replica", []uint64{1, 2}, nil, 2, 2},
		{"already full", []uint64{1, 2, 3}, nil, 99, 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := wkdb.ChannelClusterConfig{ReplicaMaxCount: 3, Replicas: tc.replicas,
				Learners: tc.learners, MigrateFrom: tc.from, MigrateTo: tc.to}
			before := cfg.Clone()
			changed, err := (&Server{}).joinNewRepliceIfNeed(&cfg)
			require.NoError(t, err)
			require.False(t, changed)
			require.Equal(t, before, cfg)
		})
	}
}

func TestConfigOrphanMigrationConcurrentRepairIsFenced(t *testing.T) {
	// One slot cannot be exported by the automatic leader balancer. Isolate
	// version competition from bootstrap ownership churn, keeping three real
	// nodes and the strict stale-version rejection below.
	servers, ctx := newConfigRPCServersWithSlots(t, 3, 1)
	owner, cfg := saveOrphanMigrationConfig(t, ctx, servers, orphanMigrationConfig(2))
	start, results := make(chan struct{}), make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			_, err := owner.maintainChannelConfig(ctx, cfg)
			results <- err
		}()
	}
	close(start)
	winners := 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, raft.ErrConfigVersionStale)
		}
	}
	require.Equal(t, 1, winners)
	current, err := owner.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, current.Replicas)
	require.ElementsMatch(t, []uint64{2, 3}, current.Learners)
	require.Contains(t, current.Learners, current.MigrateTo)
	require.Equal(t, current.MigrateTo, current.MigrateFrom)
	require.Greater(t, current.ConfVersion, cfg.ConfVersion)
	// A delayed callback from before repair must not erase the new learners.
	_, err = owner.saveChannelConfigTransition(ctx, cfg.ChannelId, cfg.ChannelType, rafttype.Config{
		Version: cfg.ConfVersion, Term: cfg.Term, Leader: cfg.LeaderId, Replicas: []uint64{1, 2},
	})
	require.ErrorIs(t, err, raft.ErrConfigVersionStale)
	after, err := owner.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, current, after)
	require.Empty(t, cfg.Learners, "maintenance must not mutate its input snapshot")
}
