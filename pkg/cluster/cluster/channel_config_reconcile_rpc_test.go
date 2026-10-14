package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func newConfigRPCServers(t *testing.T, count int) ([]*Server, context.Context) {
	t.Helper()
	return newConfigRPCServersWithSlots(t, count, count)
}

func newConfigRPCServersWithSlots(t *testing.T, count, slotCount int) ([]*Server, context.Context) {
	t.Helper()
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	addresses := make(map[uint64]string)
	for id := uint64(1); id <= uint64(count); id++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[id] = listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	var servers []*Server
	for id := uint64(1); id <= uint64(count); id++ {
		config := newTestOptions(t, id, addresses, clusterconfig.WithSlotCount(uint32(slotCount)), clusterconfig.WithSlotMaxReplicaCount(uint32(count)))
		s := New(NewOptions(WithAddr("tcp://"+addresses[id]), WithConfigOptions(config), WithDataDir(t.TempDir()),
			WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
		require.NoError(t, s.Start())
		t.Cleanup(s.Stop)
		// Control all hints in this test, while leaving the real RPC handlers and
		// applied metadata barrier running on both TCP peers.
		s.configReconciler.stop()
		servers = append(servers, s)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	for _, s := range servers {
		require.NoError(t, s.WaitAllSlotReady(ctx, slotCount))
	}
	return servers, ctx
}

func TestChannelConfigReconcileRPC(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 2)
	owner, leader := servers[0], servers[1]
	var id string
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("reconcile-rpc-%d", i)
		if owner.cfgServer.SlotLeaderId(owner.getSlotId(candidate)) == 1 {
			id = candidate
			break
		}
	}
	require.NotEmpty(t, id)
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 2, Term: 1, ConfVersion: 1, Replicas: []uint64{2}}
	require.NoError(t, owner.db.SaveChannelClusterConfig(cfg))
	require.NoError(t, owner.requestChannelConfigReconcile(ctx, cfg))
	require.False(t, leader.channelServer.ExistChannel(id, 2), "ordinary dormant channel must stay dormant")
	require.NoError(t, leader.channelServer.WakeLeaderIfNeed(cfg))
	old := cfg.Clone()
	cfg.ConfVersion = 2
	require.NoError(t, owner.db.SaveChannelClusterConfig(cfg))
	require.ErrorIs(t, owner.requestChannelConfigReconcile(ctx, old), ErrConversationReadRetry, "stale hint must reload metadata")
	require.NoError(t, owner.requestChannelConfigReconcile(ctx, cfg))
	state, err := leader.channelServer.ReadLeaderState(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.ConfigVersion)
	// The next attempt follows the new message leader; delayed requests to the
	// old leader cannot install their stale routing snapshot.
	old = cfg.Clone()
	cfg.ConfVersion, cfg.LeaderId, cfg.Term, cfg.Replicas = 3, 1, 2, []uint64{1, 2}
	require.NoError(t, owner.db.SaveChannelClusterConfig(cfg))
	require.ErrorIs(t, owner.requestChannelConfigReconcile(ctx, old), ErrConversationReadRetry)
	require.NoError(t, owner.channelServer.WakeLeaderIfNeed(cfg))
	require.NoError(t, leader.reconcileChannelConfig(ctx, channelConfigKey{id, 2}))
	require.NoError(t, owner.reconcileChannelConfig(ctx, channelConfigKey{id, 2}))
	state, err = owner.channelServer.ReadLeaderState(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(3), state.ConfigVersion)
	require.Equal(t, uint64(1), state.LeaderID)

	// A different channel exercises the callback to its remote slot owner,
	// including recovery of a durable term which is ahead of metadata.
	id += "-term"
	for owner.cfgServer.SlotLeaderId(owner.getSlotId(id)) != 1 {
		id += "x"
	}
	cfg = wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 2, Term: 1, Replicas: []uint64{2}}
	version, err := owner.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	cfg.ConfVersion = version
	require.NoError(t, leader.db.SaveRaftHardState(wkutil.ChannelToKey(id, 2), rafttype.HardState{Term: 5}))
	require.NoError(t, leader.channelServer.WakeLeaderIfNeed(cfg))
	require.ErrorIs(t, owner.requestChannelConfigReconcile(ctx, cfg), ErrConversationReadRetry)
	recovered, err := owner.loadConversationConfig(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(6), recovered.Term)
	require.Greater(t, recovered.ConfVersion, cfg.ConfVersion)
	require.NoError(t, owner.requestChannelConfigReconcile(ctx, recovered))
	require.NoError(t, leader.ValidateLocalChannelRead(ctx, recovered))
	_, err = leader.saveChannelConfigTransition(ctx, id, 2, rafttype.Config{
		Version: cfg.ConfVersion, Term: cfg.Term, Leader: 2, Replicas: []uint64{2},
	})
	require.ErrorIs(t, err, ErrConversationReadRetry, "a delayed remote callback cannot regress the recovered metadata")

	info, err := owner.rpcClient.RequestChannelLastLogInfo(ctx, 2, id, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(6), info.Term, "remote election probes include the recovered runtime/hard-state term")
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	leader.Route("/rpc/channel/lastLogInfo", func(c *wkserver.Context) {
		close(entered)
		<-release
		c.WriteErr(ErrConversationReadRetry)
	})
	probeCtx, stopProbe := context.WithCancel(ctx)
	defer stopProbe()
	done := make(chan error, 1)
	go func() {
		_, err := owner.requestChannelLastLogInfos(probeCtx, []uint64{2}, id, 2)
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("election probe did not reach peer")
	}
	stopProbe()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancelled election must not wait for the peer's independent RPC timeout")
	}
}

func TestConfigElectionProbePreservesQuorumBeforeWorkerDeadline(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	slow := servers[2]
	entered, release := make(chan struct{}, 4), make(chan struct{})
	defer close(release)
	slow.Route("/rpc/channel/lastLogInfo", func(c *wkserver.Context) {
		entered <- struct{}{}
		<-release
		c.WriteErr(ErrConversationReadRetry)
	})
	owner := servers[0]
	id := "slow-election"
	for owner.cfgServer.SlotLeaderId(owner.getSlotId(id)) != 1 {
		id += "x"
	}
	// Wait out initial slot/config publication before the measured failure.
	// WaitAllSlotReady alone does not freeze concurrent startup transitions.
	var cfg wkdb.ChannelClusterConfig
	require.Eventually(t, func() bool {
		var err error
		cfg, err = owner.GetOrCreateChannelClusterConfigFromSlotLeader(id, 2)
		return err == nil && len(cfg.Replicas) == 3
	}, 5*time.Second, 10*time.Millisecond)
	workerCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	infos, err := owner.requestChannelLastLogInfos(workerCtx, cfg.Replicas, id, 2)
	require.NoError(t, err, "the slow probe must expire before the enclosing worker")
	require.NoError(t, workerCtx.Err())
	require.Len(t, entered, 1, "the peer must accept the request and stall, not fail fast")
	require.Len(t, infos, 2)
	next := cfg.Clone()
	changed, err := electChannelLeader(&next, infos)
	require.NoError(t, err)
	require.True(t, changed)
	version, err := owner.saveChannelConfig(workerCtx, next)
	require.NoError(t, err, "the same worker budget must still allow publication")
	require.Greater(t, version, cfg.ConfVersion)
	minority := cfg.Clone()
	_, err = electChannelLeader(&minority, map[uint64]*ChannelLastLogInfoResponse{1: infos[1]})
	require.ErrorIs(t, err, ErrConversationReadRetry, "partial results never bypass quorum")
	require.Equal(t, cfg, minority)
}
