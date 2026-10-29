package cluster

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

// Observe immutable snapshots on the config apply callback; polling the legacy
// GetClusterConfig API would race its mutable node records during replay.
type recoveryConfigObserver struct {
	latest atomic.Pointer[nodetypes.Config]
}

func (o *recoveryConfigObserver) OnConfigChange(cfg *nodetypes.Config) { o.latest.Store(cfg.Clone()) }

func TestSeededEmptyDiskRejoinCompletesRecoveryOverTCP(t *testing.T) {
	for _, withHistory := range []bool{false, true} {
		name := "unused-slots"
		if withHistory {
			name = "surviving-history"
		}
		t.Run(name, func(t *testing.T) { testSeededEmptyDiskRejoin(t, withHistory) })
	}
}

func testSeededEmptyDiskRejoin(t *testing.T, withHistory bool) {
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	addresses := map[uint64]string{}
	for id := uint64(1); id <= 3; id++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[id] = listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	servers := map[uint64]*Server{}
	observers := map[uint64]*recoveryConfigObserver{}
	start := func(id uint64, seed string) *Server {
		initNodes := addresses
		if seed != "" {
			initNodes = nil
		}
		cfg := newTestOptions(t, id, initNodes, clusterconfig.WithSlotCount(3), clusterconfig.WithSlotMaxReplicaCount(3), clusterconfig.WithServerAddr(addresses[id]), clusterconfig.WithSeed(seed))
		s := New(NewOptions(WithAddr("tcp://"+addresses[id]), WithServerAddr(addresses[id]), WithSeed(seed), WithConfigOptions(cfg), WithDataDir(t.TempDir()), WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
		observer := &recoveryConfigObserver{}
		s.cfgServer.AddEventListener(observer)
		observers[id] = observer
		require.NoError(t, s.Start())
		s.configReconciler.stop()
		servers[id] = s
		t.Cleanup(func() {
			if servers[id] == s {
				s.Stop()
			}
		})
		return s
	}
	for id := uint64(1); id <= 3; id++ {
		start(id, "")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	for id, s := range servers {
		require.Eventually(t, func() bool {
			cfg := observers[id].latest.Load()
			if cfg == nil || len(cfg.Slots) != 3 {
				return false
			}
			for _, st := range cfg.Slots {
				state, err := s.slotServer.ReadLeaderState(ctx, st.Id)
				if err != nil || !state.Exists || state.LeaderID == 0 || st.Leader == id && !state.Ready {
					return false
				}
			}
			return true
		}, 10*time.Second, 10*time.Millisecond)
	}

	// Keep the slots unused. A prefix-only fence must not permanently prevent
	// the replacement from leading a slot that every voter confirms is empty.
	for id, s := range servers {
		for _, st := range observers[id].latest.Load().Slots {
			index, err := s.slotServer.LastIndex(st.Id)
			require.NoError(t, err)
			require.Zero(t, index)
		}
	}

	// Persist a real metadata command on all voters before replacing its owner.
	// The recovery must preserve this prefix before admitting a subsequent write.
	var old wkdb.ChannelClusterConfig
	if withHistory {
		id := "before-emptydisk-rejoin"
		for i := 0; i < 1024; i++ {
			found := false
			for _, st := range observers[1].latest.Load().Slots {
				if st.Id == servers[1].getSlotId(id) && st.Leader == 1 {
					found = true
				}
			}
			if found {
				break
			}
			id += "x"
		}
		old = wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 3, Term: 7, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}
		version, err := saveRecoveryTestConfig(t, servers[1], ctx, old)
		require.NoError(t, err)
		old.ConfVersion = version
		for _, s := range servers {
			require.Eventually(t, func() bool {
				got, err := s.db.GetChannelClusterConfig(id, 2)
				return err == nil && got.ConfVersion == version
			}, 3*time.Second, 10*time.Millisecond)
		}
	}
	servers[1].Stop()
	delete(servers, 1)
	replacement := start(1, fmt.Sprintf("2@%s", addresses[2]))
	require.True(t, replacement.freshSeededStart)
	require.Eventually(t, func() bool {
		cfg := observers[1].latest.Load()
		if cfg == nil {
			return false
		}
		for _, n := range cfg.Nodes {
			if n.Id == 1 && n.Status == nodetypes.NodeStatus_NodeStatusJoined && cfg.Version > 0 {
				for _, st := range cfg.Slots {
					state, err := replacement.slotServer.ReadLeaderState(ctx, st.Id)
					if err != nil || !state.Exists || state.LeaderID == 0 || st.Leader == 1 && !state.Ready {
						return false
					}
				}
				return len(cfg.Slots) == 3
			}
		}
		return false
	}, 25*time.Second, 20*time.Millisecond, "this boot's join RPC and config replay must converge")
	// Use the public metadata proposal path so a successful write also exercises
	// the slot apply decoder, not an arbitrary Raft-only payload.

	if withHistory {
		require.Eventually(t, func() bool {
			got, err := replacement.db.GetChannelClusterConfig(old.ChannelId, 2)
			return err == nil && got.ConfVersion == old.ConfVersion && got.Term == old.Term && got.LeaderId == old.LeaderId
		}, 5*time.Second, 10*time.Millisecond, "the replacement must recover previously committed metadata")
	}
	id := "after-emptydisk-rejoin"
	slotOwner := func(id string) uint64 {
		for _, st := range observers[1].latest.Load().Slots {
			if st.Id == replacement.getSlotId(id) {
				return st.Leader
			}
		}
		return 0
	}

	// Recovery may legitimately elect a survivor instead of the replacement.
	// Exercise whichever owner the real controller published; the slot runtime
	// tests separately force the returning node's all-empty leadership case.

	owner := servers[slotOwner(id)]
	require.NotNil(t, owner)
	channel := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 1, Term: 1, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}

	var version uint64
	require.Eventually(t, func() bool {
		v, err := owner.saveChannelConfig(ctx, channel)
		if err == nil {
			version = v
		}
		return err == nil
	}, 5*time.Second, 20*time.Millisecond)
	require.NotZero(t, version)
	for _, s := range servers {
		require.Eventually(t, func() bool {
			cfg, err := s.db.GetChannelClusterConfig(channel.ChannelId, 2)
			return err == nil && cfg.ConfVersion == version
		}, 3*time.Second, 10*time.Millisecond)
	}
}
