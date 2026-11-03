package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// Drop only the joining node's outgoing synchronization requests. All other
// traffic uses real TCP; these gates make interruption timing reproducible.
type expansionGroupGate struct {
	base             raftgroup.ITransport
	pause            atomic.Bool
	maxConfigVersion atomic.Uint64 // nonzero: hold acknowledgement of a later membership
	capConfigVersion atomic.Uint64 // retain log sync while delaying installed-config acknowledgement
	holdLogTo        atomic.Uint64 // bit mask: withhold data responses to selected followers
}

func (g *expansionGroupGate) Send(key string, e rafttypes.Event) {
	if e.Type == rafttypes.SyncResp && len(e.Logs) > 0 && g.holdLogTo.Load()&(1<<e.To) != 0 {
		return
	}
	if e.Type == rafttypes.SyncReq {
		if limit := g.maxConfigVersion.Load(); g.pause.Load() || limit != 0 && e.ConfigVersion > limit {
			return
		}
		if limit := g.capConfigVersion.Load(); limit != 0 && e.ConfigVersion > limit {
			e.ConfigVersion = limit
		}
	}
	g.base.Send(key, e)
}

type expansionConfigGate struct {
	base  *nodeTransport
	pause atomic.Bool
}

func (g *expansionConfigGate) Send(e rafttypes.Event) {
	if g.pause.Load() && e.Type == rafttypes.SyncReq {
		return
	}
	g.base.Send(e)
}

type expansionFixture struct {
	t           *testing.T
	ctx         context.Context
	addresses   map[uint64]string
	dirs        map[uint64]string
	servers     map[uint64]*Server
	slots       map[uint64]*expansionGroupGate
	channels    map[uint64]*expansionGroupGate
	configs     map[uint64]*expansionConfigGate
	channelIDs  []string
	messages    int
	pongMaxTick int
}

func newExpansionFixture(t *testing.T) *expansionFixture { return newExpansionFixtureWithPong(t, 10) }

func newExpansionFixtureWithPong(t *testing.T, pong int) *expansionFixture {
	t.Helper()
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	f := &expansionFixture{t: t, ctx: ctx, pongMaxTick: pong, addresses: map[uint64]string{}, dirs: map[uint64]string{}, servers: map[uint64]*Server{},
		slots: map[uint64]*expansionGroupGate{}, channels: map[uint64]*expansionGroupGate{}, configs: map[uint64]*expansionConfigGate{}}
	for id := uint64(1); id <= 5; id++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		f.addresses[id] = listener.Addr().String()
		require.NoError(t, listener.Close())
		f.dirs[id] = t.TempDir()
	}
	t.Cleanup(func() {
		for id := uint64(1); id <= 5; id++ {
			f.stop(id)
		}
	})
	for id := uint64(1); id <= 3; id++ {
		f.start(id, "", false, false)
	}
	f.waitStable([]uint64{1, 2, 3}, 3)
	for i := 0; i < 12; i++ {
		f.channelIDs = append(f.channelIDs, fmt.Sprintf("expansion-recovery-%d", i))
	}
	f.send(1, 4)
	f.checkMessages([]uint64{1, 2, 3})
	return f
}
func (f *expansionFixture) start(id uint64, seed string, pauseConfig, pauseData bool) {
	f.t.Helper()
	init := map[uint64]string{1: f.addresses[1], 2: f.addresses[2], 3: f.addresses[3]}
	if seed != "" {
		init = nil
	}
	cfg := clusterconfig.NewOptions(clusterconfig.WithNodeId(id), clusterconfig.WithInitNodes(init), clusterconfig.WithSlotCount(8),
		clusterconfig.WithConfigPath(filepath.Join(f.dirs[id], "cfg", "cluster.json")), clusterconfig.WithSlotMaxReplicaCount(3),
		clusterconfig.WithServerAddr(f.addresses[id]), clusterconfig.WithSeed(seed), clusterconfig.WithPongMaxTick(f.pongMaxTick))
	sg, cg, ng := &expansionGroupGate{}, &expansionGroupGate{}, &expansionConfigGate{}
	sg.pause.Store(pauseData)
	cg.pause.Store(pauseData)
	ng.pause.Store(pauseConfig)
	s := New(NewOptions(WithAddr("tcp://"+f.addresses[id]), WithServerAddr(f.addresses[id]), WithSeed(seed), WithConfigOptions(cfg),
		WithDataDir(f.dirs[id]), WithSlotTransport(sg), WithChannelTransport(cg), WithDBWKDbShardNum(1), WithDBSlotShardNum(1),
		WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
	sg.base, cg.base, ng.base = newSlotTransport(s), newChannelTransport(s), newNodeTransport(s)
	cfg.Transport = ng
	f.servers[id], f.slots[id], f.channels[id], f.configs[id] = s, sg, cg, ng
	require.NoError(f.t, s.Start())
}
func (f *expansionFixture) stop(id uint64) {
	if s := f.servers[id]; s != nil {
		s.Stop()
		delete(f.servers, id)
	}
}
func (f *expansionFixture) live() uint64 {
	for _, id := range []uint64{1, 2, 3, 4} {
		if f.servers[id] != nil {
			return id
		}
	}
	panic("no live node")
}
func (f *expansionFixture) configLeader() uint64 {
	for id, s := range f.servers {
		if s.cfgServer.IsLeader() {
			return id
		}
	}
	return 0
}
func (f *expansionFixture) waitOffline(victim uint64) {
	f.t.Helper()
	require.Eventually(f.t, func() bool {
		for _, s := range f.servers {
			cfg := s.cfgServer.Snapshot()
			for _, n := range cfg.Nodes {
				if n.Id == victim && n.Online {
					return false
				}
			}
			for _, st := range cfg.Slots {
				if st.Leader == victim || st.Status != nodetypes.SlotStatus_SlotStatusNormal {
					return false
				}
			}
		}
		return f.configLeader() != 0
	}, 15*time.Second, 25*time.Millisecond)
}
func (f *expansionFixture) waitStable(ids []uint64, members int) {
	f.t.Helper()
	defer func() {
		if f.t.Failed() {
			f.dump()
		}
	}()
	require.Eventually(f.t, func() bool {
		for _, id := range ids {
			s := f.servers[id]
			if s == nil {
				return false
			}
			cfg := s.cfgServer.Snapshot()
			if len(cfg.Nodes) != members || len(cfg.Learners) != 0 || len(cfg.Slots) != 8 {
				return false
			}
			for _, n := range cfg.Nodes {
				if !n.Online || n.Status != nodetypes.NodeStatus_NodeStatusJoined {
					return false
				}
			}
			for _, st := range cfg.Slots {
				if len(st.Replicas) != 3 || len(st.Learners) != 0 || st.MigrateFrom != 0 || st.MigrateTo != 0 || st.Status != nodetypes.SlotStatus_SlotStatusNormal {
					return false
				}
				for _, replica := range st.Replicas {
					if f.servers[replica] == nil {
						return false
					}
				}
				state, err := s.slotServer.ReadLeaderState(f.ctx, st.Id)
				local := false
				for _, replica := range st.Replicas {
					local = local || replica == id
				}
				if err != nil || local && (!state.Exists || state.LeaderID != st.Leader || st.Leader == id && !state.Ready) || !local && state.Exists {
					return false
				}
			}
		}
		return true
	}, 25*time.Second, 25*time.Millisecond, "committed assignments, live membership and local runtime roles must agree")
}
func (f *expansionFixture) send(from uint64, count int) {
	f.t.Helper()
	for seq := f.messages + 1; seq <= f.messages+count; seq++ {
		for _, id := range f.channelIDs {
			var err error
			require.Eventually(f.t, func() bool {
				ctx, cancel := context.WithTimeout(f.ctx, time.Second)
				defer cancel()
				_, err = f.servers[from].store.AppendMessages(ctx, id, 2, []wkdb.Message{{RecvPacket: wkproto.RecvPacket{ChannelID: id, ChannelType: 2, FromUID: "expansion-test", MessageID: int64(seq), ClientMsgNo: fmt.Sprintf("%s-%d", id, seq), Payload: []byte(fmt.Sprintf("payload-%s-%d", id, seq))}}})
				return err == nil
			}, 8*time.Second, 25*time.Millisecond, "message %s/%d: %v", id, seq, err)
		}
	}
	f.messages += count
}
func (f *expansionFixture) checkMessages(ids []uint64) {
	f.t.Helper()
	defer func() {
		if f.t.Failed() {
			f.dump()
		}
	}()
	require.Eventually(f.t, func() bool {
		for _, id := range f.channelIDs {
			cfg, err := f.servers[ids[0]].loadConversationConfig(f.ctx, id, 2)
			if err != nil || len(cfg.Replicas) != 3 || len(cfg.Learners) != 0 || cfg.MigrateTo != 0 || cfg.MigrateFrom != 0 {
				return false
			}
			for _, replica := range cfg.Replicas {
				peer := f.servers[replica]
				if peer == nil {
					return false
				}
				msgs, err := peer.db.LoadLastMsgs(id, 2, 100)
				if err != nil || len(msgs) != f.messages {
					return false
				}
				for i, msg := range msgs {
					if msg.MessageID != int64(i+1) || msg.MessageSeq != uint32(i+1) || string(msg.Payload) != fmt.Sprintf("payload-%s-%d", id, i+1) {
						return false
					}
				}
			}
		}
		return true
	}, 20*time.Second, 25*time.Millisecond, "all configured message replicas must persist IDs, sequences and payloads")
}

func TestExpansionRecoveryOverTCP(t *testing.T) {
	for _, killLeader := range []bool{false, true} {
		t.Run(fmt.Sprintf("leader_failure_%t", killLeader), func(t *testing.T) {
			f := newExpansionFixture(t)
			victim := f.configLeader()
			if !killLeader {
				for id := uint64(1); id <= 3; id++ {
					if id != victim {
						victim = id
						break
					}
				}
			}
			f.stop(victim)
			f.waitOffline(victim)
			f.send(f.live(), 2)
			seed := f.live()
			f.start(4, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, false)
			var ids []uint64
			for id := uint64(1); id <= 4; id++ {
				if id != victim {
					ids = append(ids, id)
				}
			}
			f.waitStable(ids, 3)
			f.checkMessages(ids)
			for _, s := range f.servers {
				require.Nil(t, s.cfgServer.Node(victim))
				require.Nil(t, s.nodeManager.node(victim))
			}
			f.send(4, 2)
			f.checkMessages(ids)
			// Restart the removed bootstrap node with its original disk and no
			// new seed. Its periodic join probe must discover and rejoin ABD.
			f.start(victim, "", false, false)
			f.waitStable([]uint64{1, 2, 3, 4}, 4)
			f.send(victim, 2)
			f.checkMessages([]uint64{1, 2, 3, 4})
		})
	}
}

func TestExpansionRecoveryCRejoinsBeforeDPromotion(t *testing.T) {
	f := newExpansionFixture(t)
	victim := uint64(3)
	f.stop(victim)
	f.waitOffline(victim)
	seed := f.live()
	f.start(4, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), true, false)
	require.Eventually(t, func() bool {
		n := f.servers[seed].cfgServer.Node(4)
		return n != nil && n.Status == nodetypes.NodeStatus_NodeStatusWillJoin
	}, 5*time.Second, 25*time.Millisecond)
	f.start(victim, "", false, false)
	require.Eventually(t, func() bool { return f.servers[seed].cfgServer.NodeIsOnline(victim) }, 8*time.Second, 25*time.Millisecond)
	f.configs[4].pause.Store(false)
	f.waitStable([]uint64{1, 2, 3, 4}, 4)
	f.send(4, 2)
	f.checkMessages([]uint64{1, 2, 3, 4})
}

func TestExpansionRecoveryResumesInterruptedDataSync(t *testing.T) {
	f := newExpansionFixture(t)
	victim := uint64(3)
	f.stop(victim)
	f.waitOffline(victim)
	seed := f.live()
	seedAddr := fmt.Sprintf("%d@%s", seed, f.addresses[seed])
	f.start(4, seedAddr, false, true)
	require.Eventually(t, func() bool {
		cfg := f.servers[seed].cfgServer.Snapshot()
		if f.servers[seed].cfgServer.Node(victim) != nil {
			return false
		}
		for _, st := range cfg.Slots {
			if len(st.Learners) != 1 || st.Learners[0] != 4 || st.MigrateFrom != victim {
				return false
			}
		}
		return len(cfg.Slots) == 8
	}, 10*time.Second, 25*time.Millisecond)
	// Persisted tasks survive a learner restart. C returns while the learner
	// is still blocked, and a controller restart must not erase those tasks.
	f.stop(4)
	f.start(4, seedAddr, false, true)
	f.start(victim, "", false, false)
	require.Eventually(t, func() bool { n := f.servers[seed].cfgServer.Node(victim); return n != nil && n.Online }, 10*time.Second, 25*time.Millisecond)
	leader := f.configLeader()
	require.NotZero(t, leader)
	f.stop(leader)
	require.Eventually(t, func() bool { return f.configLeader() != 0 }, 8*time.Second, 25*time.Millisecond)
	f.start(leader, "", false, leader == 4)
	f.slots[4].pause.Store(false)
	f.channels[4].pause.Store(false)
	f.waitStable([]uint64{1, 2, 3, 4}, 4)
	f.send(4, 2)
	f.checkMessages([]uint64{1, 2, 3, 4})
}

func (f *expansionFixture) dump() {
	for id, s := range f.servers {
		data, _ := json.Marshal(s.cfgServer.Snapshot())
		f.t.Logf("node %d config %s", id, data)
		for _, ch := range f.channelIDs {
			cfg, err := s.db.GetChannelClusterConfig(ch, 2)
			msgs, merr := s.db.LoadLastMsgs(ch, 2, 100)
			var ids []string
			for _, m := range msgs {
				ids = append(ids, fmt.Sprintf("%d/%d", m.MessageID, m.MessageSeq))
			}
			f.t.Logf("node %d channel %s cfg %+v err %v msgs %v err %v", id, ch, cfg, err, ids, merr)
		}
	}
}

func TestExpansionRecoveryRetriesWithAnotherAddedNode(t *testing.T) {
	f := newExpansionFixture(t)
	f.stop(3)
	f.waitOffline(3)
	seed := f.live()
	f.start(4, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, true)
	require.Eventually(t, func() bool {
		cfg := f.servers[seed].cfgServer.Snapshot()
		if f.servers[seed].cfgServer.Node(3) != nil {
			return false
		}
		for _, st := range cfg.Slots {
			if st.MigrateTo != 4 {
				return false
			}
		}
		return len(cfg.Slots) == 8
	}, 10*time.Second, 25*time.Millisecond)
	f.stop(4)
	f.waitOffline(4)
	f.start(5, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, false)
	f.waitStable([]uint64{1, 2, 5}, 3)
	f.checkMessages([]uint64{1, 2, 5})
	f.send(5, 2)
	f.checkMessages([]uint64{1, 2, 5})
	for _, s := range f.servers {
		require.Nil(t, s.cfgServer.Node(3))
		require.Nil(t, s.cfgServer.Node(4))
		require.Nil(t, s.nodeManager.node(3))
		require.Nil(t, s.nodeManager.node(4))
	}
}

func TestExpansionRecoveryRetiredNodeReturnsWithEmptyDisk(t *testing.T) {
	f := newExpansionFixture(t)
	f.stop(3)
	f.waitOffline(3)
	seed := f.live()
	f.start(4, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, false)
	f.waitStable([]uint64{1, 2, 4}, 3)
	f.checkMessages([]uint64{1, 2, 4})
	// Keep the original bootstrap addresses but discard every local database.
	// The joining node must discover its retirement and recover as a learner.
	f.dirs[3] = t.TempDir()
	f.start(3, "", false, false)
	f.waitStable([]uint64{1, 2, 3, 4}, 4)
	f.send(3, 2)
	f.checkMessages([]uint64{1, 2, 3, 4})
}

func TestExpansionRecoveryJoinBeforeHeartbeatTimeout(t *testing.T) {
	f := newExpansionFixtureWithPong(t, 80)
	f.stop(3)
	seed := f.live()
	f.start(4, fmt.Sprintf("%d@%s", seed, f.addresses[seed]), false, false)
	require.Eventually(t, func() bool {
		cfg := f.servers[seed].cfgServer.Snapshot()
		onlineC, joinedD := false, false
		for _, n := range cfg.Nodes {
			if n.Id == 3 {
				onlineC = n.Online
			}
			if n.Id == 4 {
				joinedD = n.Status == nodetypes.NodeStatus_NodeStatusJoined
			}
		}
		return len(cfg.Nodes) == 4 && onlineC && joinedD
	}, 8*time.Second, 25*time.Millisecond, "D must first join while C is still classified online")
	f.waitOffline(3)
	f.waitStable([]uint64{1, 2, 4}, 3)
	f.checkMessages([]uint64{1, 2, 4})
	f.send(4, 2)
	f.checkMessages([]uint64{1, 2, 4})
	for _, s := range f.servers {
		require.Nil(t, s.cfgServer.Node(3))
		require.Nil(t, s.nodeManager.node(3))
	}
}

func TestExpansionRecoveryRetriesAfterConfigurationLearnerCrash(t *testing.T) {
	f := newExpansionFixture(t)
	f.stop(3)
	f.waitOffline(3)
	seed := f.live()
	seedAddr := fmt.Sprintf("%d@%s", seed, f.addresses[seed])
	f.start(4, seedAddr, true, false)
	require.Eventually(t, func() bool {
		n := f.servers[seed].cfgServer.Node(4)
		return n != nil && n.Status == nodetypes.NodeStatus_NodeStatusWillJoin
	}, 5*time.Second, 25*time.Millisecond)
	f.stop(4)
	require.Eventually(t, func() bool { return f.servers[seed].cfgServer.Node(4) == nil }, 10*time.Second, 25*time.Millisecond, "a dead unpromoted learner must not block future joins")
	f.start(5, seedAddr, false, false)
	f.waitStable([]uint64{1, 2, 5}, 3)
	f.checkMessages([]uint64{1, 2, 5})
	f.send(5, 2)
	f.checkMessages([]uint64{1, 2, 5})
}
