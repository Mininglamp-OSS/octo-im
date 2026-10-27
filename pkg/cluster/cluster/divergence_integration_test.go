package cluster

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// In-process three-node cluster on loopback TCP and temporary Pebble
// directories. Nothing outside the test process is touched.

const divSlotCount = 4

type divNode struct {
	id      uint64
	dir     string
	addr    string
	api     *httptest.Server
	handler atomic.Value // http.Handler
	s       *Server
	stopped bool
}

type divCluster struct {
	t     *testing.T
	nodes map[uint64]*divNode
	addrs map[uint64]string
}

func newDivCluster(t *testing.T) *divCluster {
	t.Helper()
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	c := &divCluster{t: t, nodes: map[uint64]*divNode{}, addrs: map[uint64]string{}}
	for id := uint64(1); id <= 3; id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		c.addrs[id] = l.Addr().String()
		require.NoError(t, l.Close())
		n := &divNode{id: id, dir: t.TempDir(), addr: c.addrs[id]}
		n.api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h, _ := n.handler.Load().(http.Handler)
			if h == nil {
				http.Error(w, "starting", http.StatusServiceUnavailable)
				return
			}
			h.ServeHTTP(w, r)
		}))
		t.Cleanup(n.api.Close)
		c.nodes[id] = n
	}
	for id := uint64(1); id <= 3; id++ {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			if !n.stopped {
				_ = n.s.Stop()
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, n := range c.nodes {
		require.NoError(t, n.s.WaitAllSlotReady(ctx, divSlotCount))
	}
	c.waitAPIAddrs()
	return c
}

func (c *divCluster) start(id uint64) {
	n := c.nodes[id]
	cfg := clusterconfig.NewOptions(
		clusterconfig.WithNodeId(id),
		clusterconfig.WithInitNodes(c.addrs),
		clusterconfig.WithConfigPath(path.Join(n.dir, "cluster.json")),
		clusterconfig.WithSlotCount(divSlotCount),
		clusterconfig.WithSlotMaxReplicaCount(3),
		clusterconfig.WithChannelMaxReplicaCount(3),
		clusterconfig.WithPongMaxTick(5),
		clusterconfig.WithApiServerAddr(n.api.URL),
	)
	s := New(NewOptions(WithAddr("tcp://"+n.addr), WithConfigOptions(cfg), WithDataDir(n.dir),
		WithDBWKDbShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
	route := wkhttp.New()
	s.ServerAPI(route, "/cluster")
	n.handler.Store(http.Handler(route.GetGinRoute()))
	require.NoError(c.t, s.Start())
	n.s = s
	n.stopped = false
}

func (c *divCluster) stop(id uint64) {
	n := c.nodes[id]
	require.NoError(c.t, n.s.Stop())
	n.stopped = true
}

func (c *divCluster) waitAPIAddrs() {
	require.Eventually(c.t, func() bool {
		for _, n := range c.nodes {
			if n.stopped {
				continue
			}
			for id := range c.nodes {
				node := n.s.cfgServer.Node(id)
				if node == nil || node.ApiServerAddr == "" {
					return false
				}
			}
		}
		return true
	}, 20*time.Second, 20*time.Millisecond)
}

func (c *divCluster) any() *Server {
	for _, n := range c.nodes {
		if !n.stopped {
			return n.s
		}
	}
	c.t.Fatal("no running node")
	return nil
}

// roles returns the slot leader of slotId and the two other replicas.
func (c *divCluster) roles(slotId uint32) (leader, f1, f2 uint64) {
	leader = c.any().cfgServer.SlotLeaderId(slotId)
	require.NotZero(c.t, leader)
	for id := range c.nodes {
		if id == leader {
			continue
		}
		if f1 == 0 {
			f1 = id
		} else {
			f2 = id
		}
	}
	if f1 > f2 {
		f1, f2 = f2, f1
	}
	return
}

// key returns a name owned by slotId.
func (c *divCluster) key(prefix string, slotId uint32) string {
	s := c.any()
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("%s-%d", prefix, i)
		if s.getSlotId(k) == slotId {
			return k
		}
	}
	c.t.Fatalf("no key for slot %d", slotId)
	return ""
}

func (c *divCluster) waitReplicated(id uint64, check func(*Server) bool) {
	require.Eventually(c.t, func() bool { return check(c.nodes[id].s) }, 15*time.Second, 10*time.Millisecond)
}

func subscribed(channelId string, uid string) func(*Server) bool {
	return func(s *Server) bool {
		ok, err := s.db.ExistSubscriber(channelId, wkproto.ChannelTypeGroup, uid)
		return err == nil && ok
	}
}

// writeDivergenceMarker marks slotId diverged in a stopped node's slot log
// storage, exactly as a refused truncation would have.
func writeDivergenceMarker(t *testing.T, dir string, slotId uint32) {
	t.Helper()
	storage := slot.NewPebbleShardLogStorage(nil, slot.SlotLogDBPath(path.Join(dir, "cluster")), uint32(slot.NewOptions().SlotDbShardNum))
	require.NoError(t, storage.Open())
	require.NoError(t, storage.MarkDiverged(slot.SlotIdToKey(slotId), rafttypes.ErrTruncateBelowApplied))
	require.NoError(t, storage.Close())
}

func httpStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

// A follower replica that opens with a durable divergence marker (the
// reachable path: restart after a refused truncation).
func TestDivergenceQuarantinedFollowerAtOpen(t *testing.T) {
	c := newDivCluster(t)
	const S, H = uint32(1), uint32(2)
	L, F, T := c.roles(S)
	chS, chH := c.key("div-s", S), c.key("div-h", H)
	uS := c.key("div-u", S)
	leader := c.nodes[L].s

	require.NoError(t, leader.store.AddSubscribers(chS, wkproto.ChannelTypeGroup, []wkdb.Member{{Uid: "alice"}}))
	require.NoError(t, leader.store.AddSubscribers(chH, wkproto.ChannelTypeGroup, []wkdb.Member{{Uid: "bob"}}))
	require.NoError(t, leader.store.AddUser(wkdb.User{Uid: uS}))
	_, err := leader.GetOrCreateChannelClusterConfigFromSlotLeader(chS, wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	c.waitReplicated(F, subscribed(chS, "alice"))
	c.waitReplicated(F, subscribed(chH, "bob"))

	// Restart F with the marker for S.
	c.stop(F)
	writeDivergenceMarker(t, c.nodes[F].dir, S)
	c.start(F)
	f := c.nodes[F].s
	require.Eventually(t, func() bool { return f.slotServer.GetSlotRaft(S) != nil }, 20*time.Second, 10*time.Millisecond)
	require.Equal(t, slot.ReplicaQuarantined, f.slotServer.ReplicaState(S))
	require.Equal(t, slot.ReplicaHealthy, f.slotServer.ReplicaState(H))
	require.Equal(t, slot.ReplicaQuarantined, f.slotServer.DivergenceStats().Slots[S])

	t.Run("T25 reads of the quarantined slot fail on that node", func(t *testing.T) {
		_, err := f.store.GetSubscribers(chS, wkproto.ChannelTypeGroup)
		require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
		_, err = f.store.ExistSubscriber(chS, wkproto.ChannelTypeGroup, "alice")
		require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
		_, err = f.store.GetUser(uS)
		require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
	})
	t.Run("T25b other slots on the same node serve", func(t *testing.T) {
		m, err := f.store.GetSubscribers(chH, wkproto.ChannelTypeGroup)
		require.NoError(t, err)
		require.Len(t, m, 1)
	})
	t.Run("proposal fence on the quarantined node", func(t *testing.T) {
		_, err := f.slotServer.ProposeUntilApplied(S, []byte("x"))
		require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
		require.ErrorIs(t, err, rafttypes.ErrProposalNotAdmitted)
	})
	t.Run("T7 the healthy majority keeps committing", func(t *testing.T) {
		require.NoError(t, leader.store.AddSubscribers(chS, wkproto.ChannelTypeGroup, []wkdb.Member{{Uid: "carol"}}))
		c.waitReplicated(T, subscribed(chS, "carol"))
		before, err := f.slotServer.LastIndex(S)
		require.NoError(t, err)
		time.Sleep(300 * time.Millisecond)
		after, err := f.slotServer.LastIndex(S)
		require.NoError(t, err)
		require.Equal(t, before, after, "the quarantined replica stores nothing")
		ok, err := f.db.ExistSubscriber(chS, wkproto.ChannelTypeGroup, "carol")
		require.NoError(t, err)
		require.False(t, ok, "the quarantined replica applies nothing")
	})
	t.Run("T25e-3 channel config still loads from the healthy slot leader", func(t *testing.T) {
		cfg, err := f.LoadOnlyChannelClusterConfig(chS, wkproto.ChannelTypeGroup)
		require.NoError(t, err)
		require.Equal(t, chS, cfg.ChannelId)
		_, err = f.GetOrCreateChannelClusterConfigFromSlotLeader(chS, wkproto.ChannelTypeGroup)
		require.NoError(t, err)
	})
	t.Run("T25e-3 the node gives no channel election info for the slot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := f.getChannelLastLogInfo(ctx, chS, wkproto.ChannelTypeGroup)
		require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
		_, err = leader.rpcClient.RequestChannelLastLogInfo(ctx, F, chS, wkproto.ChannelTypeGroup)
		require.ErrorIs(t, err, rafttypes.ErrSlotDiverged, "typed across the RPC")
		infos, err := leader.requestChannelLastLogInfos(ctx, []uint64{L, F, T}, chS, wkproto.ChannelTypeGroup)
		require.NoError(t, err)
		require.NotContains(t, infos, F)
	})
	t.Run("T10 slot election excludes the replica on its own report", func(t *testing.T) {
		resps, err := leader.requestSlotInfos(map[uint64][]uint32{L: {S}, F: {S}, T: {S}})
		require.NoError(t, err)
		require.Len(t, resps, 3)
		for _, r := range resps {
			for _, si := range r.Slots {
				require.Equal(t, SlotInfoExtValid, si.ExtState)
				require.Equal(t, r.NodeId == F, si.Diverged)
			}
		}
		infos, err := leader.collectSlotLogInfo(resps)
		require.NoError(t, err)
		require.NotContains(t, infos[F], S)
		require.Contains(t, infos[L], S)
		require.Contains(t, infos[T], S)
	})
	t.Run("D6 concurrent election info collection is race-free", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resps, err := leader.requestSlotInfos(map[uint64][]uint32{L: {S, H}, F: {S, H}, T: {S, H}})
				require.NoError(t, err)
				require.Len(t, resps, 3)
			}()
		}
		wg.Wait()
	})
}

// injectLocalStorageRefusal is an in-process-only test seam: it hands the
// slot raft a storage result that carries the local, never-encoded storage
// verdict, as raftgroup does after a refused truncation. It cannot be
// produced from the network. No storage refusal and no marker write happen;
// those are covered by the slot storage and raftgroup tests, and the
// marker-at-open path by TestDivergenceQuarantinedFollowerAtOpen.
func injectLocalStorageRefusal(s *Server, slotId uint32) {
	s.AddSlotEvent(slot.SlotIdToKey(slotId), rafttypes.Event{Type: rafttypes.TruncateResp, Reason: rafttypes.ReasonDiverged}.
		WithLocalStorageVerdict(rafttypes.StorageVerdictDiverged))
}

// The slot leader replica is quarantined. A running leader cannot reach
// this through storage (it never truncates and only appends at its tail),
// so the transition comes from injectLocalStorageRefusal. Everything after
// the injection runs unmodified code. Coverage limit: the leader writes no
// marker and runs no storage refusal in this test.
func TestDivergenceQuarantinedSlotLeader(t *testing.T) {
	c := newDivCluster(t)
	const S, H = uint32(3), uint32(0)
	L, F, T := c.roles(S)
	leader := c.nodes[L].s
	chS1, chS2, chH := c.key("lead-s1", S), c.key("lead-s2", S), c.key("lead-h", H)
	chNew := c.key("lead-new", S)

	require.NoError(t, leader.store.AddSubscribers(chS1, wkproto.ChannelTypeGroup, []wkdb.Member{{Uid: "alice"}}))
	cfgS1, err := leader.GetOrCreateChannelClusterConfigFromSlotLeader(chS1, wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	// Start the channel raft of chS1 on its channel leader.
	chLeader := c.nodes[cfgS1.LeaderId].s
	var msgID atomic.Int64
	msgID.Store(time.Now().UnixNano())
	appendMsg := func(s *Server, ch string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		id := msgID.Add(1)
		_, err := s.store.AppendMessages(ctx, ch, wkproto.ChannelTypeGroup, []wkdb.Message{{Term: 1, RecvPacket: wkproto.RecvPacket{
			MessageID: id, ChannelID: ch, ChannelType: wkproto.ChannelTypeGroup, FromUID: "alice",
			ClientMsgNo: fmt.Sprintf("%s-%d", ch, id), Payload: []byte("m")}}})
		return err
	}
	require.NoError(t, appendMsg(chLeader, chS1))
	c.waitReplicated(F, subscribed(chS1, "alice"))
	c.waitReplicated(T, subscribed(chS1, "alice"))
	slotLast := func() map[uint64]uint64 {
		out := map[uint64]uint64{}
		for id, n := range c.nodes {
			if n.stopped {
				continue
			}
			idx, err := n.s.slotServer.LastIndex(S)
			require.NoError(t, err)
			out[id] = idx
		}
		return out
	}

	injectLocalStorageRefusal(leader, S)
	require.Eventually(t, func() bool { return leader.slotServer.ReplicaState(S) == slot.ReplicaQuarantined }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		r := leader.slotServer.GetSlotRaft(S)
		return r != nil && r.Diverged() && !r.IsLeader()
	}, 5*time.Second, time.Millisecond)
	require.EqualValues(t, L, leader.cfgServer.SlotLeaderId(S), "no automatic failover (K1 deferred)")
	before := slotLast()

	t.Run("T25e-1 channel config reads fail and nothing is created", func(t *testing.T) {
		_, err := leader.LoadOnlyChannelClusterConfig(chS1, wkproto.ChannelTypeGroup)
		require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
		_, err = leader.GetOrCreateChannelClusterConfigFromSlotLeader(chNew, wkproto.ChannelTypeGroup)
		require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
		for _, id := range []uint64{F, T} {
			s := c.nodes[id].s
			_, err = s.LoadOnlyChannelClusterConfig(chS1, wkproto.ChannelTypeGroup)
			require.ErrorIs(t, err, rafttypes.ErrSlotDiverged, "remote caller gets an error, not an empty config")
			_, err = s.GetOrCreateChannelClusterConfigFromSlotLeader(chNew, wkproto.ChannelTypeGroup)
			require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
			_, err = s.rpcClient.RequestGetChannelClusterConfig(L, chS1, wkproto.ChannelTypeGroup)
			require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
		}
		for _, n := range c.nodes {
			_, err := n.s.db.GetChannelClusterConfig(chNew, wkproto.ChannelTypeGroup)
			require.ErrorIs(t, err, wkdb.ErrNotFound, "no config created on node %d", n.id)
		}
	})
	t.Run("cost C every channel-leader lookup for the slot fails cluster-wide", func(t *testing.T) {
		for _, n := range c.nodes {
			_, err := n.s.LeaderOfChannel(chS1, wkproto.ChannelTypeGroup)
			require.Error(t, err)
			_, err = n.s.LeaderIdOfChannel(chS1, wkproto.ChannelTypeGroup)
			require.Error(t, err)
			_, err = n.s.LeaderOfChannelForRead(chS1, wkproto.ChannelTypeGroup)
			require.Error(t, err)
			_, err = n.s.LeaderOfChannel(chH, wkproto.ChannelTypeGroup)
			require.NoError(t, err, "other slots are unaffected")
		}
	})
	t.Run("T25e-2 running channel rafts keep serving, new ones cannot start", func(t *testing.T) {
		require.NoError(t, appendMsg(chLeader, chS1), "already-running channel raft")
		for _, n := range c.nodes {
			require.Error(t, appendMsg(n.s, chS2), "new channel in the slot on node %d", n.id)
		}
		require.NoError(t, appendMsg(c.nodes[F].s, chH), "a channel in another slot starts")
	})
	t.Run("T25e-4 slot-leader list handlers fail closed and forwarders relay it", func(t *testing.T) {
		for _, list := range []string{"subscribers", "denylist", "allowlist"} {
			p := leader.formatPath(fmt.Sprintf("/channels/%s/%d/%s", chS1, wkproto.ChannelTypeGroup, list))
			for _, n := range c.nodes {
				require.NotEqual(t, http.StatusOK, httpStatus(t, n.api.URL+p), "%s via node %d", list, n.id)
			}
			ph := leader.formatPath(fmt.Sprintf("/channels/%s/%d/%s", chH, wkproto.ChannelTypeGroup, list))
			require.Equal(t, http.StatusOK, httpStatus(t, c.nodes[F].api.URL+ph))
		}
	})
	t.Run("T25e-6 a config save fails before any proposal", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cfg := cfgS1
		cfg.Term++
		_, err := leader.saveChannelConfig(ctx, cfg)
		require.ErrorIs(t, err, rafttypes.ErrSlotDiverged)
		_, err = c.nodes[F].s.slotServer.ProposeUntilApplied(S, []byte("x"))
		require.Error(t, err, "proposals for the slot have no writer")
		require.NoError(t, c.nodes[F].s.store.AddSubscribers(chH, wkproto.ChannelTypeGroup, []wkdb.Member{{Uid: "dan"}}))
		require.Equal(t, before, slotLast(), "zero entries proposed to the slot")
	})

	// Manual Candidate (the existing admin path; in-process only). With the
	// quarantined leader excluded and one healthy replica offline there is
	// no healthy quorum: no leader is elected (T7).
	c.stop(T)
	require.Eventually(t, func() bool {
		n := leader.cfgServer.Node(T)
		return n != nil && !n.Online
	}, 20*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		return leader.cfgServer.ProposeSlotStatusChange(S, types.SlotStatus_SlotStatusCandidate) == nil
	}, 20*time.Second, 100*time.Millisecond)
	time.Sleep(2 * time.Second)
	st := leader.cfgServer.Slot(S)
	require.NotEqual(t, F, st.Leader, "no election without a healthy quorum")
	start := time.Now()
	_, err = c.nodes[F].s.LeaderOfChannel(chS1, wkproto.ChannelTypeGroup)
	require.Error(t, err)
	require.Less(t, time.Since(start), 11*time.Second, "fails within the request budget, no hang")

	// With the healthy replica back, the election excludes the quarantined
	// replica and moves leadership with a higher term.
	termBefore := st.Term
	c.start(T)
	require.Eventually(t, func() bool {
		s := c.nodes[F].s.cfgServer.Slot(S)
		return s != nil && s.Leader != L && s.Leader != 0 && s.Status == types.SlotStatus_SlotStatusNormal
	}, 30*time.Second, 50*time.Millisecond)
	newLeader := c.nodes[F].s.cfgServer.Slot(S)
	require.Greater(t, newLeader.Term, termBefore)
	require.Contains(t, []uint64{F, T}, newLeader.Leader)
	nl := c.nodes[newLeader.Leader].s
	require.Eventually(t, func() bool {
		m, err := nl.store.GetSubscribers(chS1, wkproto.ChannelTypeGroup)
		return err == nil && len(m) == 1 && m[0].Uid == "alice"
	}, 15*time.Second, 20*time.Millisecond, "acknowledged rows are served by the new leader")
	_, err = leader.store.GetSubscribers(chS1, wkproto.ChannelTypeGroup)
	require.ErrorIs(t, err, rafttypes.ErrSlotDiverged, "the quarantined replica still serves nothing")
}
