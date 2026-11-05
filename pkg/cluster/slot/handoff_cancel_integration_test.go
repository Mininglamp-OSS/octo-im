package slot

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nodeevent "github.com/WuKongIM/WuKongIM/pkg/cluster/node/event"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type handoffConfigTransport struct {
	peers map[uint64]*nodeevent.Server
	dead  *atomic.Uint64
}

func (tr *handoffConfigTransport) Send(e types.Event) {
	if tr.dead.Load()&(1<<e.From|1<<e.To) != 0 {
		return
	}
	if peer := tr.peers[e.To]; peer != nil {
		peer.Step(e)
	}
}

type handoffSlotTransport struct {
	peers map[uint64]*Server
	dead  *atomic.Uint64
}

func (tr *handoffSlotTransport) Send(key string, e types.Event) {
	if tr.dead.Load()&(1<<e.From|1<<e.To) != 0 {
		return
	}
	if peer := tr.peers[e.To]; peer != nil {
		peer.AddEvent(key, e)
	}
}

type handoffUnexpectedElection struct{}

func (handoffUnexpectedElection) OnSlotElection([]*nodetypes.Slot) error {
	return errors.New("the original single-replica leaders must remain online")
}

type heldSlotHandoff struct {
	leader   uint64
	slot     uint32
	returned chan struct{}
}

// Keep three real configuration voters alive when D fails. Individual slots
// still have one voter, so ordinary scale-out uses the leader-to-learner path.
// Both Raft layers, storage workers, heartbeat detection and the cancellation
// planner are production implementations; only transport and SaveConfig timing
// are controlled here, through existing options.
func TestSingleReplicaSlotCancellationAfterHeldHandoff(t *testing.T) {
	for _, staleSuccess := range []bool{false, true} {
		t.Run(fmt.Sprintf("late_success_%t", staleSuccess), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var dead atomic.Uint64
			ct := &handoffConfigTransport{peers: map[uint64]*nodeevent.Server{}, dead: &dead}
			st := &handoffSlotTransport{peers: map[uint64]*Server{}, dead: &dead}
			configs := map[uint64]*clusterconfig.Server{}
			entered := make(chan heldSlotHandoff, 24)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			var stopped [5]bool
			stop := func(id uint64) {
				if !stopped[id] {
					stopped[id] = true
					ct.peers[id].Stop()
					configs[id].Stop()
					st.peers[id].Stop()
				}
			}
			t.Cleanup(func() {
				unblock()
				for id := uint64(1); id <= 4; id++ {
					stop(id)
				}
			})
			initial := map[uint64]string{1: "127.0.0.1:11111", 2: "127.0.0.1:11112", 3: "127.0.0.1:11113"}
			for id := uint64(1); id <= 4; id++ {
				init, seed := initial, ""
				if id == 4 {
					init, seed = nil, "1@127.0.0.1:11111"
				}
				opts := clusterconfig.NewOptions(clusterconfig.WithNodeId(id), clusterconfig.WithInitNodes(init),
					clusterconfig.WithSeed(seed), clusterconfig.WithTransport(ct), clusterconfig.WithSlotCount(12),
					clusterconfig.WithSlotMaxReplicaCount(1), clusterconfig.WithChannelMaxReplicaCount(1),
					clusterconfig.WithPongMaxTick(15), clusterconfig.WithConfigPath(filepath.Join(t.TempDir(), "cluster.json")))
				cfg := clusterconfig.New(opts)
				configs[id] = cfg
				ct.peers[id] = nodeevent.NewServer(handoffUnexpectedElection{}, opts, cfg)
				s := NewServer(NewOptions(WithNodeId(id), WithDataDir(t.TempDir()), WithSlotDbShardNum(1), WithNode(cfg),
					WithTransport(st), WithOnApply(func(uint32, []types.Log) error { return nil }),
					WithOnSaveConfig(func(slotID uint32, next types.Config) error {
						returned := make(chan struct{})
						defer close(returned)
						entered <- heldSlotHandoff{leader: id, slot: slotID, returned: returned}
						<-release
						// Exercise both possible late completions. The committed
						// cancellation must remain authoritative in either case.
						if staleSuccess {
							return nil
						}
						return errors.New("slot membership transition superseded")
					})))
				st.peers[id] = s
				cfg.AddEventListener(s)
			}
			for id := uint64(1); id <= 4; id++ {
				require.NoError(t, st.peers[id].Start())
				require.NoError(t, configs[id].Start())
			}
			for id := uint64(1); id <= 4; id++ {
				require.NoError(t, ct.peers[id].Start())
			}
			var leader *clusterconfig.Server
			require.Eventually(t, func() bool {
				for id := uint64(1); id <= 3; id++ {
					if configs[id].IsLeader() && len(configs[id].Slots()) == 12 {
						leader = configs[id]
						return true
					}
				}
				return false
			}, 5*time.Second, 10*time.Millisecond)
			for _, slot := range leader.Slots() {
				_, err := st.peers[slot.Leader].ProposeUntilApplied(slot.Id, []byte("before-handoff"))
				require.NoError(t, err)
			}
			_, err := leader.ProposeJoinWithVersion(&nodetypes.Node{Id: 4, ClusterAddr: "127.0.0.1:11114", Online: true,
				AllowVote: true, Role: nodetypes.NodeRole_NodeRoleReplica, Status: nodetypes.NodeStatus_NodeStatusWillJoin})
			require.NoError(t, err)
			var held heldSlotHandoff
			select {
			case held = <-entered:
			case <-ctx.Done():
				t.Fatal("the learner did not reach the real asynchronous SaveConfig worker")
			}
			s := st.peers[held.leader]
			original := s.GetSlotRaft(held.slot)
			before, err := s.ReadLeaderState(ctx, held.slot)
			require.NoError(t, err)
			require.Equal(t, held.leader, before.LeaderID)
			require.False(t, before.Ready, "the held handoff must have fenced the original leader")
			learnerTail, err := st.peers[4].LastIndex(held.slot)
			require.NoError(t, err)
			require.Equal(t, uint64(1), learnerTail, "the learner must have the complete persisted history")
			dead.Store(1 << 4)
			stop(4)
			require.Eventually(t, func() bool {
				cfg := configs[held.leader].Snapshot()
				for _, node := range cfg.Nodes {
					if node.Id == 4 && node.Online {
						return false
					}
				}
				slot := configs[held.leader].Slot(held.slot)
				if slot == nil || len(slot.Learners) != 0 || slot.MigrateFrom != 0 || slot.MigrateTo != 0 || slot.Leader != held.leader {
					return false
				}
				state, err := s.ReadLeaderState(ctx, held.slot)
				return err == nil && state.ConfigVersion >= cfg.Version && state.Term == before.Term
			}, 10*time.Second, 10*time.Millisecond, "heartbeat failure must commit and install the actual planner cancellation")
			unblock()
			select {
			case <-held.returned:
			case <-ctx.Done():
				t.Fatal("the original slot's held SaveConfig did not return")
			}
			require.Same(t, original, s.GetSlotRaft(held.slot), "recovery must not rely on replacing the Raft instance")
			writeCtx, writeCancel := context.WithTimeout(ctx, 3*time.Second)
			defer writeCancel()
			resps, err := s.ProposeUntilAppliedTimeoutForLocal(writeCtx, held.slot, types.ProposeReqSet{{Id: 5001, Data: []byte("after-cancellation")}})
			require.NoError(t, err, "the original sole voter must commit after cancellation without a restart")
			require.Len(t, resps, 1)
			logs, err := s.storage.GetLogs(SlotIdToKey(held.slot), 1, 3, 0)
			require.NoError(t, err)
			require.Len(t, logs, 2)
			require.Equal(t, []byte("before-handoff"), logs[0].Data)
			require.Equal(t, []byte("after-cancellation"), logs[1].Data)
			applied, err := s.AppliedIndex(held.slot)
			require.NoError(t, err)
			require.Equal(t, uint64(2), applied)
			state, err := s.ReadLeaderState(ctx, held.slot)
			require.NoError(t, err)
			require.Equal(t, before.Term, state.Term)
			require.Equal(t, held.leader, state.LeaderID)
			require.Equal(t, uint64(2), state.CommittedIndex)
		})
	}
}

// Load a legal multi-voter state from Pebble: the leader has durable tail 100,
// but only 90 is applied. Learner 4 reports durable 99, so this migration pauses
// proposals before requesting any asynchronous SaveConfig. Voter 2 is retained
// and later supplies the actual replication quorum for the post-cancel write.
func TestSlotNearTailCancellationCommitsWithoutRoleCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var isolated atomic.Uint64
	isolated.Store(1 << 2)
	tr := &handoffSlotTransport{peers: map[uint64]*Server{}, dead: &isolated}
	var saves atomic.Int32
	for _, id := range []uint64{1, 2} {
		s := NewServer(NewOptions(WithNodeId(id), WithDataDir(t.TempDir()), WithSlotDbShardNum(1), WithTransport(tr),
			WithOnApply(func(uint32, []types.Log) error { return nil }), WithOnSaveConfig(func(uint32, types.Config) error {
				saves.Add(1)
				return errors.New("near-tail cancellation must not issue a role request")
			})))
		require.NoError(t, s.storage.Open())
		logs := make([]types.Log, 100)
		for i := range logs {
			logs[i] = types.Log{Index: uint64(i + 1), Term: 1, Data: []byte(fmt.Sprintf("history-%d", i+1))}
		}
		require.NoError(t, s.storage.AppendLogs("1", logs, &types.TermStartIndexInfo{Term: 1, Index: 1}))
		require.NoError(t, s.storage.Apply("1", logs[:90]))
		tr.peers[id] = s
		t.Cleanup(s.Stop)
	}
	for _, s := range tr.peers {
		require.NoError(t, s.raftGroup.Start())
	}
	cfg := &nodetypes.Config{Version: 1, Slots: []*nodetypes.Slot{{Id: 1, Leader: 1, Term: 1, Replicas: []uint64{1, 2, 3},
		Learners: []uint64{4}, MigrateFrom: 1, MigrateTo: 4}}}
	for _, s := range tr.peers {
		s.OnConfigChange(cfg)
	}
	leader := tr.peers[1]
	before := waitRuntimeSlot(t, leader, 1, true)
	require.Equal(t, uint64(90), before.CommittedIndex)
	require.NoError(t, leader.raftGroup.Do(ctx, "1", func(r raftgroup.IRaft) error {
		return r.Step(types.Event{Type: types.SyncReq, From: 4, Term: 1, Index: 100, StoredIndex: 100,
			Reason: types.ReasonOnlySync, ConfigVersion: 1})
	}))
	paused := waitRuntimeSlot(t, leader, 1, false)
	require.Equal(t, uint64(90), paused.CommittedIndex)
	require.Zero(t, saves.Load())
	// Install the cancellation through the same slot configuration listener
	// used by committed configuration logs, keeping its leader and term.
	cancelled := cfg.Clone()
	cancelled.Version++
	cancelled.Slots[0].Learners = nil
	cancelled.Slots[0].MigrateFrom, cancelled.Slots[0].MigrateTo = 0, 0
	for _, s := range tr.peers {
		s.OnConfigChange(cancelled)
	}
	require.Eventually(t, func() bool {
		state, err := leader.ReadLeaderState(ctx, 1)
		return err == nil && state.ConfigVersion == 2
	}, time.Second, time.Millisecond)
	isolated.Store(0)
	resps, err := leader.ProposeUntilAppliedTimeoutForLocal(ctx, 1, types.ProposeReqSet{{Id: 5002, Data: []byte("after-near-tail-cancellation")}})
	require.NoError(t, err, "cancelling a near-tail pause must not wait for a callback that was never requested")
	require.Len(t, resps, 1)
	require.Equal(t, uint64(101), resps[0].Index)
	require.Zero(t, saves.Load())
	state, err := leader.ReadLeaderState(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, before.LeaderID, state.LeaderID)
	require.Equal(t, before.Term, state.Term)
	require.Equal(t, uint64(101), state.CommittedIndex)
	for _, s := range tr.peers {
		require.Eventually(t, func() bool {
			applied, err := s.AppliedIndex(1)
			return err == nil && applied == 101
		}, 3*time.Second, time.Millisecond)
		logs, err := s.storage.GetLogs("1", 101, 102, 0)
		require.NoError(t, err)
		require.Len(t, logs, 1)
		require.Equal(t, []byte("after-near-tail-cancellation"), logs[0].Data)
	}
}
