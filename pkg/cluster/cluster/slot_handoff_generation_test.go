package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/require"
)

type slotGenerationConfigTransport struct {
	peers map[uint64]*clusterconfig.Server
}

func (tr *slotGenerationConfigTransport) Send(e types.Event) {
	if peer := tr.peers[e.To]; peer != nil {
		peer.StepRaftEvent(e)
	}
}

type slotGenerationDataTransport struct {
	peers  map[uint64]*slot.Server
	paused atomic.Bool
}

func (tr *slotGenerationDataTransport) Send(key string, e types.Event) {
	if tr.paused.Load() && (e.From == 4 || e.To == 4) {
		return
	}
	if peer := tr.peers[e.To]; peer != nil {
		peer.AddEvent(key, e)
	}
}

// Delay the real asynchronous slot SaveConfig side effect, not just its
// response. Configuration proposals run through a live four-voter Raft group;
// slot storage/workers are real Pebble implementations. Only transport timing
// is controlled, so D cannot acquire X before the obsolete worker is released.
func TestSlotHandoffSaveConfigRejectsRestartedGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ct := &slotGenerationConfigTransport{peers: map[uint64]*clusterconfig.Server{}}
	dt := &slotGenerationDataTransport{peers: map[uint64]*slot.Server{}}
	entered := make(chan types.Config, 1)
	release := make(chan struct{})
	returned := make(chan error, 1)
	unblock := sync.OnceFunc(func() { close(release) })
	var requests atomic.Int32
	t.Cleanup(func() {
		unblock()
		for _, s := range dt.peers {
			s.Stop()
		}
		for _, s := range ct.peers {
			s.Stop()
		}
	})
	addresses := map[uint64]string{1: "127.0.0.1:12001", 2: "127.0.0.1:12002", 3: "127.0.0.1:12003", 4: "127.0.0.1:12004"}
	initial := &nodetypes.Config{SlotCount: 1, SlotReplicaCount: 1, ChannelReplicaCount: 1,
		Slots: []*nodetypes.Slot{{Id: 0, Term: 1, Leader: 1, Replicas: []uint64{1}}}}
	for id := uint64(1); id <= 4; id++ {
		ct.peers[id] = clusterconfig.New(clusterconfig.NewOptions(clusterconfig.WithNodeId(id), clusterconfig.WithInitNodes(addresses),
			clusterconfig.WithTransport(ct), clusterconfig.WithSlotCount(1), clusterconfig.WithSlotMaxReplicaCount(1),
			clusterconfig.WithConfigPath(filepath.Join(t.TempDir(), "cluster.json"))))
		initial.Nodes = append(initial.Nodes, &nodetypes.Node{Id: id, ClusterAddr: addresses[id], Online: true, AllowVote: true,
			Status: nodetypes.NodeStatus_NodeStatusJoined})
	}
	for _, id := range []uint64{1, 4} {
		// This is the actual cluster callback authority, with its live config
		// Raft. Other cluster services are unnecessary for raw slot log writes.
		authority := &Server{cfgServer: ct.peers[id], Log: wklog.NewWKLog(fmt.Sprintf("generation-authority-%d", id))}
		save := func(slotID uint32, next types.Config) error {
			if id == 1 && requests.Add(1) == 1 {
				entered <- next.Clone()
				<-release
				err := authority.onSaveSlotConfig(slotID, next)
				returned <- err
				return err
			}
			return authority.onSaveSlotConfig(slotID, next)
		}
		s := slot.NewServer(slot.NewOptions(slot.WithNodeId(id), slot.WithDataDir(t.TempDir()), slot.WithSlotDbShardNum(1),
			slot.WithNode(ct.peers[id]), slot.WithTransport(dt), slot.WithOnApply(func(uint32, []types.Log) error { return nil }),
			slot.WithOnSaveConfig(save)))
		authority.slotServer = s
		dt.peers[id] = s
		ct.peers[id].AddEventListener(s)
	}
	for _, s := range dt.peers {
		require.NoError(t, s.Start())
	}
	for id := uint64(1); id <= 4; id++ {
		require.NoError(t, ct.peers[id].Start())
	}
	var configLeader *clusterconfig.Server
	require.Eventually(t, func() bool {
		for _, s := range ct.peers {
			if s.IsLeader() {
				configLeader = s
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, configLeader.ProposeConfig(initial))
	a, d := dt.peers[1], dt.peers[4]
	require.Eventually(t, func() bool {
		state, err := a.ReadLeaderState(ctx, 0)
		return err == nil && state.Ready && state.LeaderID == 1
	}, time.Second, 5*time.Millisecond)
	_, err := a.ProposeUntilApplied(0, []byte("before-handoff"))
	require.NoError(t, err)
	require.NoError(t, configLeader.ProposeMigrateSlot(0, 1, 4))
	var old types.Config
	select {
	case old = <-entered:
	case <-ctx.Done():
		t.Fatal("the full-tail handoff did not reach the real SaveConfig worker")
	}
	require.Equal(t, uint64(4), old.Leader)
	tail, err := d.LastIndex(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), tail)
	state, err := a.ReadLeaderState(ctx, 0)
	require.NoError(t, err)
	require.False(t, state.Ready)
	originalTerm := state.Term
	require.Equal(t, originalTerm+1, old.Term, "the prepared new-leader configuration advances the original term")

	// D's data path fails. Commit the same cancellation the maintenance
	// planner uses, retaining the original slot leader and term.
	dt.paused.Store(true)
	current := configLeader.Slot(0)
	require.Equal(t, originalTerm, current.Term)
	cancelled := current.Clone()
	cancelled.Learners, cancelled.MigrateFrom, cancelled.MigrateTo = nil, 0, 0
	require.NoError(t, configLeader.ProposeSlotsIfCurrent([]*nodetypes.Slot{current}, []*nodetypes.Slot{cancelled}))
	require.Eventually(t, func() bool {
		state, err := a.ReadLeaderState(ctx, 0)
		dState, dErr := d.ReadLeaderState(ctx, 0)
		return err == nil && state.Ready && state.LeaderID == 1 && state.Term == originalTerm && state.ConfigVersion > old.Version &&
			dErr == nil && !dState.Exists
	}, time.Second, 5*time.Millisecond)
	_, err = a.ProposeUntilApplied(0, []byte("X-committed-after-cancellation"))
	require.NoError(t, err)
	x, err := a.LastLog(0)
	require.NoError(t, err)
	require.Equal(t, uint64(2), x.Index)
	require.Equal(t, []byte("X-committed-after-cancellation"), x.Data)
	applied, err := a.AppliedIndex(0)
	require.NoError(t, err)
	require.Equal(t, x.Index, applied)

	// Re-create A->D at the same term, but keep D behind X. The old prepared
	// result has the same membership shape; only its originating task differs.
	require.NoError(t, configLeader.ProposeMigrateSlot(0, 1, 4))
	require.Eventually(t, func() bool {
		cfg := ct.peers[1].Slot(0)
		state, err := a.ReadLeaderState(ctx, 0)
		dState, dErr := d.ReadLeaderState(ctx, 0)
		return cfg != nil && cfg.MigrateFrom == 1 && cfg.MigrateTo == 4 && cfg.Term == originalTerm &&
			err == nil && state.ConfigVersion > old.Version && dErr == nil && dState.Exists
	}, time.Second, 5*time.Millisecond)
	tail, err = d.LastIndex(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), tail, "D must still be missing committed entry X")
	unblock()
	var saveErr error
	select {
	case saveErr = <-returned:
	case <-ctx.Done():
		t.Fatal("the obsolete SaveConfig did not finish its real conditional proposal")
	}
	installed := ct.peers[1].Slot(0)
	t.Logf("old request version=%d; controller version=%d; stale SaveConfig error=%v; installed=%v; A persisted X=%d; D tail=%d",
		old.Version, ct.peers[1].Snapshot().Version, saveErr, installed, x.Index, tail)
	require.Error(t, saveErr, "an old worker must not publish D using the restarted task's assignment")
	require.Equal(t, uint64(1), installed.Leader)
	require.Equal(t, uint64(4), installed.MigrateTo)
	require.Equal(t, []uint64{1}, installed.Replicas)

	// An unrelated config log entry advances the global version without
	// replacing this slot's task. Its genuine SaveConfig must still succeed.
	state, err = a.ReadLeaderState(ctx, 0)
	require.NoError(t, err)
	taskVersion, taskGeneration := state.ConfigVersion, installed.Generation
	globalVersion := ct.peers[1].Snapshot().Version
	require.NoError(t, configLeader.ProposeApiServerAddr(2, "http://127.0.0.1:15002"))
	require.Eventually(t, func() bool {
		return ct.peers[1].Snapshot().Version > globalVersion
	}, time.Second, 5*time.Millisecond)
	require.Equal(t, "http://127.0.0.1:15002", ct.peers[1].Node(2).ApiServerAddr)
	require.Equal(t, taskGeneration, ct.peers[1].Slot(0).Generation)
	state, err = a.ReadLeaderState(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, taskVersion, state.ConfigVersion)
	require.Greater(t, ct.peers[1].Snapshot().Version, state.ConfigVersion)

	// The current generation can finish after actual catch-up. Its surviving
	// sole voter must retain X and commit another write through the same path.
	dt.paused.Store(false)
	require.Eventually(t, func() bool {
		cfg := configLeader.Slot(0)
		state, err := d.ReadLeaderState(ctx, 0)
		return cfg != nil && cfg.Leader == 4 && len(cfg.Learners) == 0 && cfg.MigrateFrom == 0 && cfg.MigrateTo == 0 &&
			cfg.Term == originalTerm+1 && err == nil && state.Ready && state.LeaderID == 4 && state.Term == cfg.Term
	}, 5*time.Second, 10*time.Millisecond)
	xOnD, err := d.LastLog(0)
	require.NoError(t, err)
	require.Equal(t, x.Index, xOnD.Index)
	require.Equal(t, x.Data, xOnD.Data)
	_, err = d.ProposeUntilApplied(0, []byte("after-current-handoff"))
	require.NoError(t, err)
	logs, err := d.GetLogsInReverseOrder(0, 1, 4, 0)
	require.NoError(t, err)
	require.Len(t, logs, 3)
	require.Equal(t, []byte("after-current-handoff"), logs[0].Data)
	require.Equal(t, x.Data, logs[1].Data)
	require.Equal(t, []byte("before-handoff"), logs[2].Data)
	require.GreaterOrEqual(t, requests.Load(), int32(2))
}
