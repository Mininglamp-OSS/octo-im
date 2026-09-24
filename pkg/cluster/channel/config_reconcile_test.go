package channel

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

type configTransport struct{ replies chan types.Event }

func (tr *configTransport) Send(_ string, e types.Event) {
	if e.Type == types.SyncResp {
		tr.replies <- e
	}
}

func TestConfigReconcileIdempotentAndMonotonic(t *testing.T) {
	tr := &configTransport{replies: make(chan types.Event, 128)}
	s := NewServer(NewOptions(WithNodeId(1), WithDB(retryDB(t)), WithGroupCount(1), WithTransport(tr)))
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := wkdb.ChannelClusterConfig{ChannelId: "reconcile", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1, 2}, Term: 1, ConfVersion: 1}
	require.NoError(t, s.WakeLeaderIfNeed(cfg))
	key := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	rg := s.getRaftGroup(key)
	var speed types.Speed
	for i := 0; i < 100; i++ {
		dormant, err := s.ReconcileConfig(ctx, cfg)
		require.NoError(t, err)
		require.False(t, dormant)
		require.NoError(t, rg.Do(ctx, key, func(r raftgroup.IRaft) error {
			return r.Step(types.Event{Type: types.SyncReq, From: 2, To: 1, Term: 1, Index: 1, Reason: types.ReasonOnlySync})
		}))
		select {
		case e := <-tr.replies:
			speed = e.Speed
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// Replaying ConfChange clears replicaSync, preventing empty-sync suspension.
	require.Equal(t, types.SpeedSuspend, speed, "identical configs must preserve replica sync history")
	conflict := cfg.Clone()
	conflict.Replicas = []uint64{1, 3}
	_, err := s.ReconcileConfig(ctx, conflict)
	require.ErrorIs(t, err, ErrConfigConflict)
	newer := cfg.Clone()
	newer.ConfVersion, newer.Term = 2, 2
	_, err = s.ReconcileConfig(ctx, newer)
	require.NoError(t, err)
	_, err = s.ReconcileConfig(ctx, cfg)
	require.ErrorIs(t, err, raft.ErrConfigVersionStale)
	newer.ConfVersion, newer.Term = 3, 1
	_, err = s.ReconcileConfig(ctx, newer)
	require.NoError(t, err)
	_, err = s.ReconcileConfig(ctx, newer)
	require.NoError(t, err, "normalized replay is idempotent")
	state, err := s.ReadLeaderState(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(3), state.ConfigVersion)
	require.Equal(t, uint32(2), state.Term)
}

type configRestoreDB struct {
	wkdb.DB
	cancel        context.CancelFunc
	failHardState atomic.Bool
}

func (d *configRestoreDB) GetLastMsg(id string, typ uint8) (wkdb.Message, error) {
	msg, err := d.DB.GetLastMsg(id, typ)
	if d.cancel != nil {
		d.cancel()
	}
	return msg, err
}

func (d *configRestoreDB) SaveRaftHardState(key string, state types.HardState) error {
	if d.failHardState.Load() {
		return errors.New("injected hard-state failure")
	}
	return d.DB.SaveRaftHardState(key, state)
}

func TestConfigInitializationCancellationDoesNotPublish(t *testing.T) {
	db := &configRestoreDB{DB: retryDB(t)}
	s := NewServer(NewOptions(WithNodeId(1), WithDB(db), WithGroupCount(1), WithTransport(retryNetwork())))
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.cancel = cancel
	cfg := wkdb.ChannelClusterConfig{ChannelId: "cancel-init", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Learners: []uint64{2}, Term: 4, ConfVersion: 4}
	_, err := s.ReconcileConfig(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, s.ExistChannel(cfg.ChannelId, cfg.ChannelType))
	db.cancel = nil
	_, err = s.ReconcileConfig(context.Background(), cfg)
	require.NoError(t, err)
	state, err := s.ReadLeaderState(context.Background(), cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.True(t, state.Ready)
	require.Equal(t, cfg.ConfVersion, state.ConfigVersion)
}

func TestConfigRestoredHigherTermAllowsWakeAndReconcile(t *testing.T) {
	for _, wake := range []bool{false, true} {
		db := retryDB(t)
		cfg := wkdb.ChannelClusterConfig{ChannelId: "higher-term", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Learners: []uint64{2}, Term: 4, ConfVersion: 4}
		key := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
		require.NoError(t, db.SaveRaftHardState(key, types.HardState{Term: 5}))
		s := NewServer(NewOptions(WithNodeId(1), WithDB(db), WithGroupCount(1), WithTransport(retryNetwork())))
		require.NoError(t, s.Start())
		t.Cleanup(s.Stop)
		if wake {
			require.NoError(t, s.WakeLeaderIfNeed(cfg))
		} else {
			_, err := s.ReconcileConfig(context.Background(), cfg)
			require.NoError(t, err)
		}
		state, err := s.ReadLeaderState(context.Background(), cfg.ChannelId, cfg.ChannelType)
		require.NoError(t, err)
		require.True(t, state.Ready)
		require.Equal(t, uint32(5), state.Term)
		require.Equal(t, cfg.ConfVersion, state.ConfigVersion)
		require.NoError(t, s.WakeLeaderIfNeed(cfg), "same normalized version remains applicable")
	}
}

func TestConfigResumeSurvivesHardStateFailure(t *testing.T) {
	db := &configRestoreDB{DB: retryDB(t)}
	msg := retryMessage(1, "alice", "one")
	msg.MessageSeq, msg.Term = 1, 1
	require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{msg}))
	db.failHardState.Store(true)
	s := NewServer(NewOptions(WithNodeId(1), WithDB(db), WithGroupCount(1), WithTransport(retryNetwork())))
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Term: 5, ConfVersion: 4}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, s.createAndApplyConfig(ctx, cfg), context.DeadlineExceeded)
	ch := s.Channel("retry", 2)
	require.NotNil(t, ch, "an applied config must not be rolled back on persistence failure")
	db.failHardState.Store(false)
	require.Eventually(t, func() bool { return s.WakeLeaderIfNeed(cfg) == nil }, time.Second, time.Millisecond)
	state, err := s.ReadLeaderState(context.Background(), "retry", 2)
	require.NoError(t, err)
	require.True(t, state.Ready)
	require.Equal(t, uint64(1), state.CommittedIndex, "retry must resume the stored tail")
	require.Same(t, ch, s.Channel("retry", 2))
	require.Eventually(t, func() bool {
		state, err := s.ReadLeaderState(context.Background(), "retry", 2)
		return err == nil && state.AppliedIndex == 1
	}, time.Second, time.Millisecond, "finish asynchronous storage before closing the test database")
}

func TestConfigReconcileLockCancellation(t *testing.T) {
	s := NewServer(NewOptions(WithNodeId(1), WithDB(retryDB(t)), WithGroupCount(1)))
	cfg := wkdb.ChannelClusterConfig{ChannelId: "locked", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Term: 1, ConfVersion: 1}
	s.wakeLeaderLock.Lock(cfg.ChannelId)
	defer s.wakeLeaderLock.Unlock(cfg.ChannelId)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := s.ReconcileConfig(ctx, cfg)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestConfigReconcileDormant(t *testing.T) {
	s := retryServer(t, 1, retryDB(t), []uint64{1}, retryNetwork())
	ctx := context.Background()
	cfg := wkdb.ChannelClusterConfig{ChannelId: "dormant", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Term: 1, ConfVersion: 1}
	dormant, err := s.ReconcileConfig(ctx, cfg)
	require.NoError(t, err)
	require.True(t, dormant)
	require.False(t, s.ExistChannel(cfg.ChannelId, cfg.ChannelType))
	cfg.Learners, cfg.MigrateFrom, cfg.MigrateTo = []uint64{2}, 2, 2
	dormant, err = s.ReconcileConfig(ctx, cfg)
	require.NoError(t, err)
	require.False(t, dormant)
	require.True(t, s.ExistChannel(cfg.ChannelId, cfg.ChannelType))
	cfg.ChannelId, cfg.LeaderId = "wrong-leader", 2
	_, err = s.ReconcileConfig(ctx, cfg)
	require.ErrorIs(t, err, ErrConfigConflict)
	require.False(t, s.ExistChannel(cfg.ChannelId, cfg.ChannelType))
}

type resumeConfigCluster struct {
	icluster.ICluster
	cfg wkdb.ChannelClusterConfig
}

func (c resumeConfigCluster) GetOrCreateChannelClusterConfigFromSlotLeader(string, uint8) (wkdb.ChannelClusterConfig, error) {
	return c.cfg, nil
}

type temporaryHardStateDB struct {
	wkdb.DB
	fail   atomic.Bool
	failed chan struct{}
}

func (d *temporaryHardStateDB) SaveRaftHardState(key string, state types.HardState) error {
	if d.fail.Load() {
		select {
		case d.failed <- struct{}{}:
		default:
		}
		return errors.New("temporary hard-state outage")
	}
	return d.DB.SaveRaftHardState(key, state)
}

func TestConfigForegroundWaitsForResume(t *testing.T) {
	for _, mode := range []string{"wake", "switch", "send", "send-local"} {
		t.Run(mode, func(t *testing.T) {
			db := &temporaryHardStateDB{DB: retryDB(t), failed: make(chan struct{}, 1)}
			msg := retryMessage(1, "alice", "one")
			msg.MessageSeq, msg.Term = 1, 1
			require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{msg}))
			cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Term: 5, ConfVersion: 5}
			s := NewServer(NewOptions(WithNodeId(1), WithDB(db), WithGroupCount(1), WithTransport(retryNetwork()), WithCluster(resumeConfigCluster{cfg: cfg})))
			require.NoError(t, s.Start())
			t.Cleanup(s.Stop)
			if mode == "switch" {
				initial := cfg.Clone()
				initial.Term, initial.ConfVersion = 1, 1
				require.NoError(t, s.WakeLeaderIfNeed(initial))
			}
			db.fail.Store(true)
			defer db.fail.Store(false)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			reqs := retryRequests(t, retryMessage(2, "alice", "two"))
			go func() {
				var err error
				switch mode {
				case "wake":
					err = s.WakeLeaderIfNeed(cfg)
				case "switch":
					err = s.SwitchConfig("retry", 2, cfg)
				case "send":
					_, err = s.ProposeBatchUntilAppliedTimeout(ctx, "retry", 2, reqs)
				case "send-local":
					_, err = s.ProposeBatchUntilAppliedTimeoutForLocal(ctx, "retry", 2, reqs)
				}
				done <- err
			}()
			select {
			case <-db.failed:
			case <-ctx.Done():
				t.Fatal("hard-state failure was not reached")
			}
			select {
			case err := <-done:
				t.Fatalf("foreground operation returned before persistence recovered: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			state, err := s.ReadLeaderState(ctx, "retry", 2)
			require.NoError(t, err)
			require.False(t, state.Ready, "waiting must not weaken hard-state/read safety")
			db.fail.Store(false)
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			want := uint64(1)
			if mode == "send" || mode == "send-local" {
				want = 2
			}
			require.Eventually(t, func() bool {
				state, err := s.ReadLeaderState(ctx, "retry", 2)
				return err == nil && state.Ready && state.Term == cfg.Term && state.CommittedIndex == want && state.AppliedIndex == want
			}, time.Second, time.Millisecond)
		})
	}
}
