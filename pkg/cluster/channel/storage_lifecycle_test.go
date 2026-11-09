package channel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type retiredChannelApplyDB struct {
	wkdb.DB
	entered, release, finished chan struct{}
	first                      atomic.Bool
}

func (d *retiredChannelApplyDB) UpdateChannelAppliedIndex(id string, typ uint8, index uint64) error {
	held := id == "retry" && index == 1 && d.first.CompareAndSwap(false, true)
	if held {
		close(d.entered)
		<-d.release
	}
	err := d.DB.UpdateChannelAppliedIndex(id, typ, index)
	if held {
		close(d.finished)
	}
	return err
}

func heldChannelApply(t *testing.T, idleTicks int) (*Server, wkdb.DB, *retiredChannelApplyDB, wkdb.ChannelClusterConfig, func()) {
	t.Helper()
	raw := retryDB(t)
	db := &retiredChannelApplyDB{DB: raw, entered: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(db.release) })
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Term: 1, ConfVersion: 1}
	s := NewServer(NewOptions(WithNodeId(1), WithDB(db), WithGroupCount(1), WithTransport(retryNetwork()), WithDestoryAfterIdleTick(idleTicks), WithCluster(resumeConfigCluster{cfg: cfg})))
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	t.Cleanup(unblock)
	require.NoError(t, s.WakeLeaderIfNeed(cfg))
	proposalCtx, cancelProposal := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelProposal()
	oldResult := make(chan error, 1)
	reqs := retryRequests(t, retryMessage(901, "writer", "first"))
	go func() {
		_, err := s.ProposeBatchUntilAppliedTimeoutForLocal(proposalCtx, "retry", 2, reqs)
		oldResult <- err
	}()
	select {
	case <-db.entered:
	case <-time.After(time.Second):
		t.Fatal("real apply worker did not reach its durable-marker write")
	}
	select {
	case err := <-oldResult:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("timed-out caller kept the old instance alive")
	}
	require.Equal(t, uint64(1), retryTail(t, raw))
	before, err := raw.GetChannelAppliedIndex("retry", 2)
	require.NoError(t, err)
	require.Zero(t, before)
	return s, raw, db, cfg, unblock
}

func finishChannelApply(t *testing.T, db *retiredChannelApplyDB, unblock func()) {
	t.Helper()
	unblock()
	select {
	case <-db.finished:
	case <-time.After(time.Second):
		t.Fatal("old apply worker did not finish")
	}
}

func requireChannelSecondWrite(t *testing.T, s *Server, raw wkdb.DB) {
	t.Helper()
	responses := retryPropose(t, s, retryMessage(902, "writer", "second"))
	require.Equal(t, uint64(2), responses[0].Index)
	finalApplied, err := raw.GetChannelAppliedIndex("retry", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), finalApplied)
	require.Equal(t, uint64(2), retryTail(t, raw))
	first, err := raw.LoadMsg("retry", 2, 1)
	require.NoError(t, err)
	require.Equal(t, int64(901), first.MessageID)
}

func TestChannelReplacementDrainsRetiredStorage(t *testing.T) {
	for _, idle := range []bool{true, false} {
		t.Run(map[bool]string{true: "idle", false: "removed"}[idle], func(t *testing.T) {
			ticks := 1000
			if idle {
				ticks = 4
			}
			s, raw, db, cfg, unblock := heldChannelApply(t, ticks)
			old := s.Channel("retry", 2)
			require.NotNil(t, old)
			if !idle {
				s.RemoveChannel("retry", 2)
			}
			require.Eventually(t, func() bool { return !s.ExistChannel("retry", 2) }, 2*time.Second, 5*time.Millisecond,
				"the old instance must retire while its accepted worker is delayed")
			woke := make(chan error, 1)
			go func() { woke <- s.WakeLeaderIfNeed(cfg) }()
			select {
			case err := <-woke:
				t.Fatalf("replacement finished before retired storage drained: %v", err)
			case <-time.After(40 * time.Millisecond):
			}
			require.False(t, s.ExistChannel("retry", 2), "do not publish from an incomplete storage snapshot")
			finishChannelApply(t, db, unblock)
			select {
			case err := <-woke:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("replacement did not resume after storage drained")
			}
			replacement := s.Channel("retry", 2)
			require.NotNil(t, replacement)
			require.NotSame(t, old, replacement)
			applied, err := raw.GetChannelAppliedIndex("retry", 2)
			require.NoError(t, err)
			require.Equal(t, uint64(1), applied, "replacement must load the completed old apply")
			requireChannelSecondWrite(t, s, raw)
		})
	}
}

func TestChannelReplacementDrainCancellationCanRetry(t *testing.T) {
	s, raw, db, cfg, unblock := heldChannelApply(t, 1000)
	old := s.Channel("retry", 2)
	s.RemoveChannel("retry", 2)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	// Use the same lock discipline as every production caller, with a short
	// context to verify the drain's cancellation without a five-second wait.
	s.wakeLeaderLock.Lock(cfg.ChannelId)
	err := s.createAndApplyConfig(ctx, cfg)
	s.wakeLeaderLock.Unlock(cfg.ChannelId)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, s.ExistChannel("retry", 2))
	finishChannelApply(t, db, unblock)
	require.NoError(t, s.WakeLeaderIfNeed(cfg))
	require.NotSame(t, old, s.Channel("retry", 2))
	requireChannelSecondWrite(t, s, raw)
}

func TestChannelConfigReusesActiveInstanceWithoutDraining(t *testing.T) {
	s, raw, db, _, unblock := heldChannelApply(t, 1000)
	old := s.Channel("retry", 2)
	// Follower wake also reaches createAndApplyConfig for an existing local
	// replica. Its live I/O must not be mistaken for retired-instance work.
	done := make(chan error, 1)
	go func() { done <- s.WakeFollowerfNeed("retry", 2) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("existing instance waited for its own storage")
	}
	require.Same(t, old, s.Channel("retry", 2))
	finishChannelApply(t, db, unblock)
	requireChannelSecondWrite(t, s, raw)
}

func TestChannelSaveConfigCanReenterReplacement(t *testing.T) {
	tr := retryNetwork()
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Learners: []uint64{2}, Term: 1, ConfVersion: 1}
	done := make(chan error, 1)
	var leader *Server
	leader = NewServer(NewOptions(WithNodeId(1), WithDB(retryDB(t)), WithGroupCount(1), WithTransport(tr),
		WithOnSaveConfig(func(_ string, _ uint8, next types.Config) error {
			// This is the real asynchronous membership SaveConfig worker,
			// reached after the learner catches up through the transport.
			leader.RemoveChannel("retry", 2)
			updated := cfg.Clone()
			updated.Replicas, updated.Learners = next.Replicas, next.Learners
			updated.MigrateFrom, updated.MigrateTo = next.MigrateFrom, next.MigrateTo
			updated.LeaderId, updated.Term, updated.ConfVersion = next.Leader, next.Term, cfg.ConfVersion+1
			err := leader.WakeLeaderIfNeed(updated)
			done <- err
			return err
		})))
	follower := NewServer(NewOptions(WithNodeId(2), WithDB(retryDB(t)), WithGroupCount(1), WithTransport(tr), WithCluster(resumeConfigCluster{cfg: cfg})))
	require.NoError(t, leader.Start())
	t.Cleanup(leader.Stop)
	require.NoError(t, follower.Start())
	t.Cleanup(follower.Stop)
	tr.mu.Lock()
	tr.nodes[1], tr.nodes[2] = leader, follower
	tr.mu.Unlock()
	require.NoError(t, leader.WakeLeaderIfNeed(cfg))
	old := leader.Channel("retry", 2)
	require.NoError(t, follower.WakeFollowerfNeed("retry", 2))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("SaveConfig waited for its own completion during channel replacement")
	}
	require.NotSame(t, old, leader.Channel("retry", 2))
	responses := retryPropose(t, leader, retryMessage(903, "writer", "after-promotion"))
	require.Equal(t, uint64(1), responses[0].Index)
	for _, s := range []*Server{leader, follower} {
		require.Eventually(t, func() bool {
			applied, err := s.opts.DB.GetChannelAppliedIndex("retry", 2)
			return err == nil && applied == 1
		}, time.Second, time.Millisecond)
	}
}
