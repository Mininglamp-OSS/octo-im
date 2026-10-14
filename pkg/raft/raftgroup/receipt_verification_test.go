package raftgroup

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

// These reads use the production GetLogs adapter, but block only verification,
// after replication has finished. Both public callers and worker exits are joined.
type verificationStorage struct {
	*memStorage
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int64
	err     error
}

func (s *verificationStorage) GetLogs(key string, start, end, limit uint64) ([]types.Log, error) {
	s.calls.Add(1)
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	if s.err != nil {
		return nil, s.err
	}
	return s.memStorage.GetLogs(key, start, end, limit)
}

func TestForwardedVerificationCancellation(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel", "stop"} {
		t.Run(mode, func(t *testing.T) {
			fp := newForwardPair(t, 5*time.Second)
			fp.transport.holdResps.Store(true)
			s := &verificationStorage{memStorage: fp.followerStorage, entered: make(chan struct{}, 1), release: make(chan struct{})}
			// Installed before any receipt exists; replication continues on memStorage.
			fp.follower.wait.readLog = func(key string, index uint64) (types.Log, bool, error) {
				logs, err := s.GetLogs(key, index, index+1, 0)
				if len(logs) == 0 {
					return types.Log{}, false, err
				}
				return logs[0], true, err
			}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(s.release) }) }
			t.Cleanup(release)
			ctx, cancel := context.WithCancel(context.Background())
			if mode == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
			}
			defer cancel()
			c := make(chan proposeResult, 1)
			go func() { resps, err := fp.forwardUntilApplied(ctx, 100); c <- proposeResult{resps, err} }()
			require.Equal(t, []uint64{100}, fp.followerStorage.waitApplied(t, 1))
			fp.transport.release()
			select {
			case <-s.entered:
			case <-time.After(time.Second):
				t.Fatal("verification did not start")
			}
			b := fp.follower.wait.bucket(admKey)
			b.mu.Lock()
			var w *receiptWaiter
			for _, candidate := range b.waiters[admKey] {
				w = candidate
			}
			b.mu.Unlock()
			require.NotNil(t, w)
			// Duplicate apply/dispatch cannot start a second read for this receipt.
			fp.follower.wait.didApply(admKey, 1, nil)
			switch mode {
			case "cancel":
				cancel()
			case "stop":
				stopVerificationGroup(fp.follower)
			}
			var r proposeResult
			select {
			case r = <-c:
			case <-time.After(time.Second):
				t.Fatal("caller exceeded cancellation/Stop bound")
			}
			require.ErrorIs(t, r.err, types.ErrProposalOutcomeUnknown)
			switch mode {
			case "deadline":
				require.ErrorIs(t, r.err, context.DeadlineExceeded)
			case "cancel":
				require.ErrorIs(t, r.err, context.Canceled)
			case "stop":
				require.ErrorIs(t, r.err, ErrGroupStopped)
			}
			require.Zero(t, fp.follower.wait.pending())
			require.False(t, fp.follower.fowardProposeWait.IsRegistered("100"))
			require.EqualValues(t, 1, fp.follower.verificationWorkers.Load())
			b.mu.Lock()
			terminalErr := w.err
			b.mu.Unlock()
			release()
			waitFor(t, "verification exit", func() bool { return fp.follower.verificationWorkers.Load() == 0 })
			require.EqualValues(t, 1, s.calls.Load())
			b.mu.Lock()
			require.True(t, w.done)
			require.Equal(t, terminalErr, w.err)
			// A detached receipt has no publication; Stop may publish its unknown result.
			select {
			case result := <-w.doneC:
				require.ErrorIs(t, result, ErrGroupStopped)
			default:
			}
			b.mu.Unlock()
			stopVerificationGroup(fp.follower)
			require.NoError(t, fp.follower.goPool.ReleaseTimeout(time.Second))
			require.Zero(t, fp.follower.goPool.Running())
			stopVerificationGroup(fp.leader)
			require.NoError(t, fp.leader.goPool.ReleaseTimeout(time.Second))
			require.Zero(t, fp.leader.wait.pending())
			require.Zero(t, fp.leader.goPool.Running())
			t.Logf("cleanup: local/forwarded receipts=0 forward registrations=0 verification workers=%d pool workers=0", fp.follower.verificationWorkers.Load())
			ok, na, unknown, superseded := fp.follower.ProposalOutcomeCounts()
			require.Equal(t, []uint64{0, 0, 1, 0}, []uint64{ok, na, unknown, superseded})
		})
	}
}

func TestReceiptVerificationSchedules(t *testing.T) {
	for _, before := range []bool{true, false} {
		for _, outcome := range []string{"match", "mismatch", "storage-error", "missing", "overload", "stopped"} {
			t.Run(outcome+map[bool]string{true: "/apply-first", false: "/register-first"}[before], func(t *testing.T) {
				g := New(NewOptions(WithGoPoolSize(1)))
				gate := make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(gate) }) }
				t.Cleanup(func() {
					release()
					stopVerificationGroup(g)
					if !g.goPool.IsClosed() {
						require.NoError(t, g.goPool.ReleaseTimeout(time.Second))
					}
				})
				var reads atomic.Int64
				g.wait.readLog = func(string, uint64) (types.Log, bool, error) {
					reads.Add(1)
					if outcome == "storage-error" {
						return types.Log{}, false, errors.New("storage read failed")
					}
					if outcome == "missing" {
						return types.Log{}, false, nil
					}
					id := uint64(7)
					if outcome == "mismatch" {
						id++
					}
					return types.Log{Id: id, Index: 1}, true, nil
				}
				if outcome == "overload" {
					entered := make(chan struct{})
					require.NoError(t, g.goPool.Submit(func() { close(entered); <-gate }))
					<-entered
				}
				if outcome == "stopped" {
					stopVerificationGroup(g)
				}
				if before {
					g.wait.didApply(admKey, 1, nil)
				}
				w, applied := g.wait.register(receipt{raftKey: admKey, seq: 1, last: 1, lastLogId: 7, forwarded: true})
				if applied {
					g.wait.verifyStored(w)
				} else {
					g.wait.didApply(admKey, 1, nil)
				}
				var err error
				select {
				case err = <-w.doneC:
				case <-time.After(time.Second):
					t.Fatal("verification did not settle")
				}
				switch outcome {
				case "match":
					require.NoError(t, err)
				case "mismatch":
					require.ErrorIs(t, err, types.ErrProposalSuperseded)
				case "overload":
					require.ErrorIs(t, err, ants.ErrPoolOverload)
					require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
				case "stopped":
					require.ErrorIs(t, err, ErrGroupStopped)
					require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
				default:
					require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
				}
				g.wait.verifyStored(w)
				waitFor(t, "verification exit", func() bool { return g.verificationWorkers.Load() == 0 })
				if outcome == "overload" || outcome == "stopped" {
					require.Zero(t, reads.Load())
				} else {
					require.EqualValues(t, 1, reads.Load())
				}
				require.Zero(t, g.wait.pending())
				release()
				stopVerificationGroup(g)
				require.NoError(t, g.goPool.ReleaseTimeout(time.Second))
				require.Zero(t, g.goPool.Running())
			})
		}
	}
}

func TestGroupPublicOutcomeConservation(t *testing.T) {
	g := New(NewOptions())
	defer g.goPool.Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := g.ProposeBatchTimeout(ctx, admKey, types.ProposeReqSet{{Id: 1}})
	require.ErrorIs(t, err, types.ErrProposalNotAdmitted)
	_, err = g.ProposeBatchUntilAppliedTimeout(context.Background(), admKey, types.ProposeReqSet{{Id: 2}})
	require.Error(t, err)
	ok, na, unknown, superseded := g.ProposalOutcomeCounts()
	require.Equal(t, []uint64{0, 1, 1, 0}, []uint64{ok, na, unknown, superseded})
	stopVerificationGroup(g)
}

func stopVerificationGroup(g *RaftGroup) {
	select {
	case <-g.stopper.ShouldStop():
	default:
		g.Stop()
	}
}

// Every pool task is tracked: Stop fences submission, WaitQuiescence is
// bounded while a task is inside Storage and succeeds, repeatedly, after it exits.
func TestGroupWaitQuiescence(t *testing.T) {
	g := New(NewOptions(WithGoPoolSize(2)))
	defer g.goPool.Release()
	require.ErrorIs(t, g.WaitQuiescence(context.Background()), errGroupNotStopped)
	entered, release := make(chan struct{}), make(chan struct{})
	exited := make(chan struct{})
	require.NoError(t, g.submit(func() { close(entered); <-release; close(exited) }))
	<-entered
	g.Stop()
	g.Stop() // idempotent
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		require.ErrorIs(t, g.WaitQuiescence(ctx), context.DeadlineExceeded)
		cancel()
	}
	var ran atomic.Bool
	require.ErrorIs(t, g.submit(func() { ran.Store(true) }), ErrGroupStopped)
	var reads atomic.Int64
	g.wait.readLog = func(string, uint64) (types.Log, bool, error) { reads.Add(1); return types.Log{}, false, nil }
	w, _ := g.wait.register(receipt{raftKey: admKey, seq: 1, last: 1, lastLogId: 7, forwarded: true})
	g.verifyReceiptAsync(w)
	select {
	case err := <-w.doneC:
		require.ErrorIs(t, err, ErrGroupStopped)
		require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
	case <-time.After(time.Second):
		t.Fatal("verification after Stop did not settle")
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, g.WaitQuiescence(ctx))
	require.NoError(t, g.WaitQuiescence(ctx))
	<-exited
	require.False(t, ran.Load())
	require.Zero(t, reads.Load())
	require.Zero(t, g.verificationWorkers.Load())
	require.Zero(t, g.wait.pending())
}
