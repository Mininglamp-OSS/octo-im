package raftgroup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// The event loop is blocked inside SaveHardState, a synchronous storage write
// that the loop performs itself (not a pool task). The bounded handoff returns
// at its deadline, callers are settled through the stop signal, storage stays
// owned, and a later call completes the same lifecycle after the write returns.
func TestGroupStopBoundsBlockedHardState(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	entered, release := make(chan struct{}), make(chan struct{})
	var saves, savesAfterQuiescence atomic.Int64
	var quiesced atomic.Bool
	save := func(types.HardState) error {
		if saves.Add(1) == 1 {
			close(entered)
			<-release
		}
		if quiesced.Load() {
			savesAfterQuiescence.Add(1)
		}
		return nil
	}
	g := New(NewOptions(WithTransport(dropTransport{}), WithStorage(&memStorage{}), WithTickInterval(time.Hour)))
	defer g.goPool.Release()
	// A single-replica node leads term 1 at construction; its first Ready
	// must persist that hard state.
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(admKey), raft.WithNodeId(1), raft.WithReplicas([]uint64{1}), raft.WithSaveHardState(save)))
	require.True(t, node.IsLeader())
	g.AddRaft(node)
	require.NoError(t, g.Start())
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("hard-state write not reached")
	}

	g.BeginStop()
	// A caller blocked on the group is settled without waiting for the loop.
	caller := make(chan error, 1)
	go func() {
		_, err := g.ProposeBatchUntilAppliedTimeout(context.Background(), admKey, types.ProposeReqSet{{Id: 1}})
		caller <- err
	}()
	select {
	case err := <-caller:
		require.ErrorIs(t, err, ErrGroupStopped)
	case <-time.After(time.Second):
		t.Fatal("caller was not settled while the loop was blocked")
	}

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		started := time.Now()
		err := g.WaitQuiescence(ctx)
		elapsed := time.Since(started)
		cancel()
		require.ErrorIs(t, err, ErrIncompleteQuiescence)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, time.Second, "the handoff must return at its deadline")
		t.Logf("bounded handoff %d returned after %s: %v", i+1, elapsed, err)
	}
	select {
	case <-g.loopDone:
		t.Fatal("lifecycle finished while the loop was blocked in storage")
	default:
	}

	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, g.WaitQuiescence(ctx))
	quiesced.Store(true)
	require.NoError(t, g.WaitQuiescence(ctx), "a finished lifecycle keeps its result")
	g.Stop() // idempotent after BeginStop
	require.EqualValues(t, 1, saves.Load())
	require.Zero(t, savesAfterQuiescence.Load())
	require.Zero(t, g.wait.pending())
	require.ErrorIs(t, g.submit(func() {}), ErrGroupStopped)
}

// Stop keeps its synchronous contract for existing callers: on return the loop
// has exited and submission is fenced.
func TestGroupStopJoinsLoop(t *testing.T) {
	g := New(NewOptions(WithTransport(dropTransport{}), WithStorage(&memStorage{}), WithTickInterval(time.Hour)))
	defer g.goPool.Release()
	require.NoError(t, g.Start())
	g.Stop()
	select {
	case <-g.loopDone:
	default:
		t.Fatal("Stop returned before the lifecycle joined the loop")
	}
	require.ErrorIs(t, g.submit(func() {}), ErrGroupStopped)
	require.NoError(t, g.WaitQuiescence(context.Background()))
}
