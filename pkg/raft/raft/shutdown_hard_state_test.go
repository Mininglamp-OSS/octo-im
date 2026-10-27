package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// hardStateBlockedStorage parks the event loop inside SaveHardState, the
// synchronous write that Stop must join before Storage may be closed.
type hardStateBlockedStorage struct {
	studyStorage
	entered, release chan struct{}
	once             sync.Once
}

func (s *hardStateBlockedStorage) SaveHardState(state types.HardState) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.studyStorage.SaveHardState(state)
}

func TestWaitQuiescenceBoundsBlockedLoopJoin(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	s := &hardStateBlockedStorage{entered: make(chan struct{}), release: make(chan struct{})}
	s.terms = make(map[uint32]uint64)
	tr := &studyTransport{c: make(chan types.Event, 64)}
	r := New(NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithStorage(s), WithTransport(tr), withTestIncarnation()))
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(s.release) }) }
	defer release()
	require.NoError(t, r.Start())
	studyRecv(t, s.entered)

	wait := func(timeout time.Duration) (error, time.Duration) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- r.WaitQuiescence(ctx) }()
		select {
		case err := <-done:
			return err, time.Since(start)
		case <-time.After(2 * time.Second):
			release()
			t.Fatal("WaitQuiescence ignored its deadline while the loop was blocked in SaveHardState")
			return nil, 0
		}
	}
	for i := 0; i < 2; i++ {
		err, elapsed := wait(20 * time.Millisecond)
		require.ErrorIs(t, err, ErrIncompleteQuiescence)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, time.Second)
	}
	release()
	err, _ := wait(5 * time.Second)
	require.NoError(t, err, "a later call waits on the same join and reports quiescence")
	require.NoError(t, r.WaitQuiescence(context.Background()))
	require.Zero(t, r.pool.Running())
	r.Stop()
}
