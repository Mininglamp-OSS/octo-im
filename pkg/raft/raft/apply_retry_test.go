package raft

import (
	"context"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type applyRetryStorage struct {
	Storage
	calls atomic.Int32
}

func (*applyRetryStorage) GetTermStartIndex(uint32) (uint64, error) { return 0, nil }
func (*applyRetryStorage) GetState() (types.RaftState, error)       { return types.RaftState{}, nil }
func (*applyRetryStorage) GetLogs(uint64, uint64, uint64) ([]types.Log, error) {
	return []types.Log{{Index: 1, Term: 1}}, nil
}
func (s *applyRetryStorage) Apply([]types.Log) error {
	if s.calls.Add(1) == 1 {
		return context.DeadlineExceeded
	}
	return nil
}
func TestApplyFailureReturnsResponseAndCanRetry(t *testing.T) {
	storage := &applyRetryStorage{}
	r := New(NewOptions(WithStorage(storage)))
	defer r.Stop()
	defer r.pool.Release()
	req := types.Event{Type: types.ApplyReq, StartIndex: 1, EndIndex: 2}
	for _, reason := range []types.Reason{types.ReasonError, types.ReasonOk} {
		r.handleApplyReq(req)
		select {
		case got := <-r.stepC:
			require.Equal(t, types.ApplyResp, got.event.Type)
			require.Equal(t, reason, got.event.Reason)
		case <-time.After(time.Second):
			t.Fatal("apply worker must always release the in-flight apply state")
		}
	}
}

type persistentApplyStorage struct {
	applyRetryStorage
	repaired atomic.Bool
}

func (*persistentApplyStorage) GetState() (types.RaftState, error) {
	return types.RaftState{LastLogIndex: 1, LastTerm: 1}, nil
}
func (*persistentApplyStorage) SaveHardState(types.HardState) error { return nil }
func (s *persistentApplyStorage) Apply([]types.Log) error {
	s.calls.Add(1)
	if !s.repaired.Load() {
		return context.DeadlineExceeded
	}
	return nil
}

func TestApplyPersistentFailureKeepsLoopResponsiveAndRecovers(t *testing.T) {
	storage := &persistentApplyStorage{}
	r := New(NewOptions(WithStorage(storage), WithNodeId(1), WithReplicas([]uint64{1}), WithTickInterval(5*time.Millisecond)))
	// Stored, committed but unapplied work, before the event loop starts.
	r.node.queue.committedIndex = 1
	require.NoError(t, r.Start())
	defer r.pool.Release()
	stop := sync.OnceFunc(r.Stop)
	defer stop()
	require.Eventually(t, func() bool { return storage.calls.Load() >= 8 }, 3*time.Second, 5*time.Millisecond)
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := r.StepWait(ctx, types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1}, Leader: 1, Term: 1}})
		cancel()
		require.NoError(t, err, "storage cooldown must not block other raft work")
	}
	require.Equal(t, int32(8), storage.calls.Load(), "incoming events cannot turn cooldown into a busy apply loop")
	applied := r.wait.waitApply(1)
	storage.repaired.Store(true)
	// Observe recovery through StepWait, keeping all raft reads/writes on its
	// owner. The storage attempt count is atomic; no unsafe public-state poll.
	select {
	case <-applied.waitC:
	case <-time.After(3 * time.Second):
		t.Fatal("repaired storage did not recover through the scheduled probe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, r.StepWait(ctx, types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1}, Leader: 1, Term: 1}}))
	stop()
	require.Equal(t, uint64(1), r.node.AppliedIndex())
}
