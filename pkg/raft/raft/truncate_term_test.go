package raft

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type retainedSingleTermStorage struct {
	Storage
	deletedAbove chan uint32
}

func (*retainedSingleTermStorage) SaveHardState(types.HardState) error { return nil }

func (*retainedSingleTermStorage) GetState() (types.RaftState, error) {
	return types.RaftState{LastLogIndex: 5, LastTerm: 1}, nil
}
func (*retainedSingleTermStorage) TruncateLogTo(uint64) error { return nil }
func (*retainedSingleTermStorage) GetLogs(uint64, uint64, uint64) ([]types.Log, error) {
	return []types.Log{{Index: 5, Term: 1}}, nil
}
func (*retainedSingleTermStorage) GetTermStartIndex(uint32) (uint64, error) { return 1, nil }
func (s *retainedSingleTermStorage) DeleteLeaderTermStartIndexGreaterThanTerm(term uint32) error {
	s.deletedAbove <- term
	return nil
}

func TestSingleTruncateRemovesDiscardedTailTerm(t *testing.T) {
	storage := &retainedSingleTermStorage{deletedAbove: make(chan uint32, 1)}
	r := New(NewOptions(WithStorage(storage), WithNodeId(2), WithReplicas([]uint64{1, 2})))
	defer r.pool.Release()
	r.node.queue.lastLogIndex = 8
	r.node.queue.storedIndex = 8
	r.node.updateLastTermStartIndex(2, 6)
	r.node.BecomeFollower(3, 1)
	r.node.Ready()
	r.handleTruncateReq(types.Event{Type: types.TruncateReq, Index: 5, LastLogTerm: 2})
	select {
	case term := <-storage.deletedAbove:
		require.Equal(t, uint32(1), term)
	case <-time.After(time.Second):
		t.Fatal("truncate did not finish")
	}
	select {
	case response := <-r.stepC:
		require.NoError(t, r.node.Step(response.event))
		require.Equal(t, uint32(1), r.node.LastLogTerm())
	case <-time.After(time.Second):
		t.Fatal("truncate response did not arrive")
	}
}

func TestSingleTruncateBeyondLocalTailKeepsRealPrefix(t *testing.T) {
	storage := &retainedSingleTermStorage{deletedAbove: make(chan uint32, 1)}
	r := New(NewOptions(WithStorage(storage), WithNodeId(2), WithReplicas([]uint64{1, 2})))
	defer r.pool.Release()
	r.node.BecomeFollower(3, 1)
	r.node.Ready()
	r.handleTruncateReq(types.Event{Type: types.TruncateReq, Index: 50, LastLogTerm: 1})
	select {
	case response := <-r.stepC:
		require.Equal(t, types.ReasonOk, response.event.Reason)
		require.Equal(t, uint64(5), response.event.Index)
		require.NoError(t, r.node.Step(response.event))
		require.Equal(t, uint64(5), r.node.LastLogIndex())
		require.Equal(t, uint32(1), r.node.LastLogTerm())
	case <-time.After(time.Second):
		t.Fatal("truncate response did not arrive")
	}
}
