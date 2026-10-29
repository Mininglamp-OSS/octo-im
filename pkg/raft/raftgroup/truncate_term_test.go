package raftgroup

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type retainedTermStorage struct {
	IStorage
	deletedAbove chan uint32
}

func (*retainedTermStorage) TruncateLogTo(string, uint64) error { return nil }
func (*retainedTermStorage) LastIndex(string) (uint64, error)   { return 5, nil }
func (*retainedTermStorage) GetLogs(string, uint64, uint64, uint64) ([]types.Log, error) {
	return []types.Log{{Index: 5, Term: 1}}, nil
}
func (*retainedTermStorage) GetTermStartIndex(string, uint32) (uint64, error) { return 1, nil }
func (s *retainedTermStorage) DeleteLeaderTermStartIndexGreaterThanTerm(_ string, term uint32) error {
	s.deletedAbove <- term
	return nil
}

func TestTruncateRemovesDiscardedTailTerm(t *testing.T) {
	storage := &retainedTermStorage{deletedAbove: make(chan uint32, 1)}
	rg := New(NewOptions(WithStorage(storage)))
	defer rg.goPool.Release()
	n := raft.NewNode(10, types.RaftState{LastLogIndex: 8, LastTerm: 2},
		raft.NewOptions(raft.WithNodeId(2), raft.WithKey("slot"), raft.WithAdvance(func() {})))
	n.BecomeFollower(3, 1)
	n.Ready()
	rg.handleTruncateReq(n, types.Event{Type: types.TruncateReq, Index: 5, LastLogTerm: 2})
	select {
	case term := <-storage.deletedAbove:
		require.Equal(t, uint32(1), term, "term 2 belongs entirely to the discarded suffix")
	case <-time.After(time.Second):
		t.Fatal("truncate did not finish")
	}
	var events []Event
	require.Eventually(t, func() bool { events = rg.mq.Get(); return len(events) > 0 }, time.Second, time.Millisecond)
	require.Len(t, events, 1)
	require.NoError(t, n.Step(events[0].Event))
	require.Equal(t, uint32(1), n.LastLogTerm(), "the owner must stop advertising the discarded term")
}

func TestTruncateBeyondLocalTailKeepsRealPrefix(t *testing.T) {
	storage := &retainedTermStorage{deletedAbove: make(chan uint32, 1)}
	rg := New(NewOptions(WithStorage(storage)))
	defer rg.goPool.Release()
	n := raft.NewNode(6, types.RaftState{LastLogIndex: 8, LastTerm: 2},
		raft.NewOptions(raft.WithNodeId(2), raft.WithKey("slot"), raft.WithAdvance(func() {})))
	n.BecomeFollower(3, 1)
	n.Ready()
	rg.handleTruncateReq(n, types.Event{Type: types.TruncateReq, Index: 50, LastLogTerm: 2})
	var events []Event
	require.Eventually(t, func() bool { events = rg.mq.Get(); return len(events) > 0 }, time.Second, time.Millisecond)
	require.Len(t, events, 1)
	require.Equal(t, types.ReasonOk, events[0].Event.Reason)
	require.Equal(t, uint64(5), events[0].Event.Index)
	require.NoError(t, n.Step(events[0].Event))
	require.Equal(t, uint64(5), n.LastLogIndex())
	require.Equal(t, uint32(1), n.LastLogTerm())
}
