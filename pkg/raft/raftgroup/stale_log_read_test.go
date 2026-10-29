package raftgroup

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type delayedLogReadStorage struct {
	IStorage
	entered chan struct{}
	release chan struct{}
}

func (s *delayedLogReadStorage) GetLogs(string, uint64, uint64, uint64) ([]types.Log, error) {
	close(s.entered)
	<-s.release
	return []types.Log{{Index: 2, Term: 2, Data: []byte("old term")}}, nil
}

func TestDelayedLogReadRetainsRequestTerm(t *testing.T) {
	storage := &delayedLogReadStorage{entered: make(chan struct{}), release: make(chan struct{})}
	rg := New(NewOptions(WithStorage(storage)))
	defer rg.goPool.Release()
	n := raft.NewNode(1, types.RaftState{LastLogIndex: 2, LastTerm: 2},
		raft.NewOptions(raft.WithNodeId(1), raft.WithKey("1"), raft.WithAdvance(func() {})))
	n.BecomeLeader(2)
	n.Ready()
	rg.handleGetLogsReq(n, types.Event{Type: types.GetLogsReq, From: 2, Term: 2,
		Index: 2, StoredIndex: 2, Reason: types.ReasonOnlySync})
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("log read did not start")
	}
	n.BecomeLeader(3)
	n.Ready()
	close(storage.release)
	var events []Event
	require.Eventually(t, func() bool {
		events = rg.mq.Get()
		return len(events) != 0
	}, time.Second, time.Millisecond)
	require.Len(t, events, 1)
	require.Equal(t, uint32(2), events[0].Event.Term, "a delayed disk read must not be relabeled with the current leader term")
	require.NoError(t, n.Step(events[0].Event))
	for _, event := range n.Ready() {
		require.NotEqual(t, types.SyncResp, event.Type)
	}
}
