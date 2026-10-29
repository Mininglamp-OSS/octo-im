package raft

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type delayedSingleLogReadStorage struct {
	applyRetryStorage
	entered chan struct{}
	release chan struct{}
}

func (*delayedSingleLogReadStorage) SaveHardState(types.HardState) error { return nil }
func (s *delayedSingleLogReadStorage) GetLogs(uint64, uint64, uint64) ([]types.Log, error) {
	close(s.entered)
	<-s.release
	return []types.Log{{Index: 1, Term: 2, Data: []byte("old term")}}, nil
}

func TestDelayedSingleRaftLogReadRetainsRequestTerm(t *testing.T) {
	storage := &delayedSingleLogReadStorage{entered: make(chan struct{}), release: make(chan struct{})}
	r := New(NewOptions(WithStorage(storage), WithNodeId(1), WithReplicas([]uint64{1, 2})))
	defer r.pool.Release()
	r.node.BecomeLeader(2)
	r.node.Ready()
	r.handleGetLogsReq(types.Event{Type: types.GetLogsReq, From: 2, Term: 2,
		Index: 1, StoredIndex: 1, Reason: types.ReasonOnlySync})
	select {
	case <-storage.entered:
	case <-time.After(time.Second):
		t.Fatal("log read did not start")
	}
	r.node.BecomeLeader(3)
	r.node.Ready()
	close(storage.release)
	select {
	case got := <-r.stepC:
		require.Equal(t, uint32(2), got.event.Term)
		require.NoError(t, r.node.Step(got.event))
		for _, event := range r.node.Ready() {
			require.NotEqual(t, types.SyncResp, event.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("log read did not complete")
	}
}
