package raft

import (
	"context"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
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
