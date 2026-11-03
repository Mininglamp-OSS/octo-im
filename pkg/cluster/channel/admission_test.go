package channel

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func TestAdmissionReturnsBeforeProcessingAndFencesTransfer(t *testing.T) {
	tr := &retryTransport{nodes: make(map[uint64]*Server), blocked: make(map[uint64]bool)}
	s := retryServer(t, 1, retryDB(t), []uint64{1}, tr)
	ctx := context.Background()
	var done func()
	require.NoError(t, s.AdmitMessage(ctx, "retry", 2, 42, 1024, func(f func()) { done = f }))
	require.NotNil(t, done, "admission returned without waiting for persistence")
	require.Equal(t, int64(1024), s.admissionBytes.Load())
	key := wkutil.ChannelToKey("retry", 2)
	rg := s.getRaftGroup(key)
	require.NoError(t, rg.Do(ctx, key, func(r raftgroup.IRaft) error {
		cfg := r.Config().Clone()
		cfg.Version++
		cfg.Learners = []uint64{2}
		cfg.MigrateFrom = 1
		cfg.MigrateTo = 2
		return r.Step(types.Event{Type: types.ConfChange, Config: cfg})
	}))
	require.NoError(t, rg.Do(ctx, key, func(r raftgroup.IRaft) error {
		return r.Step(types.Event{Type: types.SyncReq, Term: 1, From: 2, Index: 1, StoredIndex: 1, LastLogTerm: 1, Reason: types.ReasonOnlySync})
	}))
	enqueued := false
	require.Error(t, s.AdmitMessage(ctx, "retry", 2, 43, 1024, func(func()) { enqueued = true }))
	require.False(t, enqueued)
	require.Equal(t, int64(1024), s.admissionBytes.Load())
	done()
	done()
	require.Zero(t, s.admissionBytes.Load())
}

func TestAdmissionRejectsCapacityCancellationAndNonLeader(t *testing.T) {
	tr := &retryTransport{nodes: make(map[uint64]*Server), blocked: make(map[uint64]bool)}
	s := retryServer(t, 1, retryDB(t), []uint64{1}, tr)
	enqueue := func(func()) { t.Fatal("rejected admission enqueued work") }
	s.admissionBytes.Store(maxAdmissionBytes)
	require.ErrorIs(t, s.AdmitMessage(context.Background(), "retry", 2, 42, 1, enqueue), ErrSendUnavailable)
	require.Equal(t, int64(maxAdmissionBytes), s.admissionBytes.Load())
	s.admissionBytes.Store(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.AdmitMessage(ctx, "retry", 2, 42, 1, enqueue), context.Canceled)
	require.Zero(t, s.admissionBytes.Load())
	require.Error(t, s.AdmitMessage(context.Background(), "absent", 2, 42, 1, enqueue))
	require.Zero(t, s.admissionBytes.Load())
}
