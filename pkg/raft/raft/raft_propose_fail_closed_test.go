package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type memStorage struct {
	mu   sync.Mutex
	logs []types.Log
}

func (s *memStorage) AppendLogs(logs []types.Log, _ *types.TermStartIndexInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = append(s.logs, logs...)
	return nil
}
func (s *memStorage) GetLogs(start, end, limit uint64) ([]types.Log, error)  { return nil, nil }
func (s *memStorage) GetState() (types.RaftState, error)                     { return types.RaftState{}, nil }
func (s *memStorage) GetTermStartIndex(uint32) (uint64, error)               { return 0, nil }
func (s *memStorage) LeaderTermGreaterEqThan(uint32) (uint32, error)         { return 0, nil }
func (s *memStorage) LeaderLastTerm() (uint32, error)                        { return 0, nil }
func (s *memStorage) TruncateLogTo(uint64) error                             { return nil }
func (s *memStorage) DeleteLeaderTermStartIndexGreaterThanTerm(uint32) error { return nil }
func (s *memStorage) Apply([]types.Log) error                                { return nil }
func (s *memStorage) SaveConfig(types.Config) error                          { return nil }

type nopTransport struct{}

func (nopTransport) Send(types.Event) {}

// Queue cancellation is now definitive before admission; allocation happens
// on the real loop. Direct StepWait proposals retain the C0-1 strict checks.
func TestConfigRaftStepWaitProposeFailsClosed(t *testing.T) {
	storage := &memStorage{}
	r := New(NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithStorage(storage), WithTransport(nopTransport{})))
	require.True(t, r.IsLeader()) // single replica: leader at term 1
	reqs := func(id uint64) types.ProposeReqSet { return types.ProposeReqSet{{Id: id, Data: []byte{byte(id)}}} }

	ctxA, cancelA := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelA()
	_, errA := r.proposeBatch(ctxA, reqs(100), nil)
	require.ErrorIs(t, errA, context.DeadlineExceeded, "A is queued at index 1 but its caller gives up")

	allocated := make(chan uint64, 1)
	bC := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := r.proposeBatch(ctx, reqs(200), func(logs []types.Log) { allocated <- logs[0].Index })
		bC <- err
	}()

	require.NoError(t, r.Start())
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(r.Stop) }
	defer stop()

	require.NoError(t, <-bC)
	require.Equal(t, uint64(1), <-allocated, "B owns the first admitted index")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.ErrorIs(t, r.StepWait(ctx, types.Event{Type: types.Propose, Logs: []types.Log{{Id: 300, Term: 2, Index: 2}}}),
		types.ErrProposalStaleTerm)
	require.ErrorIs(t, r.StepWait(ctx, types.Event{Type: types.Propose, Logs: []types.Log{{Id: 301, Term: 1, Index: 1}}}),
		types.ErrLeaderLocalOverlap)

	// A was not admitted; B owns index 1 and the next proposal gets index 2.
	resps, err := r.proposeBatch(ctx, reqs(400), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), resps[0].Index)
	stop()
	require.Equal(t, uint64(2), r.node.queue.lastLogIndex)
	overlap, stale := r.node.ProposeRejectCounts()
	require.Equal(t, uint64(1), overlap)
	require.Equal(t, uint64(1), stale)
}
