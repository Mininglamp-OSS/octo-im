package slot

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type delayedTruncateStorage struct {
	*PebbleShardLogStorage
	entered, release chan struct{}
	once             sync.Once
}

func (s *delayedTruncateStorage) TruncateLogTo(key string, index uint64) error {
	s.once.Do(func() { close(s.entered); <-s.release })
	return s.PebbleShardLogStorage.TruncateLogTo(key, index)
}

type truncateRecoveryTransport struct{ events chan types.Event }

func (tr *truncateRecoveryTransport) Send(_ string, e types.Event) { tr.events <- e }

// A real asynchronous storage worker holds the old term's cut while the same
// Raft instance accepts a new leader. Verify both durable recovery and writes.
func TestSlotOldTermTruncateReconcilesWithoutCommittingDivergence(t *testing.T) {
	for _, learner := range []bool{false, true} {
		t.Run(map[bool]string{false: "follower", true: "learner"}[learner], func(t *testing.T) {
			applied := make(chan []types.Log, 8)
			s := NewServer(NewOptions(WithDataDir(t.TempDir()), WithSlotDbShardNum(1), WithOnApply(func(_ uint32, logs []types.Log) error { applied <- logs; return nil })))
			require.NoError(t, s.storage.Open())
			t.Cleanup(s.Stop)
			old := make([]types.Log, 8)
			for i := range old {
				old[i] = types.Log{Index: uint64(i + 1), Term: 2, Data: []byte("old divergent history")}
			}
			old[0].Term, old[1].Term = 1, 1
			require.NoError(t, s.storage.AppendLogs("59", old[:2], &types.TermStartIndexInfo{Term: 1, Index: 1}))
			require.NoError(t, s.storage.AppendLogs("59", old[2:], &types.TermStartIndexInfo{Term: 2, Index: 3}))
			require.NoError(t, s.storage.Apply("59", old[:2]))
			<-applied
			storage := &delayedTruncateStorage{PebbleShardLogStorage: s.storage, entered: make(chan struct{}), release: make(chan struct{})}
			tr := &truncateRecoveryTransport{events: make(chan types.Event, 100)}
			g := raftgroup.New(raftgroup.NewOptions(raftgroup.WithStorage(storage), raftgroup.WithTransport(tr), raftgroup.WithTickInterval(time.Hour)))
			n := raft.NewNode(3, types.RaftState{LastLogIndex: 8, LastTerm: 2, AppliedIndex: 2}, raft.NewOptions(raft.WithKey("59"), raft.WithNodeId(2), raft.WithReplicas([]uint64{1, 2, 3})))
			if learner {
				n.BecomeLearner(4, 1)
			} else {
				n.BecomeFollower(4, 1)
			}
			g.AddRaft(n)
			require.NoError(t, g.Start())
			t.Cleanup(g.Stop)
			unblock := sync.OnceFunc(func() { close(storage.release) })
			t.Cleanup(unblock)
			step := func(e types.Event) {
				require.NoError(t, g.Do(context.Background(), "59", func(r raftgroup.IRaft) error { return r.Step(e) }))
			}
			step(types.Event{Type: types.SyncResp, From: 1, Term: 4, Reason: types.ReasonTruncate, Index: 5})
			select {
			case <-storage.entered:
			case <-time.After(time.Second):
				t.Fatal("truncate did not reach worker")
			}
			step(types.Event{Type: types.Ping, From: 3, Term: 5, CommittedIndex: 5})
			unblock()
			waitLast := func(want uint64) {
				require.Eventually(t, func() bool {
					var last uint64
					require.NoError(t, g.Do(context.Background(), "59", func(r raftgroup.IRaft) error { last = r.LastLogIndex(); return nil }))
					return last == want
				}, time.Second, time.Millisecond)
			}
			waitLast(5)
			step(types.Event{Type: types.Ping, From: 3, Term: 5, CommittedIndex: 5})
			var committed uint64
			require.NoError(t, g.Do(context.Background(), "59", func(r raftgroup.IRaft) error {
				committed = r.CommittedIndex()
				return nil
			}))
			require.Equal(t, uint64(2), committed, "old physical cut must not certify leader 3's retained history")
			var request types.Event
			select {
			case request = <-tr.events:
			case <-time.After(time.Second):
				t.Fatal("new leader alignment did not resume")
			}
			require.Equal(t, types.SyncReq, request.Type)
			require.Equal(t, uint64(3), request.To)
			require.NotEqual(t, types.ReasonOnlySync, request.Reason)
			step(types.Event{Type: types.SyncResp, From: 3, Term: 5, Reason: types.ReasonTruncate, Index: 2})
			waitLast(2)
			canonical := []types.Log{{Index: 3, Term: 3, Data: []byte("canonical-3")}, {Index: 4, Term: 3, Data: []byte("canonical-4")}, {Index: 5, Term: 3, Data: []byte("canonical-5")}}
			step(types.Event{Type: types.SyncResp, From: 3, Term: 5, Reason: types.ReasonOk, Index: 3, Logs: canonical})
			require.Eventually(t, func() bool {
				logs, err := s.storage.GetLogs("59", 3, 6, 0)
				return err == nil && len(logs) == 3 && logs[2].Term == 3
			}, time.Second, time.Millisecond)
			// Each heartbeat is serialized with StoreResp on the owner. A cut
			// that left the instance stuck can never complete this real Apply.
			require.Eventually(t, func() bool {
				step(types.Event{Type: types.Ping, From: 3, Term: 5, CommittedIndex: 5})
				v, err := s.storage.AppliedIndex("59")
				return err == nil && v == 5
			}, time.Second, time.Millisecond)
			select {
			case logs := <-applied:
				require.Len(t, logs, len(canonical))
				for i := range canonical {
					require.Equal(t, canonical[i].Index, logs[i].Index)
					require.Equal(t, canonical[i].Term, logs[i].Term)
					require.Equal(t, canonical[i].Data, logs[i].Data)
				}
			case <-time.After(time.Second):
				t.Fatal("repaired logs were not applied")
			}
			actual, err := s.storage.GetLogs("59", 1, 6, 0)
			require.NoError(t, err)
			expected := append(old[:2:2], canonical...)
			require.Len(t, actual, len(expected))
			for i := range expected {
				require.Equal(t, expected[i].Index, actual[i].Index)
				require.Equal(t, expected[i].Term, actual[i].Term)
				require.Equal(t, expected[i].Data, actual[i].Data)
			}
		})
	}
}
