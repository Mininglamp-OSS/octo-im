package raftgroup

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type recordingTransport struct{ events chan types.Event }

func (tr *recordingTransport) Send(_ string, e types.Event) {
	select {
	case tr.events <- e:
	default:
	}
}

type truncateCountingStorage struct {
	*memStorage
	truncates atomic.Int32
	storing   chan struct{}
}

func (s *truncateCountingStorage) AppendLogs(key string, logs []types.Log, termStart *types.TermStartIndexInfo) error {
	select {
	case s.storing <- struct{}{}:
	default:
	}
	return s.memStorage.AppendLogs(key, logs, termStart)
}

func (s *truncateCountingStorage) TruncateLogTo(key string, index uint64) error {
	defer s.truncates.Add(1)
	return s.memStorage.TruncateLogTo(key, index)
}

func (s *truncateCountingStorage) lastIndex() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.logs))
}

// A follower hands the old leader's tail to storage; the store is slow. A new
// leader then requests a truncation below that tail. Store and truncation are
// independent pool tasks, so the truncation must not reach storage before the
// store returns: otherwise the store writes the discarded entries back after
// the truncation, and its late StoreResp panics the event loop with
// "invalid append response".
func TestRaftGroupTruncationWaitsForInFlightStore(t *testing.T) {
	gate := make(chan struct{})
	storage := &truncateCountingStorage{memStorage: &memStorage{appendGate: gate}, storing: make(chan struct{}, 1)}
	tr := &recordingTransport{events: make(chan types.Event, 64)}
	g := New(NewOptions(WithTransport(tr), WithStorage(storage), WithTickInterval(time.Hour)))
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(admKey), raft.WithNodeId(1), raft.WithReplicas([]uint64{1, 2, 3})))
	node.BecomeFollower(1, 3)
	g.AddRaft(node)
	require.NoError(t, g.Start())
	released := false
	t.Cleanup(func() {
		if !released {
			close(gate)
		}
		g.Stop()
		g.goPool.Release()
	})
	tail := make([]types.Log, 0, 20)
	for i := uint64(1); i <= 20; i++ {
		tail = append(tail, types.Log{Index: i, Term: 1, Id: 1000 + i, Data: []byte("old-leader")})
	}
	g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 1, From: 3, To: 1, Reason: types.ReasonOk, Logs: tail})
	g.Advance()
	select {
	case <-storage.storing: // the store is in flight, held at the gate
	case <-time.After(5 * time.Second):
		t.Fatal("store not dispatched")
	}

	// The new term-2 leader asks for a sync and answers with a truncation.
	g.AddEvent(admKey, types.Event{Type: types.Ping, Term: 2, From: 2, To: 1})
	g.AddEvent(admKey, types.Event{Type: types.NotifySync, Term: 2, From: 2, To: 1})
	g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 2, From: 2, To: 1, Reason: types.ReasonTruncate, Index: 10})
	g.Advance()
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, storage.truncates.Load(), "truncation reached storage while a store was in flight")

	close(gate)
	released = true
	require.Eventually(t, func() bool { return storage.truncates.Load() == 1 }, 5*time.Second, time.Millisecond, "deferred truncation runs once the store returns")
	// Observe the node through its sync instead of reading loop state: after
	// the truncation it syncs from the truncated index.
	deadline := time.After(5 * time.Second)
	for {
		var e types.Event
		// The group ticks hourly here, so drive the loop to step the
		// TruncateResp.
		g.Advance()
		select {
		case e = <-tr.events:
		case <-time.After(time.Millisecond):
			continue
		case <-deadline:
			t.Fatal("follower did not resume syncing")
		}
		if e.Type == types.SyncReq && e.To == 2 && e.Index == 11 {
			break
		}
	}
	require.EqualValues(t, 10, storage.lastIndex(), "no discarded entry was written back")
}

// awaitSync drives the loop until the follower sends a SyncReq to leader 2
// starting at index.
func awaitSync(t *testing.T, g *RaftGroup, tr *recordingTransport, index uint64) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		g.Advance()
		select {
		case e := <-tr.events:
			if e.Type == types.SyncReq && e.To == 2 && e.Index == index {
				return
			}
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatalf("no sync from index %d", index)
		}
	}
}

// The follower receives more entries while the old leader's tail is being
// stored, so its queue holds an unstored tail past the in-flight store. After
// the deferred truncation, only the new leader's entries may reach storage.
func TestRaftGroupDeferredTruncationDropsTailBeyondStore(t *testing.T) {
	gate := make(chan struct{})
	storage := &truncateCountingStorage{memStorage: &memStorage{appendGate: gate}, storing: make(chan struct{}, 1)}
	tr := &recordingTransport{events: make(chan types.Event, 256)}
	g := New(NewOptions(WithTransport(tr), WithStorage(storage), WithTickInterval(time.Hour)))
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(admKey), raft.WithNodeId(1), raft.WithReplicas([]uint64{1, 2, 3})))
	node.BecomeFollower(1, 3)
	g.AddRaft(node)
	require.NoError(t, g.Start())
	released := false
	t.Cleanup(func() {
		if !released {
			close(gate)
		}
		g.Stop()
		g.goPool.Release()
	})
	logs := func(term uint32, first, last uint64) []types.Log {
		out := make([]types.Log, 0, last-first+1)
		for i := first; i <= last; i++ {
			out = append(out, types.Log{Index: i, Term: term, Id: uint64(term)*1000 + i, Data: []byte{byte(term)}})
		}
		return out
	}
	g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 1, From: 3, To: 1, Reason: types.ReasonOk, Logs: logs(1, 1, 20)})
	g.Advance()
	select {
	case <-storage.storing:
	case <-time.After(5 * time.Second):
		t.Fatal("store not dispatched")
	}
	// More of the old leader's log arrives while 1..20 is still being stored.
	g.AddEvent(admKey, types.Event{Type: types.NotifySync, Term: 1, From: 3, To: 1})
	g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 1, From: 3, To: 1, Reason: types.ReasonOk, Logs: logs(1, 21, 25)})
	// The new term-2 leader truncates below the in-flight store.
	g.AddEvent(admKey, types.Event{Type: types.Ping, Term: 2, From: 2, To: 1})
	g.AddEvent(admKey, types.Event{Type: types.NotifySync, Term: 2, From: 2, To: 1})
	g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 2, From: 2, To: 1, Reason: types.ReasonTruncate, Index: 10})
	g.Advance()
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, storage.truncates.Load())

	close(gate)
	released = true
	require.Eventually(t, func() bool { return storage.truncates.Load() == 1 }, 5*time.Second, time.Millisecond)
	awaitSync(t, g, tr, 11)
	g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 2, From: 2, To: 1, Reason: types.ReasonOk, Logs: logs(2, 11, 15)})
	awaitSync(t, g, tr, 16) // stored and synced again: no panic on the way
	stored, err := storage.GetLogs(admKey, 1, 0, 0)
	require.NoError(t, err)
	require.Len(t, stored, 15, "nothing past the new leader's entries")
	for i, l := range stored {
		want := uint32(1)
		if l.Index > 10 {
			want = 2
		}
		require.EqualValues(t, i+1, l.Index)
		require.Equal(t, want, l.Term, "index %d", l.Index)
	}
}
