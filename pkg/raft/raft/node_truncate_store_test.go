package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func termLogs(term uint32, first, last uint64) []types.Log {
	logs := make([]types.Log, 0, last-first+1)
	for i := first; i <= last; i++ {
		logs = append(logs, types.Log{Index: i, Term: term, Data: []byte("x")})
	}
	return logs
}

// A follower stores the tail it received from the term-1 leader. Before that
// store returns, the new term-2 leader asks it to sync and answers with a
// truncation below the tail. The store and the truncation run as independent
// storage tasks, so the truncation can complete first. Its StoreResp then
// reports an index the queue no longer holds.
//
// The truncation must wait for the in-flight store: otherwise the follower
// panics on the late StoreResp, or the late write lands after the truncation
// and leaves the old leader's entries in storage.
func TestFollowerTruncationWaitsForInFlightStore(t *testing.T) {
	for _, learner := range []bool{false, true} {
		name := "follower"
		if learner {
			name = "learner"
		}
		t.Run(name, func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2, 3})
			become := func(term uint32, leader uint64) {
				if learner {
					n.BecomeLearner(term, leader)
				} else {
					n.BecomeFollower(term, leader)
				}
			}
			become(1, 3)
			clearEvents(n)
			n.onlySync = true
			// The old leader's tail 1..20 arrives and is handed to storage.
			n.Step(types.Event{Type: types.SyncResp, Term: 1, From: 3, Reason: types.ReasonOk, Logs: termLogs(1, 1, 20)})
			store, ok := findEvent(collectEvents(n), types.StoreReq)
			require.True(t, ok, "tail is handed to storage")
			require.EqualValues(t, 20, store.Logs[len(store.Logs)-1].Index)

			// A new leader takes over and the follower syncs with it while
			// the store is still in flight.
			become(2, 2)
			clearEvents(n)
			n.Step(types.Event{Type: types.NotifySync, Term: 2, From: 2})
			_, ok = findEvent(collectEvents(n), types.SyncReq)
			require.True(t, ok)
			n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonTruncate, Index: 10})
			events := collectEvents(n)
			if trunc, sent := findEvent(events, types.TruncateReq); sent {
				// The truncation already reached storage: complete it first,
				// as a faster storage task would.
				require.NotPanics(t, func() {
					n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonOk, Index: trunc.Index})
					n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 20, Logs: store.Logs})
				}, "late StoreResp after truncation")
				t.Fatal("truncation was dispatched while a store was in flight")
			}
			require.True(t, n.truncating, "no sync while the truncation is pending")
			_, ok = findEvent(events, types.StoreReq)
			require.False(t, ok, "no new store while the truncation is pending")

			// The store lands; only now is the truncation dispatched.
			n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 20, Logs: store.Logs})
			trunc, ok := findEvent(collectEvents(n), types.TruncateReq)
			require.True(t, ok, "deferred truncation is dispatched after the store")
			require.EqualValues(t, 10, trunc.Index)
			n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonOk, Index: 10})
			require.False(t, n.truncating)
			require.EqualValues(t, 10, n.queue.lastLogIndex)
			require.EqualValues(t, 10, n.queue.storedIndex)
			req, ok := findEvent(collectEvents(n), types.SyncReq)
			require.True(t, ok, "sync resumes from the truncated index")
			require.EqualValues(t, 11, req.Index)
		})
	}
}

// A failed in-flight store also releases a deferred truncation.
func TestFollowerDeferredTruncationAfterFailedStore(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	n.BecomeFollower(1, 3)
	clearEvents(n)
	n.onlySync = true
	n.Step(types.Event{Type: types.SyncResp, Term: 1, From: 3, Reason: types.ReasonOk, Logs: termLogs(1, 1, 20)})
	clearEvents(n)
	n.BecomeFollower(2, 2)
	clearEvents(n)
	n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonTruncate, Index: 10})
	_, sent := findEvent(collectEvents(n), types.TruncateReq)
	require.False(t, sent)
	n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonError})
	trunc, ok := findEvent(collectEvents(n), types.TruncateReq)
	require.True(t, ok)
	require.EqualValues(t, 10, trunc.Index)
}

// Without a store in flight the truncation is dispatched immediately.
func TestFollowerTruncationWithoutInFlightStore(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 3, 2)
	n.Step(types.Event{Type: types.SyncResp, Term: 3, From: 2, Reason: types.ReasonTruncate, Index: 5})
	trunc, ok := findEvent(collectEvents(n), types.TruncateReq)
	require.True(t, ok)
	require.EqualValues(t, 5, trunc.Index)
}

// A deferred truncation that never reached storage is dropped on a role
// change, so the node can sync with the next leader; a leader is never held
// from storing by truncation state.
func TestDeferredTruncationDroppedOnRoleChange(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	n.BecomeFollower(1, 3)
	clearEvents(n)
	n.onlySync = true
	n.Step(types.Event{Type: types.SyncResp, Term: 1, From: 3, Reason: types.ReasonOk, Logs: termLogs(1, 1, 20)})
	clearEvents(n)
	n.Step(types.Event{Type: types.SyncResp, Term: 1, From: 3, Reason: types.ReasonTruncate, Index: 10})
	require.True(t, n.truncating)
	n.BecomeFollower(2, 2)
	require.False(t, n.truncating)
	require.False(t, n.hasDeferredTruncate)
	n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 20})
	_, sent := findEvent(collectEvents(n), types.TruncateReq)
	require.False(t, sent, "the dropped truncation is not dispatched later")

	// Truncation state does not hold a leader's stores.
	l := newTestNode(1, []uint64{1})
	l.BecomeLeader(1)
	clearEvents(l)
	l.truncating = true
	require.NoError(t, l.queue.append(types.Log{Index: l.queue.lastLogIndex + 1, Term: 1, Id: 1, Data: []byte("x")}))
	_, ok := findEvent(collectEvents(l), types.StoreReq)
	require.True(t, ok)
}

// requireQueueShape checks that the unstored slice is exactly
// (storedIndex, lastLogIndex], in order.
func requireQueueShape(t *testing.T, q *queue) {
	t.Helper()
	require.EqualValues(t, q.lastLogIndex-q.storedIndex, len(q.logs), "unstored slice length")
	for i, l := range q.logs {
		require.EqualValues(t, q.storedIndex+1+uint64(i), l.Index, "unstored slice position %d", i)
	}
}

// The follower keeps syncing while a store is in flight, so its queue can
// hold an unstored tail past the range being stored. A deferred truncation
// below that range must discard the tail too: otherwise the next store writes
// the discarded old-term entries back and its StoreResp panics.
func TestDeferredTruncationDropsUnstoredTailBeyondStore(t *testing.T) {
	for _, learner := range []bool{false, true} {
		name := "follower"
		if learner {
			name = "learner"
		}
		t.Run(name, func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2, 3})
			become := func(term uint32, leader uint64) {
				if learner {
					n.BecomeLearner(term, leader)
				} else {
					n.BecomeFollower(term, leader)
				}
			}
			become(1, 3)
			clearEvents(n)
			n.onlySync = true
			n.Step(types.Event{Type: types.SyncResp, Term: 1, From: 3, Reason: types.ReasonOk, Logs: termLogs(1, 1, 20)})
			store, ok := findEvent(collectEvents(n), types.StoreReq)
			require.True(t, ok)
			// Sync is not held by the in-flight store: 21..25 arrive meanwhile.
			n.Step(types.Event{Type: types.NotifySync, Term: 1, From: 3})
			_, ok = findEvent(collectEvents(n), types.SyncReq)
			require.True(t, ok)
			n.Step(types.Event{Type: types.SyncResp, Term: 1, From: 3, Reason: types.ReasonOk, Logs: termLogs(1, 21, 25)})
			clearEvents(n)
			require.EqualValues(t, 25, n.queue.lastLogIndex)

			become(2, 2)
			clearEvents(n)
			n.Step(types.Event{Type: types.NotifySync, Term: 2, From: 2})
			clearEvents(n)
			n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonTruncate, Index: 10})
			_, sent := findEvent(collectEvents(n), types.TruncateReq)
			require.False(t, sent, "deferred behind the in-flight store")
			n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 20, Logs: store.Logs})
			trunc, ok := findEvent(collectEvents(n), types.TruncateReq)
			require.True(t, ok)
			n.Step(types.Event{Type: types.TruncateResp, Reason: types.ReasonOk, Index: trunc.Index})
			clearEvents(n)
			require.EqualValues(t, 10, n.queue.storedIndex)
			require.EqualValues(t, 10, n.queue.lastLogIndex)
			requireQueueShape(t, n.queue)

			n.Step(types.Event{Type: types.SyncResp, Term: 2, From: 2, Reason: types.ReasonOk, Logs: termLogs(2, 11, 15)})
			next, ok := findEvent(collectEvents(n), types.StoreReq)
			require.True(t, ok)
			require.Len(t, next.Logs, 5)
			for i, l := range next.Logs {
				require.EqualValues(t, 11+uint64(i), l.Index)
				require.EqualValues(t, 2, l.Term, "only the new leader's entries are stored")
			}
			require.NotPanics(t, func() {
				n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 15, Logs: next.Logs})
			})
			require.EqualValues(t, 15, n.queue.storedIndex)
			require.EqualValues(t, 15, n.queue.lastLogIndex)
			requireQueueShape(t, n.queue)
		})
	}
}

// truncateLogTo keeps the unstored slice equal to (storedIndex, lastLogIndex]
// for both of its callers, and never lets later appends write into memory a
// store in flight may still read.
func TestQueueTruncateKeepsUnstoredSliceExact(t *testing.T) {
	newQ := func() (*queue, []types.Log) {
		q := newQueue("test", 0, 0)
		require.NoError(t, q.append(termLogs(1, 1, 15)...))
		q.nextStoreLogs(10) // 1..10 handed to storage
		q.storeTo(10)
		require.NoError(t, q.append(termLogs(1, 16, 18)...))
		requireQueueShape(t, q)
		return q, append([]types.Log(nil), q.logs...)
	}
	t.Run("below_stored", func(t *testing.T) {
		q, _ := newQ()
		q.truncateLogTo(7)
		require.EqualValues(t, 7, q.storedIndex)
		require.EqualValues(t, 7, q.lastLogIndex)
		requireQueueShape(t, q)
		require.NoError(t, q.append(termLogs(2, 8, 9)...))
		requireQueueShape(t, q)
		require.Equal(t, termLogs(2, 8, 9), q.nextStoreLogs(0))
	})
	t.Run("above_stored", func(t *testing.T) {
		q, _ := newQ()
		q.truncateLogTo(12)
		require.EqualValues(t, 10, q.storedIndex)
		require.EqualValues(t, 12, q.lastLogIndex)
		requireQueueShape(t, q)
	})
	t.Run("append_overlap_below_stored", func(t *testing.T) {
		q, _ := newQ()
		require.NoError(t, q.append(termLogs(1, 8, 20)...))
		requireQueueShape(t, q)
		require.EqualValues(t, 20, q.lastLogIndex)
	})
	t.Run("in_flight_slice_untouched", func(t *testing.T) {
		q, _ := newQ()
		inFlight := q.nextStoreLogs(0) // 11..18 in flight
		want := append([]types.Log(nil), inFlight...)
		q.truncateLogTo(13)
		require.NoError(t, q.append(termLogs(3, 14, 18)...))
		require.Equal(t, want, inFlight, "truncate and append rewrote a slice held by an in-flight store")
		requireQueueShape(t, q)
	})
}
