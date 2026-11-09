package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestOldTruncateCompletionCannotCertifyNewLeader(t *testing.T) {
	for _, learner := range []bool{false, true} {
		t.Run(map[bool]string{false: "follower", true: "learner"}[learner], func(t *testing.T) {
			n := NewNode(3, types.RaftState{LastLogIndex: 8, LastTerm: 2, AppliedIndex: 2}, NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3})))
			if learner {
				makeLearner(n, 4, 1)
			} else {
				makeFollower(n, 4, 1)
			}
			require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 4, Reason: types.ReasonTruncate, Index: 5}))
			request, ok := findEvent(collectEvents(n), types.TruncateReq)
			require.True(t, ok)
			require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 3, Term: 5, CommittedIndex: 5}))
			require.Equal(t, uint64(2), n.CommittedIndex())
			// Complete the old leader's already submitted physical truncation.
			require.NoError(t, n.Step(types.Event{Type: types.TruncateResp, Term: request.Term, TruncateID: request.TruncateID, Index: request.Index, Reason: types.ReasonOk}))
			require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 3, Term: 5, CommittedIndex: 5}))
			require.Equal(t, uint64(2), n.CommittedIndex(), "old storage completion is not prefix validation by leader 3")
			require.Equal(t, uint64(5), n.LastLogIndex(), "owner must reconcile the completed physical cut even across terms")
			require.False(t, n.truncating, "discarding an old-term completion must not strand the barrier")
			syncReq, ok := findEvent(collectEvents(n), types.SyncReq)
			require.True(t, ok)
			require.Equal(t, uint64(3), syncReq.To)
			require.NotEqual(t, types.ReasonOnlySync, syncReq.Reason)
		})
	}
}

func TestTruncateFailureKeepsBarrierAndIgnoresObsoleteRetry(t *testing.T) {
	n := NewNode(3, types.RaftState{LastLogIndex: 8, LastTerm: 2, AppliedIndex: 2}, NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3})))
	makeFollower(n, 4, 1)
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 4, Reason: types.ReasonTruncate, Index: 5}))
	first, ok := findEvent(collectEvents(n), types.TruncateReq)
	require.True(t, ok)
	require.NoError(t, n.Step(types.Event{Type: types.TruncateResp, TruncateID: first.TruncateID, Term: first.Term, Index: first.Index, Reason: types.ReasonError}))
	makeLeader(n, 5)
	require.False(t, n.LeaderReadReady())
	require.Error(t, n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Index: 9, Term: 5}}}))
	n.Tick()
	retry, ok := findEvent(collectEvents(n), types.TruncateReq)
	require.True(t, ok)
	require.Greater(t, retry.TruncateID, first.TruncateID)
	require.Equal(t, first.Index, retry.Index)
	require.NoError(t, n.Step(types.Event{Type: types.TruncateResp, TruncateID: first.TruncateID, Term: first.Term, Index: first.Index, Reason: types.ReasonOk}))
	require.True(t, n.truncating, "an obsolete completion cannot release the retry's barrier")
	require.NoError(t, n.Step(types.Event{Type: types.TruncateResp, TruncateID: retry.TruncateID, Term: retry.Term, Index: retry.Index, Reason: types.ReasonOk, TermStartIndexInfo: &types.TermStartIndexInfo{Term: 2, Index: 3}}))
	require.False(t, n.truncating)
	require.Equal(t, uint64(5), n.LastLogIndex())
	require.NoError(t, n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Index: 6, Term: 5}}}), "new writes resume only after disk and queue agree")
}

func TestTruncateWaitsForInflightAppend(t *testing.T) {
	n := NewNode(3, types.RaftState{LastLogIndex: 8, LastTerm: 2, AppliedIndex: 2}, NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3})))
	makeFollower(n, 4, 1)
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 4, Reason: types.ReasonOk, Logs: []types.Log{{Index: 9, Term: 4}}}))
	_, ok := findEvent(collectEvents(n), types.StoreReq)
	require.True(t, ok)
	require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 3, Term: 5}))
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 3, Term: 5, Reason: types.ReasonTruncate, Index: 5}))
	require.Zero(t, countEvents(collectEvents(n), types.TruncateReq), "cannot race a physical append already in the worker pool")
	require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Reason: types.ReasonOk, Index: 9}))
	req, ok := findEvent(collectEvents(n), types.SyncReq)
	require.True(t, ok)
	require.NotEqual(t, types.ReasonOnlySync, req.Reason)
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 3, Term: 5, Reason: types.ReasonTruncate, Index: 5}))
	require.Equal(t, 1, countEvents(collectEvents(n), types.TruncateReq))
	// A higher-term response cannot append over the pending physical cut.
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 6, Reason: types.ReasonOk, Logs: []types.Log{{Index: 10, Term: 6}}}))
	require.Equal(t, uint64(9), n.LastLogIndex())
	require.Zero(t, countEvents(collectEvents(n), types.StoreReq))
	n.campaign()
	require.Equal(t, types.RoleFollower, n.cfg.Role)
}

type truncateCallbackStorage struct {
	Storage
	fail             string
	entered, release chan struct{}
}

func (*truncateCallbackStorage) GetState() (types.RaftState, error)       { return types.RaftState{}, nil }
func (*truncateCallbackStorage) GetTermStartIndex(uint32) (uint64, error) { return 3, nil }
func (*truncateCallbackStorage) GetLogs(start, _, _ uint64) ([]types.Log, error) {
	return []types.Log{{Index: start, Term: 2}}, nil
}
func (s *truncateCallbackStorage) TruncateLogTo(uint64) error {
	close(s.entered)
	<-s.release
	if s.fail == "truncate" {
		return errors.New("truncate failure")
	}
	return nil
}
func (s *truncateCallbackStorage) DeleteLeaderTermStartIndexGreaterThanTerm(uint32) error {
	if s.fail == "metadata" {
		return errors.New("metadata failure")
	}
	return nil
}

func TestStandaloneTruncateWorkerRetainsRequestIdentity(t *testing.T) {
	for _, fail := range []string{"", "truncate", "metadata"} {
		t.Run("failure="+fail, func(t *testing.T) {
			storage := &truncateCallbackStorage{fail: fail, entered: make(chan struct{}), release: make(chan struct{})}
			r := New(NewOptions(WithStorage(storage)))
			t.Cleanup(r.pool.Release)
			r.handleTruncateReq(types.Event{Type: types.TruncateReq, Index: 5, Term: 4, TruncateID: 17})
			select {
			case <-storage.entered:
			case <-time.After(time.Second):
				t.Fatal("worker did not start")
			}
			close(storage.release)
			select {
			case got := <-r.stepC:
				require.Equal(t, types.TruncateResp, got.event.Type)
				require.Equal(t, uint64(17), got.event.TruncateID)
				require.Equal(t, uint32(4), got.event.Term)
				require.Equal(t, uint64(5), got.event.Index)
				if fail == "" {
					require.Equal(t, types.ReasonOk, got.event.Reason)
				} else {
					require.Equal(t, types.ReasonError, got.event.Reason)
				}
			case <-time.After(time.Second):
				t.Fatal("worker did not complete")
			}
		})
	}
}

func TestPendingTruncateBlocksNewLeaderWrites(t *testing.T) {
	n := NewNode(3, types.RaftState{LastLogIndex: 8, LastTerm: 2, AppliedIndex: 2}, NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3})))
	makeFollower(n, 4, 1)
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 4, Reason: types.ReasonTruncate, Index: 5}))
	clearEvents(n)
	makeLeader(n, 5)
	require.Error(t, n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Index: 9, Term: 5}}}), "new writes cannot race an old pending physical cut")
	require.Equal(t, uint64(8), n.LastLogIndex())
}

func TestDelayedTruncateResponseCannotCutPastCurrentTail(t *testing.T) {
	for _, learner := range []bool{false, true} {
		t.Run(map[bool]string{false: "follower", true: "learner"}[learner], func(t *testing.T) {
			n := NewNode(3, types.RaftState{LastLogIndex: 8, LastTerm: 2, AppliedIndex: 2}, NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3})))
			if learner {
				makeLearner(n, 4, 1)
			} else {
				makeFollower(n, 4, 1)
			}
			require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 4, Reason: types.ReasonTruncate, Index: 2}))
			request, ok := findEvent(collectEvents(n), types.TruncateReq)
			require.True(t, ok)
			require.NoError(t, n.Step(types.Event{Type: types.TruncateResp, TruncateID: request.TruncateID, Term: request.Term, Index: 2, Reason: types.ReasonOk, TermStartIndexInfo: &types.TermStartIndexInfo{Term: 1, Index: 1}}))
			clearEvents(n)
			for _, cut := range []uint64{6, 2} {
				require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 4, Reason: types.ReasonTruncate, Index: cut}))
				require.False(t, n.truncating, "stale no-op cut must not enter an unrecoverable storage retry")
				events := collectEvents(n)
				require.Zero(t, countEvents(events, types.TruncateReq))
				current, ok := findEvent(events, types.SyncReq)
				require.True(t, ok)
				require.Equal(t, uint64(3), current.Index)
				require.NotEqual(t, types.ReasonOnlySync, current.Reason)
			}
		})
	}
}
