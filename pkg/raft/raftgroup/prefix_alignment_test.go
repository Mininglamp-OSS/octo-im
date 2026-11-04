package raftgroup

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type prefixAlignmentStorage struct {
	IStorage
	logs         []types.Log
	deletedAbove uint32
}

func (s *prefixAlignmentStorage) TruncateLogTo(_ string, index uint64) error {
	s.logs = s.logs[:index]
	return nil
}
func (s *prefixAlignmentStorage) DeleteLeaderTermStartIndexGreaterThanTerm(_ string, term uint32) error {
	s.deletedAbove = term
	return nil
}

type prefixAlignmentRaft struct{ *lifecycleRaft }

func (*prefixAlignmentRaft) LastLogIndex() uint64 { return 6 }

func (s *prefixAlignmentStorage) GetLogs(_ string, start, end, _ uint64) ([]types.Log, error) {
	var logs []types.Log
	for _, log := range s.logs {
		if log.Index >= start && log.Index < end {
			logs = append(logs, log)
		}
	}
	return logs, nil
}
func (s *prefixAlignmentStorage) GetTermStartIndex(_ string, term uint32) (uint64, error) {
	for _, log := range s.logs {
		if log.Term == term {
			return log.Index, nil
		}
	}
	return 0, nil
}
func (s *prefixAlignmentStorage) LeaderLastLogTerm(string) (uint32, error) {
	if len(s.logs) == 0 {
		return 0, nil
	}
	return s.logs[len(s.logs)-1].Term, nil
}
func (s *prefixAlignmentStorage) LeaderTermGreaterEqThan(_ string, term uint32) (uint32, error) {
	for _, log := range s.logs {
		if log.Term >= term {
			return log.Term, nil
		}
	}
	return 0, nil
}

func TestPrefixAlignmentFindsActualCommonPrefix(t *testing.T) {
	storage := &prefixAlignmentStorage{logs: []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1}, {Index: 5, Term: 1}, {Index: 6, Term: 3}}}
	g := New(NewOptions(WithStorage(storage)))
	defer g.Stop()
	r := &prefixAlignmentRaft{&lifecycleRaft{waitTestRaft: &waitTestRaft{}}}
	g.AddRaft(r)
	// The follower has term 1 through index 2 and divergent term 2 through 8.
	follower := []uint32{1, 1, 2, 2, 2, 2, 2, 2}
	for attempts := 0; attempts < 10; attempts++ {
		var term uint32
		if len(follower) > 0 {
			term = follower[len(follower)-1]
		}
		r.events = nil
		g.handleGetLogsReq(r, types.Event{Index: uint64(len(follower) + 1), StoredIndex: 6, LastLogTerm: term})
		g.workers.Wait()
		g.handleReceivedEvents()
		require.Len(t, r.events, 1)
		cut, reason := r.events[0].Index, r.events[0].Reason
		if reason == types.ReasonOk {
			require.Len(t, follower, 2, "alignment must not certify any term-2 divergent entry")
			return
		}
		require.Equal(t, types.ReasonTruncate, reason)
		require.GreaterOrEqual(t, cut, uint64(2), "preserve the committed common prefix")
		require.Less(t, cut, uint64(len(follower)), "each repair round must progress")
		follower = follower[:cut]
	}
	t.Fatal("alignment did not converge")
}

func TestPrefixAlignmentRepairsAnEmptyCommonPrefix(t *testing.T) {
	g := New(NewOptions(WithStorage(&prefixAlignmentStorage{})))
	defer g.Stop()
	cut, reason := g.getTrunctLogIndex("slot", 0, types.Event{Index: 2, StoredIndex: 0, LastLogTerm: 1})
	require.Equal(t, types.ReasonTruncate, reason)
	require.Zero(t, cut)
}

func TestTruncateCompletionRestoresRetainedTerm(t *testing.T) {
	storage := &prefixAlignmentStorage{logs: []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 2}}}
	g := New(NewOptions(WithStorage(storage)))
	defer g.Stop()
	r := &lifecycleRaft{waitTestRaft: &waitTestRaft{}}
	g.AddRaft(r)
	g.handleTruncateReq(r, types.Event{Index: 2, LastLogTerm: 2})
	g.workers.Wait()
	g.handleReceivedEvents()
	require.Len(t, r.events, 1)
	require.Equal(t, types.ReasonOk, r.events[0].Reason)
	require.Equal(t, &types.TermStartIndexInfo{Term: 1, Index: 1}, r.events[0].TermStartIndexInfo)
	require.Equal(t, uint32(1), storage.deletedAbove)
}
