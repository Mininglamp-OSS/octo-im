package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestMultiTermSyncReportsDurableTailTerm(t *testing.T) {
	n := newTestNode(2, []uint64{1, 2})
	n.BecomeFollower(3, 1)
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 3,
		Reason: types.ReasonOk, Logs: []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 2},
			{Index: 3, Term: 2}, {Index: 4, Term: 3}}}))
	stored, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Index: stored.Logs[len(stored.Logs)-1].Index, Reason: types.ReasonOk}))
	request, ok := findEvent(n.Ready(), types.SyncReq)
	require.True(t, ok)
	require.Equal(t, uint32(3), request.LastLogTerm, "the first log's term must not masquerade as the durable tail's term")
}

func TestEmptyLeaderCannotVerifyNonemptyReplicaPrefix(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2})
	n.BecomeLeader(3)
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 2, Term: 3,
		Index: 5, StoredIndex: 5, LastLogTerm: 2}))
	response, ok := findEvent(n.Ready(), types.SyncResp)
	require.True(t, ok)
	require.NotEqual(t, types.ReasonOk, response.Reason)
}
