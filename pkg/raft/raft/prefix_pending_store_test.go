package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestPrefixProbeWaitsForEveryPendingTermToBeStored(t *testing.T) {
	n := newTestNode(2, []uint64{1, 2, 3})
	makeFollower(n, 4, 1)
	require.NoError(t, n.queue.append(types.Log{Index: 1, Term: 1}, types.Log{Index: 2, Term: 3}))
	n.sendSyncReq()
	require.Zero(t, countEvents(n.events, types.SyncReq))
	for _, index := range []uint64{1, 2} {
		events := n.Ready()
		_, ok := findEvent(events, types.StoreReq)
		require.True(t, ok)
		require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Index: index, Reason: types.ReasonOk}))
		if index == 1 {
			require.Zero(t, countEvents(n.events, types.SyncReq))
		}
	}
	e, ok := findEvent(n.events, types.SyncReq)
	require.True(t, ok)
	require.Equal(t, uint64(3), e.Index)
	require.Equal(t, uint64(3), e.StoredIndex)
	require.Equal(t, uint32(3), e.LastLogTerm)
	require.NotEqual(t, types.ReasonOnlySync, e.Reason)
}

func TestEmptyLeaderDoesNotConfirmOldFollowerTail(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 3)
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, From: 2, Term: 3, Index: 2, StoredIndex: 2, LastLogTerm: 1}))
	events := n.Ready()
	require.Zero(t, countEvents(events, types.SyncResp), "a nonempty old tail needs an actual alignment response")
	e, ok := findEvent(events, types.GetLogsReq)
	require.True(t, ok)
	require.Zero(t, e.StoredIndex)
}
