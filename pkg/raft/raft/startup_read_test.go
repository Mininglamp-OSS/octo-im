package raft

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStartupReadCompletionFencedAcrossConfig(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 3)
	n.cfg.Version = 10
	n.replicaSync[2] = &SyncInfo{GetingLogs: true}
	n.sendGetLogsReq(types.Event{From: 2, Term: 3})
	old := n.replicaSync[2].readID
	n.Ready()
	cfg := n.cfg.Clone()
	cfg.Version++
	require.NoError(t, n.switchConfig(cfg))
	n.Ready()
	stale := types.Event{Type: types.GetLogsResp, To: 2, LocalReadID: old, Reason: types.ReasonOk, Index: 1}
	require.NoError(t, n.Step(stale))
	require.Empty(t, n.Ready())
	n.replicaSync[2] = &SyncInfo{GetingLogs: true}
	n.sendGetLogsReq(types.Event{From: 2, Term: 3})
	fresh := n.replicaSync[2].readID
	n.Ready()
	require.NotEqual(t, old, fresh)
	require.NoError(t, n.Step(stale))
	require.True(t, n.replicaSync[2].GetingLogs)
	require.Empty(t, n.Ready())
	stale.LocalReadID = fresh
	require.NoError(t, n.Step(stale))
	require.False(t, n.replicaSync[2].GetingLogs)
	require.NotEmpty(t, n.Ready())
	require.NoError(t, n.Step(stale))
	require.Empty(t, n.Ready(), "duplicate completion")
}
func TestStartupReadIdentityNotReusedByReplacement(t *testing.T) {
	var ids []uint64
	for i := 0; i < 2; i++ {
		n := newTestNode(1, []uint64{1, 2, 3})
		makeLeader(n, 3)
		n.replicaSync[2] = &SyncInfo{GetingLogs: true}
		n.sendGetLogsReq(types.Event{From: 2})
		ids = append(ids, n.replicaSync[2].readID)
	}
	require.NotEqual(t, ids[0], ids[1])
}
