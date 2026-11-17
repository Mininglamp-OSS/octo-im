package raftgroup

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestARCH_NEW_1_GetLogsBackpressureReturnsReasonErrorNotOk(t *testing.T) {
	tr := &captureTransport{}
	rg := New(NewOptions(
		WithTransport(tr),
		WithMaxLogSizePerBatch(1024),
		WithMaxInFlightGetLogs(1),
		WithMaxInFlightGetLogsBytes(1024),
	))
	t.Cleanup(rg.Stop)

	leader := raft.NewNode(1, types.RaftState{
		LastLogIndex: 5,
		LastTerm:     1,
		AppliedIndex: 5,
	}, raft.NewOptions(
		raft.WithKey("ch-bp-reason"),
		raft.WithNodeId(1),
		raft.WithReplicas([]uint64{1, 2}),
	))
	require.NoError(t, leader.Step(types.Event{
		Type: types.ConfChange,
		Config: types.Config{
			Replicas: []uint64{1, 2},
			Leader:   1,
			Term:     1,
			Version:  1,
			Role:     types.RoleLeader,
		},
	}))
	_ = leader.Ready()
	rg.AddRaft(leader)

	// Occupy the sole global GetLogs permit on another key.
	relOther, ok := rg.tryAcquireGetLogs("other-ch", 1024)
	require.True(t, ok)
	defer relOther()

	// Follower 2 sends bulk catch-up SyncReq for index 2 (StoredIndex=5 >= Index=2).
	rg.handleGetLogsReq(leader, types.Event{
		Type:        types.GetLogsReq,
		From:        2,
		To:          1,
		Term:        1,
		Index:       2,
		StoredIndex: 5,
		Reason:      types.ReasonOnlySync,
	})

	queued := rg.mq.Get()
	require.Len(t, queued, 1)
	resp := queued[0].Event
	require.Equal(t, types.GetLogsResp, resp.Type)
	require.Equal(t, uint64(2), resp.To)
	require.Equal(t, uint64(2), resp.Index)
	require.Equal(t, types.ReasonError, resp.Reason, "ARCH-NEW-1: GetLogs backpressure must return ReasonError, never empty ReasonOk")
}
