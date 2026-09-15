package raftgroup_test

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// select evaluates channel expressions before choosing a ready case. The
// second Done call is after the request was accepted by the real event loop.
// A subsequent ConfChange round trip proves that handleReadState has returned
// (and its buffered snapshot has been delivered) before cancellation fires.
type cancelAfterSnapshotContext struct {
	context.Context
	calls         int
	afterDelivery func()
}

func (c *cancelAfterSnapshotContext) Done() <-chan struct{} {
	c.calls++
	if c.calls == 2 {
		c.afterDelivery()
	}
	return c.Context.Done()
}

func TestReadLeaderStateDeliveredSnapshotWinsCancellation(t *testing.T) {
	rg := raftgroup.New(raftgroup.NewOptions(raftgroup.WithTickInterval(time.Hour)))
	require.NoError(t, rg.Start())
	defer rg.Stop()
	n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey("channel"), raft.WithNodeId(1)))
	rg.AddRaft(n)
	cfg := types.Config{Leader: 1, Term: 2, Version: 10, Role: types.RoleLeader, Replicas: []uint64{1, 2}}
	require.NoError(t, rg.AddEventWait("channel", types.Event{Type: types.ConfChange, Config: cfg}))
	for i := 0; i < 30; i++ {
		parent, cancel := context.WithCancel(context.Background())
		ctx := &cancelAfterSnapshotContext{Context: parent, afterDelivery: func() {
			require.NoError(t, rg.AddEventWait("channel", types.Event{Type: types.ConfChange, Config: cfg}))
			cancel()
		}}
		state, err := rg.ReadLeaderState(ctx, "channel")
		cancel()
		require.ErrorIs(t, parent.Err(), context.Canceled)
		require.NoError(t, err, "a snapshot already delivered by the real event loop is a successful read")
		require.True(t, state.Ready)
		require.Equal(t, uint64(10), state.ConfigVersion)
	}
}
