package raftgroup_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestProposeAfterReadAllowsUnrelatedLogsAndFencesTermChanges(t *testing.T) {
	rg := raftgroup.New(newTestOptions(raftgroup.WithStorage(newTestStorage())))
	require.NoError(t, rg.Start())
	t.Cleanup(rg.Stop)
	n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey("slot"), raft.WithNodeId(1)))
	rg.AddRaft(n)
	cfg := types.Config{Leader: 1, Term: 2, Version: 1, Role: types.RoleLeader, Replicas: []uint64{1}}
	require.NoError(t, rg.AddEventWait("slot", types.Event{Type: types.ConfChange, Config: cfg}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	before, err := rg.ReadLeaderState(ctx, "slot")
	require.NoError(t, err)
	validate := func() error { return nil }
	index, err := rg.ProposeAfterRead(ctx, "slot", before, 1, []byte("first"), validate)
	require.NoError(t, err)
	require.Equal(t, uint64(1), index)
	index, err = rg.ProposeAfterRead(ctx, "slot", before, 2, []byte("unrelated log progress"), validate)
	require.NoError(t, err, "log progress alone does not invalidate a revalidated decision")
	require.Equal(t, uint64(2), index)
	before, err = rg.ReadLeaderState(ctx, "slot")
	require.NoError(t, err)
	cfg.Term++
	require.NoError(t, rg.AddEventWait("slot", types.Event{Type: types.ConfChange, Config: cfg}))
	_, err = rg.ProposeAfterRead(ctx, "slot", before, 3, []byte("old term"), validate)
	require.ErrorIs(t, err, raftgroup.ErrReadStateChanged)
	state, err := rg.ReadLeaderState(ctx, "slot")
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.AppliedIndex)
	index, err = rg.ProposeAfterRead(ctx, "slot", state, 4, []byte("new term"), validate)
	require.NoError(t, err)
	require.Equal(t, uint64(3), index)
	_, err = rg.ProposeAfterRead(ctx, "slot", state, 5, []byte("unchecked"), nil)
	require.ErrorContains(t, err, "revalidation is required")
}

func TestProposeAfterReadDrainsPrefixAndRevalidates(t *testing.T) {
	for _, mode := range []string{"success", "stale decision", "term changes during validation", "cancel while draining"} {
		t.Run(mode, func(t *testing.T) {
			storage := &heldApplyStorage{testStorage: newTestStorage(), entered: make(chan []types.Log, 4),
				release: make(chan struct{}), finished: make(chan struct{}, 4)}
			rg := raftgroup.New(newTestOptions(raftgroup.WithStorage(storage)))
			require.NoError(t, rg.Start())
			t.Cleanup(rg.Stop)
			t.Cleanup(storage.unblock)
			n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey("slot"), raft.WithNodeId(1)))
			rg.AddRaft(n)
			cfg := types.Config{Leader: 1, Term: 2, Version: 1, Role: types.RoleLeader, Replicas: []uint64{1}}
			require.NoError(t, rg.AddEventWait("slot", types.Event{Type: types.ConfChange, Config: cfg}))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			resp, err := rg.ProposeTimeout(ctx, "slot", 1, []byte("pending metadata"))
			require.NoError(t, err)
			select {
			case <-storage.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			before, err := rg.ReadLeaderState(ctx, "slot")
			require.NoError(t, err)
			require.Less(t, before.AppliedIndex, resp.Index)
			var validated atomic.Bool
			validate := func() error {
				state, err := rg.ReadLeaderState(ctx, "slot")
				if err != nil {
					return err
				}
				if state.AppliedIndex < resp.Index {
					return fmt.Errorf("read business state before admitted prefix applied")
				}
				validated.Store(true)
				if mode == "stale decision" {
					return raft.ErrConfigVersionStale
				}
				if mode == "term changes during validation" {
					cfg.Term++
					return rg.AddEventWait("slot", types.Event{Type: types.ConfChange, Config: cfg})
				}
				return nil
			}
			type result struct {
				index uint64
				err   error
			}
			done := make(chan result, 1)
			writeCtx, cancelWrite := context.WithCancel(ctx)
			defer cancelWrite()
			go func() {
				index, err := rg.ProposeAfterRead(writeCtx, "slot", before, 2, []byte("conditional metadata"), validate)
				done <- result{index, err}
			}()
			// No other proposal is running: a held admission lock here means the
			// new writer reserved its turn despite an unapplied log already present.
			require.Eventually(t, func() bool {
				if n.TryLock() {
					n.Unlock()
					return false
				}
				return true
			}, time.Second, time.Millisecond)
			require.False(t, validated.Load(), "must not read stale business state before Apply")
			if mode == "cancel while draining" {
				cancelWrite()
			} else {
				storage.unblock()
			}
			var got result
			select {
			case got = <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			switch mode {
			case "success":
				require.NoError(t, got.err)
				require.Equal(t, resp.Index+1, got.index)
			case "stale decision":
				require.ErrorIs(t, got.err, raft.ErrConfigVersionStale)
				require.Zero(t, got.index)
			case "term changes during validation":
				require.ErrorIs(t, got.err, raftgroup.ErrReadStateChanged)
				require.Zero(t, got.index)
			case "cancel while draining":
				require.ErrorIs(t, got.err, context.Canceled)
				require.Zero(t, got.index)
				// The original Apply is still held. Cancellation must release
				// admission so an ordinary proposal can enter immediately.
				next, err := rg.ProposeTimeout(ctx, "slot", 3, []byte("after cancellation"))
				require.NoError(t, err)
				require.Equal(t, resp.Index+1, next.Index)
				storage.unblock()
			}
			if mode != "cancel while draining" {
				require.True(t, validated.Load())
			}
		})
	}
}
