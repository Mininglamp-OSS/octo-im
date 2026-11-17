package slot

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type round2MockSlotNode struct {
	icluster.Node
	slot *clustertypes.Slot
}

func (n *round2MockSlotNode) Slot(id uint32) *clustertypes.Slot {
	return n.slot
}

func (n *round2MockSlotNode) Slots() []*clustertypes.Slot {
	if n.slot == nil {
		return nil
	}
	return []*clustertypes.Slot{n.slot}
}

type round2MockSlotRPC struct {
	icluster.RPC
	mu       sync.Mutex
	calls    atomic.Int32
	blockDur time.Duration
	resps    types.ProposeRespSet
	err      error
}

func (r *round2MockSlotRPC) RequestSlotProposeBatchUntilApplied(nodeId uint64, slotId uint32, reqs types.ProposeReqSet) (types.ProposeRespSet, error) {
	r.calls.Add(1)
	r.mu.Lock()
	bd := r.blockDur
	resps := r.resps
	err := r.err
	r.mu.Unlock()
	if bd > 0 {
		time.Sleep(bd)
	}
	return resps, err
}

func TestRound2_BFR2_3_SlotProposeEmptyRespContextDeadlineAndNoStaleForward(t *testing.T) {
	dir := t.TempDir()
	nodeStub := &round2MockSlotNode{
		slot: &clustertypes.Slot{
			Id:       1,
			Leader:   2,
			Replicas: []uint64{1, 2},
			Term:     1,
		},
	}
	rpcStub := &round2MockSlotRPC{
		resps: nil,
		err:   nil,
	}
	srv := NewServer(NewOptions(
		WithNodeId(1),
		WithDataDir(dir),
		WithSlotDbShardNum(1),
		WithNode(nodeStub),
		WithRPC(rpcStub),
	))
	require.NoError(t, srv.Start())
	defer srv.Stop()

	// 1. Empty response from RPC must return ErrEmptySlotProposeResponse, never (nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	resp, err := srv.ProposeUntilAppliedTimeout(ctx, 1, []byte("cmd"))
	require.ErrorIs(t, err, ErrEmptySlotProposeResponse)
	require.Nil(t, resp)

	// 2. Context deadline must bound remote RPC wait (must not block for 5s when ctx is 60ms)
	rpcStub.mu.Lock()
	rpcStub.blockDur = 200 * time.Millisecond
	rpcStub.mu.Unlock()
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer shortCancel()
	start := time.Now()
	_, err = srv.ProposeUntilAppliedTimeout(shortCtx, 1, []byte("cmd"))
	elapsed := time.Since(start)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, 400*time.Millisecond, "ProposeUntilAppliedTimeout must respect caller ctx deadline")

	// 3. ProposeUntilAppliedForLocal must NOT forward pre-encoded local RMW payloads to remote leader (node 2)
	beforeCalls := rpcStub.calls.Load()
	_, err = srv.ProposeUntilAppliedForLocal(1, []byte("stale-local-conversation-row"))
	require.ErrorIs(t, err, types.ErrNotLeader)
	require.Equal(t, beforeCalls, rpcStub.calls.Load(), "ProposeUntilAppliedForLocal must not replay pre-encoded local payload to remote slot leader")

	// 4. General ProposeUntilApplied (used by cross-slot callers such as UpdateUserPluginNo / RemovePluginUser / SaveChannelClusterConfig)
	// MUST forward to the remote slot leader (node 2) and succeed when a valid ProposeResp is returned.
	rpcStub.mu.Lock()
	rpcStub.blockDur = 0
	rpcStub.err = nil
	rpcStub.resps = types.ProposeRespSet{
		{Id: 1, Index: 10},
	}
	rpcStub.mu.Unlock()
	resp, err = srv.ProposeUntilApplied(1, []byte("cross-slot-plugin-update"))
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(10), resp.Index)
	require.Equal(t, beforeCalls+1, rpcStub.calls.Load(), "general ProposeUntilApplied must forward to remote slot leader")
}
