package slot

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type forwardingNode struct{ icluster.Node }

func (forwardingNode) Slot(uint32) *nodetypes.Slot { return &nodetypes.Slot{Id: 1, Leader: 2} }

type forwardingRPC struct {
	icluster.RPC
	ctx context.Context
}

func (r *forwardingRPC) RequestSlotProposeBatchUntilAppliedWithContext(ctx context.Context, _ uint64, _ uint32, _ types.ProposeReqSet) (types.ProposeRespSet, error) {
	r.ctx = ctx
	return nil, ctx.Err()
}

func TestSlotProposalForwardingPreservesCancellation(t *testing.T) {
	rpc := &forwardingRPC{}
	s := NewServer(NewOptions(WithNodeId(1), WithNode(forwardingNode{}), WithRPC(rpc)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.ProposeUntilAppliedTimeout(ctx, 1, []byte("operation"))
	require.ErrorIs(t, err, context.Canceled)
	require.Same(t, ctx, rpc.ctx)
}

func TestSlotProposalGateRunsBeforeRaft(t *testing.T) {
	s := &Server{opts: &Options{BeforePropose: func(context.Context, uint32, types.ProposeReqSet) error { return context.Canceled }}}
	_, err := s.ProposeUntilAppliedTimeoutForLocal(context.Background(), 1, nil)
	require.ErrorIs(t, err, context.Canceled)
}
