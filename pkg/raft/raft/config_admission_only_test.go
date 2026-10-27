package raft

import (
	"context"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// An admission-only proposal that was handled without error has achieved its
// success condition. This is the state cancelProposal sees when p.done and
// cancellation/Stop are both ready and the caller selects cancellation; it
// must not turn that into an unknown outcome or drop the assigned indexes.
func TestConfigCancelProposalKeepsCompletedAdmission(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, types.ErrStopped} {
		t.Run(cause.Error(), func(t *testing.T) {
			r := &Raft{wait: newWait("test")}
			p := &configProposal{ctx: context.Background(), handled: true, done: make(chan struct{})}
			close(p.done)
			require.NoError(t, r.cancelProposal(p, cause))
			require.False(t, p.abandoned)
		})
	}

	t.Run("not_handled", func(t *testing.T) {
		r := &Raft{wait: newWait("test")}
		p := &configProposal{ctx: context.Background(), done: make(chan struct{})}
		err := r.cancelProposal(p, context.Canceled)
		require.ErrorIs(t, err, types.ErrProposalNotAdmitted)
		require.True(t, p.abandoned)
	})
	t.Run("rejected", func(t *testing.T) {
		r := &Raft{wait: newWait("test")}
		p := &configProposal{ctx: context.Background(), handled: true, err: proposalNotAdmitted(types.ErrNotLeader), done: make(chan struct{})}
		close(p.done)
		require.ErrorIs(t, r.cancelProposal(p, context.Canceled), types.ErrNotLeader)
	})
	t.Run("applied_wait_incomplete", func(t *testing.T) {
		r := &Raft{wait: newWait("test")}
		logs := []types.Log{{Id: 1, Index: 1, Data: []byte("a")}}
		p := &configProposal{ctx: context.Background(), handled: true, waitApplied: true, waiter: r.wait.registerReceipt(logs, false), done: make(chan struct{})}
		close(p.done)
		err := r.cancelProposal(p, context.Canceled)
		require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
		require.ErrorIs(t, err, context.Canceled)
	})
}

// Public-call counterpart: cancellation or Stop arrives right after the loop
// admitted the proposal. Whichever case the caller observes, it returns the
// assigned index. The deterministic both-ready case is covered above.
func TestConfigAdmissionOnlyCompletionRacesCancellation(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(fmt.Sprintf("stop=%t", stop), func(t *testing.T) {
			for i := 0; i < 64; i++ {
				c := studyNew(t, false)
				r := c.rr[0]
				ctx, cancel := context.WithCancel(context.Background())
				id := uint64(1000 + i)
				out := make(chan studyResult, 1)
				go func() {
					resps, err := r.ProposeBatchTimeout(ctx, types.ProposeReqSet{{Id: id, Data: []byte(fmt.Sprintf("request-%d", id))}})
					out <- studyResult{resps, err}
				}()
				c.step(0, studyRecv(t, r.stepC))
				if stop {
					r.Stop()
				} else {
					cancel()
				}
				result := studyRecv(t, out)
				cancel()
				require.NoError(t, result.err, "iteration %d", i)
				require.Len(t, result.resps, 1)
				require.Equal(t, id, result.resps[0].Id)
				require.Equal(t, uint64(1), result.resps[0].Index)
				require.Equal(t, uint64(1), r.node.queue.lastLogIndex)
			}
		})
	}
}
