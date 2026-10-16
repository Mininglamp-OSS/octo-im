package raft

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// Seeds are fixed; each seed launches four concurrent callers in each of four
// rounds (16 proposals). Context deadlines vary from 2 to 4 seconds solely as
// deadlock guards. Queue rendezvous and explicit cancellation establish order.
func TestConfigFixedSeedInterleavings(t *testing.T) {
	for _, seed := range []int64{7, 2095, 2113} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			c := studyNew(t, false)
			r := c.rr[0]
			seen := make(map[uint64]bool)
			notAdmitted := make(map[uint64]bool)
			for round := 0; round < 4; round++ {
				cancels := make(map[uint64]context.CancelFunc)
				results := make(map[uint64]<-chan studyResult)
				for j := 0; j < 4; j++ {
					id := uint64(2000 + round*4 + j)
					ctx, cancel := context.WithTimeout(context.Background(), time.Duration(2+rng.Intn(3))*time.Second)
					cancels[id] = cancel
					results[id] = studyCall(r, ctx, id)
				}
				queued := make([]stepReq, 4)
				for j := range queued {
					queued[j] = studyRecv(t, r.stepC)
				}
				rng.Shuffle(len(queued), func(i, j int) { queued[i], queued[j] = queued[j], queued[i] })
				for _, req := range queued {
					id := req.proposal.logs[0].Id
					mode := rng.Intn(4)
					var result studyResult
					if mode == 0 {
						cancels[id]()
						result = studyRecv(t, results[id])
						require.ErrorIs(t, result.err, types.ErrProposalNotAdmitted)
						notAdmitted[id] = true
						c.step(0, req)
					} else {
						c.step(0, req)
						switch mode {
						case 1:
							cancels[id]()
							result = studyRecv(t, results[id])
							require.ErrorIs(t, result.err, types.ErrProposalOutcomeUnknown)
						case 2:
							r.BecomeLeader(r.node.cfg.Term + 1)
							result = studyRecv(t, results[id])
							require.ErrorIs(t, result.err, types.ErrProposalOutcomeUnknown)
						}
						c.settle(r.node.queue.lastLogIndex)
						if mode == 3 {
							result = studyRecv(t, results[id])
							require.NoError(t, result.err)
							require.False(t, seen[id], "duplicate success")
							seen[id] = true
							stored, applied := c.ss[0].snapshot()
							studyIdentity(t, result.resps[0], stored, id, req.proposal.logs[0].Term)
							studyIdentity(t, result.resps[0], applied, id, req.proposal.logs[0].Term)
						}
					}
					cancels[id]()
					require.Zero(t, studyCount(r))
					require.Empty(t, r.forwardPending)
				}
			}
			stored, _ := c.ss[0].snapshot()
			for _, log := range stored {
				require.False(t, notAdmitted[log.Id], "NotAdmitted ID executed")
			}
			counts := r.ProposalOutcomes()
			require.Equal(t, uint64(16), counts.OK+counts.NotAdmitted+counts.Unknown+counts.RejectedOrSuperseded)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, r.WaitQuiescence(ctx))
			require.Zero(t, r.pool.Running())
			t.Logf("seed=%d callers=4 rounds=4 deadline=2..4s success=%d not_admitted=%d outcomes=%+v", seed, len(seen), len(notAdmitted), counts)
		})
	}
}
