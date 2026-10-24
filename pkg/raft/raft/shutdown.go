package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// ErrIncompleteQuiescence means caller settlement has completed, but a storage
// worker has not exited. Storage remains owned by the caller and must stay open.
var ErrIncompleteQuiescence = errors.New("raft: storage quiescence incomplete")

// submitWorker serializes submission with Stop. A task handed to an ants worker
// before Stop but not yet started skips storage work after the shutdown fence.
func (r *Raft) submitWorker(work func()) error {
	r.workerMu.Lock()
	defer r.workerMu.Unlock()
	if r.stopped.Load() {
		return types.ErrStopped
	}
	return r.pool.Submit(func() {
		r.workerMu.Lock()
		stopped := r.stopped.Load()
		r.workerMu.Unlock()
		if !stopped {
			work()
		}
	})
}

// WaitQuiescence initiates Stop and waits for actual worker exit, independently
// of caller settlement. It cannot terminate synchronous Storage I/O. A nil
// result permits the storage owner to close dependent resources; an incomplete
// result preserves context error compatibility and can be queried again after
// I/O returns. No detached join goroutine is created.
func (r *Raft) WaitQuiescence(ctx context.Context) error {
	r.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if r.pool.Running() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", ErrIncompleteQuiescence, ctx.Err())
		case <-tick.C:
		}
	}
}

// The loop is joined before draining. Resolve pending handoffs and release
// queued payload references even when callers have already abandoned them.
func (r *Raft) drainStoppedQueue() {
	for {
		select {
		case req := <-r.stepC:
			if p := req.proposal; p != nil {
				p.mu.Lock()
				if !p.handled {
					p.handled = true
					p.err = proposalNotAdmitted(types.ErrStopped)
					close(p.done)
				}
				p.mu.Unlock()
			}
			if req.resp != nil {
				select {
				case req.resp <- types.ErrStopped:
				default:
				}
			}
		default:
			return
		}
	}
}
