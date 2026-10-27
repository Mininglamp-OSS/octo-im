package raftgroup

import (
	"context"
	"errors"
	"fmt"
)

var errGroupNotStopped = errors.New("raftgroup: not stopped")

// ErrIncompleteQuiescence means the shutdown lifecycle has not yet joined the
// event loop or every accepted pool task. Storage remains owned by the caller
// and must stay open.
var ErrIncompleteQuiescence = errors.New("raftgroup: storage quiescence incomplete")

// submit runs fn on the bounded nonblocking pool. Every pool task may touch
// Storage, so each accepted task is counted until it exits, and submission
// is refused from BeginStop on. WaitQuiescence joins the counted tasks.
func (rg *RaftGroup) submit(fn func()) error {
	rg.verificationMu.Lock()
	if rg.verificationStopped {
		rg.verificationMu.Unlock()
		return ErrGroupStopped
	}
	rg.poolTasks++
	rg.verificationMu.Unlock()
	err := rg.goPool.Submit(func() {
		defer rg.poolTaskDone()
		fn()
	})
	if err != nil {
		rg.poolTaskDone()
	}
	return err
}

func (rg *RaftGroup) poolTaskDone() {
	rg.verificationMu.Lock()
	defer rg.verificationMu.Unlock()
	rg.poolTasks--
	if rg.loopJoined && rg.poolTasks == 0 {
		close(rg.poolIdle)
	}
}

// WaitQuiescence waits, after Stop or BeginStop, until the event loop has
// exited and every pool task accepted before the fence has exited. After it
// returns nil the group never touches Storage again. Storage I/O cannot be
// interrupted: on error the owner must keep Storage open and may call
// WaitQuiescence again.
func (rg *RaftGroup) WaitQuiescence(ctx context.Context) error {
	if !rg.stopBegun.Load() {
		return errGroupNotStopped
	}
	// poolIdle closes only after the loop has been joined. Completion that
	// has already happened wins over an expired context.
	select {
	case <-rg.poolIdle:
		return nil
	default:
	}
	select {
	case <-rg.poolIdle:
		return nil
	case <-ctx.Done():
	}
	select {
	case <-rg.loopDone:
		return fmt.Errorf("%w: storage workers still running: %w", ErrIncompleteQuiescence, ctx.Err())
	default:
		return fmt.Errorf("%w: event loop still running: %w", ErrIncompleteQuiescence, ctx.Err())
	}
}

// verifyReceiptAsync uses the tracked bounded pool. Accepted verification
// tasks are additionally counted for receipt cleanup assertions.
func (rg *RaftGroup) verifyReceiptAsync(w *receiptWaiter) {
	rg.verificationWorkers.Add(1)
	err := rg.submit(func() {
		defer rg.verificationWorkers.Add(-1)
		rg.verificationMu.Lock()
		stopped := rg.verificationStopped
		rg.verificationMu.Unlock()
		if stopped {
			rg.wait.complete(w, proposalOutcomeUnknown(ErrGroupStopped))
			return
		}
		rg.wait.readReceipt(w)
	})
	if err != nil {
		rg.verificationWorkers.Add(-1)
		rg.wait.complete(w, proposalOutcomeUnknown(err))
	}
}
