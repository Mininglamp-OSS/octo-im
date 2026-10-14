package raftgroup

import (
	"context"
	"errors"
	"fmt"
)

var errGroupNotStopped = errors.New("raftgroup: not stopped")

// submit runs fn on the bounded nonblocking pool. Every pool task may touch
// Storage, so each accepted task is counted until it exits and submission is
// fenced by Stop. WaitQuiescence joins the counted tasks.
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
	if rg.verificationStopped && rg.poolTasks == 0 {
		close(rg.poolIdle)
	}
}

// WaitQuiescence waits, after Stop, until no pool task can touch Storage.
// Storage I/O cannot be interrupted: on error the owner must keep Storage
// open and may call WaitQuiescence again.
func (rg *RaftGroup) WaitQuiescence(ctx context.Context) error {
	rg.verificationMu.Lock()
	stopped := rg.verificationStopped
	rg.verificationMu.Unlock()
	if !stopped {
		return errGroupNotStopped
	}
	select {
	case <-rg.poolIdle:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("raftgroup: storage workers still running: %w", ctx.Err())
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
