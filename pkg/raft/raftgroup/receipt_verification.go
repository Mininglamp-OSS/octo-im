package raftgroup

// verifyReceiptAsync uses the existing bounded nonblocking pool. Submission is
// fenced by Stop; accepted tasks are counted before dispatch and until exit.
// Storage I/O cannot be interrupted, so Stop settles callers without claiming
// worker quiescence. The storage owner must still join the pool before close.
func (rg *RaftGroup) verifyReceiptAsync(w *receiptWaiter) {
	rg.verificationMu.Lock()
	defer rg.verificationMu.Unlock()
	if rg.verificationStopped {
		rg.wait.complete(w, proposalOutcomeUnknown(ErrGroupStopped))
		return
	}
	rg.verificationWorkers.Add(1)
	err := rg.goPool.Submit(func() {
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
