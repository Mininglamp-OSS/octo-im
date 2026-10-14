package raftgroup

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
)

// Proposal states. The caller and the event loop race only on the first
// transition out of pending: the loop takes pending→admitting before it
// admits, the caller takes pending→abandoned when it stops waiting. Exactly
// one of the two CAS operations succeeds.
const (
	proposalPending uint32 = iota
	proposalAdmitting
	proposalAdmitted
	proposalRejected
	proposalAbandoned
)

// proposal is the in-process handshake between a proposing caller and the
// event loop.
type proposal struct {
	seq         uint64
	state       atomic.Uint32
	waitApplied bool
	// admitC has capacity 1 and receives exactly one result, published by the
	// loop after it left pending, so publishing never blocks.
	admitC chan admitResult
}

type admitResult struct {
	adm types.Admission
	// waiter is registered before the result is published, and therefore
	// before store or apply can be dispatched for the admitted range.
	waiter *receiptWaiter
	err    error
}

func newProposal(seq uint64, waitApplied bool) *proposal {
	return &proposal{seq: seq, waitApplied: waitApplied, admitC: make(chan admitResult, 1)}
}

func proposalNotAdmitted(cause error) error {
	return fmt.Errorf("%w: %w", types.ErrProposalNotAdmitted, cause)
}

func proposalOutcomeUnknown(cause error) error {
	return fmt.Errorf("%w: %w", types.ErrProposalOutcomeUnknown, cause)
}

// proposalOutcomeLogInterval limits outcome logs to one line per interval.
const proposalOutcomeLogInterval = time.Second

// proposalStats counts proposal outcomes reported to callers.
type proposalStats struct {
	ok          atomic.Uint64
	notAdmitted atomic.Uint64
	unknown     atomic.Uint64
	superseded  atomic.Uint64

	lastLogAt  atomic.Int64
	suppressed atomic.Uint64
}

// ProposalOutcomeCounts returns how many proposals succeeded, were not
// admitted, ended with an unknown outcome and were superseded.
func (rg *RaftGroup) ProposalOutcomeCounts() (ok, notAdmitted, unknown, superseded uint64) {
	s := &rg.proposalStats
	return s.ok.Load(), s.notAdmitted.Load(), s.unknown.Load(), s.superseded.Load()
}

// recordOutcome counts err and logs unknown and superseded outcomes without
// any payload. It does not change err.
func (rg *RaftGroup) recordOutcome(raftKey string, r receipt, err error) {
	s := &rg.proposalStats
	switch {
	case err == nil:
		s.ok.Add(1)
		return
	case errors.Is(err, types.ErrProposalNotAdmitted):
		s.notAdmitted.Add(1)
		return
	case errors.Is(err, types.ErrProposalSuperseded):
		s.superseded.Add(1)
	default:
		s.unknown.Add(1)
	}
	now := time.Now().UnixNano()
	last := s.lastLogAt.Load()
	if last != 0 && now-last < int64(proposalOutcomeLogInterval) || !s.lastLogAt.CompareAndSwap(last, now) {
		s.suppressed.Add(1)
		return
	}
	ok, notAdmitted, unknown, superseded := rg.ProposalOutcomeCounts()
	rg.Warn("proposal outcome is not success",
		zap.Error(err),
		zap.String("raftKey", raftKey),
		zap.Uint64("seq", r.seq),
		zap.Uint32("term", r.term),
		zap.Uint64("firstIndex", r.first),
		zap.Uint64("lastIndex", r.last),
		zap.Bool("forwarded", r.forwarded),
		zap.Uint64("okTotal", ok),
		zap.Uint64("notAdmittedTotal", notAdmitted),
		zap.Uint64("unknownTotal", unknown),
		zap.Uint64("supersededTotal", superseded),
		zap.Uint64("suppressed", s.suppressed.Swap(0)))
}
