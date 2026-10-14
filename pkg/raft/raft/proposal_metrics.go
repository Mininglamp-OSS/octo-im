package raft

import (
	"errors"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

type proposalCounters struct {
	ok, notAdmitted, unknown, rejected atomic.Uint64
}

// ProposalOutcomes is a low-cardinality, process-local cumulative snapshot.
// Each public proposal call records exactly one caller-visible terminal result;
// forwarded leader work and late storage completions do not count a second time.
type ProposalOutcomes struct {
	OK, NotAdmitted, Unknown, RejectedOrSuperseded uint64
}

func (r *Raft) ProposalOutcomes() ProposalOutcomes {
	return ProposalOutcomes{r.outcomes.ok.Load(), r.outcomes.notAdmitted.Load(), r.outcomes.unknown.Load(), r.outcomes.rejected.Load()}
}

func (r *Raft) recordProposalOutcome(err error) {
	switch {
	case err == nil:
		r.outcomes.ok.Add(1)
	case errors.Is(err, types.ErrProposalSuperseded), errors.Is(err, types.ErrNotLeader), errors.Is(err, types.ErrProposalDropped):
		r.outcomes.rejected.Add(1)
	case errors.Is(err, types.ErrProposalNotAdmitted):
		r.outcomes.notAdmitted.Add(1)
	default:
		r.outcomes.unknown.Add(1)
	}
}
