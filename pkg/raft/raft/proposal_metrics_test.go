package raft

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConfigOutcomeWrapperPrecedence(t *testing.T) {
	r := &Raft{}
	for _, cause := range []error{types.ErrNotLeader, types.ErrProposalDropped, types.ErrProposalSuperseded} {
		r.recordProposalOutcome(proposalNotAdmitted(cause))
		r.recordProposalOutcome(proposalUnknown(cause))
	}
	r.recordProposalOutcome(proposalUnknown(types.ErrForwardRejected))
	r.recordProposalOutcome(types.ErrProposalSuperseded)
	r.recordProposalOutcome(nil)
	require.Equal(t, ProposalOutcomes{OK: 1, NotAdmitted: 3, Unknown: 4, RejectedOrSuperseded: 1}, r.ProposalOutcomes())
}
