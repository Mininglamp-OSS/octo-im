package raftgroup

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

type Event struct {
	RaftKey string
	types.Event
	WaitC chan error
	// Proposal is set on Propose events created by RaftGroup proposals. Such
	// an event is admitted by the event loop instead of being stepped.
	Proposal *proposal
}
