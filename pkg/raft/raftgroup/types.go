package raftgroup

import (
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

type Event struct {
	RaftKey string
	types.Event
	WaitC chan error
	// claim, set with WaitC, is won either by the event loop before it
	// handles the event or by the waiter once shutdown began; the loser never
	// touches the event. It keeps a handled result apart from an abandoned one.
	claim *atomic.Bool
	// Proposal is set on Propose events created by RaftGroup proposals. Such
	// an event is admitted by the event loop instead of being stepped.
	Proposal *proposal
}
