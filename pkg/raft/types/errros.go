package types

import "errors"

var (
	ErrLogIndexNotContinuous = errors.New("log index is not continuous")
	ErrStopped               = errors.New("raft is stopped")
	ErrNotLeader             = errors.New("not leader")
	ErrPaused                = errors.New("raft is paused")
	ErrProposalDropped       = errors.New("proposal dropped")

	// ErrLeaderLocalOverlap reports a locally proposed log range that does not
	// start exactly at lastLogIndex+1 or is not contiguous. The proposal was
	// not appended and no existing log was truncated.
	ErrLeaderLocalOverlap = errors.New("leader-local proposal overlaps or is not contiguous with the log")
	// ErrProposalStaleTerm reports a locally proposed log whose term differs
	// from the leader's current term. The proposal was not appended.
	ErrProposalStaleTerm = errors.New("leader-local proposal term is not the current term")
)
