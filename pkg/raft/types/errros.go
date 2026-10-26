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

// Divergence containment. Storage returns the first two when it refuses a
// request that would leave the applied state inconsistent with the log; the
// replica is then quarantined and every fenced read, proposal and election
// path of that replica reports ErrSlotDiverged. ErrSlotDiverged never wraps
// or equals a not-found error, so callers cannot mistake it for absence.
var (
	ErrTruncateBelowApplied = errors.New("log truncation below the applied index refused")
	ErrAppendBelowApplied   = errors.New("conflicting log append at or below the applied index refused")
	ErrSlotDiverged         = errors.New("slot replica diverged: local replica is quarantined")
)

// Proposal outcomes reported by RaftGroup. They are process-local and are
// usually wrapped together with the cause (context error, ErrGroupStopped,
// ErrNotLeader, ...), so errors.Is matches both the outcome and the cause.
var (
	// ErrProposalNotAdmitted means the proposal definitely did not enter the
	// log: it was cancelled or rejected before the leader admitted it.
	ErrProposalNotAdmitted = errors.New("proposal not admitted")
	// ErrProposalOutcomeUnknown means the proposal was, or may have been,
	// admitted, but the caller stopped waiting before its outcome was known.
	// It may still be committed and applied.
	ErrProposalOutcomeUnknown = errors.New("proposal outcome unknown")
	// ErrProposalSuperseded means the proposal was admitted, but another
	// entry was applied at its log index.
	ErrProposalSuperseded = errors.New("proposal superseded by another log entry")
	// ErrForwardRejected means the leader answered a forwarded proposal with
	// an error. A leader running an older release can still have queued the
	// proposal, so this does not imply ErrProposalNotAdmitted.
	ErrForwardRejected = errors.New("forwarded proposal rejected by leader")
)

// Admission is the log range and term the leader assigned to an admitted
// proposal.
type Admission struct {
	Term       uint32
	FirstIndex uint64
	LastIndex  uint64
}
