package raftgroup

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
)

// Forwarded proposal correlation (attempt token, version 1).
//
// A follower stamps every SendPropose with a token carried in the otherwise
// unused header fields of Logs[0]: Term = forwardTokenVersion, Index = group
// incarnation, Id = attempt. The leader copies the header verbatim into
// Logs[0] of its SendProposeResp, for success and rejection alike. Event and
// log encodings are unchanged.
//
// A registration is keyed by raft key and last request ID and is settled only
// by the exact token it was sent with, from the leader it was sent to. A
// response of an earlier attempt that reused the request ID is dropped, so it
// cannot hand its indexes to a newer caller. The incarnation is drawn at
// random per RaftGroup instance (nonzero) and the attempt is a per-instance
// counter, so a restarted process does not reuse tokens except with
// probability 2^-64. A second live forward ending in the same request ID is
// refused before it is sent.
//
// Mixed versions: older followers send a zero header and the leader echoes
// it, so their behaviour is unchanged. A leader that predates the token
// answers with a zero header; its response settles the registration for the
// key, as before, if it came from the registered leader. Nothing ties it to
// this attempt: a delayed response of an earlier attempt that reused the
// request IDs looks the same. Receipts verify the Id and Data of every entry
// in the returned range, so for ProposeBatchUntilAppliedTimeout a
// misattributed legacy response ends as superseded or unknown, never as a
// false success. The admission-only ProposeBatchTimeout has no receipt to
// check, so it returns ErrProposalOutcomeUnknown for a legacy success
// response instead of its indexes. Any other header version is dropped.
const forwardTokenVersion uint32 = 1

// legacyForwardResps is a success response settled by request ID alone,
// from a leader that predates the token.
type legacyForwardResps types.ProposeRespSet

var errLegacyForwardUncorrelated = errors.New("raftgroup: leader predates forwarding tokens; its admission response is not correlated with this attempt")

// forwardPeerLimit bounds per-sender warning state; senders are cluster nodes.
const forwardPeerLimit = 1024

type forwardToken struct {
	incarnation uint64
	attempt     uint64
}

func (t forwardToken) log(data []byte) types.Log {
	return types.Log{Id: t.attempt, Index: t.incarnation, Term: forwardTokenVersion, Data: data}
}

// echoForwardHeader returns the response header for a SendPropose request.
func echoForwardHeader(e types.Event, data []byte) types.Log {
	if len(e.Logs) == 0 {
		return types.Log{Data: data}
	}
	return types.Log{Id: e.Logs[0].Id, Index: e.Logs[0].Index, Term: e.Logs[0].Term, Data: data}
}

func newForwardIncarnation() uint64 {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			panic(fmt.Sprintf("raftgroup: forwarding incarnation: %v", err))
		}
		if v := binary.BigEndian.Uint64(b[:]); v != 0 {
			return v
		}
	}
}

type forwardRegistration struct {
	leader  uint64
	token   forwardToken
	resultC chan any // capacity 1, written once under forwardMu
}

// ForwardStats is a process-local snapshot of forwarded proposal correlation.
type ForwardStats struct {
	Pending                                         uint64
	DuplicateRejected                               uint64
	StaleResponses, SenderMismatches, UnknownHeader uint64
	// Zero-header responses from leaders that predate the token, and those
	// that settled a registration by request ID.
	LegacyResponses, LegacySettled uint64
}

// forwardStats is guarded by RaftGroup.forwardMu.
type forwardStats struct {
	duplicateRejected                    uint64
	stale, senderMismatch, unknownHeader uint64
	legacy, legacySettled                uint64
}

func (rg *RaftGroup) ForwardStats() ForwardStats {
	rg.forwardMu.Lock()
	defer rg.forwardMu.Unlock()
	st := rg.forwardStats
	return ForwardStats{
		Pending:           uint64(len(rg.forwardPending)),
		DuplicateRejected: st.duplicateRejected,
		StaleResponses:    st.stale,
		SenderMismatches:  st.senderMismatch,
		UnknownHeader:     st.unknownHeader,
		LegacyResponses:   st.legacy,
		LegacySettled:     st.legacySettled,
	}
}

func forwardKey(raftKey string, lastID uint64) string {
	return fmt.Sprintf("%s/%d", raftKey, lastID)
}

// registerForward issues a token and registers it, or reports that a forward
// ending in the same request ID is already live.
func (rg *RaftGroup) registerForward(key string, leader uint64) (*forwardRegistration, bool) {
	rg.forwardMu.Lock()
	defer rg.forwardMu.Unlock()
	if _, live := rg.forwardPending[key]; live {
		rg.forwardStats.duplicateRejected++
		return nil, false
	}
	rg.forwardAttempt++
	reg := &forwardRegistration{
		leader:  leader,
		token:   forwardToken{incarnation: rg.forwardIncarnation, attempt: rg.forwardAttempt},
		resultC: make(chan any, 1),
	}
	rg.forwardPending[key] = reg
	rg.observeForwardLocked(key, "register", "")
	return reg, true
}

// retireForward removes reg if it is still the live registration for key.
func (rg *RaftGroup) retireForward(key string, reg *forwardRegistration) {
	rg.forwardMu.Lock()
	defer rg.forwardMu.Unlock()
	if rg.forwardPending[key] == reg {
		delete(rg.forwardPending, key)
		rg.observeForwardLocked(key, "retire", "")
	}
}

// settleForwardLocked delivers result to the live registration for key.
func (rg *RaftGroup) settleForwardLocked(key string, reg *forwardRegistration, result any) {
	delete(rg.forwardPending, key)
	reg.resultC <- result
	rg.observeForwardLocked(key, "settle", fmt.Sprintf("%T", result))
}

// observeForwardLocked reports a correlation event to an optional test
// observer installed before the group starts.
func (rg *RaftGroup) observeForwardLocked(key, phase, detail string) {
	if rg.forwardObserve != nil {
		rg.forwardObserve(key, phase, detail)
	}
}

// warnForwardLocked logs class at most once per sender.
func (rg *RaftGroup) warnForwardLocked(from uint64, class uint8, msg string, fields ...zap.Field) {
	if _, ok := rg.forwardWarned[from]; !ok && len(rg.forwardWarned) >= forwardPeerLimit {
		clear(rg.forwardWarned)
	}
	if rg.forwardWarned[from]&class != 0 {
		return
	}
	rg.forwardWarned[from] |= class
	rg.Warn(msg, append(fields, zap.Uint64("from", from))...)
}

const (
	warnForwardLegacy uint8 = 1 << iota
	warnForwardUnknownHeader
)
