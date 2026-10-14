package raft

import (
	"errors"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// Forwarded proposal correlation (attempt token, version 1).
//
// A follower stamps every SendPropose with a token carried in the otherwise
// unused header fields of Logs[0]: Term = forwardTokenVersion, Index = sender
// incarnation, Id = attempt. A token-aware leader copies those three fields
// verbatim into Logs[0] of its SendProposeResp. The event and log encodings
// are unchanged, so every peer version decodes every packet.
//
// Settlement and retirement require the echoed token to equal the live
// registration's token. Tokens are never reissued: the incarnation is supplied
// by the owner through Options.ForwardIncarnation and must be durably reserved
// and strictly larger than every earlier incarnation of the node (the Config
// Raft owner reserves it in its storage before constructing Raft), and the
// attempt is a per instance counter that refuses to wrap. A delayed response
// therefore only matches the registration it was produced for. Without a
// reserved incarnation (zero) forwarding is refused before anything is sent.
// A registration retires on its first settlement (response, cancellation,
// route change or Stop) and its request ID may be registered again under a new
// token.
//
// Mixed versions: a leader that predates the token answers with zero header
// fields. Such a response cannot be attributed to an attempt, so it never
// settles or retires a registration; the caller ends at its own deadline as
// outcome-unknown. Older followers send zero tokens and ignore the echo, so
// their behaviour is unchanged. Forwarded Config proposals from upgraded
// followers need an upgraded leader to complete.
const forwardTokenVersion uint32 = 1

// configForwardLiveLimit bounds simultaneously live forwarded registrations.
// Settled registrations hold no state, so the limit is recoverable.
const configForwardLiveLimit = 65536

type forwardToken struct {
	incarnation uint64
	attempt     uint64
}

func (t forwardToken) log(data []byte) types.Log {
	return types.Log{Id: t.attempt, Index: t.incarnation, Term: forwardTokenVersion, Data: data}
}

// echoForwardToken returns the response header for a SendPropose request.
func echoForwardToken(e types.Event, data []byte) types.Log {
	if len(e.Logs) == 0 {
		return types.Log{Data: data}
	}
	return types.Log{Id: e.Logs[0].Id, Index: e.Logs[0].Index, Term: e.Logs[0].Term, Data: data}
}

// forwardTokenSource is guarded by Raft.forwardMu.
type forwardTokenSource struct {
	incarnation uint64
	next        uint64
}

var (
	errForwardIncarnationUnset  = errors.New("raft: forwarding incarnation not reserved")
	errForwardAttemptsExhausted = errors.New("raft: forwarding attempt space exhausted")
)

func newForwardTokenSource(incarnation uint64) forwardTokenSource {
	return forwardTokenSource{incarnation: incarnation, next: 1}
}

func (s *forwardTokenSource) issue() (forwardToken, error) {
	if s.incarnation == 0 {
		return forwardToken{}, errForwardIncarnationUnset
	}
	if s.next == math.MaxUint64 {
		return forwardToken{}, errForwardAttemptsExhausted
	}
	t := forwardToken{incarnation: s.incarnation, attempt: s.next}
	s.next++
	return t, nil
}

type forwardRegistration struct {
	leader uint64
	token  forwardToken
}

// ForwardIdentityStats is a process-local snapshot of forwarded proposal
// correlation state. Pending is the live registration count and is the only
// state retained per identity.
type ForwardIdentityStats struct {
	Pending, PendingHighWater                             uint64
	Registered, Retired, DuplicateRejected, LimitRejected uint64
	// Forwarding refused because no sender incarnation was reserved.
	UnreservedRejected uint64
	// Responses ignored because they cannot settle a live registration.
	UnechoedResponses, StaleResponses, SenderMismatches uint64
}

// forwardStats is guarded by Raft.forwardMu.
type forwardStats struct {
	highWater                                             int
	registered, retired, duplicateRejected, limitRejected uint64
	unreservedRejected                                    uint64
	unechoed, stale, senderMismatch                       uint64
}

func (r *Raft) ForwardIdentityStats() ForwardIdentityStats {
	r.forwardMu.Lock()
	defer r.forwardMu.Unlock()
	st := r.forwardStats
	return ForwardIdentityStats{
		Pending:            uint64(len(r.forwardPending)),
		PendingHighWater:   uint64(st.highWater),
		Registered:         st.registered,
		Retired:            st.retired,
		DuplicateRejected:  st.duplicateRejected,
		LimitRejected:      st.limitRejected,
		UnreservedRejected: st.unreservedRejected,
		UnechoedResponses:  st.unechoed,
		StaleResponses:     st.stale,
		SenderMismatches:   st.senderMismatch,
	}
}

// retireForwardLocked settles one live registration. The caller holds forwardMu.
func (r *Raft) retireForwardLocked(key string, result any) {
	delete(r.forwardPending, key)
	r.forwardStats.retired++
	r.fowardProposeWait.Trigger(key, result)
}

func (r *Raft) settleForwardsLocked(err error) {
	for key := range r.forwardPending {
		r.retireForwardLocked(key, err)
	}
}
