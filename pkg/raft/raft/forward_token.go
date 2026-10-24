package raft

import (
	"errors"
	"math"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
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
// Mixed versions (rolling upgrade or rollback, in any order):
//   - Older followers send zero headers and ignore the echo; an upgraded
//     leader echoes the zero header, so their behaviour is unchanged.
//   - A leader that predates the token answers with zero header fields. The
//     follower then falls back to the previous release's correlation: the
//     response settles the live registration for its request ID if it came
//     from that registration's leader. fowardPropose still checks the response
//     IDs and indexes, and the receipt still verifies Id, Index and Data of the
//     applied entry, so a misattributed response can end as unknown or
//     superseded but never as a false success. A rejection settles as
//     outcome-unknown immediately instead of at the caller's deadline. The
//     first such response per leader and term is logged at WARN and every one
//     is counted (LegacyResponses, LegacySettled).
//   - A leader that echoed the token of a live registration while this
//     node's view is in term T is token-aware for T, and a zero-header
//     response attributed to it in T is dropped as unattributable
//     (UnechoedResponses). Only a live match counts: registrations retire on
//     every route change, so a stale echo can predate the term and proves
//     nothing about its leader. A restarted or rolled-back leader can only
//     lead again in a later term, which clears the observation.
//   - A response with any other header version is dropped and logged.
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
	// Zero-header responses from leaders that predate the token, and those
	// that settled a registration by request ID.
	LegacyResponses, LegacySettled uint64
}

// forwardStats is guarded by Raft.forwardMu.
type forwardStats struct {
	highWater                                             int
	registered, retired, duplicateRejected, limitRejected uint64
	unreservedRejected                                    uint64
	unechoed, stale, senderMismatch                       uint64
	legacy, legacySettled                                 uint64
}

// forwardPeer is per-leader compatibility state, guarded by Raft.forwardMu.
type forwardPeer struct {
	aware     bool   // echoed a token in awareTerm
	awareTerm uint32 // this node's view term at the time
	warnTerm  uint32
	warned    uint8 // forwardWarn classes logged in warnTerm
}

type forwardWarn uint8

const (
	warnLegacySettled forwardWarn = 1 << iota
	warnLegacyDropped
	warnUnknownVersion
)

// forwardPeerLimit bounds per-sender state; senders are cluster nodes.
const forwardPeerLimit = 1024

func (r *Raft) forwardPeerLocked(from uint64) forwardPeer {
	if _, ok := r.forwardPeers[from]; !ok && len(r.forwardPeers) >= forwardPeerLimit {
		clear(r.forwardPeers)
	}
	return r.forwardPeers[from]
}

func (r *Raft) markTokenAwareLocked(from uint64) {
	p := r.forwardPeerLocked(from)
	p.aware, p.awareTerm = true, r.view.Load().(nodeView).term
	r.forwardPeers[from] = p
}

// acceptLegacyResponseLocked decides whether a zero-header response may settle
// the live registration for key. The caller holds forwardMu.
func (r *Raft) acceptLegacyResponseLocked(key string, from uint64) bool {
	r.forwardStats.legacy++
	if p := r.forwardPeerLocked(from); p.aware && p.awareTerm == r.view.Load().(nodeView).term {
		r.forwardStats.unechoed++
		r.warnForwardLocked(from, warnLegacyDropped, "zero-header forwarding response from a token-aware leader dropped")
		return false
	}
	reg, ok := r.forwardPending[key]
	if !ok {
		r.forwardStats.stale++
		return false
	}
	if reg.leader != from {
		r.forwardStats.senderMismatch++
		return false
	}
	r.forwardStats.legacySettled++
	r.warnForwardLocked(from, warnLegacySettled, "leader predates forwarding tokens; settling forwarded proposals by request ID until it is upgraded")
	return true
}

// warnForwardLocked logs class at most once per sender and view term.
func (r *Raft) warnForwardLocked(from uint64, class forwardWarn, msg string, fields ...zap.Field) {
	term := r.view.Load().(nodeView).term
	p := r.forwardPeerLocked(from)
	if p.warnTerm != term {
		p.warnTerm, p.warned = term, 0
	}
	if p.warned&uint8(class) != 0 {
		return
	}
	p.warned |= uint8(class)
	r.forwardPeers[from] = p
	r.Warn(msg, append(fields, zap.Uint64("leader", from), zap.Uint32("viewTerm", term))...)
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
		LegacyResponses:    st.legacy,
		LegacySettled:      st.legacySettled,
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
