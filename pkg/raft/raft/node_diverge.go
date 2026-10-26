package raft

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
)

// Divergence containment. A replica whose storage refused a truncation or an
// append below its applied index, or that opened with applied > last, can no
// longer be trusted: its applied state does not match its log. Such a replica
// is fenced for the rest of the process lifetime. It never proposes, leads,
// votes, replicates, stores, applies, syncs or certifies a read again.
const (
	divergeNone uint32 = iota
	divergeQuarantined
	divergeFailStopped
)

// Diverged reports whether this replica is quarantined or fail-stopped. It is
// safe to call from any goroutine.
func (n *Node) Diverged() bool {
	return n.divergeState.Load() != divergeNone
}

// FailStopped reports whether the divergence marker of this replica could not
// be persisted. It is safe to call from any goroutine.
func (n *Node) FailStopped() bool {
	return n.divergeState.Load() == divergeFailStopped
}

func divergeReason(r types.Reason) (uint32, bool) {
	switch r {
	case types.ReasonDiverged:
		return divergeQuarantined, true
	case types.ReasonDivergedFailStop:
		return divergeFailStopped, true
	}
	return divergeNone, false
}

// quarantine moves the replica to at least state. States only increase. It
// runs on the owner loop.
func (n *Node) quarantine(state uint32, why string) {
	for {
		cur := n.divergeState.Load()
		if cur >= state {
			return
		}
		if n.divergeState.CompareAndSwap(cur, state) {
			break
		}
	}
	wasLeader, term := n.IsLeader(), n.cfg.Term
	n.Error("replica quarantined",
		zap.String("key", n.Key()),
		zap.Uint64("nodeId", n.opts.NodeId),
		zap.Bool("failStop", state == divergeFailStopped),
		zap.Bool("wasLeader", wasLeader),
		zap.Uint32("term", term),
		zap.Uint64("appliedIndex", n.queue.appliedIndex),
		zap.Uint64("lastLogIndex", n.queue.lastLogIndex),
		zap.String("why", why))

	// Nothing released after this point may replicate, store, apply, vote or
	// sync. Only proposal fences survive: they settle local waiters.
	kept := n.events[:0]
	for _, e := range n.events {
		if e.Type == types.ProposalFence {
			kept = append(kept, e)
		}
	}
	n.events = kept
	n.tickFnc = nil
	n.stepFunc = nil
	n.stopPropose = true
	n.dropDeferredTruncate()
	if n.cfg.Leader == n.opts.NodeId {
		n.cfg.Leader = None
	}
	if n.cfg.Role == types.RoleLeader || n.cfg.Role == types.RoleCandidate {
		n.cfg.Role = types.RoleFollower
	}
	// A quarantined leader steps down: it stops proposing, replicating and
	// advancing commit, and its waiters are settled by the fence.
	n.fenceProposals(wasLeader, term)
	if n.opts.OnQuarantine != nil {
		n.opts.OnQuarantine(state == divergeFailStopped, why)
	}
}

// stepDiverged handles every event of a quarantined replica. It never
// changes the log, the commit index or the role towards leadership.
func (n *Node) stepDiverged(e types.Event) error {
	switch e.Type {
	case types.Propose:
		return types.ErrSlotDiverged
	case types.ConfChange:
		cfg := e.Config
		if cfg.Role == types.RoleLeader || cfg.Leader == n.opts.NodeId {
			n.Error("refused leader configuration for a quarantined replica",
				zap.String("key", n.Key()), zap.Uint64("leader", cfg.Leader), zap.Uint32("term", cfg.Term))
			return types.ErrSlotDiverged
		}
		if cfg.Version < n.cfg.Version {
			return ErrConfigVersionStale
		}
		// Membership bookkeeping only; the local role never changes.
		role := n.cfg.Role
		if cfg.Term < n.cfg.Term {
			cfg.Term = n.cfg.Term
		}
		n.cfg = cfg
		n.cfg.Role = role
		n.setTerm(cfg.Term)
	case types.StoreResp:
		n.queue.appending = false
	case types.TruncateResp:
		n.truncating = false
	case types.ApplyResp:
		// An apply submitted before the fence may still finish; record it so
		// the reported applied index stays truthful. Nothing new is applied.
		n.queue.applying = false
		if e.Reason == types.ReasonOk {
			n.queue.appliedTo(e.Index)
		}
	}
	return nil
}
