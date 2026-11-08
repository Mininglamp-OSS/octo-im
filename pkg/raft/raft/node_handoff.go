package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

// The pause starts before an asynchronous role request exists. Its owner is
// the migration; requestID distinguishes repeated attempts of that migration.
type leaderHandoff struct {
	term         uint32
	version      uint64
	from, to     uint64
	requestID    uint64
	responseType types.EventType
	attempted    bool // SaveConfig may have committed despite a failed response
	paused       bool // accepted work has drained and the final tail is fixed
}

func (n *Node) beginLeaderHandoff() {
	if n.handoff == nil {
		n.handoff = &leaderHandoff{term: n.cfg.Term, version: n.cfg.Version, from: n.cfg.MigrateFrom, to: n.cfg.MigrateTo}
	}
}

func (n *Node) pauseLeaderHandoff() {
	n.beginLeaderHandoff()
	n.handoff.paused = true
	n.stopPropose = true
}

func (n *Node) clearLeaderHandoff() {
	n.handoff = nil
	n.stopPropose, n.drainingAdmissions = false, false
	n.admissionDrainTicks, n.admissionDrainCooldown = 0, 0
}

func (n *Node) sendLeaderHandoffReq(kind types.EventType, target uint64) {
	n.pauseLeaderHandoff()
	n.roleChangeID++
	h := n.handoff
	h.requestID, h.attempted = n.roleChangeID, true
	h.responseType = types.LearnerToLeaderResp
	if kind == types.FollowerToLeaderReq {
		h.responseType = types.FollowerToLeaderResp
	}
	n.events = append(n.events, types.Event{Type: kind, From: target, Term: h.term,
		ConfigVersion: h.version, RoleChangeID: h.requestID})
}

func (n *Node) reconcileLeaderHandoff(h *leaderHandoff) {
	if h == nil {
		if n.cfg.MigrateTo == 0 || n.cfg.MigrateFrom != n.cfg.Leader {
			n.clearLeaderHandoff()
		}
		return
	}
	if !n.IsLeader() || n.cfg.Term != h.term || n.cfg.MigrateFrom != h.from || n.cfg.MigrateTo != h.to ||
		(!n.isVoter(h.to) && !n.isLearner(h.to)) {
		n.clearLeaderHandoff()
		return
	}
	// RoleUnknown can re-enter BecomeLeader while retaining the same leader
	// and term. Preserve this task's uncertainty across that local reset too.
	n.handoff, n.stopPropose = h, h.paused
	n.drainingAdmissions = true
	if n.cfg.Version != h.version {
		// A version refresh is not cancellation. Retain the unknown outcome
		// fence, but let the next SyncReq retry against the current config.
		h.version, h.requestID = n.cfg.Version, 0
	}
}

func (n *Node) handleLeaderHandoffResp(e types.Event) error {
	h := n.handoff
	if h == nil || !n.IsLeader() || e.RoleChangeID != h.requestID || e.From != h.to ||
		e.Term != h.term || e.ConfigVersion != h.version || e.Type != h.responseType {
		return nil
	}
	// Neither success nor a storage error proves that the owner has applied
	// the result. Only authoritative ConfChange releases this pause. Retry
	// through normal synchronization without accepting a new business write.
	if info := n.replicaSync[e.From]; info != nil {
		info.roleSwitching = false
	}
	for _, id := range n.cfg.Learners {
		if id != n.opts.NodeId {
			n.sendNotifySync(id)
		}
	}
	return nil
}
