package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

const (
	initialLeaderTransferRetryTicks = 4
	maxLeaderTransferRetryTicks     = 32
	maxLeaderTransferFailures       = 6
	leaderTransferRecoveryTicks     = 128
)

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
	published    bool // channel config save succeeded; wait for authoritative ConfChange
	retryTicks   int
	failures     int
	probeTicks   int
	quietTicks   int
}

func (h *leaderHandoff) deferRetry() {
	h.failures = min(h.failures+1, maxLeaderTransferFailures)
	if h.retryExhausted() {
		h.retryTicks = leaderTransferRecoveryTicks
		return
	}
	h.retryTicks = min(initialLeaderTransferRetryTicks<<(h.failures-1), maxLeaderTransferRetryTicks)
}

func (h *leaderHandoff) retryExhausted() bool {
	return h.failures == maxLeaderTransferFailures
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
	n.recordHandoffFence()
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
		h.retryTicks, h.failures, h.published = 0, 0, false
		h.probeTicks, h.quietTicks = 0, 0
	}
}

func (n *Node) handleLeaderHandoffResp(e types.Event) error {
	h := n.handoff
	if h == nil || h.requestID == 0 || !n.IsLeader() || e.RoleChangeID != h.requestID || e.From != h.to ||
		e.Term != h.term || e.ConfigVersion != h.version || e.Type != h.responseType {
		return nil
	}
	// Neither success nor a storage error proves that the owner has applied
	// the result. Only authoritative ConfChange releases this pause. Retry
	// through normal synchronization without accepting a new business write.
	h.requestID = 0
	if n.opts.AutoSuspend {
		if e.Reason == types.ReasonOk {
			h.published = true
		} else {
			h.deferRetry()
		}
	}
	if info := n.replicaSync[e.From]; info != nil {
		info.roleSwitching = false
	}
	n.wakeLearnersAfterRoleChange()
	return nil
}

func (n *Node) wakeLearnersAfterRoleChange() {
	for _, id := range n.cfg.Learners {
		if id == n.opts.NodeId {
			continue
		}
		if n.opts.AutoSuspend && n.isLeaderTransferTarget(id) {
			if n.canWakeLeaderTransferTarget(id) {
				n.wakeLeaderTransferTarget(id)
			}
		} else {
			n.sendNotifySync(id)
		}
	}
}

func (n *Node) isLeaderTransferTarget(id uint64) bool {
	return id != None && id != n.opts.NodeId && n.IsLeader() &&
		n.cfg.MigrateFrom == n.opts.NodeId && n.cfg.MigrateTo == id &&
		(n.isVoter(id) || n.isLearner(id))
}

func (n *Node) canWakeLeaderTransferTarget(id uint64) bool {
	return n.opts.AutoSuspend && n.admissionDrainCooldown == 0 && n.isLeaderTransferTarget(id) &&
		(n.handoff == nil || n.handoff.canPublish() && !n.handoff.retryExhausted())
}

func (h *leaderHandoff) canPublish() bool {
	return h.requestID == 0 && h.retryTicks == 0 && !h.published
}

func (n *Node) probeExhaustedLeaderTransfer(id uint64) bool {
	h := n.handoff
	if h == nil || !h.retryExhausted() || !h.canPublish() || h.probeTicks != 0 || !n.isLeaderTransferTarget(id) {
		return false
	}
	// Leave enough time for a final sync response before retiring the source.
	// Waking a target just before eviction would leave it polling a dead owner.
	if n.opts.AutoDestory && n.opts.DestoryAfterIdleTick-n.idleTicks() <=
		n.opts.SyncIntervalTick+n.opts.SyncRespTimeoutTick {
		return false
	}
	h.probeTicks = leaderTransferRecoveryTicks
	n.wakeLeaderTransferTarget(id)
	return true
}

func (n *Node) wakeLeaderTransferTarget(id uint64) {
	// A higher-term NotifySync can arrive before the old replica knows its
	// leader. Use a heartbeat until the replica has addressed this leader.
	if info := n.replicaSync[id]; info != nil && info.leaderKnown {
		n.sendNotifySync(id)
		n.sendPing(id)
		return
	}
	n.sendPingTo(id, types.ReasonOnlySync)
}

func (n *Node) grantHandoffIdle(e types.Event) bool {
	h := n.handoff
	return n.opts.AutoSuspend && h != nil && h.retryExhausted() && h.requestID == 0 && !h.published &&
		e.ConfigVersion != 0 && e.ConfigVersion == n.cfg.Version && e.Reason == types.ReasonOnlySync &&
		e.Index == n.queue.lastLogIndex+1 && e.StoredIndex == e.Index
}

func (n *Node) canSuspendFromSyncResponse(e types.Event) bool {
	if e.Speed != types.SpeedSuspend {
		return false
	}
	if len(n.cfg.Learners) == 0 && n.cfg.MigrateFrom == 0 && n.cfg.MigrateTo == 0 {
		return true
	}
	// During membership work, only a fully caught-up replica with a grant
	// from the leader of this exact handoff may go idle.
	return n.opts.AutoSuspend && n.cfg.MigrateFrom == n.cfg.Leader &&
		n.cfg.MigrateTo != None && n.cfg.MigrateTo != n.cfg.Leader &&
		e.From == n.cfg.Leader && e.Term == n.cfg.Term && e.ConfigVersion != 0 &&
		e.ConfigVersion == n.cfg.Version && e.Index == n.queue.lastLogIndex+1 &&
		n.queue.storedIndex == n.queue.lastLogIndex
}

func (n *Node) resumeSyncOnPing(e types.Event) {
	if n.suspend && n.opts.AutoSuspend && e.Reason == types.ReasonOnlySync &&
		e.Term == n.cfg.Term && e.From == n.cfg.Leader {
		n.suspend = false
	}
}
