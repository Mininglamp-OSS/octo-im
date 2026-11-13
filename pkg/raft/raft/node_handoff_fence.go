package raft

// HandoffFence identifies a publication whose outcome still needs to be
// resolved. It carries no instance-bound callback or replica progress.
type HandoffFence struct {
	Version  uint64
	From, To uint64
}

func (n *Node) recordHandoffFence() {
	if save := n.opts.SaveHandoffFence; save != nil {
		h := n.handoff
		save(HandoffFence{Version: h.version, From: h.from, To: h.to})
	}
}

// RestoreHandoffFence runs on the owner after applying the matching channel
// configuration. A retired instance's request ID must never be restored.
func (n *Node) RestoreHandoffFence() {
	if !n.isLeaderTransferTarget(n.cfg.MigrateTo) || n.handoff != nil && n.handoff.attempted {
		return
	}
	n.pauseLeaderHandoff()
	n.drainingAdmissions = true
	n.handoff.attempted = true
}
