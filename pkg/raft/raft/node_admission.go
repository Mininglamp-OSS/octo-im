package raft

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

const maxPendingAdmissions = 4096

// RaftGroup ticks every 100 ms by default. Only the pre-publication drain may
// time out; once a SaveConfig request was emitted its outcome can be unknown.
const maxAdmissionDrainTicks = 100

func (n *Node) maxDrainTicks() int {
	if n.opts != nil && n.opts.MaxAdmissionDrainTicks > 0 {
		return n.opts.MaxAdmissionDrainTicks
	}
	return maxAdmissionDrainTicks
}

func (n *Node) tickAdmissionDrain() {
	if n.admissionDrainCooldown > 0 {
		n.admissionDrainCooldown--
	}
	n.admissionMu.Lock()
	draining := n.drainingAdmissions
	n.admissionMu.Unlock()
	if !draining && (n.handoff == nil || !n.handoff.paused) {
		return
	}
	if n.handoff != nil && n.handoff.attempted {
		return
	}
	n.admissionDrainTicks++
	limit := n.maxDrainTicks()
	if n.admissionDrainTicks >= limit {
		// Fence any stale pre-publication handoff state and recheck local leader identity before resuming writes.
		n.roleChangeID++
		n.clearLeaderHandoff()
		n.admissionDrainCooldown = limit
	}
}

func (n *Node) pendingAdmissionsLocked() int {
	total := len(n.messageAdmissions)
	for id, cnt := range n.proposalAdmissions {
		if cnt > 0 {
			if _, ok := n.messageAdmissions[id]; !ok {
				total++
			}
		}
	}
	return total
}

func (n *Node) hasPendingAdmissions() bool {
	n.admissionMu.Lock()
	defer n.admissionMu.Unlock()
	return len(n.messageAdmissions) > 0 || len(n.proposalAdmissions) > 0 || n.proposalBatches > 0
}

// AdmitMessage and CompleteMessage run on the owner loop or proposal admission path.
// Admission covers queued work as well as proposals: a transfer must drain both before
// fixing its final log boundary. No timer may silently expire an accepted message.
func (n *Node) AdmitMessage(id uint64) error {
	if id == 0 || !n.LeaderReadReady() {
		return types.ErrProposalDropped
	}
	n.admissionMu.Lock()
	if n.drainingAdmissions || n.pendingAdmissionsLocked() >= maxPendingAdmissions {
		n.admissionMu.Unlock()
		return types.ErrProposalDropped
	}
	if _, exists := n.messageAdmissions[id]; exists {
		n.admissionMu.Unlock()
		return types.ErrProposalDropped
	}
	if n.messageAdmissions == nil {
		n.messageAdmissions = make(map[uint64]struct{})
	}
	n.messageAdmissions[id] = struct{}{}
	n.admissionMu.Unlock()
	n.KeepAlive()
	return nil
}

func (n *Node) CompleteMessage(id uint64) {
	n.admissionMu.Lock()
	delete(n.messageAdmissions, id)
	n.admissionMu.Unlock()
}

// AdmitProposalBatch registers a runtime proposal batch in the admission tracker
// before enqueueing types.Propose onto the Raft/RaftGroup event loop, and returns
// an idempotent release function called when Step(Propose) or the caller finishes.
func (n *Node) AdmitProposalBatch(logs []types.Log) (func(), error) {
	if len(logs) == 0 {
		return func() {}, nil
	}
	n.stateMu.RLock()
	isLeader := n.cfg.Leader == n.opts.NodeId
	stoppedOrTruncating := n.stopPropose || n.truncating
	n.stateMu.RUnlock()
	if !isLeader {
		return nil, types.ErrNotLeader
	}
	if stoppedOrTruncating {
		return nil, types.ErrProposalDropped
	}

	n.admissionMu.Lock()
	if n.drainingAdmissions {
		n.admissionMu.Unlock()
		return nil, types.ErrProposalDropped
	}
	newIDs := 0
	seenInBatch := make(map[uint64]struct{}, len(logs))
	for _, lg := range logs {
		if lg.Id == 0 {
			continue
		}
		if _, dup := seenInBatch[lg.Id]; dup {
			continue
		}
		seenInBatch[lg.Id] = struct{}{}
		if _, inMsg := n.messageAdmissions[lg.Id]; !inMsg && n.proposalAdmissions[lg.Id] == 0 {
			newIDs++
		}
	}
	if n.pendingAdmissionsLocked()+newIDs > maxPendingAdmissions {
		n.admissionMu.Unlock()
		return nil, types.ErrProposalDropped
	}
	if n.proposalAdmissions == nil {
		n.proposalAdmissions = make(map[uint64]int)
	}
	admittedIDs := make([]uint64, 0, len(logs))
	for _, lg := range logs {
		if lg.Id != 0 {
			n.proposalAdmissions[lg.Id]++
			admittedIDs = append(admittedIDs, lg.Id)
		}
	}
	n.proposalBatches++
	n.admissionMu.Unlock()
	n.KeepAlive()

	var once sync.Once
	return func() {
		once.Do(func() {
			n.admissionMu.Lock()
			if n.proposalBatches > 0 {
				n.proposalBatches--
			}
			for _, id := range admittedIDs {
				if cnt := n.proposalAdmissions[id]; cnt <= 1 {
					delete(n.proposalAdmissions, id)
				} else {
					n.proposalAdmissions[id] = cnt - 1
				}
			}
			n.admissionMu.Unlock()
		})
	}, nil
}

func (n *Node) admittedProposals(logs []types.Log) bool {
	n.admissionMu.Lock()
	defer n.admissionMu.Unlock()
	if !n.drainingAdmissions {
		return true
	}
	if len(logs) == 0 {
		return false
	}
	for _, log := range logs {
		if log.Id == 0 {
			if n.proposalBatches == 0 {
				return false
			}
			continue
		}
		_, inMsg := n.messageAdmissions[log.Id]
		inProp := n.proposalAdmissions[log.Id] > 0
		if !inMsg && !inProp {
			return false
		}
	}
	return true
}
