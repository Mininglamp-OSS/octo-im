package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

const maxPendingAdmissions = 4096

// RaftGroup ticks every 100 ms by default. Only the pre-publication drain may
// time out; once a SaveConfig request was emitted its outcome can be unknown.
const maxAdmissionDrainTicks = 100

func (n *Node) tickAdmissionDrain() {
	if n.admissionDrainCooldown > 0 {
		n.admissionDrainCooldown--
	}
	if !n.drainingAdmissions || n.handoff != nil && n.handoff.attempted {
		return
	}
	n.admissionDrainTicks++
	if n.admissionDrainTicks >= maxAdmissionDrainTicks {
		n.clearLeaderHandoff()
		n.admissionDrainCooldown = maxAdmissionDrainTicks
	}
}

// AdmitMessage and CompleteMessage run on the owner loop. Admission covers
// queued work as well as proposals: a transfer must drain both before fixing
// its final log boundary. No timer may silently expire an accepted message.
func (n *Node) AdmitMessage(id uint64) error {
	if id == 0 || !n.LeaderReadReady() || n.drainingAdmissions || len(n.messageAdmissions) >= maxPendingAdmissions {
		return types.ErrProposalDropped
	}
	if _, exists := n.messageAdmissions[id]; exists {
		return types.ErrProposalDropped
	}
	if n.messageAdmissions == nil {
		n.messageAdmissions = make(map[uint64]struct{})
	}
	n.messageAdmissions[id] = struct{}{}
	n.KeepAlive()
	return nil
}

func (n *Node) CompleteMessage(id uint64) { delete(n.messageAdmissions, id) }

func (n *Node) admittedProposals(logs []types.Log) bool {
	if !n.drainingAdmissions {
		return true
	}
	for _, log := range logs {
		if _, ok := n.messageAdmissions[log.Id]; !ok {
			return false
		}
	}
	return true
}
