package raft

import (
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

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

func (n *Node) CompleteMessage(id uint64) {
	if _, ok := n.messageAdmissions[id]; !ok {
		return
	}
	delete(n.messageAdmissions, id)
	n.wakeDrainedTransfer()
}

// Only the Raft owner selects the current generation. An abandoned request
// releases its own generation without waiting for the owner or touching a newer one.
type messageConfirmationGeneration struct {
	pending atomic.Int64
}

// MessageConfirmation holds an accepted channel proposal until its result can
// be certified or the caller abandons confirmation.
type MessageConfirmation struct {
	generation *messageConfirmationGeneration
	released   atomic.Bool
}

// BeginMessageConfirmation must run in the same owner callback as a successful
// proposal. Planned handoff may start as soon as the callback returns.
func (n *Node) BeginMessageConfirmation() *MessageConfirmation {
	if n.messageConfirmations == nil {
		n.messageConfirmations = &messageConfirmationGeneration{}
	}
	n.messageConfirmations.pending.Add(1)
	return &MessageConfirmation{generation: n.messageConfirmations}
}

// MessageConfirmationActive and CompleteMessageConfirmation run on the owner.
func (n *Node) MessageConfirmationActive(confirmation *MessageConfirmation) bool {
	return confirmation.generation == n.messageConfirmations && !confirmation.released.Load()
}

func (n *Node) CompleteMessageConfirmation(confirmation *MessageConfirmation) bool {
	if confirmation.generation != n.messageConfirmations || !confirmation.release() {
		return false
	}
	n.wakeDrainedTransfer()
	return true
}

// Abandon relinquishes only the right to certify the result. The accepted log
// entry may still commit. The transfer target will report its tail on a later sync.
func (confirmation *MessageConfirmation) Abandon() {
	confirmation.release()
}

func (confirmation *MessageConfirmation) release() bool {
	if !confirmation.released.CompareAndSwap(false, true) {
		return false
	}
	confirmation.generation.pending.Add(-1)
	return true
}

func (n *Node) hasPendingHandoffWork() bool {
	return len(n.messageAdmissions) != 0 ||
		n.messageConfirmations != nil && n.messageConfirmations.pending.Load() != 0
}

func (n *Node) wakeDrainedTransfer() {
	if n.drainingAdmissions && !n.hasPendingHandoffWork() && n.canWakeLeaderTransferTarget(n.cfg.MigrateTo) {
		n.wakeLeaderTransferTarget(n.cfg.MigrateTo)
	}
}

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
