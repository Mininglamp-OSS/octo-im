package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

// LeaderReadReady must be called on the owner's event loop, just like Step.
// A leader draining proposals for transfer must not certify a read boundary.
func (n *Node) LeaderReadReady() bool {
	return n.IsLeader() && !n.stopPropose && !n.truncating
}

// BufferedLogs exposes the not-yet-stored suffix to the owner callback only.
// Neither the slice nor its entries may be retained or modified.
func (n *Node) BufferedLogs() []types.Log { return n.queue.logs }

// ResumeReplication also lets a restored single-voter log regain its commit
// bound. Multi-voter logs still require the existing quorum calculation.
func (n *Node) ResumeReplication() {
	if !n.LeaderReadReady() {
		return
	}
	n.KeepAlive()
	n.updateLeaderCommittedIndex()
	n.sendNotifySync(All)
}
