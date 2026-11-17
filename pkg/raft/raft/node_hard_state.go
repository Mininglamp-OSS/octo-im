package raft

import "github.com/WuKongIM/WuKongIM/pkg/raft/types"

func (n *Node) setTerm(term uint32) {
	if term > n.cfg.Term {
		n.stateMu.Lock()
		n.voteFor = None
		n.cfg.Term = term
		n.stateMu.Unlock()
	}
}

func (n *Node) hardState() types.HardState {
	return types.HardState{Term: n.cfg.Term, Vote: n.voteFor}
}

func (n *Node) hasUnpersistedHardState() bool {
	return n.opts.SaveHardState != nil && n.hardState() != n.persistedHardState
}
