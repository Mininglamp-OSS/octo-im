package raft

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"go.uber.org/zap"
)

// proposeRejectLogInterval limits the rejection log to one line per node per interval.
const proposeRejectLogInterval = time.Second

type proposeRejectStats struct {
	overlap   atomic.Uint64
	staleTerm atomic.Uint64

	lastLogAt  time.Time // only used by the node's step goroutine
	suppressed uint64
}

// ProposeRejectCounts returns how many leader-local proposals were rejected
// because their index range was not contiguous with the log, and because
// their term was not the current term.
func (n *Node) ProposeRejectCounts() (overlap, staleTerm uint64) {
	return n.proposeRejects.overlap.Load(), n.proposeRejects.staleTerm.Load()
}

// appendLeaderLocal admits logs proposed on this leader. Every log must carry
// the current term and the range must start at lastLogIndex+1; otherwise the
// proposal fails closed with a typed error and the queue is not modified.
func (n *Node) appendLeaderLocal(logs []types.Log) error {
	for _, log := range logs {
		if log.Term != n.cfg.Term {
			n.proposeRejects.staleTerm.Add(1)
			n.logProposeReject(types.ErrProposalStaleTerm, logs, log.Term)
			return types.ErrProposalStaleTerm
		}
	}
	if err := n.queue.appendLeaderLocal(logs...); err != nil {
		if errors.Is(err, types.ErrLeaderLocalOverlap) {
			n.proposeRejects.overlap.Add(1)
		}
		n.logProposeReject(err, logs, logs[0].Term)
		return err
	}
	return nil
}

func (n *Node) logProposeReject(err error, logs []types.Log, logTerm uint32) {
	s := &n.proposeRejects
	now := time.Now()
	if !s.lastLogAt.IsZero() && now.Sub(s.lastLogAt) < proposeRejectLogInterval {
		s.suppressed++
		return
	}
	suppressed := s.suppressed
	s.lastLogAt, s.suppressed = now, 0
	overlap, staleTerm := n.ProposeRejectCounts()
	n.Error("leader-local proposal rejected",
		zap.Error(err),
		zap.String("key", n.Key()),
		zap.Uint64("nodeId", n.opts.NodeId),
		zap.Uint32("term", n.cfg.Term),
		zap.Uint32("logTerm", logTerm),
		zap.Uint64("expectedIndex", n.queue.lastLogIndex+1),
		zap.Uint64("firstIndex", logs[0].Index),
		zap.Uint64("lastIndex", logs[len(logs)-1].Index),
		zap.Int("count", len(logs)),
		zap.Uint64("overlapTotal", overlap),
		zap.Uint64("staleTermTotal", staleTerm),
		zap.Uint64("suppressed", suppressed))
}

// Admit assigns the next contiguous log indexes and the current term to logs
// and appends them through the strict leader-local path. It is the only place
// where RaftGroup proposals get their indexes, and it must be called on the
// goroutine that steps this node, so concurrent proposals can never be given
// the same range. The returned admission identifies the appended range.
func (n *Node) Admit(logs []types.Log) (types.Admission, error) {
	if len(logs) == 0 {
		return types.Admission{}, errors.New("raft: empty proposal")
	}
	term, first := n.cfg.Term, n.queue.lastLogIndex+1
	for i := range logs {
		logs[i].Index = first + uint64(i)
		logs[i].Term = term
	}
	// Step applies the existing role, stopPropose and strict-append checks.
	if err := n.Step(types.Event{Type: types.Propose, Logs: logs}); err != nil {
		return types.Admission{}, err
	}
	return types.Admission{Term: term, FirstIndex: first, LastIndex: first + uint64(len(logs)) - 1}, nil
}

// fenceProposals emits a ProposalFence when this node led term and no longer
// leads it. It is called after every role or configuration change.
func (n *Node) fenceProposals(wasLeader bool, term uint32) {
	if !wasLeader || (n.IsLeader() && n.cfg.Term == term) {
		return
	}
	n.events = append(n.events, types.Event{Type: types.ProposalFence, Term: term})
}
