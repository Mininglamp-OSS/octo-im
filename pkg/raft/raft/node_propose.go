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
