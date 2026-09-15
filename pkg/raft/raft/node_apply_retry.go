package raft

import "go.uber.org/zap"

const (
	// After eight failed attempts stop the exponential retry burst. Keep one
	// recovery probe per 256 ticks (38.4s at the default 150ms interval), so a
	// repaired disk recovers automatically without a process restart.
	applyRetryLimit = 8
	applyProbeTicks = 256
)

func (n *Node) backoffApply() {
	if n.applyFailures < applyRetryLimit {
		n.applyFailures++
	}
	if n.applyFailures >= applyRetryLimit {
		n.applyRetryTicks = applyProbeTicks
		// An explicit persistent-failure alarm, at most once per probe window.
		n.Error("raft apply retry limit reached; inspect storage/config errors; probing after cooldown",
			zap.Uint64("appliedIndex", n.queue.appliedIndex),
			zap.Uint64("committedIndex", n.queue.committedIndex),
			zap.Int("retryAfterTicks", n.applyRetryTicks))
		return
	}
	n.applyRetryTicks = 1 << (n.applyFailures - 1)
}
