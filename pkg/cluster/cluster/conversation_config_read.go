package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

type conversationConfigReader struct {
	nodeID  uint64
	maxWait time.Duration
	leader  func() uint64
	epoch   func() (uint32, uint64)
	state   func(context.Context) (raftgroup.ReadState, error)
	load    func() (wkdb.ChannelClusterConfig, error)
}

func sameConversationLeader(before, after raftgroup.ReadState) bool {
	return after.Exists && after.Ready && before.LeaderID == after.LeaderID &&
		before.Term == after.Term && before.ConfigVersion == after.ConfigVersion
}

func (r conversationConfigReader) readEpoch() (uint32, uint64) {
	if r.epoch == nil {
		return 0, 0
	}
	return r.epoch()
}

func (r conversationConfigReader) read(ctx context.Context) (wkdb.ChannelClusterConfig, error) {
	if r.state == nil || r.leader == nil || r.load == nil {
		return wkdb.EmptyChannelClusterConfig, fmt.Errorf("%w: config reader dependencies not initialized", ErrConversationReadRetry)
	}
	beforeSlotTerm, beforeCfgVer := r.readEpoch()
	before, err := r.state(ctx)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	if !before.Exists || !before.Ready || before.LeaderID != r.nodeID || r.leader() != r.nodeID {
		return wkdb.EmptyChannelClusterConfig, fmt.Errorf(
			"%w: metadata slot is not a ready local leader (node=%d stateLeader=%d routeLeader=%d term=%d ver=%d)",
			ErrConversationReadRetry, r.nodeID, before.LeaderID, r.leader(), before.Term, before.ConfigVersion,
		)
	}
	if r.epoch != nil {
		curSlotTerm, curCfgVer := r.readEpoch()
		if curSlotTerm != beforeSlotTerm || curCfgVer != beforeCfgVer || (curSlotTerm != 0 && before.Term != curSlotTerm) {
			return wkdb.EmptyChannelClusterConfig, fmt.Errorf(
				"%w: metadata slot epoch changed before apply wait (slotTerm %d->%d raftTerm=%d cfgVer %d->%d)",
				ErrConversationReadRetry, beforeSlotTerm, curSlotTerm, before.Term, beforeCfgVer, curCfgVer,
			)
		}
	}
	// Fix the target once. Unrelated proposals can continue to commit while we
	// wait; chasing their growing committed index would require an idle slot.
	current := before
	if current.AppliedIndex < before.CommittedIndex {
		waitCtx := ctx
		cancel := func() {}
		if r.maxWait > 0 {
			waitCtx, cancel = context.WithTimeout(ctx, r.maxWait)
		}
		defer cancel()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for current.AppliedIndex < before.CommittedIndex {
			select {
			case <-waitCtx.Done():
				if err := ctx.Err(); err != nil {
					return wkdb.EmptyChannelClusterConfig, fmt.Errorf("%w: %w", ErrConversationReadRetry, err)
				}
				return wkdb.EmptyChannelClusterConfig, fmt.Errorf("%w: metadata slot apply wait exceeded %s: %w", ErrConversationReadRetry, r.maxWait, errors.Join(waitCtx.Err()))
			case <-ticker.C:
			}
			current, err = r.state(waitCtx)
			if err != nil {
				if ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
					return wkdb.EmptyChannelClusterConfig, fmt.Errorf("%w: metadata slot apply wait exceeded %s: %w", ErrConversationReadRetry, r.maxWait, err)
				}
				return wkdb.EmptyChannelClusterConfig, err
			}
			curSlotTerm, curCfgVer := r.readEpoch()
			if !sameConversationLeader(before, current) || r.leader() != r.nodeID ||
				(r.epoch != nil && (curSlotTerm != beforeSlotTerm || curCfgVer != beforeCfgVer)) {
				return wkdb.EmptyChannelClusterConfig, fmt.Errorf(
					"%w: metadata leadership changed while waiting for apply (leader %d->%d term %d->%d slotTerm %d->%d cfgVer %d->%d)",
					ErrConversationReadRetry, before.LeaderID, current.LeaderID, before.Term, current.Term, beforeSlotTerm, curSlotTerm, beforeCfgVer, curCfgVer,
				)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	tmpCfg, readErr := r.load()
	after, err := r.state(ctx)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	afterSlotTerm, afterCfgVer := r.readEpoch()
	if !sameConversationLeader(before, after) || r.leader() != r.nodeID || after.AppliedIndex < before.CommittedIndex ||
		(r.epoch != nil && (afterSlotTerm != beforeSlotTerm || afterCfgVer != beforeCfgVer)) {
		return wkdb.EmptyChannelClusterConfig, fmt.Errorf(
			"%w: metadata leadership or applied barrier changed during read (leader %d->%d term %d->%d slotTerm %d->%d cfgVer %d->%d)",
			ErrConversationReadRetry, before.LeaderID, after.LeaderID, before.Term, after.Term, beforeSlotTerm, afterSlotTerm, beforeCfgVer, afterCfgVer,
		)
	}
	if err := ctx.Err(); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	return tmpCfg, readErr
}
