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
	state   func(context.Context) (raftgroup.ReadState, error)
	load    func() (wkdb.ChannelClusterConfig, error)
}

func sameConversationLeader(before, after raftgroup.ReadState) bool {
	return after.Exists && after.Ready && before.LeaderID == after.LeaderID &&
		before.Term == after.Term && before.ConfigVersion == after.ConfigVersion
}

func (r conversationConfigReader) read(ctx context.Context) (wkdb.ChannelClusterConfig, error) {
	before, err := r.state(ctx)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	if !before.Exists || !before.Ready || before.LeaderID != r.nodeID || r.leader() != r.nodeID {
		return wkdb.EmptyChannelClusterConfig, fmt.Errorf("%w: metadata slot is not a ready local leader", ErrConversationReadRetry)
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
			if !sameConversationLeader(before, current) || r.leader() != r.nodeID {
				return wkdb.EmptyChannelClusterConfig, fmt.Errorf("%w: metadata leadership changed while waiting for apply", ErrConversationReadRetry)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	cfg, readErr := r.load()
	after, err := r.state(ctx)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	if !sameConversationLeader(before, after) || r.leader() != r.nodeID || after.AppliedIndex < before.CommittedIndex {
		return wkdb.EmptyChannelClusterConfig, fmt.Errorf("%w: metadata leadership or applied barrier changed during read", ErrConversationReadRetry)
	}
	if err := ctx.Err(); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	return cfg, readErr
}
