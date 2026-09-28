package store

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestSubscriberCompletionUsesRemainingRequestDeadline(t *testing.T) {
	s, slots := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Paused = true
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	r, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{
		OperationID: "remaining-budget", ChannelID: "deadline-group", ChannelType: 2, Mode: "add", UIDs: []string{"u"},
	})
	require.NoError(t, err)
	// The source/read-floor/admission already consumed some HTTP budget.
	// A healthy 5ms target must still run with one second left; a guessed
	// two-second reserve must not turn this into an artificial timeout.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err = s.CompleteSubscriberOperation(ctx, r)
	require.NoError(t, err)
	require.Equal(t, "complete", r.State)
	require.NoError(t, ctx.Err())
	require.Greater(t, slots.peak.Load(), int32(0))
	_, err = s.DB().GetConversation("u", "deadline-group", 2)
	require.NoError(t, err)
}

func TestSubscriberActualDeadlineRetainsRecoverablePendingWork(t *testing.T) {
	s, slots := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Paused = true
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	r, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{
		OperationID: "actual-deadline", ChannelID: "blocked-group", ChannelType: 2, Mode: "add", UIDs: []string{"u"},
	})
	require.NoError(t, err)
	slots.targetBlock = make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r, err = s.CompleteSubscriberOperation(ctx, r)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, "pending", r.State)
	require.Greater(t, slots.peak.Load(), int32(0), "target should have been attempted within the real deadline")
	close(slots.targetBlock)
	// Retry the same durable receipt, with no new business operation.
	r, err = s.CompleteSubscriberOperation(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, "complete", r.State)
	backlog, err := s.SubscriberBacklog()
	require.NoError(t, err)
	require.Empty(t, backlog)
}
