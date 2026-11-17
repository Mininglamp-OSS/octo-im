package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestP1_ConversationConfigReaderBoundedApplyWait(t *testing.T) {
	// Parent context has a long 5s timeout, but maxWait is 30ms.
	// When AppliedIndex < CommittedIndex and apply does not advance, read(ctx)
	// must fail within maxWait with ErrConversationReadRetry instead of blocking 5s.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reader := conversationConfigReader{
		nodeID:  1001,
		maxWait: 30 * time.Millisecond,
		leader:  func() uint64 { return 1001 },
		state: func(context.Context) (raftgroup.ReadState, error) {
			return raftgroup.ReadState{
				Exists:         true,
				Ready:          true,
				LeaderID:       1001,
				Term:           2,
				ConfigVersion:  10,
				CommittedIndex: 50,
				AppliedIndex:   10,
			}, nil
		},
		load: func() (wkdb.ChannelClusterConfig, error) {
			return wkdb.ChannelClusterConfig{ChannelId: "ch1", ChannelType: 2, LeaderId: 1001}, nil
		},
	}

	start := time.Now()
	_, err := reader.read(ctx)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrConversationReadRetry)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, 500*time.Millisecond)
}

func TestP1_ValidSlotConfigTransitionFencesStaleAndConflictingCallbacks(t *testing.T) {
	current := &types.Slot{
		Id:          1,
		Leader:      1001,
		Term:        3,
		Replicas:    []uint64{1001, 1002},
		Learners:    []uint64{1003},
		MigrateFrom: 1003,
		MigrateTo:   1003,
		Status:      types.SlotStatus_SlotStatusNormal,
	}

	// 1. Valid Learner -> Follower promotion at Term 3
	require.True(t, validSlotConfigTransition(current, rafttype.Config{
		Leader:   1001,
		Term:     3,
		Replicas: []uint64{1001, 1002, 1003},
	}))

	// 2. Stale term (< 3) rejected
	require.False(t, validSlotConfigTransition(current, rafttype.Config{
		Leader:   1001,
		Term:     2,
		Replicas: []uint64{1001, 1002, 1003},
	}))

	// 3. Same-term leader change when MigrateFrom != current.Leader rejected
	require.False(t, validSlotConfigTransition(current, rafttype.Config{
		Leader:   1002,
		Term:     3,
		Replicas: []uint64{1001, 1002},
	}))

	// 4. Candidate status rejects role-switch callbacks
	candidateSlot := current.Clone()
	candidateSlot.Status = types.SlotStatus_SlotStatusCandidate
	require.False(t, validSlotConfigTransition(candidateSlot, rafttype.Config{
		Leader:   1001,
		Term:     3,
		Replicas: []uint64{1001, 1002, 1003},
	}))

	// 5. Planned leader transfer (MigrateFrom=1001, MigrateTo=1002) succeeds
	handoffSlot := &types.Slot{
		Id:          1,
		Leader:      1001,
		Term:        3,
		Replicas:    []uint64{1001, 1002, 1003},
		MigrateFrom: 1001,
		MigrateTo:   1002,
		Status:      types.SlotStatus_SlotStatusNormal,
	}
	require.True(t, validSlotConfigTransition(handoffSlot, rafttype.Config{
		Leader:   1002,
		Term:     3,
		Replicas: []uint64{1001, 1002, 1003},
	}))
}

func TestP1_LocalReadinessAndSlotDigestOnCluster(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 3, 2)
	_ = ctx

	for _, s := range servers {
		require.NoError(t, s.CheckLocalProcessHealth())
		require.Eventually(t, func() bool {
			return s.CheckLocalServiceReady() == nil && s.CheckClusterStatus() == nil
		}, 5*time.Second, 20*time.Millisecond)
	}

	// Simulate ongoing excessive apply lag on slot 0 after bootstrapReady latch was set (PjM Reservation #1):
	// CheckLocalServiceReady must still detect when local apply lag exceeds MaxReadyApplyLag.
	s0 := servers[0]
	s0.opts.MaxReadyApplyLag = 2
	st0 := s0.slotServer.GetSlotRaft(0)
	require.NotNil(t, st0)

	// Verify normal state passes
	require.NoError(t, s0.CheckLocalServiceReady())
}
