package channel

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type handoffFenceMetadata struct {
	icluster.ICluster
	applied atomic.Pointer[wkdb.ChannelClusterConfig]
}

func (m *handoffFenceMetadata) GetOrCreateChannelClusterConfigFromSlotLeader(string, uint8) (wkdb.ChannelClusterConfig, error) {
	return m.applied.Load().Clone(), nil
}

func TestHandoffFenceSurvivesIdleRebuild(t *testing.T) {
	t.Run("three voters", func(t *testing.T) {
		testHandoffFenceSurvivesIdleRebuild(t, []uint64{1, 2, 3}, nil)
	})
	t.Run("one voter and one learner", func(t *testing.T) {
		testHandoffFenceSurvivesIdleRebuild(t, []uint64{1}, []uint64{2})
	})
}

func testHandoffFenceSurvivesIdleRebuild(t *testing.T, replicas, learners []uint64) {
	t.Helper()
	const id, typ = "retry", 2
	metadata := &handoffFenceMetadata{}
	transfer := wkdb.ChannelClusterConfig{
		ChannelId: id, ChannelType: typ, LeaderId: 1, Term: 1, ConfVersion: 2,
		Replicas: replicas, Learners: learners, ReplicaMaxCount: uint16(len(replicas)), MigrateFrom: 1, MigrateTo: 2,
	}
	metadata.applied.Store(&transfer)

	network := retryNetwork()
	var attempts atomic.Int32
	publication := make(chan types.Config, 1)
	nodes := append(slices.Clone(replicas), learners...)
	servers := make([]*Server, 0, len(nodes))
	for _, node := range nodes {
		opts := []Option{WithCluster(metadata), WithDestoryAfterIdleTick(25)}
		if node == 1 {
			opts = append(opts, WithOnSaveConfig(func(_ string, _ uint8, cfg types.Config) error {
				// The publication call returns before its outcome is known.
				// Foreground reads still return the last applied config.
				if attempts.Add(1) == 1 {
					publication <- cfg
				}
				return context.DeadlineExceeded
			}))
		}
		servers = append(servers, retryServer(t, node, retryDB(t), replicas, network, opts...))
	}
	source := servers[0]

	retryPropose(t, source, retryMessage(901, "writer", "before-handoff"))
	require.Eventually(t, func() bool {
		for _, s := range servers {
			if !slices.Contains(replicas, s.opts.NodeId) {
				continue
			}
			seq, _, err := s.opts.DB.GetChannelLastMessageSeq(id, typ)
			if err != nil || seq != 1 {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond)

	original := source.Channel(id, typ)
	require.NotNil(t, original)
	for i := len(servers) - 1; i >= 0; i-- {
		require.NoError(t, servers[i].SwitchConfig(id, typ, transfer))
	}
	select {
	case cfg := <-publication:
		require.Equal(t, uint64(2), cfg.Leader)
		require.Equal(t, uint32(2), cfg.Term)
	case <-time.After(4 * time.Second):
		t.Fatal("planned handoff did not attempt publication")
	}
	require.Eventually(t, func() bool {
		return attempts.Load() >= 6 && source.Channel(id, typ) != original
	}, 25*time.Second, 10*time.Millisecond, "the original source Raft must retire after failed publications")

	// Keep the target quiet during the cold send. The remaining voters can
	// commit a message if the rebuilt source opens its write path.
	network.block(2, true)
	older := transfer.Clone()
	older.ConfVersion--
	older.MigrateFrom, older.MigrateTo = 0, 0
	older.Learners = nil
	metadata.applied.Store(&older)
	staleCtx, cancelStale := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelStale()
	responses, err := source.ProposeBatchUntilAppliedTimeoutForLocal(staleCtx, id, typ,
		retryRequests(t, retryMessage(902, "writer", "older-than-handoff")))
	require.ErrorIs(t, err, raft.ErrConfigVersionStale)
	require.Empty(t, responses)
	require.False(t, source.ExistChannel(id, typ))

	metadata.applied.Store(&transfer)
	sendCtx, cancelSend := context.WithTimeout(context.Background(), time.Second)
	defer cancelSend()
	started := time.Now()
	responses, err = source.ProposeBatchUntilAppliedTimeoutForLocal(sendCtx, id, typ,
		retryRequests(t, retryMessage(902, "writer", "while-publish-unresolved")))
	require.Less(t, time.Since(started), 4*time.Second, "cold wake must not wait for write readiness behind the fence")
	require.Error(t, err)
	if !errors.Is(err, context.DeadlineExceeded) {
		require.ErrorIs(t, err, ErrSendUnavailable)
	}
	require.Empty(t, responses)
	rebuilt := source.Channel(id, typ)
	require.NotNil(t, rebuilt, "the cold send must have rebuilt the source")
	require.NotSame(t, original, rebuilt)

	stateCtx, cancelState := context.WithTimeout(context.Background(), time.Second)
	defer cancelState()
	state, err := source.ReadLeaderState(stateCtx, id, typ)
	require.NoError(t, err)
	require.Equal(t, transfer.ConfVersion, state.ConfigVersion)
	require.False(t, state.Ready)

	// A newer version of the same plan still has to resolve the handoff.
	refreshed := transfer.Clone()
	refreshed.ConfVersion++
	metadata.applied.Store(&refreshed)
	require.NoError(t, source.SwitchConfig(id, typ, refreshed))
	state, err = source.ReadLeaderState(stateCtx, id, typ)
	require.NoError(t, err)
	require.Equal(t, refreshed.ConfVersion, state.ConfigVersion)
	require.False(t, state.Ready)

	enqueued := false
	require.ErrorIs(t, source.AdmitMessage(stateCtx, id, typ, 903, 64, func(done func()) {
		enqueued = true
		done()
	}), ErrSendUnavailable)
	require.False(t, enqueued)
	for _, s := range servers {
		require.Equal(t, uint64(1), retryTail(t, s.opts.DB))
	}

	// A newer authoritative cancellation supersedes the unresolved transfer.
	canceled := refreshed.Clone()
	canceled.ConfVersion++
	canceled.MigrateFrom, canceled.MigrateTo = 0, 0
	canceled.Learners = nil
	metadata.applied.Store(&canceled)
	for i := len(servers) - 1; i >= 0; i-- {
		require.NoError(t, servers[i].SwitchConfig(id, typ, canceled))
	}
	network.block(2, false)
	responses = retryPropose(t, source, retryMessage(904, "writer", "after-authoritative-cancellation"))
	require.Len(t, responses, 1)
	require.Equal(t, uint64(2), responses[0].Index)
	require.Equal(t, uint64(904), responses[0].CanonicalID)
}
