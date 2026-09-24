package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestConfigQueueGenerationAndOverflow(t *testing.T) {
	r := newChannelConfigReconciler(context.Background())
	t.Cleanup(r.stop)
	r.capacity = 1
	key := channelConfigKey{"one", 2}
	require.True(t, r.add(key, true))
	_, revision, ok := r.take()
	require.True(t, ok)
	for i := 0; i < 10000; i++ {
		require.True(t, r.add(key, true))
	}
	require.False(t, r.add(channelConfigKey{"two", 2}, true))
	require.Len(t, r.pending, 1)
	r.finish(key, revision, nil)
	_, next, ok := r.take()
	require.True(t, ok, "an old completion must not erase a newer notification")
	require.Greater(t, next, revision)
	r.capacity = 2
	r.finish(key, next, errors.New("unavailable"))
	_, _, ok = r.take()
	require.False(t, ok, "failure must back off")
	r.pending[key].due = time.Now().Add(-time.Second)
	_, next, ok = r.take()
	require.True(t, ok)
	r.finish(key, next, nil)
	require.Empty(t, r.pending)
}

func TestConfigQueueFailureDoesNotBlockScan(t *testing.T) {
	r := newChannelConfigReconciler(context.Background())
	t.Cleanup(r.stop)
	r.capacity = 1
	key := channelConfigKey{"unavailable", 2}
	r.add(key, true)
	_, revision, ok := r.take()
	require.True(t, ok)
	// Concurrent writes and read retries must not disable pressure eviction.
	for i := 0; i < 100; i++ {
		r.add(key, true)
		r.add(key, false)
	}
	r.finish(key, revision, errors.New("peer down"))
	require.True(t, r.add(channelConfigKey{"next persisted row", 2}, false),
		"a full queue of failed tasks must yield space to the recovery scan")
}

func TestConfigScanResumesAfterOverflowAndRevisitsOldKeys(t *testing.T) {
	r := newChannelConfigReconciler(context.Background())
	t.Cleanup(r.stop)
	r.capacity = 1
	r.owned = func(channelConfigKey) bool { return true }
	r.page = func(offset uint64, limit int) ([]wkdb.ChannelClusterConfig, error) {
		var result []wkdb.ChannelClusterConfig
		for id := offset + 1; id <= 3; id++ {
			result = append(result, wkdb.ChannelClusterConfig{Id: id, ChannelId: fmt.Sprint(id), ChannelType: 2})
		}
		return result, nil
	}
	var offset uint64
	for want := uint64(1); want <= 3; want++ {
		next, done := r.scanPage(offset)
		require.Equal(t, want, next)
		require.Equal(t, want == 3, done)
		key, revision, ok := r.take()
		require.True(t, ok)
		require.Equal(t, fmt.Sprint(want), key.id)
		r.finish(key, revision, nil)
		offset = next
	}
	r.scanPage(0)
	key, _, ok := r.take()
	require.True(t, ok)
	require.Equal(t, "1", key.id, "full passes recover edits behind the cursor")
}

func TestConfigWorkersBoundConcurrencyAndCancel(t *testing.T) {
	r := newChannelConfigReconciler(context.Background())
	r.page = func(uint64, int) ([]wkdb.ChannelClusterConfig, error) { return nil, nil }
	r.owned = func(channelConfigKey) bool { return true }
	var active, peak atomic.Int32
	r.process = func(ctx context.Context, key channelConfigKey) error {
		count := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); count > old && !peak.CompareAndSwap(old, count); old = peak.Load() {
		}
		<-ctx.Done()
		return ctx.Err()
	}
	for i := 0; i < 100; i++ {
		r.add(channelConfigKey{fmt.Sprint(i), 2}, true)
	}
	r.start()
	require.Eventually(t, func() bool { return active.Load() == 4 }, time.Second, time.Millisecond)
	r.stop()
	require.Equal(t, int32(4), peak.Load())
	require.Zero(t, active.Load())
	require.False(t, r.add(channelConfigKey{"stopped", 2}, true))
}

func TestConfigWorkersRecoverDroppedHints(t *testing.T) {
	r := newChannelConfigReconciler(context.Background())
	r.capacity = 8
	r.owned = func(channelConfigKey) bool { return true }
	r.page = func(offset uint64, limit int) ([]wkdb.ChannelClusterConfig, error) {
		var configs []wkdb.ChannelClusterConfig
		for id := offset + 1; id <= 64; id++ {
			configs = append(configs, wkdb.ChannelClusterConfig{Id: id, ChannelId: fmt.Sprint(id), ChannelType: 2})
		}
		return configs, nil
	}
	var seen sync.Map
	var attempts, completed atomic.Int32
	r.process = func(ctx context.Context, key channelConfigKey) error {
		if attempts.Add(1) <= 2 {
			return errors.New("temporary peer outage")
		}
		if _, loaded := seen.LoadOrStore(key, true); !loaded {
			completed.Add(1)
		}
		return nil
	}
	// Fill the bounded queue and lose all subsequent initial notifications.
	for id := 1; id <= 64; id++ {
		r.add(channelConfigKey{fmt.Sprint(id), 2}, true)
	}
	r.start()
	t.Cleanup(r.stop)
	var writers sync.WaitGroup
	for i := 0; i < 4; i++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for id := 1; id <= 64; id++ {
				r.add(channelConfigKey{fmt.Sprint(id), 2}, true)
			}
		}()
	}
	writers.Wait()
	require.Eventually(t, func() bool { return completed.Load() == 64 }, 3*time.Second, 10*time.Millisecond)
	r.mu.Lock()
	overflow, retries, pending := r.overflow, r.retries, len(r.pending)
	r.mu.Unlock()
	require.Positive(t, overflow)
	require.Equal(t, uint64(2), retries)
	require.LessOrEqual(t, pending, 8)
}

func TestConfigReconcilePersistedChanges(t *testing.T) {
	for _, mode := range []string{"notification", "scan", "read-hint"} {
		t.Run(mode, func(t *testing.T) {
			s, ctx := newConversationConsistencyServer(t, func(s *Server) {
				if mode != "scan" {
					s.configReconciler.page = func(uint64, int) ([]wkdb.ChannelClusterConfig, error) { return nil, nil }
				}
				if mode != "notification" {
					// Simulate losing the post-write callback, as on a process crash.
					s.store = store.New(store.NewOptions(store.WithNodeId(1), store.WithSlot(s.slotServer), store.WithChannel(s.channelServer), store.WithDB(s.db)))
				}
			})
			cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("recovery", 2)
			require.NoError(t, err)
			require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))
			require.Eventually(t, func() bool {
				s.configReconciler.mu.Lock()
				defer s.configReconciler.mu.Unlock()
				return len(s.configReconciler.pending) == 0
			}, time.Second, time.Millisecond, "drain initial creation hints before changing metadata")
			version, err := s.store.SaveChannelClusterConfig(cfg)
			require.NoError(t, err)
			require.Greater(t, version, cfg.ConfVersion)
			cfg.ConfVersion = version
			if mode == "scan" {
				s.configReconciler.rescan()
			}
			if mode == "read-hint" {
				_ = s.ValidateLocalChannelRead(ctx, cfg)
			}
			require.Eventually(t, func() bool {
				state, err := s.channelServer.ReadLeaderState(ctx, cfg.ChannelId, cfg.ChannelType)
				return err == nil && state.ConfigVersion == version && state.Ready
			}, 5*time.Second, 10*time.Millisecond)
			require.NoError(t, s.ValidateLocalChannelRead(ctx, cfg))
		})
	}
}

func TestConfigReadHintForDivergenceButNotNewerRuntime(t *testing.T) {
	cfg := consistencyConfig()
	cfg.ConfVersion = 3
	hints := 0
	r := conversationReader{hint: func(string, uint8) { hints++ }}
	r.hintLaggingConfig(raftgroup.ReadState{Exists: true, ConfigVersion: 2}, cfg)
	require.Equal(t, 1, hints)
	r.hintLaggingConfig(raftgroup.ReadState{Exists: true, ConfigVersion: 3, Ready: false}, cfg)
	r.hintLaggingConfig(raftgroup.ReadState{Exists: true, ConfigVersion: 4}, cfg)
	r.hintLaggingConfig(raftgroup.ReadState{}, cfg)
	require.Equal(t, 2, hints)
	cfg.Learners = []uint64{2}
	r.hintLaggingConfig(raftgroup.ReadState{}, cfg)
	require.Equal(t, 3, hints)
	require.True(t, conversationStateReady(raftgroup.ReadState{}, cfg), "an orphan learner alone must not block dormant reads")
}
