package cluster

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func TestRound2_BFR2_5_ReversiblePostBootstrapStorageAndChannelReadiness(t *testing.T) {
	s, _ := newConversationConsistencyServer(t, nil)

	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, s.CheckLocalProcessHealth())

	// 1. Post-bootstrap local storage directory becomes read-only (chmod a-w):
	// CheckLocalServiceReady (/readyz) must fail and reset bootstrapReady to false,
	// while CheckLocalProcessHealth (/healthz) must remain healthy.
	wkdbDir := filepath.Join(s.opts.DataDir, "db", "wukongimdb")
	require.NoError(t, os.Chmod(wkdbDir, 0500))
	t.Cleanup(func() { _ = os.Chmod(wkdbDir, 0700) })

	err := s.CheckLocalServiceReady()
	require.Error(t, err, "CheckLocalServiceReady must fail when local wukongimdb directory is read-only after bootstrap")
	require.False(t, s.bootstrapReady.Load(), "bootstrapReady latch must reset to false on post-bootstrap storage failure")
	require.NoError(t, s.CheckLocalProcessHealth(), "CheckLocalProcessHealth (/healthz) must not kill liveness on storage readiness degradation")

	// Restore write permission: CheckLocalServiceReady must recover automatically.
	require.NoError(t, os.Chmod(wkdbDir, 0700))
	require.NoError(t, s.CheckLocalServiceReady())
	require.True(t, s.bootstrapReady.Load())

	// 2. Post-bootstrap channel Raft apply failure must remove readiness without killing liveness,
	// and recover once ApplyResp succeeds.
	cfg := consistencyConfig()
	_, err = s.store.SaveChannelClusterConfig(cfg)
	require.NoError(t, err)
	require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))

	msg := consistencyMessage(1)
	_, err = s.store.AppendMessages(s.cancelCtx, cfg.ChannelId, cfg.ChannelType, []wkdb.Message{msg})
	require.NoError(t, err)

	chKey := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	ch := s.channelServer.Channel(cfg.ChannelId, cfg.ChannelType)
	require.NotNil(t, ch)

	s.channelServer.AddEvent(chKey, rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Reason: rafttypes.ReasonError,
	})
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() != nil && !s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond, "CheckLocalServiceReady must fail when channel raft apply failure occurs")
	require.NoError(t, s.CheckLocalProcessHealth(), "liveness must remain healthy during channel apply failure")

	s.channelServer.AddEvent(chKey, rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Index:  ch.AppliedIndex(),
		Reason: rafttypes.ReasonOk,
	})
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond, "CheckLocalServiceReady must recover once channel ApplyResp succeeds")

	// 3. Slot Raft store/apply failure also removes readiness reversibly without killing liveness
	slotRaft := s.slotServer.GetSlotRaft(0)
	require.NotNil(t, slotRaft)
	s.slotServer.AddEvent("0", rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Reason: rafttypes.ReasonError,
	})
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() != nil && !s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond)
	require.NoError(t, s.CheckLocalProcessHealth())

	s.slotServer.AddEvent("0", rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Index:  slotRaft.AppliedIndex(),
		Reason: rafttypes.ReasonOk,
	})
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond)
}

func TestRound3_BFR3_2_CrossSlotProposeAndLocalConversationFence(t *testing.T) {
	s, _ := newConversationConsistencyServer(t, nil)
	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil
	}, 5*time.Second, 20*time.Millisecond)

	// 1. Cross-slot / general Store proposals (UpdateUserPluginNo, RemovePluginUser, SaveChannelClusterConfig, AddOrUpdateUserConversations)
	require.NoError(t, s.store.UpdateUserPluginNo("u_plugin_1", "plugin.alpha"))
	require.NoError(t, s.store.RemovePluginUser("u_plugin_1", "plugin.alpha"))

	now := time.Now()
	uid := "u_conv_fence_test"
	conv := wkdb.Conversation{
		Uid:          uid,
		Type:         wkdb.ConversationTypeChat,
		ChannelId:    "g_conv_1",
		ChannelType:  2,
		ReadToMsgSeq: 12,
		CreatedAt:    &now,
		UpdatedAt:    &now,
	}
	// On authoritative local slot leader, AddOrUpdateUserConversationsForLocalLeader succeeds:
	require.NoError(t, s.store.AddOrUpdateUserConversationsForLocalLeader(uid, []wkdb.Conversation{conv}))
}

func TestRound3_BFR3_5_AllShardsSlotLogdbAndConcurrentReadinessProbes(t *testing.T) {
	s, _ := newConversationConsistencyServer(t, nil)

	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 5*time.Second, 20*time.Millisecond)

	// 1. Verify read-only failure & recovery across wukongimdb shards, cluster/logdb shards, and cfglogdb
	targetDirs := []string{
		filepath.Join(s.opts.DataDir, "db", "wukongimdb", "shard000"),
		filepath.Join(s.opts.DataDir, "cluster", "logdb"),
		filepath.Join(s.opts.DataDir, "cluster", "logdb", "shard000"),
		filepath.Join(filepath.Dir(s.opts.ConfigOptions.ConfigPath), "cfglogdb"),
	}
	for _, dir := range targetDirs {
		if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
			continue
		}
		require.NoError(t, os.Chmod(dir, 0500), "chmod 0500 %s", dir)
		err := s.CheckLocalServiceReady()
		require.Error(t, err, "CheckLocalServiceReady must fail immediately when %s is read-only", dir)
		require.False(t, s.bootstrapReady.Load())
		require.NoError(t, os.Chmod(dir, 0700), "chmod 0700 %s", dir)
		require.NoError(t, s.CheckLocalServiceReady(), "CheckLocalServiceReady must recover when %s is writable", dir)
		require.True(t, s.bootstrapReady.Load())
	}

	// 2. Concurrent CheckLocalServiceReady calls (simulating concurrent onConnect + /readyz) must never race
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				require.NoError(t, s.CheckLocalServiceReady())
			}
		}()
	}
	wg.Wait()
}

func TestRound4_BFR4_3_DeletedShardDirAndWriteErrorAndClusterConfigReadiness(t *testing.T) {
	s, _ := newConversationConsistencyServer(t, nil)

	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 5*time.Second, 20*time.Millisecond)

	// 1. Missing / renamed open shard directories (ENOENT) must immediately fail CheckLocalServiceReady
	//    and component CheckWriteHealth, and recover when moved back.
	shardPaths := []string{
		filepath.Join(s.opts.DataDir, "db", "wukongimdb", "shard000"),
		filepath.Join(s.opts.DataDir, "cluster", "logdb", "shard000"),
		filepath.Join(filepath.Dir(s.opts.ConfigOptions.ConfigPath), "cfglogdb"),
	}
	for _, p := range shardPaths {
		backupPath := p + ".bak_r4"
		require.NoError(t, os.Rename(p, backupPath))
		err := s.CheckLocalServiceReady()
		require.Error(t, err, "CheckLocalServiceReady must fail when open directory %s is missing (ENOENT)", p)
		require.False(t, s.bootstrapReady.Load())
		require.NoError(t, s.CheckLocalProcessHealth(), "liveness must remain healthy when storage readiness fails")

		require.NoError(t, os.Rename(backupPath, p))
		require.NoError(t, s.CheckLocalServiceReady(), "CheckLocalServiceReady must recover once %s is restored", p)
		require.True(t, s.bootstrapReady.Load())
	}

	// 2. Recorded Pebble write error on wukongDB must immediately fail CheckWriteHealth and CheckLocalServiceReady,
	//    and recover when a subsequent write clears the error.
	errRecorder, ok := s.db.(interface {
		RecordWriteError(err error)
		CheckWriteHealth() error
	})
	require.True(t, ok)
	errRecorder.RecordWriteError(os.ErrPermission)
	require.Error(t, errRecorder.CheckWriteHealth())
	require.Error(t, s.CheckLocalServiceReady())
	require.False(t, s.bootstrapReady.Load())

	errRecorder.RecordWriteError(nil)
	require.NoError(t, errRecorder.CheckWriteHealth())
	require.NoError(t, s.CheckLocalServiceReady())
	require.True(t, s.bootstrapReady.Load())

	// 3. ClusterConfig Raft apply/store failure must fail cfgServer.CheckWriteHealth() and CheckLocalServiceReady(),
	//    and recover once ApplyResp succeeds.
	s.cfgServer.StepRaftEvent(rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Reason: rafttypes.ReasonError,
	})
	require.Eventually(t, func() bool {
		return s.cfgServer.CheckWriteHealth() != nil && s.CheckLocalServiceReady() != nil && !s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond, "clusterconfig raft apply failure must fail CheckWriteHealth and CheckLocalServiceReady")

	appliedIdx, err := s.cfgServer.AppliedLogIndex()
	require.NoError(t, err)
	s.cfgServer.StepRaftEvent(rafttypes.Event{
		Type:   rafttypes.ApplyResp,
		Index:  appliedIdx,
		Reason: rafttypes.ReasonOk,
	})
	require.Eventually(t, func() bool {
		return s.cfgServer.CheckWriteHealth() == nil && s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 2*time.Second, 10*time.Millisecond, "clusterconfig raft readiness must recover once ApplyResp succeeds")
}

func TestRound5_BFR5_4_PerShardWriteErrorIsolationInSlotLogStorageAndDirectWukongDBWrites(t *testing.T) {
	previousTrace := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previousTrace) })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	config := newTestOptions(t, 1, map[uint64]string{1: address}, clusterconfig.WithSlotCount(1), clusterconfig.WithSlotMaxReplicaCount(1))
	s := New(NewOptions(
		WithAddr("tcp://"+address),
		WithConfigOptions(config),
		WithDataDir(t.TempDir()),
		WithDBWKDbShardNum(2),
		WithDBSlotShardNum(2),
		WithDBWKDbMemTableSize(1<<20),
		WithDBSlotMemTableSize(1<<20),
	))
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)

	require.Eventually(t, func() bool {
		return s.CheckLocalServiceReady() == nil && s.bootstrapReady.Load()
	}, 5*time.Second, 20*time.Millisecond)

	// 1. PebbleShardLogStorage per-shard write-error isolation:
	//    Find two slot keys mapping to distinct physical shards (shardA != shardB).
	slotStore := s.slotServer.Storage()
	require.NotNil(t, slotStore)
	shardKeyB := "0"
	shardIdxB := slotStore.ShardIndex(shardKeyB)
	var shardKeyA string
	for i := 1; i < 64; i++ {
		cand := strconv.Itoa(i)
		if slotStore.ShardIndex(cand) != shardIdxB {
			shardKeyA = cand
			break
		}
	}
	require.NotEmpty(t, shardKeyA, "must locate two slot keys on distinct PebbleShardLogStorage shards")
	shardIdxA := slotStore.ShardIndex(shardKeyA)
	require.NotEqual(t, shardIdxA, shardIdxB)

	// Record a write error on Shard A:
	slotStore.RecordShardWriteError(shardIdxA, os.ErrPermission)
	require.Error(t, slotStore.CheckWriteHealth(), "slotStorage.CheckWriteHealth must fail when Shard A has a write error")
	require.Error(t, s.CheckLocalServiceReady(), "CheckLocalServiceReady must fail when Shard A of slotStorage has a write error")
	require.False(t, s.bootstrapReady.Load())

	// Perform a successful direct SaveHardState and SetLeaderTermStartIndex write on healthy Shard B:
	// Shard A's write error MUST NOT be cleared by Shard B's success!
	require.NoError(t, slotStore.SaveHardState(shardKeyB, rafttypes.HardState{Term: 1}))
	require.NoError(t, slotStore.SetLeaderTermStartIndex(shardKeyB, 1, 1))
	require.Error(t, slotStore.CheckWriteHealth(), "successful write on Shard B must NEVER clear Shard A's write error in PebbleShardLogStorage")
	require.Error(t, s.CheckLocalServiceReady(), "CheckLocalServiceReady must remain failed while Shard A still has an uncleared write error")

	// Perform a successful write on Shard A itself -> Shard A's write error is cleared and readiness recovers:
	require.NoError(t, slotStore.SaveHardState(shardKeyA, rafttypes.HardState{Term: 1}))
	require.NoError(t, slotStore.CheckWriteHealth(), "successful write on Shard A must clear Shard A's write error")
	require.NoError(t, s.CheckLocalServiceReady())
	require.True(t, s.bootstrapReady.Load())

	// 2. wukongDB per-shard write-error tracking across direct Pebble writes (SaveRaftHardState, AddChannel, AddDenylist, AddAllowlist):
	shardErrRecorder, ok := s.db.(interface {
		RecordShardWriteError(shardIdx int, err error)
		CheckWriteHealth() error
	})
	require.True(t, ok, "wukongDB must implement RecordShardWriteError and CheckWriteHealth")

	// Locate a channel that maps to shard 1 (distinct from shard 0):
	var chShard0, chShard1 string
	for i := 0; i < 128; i++ {
		cid := "ch_shard_probe_" + strconv.Itoa(i)
		idx := s.db.GetChannelShardIndex(cid, 2)
		if idx == 0 && chShard0 == "" {
			chShard0 = cid
		} else if idx == 1 && chShard1 == "" {
			chShard1 = cid
		}
		if chShard0 != "" && chShard1 != "" {
			break
		}
	}
	require.NotEmpty(t, chShard0)
	require.NotEmpty(t, chShard1)

	// Record a write error on wukongDB Shard 0:
	shardErrRecorder.RecordShardWriteError(0, os.ErrPermission)
	require.Error(t, shardErrRecorder.CheckWriteHealth())
	require.Error(t, s.CheckLocalServiceReady())
	require.False(t, s.bootstrapReady.Load())

	// Perform a successful direct AddChannel / UpdateChannelAppliedIndex write on Shard 1:
	// Shard 0's write error MUST NOT be cleared by Shard 1's success!
	_, err = s.db.AddChannel(wkdb.ChannelInfo{ChannelId: chShard1, ChannelType: 2})
	require.NoError(t, err)
	require.NoError(t, s.db.UpdateChannelAppliedIndex(chShard1, 2, 1))
	require.Error(t, shardErrRecorder.CheckWriteHealth(), "successful direct write on wukongDB Shard 1 must NEVER clear Shard 0's write error")
	require.Error(t, s.CheckLocalServiceReady())

	// Perform a successful direct write on Shard 0 itself -> Shard 0's write error is cleared and readiness recovers:
	_, err = s.db.AddChannel(wkdb.ChannelInfo{ChannelId: chShard0, ChannelType: 2})
	require.NoError(t, err)
	require.NoError(t, shardErrRecorder.CheckWriteHealth(), "successful direct write on wukongDB Shard 0 must clear Shard 0's write error")
	require.NoError(t, s.CheckLocalServiceReady())
	require.True(t, s.bootstrapReady.Load())
}
