package options

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestOptionsDefaultsAndCheckValidation(t *testing.T) {
	opts := New(WithRootDir(t.TempDir()), WithLoggerDir(t.TempDir()))
	vp := viper.New()
	opts.ConfigureWithViper(vp)

	require.NoError(t, opts.Check())
	require.Equal(t, 16*1024*1024, opts.Db.MemTableSize)
	require.Equal(t, 16*1024*1024, opts.Db.SlotMemTableSize)
	require.Equal(t, int64(16*1024*1024), opts.Db.BlockCacheSize)
	require.Equal(t, 256, opts.Db.MaxMessagesPerBatch)
	require.Equal(t, 2*1024*1024, opts.Db.MaxBatchBytes)
	require.Equal(t, uint64(2*1024*1024), opts.Cluster.MaxLogSizePerBatch)
	require.Equal(t, 500, opts.Cluster.MaxLogCountPerBatch)
	require.Equal(t, uint64(8), opts.Cluster.LearnerToFollowerMaxApplyLag)
	require.Equal(t, uint64(64), opts.Cluster.MaxReadyApplyLag)
	require.Equal(t, 50, opts.Cluster.MaxApplyStallTicks)
	require.Equal(t, 500*time.Millisecond, opts.Cluster.MaxSlotApplyWait)
	require.Equal(t, 100, opts.Cluster.HandoffDrainTimeoutTick)
	require.Equal(t, 8, opts.Cluster.MaxConcurrentChannelMigrations)
	require.True(t, opts.Cluster.RejectConnBeforeReady)
	require.False(t, opts.Cluster.ChannelLeaderAutoBalance)

	// Verify Viper override binding
	vp2 := viper.New()
	vp2.Set("db.slotMemTableSize", 8*1024*1024)
	vp2.Set("db.blockCacheSize", 32*1024*1024)
	vp2.Set("db.maxMessagesPerBatch", 128)
	vp2.Set("db.maxBatchBytes", 1024*1024)
	vp2.Set("cluster.maxLogSizePerBatch", 512*1024)
	vp2.Set("cluster.maxLogCountPerBatch", 200)
	vp2.Set("cluster.learnerToFollowerMaxApplyLag", 4)
	vp2.Set("cluster.maxReadyApplyLag", 32)
	vp2.Set("cluster.maxApplyStallTicks", 25)
	vp2.Set("cluster.maxSlotApplyWait", "250ms")
	vp2.Set("cluster.handoffDrainTimeoutTick", 40)
	vp2.Set("cluster.maxConcurrentChannelMigrations", 4)
	vp2.Set("cluster.rejectConnBeforeReady", false)
	vp2.Set("cluster.channelLeaderAutoBalance", true)

	opts2 := New(WithRootDir(t.TempDir()), WithLoggerDir(t.TempDir()))
	opts2.ConfigureWithViper(vp2)
	require.NoError(t, opts2.Check())
	require.Equal(t, 8*1024*1024, opts2.Db.SlotMemTableSize)
	require.Equal(t, int64(32*1024*1024), opts2.Db.BlockCacheSize)
	require.Equal(t, 128, opts2.Db.MaxMessagesPerBatch)
	require.Equal(t, 1024*1024, opts2.Db.MaxBatchBytes)
	require.Equal(t, uint64(512*1024), opts2.Cluster.MaxLogSizePerBatch)
	require.Equal(t, 200, opts2.Cluster.MaxLogCountPerBatch)
	require.Equal(t, uint64(4), opts2.Cluster.LearnerToFollowerMaxApplyLag)
	require.Equal(t, uint64(32), opts2.Cluster.MaxReadyApplyLag)
	require.Equal(t, 25, opts2.Cluster.MaxApplyStallTicks)
	require.Equal(t, 250*time.Millisecond, opts2.Cluster.MaxSlotApplyWait)
	require.Equal(t, 40, opts2.Cluster.HandoffDrainTimeoutTick)
	require.Equal(t, 4, opts2.Cluster.MaxConcurrentChannelMigrations)
	require.False(t, opts2.Cluster.RejectConnBeforeReady)
	require.True(t, opts2.Cluster.ChannelLeaderAutoBalance)

	// Invalid values must fail fast in Check()
	bad := *opts2
	bad.Db.SlotMemTableSize = 0
	require.ErrorContains(t, bad.Check(), "db.slotMemTableSize")

	bad = *opts2
	bad.Cluster.MaxLogCountPerBatch = 0
	require.ErrorContains(t, bad.Check(), "cluster.maxLogCountPerBatch")

	bad = *opts2
	bad.Cluster.HandoffDrainTimeoutTick = 0
	require.ErrorContains(t, bad.Check(), "cluster.handoffDrainTimeoutTick")
}
