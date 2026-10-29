package wkdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigCacheRejectsLateReadAfterWrite(t *testing.T) {
	for _, evict := range []bool{false, true} {
		cache := NewChannelClusterConfigCache(1)
		old := ChannelClusterConfig{ChannelId: "history", ChannelType: 2, ConfVersion: 13,
			Replicas: []uint64{1, 2}, Learners: []uint64{3}}
		generation := cache.readGeneration()
		newer := old.Clone()
		newer.ConfVersion, newer.Replicas, newer.Learners = 14, []uint64{1, 2, 3}, nil
		cache.BatchSetChannelClusterConfigs([]ChannelClusterConfig{newer})
		if evict {
			cache.SetChannelClusterConfig(ChannelClusterConfig{ChannelId: "unrelated", ChannelType: 2})
		}
		cache.cacheReadResult(old, generation)
		got, ok := cache.GetChannelClusterConfig(old.ChannelId, old.ChannelType)
		if evict {
			require.False(t, ok, "eviction must not allow an in-flight old read to republish")
		} else {
			require.True(t, ok)
			require.Equal(t, newer, got)
		}
	}
}

func TestConfigCacheInvalidationFencesInFlightRead(t *testing.T) {
	cache := NewChannelClusterConfigCache(10)
	cfg := ChannelClusterConfig{ChannelId: "history", ChannelType: 2, ConfVersion: 13}
	generation := cache.readGeneration()
	cache.InvalidateChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	cache.cacheReadResult(cfg, generation)
	_, ok := cache.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.False(t, ok)
	cache.cacheReadResult(cfg, cache.readGeneration())
	_, ok = cache.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.True(t, ok, "a fresh read may repopulate the cache")
}

func TestConfigCacheOwnsMemberSlices(t *testing.T) {
	cache := NewChannelClusterConfigCache(10)
	cfg := ChannelClusterConfig{ChannelId: "history", ChannelType: 2, Replicas: []uint64{1, 2}}
	cache.SetChannelClusterConfig(cfg)
	cfg.Replicas[0] = 99
	got, ok := cache.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.True(t, ok)
	require.Equal(t, uint64(1), got.Replicas[0])
	got.Replicas[0] = 98
	got, _ = cache.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
	require.Equal(t, uint64(1), got.Replicas[0])
}
