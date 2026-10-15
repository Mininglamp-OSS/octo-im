package wkdb

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

// UpdateChannelInfoBatch combines adjacent metadata commands. Each channel's
// final row and indexes share one synchronous batch. Callers must not group
// commands across membership changes, deletion, or any other command type.
func (wk *wukongDB) UpdateChannelInfoBatch(infos []ChannelInfo) error {
	batches := make(map[uint32]*pebble.Batch)
	pending := make(map[uint64]ChannelInfo)
	changed := make(map[uint32][]ChannelInfo)
	defer func() {
		for _, batch := range batches {
			_ = batch.Close()
		}
	}()
	for _, info := range infos {
		pk, err := wk.getChannelPrimaryKey(info.ChannelId, info.ChannelType)
		if err != nil {
			return err
		}
		if previous, exists := pending[pk]; exists && info.UpdatedAt == nil {
			info.UpdatedAt = previous.UpdatedAt
		}
		info.Id = pk
		pending[pk] = info
		wk.metrics.UpdateChannelAdd(1)
	}
	for pk, info := range pending {
		shard := wk.GetChannelShardIndex(info.ChannelId, info.ChannelType)
		batch := batches[shard]
		if batch == nil {
			batch = wk.dbs[shard].NewBatch()
			batches[shard] = batch
		}
		// Read persisted fields without consulting a potentially stale cache.
		iter := wk.dbs[shard].NewIter(&pebble.IterOptions{
			LowerBound: key.NewChannelInfoColumnKey(pk, key.MinColumnKey),
			UpperBound: key.NewChannelInfoColumnKey(pk, key.MaxColumnKey),
		})
		var old ChannelInfo
		err := wk.iterChannelInfo(iter, func(value ChannelInfo) bool { old = value; return false })
		if err == nil {
			err = iter.Error()
		}
		_ = iter.Close()
		if err != nil {
			return err
		}
		old.CreatedAt = nil
		if !IsEmptyChannelInfo(old) {
			if err := wk.deleteChannelInfoBaseIndex(old, batch); err != nil {
				return err
			}
		}
		info.CreatedAt = nil
		if info.UpdatedAt == nil {
			info.UpdatedAt = old.UpdatedAt
		}
		if err := wk.writeChannelInfo(pk, info, batch); err != nil {
			return err
		}
		changed[shard] = append(changed[shard], info)
	}
	// There is at most one commit per physical shard. Wait for every started
	// commit, including after an error, before returning or closing batches.
	type result struct {
		shard uint32
		err   error
	}
	done := make(chan result, len(batches))
	for shard, batch := range batches {
		go func(shard uint32, batch *pebble.Batch) {
			done <- result{shard, batch.Commit(wk.sync)}
		}(shard, batch)
	}
	var firstErr error
	for range batches {
		finished := <-done
		if finished.err != nil {
			if firstErr == nil {
				firstErr = finished.err
			}
			continue
		}
		for _, info := range changed[finished.shard] {
			wk.channelInfoCache.InvalidateChannelInfo(info.ChannelId, info.ChannelType)
		}
	}
	return firstErr
}
