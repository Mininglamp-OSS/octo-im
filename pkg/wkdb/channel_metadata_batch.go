package wkdb

import "github.com/cockroachdb/pebble"

// UpdateChannelInfoBatch applies adjacent metadata updates in Raft order.
// A channel's final flags and last non-nil update time are written atomically.
// Thus a partial cross-shard failure can expose only the old or final value
// per channel. The caller checkpoints only after every shard is durable.
func (wk *wukongDB) UpdateChannelInfoBatch(infos []ChannelInfo) error {
	batches := map[uint32]*pebble.Batch{}
	pending := map[uint64]ChannelInfo{}
	changed := map[uint32][]ChannelInfo{}
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
		old, err := wk.recoveryChannel(info.ChannelId, info.ChannelType)
		if err != nil {
			return err
		}
		old.CreatedAt = nil // an update never changes the creation timestamp/index
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
	return commitRecoveryBatches(batches, wk.sync, func(shard uint32) {
		for _, info := range changed[shard] {
			wk.channelInfoCache.InvalidateChannelInfo(info.ChannelId, info.ChannelType)
		}
	})
}
