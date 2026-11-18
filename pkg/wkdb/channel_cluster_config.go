package wkdb

import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

func canonicalChannelConfigLockKey(channelId string, channelType uint8) string {
	var builder strings.Builder
	builder.Grow(len(channelId) + 4)
	builder.WriteString(channelId)
	builder.WriteByte(':')
	builder.WriteString(strconv.FormatUint(uint64(channelType), 10))
	return builder.String()
}

func (wk *wukongDB) getChannelClusterConfigVersionByPrimaryKey(primaryKey uint64) (uint64, error) {
	result, closer, err := wk.defaultShardDB().Get(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ConfVersion))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, err
	}
	defer closer.Close()
	if len(result) < 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(result), nil
}

func (wk *wukongDB) saveChannelClusterConfigsMonotonic(channelClusterConfigs []ChannelClusterConfig, assignPrimaryKeyId bool) error {
	if len(channelClusterConfigs) == 0 {
		return nil
	}

	// 1. Coalesce duplicate (ChannelId, ChannelType) entries in the input slice to the highest ConfVersion
	//    (keeping the later entry when ConfVersion ties, e.g. ConfVersion == 0 in unit tests).
	dedup := make(map[string]ChannelClusterConfig, len(channelClusterConfigs))
	for _, cfg := range channelClusterConfigs {
		lockKey := canonicalChannelConfigLockKey(cfg.ChannelId, cfg.ChannelType)
		if prev, exists := dedup[lockKey]; exists {
			if cfg.ConfVersion < prev.ConfVersion {
				continue
			}
		}
		dedup[lockKey] = cfg.Clone()
	}

	// 2. Acquire per-channel locks in strict canonical lexicographical order to prevent any lock-order inversion.
	sortedKeys := make([]string, 0, len(dedup))
	for k := range dedup {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		wk.dblock.channelClusterConfig.Lock(k)
	}
	defer func() {
		for i := len(sortedKeys) - 1; i >= 0; i-- {
			wk.dblock.channelClusterConfig.Unlock(sortedKeys[i])
		}
	}()

	// 3. Under per-channel locks, atomically compare incoming ConfVersion against persisted ConfVersion in Pebble.
	//    Never allow an older ConfVersion write finishing last to downgrade Pebble or ChannelClusterConfigCache.
	db := wk.defaultShardBatchDB()
	batch := db.NewBatch()
	written := make([]ChannelClusterConfig, 0, len(sortedKeys))
	refreshPrimaryKeys := make([]uint64, 0)

	for _, k := range sortedKeys {
		cfg := dedup[k]
		primaryKey := key.ChannelToNum(cfg.ChannelId, cfg.ChannelType)
		if assignPrimaryKeyId {
			cfg.Id = primaryKey
		}

		persistedVer, err := wk.getChannelClusterConfigVersionByPrimaryKey(primaryKey)
		if err != nil {
			return err
		}
		if persistedVer > 0 && cfg.ConfVersion < persistedVer {
			if cached, found := wk.clusterConfigCache.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType); !found || cached.ConfVersion < persistedVer {
				refreshPrimaryKeys = append(refreshPrimaryKeys, primaryKey)
			}
			continue
		}

		if err := wk.writeChannelClusterConfig(primaryKey, cfg, batch); err != nil {
			return err
		}
		written = append(written, cfg)
	}

	if len(written) > 0 {
		if err := batch.CommitWait(); err != nil {
			return err
		}
		wk.clusterConfigCache.BatchSetChannelClusterConfigs(written)
	}

	for _, pk := range refreshPrimaryKeys {
		if persistedCfg, err := wk.getChannelClusterConfigById(pk); err == nil {
			if !assignPrimaryKeyId {
				persistedCfg.Id = 0
			}
			wk.clusterConfigCache.SetChannelClusterConfig(persistedCfg)
		}
	}

	return nil
}

func (wk *wukongDB) SaveChannelClusterConfig(channelClusterConfig ChannelClusterConfig) error {

	wk.metrics.SaveChannelClusterConfigAdd(1)

	if wk.opts.EnableCost {
		start := time.Now()
		defer func() {
			end := time.Since(start)
			if end > time.Millisecond*500 {
				wk.Warn("save channel cluster config", zap.Duration("cost", time.Since(start)), zap.String("channelId", channelClusterConfig.ChannelId), zap.Uint8("channelType", channelClusterConfig.ChannelType))
			}
		}()
	}

	return wk.saveChannelClusterConfigsMonotonic([]ChannelClusterConfig{channelClusterConfig}, true)
}

func (wk *wukongDB) SaveChannelClusterConfigs(channelClusterConfigs []ChannelClusterConfig) error {

	if wk.opts.EnableCost {
		start := time.Now()
		defer func() {
			end := time.Since(start)
			if end > time.Millisecond*500 {
				wk.Warn("SaveChannelClusterConfigs too cost", zap.Duration("cost", time.Since(start)), zap.Int("count", len(channelClusterConfigs)))
			}
		}()
	}

	wk.metrics.SaveChannelClusterConfigsAdd(1)

	return wk.saveChannelClusterConfigsMonotonic(channelClusterConfigs, false)
}

func (wk *wukongDB) GetChannelClusterConfig(channelId string, channelType uint8) (ChannelClusterConfig, error) {

	wk.metrics.GetChannelClusterConfigAdd(1)

	// 先从缓存获取
	if config, found := wk.clusterConfigCache.GetChannelClusterConfig(channelId, channelType); found {
		return config, nil
	}

	start := time.Now()
	defer func() {
		end := time.Since(start)
		if end > time.Millisecond*200 {
			wk.Warn("get channel cluster config cost to long", zap.Duration("cost", end), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		}
	}()

	// 持频道锁重检缓存并回源 Pebble，防止并发保存与缓存回填乱序覆盖
	wk.dblock.channelClusterConfig.lockByChannel(channelId, channelType)
	defer wk.dblock.channelClusterConfig.unlockByChannel(channelId, channelType)

	if config, found := wk.clusterConfigCache.GetChannelClusterConfig(channelId, channelType); found {
		return config, nil
	}

	primaryKey := key.ChannelToNum(channelId, channelType)

	config, err := wk.getChannelClusterConfigById(primaryKey)
	if err != nil {
		return EmptyChannelClusterConfig, err
	}

	// 将结果写入缓存
	wk.clusterConfigCache.SetChannelClusterConfig(config)

	return config.Clone(), nil
}

func (wk *wukongDB) getChannelClusterConfigById(id uint64) (ChannelClusterConfig, error) {
	iter := wk.defaultShardDB().NewIter(&pebble.IterOptions{
		LowerBound: key.NewChannelClusterConfigColumnKey(id, key.MinColumnKey),
		UpperBound: key.NewChannelClusterConfigColumnKey(id, key.MaxColumnKey),
	})
	defer iter.Close()

	var resultCfg ChannelClusterConfig = EmptyChannelClusterConfig
	err := wk.iteratorChannelClusterConfig(iter, func(cfg ChannelClusterConfig) bool {
		resultCfg = cfg
		return false
	})

	if err != nil {
		return EmptyChannelClusterConfig, err
	}

	if IsEmptyChannelClusterConfig(resultCfg) {
		return EmptyChannelClusterConfig, ErrNotFound
	}
	resultCfg.Id = id

	return resultCfg, nil
}

func (wk *wukongDB) GetChannelClusterConfigVersion(channelId string, channelType uint8) (uint64, error) {
	primaryKey := key.ChannelToNum(channelId, channelType)
	result, closer, err := wk.defaultShardDB().Get(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ConfVersion))
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(result) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(result), nil
}

// func (wk *wukongDB) DeleteChannelClusterConfig(channelId string, channelType uint8) error {

// 	primaryKey := key.channelToNum(channelId, channelType)

// 	cfg, err := wk.getChannelClusterConfigById(primaryKey)
// 	if err != nil {
// 		return err
// 	}
// 	batch := wk.defaultShardDB().NewIndexedBatch()
// 	defer batch.Close()
// 	err = wk.deleteChannelClusterConfig(cfg, batch)
// 	if err != nil {
// 		return err
// 	}
// 	return batch.Commit(wk.sync)
// }

func (wk *wukongDB) GetChannelClusterConfigs(offsetId uint64, limit int) ([]ChannelClusterConfig, error) {
	if offsetId == math.MaxUint64 || limit <= 0 {
		return nil, nil
	}

	wk.metrics.GetChannelClusterConfigsAdd(1)

	iter := wk.defaultShardDB().NewIter(&pebble.IterOptions{
		LowerBound: key.NewChannelClusterConfigColumnKey(offsetId+1, key.MinColumnKey),
		UpperBound: key.NewChannelClusterConfigColumnKey(math.MaxUint64, key.MaxColumnKey),
	})
	defer iter.Close()

	results := make([]ChannelClusterConfig, 0)
	err := wk.iteratorChannelClusterConfig(iter, func(cfg ChannelClusterConfig) bool {
		if len(results) >= limit {
			return false
		}
		if cfg.Id > offsetId {
			results = append(results, cfg)
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	return results, nil

}

func (wk *wukongDB) SearchChannelClusterConfig(req ChannelClusterConfigSearchReq, filter ...func(cfg ChannelClusterConfig) bool) ([]ChannelClusterConfig, error) {

	wk.metrics.SearchChannelClusterConfigAdd(1)

	if req.ChannelId != "" {
		cfg, err := wk.GetChannelClusterConfig(req.ChannelId, req.ChannelType)
		if err != nil {
			if err == pebble.ErrNotFound {
				return nil, nil
			}
			return nil, err
		}
		if len(filter) > 0 {
			for _, fnc := range filter {
				v := fnc(cfg)
				if !v {
					return nil, nil
				}
			}
		}
		return []ChannelClusterConfig{cfg}, nil
	}

	iterFnc := func(cfgs *[]ChannelClusterConfig) func(cfg ChannelClusterConfig) bool {
		currentSize := 0
		return func(cfg ChannelClusterConfig) bool {
			if req.Pre {
				if req.OffsetCreatedAt > 0 && cfg.CreatedAt != nil && cfg.CreatedAt.UnixNano() <= req.OffsetCreatedAt {
					return false
				}
			} else {
				if req.OffsetCreatedAt > 0 && cfg.CreatedAt != nil && cfg.CreatedAt.UnixNano() >= req.OffsetCreatedAt {
					return false
				}
			}

			if currentSize > req.Limit {
				return false
			}

			if len(filter) > 0 {
				for _, fnc := range filter {
					v := fnc(cfg)
					if !v {
						return true
					}
				}
			}

			currentSize++
			*cfgs = append(*cfgs, cfg)

			return true
		}
	}

	cfgs := make([]ChannelClusterConfig, 0, req.Limit)
	fnc := iterFnc(&cfgs)

	start := uint64(req.OffsetCreatedAt)
	end := uint64(math.MaxUint64)
	if req.OffsetCreatedAt > 0 {
		if req.Pre {
			start = uint64(req.OffsetCreatedAt + 1)
			end = uint64(math.MaxUint64)
		} else {
			start = 0
			end = uint64(req.OffsetCreatedAt)
		}
	}

	db := wk.defaultShardDB()

	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.CreatedAt, start, 0),
		UpperBound: key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.CreatedAt, end, 0),
	})
	defer iter.Close()

	var iterStepFnc func() bool
	if req.Pre {
		if !iter.First() {
			return nil, nil
		}
		iterStepFnc = iter.Next
	} else {
		if !iter.Last() {
			return nil, nil
		}
		iterStepFnc = iter.Prev
	}
	for ; iter.Valid(); iterStepFnc() {
		_, id, err := key.ParseChannelClusterConfigSecondIndexKey(iter.Key())
		if err != nil {
			return nil, err
		}

		dataIter := db.NewIter(&pebble.IterOptions{
			LowerBound: key.NewChannelClusterConfigColumnKey(id, key.MinColumnKey),
			UpperBound: key.NewChannelClusterConfigColumnKey(id, key.MaxColumnKey),
		})
		defer dataIter.Close()

		var c ChannelClusterConfig
		err = wk.iteratorChannelClusterConfig(dataIter, func(cfg ChannelClusterConfig) bool {
			c = cfg
			return false
		})
		if err != nil {
			return nil, err
		}
		if !fnc(c) {
			break
		}
	}

	return cfgs, nil
}

func (wk *wukongDB) GetChannelClusterConfigCountWithSlotId(slotId uint32) (int, error) {

	wk.metrics.GetChannelClusterConfigCountWithSlotIdAdd(1)

	cfgs, err := wk.GetChannelClusterConfigWithSlotId(slotId)
	if err != nil {
		return 0, err
	}
	return len(cfgs), nil
}

func (wk *wukongDB) GetChannelClusterConfigWithSlotId(slotId uint32) ([]ChannelClusterConfig, error) {

	wk.metrics.GetChannelClusterConfigWithSlotIdAdd(1)

	iter := wk.defaultShardDB().NewIter(&pebble.IterOptions{
		LowerBound: key.NewChannelClusterConfigColumnKey(0, key.MinColumnKey),
		UpperBound: key.NewChannelClusterConfigColumnKey(math.MaxUint64, key.MaxColumnKey),
	})
	defer iter.Close()

	results := make([]ChannelClusterConfig, 0)
	err := wk.iteratorChannelClusterConfig(iter, func(cfg ChannelClusterConfig) bool {
		resultSlotId := wk.channelSlotId(cfg.ChannelId)
		if slotId == resultSlotId {
			results = append(results, cfg)
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// func (wk *wukongDB) deleteChannelClusterConfig(channelClusterConfig ChannelClusterConfig, w *Batch) error {

// 	// delete channel cluster config
// 	w.DeleteRange(key.NewChannelClusterConfigColumnKey(channelClusterConfig.Id, key.MinColumnKey), key.NewChannelClusterConfigColumnKey(channelClusterConfig.Id, key.MaxColumnKey))

// 	// delete index
// 	err := wk.deleteChannelClusterConfigIndex(channelClusterConfig.Id, channelClusterConfig, w)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func (wk *wukongDB) deleteChannelClusterConfigLeaderIndex(id uint64, w *pebble.Batch) error {

// 	leaderValue, closer, err := w.Get(key.NewChannelClusterConfigColumnKey(id, key.TableChannelClusterConfig.Column.LeaderId))
// 	if err != nil && err != pebble.ErrNotFound {
// 		return err
// 	}
// 	if closer != nil {
// 		defer closer.Close()
// 	}

// 	if len(leaderValue) == 0 {
// 		return nil
// 	}

// 	leaderId := wk.endian.Uint64(leaderValue)

// 	// delete old leader second index
// 	err = w.Delete(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.OtherIndex.LeaderId, leaderId, id), wk.noSync)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (wk *wukongDB) writeChannelClusterConfig(primaryKey uint64, channelClusterConfig ChannelClusterConfig, w *Batch) error {

	// channelId
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ChannelId), []byte(channelClusterConfig.ChannelId))

	// channelType
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ChannelType), []byte{channelClusterConfig.ChannelType})

	// replicaMaxCount
	var replicaMaxCountBytes = make([]byte, 2)
	binary.BigEndian.PutUint16(replicaMaxCountBytes, channelClusterConfig.ReplicaMaxCount)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ReplicaMaxCount), replicaMaxCountBytes)

	// replicas
	var replicasBytes = make([]byte, 8*len(channelClusterConfig.Replicas))
	for i, replica := range channelClusterConfig.Replicas {
		binary.BigEndian.PutUint64(replicasBytes[i*8:], replica)
	}
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Replicas), replicasBytes)

	// learners
	var learnersBytes = make([]byte, 8*len(channelClusterConfig.Learners))
	for i, learner := range channelClusterConfig.Learners {
		binary.BigEndian.PutUint64(learnersBytes[i*8:], learner)
	}
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Learners), learnersBytes)

	// leaderId
	leaderIdBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(leaderIdBytes, channelClusterConfig.LeaderId)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.LeaderId), leaderIdBytes)

	// term
	termBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(termBytes, channelClusterConfig.Term)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Term), termBytes)

	// migrateFrom
	migrateFromBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(migrateFromBytes, channelClusterConfig.MigrateFrom)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.MigrateFrom), migrateFromBytes)

	// migrateTo
	migrateToBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(migrateToBytes, channelClusterConfig.MigrateTo)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.MigrateTo), migrateToBytes)

	// status
	statusBytes := make([]byte, 1)
	statusBytes[0] = uint8(channelClusterConfig.Status)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Status), statusBytes)

	// config version
	configVersionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(configVersionBytes, channelClusterConfig.ConfVersion)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.ConfVersion), configVersionBytes)

	//version
	versionBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(versionBytes, channelClusterConfig.version)
	w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.Version), versionBytes)

	if channelClusterConfig.CreatedAt != nil {
		// createdAt
		ct := uint64(channelClusterConfig.CreatedAt.UnixNano())
		var createdAtBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(createdAtBytes, ct)
		w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.CreatedAt), createdAtBytes)
	}

	if channelClusterConfig.UpdatedAt != nil {
		// updatedAt
		up := uint64(channelClusterConfig.UpdatedAt.UnixNano())
		var updatedAtBytes = make([]byte, 8)
		binary.BigEndian.PutUint64(updatedAtBytes, up)
		w.Set(key.NewChannelClusterConfigColumnKey(primaryKey, key.TableChannelClusterConfig.Column.UpdatedAt), updatedAtBytes)
	}

	// write index
	if err := wk.writeChannelClusterConfigIndex(primaryKey, channelClusterConfig, w); err != nil {
		return err
	}

	return nil
}

func (wk *wukongDB) writeChannelClusterConfigIndex(primaryKey uint64, channelClusterConfig ChannelClusterConfig, w *Batch) error {

	// channel index
	primaryKeyBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(primaryKeyBytes, primaryKey)
	w.Set(key.NewChannelClusterConfigIndexKey(channelClusterConfig.ChannelId, channelClusterConfig.ChannelType), primaryKeyBytes)

	// leader second index
	w.Set(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.LeaderId, channelClusterConfig.LeaderId, primaryKey), nil)
	if channelClusterConfig.CreatedAt != nil {
		ct := uint64(channelClusterConfig.CreatedAt.UnixNano())
		// createdAt second index
		w.Set(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.CreatedAt, ct, primaryKey), nil)
	}

	if channelClusterConfig.UpdatedAt != nil {
		up := uint64(channelClusterConfig.UpdatedAt.UnixNano())
		// updatedAt second index
		w.Set(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.UpdatedAt, up, primaryKey), nil)
	}
	return nil
}

func (wk *wukongDB) deleteChannelClusterConfigIndex(primaryKey uint64, channelClusterConfig ChannelClusterConfig, w *Batch) error {

	// channel index
	w.Delete(key.NewChannelClusterConfigIndexKey(channelClusterConfig.ChannelId, channelClusterConfig.ChannelType))

	// leader second index
	w.Delete(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.LeaderId, channelClusterConfig.LeaderId, primaryKey))

	if channelClusterConfig.CreatedAt != nil {
		ct := uint64(channelClusterConfig.CreatedAt.UnixNano())
		// createdAt second index
		w.Delete(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.CreatedAt, ct, primaryKey))
	}

	if channelClusterConfig.UpdatedAt != nil {
		up := uint64(channelClusterConfig.UpdatedAt.UnixNano())
		// updatedAt second index
		w.Delete(key.NewChannelClusterConfigSecondIndexKey(key.TableChannelClusterConfig.SecondIndex.UpdatedAt, up, primaryKey))
	}

	return nil
}

func (wk *wukongDB) iteratorChannelClusterConfig(iter *pebble.Iterator, iterFnc func(cfg ChannelClusterConfig) bool) error {

	var (
		preId                   uint64
		preChannelClusterConfig ChannelClusterConfig
		lastNeedAppend          bool = true
		hasData                 bool = false
	)
	for iter.First(); iter.Valid(); iter.Next() {

		id, columnName, err := key.ParseChannelClusterConfigColumnKey(iter.Key())
		if err != nil {
			return err
		}

		if id != preId {
			if preId != 0 {
				if !iterFnc(preChannelClusterConfig) {
					lastNeedAppend = false
					break
				}

			}
			preId = id
			preChannelClusterConfig = ChannelClusterConfig{
				Id: id,
			}
		}

		switch columnName {
		case key.TableChannelClusterConfig.Column.ChannelId:
			preChannelClusterConfig.ChannelId = string(iter.Value())
		case key.TableChannelClusterConfig.Column.ChannelType:
			preChannelClusterConfig.ChannelType = iter.Value()[0]
		case key.TableChannelClusterConfig.Column.ReplicaMaxCount:
			preChannelClusterConfig.ReplicaMaxCount = wk.endian.Uint16(iter.Value())
		case key.TableChannelClusterConfig.Column.Replicas:
			replicas := make([]uint64, len(iter.Value())/8)
			for i := 0; i < len(replicas); i++ {
				replicas[i] = wk.endian.Uint64(iter.Value()[i*8:])
			}
			preChannelClusterConfig.Replicas = replicas
		case key.TableChannelClusterConfig.Column.Learners:
			learners := make([]uint64, len(iter.Value())/8)
			for i := 0; i < len(learners); i++ {
				learners[i] = wk.endian.Uint64(iter.Value()[i*8:])
			}
			preChannelClusterConfig.Learners = learners
		case key.TableChannelClusterConfig.Column.LeaderId:
			preChannelClusterConfig.LeaderId = wk.endian.Uint64(iter.Value())
		case key.TableChannelClusterConfig.Column.Term:
			preChannelClusterConfig.Term = wk.endian.Uint32(iter.Value())
		case key.TableChannelClusterConfig.Column.MigrateFrom:
			preChannelClusterConfig.MigrateFrom = wk.endian.Uint64(iter.Value())
		case key.TableChannelClusterConfig.Column.MigrateTo:
			preChannelClusterConfig.MigrateTo = wk.endian.Uint64(iter.Value())
		case key.TableChannelClusterConfig.Column.Status:
			preChannelClusterConfig.Status = ChannelClusterStatus(iter.Value()[0])
		case key.TableChannelClusterConfig.Column.ConfVersion:
			preChannelClusterConfig.ConfVersion = wk.endian.Uint64(iter.Value())
		case key.TableChannelClusterConfig.Column.Version:
			preChannelClusterConfig.version = wk.endian.Uint16(iter.Value())
		case key.TableChannelClusterConfig.Column.CreatedAt:
			tm := int64(wk.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preChannelClusterConfig.CreatedAt = &t
			}
		case key.TableChannelClusterConfig.Column.UpdatedAt:
			tm := int64(wk.endian.Uint64(iter.Value()))
			if tm > 0 {
				t := time.Unix(tm/1e9, tm%1e9)
				preChannelClusterConfig.UpdatedAt = &t
			}
		}
		hasData = true
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if lastNeedAppend && hasData {
		_ = iterFnc(preChannelClusterConfig)
	}
	return nil
}
