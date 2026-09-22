package wkdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

const recoveryEffectPage byte = 11
const SubscriberWorkChunkSize = 64
const MaxSubscriberWorkPage = 1024

func recoveryEffectPrefix(slot uint32, version uint64) []byte {
	return binary.BigEndian.AppendUint64(recoverySlotKey(recoveryEffectPage, slot), version)
}

func recoveryEffectKey(slot uint32, version uint64, page int) []byte {
	return binary.BigEndian.AppendUint64(recoveryEffectPrefix(slot, version), uint64(page))
}

// Effects are immutable; the small scheduling header is the only value that
// changes on retries or progress. Do not retain the original (possibly 1 MiB)
// request in that header: finalization only needs the operation identity.
func stageSubscriberWork(batch *pebble.Batch, w SubscriberWork) error {
	w.Total = len(w.Effects)
	for start := 0; start < w.Total; start += SubscriberWorkChunkSize {
		if err := recoverySet(batch, recoveryEffectKey(w.SlotID, w.Version, start/SubscriberWorkChunkSize), w.Effects[start:min(start+SubscriberWorkChunkSize, w.Total)]); err != nil {
			return err
		}
	}
	w.Effects = nil
	w.Paged = true
	w.Operation = SubscriberOperation{OperationID: w.Operation.OperationID, ChannelID: w.Operation.ChannelID, ChannelType: w.Operation.ChannelType, Mode: w.Operation.Mode, CreatedAt: w.Operation.CreatedAt}
	return recoverySet(batch, recoveryPendingKey(w.SlotID, w.Version), w)
}

// GetSubscriberWorkPage reads at most limit effects, even for very large groups.
// The caller holds the operation gate and checkpoints only after target apply.
func (wk *wukongDB) GetSubscriberWorkPage(w SubscriberWork, limit int) ([]ConversationEffect, error) {
	if limit < 1 || limit > MaxSubscriberWorkPage || w.Next < 0 || w.Next > w.Total {
		return nil, errors.New("invalid subscriber work page")
	}
	end := min(w.Next+limit, w.Total)
	if !w.Paged {
		return append([]ConversationEffect(nil), w.Effects[w.Next:end]...), nil
	}
	db := wk.channelDb(w.Operation.ChannelID, w.Operation.ChannelType)
	effects := make([]ConversationEffect, 0, end-w.Next)
	for at := w.Next; at < end; {
		pageIndex := at / SubscriberWorkChunkSize
		var page []ConversationEffect
		found, err := recoveryRead(db, recoveryEffectKey(w.SlotID, w.Version, pageIndex), &page)
		if err != nil {
			return nil, err
		}
		want := min(SubscriberWorkChunkSize, w.Total-pageIndex*SubscriberWorkChunkSize)
		if !found || len(page) != want {
			return nil, &PermanentApplyError{Err: fmt.Errorf("missing or invalid subscriber effect page %d", pageIndex)}
		}
		count := min(end-at, len(page)-at%SubscriberWorkChunkSize)
		effects = append(effects, page[at%SubscriberWorkChunkSize:at%SubscriberWorkChunkSize+count]...)
		at += count
	}
	return effects, nil
}

// Upgrade an existing local data directory once, before request/worker startup.
// Each legacy value and all its pages change atomically; interrupted upgrades
// can be resumed without losing either progress or the unfinished effects.
func (wk *wukongDB) migrateSubscriberWorkPages() error {
	for _, db := range wk.dbs {
		if err := func() error {
			it := db.NewIter(&pebble.IterOptions{LowerBound: recoveryKey(recoveryPending), UpperBound: recoveryKey(recoveryPending + 1)})
			defer it.Close()
			for it.First(); it.Valid(); it.Next() {
				var w SubscriberWork
				if err := json.Unmarshal(it.Value(), &w); err != nil {
					return &PermanentApplyError{Err: err}
				}
				if w.Paged {
					continue
				}
				if !bytes.Equal(it.Key(), recoveryPendingKey(w.SlotID, w.Version)) || w.Next < 0 || w.Next > len(w.Effects) {
					return &PermanentApplyError{Err: errors.New("invalid legacy subscriber work")}
				}
				batch := db.NewBatch()
				err := stageSubscriberWork(batch, w)
				if err == nil {
					err = batch.Commit(wk.sync)
				}
				batch.Close()
				if err != nil {
					return err
				}
			}
			return it.Error()
		}(); err != nil {
			return err
		}
	}
	return nil
}
