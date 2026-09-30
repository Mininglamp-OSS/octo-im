package wkdb

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

// Shared wire bound for request-path and background proposals.
const MaxConversationEffects = 64

// Caller holds the recovery user lock. Deleting a recent-conversation row is
// independent of leaving the channel: a later read-position update may restore
// an active lifecycle, but must never revive a membership tombstone.
func (wk *wukongDB) restoreManagedConversation(uid, channel string, tp uint8, seq uint64) error {
	if !wk.SubscriberRecoveryActive() {
		return nil
	}
	effect, found, err := wk.ConversationLifecycle(uid, channel, tp)
	if err != nil || !found || effect.Deleted {
		return err
	}
	at := time.Unix(0, effect.CreatedAt)
	conversation := Conversation{Id: effect.ConversationID, Uid: uid, ChannelId: channel, ChannelType: tp, Type: ConversationTypeChat, ReadToMsgSeq: max(seq, effect.ReadToMsgSeq), CreatedAt: &at, UpdatedAt: &at}
	batch := wk.sharedBatchDB(uid).NewBatch()
	if err := wk.writeConversation(conversation, batch); err != nil {
		return err
	}
	// Publish the relation first. If the row commit fails, a Raft retry still
	// sees a missing row and repeats both writes, repairing the crash gap.
	if err := wk.setConversationLocalUserRelation([]Conversation{conversation}, true); err != nil {
		return err
	}
	if err := batch.CommitWait(); err != nil {
		return err
	}
	wk.conversationCache.UpdateConversationsInCache([]Conversation{conversation})
	return nil
}

func (wk *wukongDB) SubscriberRecoveryActive() bool {
	return wk.recoveryActive.Load()
}

// loadSubscriberRecoveryActive preserves lifecycle fencing after a restart.
// Pausing workers retains versioned foreground writes, so queued effects cannot
// bypass membership changes performed while recovery is paused.
func (wk *wukongDB) loadSubscriberRecoveryActive() error {
	if wk.SubscriberRecoveryActive() {
		return nil
	}
	lower := recoveryKey(recoveryReceipt)
	upper := recoveryKey(recoveryApplied + 1)
	for _, db := range wk.dbs {
		iter := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		found := iter.First()
		err := iter.Error()
		closeErr := iter.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if found {
			wk.recoveryActive.Store(true)
			return nil
		}
	}
	return nil
}

// lockRecoveryUsers uses a fixed number of locks, acquired in the same order.
// Generic conversation writers and lifecycle application share these locks.
func (wk *wukongDB) lockRecoveryUsers(uids []string) func() {
	if !wk.SubscriberRecoveryActive() {
		return func() {}
	}
	var used [64]bool
	for _, uid := range uids {
		used[key.HashWithString(uid)%64] = true
	}
	for i, v := range used {
		if v {
			wk.recoveryUserLocks[i].Lock()
		}
	}
	return func() {
		for i := len(used) - 1; i >= 0; i-- {
			if used[i] {
				wk.recoveryUserLocks[i].Unlock()
			}
		}
	}
}

func (wk *wukongDB) lockRecoveryConversations(cs []Conversation) func() {
	uids := make([]string, len(cs))
	for i, c := range cs {
		uids[i] = c.Uid
	}
	return wk.lockRecoveryUsers(uids)
}

type lifecycleCacheEntry struct {
	effect  ConversationEffect
	found   bool
	version uint64
}

func (wk *wukongDB) ConversationLifecycle(uid, ch string, tp uint8) (ConversationEffect, bool, error) {
	k := recoveryKey(recoveryLifecycle, uid, ch, string([]byte{tp}))
	version := wk.lifecycleCacheVersion[key.HashWithString(uid)%64].Load()
	if cached, ok := wk.lifecycleCache.Get(string(k)); ok && cached.version == version {
		return cached.effect, cached.found, nil
	}
	var e ConversationEffect
	ok, err := recoveryRead(wk.shardDB(uid), k, &e)
	if err == nil {
		// Tag with the version captured BEFORE the read, so a concurrent commit
		// cannot be overwritten by a delayed cache fill (including a cached miss).
		wk.lifecycleCache.Add(string(k), lifecycleCacheEntry{e, ok, version})
	}
	return e, ok, err
}

// Fencing happens before legacy writers replace the incoming ID with the stored
// ID. A stale cache flush cannot resurrect a departed membership or overwrite a
// new join's read position. Unmanaged conversations retain their old behavior.
func (wk *wukongDB) filterRecoveryConversations(cs []Conversation) ([]Conversation, error) {
	if !wk.SubscriberRecoveryActive() {
		return cs, nil
	}
	filtered := make([]Conversation, 0, len(cs))
	var missingUID, deleted, mismatchedID int
	for _, c := range cs {
		if c.Uid == "" {
			missingUID++
			continue
		}
		e, ok, err := wk.ConversationLifecycle(c.Uid, c.ChannelId, c.ChannelType)
		if err != nil {
			return nil, err
		}
		if ok && (e.Deleted || c.Id != e.ConversationID) {
			if e.Deleted {
				deleted++
			} else {
				mismatchedID++
			}
			continue
		}
		if ok && c.ReadToMsgSeq < e.ReadToMsgSeq {
			c.ReadToMsgSeq = e.ReadToMsgSeq
		}
		filtered = append(filtered, c)
	}
	if missingUID+deleted+mismatchedID > 0 {
		// Keep old-generation writes fenced; rebinding an arbitrary ID here
		// could resurrect a stale read position. Make every filtered batch visible.
		wk.Warn("conversation writes rejected by lifecycle fence", zap.Int("missingUID", missingUID), zap.Int("deleted", deleted), zap.Int("mismatchedID", mismatchedID))
	}
	return filtered, nil
}

// ApplyConversationEffects is bounded by the wire page size. Each user row and
// lifecycle commit atomically. The reverse relation lives in another physical
// DB: RelationDone is a durable continuation, retried before acknowledging work.
func (wk *wukongDB) ApplyConversationEffects(effects []ConversationEffect) error {
	if len(effects) == 0 || len(effects) > MaxConversationEffects {
		return &PermanentApplyError{errors.New("invalid conversation effect page")}
	}
	for _, e := range effects {
		if e.UID == "" || e.ChannelID == "" || e.ChannelType == 0 || e.Version == 0 || (!e.Deleted && e.ConversationID == 0) || e.CreatedAt <= 0 {
			return &PermanentApplyError{errors.New("invalid conversation effect")}
		}
	}
	wk.recoveryActive.Store(true)
	seen := make(map[string]struct{}, len(effects))
	for _, e := range effects {
		identity := string(recoveryKey(recoveryLifecycle, e.UID, e.ChannelID, string([]byte{e.ChannelType})))
		if _, ok := seen[identity]; ok {
			// Production pages contain one effect per user. Preserve strict input
			// order for defensive callers which submit repeated lifecycle keys.
			for _, effect := range effects {
				if err := wk.applyConversationEffect(effect); err != nil {
					return err
				}
			}
			return nil
		}
		seen[identity] = struct{}{}
	}
	return wk.applyConversationEffectBatch(effects)
}

// applyConversationEffectBatch syncs the user rows, lifecycle and reverse
// relations together before writing RelationDone or acknowledging the target
// Raft entry. Different physical shards may finish in either order: the entry
// remains unapplied until all are durable, and replay repairs either prefix.
func (wk *wukongDB) applyConversationEffectBatch(effects []ConversationEffect) error {
	uids := make([]string, len(effects))
	for i := range effects {
		uids[i] = effects[i].UID
	}
	unlock := wk.lockRecoveryUsers(uids)
	defer unlock()

	type pendingEffect struct {
		effect ConversationEffect
		userDB uint32
	}
	pending := make([]pendingEffect, 0, len(effects))
	stageBatches := make(map[uint32]*pebble.Batch)
	stageRows := make(map[uint32][]ConversationEffect)
	defer closeRecoveryBatches(stageBatches)
	for _, effect := range effects {
		dbIndex := wk.shardId(effect.UID)
		db := wk.shardDBById(dbIndex)
		lifecycleKey := recoveryKey(recoveryLifecycle, effect.UID, effect.ChannelID, string([]byte{effect.ChannelType}))
		old, ok, err := wk.ConversationLifecycle(effect.UID, effect.ChannelID, effect.ChannelType)
		if err != nil {
			return err
		}
		if ok && old.Version > effect.Version {
			continue
		}
		if ok && old.Version == effect.Version {
			if old.Deleted != effect.Deleted || old.ConversationID != effect.ConversationID {
				return &PermanentApplyError{errors.New("conflicting conversation lifecycle")}
			}
			if old.RelationDone {
				continue
			}
			pending = append(pending, pendingEffect{effect: old, userDB: dbIndex})
			continue
		}

		staged := &Batch{}
		var retained Conversation
		if !ok && !effect.Deleted && effect.PreserveExisting {
			retained, err = wk.GetConversation(effect.UID, effect.ChannelID, effect.ChannelType)
			if err != nil && err != ErrNotFound {
				return err
			}
			if !IsEmptyConversation(retained) {
				effect.ReadToMsgSeq = retained.ReadToMsgSeq
			}
		}
		if !ok {
			if err := wk.deleteConversation(effect.UID, effect.ChannelID, effect.ChannelType, staged); err != nil {
				return err
			}
		} else if !old.Deleted {
			conversation, err := wk.GetConversation(effect.UID, effect.ChannelID, effect.ChannelType)
			if err != nil && err != ErrNotFound {
				return err
			}
			if !IsEmptyConversation(conversation) {
				if err := wk.deleteConversationIndex(conversation, staged); err != nil {
					return err
				}
				staged.DeleteRange(key.NewConversationColumnKey(effect.UID, conversation.Id, key.MinColumnKey), key.NewConversationColumnKey(effect.UID, conversation.Id, key.MaxColumnKey))
			}
		}
		if !effect.Deleted {
			at := time.Unix(0, effect.CreatedAt)
			conversation := Conversation{Id: effect.ConversationID, Uid: effect.UID, ChannelId: effect.ChannelID, ChannelType: effect.ChannelType, Type: ConversationTypeChat, ReadToMsgSeq: effect.ReadToMsgSeq, CreatedAt: &at, UpdatedAt: &at}
			if !IsEmptyConversation(retained) {
				conversation = retained
				conversation.Id = effect.ConversationID
			}
			if err := wk.writeLifecycleConversation(conversation, staged); err != nil {
				return err
			}
		}
		batch := stageBatches[dbIndex]
		if batch == nil {
			batch = db.NewBatch()
			stageBatches[dbIndex] = batch
		}
		if err := stageRecoveryBatch(db, batch, staged); err != nil {
			return err
		}
		effect.RelationDone = false
		if err := recoverySet(batch, lifecycleKey, effect); err != nil {
			return err
		}
		stageRows[dbIndex] = append(stageRows[dbIndex], effect)
		pending = append(pending, pendingEffect{effect: effect, userDB: dbIndex})
	}
	if len(pending) == 0 {
		return nil
	}

	for _, item := range pending {
		effect := item.effect
		dbIndex := wk.GetChannelShardIndex(effect.ChannelID, effect.ChannelType)
		batch := stageBatches[dbIndex]
		if batch == nil {
			batch = wk.shardDBById(dbIndex).NewBatch()
			stageBatches[dbIndex] = batch
		}
		relationKey := key.NewConversationLocalUserKey(effect.ChannelID, effect.ChannelType, effect.UID)
		var err error
		if effect.Deleted {
			err = batch.Delete(relationKey, wk.noSync)
		} else {
			err = batch.Set(relationKey, nil, wk.noSync)
		}
		if err != nil {
			return err
		}
	}
	// Same-shard rows and relations use one atomic batch. Cross-shard writes
	// share one wait-all barrier instead of two serial fsync rounds. On a
	// partial failure, RelationDone stays false and the target is not ACKed.
	if err := commitRecoveryBatches(stageBatches, wk.sync, func(shard uint32) {
		for _, effect := range stageRows[shard] {
			wk.lifecycleCacheVersion[key.HashWithString(effect.UID)%64].Add(1)
			wk.conversationCache.InvalidateUserConversations(effect.UID)
		}
	}); err != nil {
		return err
	}

	doneBatches := make(map[uint32]*pebble.Batch)
	doneRows := make(map[uint32][]ConversationEffect)
	defer closeRecoveryBatches(doneBatches)
	for _, item := range pending {
		effect := item.effect
		effect.RelationDone = true
		batch := doneBatches[item.userDB]
		if batch == nil {
			batch = wk.shardDBById(item.userDB).NewBatch()
			doneBatches[item.userDB] = batch
		}
		if err := recoverySet(batch, recoveryKey(recoveryLifecycle, effect.UID, effect.ChannelID, string([]byte{effect.ChannelType})), effect); err != nil {
			return err
		}
		doneRows[item.userDB] = append(doneRows[item.userDB], effect)
	}
	// The relation was synced before this marker. Losing this NoSync marker on
	// a crash only makes replay repeat the idempotent relation update.
	return commitRecoveryBatches(doneBatches, wk.noSync, func(shard uint32) {
		for _, effect := range doneRows[shard] {
			wk.lifecycleCacheVersion[key.HashWithString(effect.UID)%64].Add(1)
		}
	})
}

func closeRecoveryBatches(batches map[uint32]*pebble.Batch) {
	for _, batch := range batches {
		_ = batch.Close()
	}
}

func commitRecoveryBatches(batches map[uint32]*pebble.Batch, options *pebble.WriteOptions, committed func(uint32)) error {
	shards := make([]int, 0, len(batches))
	for shard := range batches {
		shards = append(shards, int(shard))
	}
	sort.Ints(shards)
	// A recovery page is bounded by MaxConversationEffects. Each physical
	// shard owns a separate batch; only independent shards in this phase
	// may commit concurrently. Later lifecycle phases still await every sync.
	errs := make([]error, len(shards))
	if options.Sync && len(shards) > 1 {
		var wg sync.WaitGroup
		for i, shard := range shards {
			wg.Add(1)
			go func(i, shard int) {
				defer wg.Done()
				errs[i] = batches[uint32(shard)].Commit(options)
			}(i, shard)
		}
		wg.Wait()
	} else {
		for i, shard := range shards {
			errs[i] = batches[uint32(shard)].Commit(options)
		}
	}
	// Invalidate every successfully committed shard even on partial failure.
	// All writes have finished before the caller may close the batches.
	var firstErr error
	for i, shard := range shards {
		if errs[i] != nil {
			if firstErr == nil {
				firstErr = errs[i]
			}
		} else if committed != nil {
			committed(uint32(shard))
		}
	}
	return firstErr
}

func (wk *wukongDB) applyConversationEffectsIndividually(effects []ConversationEffect) error {
	for _, e := range effects {
		if err := wk.applyConversationEffect(e); err != nil {
			return err
		}
	}
	return nil
}

func (wk *wukongDB) applyConversationEffect(e ConversationEffect) error {
	unlock := wk.lockRecoveryUsers([]string{e.UID})
	defer unlock()
	db := wk.shardDB(e.UID)
	lifecycleKey := recoveryKey(recoveryLifecycle, e.UID, e.ChannelID, string([]byte{e.ChannelType}))
	old, ok, err := wk.ConversationLifecycle(e.UID, e.ChannelID, e.ChannelType)
	if err != nil {
		return err
	}
	if ok && old.Version > e.Version {
		return nil
	}
	if ok && old.Version == e.Version {
		if old.Deleted != e.Deleted || old.ConversationID != e.ConversationID {
			return &PermanentApplyError{errors.New("conflicting conversation lifecycle")}
		}
		if old.RelationDone {
			return nil
		}
		e = old
	} else {
		staged := &Batch{}
		var retained Conversation
		if !ok && !e.Deleted && e.PreserveExisting {
			retained, err = wk.GetConversation(e.UID, e.ChannelID, e.ChannelType)
			if err != nil && err != ErrNotFound {
				return err
			}
			if !IsEmptyConversation(retained) {
				e.ReadToMsgSeq = retained.ReadToMsgSeq
			}
		}
		if !ok {
			// Upgrade cleanup includes duplicate rows left by the old implementation.
			if err := wk.deleteConversation(e.UID, e.ChannelID, e.ChannelType, staged); err != nil {
				return err
			}
		} else if !old.Deleted {
			c, err := wk.GetConversation(e.UID, e.ChannelID, e.ChannelType)
			if err != nil && err != ErrNotFound {
				return err
			}
			if !IsEmptyConversation(c) {
				if err := wk.deleteConversationIndex(c, staged); err != nil {
					return err
				}
				staged.DeleteRange(key.NewConversationColumnKey(e.UID, c.Id, key.MinColumnKey), key.NewConversationColumnKey(e.UID, c.Id, key.MaxColumnKey))
			}
		}
		if !e.Deleted {
			at := time.Unix(0, e.CreatedAt)
			c := Conversation{Id: e.ConversationID, Uid: e.UID, ChannelId: e.ChannelID, ChannelType: e.ChannelType, Type: ConversationTypeChat, ReadToMsgSeq: e.ReadToMsgSeq, CreatedAt: &at, UpdatedAt: &at}
			if !IsEmptyConversation(retained) {
				c = retained
				c.Id = e.ConversationID
			}
			if err := wk.writeLifecycleConversation(c, staged); err != nil {
				return err
			}
		}
		b := db.NewBatch()
		defer b.Close()
		if err := stageRecoveryBatch(db, b, staged); err != nil {
			return err
		}
		e.RelationDone = false
		if err := recoverySet(b, lifecycleKey, e); err != nil {
			return err
		}
		if err := b.Commit(wk.sync); err != nil {
			return err
		}
		wk.lifecycleCacheVersion[key.HashWithString(e.UID)%64].Add(1)
		wk.conversationCache.InvalidateUserConversations(e.UID)
	}
	relation := wk.channelDb(e.ChannelID, e.ChannelType).NewBatch()
	defer relation.Close()
	k := key.NewConversationLocalUserKey(e.ChannelID, e.ChannelType, e.UID)
	if e.Deleted {
		err = relation.Delete(k, wk.noSync)
	} else {
		err = relation.Set(k, nil, wk.noSync)
	}
	if err != nil {
		return err
	}
	if err = relation.Commit(wk.sync); err != nil {
		return err
	}
	e.RelationDone = true
	b := db.NewBatch()
	defer b.Close()
	if err := recoverySet(b, lifecycleKey, e); err != nil {
		return err
	}
	// The reverse relation was synced first. This marker may safely be NoSync:
	// after a crash, RelationDone=false simply replays the idempotent relation.
	if err := b.Commit(wk.noSync); err != nil {
		return err
	}
	wk.lifecycleCacheVersion[key.HashWithString(e.UID)%64].Add(1)
	return nil
}

// Adoption replaces the row identity. The ordinary writer intentionally leaves
// the deletion boundary untouched on updates, so copy it explicitly when
// reconstructing a retained row. Keep it in the same durable lifecycle batch.
func (wk *wukongDB) writeLifecycleConversation(c Conversation, batch *Batch) error {
	if err := wk.writeConversation(c, batch); err != nil {
		return err
	}
	if c.DeletedAtMsgSeq != 0 {
		value := make([]byte, 8)
		wk.endian.PutUint64(value, c.DeletedAtMsgSeq)
		batch.Set(key.NewConversationColumnKey(c.Uid, c.Id, key.TableConversation.Column.DeletedAtMsgSeq), value)
	}
	return nil
}
