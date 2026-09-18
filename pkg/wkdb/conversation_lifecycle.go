package wkdb

import (
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
)

// lockRecoveryUsers uses a fixed number of locks, acquired in the same order.
// Generic conversation writers and lifecycle application share these locks.
func (wk *wukongDB) lockRecoveryUsers(uids []string) func() {
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

func (wk *wukongDB) ConversationLifecycle(uid, ch string, tp uint8) (ConversationEffect, bool, error) {
	var e ConversationEffect
	ok, err := recoveryRead(wk.shardDB(uid), recoveryKey(recoveryLifecycle, uid, ch, string([]byte{tp})), &e)
	return e, ok, err
}

// Fencing happens before legacy writers replace the incoming ID with the stored
// ID. A stale cache flush cannot resurrect a departed membership or overwrite a
// new join's read position. Unmanaged conversations retain their old behavior.
func (wk *wukongDB) filterRecoveryConversations(cs []Conversation) ([]Conversation, error) {
	filtered := make([]Conversation, 0, len(cs))
	for _, c := range cs {
		if c.Uid == "" {
			continue
		}
		e, ok, err := wk.ConversationLifecycle(c.Uid, c.ChannelId, c.ChannelType)
		if err != nil {
			return nil, err
		}
		if ok && (e.Deleted || c.Id != e.ConversationID) {
			continue
		}
		if ok && c.ReadToMsgSeq < e.ReadToMsgSeq {
			c.ReadToMsgSeq = e.ReadToMsgSeq
		}
		filtered = append(filtered, c)
	}
	return filtered, nil
}

// ApplyConversationEffects is bounded by the wire page size. Each user row and
// lifecycle commit atomically. The reverse relation lives in another physical
// DB: RelationDone is a durable continuation, retried before acknowledging work.
func (wk *wukongDB) ApplyConversationEffects(effects []ConversationEffect) error {
	if len(effects) == 0 || len(effects) > 16 {
		return errors.New("invalid conversation effect page")
	}
	for _, e := range effects {
		if e.UID == "" || e.ChannelID == "" || e.ChannelType == 0 || e.Version == 0 || (!e.Deleted && e.ConversationID == 0) || e.CreatedAt <= 0 {
			return errors.New("invalid conversation effect")
		}
	}
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
			return errors.New("conflicting conversation lifecycle")
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
			if err := wk.writeConversation(c, staged); err != nil {
				return err
			}
		}
		b := db.NewBatch()
		defer b.Close()
		if err := stageRecoveryBatch(b, staged); err != nil {
			return err
		}
		e.RelationDone = false
		if err := recoverySet(b, lifecycleKey, e); err != nil {
			return err
		}
		if err := b.Commit(wk.sync); err != nil {
			return err
		}
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
	return b.Commit(wk.sync)
}
