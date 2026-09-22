package wkdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
)

const (
	// Bound the wire payload, not the size of an existing channel.
	MaxSubscriberOperationMembers = MaxSubscriberOperationBytes / 3
	MaxSubscriberOperationBytes   = 1 << 20
	// Dedicated namespace; these records must survive normal channel deletion.
	subscriberRecoveryTable byte = 0x30
	recoveryReceipt         byte = 1
	recoveryPending         byte = 2
	recoveryIntent          byte = 3
	recoveryLifecycle       byte = 4
	recoveryCounter         byte = 5
	recoveryApplied         byte = 6
	recoveryReceiptExpiry   byte = 9
)

// Completed/rejected operation IDs have a seven-day idempotency window.
// Pending work and lifecycle fences are never expired by this policy.
const SubscriberReceiptRetention = 7 * 24 * time.Hour

// SubscriberOperation is an immutable request. IDs and timestamps are selected
// before replication; retries reuse OperationID, not a newly generated intent.
type SubscriberOperation struct {
	RestoreConversationIDs []uint64     `json:"restore_conversation_ids,omitempty"`
	OperationID            string       `json:"operation_id"`
	ChannelID              string       `json:"channel_id"`
	ChannelType            uint8        `json:"channel_type"`
	Mode                   string       `json:"mode"`
	UIDs                   []string     `json:"uids"`
	ConversationIDs        []uint64     `json:"conversation_ids,omitempty"`
	Channel                *ChannelInfo `json:"channel,omitempty"`
	ReadToMsgSeq           uint64       `json:"read_to_msg_seq"`
	CreatedAt              int64        `json:"created_at"`
	// MaxPending is a replicated admission limit per source slot/DB partition.
	MaxPending uint64 `json:"max_pending"`
}

func (o SubscriberOperation) Validate() error {
	if o.OperationID == "" || len(o.OperationID) > 128 || o.ChannelID == "" || len(o.ChannelID) > 1024 || o.ChannelType == 0 || o.CreatedAt <= 0 || o.MaxPending == 0 || len(o.UIDs) > MaxSubscriberOperationMembers {
		return errors.New("invalid subscriber operation")
	}
	switch o.Mode {
	case "add", "remove", "reset", "remove_all", "disband", "deny_add", "deny_set", "deny_remove", "deny_remove_all":
	default:
		return errors.New("invalid subscriber operation mode")
	}
	if len(o.RestoreConversationIDs) > MaxSubscriberOperationMembers {
		return errors.New("too many restore IDs")
	}
	for _, id := range o.RestoreConversationIDs {
		if id == 0 {
			return errors.New("invalid restore ID")
		}
	}
	if len(o.UIDs) != len(o.ConversationIDs) {
		return errors.New("subscriber operation IDs are not aligned")
	}
	for i, uid := range o.UIDs {
		if strings.TrimSpace(uid) == "" || len(uid) > 1024 || (i > 0 && o.UIDs[i-1] >= uid) || o.ConversationIDs[i] == 0 {
			return errors.New("subscriber operation members must be canonical and have IDs")
		}
	}
	if o.Channel != nil && (o.Channel.ChannelId != o.ChannelID || o.Channel.ChannelType != o.ChannelType) {
		return errors.New("subscriber channel identity mismatch")
	}
	b, err := json.Marshal(o)
	if err != nil {
		return err
	}
	if len(b) > MaxSubscriberOperationBytes {
		return errors.New("subscriber operation too large")
	}
	return nil
}

// Digest excludes server-selected timestamps, IDs, tail and admission budget.
// A retried client request must have identical business input.
func (o SubscriberOperation) Digest() string {
	o.OperationID = ""
	o.ConversationIDs = nil
	o.RestoreConversationIDs = nil
	o.ReadToMsgSeq = 0
	o.CreatedAt = 0
	o.MaxPending = 0
	if o.Channel != nil {
		c := *o.Channel
		c.Id = 0
		c.CreatedAt = nil
		c.UpdatedAt = nil
		o.Channel = &c
	}
	b, _ := json.Marshal(o)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// SubscriberReceipt remains after completion so an old HTTP retry cannot remove
// members who rejoined later. Pending/rejected are never reported as completion.
type SubscriberReceipt struct {
	Attempts    uint32 `json:"attempts"`
	LastError   string `json:"last_error,omitempty"`
	NextAttempt int64  `json:"next_attempt,omitempty"`
	OperationID string `json:"operation_id"`
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	Digest      string `json:"-"`
	Version     uint64 `json:"version"`
	State       string `json:"state"`
	Error       string `json:"error,omitempty"`
	Total       int    `json:"total"`
	Completed   int    `json:"completed"`
	CreatedAt   int64  `json:"created_at"`
	CompletedAt int64  `json:"completed_at,omitempty"`
}

// ConversationEffect carries the source lifecycle, not a scheduling attempt ID.
type ConversationEffect struct {
	PreserveExisting bool   `json:"preserve_existing,omitempty"`
	UID              string `json:"uid"`
	ChannelID        string `json:"channel_id"`
	ChannelType      uint8  `json:"channel_type"`
	Version          uint64 `json:"version"`
	ConversationID   uint64 `json:"conversation_id"`
	Deleted          bool   `json:"deleted"`
	ReadToMsgSeq     uint64 `json:"read_to_msg_seq"`
	CreatedAt        int64  `json:"created_at"`
	RelationDone     bool   `json:"relation_done,omitempty"`
}

// SubscriberWork is source-owned and durably checkpointed after each bounded page.
type SubscriberWork struct {
	Paged       bool                 `json:"paged,omitempty"`
	Total       int                  `json:"total"`
	LastRetryAt int64                `json:"last_retry_at,omitempty"`
	Attempts    uint32               `json:"attempts"`
	NextAttempt int64                `json:"next_attempt,omitempty"`
	SlotID      uint32               `json:"slot_id"`
	Operation   SubscriberOperation  `json:"operation"`
	Version     uint64               `json:"version"`
	Effects     []ConversationEffect `json:"effects,omitempty"` // legacy on-disk format only
	Next        int                  `json:"next"`
}

type SubscriberCheckpoint struct {
	RetryAt     int64  `json:"retry_at,omitempty"`
	Error       string `json:"error,omitempty"`
	SlotID      uint32 `json:"slot_id"`
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	OperationID string `json:"operation_id"`
	Version     uint64 `json:"version"`
	Previous    int    `json:"previous"`
	Next        int    `json:"next"`
	Done        bool   `json:"done"`
	At          int64  `json:"at"`
}

// SubscriberRecoveryCommand is one source-slot recovery mutation in Raft log
// order. Batching these commands only overlaps their WAL sync waits; every
// command remains an independent atomic Pebble batch and is visible to the
// command which follows it.
type SubscriberRecoveryCommand struct {
	SlotID     uint32
	Version    uint64
	Operation  *SubscriberOperation
	Checkpoint *SubscriberCheckpoint
}

type SubscriberRecoveryDB interface {
	ApplySubscriberOperation(uint32, uint64, SubscriberOperation) error
	ApplySubscriberRecoveryCommands([]SubscriberRecoveryCommand) error
	GetSubscriberReceipt(string, uint8, string) (SubscriberReceipt, bool, error)
	GetSubscriberWork(string, uint8, uint32, uint64) (SubscriberWork, bool, error)
	GetSubscriberWorkPage(SubscriberWork, int) ([]ConversationEffect, error)
	GetSubscriberIntent(string, uint8, string) (ConversationEffect, bool, error)
	ListSubscriberWork(int, []byte, int) ([]SubscriberWork, []byte, bool, error)
	CheckpointSubscriberWork(SubscriberCheckpoint) error
	ApplyConversationEffects([]ConversationEffect) error
	ConversationLifecycle(string, string, uint8) (ConversationEffect, bool, error)
	SubscriberBacklog() ([]SubscriberBacklogPartition, error)
}

func (wk *wukongDB) GetSubscriberWork(channelID string, channelType uint8, slotID uint32, version uint64) (SubscriberWork, bool, error) {
	var work SubscriberWork
	ok, err := recoveryRead(wk.channelDb(channelID, channelType), recoveryPendingKey(slotID, version), &work)
	if !work.Paged {
		work.Total = len(work.Effects)
	}
	return work, ok, err
}

func (wk *wukongDB) GetSubscriberIntent(channelID string, channelType uint8, uid string) (ConversationEffect, bool, error) {
	var effect ConversationEffect
	ok, err := recoveryRead(wk.channelDb(channelID, channelType), recoveryChannelKey(recoveryIntent, channelID, channelType, uid), &effect)
	return effect, ok, err
}

func recoveryKey(kind byte, parts ...string) []byte {
	b := []byte{subscriberRecoveryTable, 1, kind}
	for _, p := range parts {
		b = binary.BigEndian.AppendUint32(b, uint32(len(p)))
		b = append(b, p...)
	}
	return b
}
func recoveryChannelKey(kind byte, channelID string, tp uint8, suffix string) []byte {
	return recoveryKey(kind, channelID, string([]byte{tp}), suffix)
}
func recoveryPendingKey(slot uint32, version uint64) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint32(recoveryKey(recoveryPending), slot), version)
}
func recoverySlotKey(kind byte, slot uint32) []byte {
	return binary.BigEndian.AppendUint32(recoveryKey(kind), slot)
}
func recoveryRead(db *pebble.DB, k []byte, out any) (bool, error) {
	b, c, e := db.Get(k)
	if errors.Is(e, pebble.ErrNotFound) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	defer c.Close()
	if e := json.Unmarshal(b, out); e != nil {
		return true, &PermanentApplyError{fmt.Errorf("decode persisted recovery record: %w", e)}
	}
	return true, nil
}
func recoverySet(w pebble.Writer, k []byte, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return w.Set(k, b, pebble.NoSync)
}

// stageRecoveryBatch preserves the already reduced delete-before-insert order.
// Callers hold the source channel or target user lock. These ranges contain one
// small record, so enumerate its actual columns (including unknown columns)
// rather than creating thousands of range tombstones during membership churn.
// It is never used to regroup an arbitrary sequence of Raft commands.
func stageRecoveryBatch(db *pebble.DB, dst *pebble.Batch, src *Batch) error {
	for _, kv := range src.delKvs {
		if e := dst.Delete(kv.key, pebble.NoSync); e != nil {
			return e
		}
	}
	for _, kv := range src.delRangeKvs {
		it := db.NewIter(&pebble.IterOptions{LowerBound: kv.key, UpperBound: kv.val})
		for it.First(); it.Valid(); it.Next() {
			if err := dst.Delete(it.Key(), pebble.NoSync); err != nil {
				it.Close()
				return err
			}
		}
		err := it.Error()
		closeErr := it.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	for _, kv := range src.setKvs {
		if e := dst.Set(kv.key, kv.val, pebble.NoSync); e != nil {
			return e
		}
	}
	return nil
}

func (wk *wukongDB) GetSubscriberReceipt(ch string, tp uint8, id string) (SubscriberReceipt, bool, error) {
	var stored struct {
		SubscriberReceipt
		RequestDigest string `json:"request_digest"`
	}
	ok, e := recoveryRead(wk.channelDb(ch, tp), recoveryChannelKey(recoveryReceipt, ch, tp, id), &stored)
	stored.Digest = stored.RequestDigest
	return stored.SubscriberReceipt, ok, e
}
func storeSubscriberReceipt(w pebble.Writer, slot uint32, r SubscriberReceipt) error {
	if r.State == "complete" || r.State == "rejected" {
		at := r.CompletedAt
		if at == 0 {
			at = r.CreatedAt
		}
		k := binary.BigEndian.AppendUint64(recoverySlotKey(recoveryReceiptExpiry, slot), uint64(at))
		k = binary.BigEndian.AppendUint64(k, r.Version)
		if err := recoverySet(w, k, r); err != nil {
			return err
		}
	}
	return recoverySet(w, recoveryChannelKey(recoveryReceipt, r.ChannelID, r.ChannelType, r.OperationID), struct {
		SubscriberReceipt
		RequestDigest string `json:"request_digest"`
	}{r, r.Digest})
}

// ApplySubscriberOperation commits the source mutation, lifecycle intents,
// receipt and pending work atomically. Expected rejections are durable results,
// not Apply errors that would block replay of committed Raft logs.
func (wk *wukongDB) ApplySubscriberOperation(slot uint32, version uint64, o SubscriberOperation) error {
	return wk.applySubscriberOperation(slot, version, o, nil)
}

type subscriberRecoverySyncGroup struct {
	wk      *wukongDB
	batches []*pebble.Batch
}

func (g *subscriberRecoverySyncGroup) commit(db *pebble.DB, batch *pebble.Batch) error {
	if err := db.ApplyNoSyncWait(batch, g.wk.sync); err != nil {
		return err
	}
	g.batches = append(g.batches, batch)
	return nil
}

// finish waits for every durability acknowledgement even after a later
// command failed. A WAL sync failure takes precedence: the caller must retry
// instead of treating an earlier visible-but-not-durable prefix as permanent.
func (g *subscriberRecoverySyncGroup) finish(commandErr error) error {
	var syncErr error
	for _, batch := range g.batches {
		if err := batch.SyncWait(); err != nil && syncErr == nil {
			syncErr = err
		}
		_ = batch.Close()
	}
	if syncErr != nil {
		return syncErr
	}
	return commandErr
}

// ApplySubscriberRecoveryCommands preserves Raft order while allowing Pebble's
// WAL writer to group fsyncs from adjacent source operations/checkpoints.
func (wk *wukongDB) ApplySubscriberRecoveryCommands(commands []SubscriberRecoveryCommand) error {
	group := &subscriberRecoverySyncGroup{wk: wk}
	var commandErr error
	for _, command := range commands {
		switch {
		case command.Operation != nil && command.Checkpoint == nil:
			commandErr = wk.applySubscriberOperation(command.SlotID, command.Version, *command.Operation, group)
		case command.Operation == nil && command.Checkpoint != nil:
			commandErr = wk.checkpointSubscriberWork(*command.Checkpoint, group)
		default:
			commandErr = &PermanentApplyError{errors.New("invalid subscriber recovery command")}
		}
		if commandErr != nil {
			break
		}
	}
	return group.finish(commandErr)
}

func (wk *wukongDB) applySubscriberOperation(slot uint32, version uint64, o SubscriberOperation, group *subscriberRecoverySyncGroup) error {
	if e := o.Validate(); e != nil {
		return &PermanentApplyError{e}
	}
	if version == 0 {
		return &PermanentApplyError{errors.New("zero subscriber operation version")}
	}
	wk.recoveryActive.Store(true)
	partitionLock := &wk.subscriberRecoveryMu[wk.GetChannelShardIndex(o.ChannelID, o.ChannelType)]
	partitionLock.Lock()
	defer partitionLock.Unlock()
	channelLock := &wk.recoveryChannelLocks[key.ChannelToNum(o.ChannelID, o.ChannelType)%64]
	channelLock.Lock()
	defer channelLock.Unlock()
	db := wk.channelDb(o.ChannelID, o.ChannelType)
	var applied uint64
	if _, e := recoveryRead(db, recoverySlotKey(recoveryApplied, slot), &applied); e != nil {
		return e
	}
	if version <= applied {
		return nil
	}
	batch := db.NewIndexedBatch()
	groupOwnsBatch := false
	defer func() {
		if !groupOwnsBatch {
			_ = batch.Close()
		}
	}()
	if e := wk.pruneSubscriberReceipts(db, batch, slot, o.CreatedAt); e != nil {
		return e
	}
	finish := func() error {
		if e := recoverySet(batch, recoverySlotKey(recoveryApplied, slot), version); e != nil {
			return e
		}
		if group == nil {
			return batch.Commit(wk.sync)
		}
		if e := group.commit(db, batch); e != nil {
			return e
		}
		groupOwnsBatch = true
		return nil
	}
	old, found, e := wk.GetSubscriberReceipt(o.ChannelID, o.ChannelType, o.OperationID)
	if e != nil {
		return e
	}
	if found && (old.State != "rejected" || (old.Error != "backlog_full" && old.Error != "restore_set_changed") || old.Digest != o.Digest()) {
		return finish()
	}
	r := SubscriberReceipt{OperationID: o.OperationID, ChannelID: o.ChannelID, ChannelType: o.ChannelType, Digest: o.Digest(), Version: version, State: "pending", CreatedAt: o.CreatedAt}
	reject := func(reason string) error {
		r.State = "rejected"
		r.Error = reason
		if e := storeSubscriberReceipt(batch, slot, r); e != nil {
			return e
		}
		return finish()
	}
	current, e := wk.recoveryChannel(o.ChannelID, o.ChannelType)
	if e != nil {
		return e
	}
	if IsEmptyChannelInfo(current) && o.Channel == nil && o.Mode != "add" && o.Mode != "reset" {
		// Cleanup of a missing channel was a successful no-op before recovery.
		// Persist that outcome too, so a lost reply cannot later mutate a newly
		// created channel when the client retries the same operation ID.
		r.State = "complete"
		r.CompletedAt = o.CreatedAt
		if e := storeSubscriberReceipt(batch, slot, r); e != nil {
			return e
		}
		return finish()
	}
	removes := []string(nil)
	adds := []string(nil)
	switch o.Mode {
	case "add":
		adds = o.UIDs
	case "remove":
		removes = o.UIDs
	case "reset", "remove_all":
		members, e := wk.recoverySubscribers(o.ChannelID, o.ChannelType)
		if e != nil {
			return e
		}
		for _, m := range members {
			removes = append(removes, m.Uid)
		}
		if o.Mode == "reset" {
			adds = o.UIDs
		}
	case "deny_add", "deny_set":
		removes = o.UIDs
	}
	// Reduce reset to final membership: retained users keep their original epoch.
	addSet := make(map[string]uint64, len(adds))
	for i, uid := range o.UIDs {
		addSet[uid] = o.ConversationIDs[i]
	}
	if o.Mode != "add" && o.Mode != "reset" {
		clear(addSet)
	}
	// Unblocking a still-subscribed member creates a fresh lifecycle. Otherwise
	// a denylist tombstone would suppress that member's future cache flushes forever.
	if o.Mode == "deny_set" || o.Mode == "deny_remove_all" || o.Mode == "deny_remove" {
		var denied []Member
		if o.Mode == "deny_remove" {
			denied, e = wk.getDenylistByUids(o.ChannelID, o.ChannelType, o.UIDs)
		} else {
			denied, e = wk.recoveryDenylist(o.ChannelID, o.ChannelType)
		}
		if e != nil {
			return e
		}
		newDeny := make(map[string]bool)
		if o.Mode == "deny_set" {
			for _, uid := range o.UIDs {
				newDeny[uid] = true
			}
		}
		inputIDs := make(map[string]uint64, len(o.UIDs))
		for i, uid := range o.UIDs {
			inputIDs[uid] = o.ConversationIDs[i]
		}
		sort.Slice(denied, func(i, j int) bool { return denied[i].Uid < denied[j].Uid })
		restoreIndex := 0
		for _, m := range denied {
			if newDeny[m.Uid] {
				continue
			}
			members, e := wk.getSubscribersByUids(o.ChannelID, o.ChannelType, []string{m.Uid})
			if e != nil {
				return e
			}
			if len(members) == 0 {
				continue
			}
			id := inputIDs[m.Uid]
			if id == 0 {
				if restoreIndex >= len(o.RestoreConversationIDs) {
					return reject("restore_set_changed")
				}
				id = o.RestoreConversationIDs[restoreIndex]
				restoreIndex++
			}
			addSet[m.Uid] = id
		}
	}
	removeSet := make(map[string]bool, len(removes))
	for _, uid := range removes {
		if _, ok := addSet[uid]; !ok {
			removeSet[uid] = true
		}
	}
	uids := make([]string, 0, len(removeSet)+len(addSet))
	for uid := range removeSet {
		uids = append(uids, uid)
	}
	for uid := range addSet {
		uids = append(uids, uid)
	}
	sort.Strings(uids)
	for _, uid := range uids {
		if uid == "" || len(uid) > 1024 {
			return reject("invalid_existing_member")
		}
	}
	var pending uint64
	if _, e := recoveryRead(db, recoverySlotKey(recoveryCounter, slot), &pending); e != nil {
		return e
	}
	weight := uint64(len(uids) + 1) // also charge operations with only tag work
	if o.ChannelType == wkproto.ChannelTypeLive {
		weight = 1
	}
	// An otherwise idle partition admits one oversized operation. Its durable
	// tail applies backpressure until drained; maxPending is not a group-size limit.
	if pending > 0 && (pending >= o.MaxPending || weight > o.MaxPending-pending) {
		return reject("backlog_full")
	}
	work := SubscriberWork{SlotID: slot, Operation: o, Version: version}
	at := time.Unix(0, o.CreatedAt)
	staged := &Batch{}
	for _, uid := range uids {
		var intent ConversationEffect
		ok, e := recoveryRead(db, recoveryChannelKey(recoveryIntent, o.ChannelID, o.ChannelType, uid), &intent)
		if e != nil {
			return e
		}
		deleted := removeSet[uid]
		members, e := wk.getSubscribersByUids(o.ChannelID, o.ChannelType, []string{uid})
		if e != nil {
			return e
		}
		if !ok || intent.Deleted != deleted {
			intent = ConversationEffect{PreserveExisting: !ok && !deleted && len(members) > 0, UID: uid, ChannelID: o.ChannelID, ChannelType: o.ChannelType, Version: version, ConversationID: addSet[uid], Deleted: deleted, ReadToMsgSeq: o.ReadToMsgSeq, CreatedAt: o.CreatedAt}
		}
		if e := recoverySet(batch, recoveryChannelKey(recoveryIntent, o.ChannelID, o.ChannelType, uid), intent); e != nil {
			return e
		}
		if o.ChannelType != wkproto.ChannelTypeLive {
			work.Effects = append(work.Effects, intent)
		} // live channels do not have recent conversations
		if o.Mode == "deny_add" || o.Mode == "deny_set" || o.Mode == "deny_remove" || o.Mode == "deny_remove_all" {
			continue
		}
		if deleted {
			for _, m := range members {
				if e := wk.removeSubscriber(o.ChannelID, o.ChannelType, m, staged); e != nil {
					return e
				}
			}
		} else {
			if len(members) > 0 {
				continue
			}
			if e := wk.writeSubscriber(o.ChannelID, o.ChannelType, Member{Id: key.HashWithString(uid), Uid: uid, CreatedAt: &at, UpdatedAt: &at}, staged); e != nil {
				return e
			}
		}
	}
	if e := stageRecoveryBatch(db, batch, staged); e != nil {
		return e
	}
	if o.Mode == "deny_add" || o.Mode == "deny_set" || o.Mode == "deny_remove" || o.Mode == "deny_remove_all" {
		if e := wk.stageRecoveryDenylist(batch, o, at); e != nil {
			return e
		}
	}
	if o.Channel != nil || IsEmptyChannelInfo(current) || o.Mode == "disband" {
		info := current
		if IsEmptyChannelInfo(info) {
			info = ChannelInfo{ChannelId: o.ChannelID, ChannelType: o.ChannelType, CreatedAt: &at, UpdatedAt: &at}
		}
		if o.Channel != nil {
			info = *o.Channel
		}
		if o.Mode == "disband" {
			info.Disband = true
			info.UpdatedAt = &at
		}
		if !IsEmptyChannelInfo(current) {
			if e := wk.deleteChannelInfoBaseIndex(current, batch); e != nil {
				return e
			}
			info.CreatedAt = current.CreatedAt
		}
		pk, e := wk.getChannelPrimaryKey(o.ChannelID, o.ChannelType)
		if e != nil {
			return e
		}
		if e := wk.writeChannelInfo(pk, info, batch); e != nil {
			return e
		}
	}
	r.Total = len(work.Effects)
	if e := storeSubscriberReceipt(batch, slot, r); e != nil {
		return e
	}
	if e := stageSubscriberWork(batch, work); e != nil {
		return e
	}
	// Charge exactly the persisted effect count plus one finalization unit.
	if e := recoverySet(batch, recoverySlotKey(recoveryCounter, slot), pending+uint64(r.Total+1)); e != nil {
		return e
	}
	if e := finish(); e != nil {
		return e
	}
	wk.permissionCache.InvalidateChannelByType(PermissionTypeSubscriber, o.ChannelID, o.ChannelType)
	wk.permissionCache.InvalidateChannelByType(PermissionTypeDenylist, o.ChannelID, o.ChannelType)
	wk.channelInfoCache.InvalidateChannelInfo(o.ChannelID, o.ChannelType)
	return nil
}

// Capture previous members before the atomic source mutation. Target work is paged.
func (wk *wukongDB) recoverySubscribers(ch string, tp uint8) ([]Member, error) {
	iter := wk.channelDb(ch, tp).NewIter(&pebble.IterOptions{LowerBound: key.NewSubscriberColumnKey(ch, tp, 0, key.MinColumnKey), UpperBound: key.NewSubscriberColumnKey(ch, tp, ^uint64(0), key.MaxColumnKey)})
	defer iter.Close()
	var result []Member
	e := wk.iterateSubscriber(iter, func(m Member) bool { result = append(result, m); return true })
	if e != nil {
		return nil, e
	}
	return result, iter.Error()
}

func (wk *wukongDB) recoveryDenylist(ch string, tp uint8) ([]Member, error) {
	iter := wk.channelDb(ch, tp).NewIter(&pebble.IterOptions{LowerBound: key.NewDenylistPrimaryKey(ch, tp, 0), UpperBound: key.NewDenylistPrimaryKey(ch, tp, ^uint64(0))})
	defer iter.Close()
	var result []Member
	err := wk.iterateDenylist(iter, func(m Member) bool { result = append(result, m); return true })
	if err != nil {
		return nil, err
	}
	return result, iter.Error()
}

func (wk *wukongDB) stageRecoveryDenylist(batch *pebble.Batch, o SubscriberOperation, at time.Time) error {
	pk, err := wk.getChannelPrimaryKey(o.ChannelID, o.ChannelType)
	if err != nil {
		return err
	}
	existing, e := wk.getDenylistByUids(o.ChannelID, o.ChannelType, o.UIDs)
	if e != nil {
		return e
	}
	replacing := o.Mode == "deny_set" || o.Mode == "deny_remove_all"
	if replacing {
		if e := batch.DeleteRange(key.NewDenylistPrimaryKey(o.ChannelID, o.ChannelType, 0), key.NewDenylistPrimaryKey(o.ChannelID, o.ChannelType, ^uint64(0)), wk.noSync); e != nil {
			return e
		}
		if e := wk.deleteAllDenylistIndex(o.ChannelID, o.ChannelType, batch); e != nil {
			return e
		}
		if e := wk.incChannelInfoDenylistCount(pk, 0, batch); e != nil {
			return e
		}
	}
	switch o.Mode {
	case "deny_add", "deny_set":
		found := make(map[string]bool, len(existing))
		if !replacing {
			for _, m := range existing {
				found[m.Uid] = true
			}
		}
		count := 0
		for _, uid := range o.UIDs {
			if found[uid] {
				continue
			}
			count++
			if e := wk.writeDenylist(o.ChannelID, o.ChannelType, Member{Id: key.HashWithString(uid), Uid: uid, CreatedAt: &at, UpdatedAt: &at}, batch); e != nil {
				return e
			}
		}
		if count > 0 {
			return wk.incChannelInfoDenylistCount(pk, count, batch)
		}
	case "deny_remove":
		for _, m := range existing {
			if e := wk.removeDenylist(o.ChannelID, o.ChannelType, m, batch); e != nil {
				return e
			}
		}
		if len(existing) > 0 {
			return wk.incChannelInfoDenylistCount(pk, -len(existing), batch)
		}
	}
	return nil
}

func (wk *wukongDB) ListSubscriberWork(shard int, after []byte, limit int) ([]SubscriberWork, []byte, bool, error) {
	if shard < 0 || shard >= len(wk.dbs) || limit < 1 || limit > 64 {
		return nil, nil, false, errors.New("invalid recovery scan")
	}
	prefix := recoveryKey(recoveryPending)
	iter := wk.dbs[shard].NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: recoveryKey(recoveryPending + 1)})
	defer iter.Close()
	valid := iter.First()
	if len(after) > 0 {
		valid = iter.SeekGE(after)
		if valid && bytes.Equal(iter.Key(), after) {
			valid = iter.Next()
		}
	}
	var result []SubscriberWork
	var next []byte
	for valid && len(result) < limit {
		var w SubscriberWork
		if e := json.Unmarshal(iter.Value(), &w); e != nil {
			return nil, nil, false, &PermanentApplyError{Err: e}
		}
		if !w.Paged {
			w.Total = len(w.Effects)
		}
		result = append(result, w)
		next = bytes.Clone(iter.Key())
		valid = iter.Next()
	}
	return result, next, !valid, iter.Error()
}

// CheckpointSubscriberWork is a compare-and-set over the exact source progress.
func (wk *wukongDB) CheckpointSubscriberWork(c SubscriberCheckpoint) error {
	return wk.checkpointSubscriberWork(c, nil)
}

func (wk *wukongDB) checkpointSubscriberWork(c SubscriberCheckpoint, group *subscriberRecoverySyncGroup) error {
	partitionLock := &wk.subscriberRecoveryMu[wk.GetChannelShardIndex(c.ChannelID, c.ChannelType)]
	partitionLock.Lock()
	defer partitionLock.Unlock()
	db := wk.channelDb(c.ChannelID, c.ChannelType)
	var w SubscriberWork
	ok, e := recoveryRead(db, recoveryPendingKey(c.SlotID, c.Version), &w)
	if e != nil || !ok {
		return e
	}
	if !w.Paged {
		w.Total = len(w.Effects)
	}
	if w.Operation.OperationID != c.OperationID || w.Operation.ChannelID != c.ChannelID || w.Operation.ChannelType != c.ChannelType {
		return &PermanentApplyError{errors.New("recovery checkpoint identity mismatch")}
	}
	if c.RetryAt > 0 && c.RetryAt <= w.LastRetryAt {
		return nil
	}
	if c.Previous != w.Next {
		return nil
	}
	if c.Next < w.Next || c.Next > w.Total || (c.Done && c.Next != w.Total) {
		return &PermanentApplyError{errors.New("invalid recovery progress")}
	}
	r, found, e := wk.GetSubscriberReceipt(c.ChannelID, c.ChannelType, c.OperationID)
	if e != nil {
		return e
	}
	if !found {
		return &PermanentApplyError{errors.New("missing recovery receipt")}
	}
	var count uint64
	if _, e := recoveryRead(db, recoverySlotKey(recoveryCounter, c.SlotID), &count); e != nil {
		return e
	}
	batch := db.NewBatch()
	groupOwnsBatch := false
	defer func() {
		if !groupOwnsBatch {
			_ = batch.Close()
		}
	}()
	decrement := uint64(c.Next - w.Next)
	if w.Paged {
		// Remove only chunks whose final effect has been acknowledged. Partial
		// chunks stay immutable, including across retries and restarts.
		for page := w.Next / SubscriberWorkChunkSize; page < c.Next/SubscriberWorkChunkSize; page++ {
			if e := batch.Delete(recoveryEffectKey(c.SlotID, c.Version, page), wk.noSync); e != nil {
				return e
			}
		}
	}
	w.Next = c.Next
	r.Completed = c.Next
	if c.RetryAt > 0 {
		if c.Next != c.Previous || c.Done {
			return &PermanentApplyError{errors.New("retry must not advance recovery work")}
		}
		w.LastRetryAt = c.RetryAt
		w.Attempts++
		w.NextAttempt = c.RetryAt
		r.Attempts = w.Attempts
		r.NextAttempt = c.RetryAt
		r.LastError = c.Error
	} else {
		w.NextAttempt = 0
		r.NextAttempt = 0
		r.LastError = ""
	}
	if c.Done {
		decrement++
		r.State = "complete"
		r.CompletedAt = c.At
		e = batch.Delete(recoveryPendingKey(c.SlotID, c.Version), wk.noSync)
		if e == nil && w.Paged {
			prefix := recoveryEffectPrefix(c.SlotID, c.Version)
			e = batch.DeleteRange(prefix, append(bytes.Clone(prefix), 0xff), wk.noSync)
		}
	} else {
		e = recoverySet(batch, recoveryPendingKey(c.SlotID, c.Version), w)
	}
	if e != nil {
		return e
	}
	if decrement > count {
		return &PermanentApplyError{fmt.Errorf("recovery counter underflow: %d > %d", decrement, count)}
	}
	if e := recoverySet(batch, recoverySlotKey(recoveryCounter, c.SlotID), count-decrement); e != nil {
		return e
	}
	if e := storeSubscriberReceipt(batch, c.SlotID, r); e != nil {
		return e
	}
	if group == nil {
		return batch.Commit(wk.sync)
	}
	if e := group.commit(db, batch); e != nil {
		return e
	}
	groupOwnsBatch = true
	return nil
}

// Cleanup uses the replicated operation timestamp, never a follower's clock.
// Every admission removes at most 64 expired rows in this slot/partition. At
// steady traffic this bounds receipts by the retention window; idle partitions
// keep a finite tail until their next write. Old expiry indexes cannot delete a
// newer reuse of the same operation ID.
func (wk *wukongDB) pruneSubscriberReceipts(db *pebble.DB, batch *pebble.Batch, slot uint32, now int64) error {
	before := now - int64(SubscriberReceiptRetention)
	if before <= 0 {
		return nil
	}
	prefix := recoverySlotKey(recoveryReceiptExpiry, slot)
	upper := binary.BigEndian.AppendUint64(bytes.Clone(prefix), uint64(before))
	it := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	defer it.Close()
	for valid, n := it.First(), 0; valid && n < 64; valid, n = it.Next(), n+1 {
		var expired SubscriberReceipt
		if err := json.Unmarshal(it.Value(), &expired); err != nil {
			return &PermanentApplyError{fmt.Errorf("decode recovery expiry record: %w", err)}
		}
		current, found, err := wk.GetSubscriberReceipt(expired.ChannelID, expired.ChannelType, expired.OperationID)
		if err != nil {
			return err
		}
		if found && current.Version == expired.Version && current.State != "pending" {
			if err := batch.Delete(recoveryChannelKey(recoveryReceipt, expired.ChannelID, expired.ChannelType, expired.OperationID), wk.noSync); err != nil {
				return err
			}
		}
		if err := batch.Delete(it.Key(), wk.noSync); err != nil {
			return err
		}
	}
	return it.Error()
}

// SubscriberBacklogPartition counts outstanding effects plus one finalization
// unit per operation. Receipts and lifecycle tombstones are not pending work.
type SubscriberBacklogPartition struct {
	Shard        int    `json:"shard"`
	SlotID       uint32 `json:"slot_id"`
	PendingUnits uint64 `json:"pending_units"`
}

func (wk *wukongDB) SubscriberBacklog() ([]SubscriberBacklogPartition, error) {
	var result []SubscriberBacklogPartition
	for shard, db := range wk.dbs {
		err := func() error {
			it := db.NewIter(&pebble.IterOptions{LowerBound: recoveryKey(recoveryCounter), UpperBound: recoveryKey(recoveryCounter + 1)})
			defer it.Close()
			for it.First(); it.Valid(); it.Next() {
				if len(it.Key()) != 7 {
					return errors.New("invalid recovery counter key")
				}
				var n uint64
				if err := json.Unmarshal(it.Value(), &n); err != nil {
					return err
				}
				if n > 0 {
					result = append(result, SubscriberBacklogPartition{shard, binary.BigEndian.Uint32(it.Key()[3:]), n})
				}
			}
			return it.Error()
		}()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Replicated decisions must not depend on a concurrently populated read cache.
func (wk *wukongDB) recoveryChannel(ch string, tp uint8) (ChannelInfo, error) {
	id, _ := wk.getChannelPrimaryKey(ch, tp)
	it := wk.channelDb(ch, tp).NewIter(&pebble.IterOptions{LowerBound: key.NewChannelInfoColumnKey(id, key.MinColumnKey), UpperBound: key.NewChannelInfoColumnKey(id, key.MaxColumnKey)})
	defer it.Close()
	var info ChannelInfo
	err := wk.iterChannelInfo(it, func(c ChannelInfo) bool { info = c; return false })
	if err != nil {
		return info, err
	}
	return info, it.Error()
}
