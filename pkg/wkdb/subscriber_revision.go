package wkdb

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

// Keep the authority fence after receipt expiry and channel deletion. A late
// request must never acquire a new source lifecycle just because its receipt
// was collected. This namespace is introduced by subscriber protocol 4.
const recoveryBusinessRevision byte = 12
const recoveryBusinessPage byte = 13

type subscriberRevision struct {
	SnapshotID           string `json:"snapshot_id,omitempty"`
	PageCount            uint32 `json:"page_count,omitempty"`
	Revision             uint64 `json:"revision"`
	Digest               string `json:"digest"`
	OperationID          string `json:"operation_id"`
	SourceProtocol       uint32 `json:"source_protocol,omitempty"`
	HasChannel           bool   `json:"has_channel,omitempty"`
	Ban                  bool   `json:"ban,omitempty"`
	Large                bool   `json:"large,omitempty"`
	Disband              bool   `json:"disband,omitempty"`
	AcceptedPages        uint32 `json:"accepted_pages,omitempty"`
	CompletedPages       uint32 `json:"completed_pages,omitempty"`
	Complete             bool   `json:"complete,omitempty"`
	LegacyRepairComplete bool   `json:"legacy_repair_complete,omitempty"`
}

// Old false-success removals can leave a conversation intent after the source
// membership has already disappeared. Include those UIDs when adopting legacy
// data. Once every legacy intent has a durable work reference, later snapshots
// need no history scan. A partial snapshot cannot suppress the remaining audit.
func legacySubscriberIntents(db *pebble.DB, o SubscriberOperation) ([]string, bool, error) {
	if o.Mode != "reconcile" || o.SourceProtocol != SubscriberAtomicProtocolVersion {
		return nil, false, nil
	}
	var current subscriberRevision
	_, err := recoveryRead(db, subscriberRevisionKey(o.ChannelID, o.ChannelType), &current)
	if err != nil {
		return nil, false, err
	}
	if current.LegacyRepairComplete {
		return nil, true, nil
	}
	prefix := recoveryKey(recoveryIntent, o.ChannelID, string([]byte{o.ChannelType}))
	it := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: append(bytes.Clone(prefix), 0xff)})
	defer it.Close()
	var uids []string
	complete := true
	for it.First(); it.Valid(); it.Next() {
		var intent ConversationEffect
		if err := json.Unmarshal(it.Value(), &intent); err != nil {
			return nil, false, &PermanentApplyError{fmt.Errorf("decode legacy subscriber intent: %w", err)}
		}
		if intent.SourceWorkVersion != 0 {
			continue
		}
		complete = false
		if o.snapshotContains(intent.UID) {
			uids = append(uids, intent.UID)
		}
	}
	return uids, complete, it.Error()
}

type subscriberSnapshotPage struct {
	Index      uint32 `json:"index"`
	RangeStart string `json:"range_start"`
	RangeEnd   string `json:"range_end"`
	Digest     string `json:"digest"`
	Complete   bool   `json:"complete,omitempty"`
}

// SubscriberSnapshotStatus describes the latest authority, independently of
// the per-operation HTTP receipt. Legacy digests cannot prove page coverage.
type SubscriberSnapshotStatus struct {
	Revision       uint64 `json:"revision"`
	SnapshotID     string `json:"snapshot_id,omitempty"`
	PageCount      uint32 `json:"page_count"`
	AcceptedPages  uint32 `json:"accepted_pages"`
	CompletedPages uint32 `json:"completed_pages"`
	Verified       bool   `json:"verified"`
	Complete       bool   `json:"complete"`
}

func (wk *wukongDB) GetSubscriberSnapshotStatus(channel string, channelType uint8) (SubscriberSnapshotStatus, bool, error) {
	var r subscriberRevision
	found, err := recoveryRead(wk.channelDb(channel, channelType), subscriberRevisionKey(channel, channelType), &r)
	return SubscriberSnapshotStatus{Revision: r.Revision, SnapshotID: r.SnapshotID, PageCount: max(1, r.PageCount), AcceptedPages: r.AcceptedPages, CompletedPages: r.CompletedPages, Verified: r.SourceProtocol == SubscriberAtomicProtocolVersion, Complete: r.Complete}, found, err
}

func subscriberRevisionKey(channel string, channelType uint8) []byte {
	return recoveryChannelKey(recoveryBusinessRevision, channel, channelType, "")
}

// Called under the source partition/channel locks. Admission and the resulting
// revision update share the source membership/intent/receipt transaction.
func (wk *wukongDB) checkSubscriberRevision(db *pebble.DB, o SubscriberOperation) (string, error) {
	var current subscriberRevision
	found, err := recoveryRead(db, subscriberRevisionKey(o.ChannelID, o.ChannelType), &current)
	if err != nil || !found {
		return "", err
	}
	if o.Mode != "reconcile" {
		return "managed_channel", nil
	}
	if o.BusinessRevision < current.Revision {
		return "stale_revision", nil
	}
	if current.SourceProtocol == SubscriberAtomicProtocolVersion && o.SourceProtocol != SubscriberAtomicProtocolVersion {
		return "revision_conflict", nil
	}
	if o.BusinessRevision == current.Revision {
		if o.PageCount != current.PageCount || o.SnapshotID != current.SnapshotID {
			return "revision_conflict", nil
		}
		if o.SourceProtocol == SubscriberAtomicProtocolVersion {
			if current.SourceProtocol != SubscriberAtomicProtocolVersion {
				// Old page digests do not contain reconstructible boundaries.
				// A new business revision must resend the complete snapshot.
				return "snapshot_upgrade_required", nil
			}
			if current.HasChannel != (o.Channel != nil) || o.Channel != nil && (current.Ban != o.Channel.Ban || current.Large != o.Channel.Large || current.Disband != o.Channel.Disband) {
				return "revision_conflict", nil
			}
			if o.PageCount == 0 && (o.Digest() != current.Digest || o.OperationID != current.OperationID) {
				return "revision_conflict", nil
			}
			return checkSnapshotPartition(db, o)
		}
		if o.PageCount == 0 {
			if o.Digest() != current.Digest || o.OperationID != current.OperationID {
				return "revision_conflict", nil
			}
		} else {
			var digest string
			found, err := recoveryRead(db, subscriberPageKey(o), &digest)
			if err != nil {
				return "", err
			}
			if found && digest != o.Digest() {
				return "revision_conflict", nil
			}
		}
	}
	return "", nil
}

// Nearest stored neighbors also catch overlap while intermediate pages have
// not arrived. Adjacent indices must touch; non-adjacent indices must leave
// room for the missing, nonempty ranges. No scan or allocation by PageCount.
func checkSnapshotPartition(db *pebble.DB, o SubscriberOperation) (string, error) {
	var page subscriberSnapshotPage
	found, err := recoveryRead(db, subscriberPageKey(o), &page)
	if err != nil {
		return "", err
	}
	if found {
		if page.Digest != o.Digest() {
			return "revision_conflict", nil
		}
		return "", nil
	}
	prefix := recoveryKey(recoveryBusinessPage, o.ChannelID, string([]byte{o.ChannelType}))
	it := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: append(bytes.Clone(prefix), 0xff)})
	defer it.Close()
	decode := func() error {
		if err := json.Unmarshal(it.Value(), &page); err != nil {
			return &PermanentApplyError{fmt.Errorf("decode snapshot neighbor: %w", err)}
		}
		return nil
	}
	if it.SeekLT(subscriberPageKey(o)) {
		if err := decode(); err != nil {
			return "", err
		}
		if page.RangeEnd == "" || page.RangeEnd > o.RangeStart ||
			(page.Index+1 == o.PageIndex && page.RangeEnd != o.RangeStart) ||
			(page.Index+1 < o.PageIndex && page.RangeEnd == o.RangeStart) {
			return "revision_conflict", nil
		}
	}
	if it.SeekGE(subscriberPageKey(o)) {
		if err := decode(); err != nil {
			return "", err
		}
		if o.RangeEnd == "" || o.RangeEnd > page.RangeStart ||
			(o.PageIndex+1 == page.Index && o.RangeEnd != page.RangeStart) ||
			(o.PageIndex+1 < page.Index && o.RangeEnd == page.RangeStart) {
			return "revision_conflict", nil
		}
	}
	return "", it.Error()
}

// SubscriberBusinessManaged also guards legacy metadata/delta commands during
// replay. Authority survives channel deletion and receipt collection.
func (wk *wukongDB) SubscriberBusinessManaged(channel string, channelType uint8) (bool, error) {
	var revision subscriberRevision
	return recoveryRead(wk.channelDb(channel, channelType), subscriberRevisionKey(channel, channelType), &revision)
}

func (o SubscriberOperation) snapshotContains(uid string) bool {
	return o.PageCount == 0 || (uid >= o.RangeStart && (o.RangeEnd == "" || uid < o.RangeEnd))
}

func (o SubscriberOperation) validateSnapshotPage() error {
	if o.PageCount == 0 {
		if o.PageIndex != 0 || o.SnapshotID != "" || o.RangeStart != "" || o.RangeEnd != "" {
			return errors.New("invalid snapshot page")
		}
		return nil
	}
	digest, err := hex.DecodeString(o.SnapshotID)
	if o.Mode != "reconcile" || err != nil || len(digest) != 32 || o.PageIndex >= o.PageCount ||
		(o.PageIndex == 0 && o.RangeStart != "") || (o.PageIndex > 0 && o.RangeStart == "") ||
		(o.PageIndex == o.PageCount-1 && o.RangeEnd != "") || (o.PageIndex < o.PageCount-1 && o.RangeEnd == "") ||
		(o.RangeEnd != "" && o.RangeStart >= o.RangeEnd) || len(o.RangeStart) > 1024 || len(o.RangeEnd) > 1024 {
		return errors.New("invalid snapshot page bounds")
	}
	for _, list := range [][]string{o.UIDs, o.DenyUIDs} {
		for _, uid := range list {
			if !o.snapshotContains(uid) {
				return errors.New("subscriber outside snapshot page")
			}
		}
	}
	return nil
}

func subscriberPageKey(o SubscriberOperation) []byte {
	return recoveryChannelKey(recoveryBusinessPage, o.ChannelID, o.ChannelType, fmt.Sprintf("%08x", o.PageIndex))
}

func (wk *wukongDB) stageSubscriberRevision(db *pebble.DB, batch *pebble.Batch, o SubscriberOperation, legacyRepairComplete bool) error {
	var current subscriberRevision
	_, err := recoveryRead(db, subscriberRevisionKey(o.ChannelID, o.ChannelType), &current)
	if err != nil {
		return err
	}
	if o.BusinessRevision > current.Revision {
		prefix := recoveryKey(recoveryBusinessPage, o.ChannelID, string([]byte{o.ChannelType}))
		if err := batch.DeleteRange(prefix, append(append([]byte(nil), prefix...), 0xff), wk.noSync); err != nil {
			return err
		}
	}
	if o.SourceProtocol == SubscriberAtomicProtocolVersion {
		var page subscriberSnapshotPage
		found := false
		if current.Revision == o.BusinessRevision {
			found, err = recoveryRead(db, subscriberPageKey(o), &page)
			if err != nil {
				return err
			}
		} else {
			current = subscriberRevision{Revision: o.BusinessRevision, Digest: o.Digest(), OperationID: o.OperationID, SnapshotID: o.SnapshotID, PageCount: o.PageCount, SourceProtocol: o.SourceProtocol, HasChannel: o.Channel != nil}
			if o.Channel != nil {
				current.Ban, current.Large, current.Disband = o.Channel.Ban, o.Channel.Large, o.Channel.Disband
			}
		}
		if !found {
			page = subscriberSnapshotPage{Index: o.PageIndex, RangeStart: o.RangeStart, RangeEnd: o.RangeEnd, Digest: o.Digest()}
			current.AcceptedPages++
			if err := recoverySet(batch, subscriberPageKey(o), page); err != nil {
				return err
			}
		}
		current.LegacyRepairComplete = legacyRepairComplete
		return recoverySet(batch, subscriberRevisionKey(o.ChannelID, o.ChannelType), current)
	}
	if o.PageCount > 0 {
		if err := recoverySet(batch, subscriberPageKey(o), o.Digest()); err != nil {
			return err
		}
	}
	return recoverySet(batch, subscriberRevisionKey(o.ChannelID, o.ChannelType), subscriberRevision{
		Revision: o.BusinessRevision, Digest: o.Digest(), OperationID: o.OperationID, SnapshotID: o.SnapshotID, PageCount: o.PageCount,
	})
}

// A source checkpoint follows target effects and tag invalidation. Update the
// page and aggregate counters in that same batch; duplicate/old checkpoints
// cannot complete a newer revision or count a page twice.
func (wk *wukongDB) completeSubscriberSnapshotPage(db *pebble.DB, batch *pebble.Batch, w SubscriberWork) error {
	o := w.Operation
	if o.Mode != "reconcile" || o.SourceProtocol != SubscriberAtomicProtocolVersion {
		return nil
	}
	var current subscriberRevision
	found, err := recoveryRead(db, subscriberRevisionKey(o.ChannelID, o.ChannelType), &current)
	if err != nil {
		return err
	}
	if !found || current.Revision != o.BusinessRevision || current.SnapshotID != o.SnapshotID || current.SourceProtocol != o.SourceProtocol {
		return nil
	}
	var page subscriberSnapshotPage
	found, err = recoveryRead(db, subscriberPageKey(o), &page)
	if err != nil {
		return err
	}
	if !found || page.Digest != w.SnapshotDigest {
		return &PermanentApplyError{errors.New("missing or mismatched snapshot page checkpoint")}
	}
	if page.Complete {
		return nil
	}
	page.Complete = true
	current.CompletedPages++
	if current.CompletedPages > current.AcceptedPages || current.AcceptedPages > max(1, current.PageCount) {
		return &PermanentApplyError{errors.New("invalid snapshot completion counters")}
	}
	current.Complete = current.AcceptedPages == max(1, current.PageCount) && current.CompletedPages == current.AcceptedPages
	if current.Complete {
		current.LegacyRepairComplete = true
	}
	if err := recoverySet(batch, subscriberPageKey(o), page); err != nil {
		return err
	}
	return recoverySet(batch, subscriberRevisionKey(o.ChannelID, o.ChannelType), current)
}

// Each page replaces only its lexical UID range. Old members outside a page
// survive until their own page applies; a newer revision fences ALL older
// pages immediately. Each HTTP receipt covers one page and its target barrier;
// the snapshot status requires all pages and all of those barriers.
func (wk *wukongDB) stageSnapshotDenylist(batch *pebble.Batch, o SubscriberOperation, at time.Time) error {
	if o.PageCount == 0 {
		o.Mode, o.UIDs = "deny_set", o.DenyUIDs
		return wk.stageRecoveryDenylist(batch, o, at)
	}
	existing, err := wk.recoveryDenylist(o.ChannelID, o.ChannelType)
	if err != nil {
		return err
	}
	wanted := make(map[string]bool, len(o.DenyUIDs))
	for _, uid := range o.DenyUIDs {
		wanted[uid] = true
	}
	delta := 0
	for _, member := range existing {
		if !o.snapshotContains(member.Uid) {
			continue
		}
		if wanted[member.Uid] {
			delete(wanted, member.Uid)
			continue
		}
		if err := wk.removeDenylist(o.ChannelID, o.ChannelType, member, batch); err != nil {
			return err
		}
		delta--
	}
	for uid := range wanted {
		if err := wk.writeDenylist(o.ChannelID, o.ChannelType, Member{Id: key.HashWithString(uid), Uid: uid, CreatedAt: &at, UpdatedAt: &at}, batch); err != nil {
			return err
		}
		delta++
	}
	if delta != 0 {
		pk, err := wk.getChannelPrimaryKey(o.ChannelID, o.ChannelType)
		if err != nil {
			return err
		}
		return wk.incChannelInfoDenylistCount(pk, delta, batch)
	}
	return nil
}
