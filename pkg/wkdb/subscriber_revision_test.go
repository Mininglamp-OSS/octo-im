package wkdb

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func snapshotOperation(revision uint64, id string, members ...string) SubscriberOperation {
	o := recoveryOperation(id, "reconcile", members...)
	o.BusinessRevision = revision
	o.RestoreConversationIDs = nil
	return o
}

func applySnapshotWork(t *testing.T, db *wukongDB, o SubscriberOperation) SubscriberWork {
	t.Helper()
	w := recoveryWork(t, db, o)
	if len(w.Effects) != 0 {
		require.NoError(t, db.ApplyConversationEffects(w.Effects))
	}
	completeRecoveryWork(t, db, w)
	return w
}

func TestSubscriberSnapshotLateRemoveCannotUndoRejoinAfterReceiptExpiry(t *testing.T) {
	dir := t.TempDir()
	db := recoveryTestDB(t, dir)
	join := snapshotOperation(1, "business-1", "a")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, join))
	applySnapshotWork(t, db, join)
	remove := snapshotOperation(2, "business-2")
	require.NoError(t, db.ApplySubscriberOperation(1, 2, remove))
	delayed := recoveryWork(t, db, remove)
	rejoin := snapshotOperation(3, "business-3", "a")
	rejoin.ConversationIDs[0] = 301
	require.NoError(t, db.ApplySubscriberOperation(1, 3, rejoin))
	applySnapshotWork(t, db, rejoin)
	// The old target message can arrive after the later snapshot completed.
	require.NoError(t, db.ApplyConversationEffects(delayed.Effects))
	completeRecoveryWork(t, db, delayed)
	// Trigger real receipt collection, rather than relying on the seven-day
	// deduplication window as the authority fence.
	prune := recoveryOperation("prune", "add")
	prune.CreatedAt = time.Now().Add(SubscriberReceiptRetention + time.Hour).UnixNano()
	prune.ChannelID = "other"
	for attempt := 0; db.GetChannelShardIndex(prune.ChannelID, 2) != db.GetChannelShardIndex("group", 2); attempt++ {
		require.Less(t, attempt, 100, "must find a channel on the same shard")
		prune.ChannelID = fmt.Sprintf("other-%d", attempt)
	}
	require.NoError(t, db.ApplySubscriberOperation(1, 4, prune))
	_, exists, err := db.GetSubscriberReceipt("group", 2, remove.OperationID)
	require.NoError(t, err)
	require.False(t, exists, "test must actually expire the old receipt")
	require.NoError(t, db.Close())
	db = reopenRecoveryTestDB(t, dir)
	defer db.Close()
	remove.CreatedAt = prune.CreatedAt
	require.NoError(t, db.ApplySubscriberOperation(1, 5, remove))
	r, found, err := db.GetSubscriberReceipt("group", 2, remove.OperationID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "stale_revision", r.Error)
	member, err := db.ExistSubscriber("group", 2, "a")
	require.NoError(t, err)
	require.True(t, member)
	conversation, err := db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	require.EqualValues(t, 301, conversation.Id)
	legacy := recoveryOperation("late-legacy-remove", "remove", "a")
	require.NoError(t, db.ApplySubscriberOperation(1, 6, legacy))
	r, _, err = db.GetSubscriberReceipt("group", 2, legacy.OperationID)
	require.NoError(t, err)
	require.Equal(t, "managed_channel", r.Error)
	member, err = db.ExistSubscriber("group", 2, "a")
	require.NoError(t, err)
	require.True(t, member)
}

func TestSubscriberSnapshotDenylistPreservesMutedMembership(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	o := snapshotOperation(1, "snapshot-1", "active", "muted")
	o.DenyUIDs = []string{"banned", "muted"}
	require.NoError(t, db.ApplySubscriberOperation(1, 1, o))
	applySnapshotWork(t, db, o)
	for _, uid := range []string{"active", "muted"} {
		present, err := db.ExistSubscriber("group", 2, uid)
		require.NoError(t, err)
		require.True(t, present)
	}
	present, err := db.ExistSubscriber("group", 2, "banned")
	require.NoError(t, err)
	require.False(t, present)
	denied, err := db.GetDenylist("group", 2)
	require.NoError(t, err)
	require.Len(t, denied, 2)
	lifecycle, found, err := db.ConversationLifecycle("muted", "group", 2)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, lifecycle.Deleted)
	// Unmute/rejoin must clear the old denylist as well as restore transport
	// membership. A correct subscriber set alone does not restore send access.
	o = snapshotOperation(2, "snapshot-2", "active", "banned", "muted")
	require.NoError(t, db.ApplySubscriberOperation(1, 2, o))
	applySnapshotWork(t, db, o)
	denied, err = db.GetDenylist("group", 2)
	require.NoError(t, err)
	require.Empty(t, denied)
	lifecycle, found, err = db.ConversationLifecycle("muted", "group", 2)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, lifecycle.Deleted)
	conversation, err := db.GetConversation("muted", "group", 2)
	require.NoError(t, err)
	require.False(t, IsEmptyConversation(conversation))
}

func TestSubscriberSnapshotRejectedAdmissionDoesNotAdvanceAuthority(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	one := snapshotOperation(1, "one", "a")
	one.MaxPending = 1
	require.NoError(t, db.ApplySubscriberOperation(1, 1, one))
	three := snapshotOperation(3, "three", "b")
	three.MaxPending = 1
	require.NoError(t, db.ApplySubscriberOperation(1, 2, three))
	r, _, err := db.GetSubscriberReceipt("group", 2, "three")
	require.NoError(t, err)
	require.Equal(t, "backlog_full", r.Error)
	applySnapshotWork(t, db, one)
	two := snapshotOperation(2, "two", "a", "b")
	require.NoError(t, db.ApplySubscriberOperation(1, 3, two))
	r, _, err = db.GetSubscriberReceipt("group", 2, "two")
	require.NoError(t, err)
	require.Equal(t, "pending", r.State, "a rejected higher revision did not supersede this snapshot")
	conflict := snapshotOperation(2, "another-two", "a")
	require.NoError(t, db.ApplySubscriberOperation(1, 4, conflict))
	r, _, err = db.GetSubscriberReceipt("group", 2, "another-two")
	require.NoError(t, err)
	require.Equal(t, "revision_conflict", r.Error)
}

func TestSubscriberSnapshotOnlySkipsDurablyCompletedUnchangedTargets(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	one := snapshotOperation(1, "one", "a", "b")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, one))
	two := snapshotOperation(2, "two", "a", "b")
	require.NoError(t, db.ApplySubscriberOperation(1, 2, two))
	w := recoveryWork(t, db, two)
	require.Len(t, w.Effects, 2, "unchanged pending work must still reach the targets before success")
	applySnapshotWork(t, db, two)
	applySnapshotWork(t, db, one)
	three := snapshotOperation(3, "three", "a", "b", "c")
	require.NoError(t, db.ApplySubscriberOperation(1, 3, three))
	w = recoveryWork(t, db, three)
	require.Len(t, w.Effects, 1)
	require.Equal(t, "c", w.Effects[0].UID, "completed members must not cause new target Raft fan-out")
}

func TestSubscriberSnapshotPagesReplaceOnlyTheirRangeAndFenceLatePages(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	initial := snapshotOperation(1, "initial", "a", "d", "m", "z")
	initial.DenyUIDs = []string{"d", "z"}
	require.NoError(t, db.ApplySubscriberOperation(1, 1, initial))
	applySnapshotWork(t, db, initial)
	first := snapshotOperation(2, "p0", "b")
	first.SnapshotID = strings.Repeat("a", 64)
	first.PageCount = 2
	first.RangeEnd = "m"
	require.NoError(t, first.Validate())
	require.NoError(t, db.ApplySubscriberOperation(1, 2, first))
	applySnapshotWork(t, db, first)
	for uid, want := range map[string]bool{"a": false, "d": false, "b": true, "m": true, "z": true} {
		got, err := db.ExistSubscriber("group", 2, uid)
		require.NoError(t, err)
		require.Equal(t, want, got, uid)
	}
	denied, err := db.GetDenylist("group", 2)
	require.NoError(t, err)
	require.Len(t, denied, 1)
	require.Equal(t, "z", denied[0].Uid)
	last := snapshotOperation(2, "p1", "n")
	last.SnapshotID = first.SnapshotID
	last.PageCount, last.PageIndex = 2, 1
	last.RangeStart = "m"
	require.NoError(t, db.ApplySubscriberOperation(1, 3, last))
	applySnapshotWork(t, db, last)
	members, err := db.GetSubscribers("group", 2)
	require.NoError(t, err)
	got := []string{}
	for _, m := range members {
		got = append(got, m.Uid)
	}
	require.ElementsMatch(t, []string{"b", "n"}, got)
	denied, err = db.GetDenylist("group", 2)
	require.NoError(t, err)
	require.Empty(t, denied)

	// Forget the ordinary receipt, retaining the permanent per-page digest.
	require.NoError(t, db.channelDb("group", 2).Delete(recoveryChannelKey(recoveryReceipt, "group", 2, "p0"), pebble.Sync))
	conflict := first
	conflict.UIDs = []string{"c"}
	require.NoError(t, db.ApplySubscriberOperation(1, 4, conflict))
	receipt, _, err := db.GetSubscriberReceipt("group", 2, "p0")
	require.NoError(t, err)
	require.Equal(t, "revision_conflict", receipt.Error)
	rejoin := snapshotOperation(3, "rejoin", "a", "b", "n")
	require.NoError(t, db.ApplySubscriberOperation(1, 5, rejoin))
	applySnapshotWork(t, db, rejoin)
	last.OperationID = "late-p1"
	require.NoError(t, db.ApplySubscriberOperation(1, 6, last))
	receipt, _, err = db.GetSubscriberReceipt("group", 2, last.OperationID)
	require.NoError(t, err)
	require.Equal(t, "stale_revision", receipt.Error)
	joined, err := db.ExistSubscriber("group", 2, "a")
	require.NoError(t, err)
	require.True(t, joined)
}

func TestSubscriberSnapshotPageValidation(t *testing.T) {
	valid := snapshotOperation(1, "p0", "a")
	valid.SnapshotID = strings.Repeat("b", 64)
	valid.PageCount = 2
	valid.RangeEnd = "m"
	require.NoError(t, valid.Validate())
	for name, mutate := range map[string]func(*SubscriberOperation){
		"outside":            func(o *SubscriberOperation) { o.UIDs = []string{"z"} },
		"denied outside":     func(o *SubscriberOperation) { o.DenyUIDs = []string{"z"} },
		"first starts late":  func(o *SubscriberOperation) { o.RangeStart = "a" },
		"unbounded middle":   func(o *SubscriberOperation) { o.RangeEnd = "" },
		"invalid digest":     func(o *SubscriberOperation) { o.SnapshotID = "x" },
		"missing page count": func(o *SubscriberOperation) { o.PageCount = 0 },
	} {
		t.Run(name, func(t *testing.T) { o := valid; mutate(&o); require.Error(t, o.Validate()) })
	}
}

func TestSubscriberSnapshotDisbandPreservesConversationsAndOtherMetadata(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	initial := snapshotOperation(1, "join", "a")
	initial.Channel = &ChannelInfo{ChannelId: "group", ChannelType: 2, Large: true}
	require.NoError(t, db.ApplySubscriberOperation(1, 1, initial))
	applySnapshotWork(t, db, initial)
	before, err := db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	disband := snapshotOperation(2, "disband", "a")
	disband.Channel = &ChannelInfo{ChannelId: "group", ChannelType: 2, Large: true, Disband: true}
	require.NoError(t, db.ApplySubscriberOperation(1, 2, disband))
	work := applySnapshotWork(t, db, disband)
	require.Empty(t, work.Effects)
	after, err := db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	require.Equal(t, before.Id, after.Id)
	info, err := db.GetChannel("group", 2)
	require.NoError(t, err)
	require.True(t, info.Disband)
	require.True(t, info.Large)
}
