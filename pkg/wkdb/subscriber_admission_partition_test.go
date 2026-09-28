package wkdb

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestRejectedSnapshotPreservesIntentsAndRetryCompletesEffects(t *testing.T) {
	dir := t.TempDir()
	db := recoveryTestDB(t, dir)
	initial := snapshotOperation(1, "initial", "a")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, initial))
	applySnapshotWork(t, db, initial)
	before, found, err := db.GetSubscriberIntent("group", 2, "a")
	require.NoError(t, err)
	require.True(t, found)
	// Occupy the same source partition without changing this channel's state.
	busy := recoveryOperation("busy", "add")
	busy.ChannelID = "busy"
	for db.GetChannelShardIndex(busy.ChannelID, 2) != db.GetChannelShardIndex("group", 2) {
		busy.ChannelID += "x"
	}
	require.NoError(t, db.ApplySubscriberOperation(1, 2, busy))
	change := snapshotOperation(2, "change", "b")
	change.MaxPending = 1
	require.NoError(t, db.ApplySubscriberOperation(1, 3, change))
	receipt, _, err := db.GetSubscriberReceipt("group", 2, "change")
	require.NoError(t, err)
	require.Equal(t, "backlog_full", receipt.Error)
	after, _, err := db.GetSubscriberIntent("group", 2, "a")
	require.NoError(t, err)
	require.Equal(t, before, after, "rejection must not overwrite a valid old intent")
	_, found, err = db.GetSubscriberIntent("group", 2, "b")
	require.NoError(t, err)
	require.False(t, found, "rejection must not leave an intent without work")
	require.NoError(t, db.Close())
	db = reopenRecoveryTestDB(t, dir)
	defer db.Close()
	completeRecoveryWork(t, db, recoveryWork(t, db, busy))
	require.NoError(t, db.ApplySubscriberOperation(1, 4, change))
	w := recoveryWork(t, db, change)
	require.Len(t, w.Effects, 2)
	applySnapshotWork(t, db, change)
	for uid, deleted := range map[string]bool{"a": true, "b": false} {
		present, err := db.ExistSubscriber("group", 2, uid)
		require.NoError(t, err)
		require.Equal(t, !deleted, present)
		lifecycle, found, err := db.ConversationLifecycle(uid, "group", 2)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, deleted, lifecycle.Deleted)
	}
	receipt, _, err = db.GetSubscriberReceipt("group", 2, "change")
	require.NoError(t, err)
	require.Equal(t, "complete", receipt.State)
	require.Equal(t, 2, receipt.Completed)
}

func TestSnapshotPartitionOutOfOrderCompletionAndPowerLoss(t *testing.T) {
	for _, order := range [][]uint32{{0, 1, 2}, {2, 0, 1}, {1, 2, 0}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			fs := vfs.NewStrictMem()
			db, closeDB := openRecoveryPowerLossDB(t, fs)
			pages := []SubscriberOperation{
				partitionPage(1, 0, 3, "", "m", "a"),
				partitionPage(1, 1, 3, "m", "t", "n"),
				partitionPage(1, 2, 3, "t", "", "z"),
			}
			for index, page := range order {
				require.NoError(t, db.ApplySubscriberOperation(1, uint64(index+1), pages[page]))
				if index < 2 {
					applySnapshotWork(t, db, pages[page])
				}
				status, found, err := db.GetSubscriberSnapshotStatus("group", 2)
				require.NoError(t, err)
				require.True(t, found)
				require.True(t, status.Verified)
				require.False(t, status.Complete, "page admission is not whole snapshot completion")
			}
			fs.SetIgnoreSyncs(true)
			closeDB()
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)
			db, closeDB = openRecoveryPowerLossDB(t, fs)
			defer closeDB()
			status, _, err := db.GetSubscriberSnapshotStatus("group", 2)
			require.NoError(t, err)
			require.EqualValues(t, 3, status.AcceptedPages)
			require.EqualValues(t, 2, status.CompletedPages)
			last := pages[order[2]]
			w := applySnapshotWork(t, db, last)
			completeRecoveryWork(t, db, w) // lost reply / duplicate checkpoint
			status, _, err = db.GetSubscriberSnapshotStatus("group", 2)
			require.NoError(t, err)
			require.True(t, status.Complete)
			require.EqualValues(t, 3, status.CompletedPages)
			for _, page := range pages {
				require.NoError(t, db.ApplySubscriberOperation(1, 10+uint64(page.PageIndex), page))
			}
			status, _, err = db.GetSubscriberSnapshotStatus("group", 2)
			require.NoError(t, err)
			require.EqualValues(t, 3, status.CompletedPages)
			require.EqualValues(t, 3, status.AcceptedPages)
		})
	}
}

func TestSnapshotPartitionOldCheckpointCannotCompleteNewRevision(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	old := partitionPage(1, 0, 1, "", "", "a")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, old))
	delayed := recoveryWork(t, db, old)
	first := partitionPage(2, 0, 2, "", "m", "b")
	require.NoError(t, db.ApplySubscriberOperation(1, 2, first))
	applySnapshotWork(t, db, first)
	require.NoError(t, db.ApplyConversationEffects(delayed.Effects))
	completeRecoveryWork(t, db, delayed)
	status, _, err := db.GetSubscriberSnapshotStatus("group", 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, status.Revision)
	require.EqualValues(t, 1, status.CompletedPages)
	require.False(t, status.Complete)
}

func TestSnapshotRepairsLegacyOrphanWithoutRecreatingHealthyConversation(t *testing.T) {
	dir := t.TempDir()
	db := recoveryTestDB(t, dir)
	initial := snapshotOperation(1, "legacy-initial", "a", "healthy")
	initial.SourceProtocol = 0
	require.NoError(t, db.ApplySubscriberOperation(1, 1, initial))
	applySnapshotWork(t, db, initial)
	require.NoError(t, db.UpdateConversationIfSeqGreater("healthy", "group", 2, 75))
	require.NoError(t, db.UpdateConversationDeletedAtMsgSeq("healthy", "group", 2, 50))
	before, err := db.GetConversation("healthy", "group", 2)
	require.NoError(t, err)
	busy := recoveryOperation("busy", "add")
	busy.ChannelID = "busy"
	for db.GetChannelShardIndex(busy.ChannelID, 2) != db.GetChannelShardIndex("group", 2) {
		busy.ChannelID += "x"
	}
	require.NoError(t, db.ApplySubscriberOperation(1, 2, busy))
	broken := snapshotOperation(2, "legacy-rejected", "b", "healthy")
	broken.SourceProtocol = 0
	broken.MaxPending = 1
	require.NoError(t, db.ApplySubscriberOperation(1, 3, broken))
	r, _, err := db.GetSubscriberReceipt("group", 2, broken.OperationID)
	require.NoError(t, err)
	require.Equal(t, "backlog_full", r.Error)
	orphan, found, err := db.GetSubscriberIntent("group", 2, "b")
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, orphan.SourceWorkVersion)
	require.NoError(t, db.Close())
	db = reopenRecoveryTestDB(t, dir)
	defer db.Close()
	completeRecoveryWork(t, db, recoveryWork(t, db, busy))
	// Replay the old bug all the way to false completion: the member is now
	// absent from the source list but still has its original live conversation.
	broken.MaxPending = 128
	require.NoError(t, db.ApplySubscriberOperation(1, 4, broken))
	require.Empty(t, recoveryWork(t, db, broken).Effects)
	applySnapshotWork(t, db, broken)
	present, err := db.ExistSubscriber("group", 2, "a")
	require.NoError(t, err)
	require.False(t, present)
	_, err = db.GetConversation("a", "group", 2)
	require.NoError(t, err, "the old false-success conversation must actually exist")
	repair := snapshotOperation(3, "repair", "b", "healthy")
	require.NoError(t, db.ApplySubscriberOperation(1, 5, repair))
	w := recoveryWork(t, db, repair)
	require.Len(t, w.Effects, 3)
	// An additional snapshot before the repair finishes must see the new work
	// reference, not mistake the missing legacy source work for completion.
	concurrent := snapshotOperation(4, "concurrent-repair", "b", "healthy")
	require.NoError(t, db.ApplySubscriberOperation(1, 6, concurrent))
	require.Len(t, recoveryWork(t, db, concurrent).Effects, 2)
	applySnapshotWork(t, db, repair)
	applySnapshotWork(t, db, concurrent)
	after, err := db.GetConversation("healthy", "group", 2)
	require.NoError(t, err)
	require.Equal(t, before, after)
	for uid, deleted := range map[string]bool{"a": true, "b": false, "healthy": false} {
		lifecycle, found, err := db.ConversationLifecycle(uid, "group", 2)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, deleted, lifecycle.Deleted)
	}
	unchanged := snapshotOperation(5, "unchanged", "b", "healthy")
	require.NoError(t, db.ApplySubscriberOperation(1, 7, unchanged))
	require.Empty(t, recoveryWork(t, db, unchanged).Effects)
	applySnapshotWork(t, db, unchanged)
}

func TestSnapshotLegacyDigestNeedsNewRevisionAndRetainsReplay(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	first := partitionPage(1, 0, 2, "", "m", "a")
	first.SourceProtocol = 0
	require.NoError(t, db.ApplySubscriberOperation(1, 1, first))
	applySnapshotWork(t, db, first)
	last := partitionPage(1, 1, 2, "m", "", "z")
	require.NoError(t, db.ApplySubscriberOperation(1, 2, last))
	r, _, err := db.GetSubscriberReceipt("group", 2, last.OperationID)
	require.NoError(t, err)
	require.Equal(t, "snapshot_upgrade_required", r.Error)
	status, _, err := db.GetSubscriberSnapshotStatus("group", 2)
	require.NoError(t, err)
	require.False(t, status.Verified)
	require.False(t, status.Complete)
	next := partitionPage(2, 0, 1, "", "", "a", "z")
	require.NoError(t, db.ApplySubscriberOperation(1, 3, next))
	applySnapshotWork(t, db, next)
	status, _, err = db.GetSubscriberSnapshotStatus("group", 2)
	require.NoError(t, err)
	require.True(t, status.Complete)
	// Historical exact retries remain successful without changing the current snapshot.
	require.NoError(t, db.ApplySubscriberOperation(1, 4, first))
	status, _, err = db.GetSubscriberSnapshotStatus("group", 2)
	require.NoError(t, err)
	require.EqualValues(t, 2, status.Revision)
}

func TestSnapshotCompletionWaitsForEveryTargetCheckpoint(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	o := partitionPage(1, 0, 1, "", "", "a", "b")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, o))
	w := recoveryWork(t, db, o)
	require.NoError(t, db.ApplyConversationEffects(w.Effects))
	require.NoError(t, db.CheckpointSubscriberWork(SubscriberCheckpoint{SlotID: 1, ChannelID: "group", ChannelType: 2, OperationID: o.OperationID, Version: 1, Next: 2, At: time.Now().UnixNano()}))
	status, _, err := db.GetSubscriberSnapshotStatus("group", 2)
	require.NoError(t, err)
	require.False(t, status.Complete)
	require.Zero(t, status.CompletedPages)
	w.Next = 2
	completeRecoveryWork(t, db, w)
	status, _, err = db.GetSubscriberSnapshotStatus("group", 2)
	require.NoError(t, err)
	require.True(t, status.Complete)
}

func partitionPage(revision uint64, index, count uint32, start, end string, uids ...string) SubscriberOperation {
	o := snapshotOperation(revision, fmt.Sprintf("partition-%d-%d", revision, index), uids...)
	o.SnapshotID, o.PageIndex, o.PageCount = strings.Repeat("a", 64), index, count
	o.RangeStart, o.RangeEnd = start, end
	o.Channel = &ChannelInfo{ChannelId: o.ChannelID, ChannelType: o.ChannelType}
	return o
}

func TestSnapshotPartitionRejectsContradictoryPagesBeforeMutation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		first, bad SubscriberOperation
	}{
		{"gap", partitionPage(1, 0, 2, "", "m", "a"), partitionPage(1, 1, 2, "z", "", "z")},
		{"overlap", partitionPage(1, 0, 2, "", "n", "m1"), partitionPage(1, 1, 2, "m", "", "z")},
		{"reverse overlap", partitionPage(1, 1, 2, "m", "", "m1"), partitionPage(1, 0, 2, "", "n", "a")},
		{"nonadjacent overlap", partitionPage(1, 0, 3, "", "n", "m1"), partitionPage(1, 2, 3, "m", "", "z")},
		{"nonadjacent no room", partitionPage(1, 0, 3, "", "m", "a"), partitionPage(1, 2, 3, "m", "", "z")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := recoveryTestDB(t, t.TempDir())
			defer db.Close()
			require.NoError(t, db.ApplySubscriberOperation(1, 1, tc.first))
			applySnapshotWork(t, db, tc.first)
			before, err := db.GetSubscribers("group", 2)
			require.NoError(t, err)
			require.NoError(t, db.ApplySubscriberOperation(1, 2, tc.bad))
			r, _, err := db.GetSubscriberReceipt("group", 2, tc.bad.OperationID)
			require.NoError(t, err)
			require.Equal(t, "revision_conflict", r.Error)
			after, err := db.GetSubscribers("group", 2)
			require.NoError(t, err)
			require.Equal(t, before, after)
			// A rejected page must not poison subsequent source Apply.
			next := snapshotOperation(2, "next", "next")
			require.NoError(t, db.ApplySubscriberOperation(1, 3, next))
			applySnapshotWork(t, db, next)
		})
	}
}

func TestSnapshotPartitionRejectsDivergentChannelFlags(t *testing.T) {
	for _, flag := range []string{"ban", "large", "disband"} {
		t.Run(flag, func(t *testing.T) {
			db := recoveryTestDB(t, t.TempDir())
			defer db.Close()
			first := partitionPage(1, 0, 2, "", "m", "a")
			require.NoError(t, db.ApplySubscriberOperation(1, 1, first))
			applySnapshotWork(t, db, first)
			last := partitionPage(1, 1, 2, "m", "", "z")
			switch flag {
			case "ban":
				last.Channel.Ban = true
			case "large":
				last.Channel.Large = true
			case "disband":
				last.Channel.Disband = true
			}
			require.NoError(t, db.ApplySubscriberOperation(1, 2, last))
			r, _, err := db.GetSubscriberReceipt("group", 2, last.OperationID)
			require.NoError(t, err)
			require.Equal(t, "revision_conflict", r.Error)
			info, err := db.GetChannel("group", 2)
			require.NoError(t, err)
			require.False(t, info.Ban || info.Large || info.Disband)
		})
	}
}
