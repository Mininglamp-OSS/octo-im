package wkdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// Use Pebble's strict filesystem to discard unsynced writes, rather than a
// graceful reopen which would persist the optional RelationDone marker too.
func openRecoveryPowerLossDB(t *testing.T, fs vfs.FS) (*wukongDB, func()) {
	t.Helper()
	wk := NewWukongDB(NewOptions(WithShardNum(2), WithNodeId(1))).(*wukongDB)
	for i := 0; i < 2; i++ {
		db, err := pebble.Open(fmt.Sprintf("shard%d", i), &pebble.Options{FS: fs, MemTableSize: 1 << 20})
		require.NoError(t, err)
		wk.dbs = append(wk.dbs, db)
		batch := NewBatchDB(i, db)
		batch.Start()
		wk.wkdbs = append(wk.wkdbs, batch)
	}
	// Pebble syncs its own directory; the test owns creation of the parent
	// directory entries and must persist those before simulating power loss.
	parent, err := fs.OpenDir("")
	require.NoError(t, err)
	require.NoError(t, parent.Sync())
	require.NoError(t, parent.Close())
	require.NoError(t, wk.loadSubscriberRecoveryActive())
	return wk, func() {
		for _, b := range wk.wkdbs {
			b.Stop()
		}
		for _, db := range wk.dbs {
			require.NoError(t, db.Close())
		}
	}
}

func TestCompletedSubscriberWorkSurvivesLostRelationDoneMarker(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprintf("remove=%v", remove), func(t *testing.T) {
			fs := vfs.NewStrictMem()
			db, closeDB := openRecoveryPowerLossDB(t, fs)
			uid, channel := "user", "group"
			for db.shardId(uid) == db.GetChannelShardIndex(channel, 2) {
				channel += "x"
			}
			operation := func(id, mode string) SubscriberOperation {
				return SubscriberOperation{OperationID: id, ChannelID: channel, ChannelType: 2,
					Mode: mode, UIDs: []string{uid}, ConversationIDs: []uint64{7},
					CreatedAt: time.Now().UnixNano(), MaxPending: 10}
			}
			join := operation("join", "add")
			var effects []ConversationEffect
			var final SubscriberOperation
			apply := func(o SubscriberOperation, version uint64) {
				require.NoError(t, db.ApplySubscriberRecoveryCommands([]SubscriberRecoveryCommand{{SlotID: 1, Version: version, Operation: &o}}))
				work := recoveryWork(t, db, o)
				effects = append(effects, work.Effects...)
				require.NoError(t, db.ApplyConversationEffects(work.Effects))
				completeRecoveryWork(t, db, work)
				final = o
			}
			apply(join, 1)
			if remove {
				apply(operation("leave", "remove"), 2)
			}
			// No business replay watermark or further user-shard fsync follows
			// completion. Suppress close-time flushes, then simulate power loss.
			fs.SetIgnoreSyncs(true)
			closeDB()
			fs.ResetToSyncedState()
			fs.SetIgnoreSyncs(false)
			db, closeDB = openRecoveryPowerLossDB(t, fs)
			defer closeDB()
			receipt, found, err := db.GetSubscriberReceipt(channel, 2, final.OperationID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "complete", receipt.State)
			lifecycle, found, err := db.ConversationLifecycle(uid, channel, 2)
			require.NoError(t, err)
			require.True(t, found)
			require.False(t, lifecycle.RelationDone, "the test must actually discard the unsynced marker")
			require.Equal(t, remove, lifecycle.Deleted)
			// Replay all target history, including the old add after a removal.
			for _, effect := range effects {
				require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{effect}))
			}
			_, err = db.GetConversation(uid, channel, 2)
			if remove {
				require.ErrorIs(t, err, ErrNotFound)
			} else {
				require.NoError(t, err)
			}
			parts, err := db.SubscriberBacklog()
			require.NoError(t, err)
			require.Empty(t, parts)
		})
	}
}
