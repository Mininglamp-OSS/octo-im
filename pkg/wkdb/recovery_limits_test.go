package wkdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestSubscriberRecoveryLargeExistingGroupAndAdmission(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	uids := make([]string, 5000)
	for i := range uids {
		uids[i] = fmt.Sprintf("u%05d", i)
	}
	o := recoveryOperation("large", "add", uids...)
	o.MaxPending = 1024
	require.NoError(t, db.ApplySubscriberOperation(1, 1, o))
	r, _, err := db.GetSubscriberReceipt("group", 2, "large")
	require.NoError(t, err)
	require.Equal(t, "pending", r.State)
	require.Equal(t, 5000, r.Total)
	blocked := recoveryOperation("blocked", "remove", "u00000")
	blocked.MaxPending = 1024
	require.NoError(t, db.ApplySubscriberOperation(1, 2, blocked))
	r, _, err = db.GetSubscriberReceipt("group", 2, "blocked")
	require.NoError(t, err)
	require.Equal(t, "backlog_full", r.Error)
	completeRecoveryWork(t, db, recoveryWork(t, db, o))
	remove := recoveryOperation("all", "remove_all")
	remove.MaxPending = 1024
	require.NoError(t, db.ApplySubscriberOperation(1, 3, remove))
	w := recoveryWork(t, db, remove)
	require.Len(t, w.Effects, 5000, "all previous members must survive in durable work")
	members, err := db.GetSubscribers("group", 2)
	require.NoError(t, err)
	require.Empty(t, members)
}

func TestSubscriberRecoveryReceiptRetentionKeepsPendingAndFences(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	o := recoveryOperation("old", "add", "a")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, o))
	w := recoveryWork(t, db, o)
	require.NoError(t, db.ApplyConversationEffects(w.Effects))
	require.NoError(t, db.CheckpointSubscriberWork(SubscriberCheckpoint{SlotID: 1, ChannelID: "group", ChannelType: 2, OperationID: "old", Version: 1, Next: 1, Done: true, At: o.CreatedAt}))
	pending := recoveryOperation("pending", "remove", "a")
	require.NoError(t, db.ApplySubscriberOperation(1, 2, pending))
	later := recoveryOperation("later", "add", "b")
	later.CreatedAt = o.CreatedAt + int64(SubscriberReceiptRetention+time.Second)
	require.NoError(t, db.ApplySubscriberOperation(1, 3, later))
	_, found, err := db.GetSubscriberReceipt("group", 2, "old")
	require.NoError(t, err)
	require.False(t, found)
	r, found, err := db.GetSubscriberReceipt("group", 2, "pending")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "pending", r.State)
	_, found, err = db.ConversationLifecycle("a", "group", 2)
	require.NoError(t, err)
	require.True(t, found, "receipt cleanup must not remove lifecycle fences")
}

func TestPermissionListsDuplicateAndReplayCounts(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(fmt.Sprint(allow), func(t *testing.T) {
			db := recoveryTestDB(t, t.TempDir())
			defer db.Close()
			add, remove, get := db.AddDenylist, db.RemoveDenylist, db.GetDenylist
			if allow {
				add, remove, get = db.AddAllowlist, db.RemoveAllowlist, db.GetAllowlist
			}
			for i := 0; i < 2; i++ {
				require.NoError(t, add("g", 2, []Member{{Uid: "a"}, {Uid: "a"}, {Uid: "b"}}))
			}
			for i := 0; i < 2; i++ {
				require.NoError(t, remove("g", 2, []string{"a", "a", "missing"}))
			}
			rows, err := get("g", 2)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			channel, err := db.GetChannel("g", 2)
			require.NoError(t, err)
			count := channel.DenylistCount
			if allow {
				count = channel.AllowlistCount
			}
			require.EqualValues(t, 1, count)
		})
	}
}

func TestConversationLifecycleCacheInvalidatesMissAndDelete(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	_, found, err := db.ConversationLifecycle("a", "g", 2)
	require.NoError(t, err)
	require.False(t, found)
	e := ConversationEffect{UID: "a", ChannelID: "g", ChannelType: 2, Version: 1, ConversationID: 7, CreatedAt: time.Now().UnixNano()}
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	got, found, err := db.ConversationLifecycle("a", "g", 2)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, got.RelationDone)
	e.Version = 2
	e.Deleted = true
	e.ConversationID = 0
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	got, _, err = db.ConversationLifecycle("a", "g", 2)
	require.NoError(t, err)
	require.True(t, got.Deleted)
}

func TestConversationLifecycleDeletesUnknownColumnsWithoutTouchingNeighbor(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	e := ConversationEffect{UID: "a", ChannelID: "g", ChannelType: 2, Version: 1, ConversationID: 7, CreatedAt: time.Now().UnixNano()}
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	unknown := key.NewConversationColumnKey("a", 7, [2]byte{0x09, 0xff})
	neighbor := key.NewConversationColumnKey("a", 8, [2]byte{0x09, 0xff})
	physical := db.shardDB("a")
	require.NoError(t, physical.Set(unknown, []byte("old"), pebble.Sync))
	require.NoError(t, physical.Set(neighbor, []byte("keep"), pebble.Sync))
	e.Version = 2
	e.Deleted = true
	e.ConversationID = 0
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	_, closer, err := physical.Get(unknown)
	if closer != nil {
		closer.Close()
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
	value, closer, err := physical.Get(neighbor)
	require.NoError(t, err)
	require.Equal(t, "keep", string(value))
	closer.Close()
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
}
