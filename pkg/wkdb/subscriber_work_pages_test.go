package wkdb

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/stretchr/testify/require"
)

func TestSubscriberWorkFiftyThousandMembersUsesBoundedCheckpoints(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	at := time.Now()
	_, err := db.AddChannel(ChannelInfo{ChannelId: "group", ChannelType: 2})
	require.NoError(t, err)
	members := make([]Member, 50000)
	for i := range members {
		uid := fmt.Sprintf("u%06d", i)
		members[i] = Member{Id: key.HashWithString(uid), Uid: uid, CreatedAt: &at, UpdatedAt: &at}
	}
	require.NoError(t, db.AddSubscribers("group", 2, members))
	o := recoveryOperation("large-tail", "remove_all")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, o))
	w, found, err := db.GetSubscriberWork("group", 2, 1, 1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 50000, w.Total)
	require.Empty(t, w.Effects)
	physical := db.channelDb("group", 2)
	header, closer, err := physical.Get(recoveryPendingKey(1, 1))
	require.NoError(t, err)
	require.Less(t, len(header), 2048, "scheduler record must not grow with membership")
	closer.Close()
	page, err := db.GetSubscriberWorkPage(w, 16)
	require.NoError(t, err)
	require.Len(t, page, 16)
	checkpoint := SubscriberCheckpoint{SlotID: 1, ChannelID: "group", ChannelType: 2, OperationID: o.OperationID, Version: 1, Next: 16}
	// Flush the large setup batch before measuring the small checkpoint so
	// WAL rotation cannot retire setup bytes during the measurement.
	require.NoError(t, physical.Flush())
	before := physical.Metrics().WAL.BytesIn
	require.NoError(t, db.CheckpointSubscriberWork(checkpoint))
	written := physical.Metrics().WAL.BytesIn - before
	require.Less(t, written, uint64(8192), "checkpoint must not rewrite megabytes of effects")
	t.Logf("50000-member work: one 16-effect checkpoint wrote %d logical WAL bytes", written)
	w, _, err = db.GetSubscriberWork("group", 2, 1, 1)
	require.NoError(t, err)
	require.Equal(t, 16, w.Next)
	page, err = db.GetSubscriberWorkPage(w, 16)
	require.NoError(t, err)
	require.Len(t, page, 16)
	require.Equal(t, "u000016", page[0].UID)

	// The scheduler and a retry checkpoint must not deserialize later pages.
	require.NoError(t, physical.Set(recoveryEffectKey(1, 1, 700), []byte("unread corrupt page"), db.sync))
	ws, _, _, err := db.ListSubscriberWork(int(db.GetChannelShardIndex("group", 2)), nil, 1)
	require.NoError(t, err)
	require.Len(t, ws, 1)
	require.Empty(t, ws[0].Effects)
	checkpoint.Previous, checkpoint.Next = 16, 16
	checkpoint.RetryAt = at.Add(time.Hour).UnixNano()
	require.NoError(t, db.CheckpointSubscriberWork(checkpoint))
	// A caller that actually reaches the corrupt page must still fail closed.
	w.Next = 700 * SubscriberWorkChunkSize
	_, err = db.GetSubscriberWorkPage(w, 16)
	var permanent *PermanentApplyError
	require.ErrorAs(t, err, &permanent)
}

func TestSubscriberWorkPagesMigrateAndResumeAfterRestart(t *testing.T) {
	dir := t.TempDir()
	db := recoveryTestDB(t, dir)
	uids := make([]string, 130)
	for i := range uids {
		uids[i] = fmt.Sprintf("u%03d", i)
	}
	o := recoveryOperation("legacy", "add", uids...)
	require.NoError(t, db.ApplySubscriberOperation(1, 1, o))
	w, _, err := db.GetSubscriberWork("group", 2, 1, 1)
	require.NoError(t, err)
	effects, err := db.GetSubscriberWorkPage(w, MaxSubscriberWorkPage)
	require.NoError(t, err)
	require.Len(t, effects, 130)
	require.NoError(t, db.CheckpointSubscriberWork(SubscriberCheckpoint{SlotID: 1, ChannelID: "group", ChannelType: 2, OperationID: o.OperationID, Version: 1, Next: 16}))
	w.Next, w.Total, w.Paged, w.Effects, w.Operation = 16, 0, false, effects, o
	physical := db.channelDb("group", 2)
	batch := physical.NewBatch()
	require.NoError(t, recoverySet(batch, recoveryPendingKey(1, 1), w))
	prefix := recoveryEffectPrefix(1, 1)
	require.NoError(t, batch.DeleteRange(prefix, append(bytes.Clone(prefix), 0xff), db.noSync))
	require.NoError(t, batch.Commit(db.sync))
	batch.Close()
	require.NoError(t, db.Close())

	db = recoveryTestDB(t, dir)
	defer db.Close()
	w, found, err := db.GetSubscriberWork("group", 2, 1, 1)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, w.Paged)
	require.Empty(t, w.Effects)
	require.Empty(t, w.Operation.UIDs)
	require.Equal(t, 16, w.Next)
	require.Equal(t, 130, w.Total)
	for w.Next < w.Total {
		page, err := db.GetSubscriberWorkPage(w, 16)
		require.NoError(t, err)
		require.Equal(t, effects[w.Next:w.Next+len(page)], page)
		next := w.Next + len(page)
		require.NoError(t, db.ApplyConversationEffects(page))
		require.NoError(t, db.CheckpointSubscriberWork(SubscriberCheckpoint{SlotID: 1, ChannelID: "group", ChannelType: 2, OperationID: o.OperationID, Version: 1, Previous: w.Next, Next: next, Done: next == w.Total, At: time.Now().UnixNano()}))
		w.Next = next
	}
	_, found, err = db.GetSubscriberWork("group", 2, 1, 1)
	require.NoError(t, err)
	require.False(t, found)
	r, found, err := db.GetSubscriberReceipt("group", 2, o.OperationID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "complete", r.State)
	for page := 0; page < 3; page++ {
		var effects []ConversationEffect
		found, err := recoveryRead(db.channelDb("group", 2), recoveryEffectKey(1, 1, page), &effects)
		require.NoError(t, err)
		require.False(t, found)
	}
}
