package wkdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecoveryInvariantErrorsArePermanent(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	var permanent *PermanentApplyError
	o := SubscriberOperation{OperationID: "join", ChannelID: "g", ChannelType: 2, Mode: "add", UIDs: []string{"u"}, ConversationIDs: []uint64{7}, CreatedAt: time.Now().UnixNano(), MaxPending: 10}
	require.NoError(t, db.ApplySubscriberOperation(0, 1, o))
	for _, c := range []SubscriberCheckpoint{
		{SlotID: 0, ChannelID: "g", ChannelType: 2, OperationID: "wrong", Version: 1},
		{SlotID: 0, ChannelID: "g", ChannelType: 2, OperationID: "join", Version: 1, Next: 2},
		{SlotID: 0, ChannelID: "g", ChannelType: 2, OperationID: "join", Version: 1, Next: 1, RetryAt: o.CreatedAt},
	} {
		require.ErrorAs(t, db.CheckpointSubscriberWork(c), &permanent)
	}
	e := ConversationEffect{UID: "u", ChannelID: "g", ChannelType: 2, Version: 1, ConversationID: 7, CreatedAt: o.CreatedAt}
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	e.ConversationID++
	require.ErrorAs(t, db.ApplyConversationEffects([]ConversationEffect{e}), &permanent)
	require.NoError(t, db.channelDb("g", 2).Set(recoverySlotKey(recoveryCounter, 0), []byte("corrupt JSON"), db.sync))
	require.ErrorAs(t, db.CheckpointSubscriberWork(SubscriberCheckpoint{SlotID: 0, ChannelID: "g", ChannelType: 2, OperationID: "join", Version: 1, Next: 1}), &permanent)
}
