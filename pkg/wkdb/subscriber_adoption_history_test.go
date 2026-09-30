package wkdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSubscriberSnapshotAdoptionPreservesBusinessConversationDeletion(t *testing.T) {
	for _, individual := range []bool{false, true} {
		name := "batch"
		if individual {
			name = "individual"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			db := recoveryTestDB(t, dir)
			defer func() { require.NoError(t, db.Close()) }()
			at := time.Now().Add(-time.Hour)
			_, err := db.AddChannel(ChannelInfo{ChannelId: "group", ChannelType: 2, CreatedAt: &at, UpdatedAt: &at})
			require.NoError(t, err)
			require.NoError(t, db.AddSubscribers("group", 2, []Member{{Uid: "a", CreatedAt: &at, UpdatedAt: &at}}))
			require.NoError(t, db.AddOrUpdateConversations([]Conversation{{Id: 44, Uid: "a", ChannelId: "group", ChannelType: 2, Type: ConversationTypeChat, ReadToMsgSeq: 3, UnreadCount: 5, CreatedAt: &at, UpdatedAt: &at}}))
			// POST /conversations/delete uses this method, NOT DeleteConversation.
			require.NoError(t, db.UpdateConversationDeletedAtMsgSeq("a", "group", 2, 20))
			before, err := db.GetConversation("a", "group", 2)
			require.NoError(t, err)
			first := snapshotOperation(1, "adopt-business", "a")
			first.ReadToMsgSeq = 50
			require.NoError(t, db.ApplySubscriberOperation(1, 1, first))
			work := recoveryWork(t, db, first)
			if individual {
				for _, effect := range work.Effects {
					require.NoError(t, db.applyConversationEffect(effect))
				}
			} else {
				require.NoError(t, db.ApplyConversationEffects(work.Effects))
			}
			completeRecoveryWork(t, db, work)
			second := snapshotOperation(2, "next-business", "a", "b")
			require.NoError(t, db.ApplySubscriberOperation(1, 2, second))
			applySnapshotWork(t, db, second)
			got, err := db.GetConversation("a", "group", 2)
			require.NoError(t, err)
			require.Equal(t, before.DeletedAtMsgSeq, got.DeletedAtMsgSeq)
			require.Equal(t, before.ReadToMsgSeq, got.ReadToMsgSeq)
			require.Equal(t, before.UnreadCount, got.UnreadCount)
			require.Equal(t, before.CreatedAt.UnixNano(), got.CreatedAt.UnixNano())
			require.NoError(t, db.Close())
			db = reopenRecoveryTestDB(t, dir)
			got, err = db.GetConversation("a", "group", 2)
			require.NoError(t, err)
			require.EqualValues(t, 20, got.DeletedAtMsgSeq)
			// Later activity keeps the history deletion boundary instead of exposing it.
			require.NoError(t, db.UpdateConversationIfSeqGreater("a", "group", 2, 51))
			got, err = db.GetConversation("a", "group", 2)
			require.NoError(t, err)
			require.EqualValues(t, 51, got.ReadToMsgSeq)
			require.EqualValues(t, 20, got.DeletedAtMsgSeq)
		})
	}
}
