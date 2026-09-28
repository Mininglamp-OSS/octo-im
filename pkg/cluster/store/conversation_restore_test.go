package store

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestRecoveryEffectReceiverRejectsOversizedPageAtBothLayers(t *testing.T) {
	s, slots := recoveryStore(t)
	page := make([]wkdb.ConversationEffect, wkdb.MaxConversationEffects+1)
	for i := range page {
		page[i] = wkdb.ConversationEffect{UID: "u", ChannelID: "g", ChannelType: 2, Version: 1, ConversationID: 7, CreatedAt: time.Now().UnixNano()}
	}
	var permanent *PermanentApplyError
	require.ErrorAs(t, s.DB().ApplyConversationEffects(page), &permanent)
	data, err := json.Marshal(page)
	require.NoError(t, err)
	require.ErrorAs(t, s.applySubscriberRecovery(slots.GetSlotId("u"), NewCMD(CMDConversationEffects, data), 1), &permanent)
}

func TestRecoveryUserDeletedConversationCanBeRecreatedWithoutRevivingMembership(t *testing.T) {
	s, _ := recoveryStore(t)
	effect := wkdb.ConversationEffect{UID: "u", ChannelID: "g", ChannelType: 2, Version: 1, ConversationID: 7, ReadToMsgSeq: 10, CreatedAt: time.Now().UnixNano()}
	require.NoError(t, s.DB().ApplyConversationEffects([]wkdb.ConversationEffect{effect}))
	require.NoError(t, s.DB().DeleteConversation("u", "g", 2))
	// The unread API supplies no ID when its read returns a missing row.
	require.NoError(t, s.AddOrUpdateUserConversations("u", []wkdb.Conversation{{Uid: "u", ChannelId: "g", ChannelType: 2, Type: wkdb.ConversationTypeChat, ReadToMsgSeq: 20}}))
	got, err := s.DB().GetConversation("u", "g", 2)
	require.NoError(t, err)
	require.EqualValues(t, 7, got.Id)
	require.EqualValues(t, 20, got.ReadToMsgSeq)
	require.NoError(t, s.DB().DeleteConversation("u", "g", 2))
	// The message/read-position update path must also restore an active row.
	require.NoError(t, s.DB().UpdateConversationIfSeqGreater("u", "g", 2, 30))
	got, err = s.DB().GetConversation("u", "g", 2)
	require.NoError(t, err)
	require.EqualValues(t, 7, got.Id)
	require.EqualValues(t, 30, got.ReadToMsgSeq)
	require.NoError(t, s.AddOrUpdateUserConversations("u", []wkdb.Conversation{{Uid: "u", Id: 999, ChannelId: "g", ChannelType: 2, ReadToMsgSeq: 500}}))
	got, err = s.DB().GetConversation("u", "g", 2)
	require.NoError(t, err)
	require.EqualValues(t, 30, got.ReadToMsgSeq, "explicit stale IDs must still be dropped")
	effect.Version, effect.Deleted, effect.ConversationID = 2, true, 0
	require.NoError(t, s.DB().ApplyConversationEffects([]wkdb.ConversationEffect{effect}))
	require.NoError(t, s.DB().UpdateConversationIfSeqGreater("u", "g", 2, 1000))
	require.NoError(t, s.AddOrUpdateUserConversations("u", []wkdb.Conversation{{Uid: "u", ChannelId: "g", ChannelType: 2, ReadToMsgSeq: 1000}}))
	_, err = s.DB().GetConversation("u", "g", 2)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
}
