package api

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestCommittedMessageReceiptRequiresAppliedSenderScopedOriginal(t *testing.T) {
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	msg := wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: "group", ChannelType: 2, FromUID: "bot", ClientMsgNo: "retry", MessageSeq: 1, MessageID: 123, Payload: []byte("answer")}}
	require.NoError(t, db.AppendMessages("group", 2, []wkdb.Message{msg}))
	_, err := committedMessageReceipt(context.Background(), db, "group", 2, "bot", "retry")
	require.ErrorIs(t, err, wkdb.ErrNotFound, "stored but uncommitted is not delivered")
	require.NoError(t, db.UpdateChannelAppliedIndex("group", 2, 1))
	got, err := committedMessageReceipt(context.Background(), db, "group", 2, "bot", "retry")
	require.NoError(t, err)
	require.Equal(t, msg.MessageID, got.MessageID)
	_, err = committedMessageReceipt(context.Background(), db, "group", 2, "other", "retry")
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	_, err = committedMessageReceipt(context.Background(), db, "elsewhere", 2, "bot", "retry")
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	msg.MessageSeq, msg.MessageID, msg.ClientMsgNo = 2, 124, "next"
	require.NoError(t, db.AppendMessages("group", 2, []wkdb.Message{msg}))
	_, err = committedMessageReceipt(context.Background(), db, "group", 2, "bot", "next")
	require.ErrorIs(t, err, wkdb.ErrNotFound, "the unapplied suffix must not be acknowledged")
}
