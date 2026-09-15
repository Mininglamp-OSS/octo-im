package wkdb_test

import (
	"context"
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMessageRetryLookupPreservesColumnsAndScope(t *testing.T) {
	d := newTestDB(t)
	require.NoError(t, d.Open())
	defer d.Close()
	m := wkdb.Message{RecvPacket: wkproto.RecvPacket{Framer: wkproto.Framer{RedDot: true, SyncOnce: true}, ChannelID: "a", ChannelType: 2, FromUID: "alice", ClientMsgNo: "key", MessageSeq: 1, MessageID: 10, StreamNo: "stream", Payload: []byte("hello")}}
	require.NoError(t, d.AppendMessages("a", 2, []wkdb.Message{m}))
	bob := m
	bob.FromUID = "bob"
	bob.MessageSeq = 2
	bob.MessageID = 11
	require.NoError(t, d.AppendMessages("a", 2, []wkdb.Message{bob}))
	other := m
	other.ChannelID = "b"
	other.MessageID = 12
	require.NoError(t, d.AppendMessages("b", 2, []wkdb.Message{other}))
	for _, want := range []wkdb.Message{m, bob, other} {
		got, err := d.LoadMsgBySenderClientMsgNo(context.Background(), want.ChannelID, 2, want.FromUID, "key")
		require.NoError(t, err)
		require.Equal(t, want.MessageID, got.MessageID)
		require.True(t, got.RedDot)
		require.True(t, got.SyncOnce)
		require.Equal(t, "stream", got.StreamNo)
		got, err = d.LoadMsg(want.ChannelID, 2, uint64(want.MessageSeq))
		require.NoError(t, err)
		require.True(t, got.RedDot)
		require.True(t, got.SyncOnce)
	}
	// Stale secondary indexes after truncation must not replay a replaced entry.
	require.NoError(t, d.TruncateLogTo("a", 2, 1))
	_, err := d.LoadMsgBySenderClientMsgNo(context.Background(), "a", 2, "bob", "key")
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	bob.MessageID = 13
	bob.ClientMsgNo = "replacement"
	require.NoError(t, d.AppendMessages("a", 2, []wkdb.Message{bob}))
	_, err = d.LoadMsgBySenderClientMsgNo(context.Background(), "a", 2, "bob", "key")
	require.ErrorIs(t, err, wkdb.ErrNotFound)
}

func TestMessageRetryLookupIntersectsSenderAndClientIndexes(t *testing.T) {
	d := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, d.Open())
	defer d.Close()
	// Legacy stores already contain both indexes. Cross-channel and
	// cross-sender rows must not prevent a valid lookup or a new sender.
	messages := make([]wkdb.Message, 1100)
	for i := range messages {
		messages[i] = wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: "pollution", ChannelType: 2, MessageSeq: uint32(i + 1), MessageID: int64(i + 1), FromUID: fmt.Sprintf("sender-%04d", i), ClientMsgNo: "counter-1", Payload: []byte("x")}}
	}
	require.NoError(t, d.AppendMessages("pollution", 2, messages))
	_, err := d.LoadMsgBySenderClientMsgNo(context.Background(), "target", 2, "alice", "counter-1")
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	_, err = d.LoadMsgBySenderClientMsgNo(context.Background(), "pollution", 2, "alice", "counter-1")
	require.ErrorIs(t, err, wkdb.ErrNotFound, "other senders cannot deny a new sender")
	// The last sender remains findable past 1024 same-number entries.
	// No backfill/version marker is needed for old databases.
	m, err := d.LoadMsgBySenderClientMsgNo(context.Background(), "pollution", 2, "sender-1099", "counter-1")
	require.NoError(t, err)
	require.Equal(t, int64(1100), m.MessageID)
}

func TestMessageRetryTruncateCannotEraseAppliedPrefix(t *testing.T) {
	d := newTestDB(t)
	require.NoError(t, d.Open())
	defer d.Close()
	messages := make([]wkdb.Message, 3)
	for i := range messages {
		messages[i] = wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: "committed", ChannelType: 2, MessageSeq: uint32(i + 1), MessageID: int64(i + 1), FromUID: "sender", ClientMsgNo: "key"}}
	}
	require.NoError(t, d.AppendMessages("committed", 2, messages))
	require.NoError(t, d.UpdateChannelAppliedIndex("committed", 2, 2))
	require.Error(t, d.TruncateLogTo("committed", 2, 1))
	tail, _, err := d.GetChannelLastMessageSeq("committed", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(3), tail)
	require.NoError(t, d.TruncateLogTo("committed", 2, 2))
	applied, err := d.GetChannelAppliedIndex("committed", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), applied)
}
