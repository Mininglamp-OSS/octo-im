package wkdb_test

import (
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
		got, err := d.LoadMsgBySenderClientMsgNo(want.ChannelID, 2, want.FromUID, "key")
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
	_, err := d.LoadMsgBySenderClientMsgNo("a", 2, "bob", "key")
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	bob.MessageID = 13
	bob.ClientMsgNo = "replacement"
	require.NoError(t, d.AppendMessages("a", 2, []wkdb.Message{bob}))
	_, err = d.LoadMsgBySenderClientMsgNo("a", 2, "bob", "key")
	require.ErrorIs(t, err, wkdb.ErrNotFound)
}
