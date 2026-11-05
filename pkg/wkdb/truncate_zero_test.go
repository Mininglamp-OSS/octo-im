package wkdb_test

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChannelTruncatesOnlyUnappliedLogToZero(t *testing.T) {
	d := newTestDB(t)
	require.NoError(t, d.Open())
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	logs := []wkdb.Message{{RecvPacket: wkproto.RecvPacket{ChannelID: "zero-prefix", ChannelType: 2, MessageSeq: 1, MessageID: 1, Payload: []byte("old")}}}
	require.NoError(t, d.AppendMessages("zero-prefix", 2, logs))
	require.NoError(t, d.TruncateLogTo("zero-prefix", 2, 0))
	seq, _, err := d.GetChannelLastMessageSeq("zero-prefix", 2)
	require.NoError(t, err)
	require.Zero(t, seq)
	remaining, err := d.LoadNextRangeMsgs("zero-prefix", 2, 1, 0, 10)
	require.NoError(t, err)
	require.Empty(t, remaining)
	require.NoError(t, d.AppendMessages("zero-prefix", 2, logs))
	require.NoError(t, d.UpdateChannelAppliedIndex("zero-prefix", 2, 1))
	require.Error(t, d.TruncateLogTo("zero-prefix", 2, 0))
	seq, _, err = d.GetChannelLastMessageSeq("zero-prefix", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)
}
