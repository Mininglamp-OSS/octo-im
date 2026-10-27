package channeltest_test

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel/channeltest"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// SK2-R5 (GF-8 through the storage): a stored message that cannot be emitted
// without loss fails the whole GetLogs call with the format error and no logs.
func TestGetLogsFailsClosedOnLossyStoredMessage(t *testing.T) {
	db := openDisk(t)
	st := channeltest.NewStorage(db)
	require.NoError(t, st.AppendLogs(testKey, legacyLogs(t, 4), nil))
	// A StreamNo without SettingStream cannot come from the decoder, so write
	// it with the column writer directly.
	bad := wkdb.Message{RecvPacket: wkproto.RecvPacket{
		Framer: wkproto.Framer{FrameType: wkproto.RECV}, MessageID: 5, MessageSeq: 5, ChannelID: testChannel,
		ChannelType: testType, FromUID: "u", StreamNo: "lost", Payload: []byte("p"),
	}, Term: 1}
	require.NoError(t, db.AppendMessages(testChannel, testType, []wkdb.Message{bad}))
	logs, err := st.GetLogs(testKey, 1, 6, 0)
	require.Nil(t, logs)
	require.ErrorIs(t, err, wkdb.ErrChannelLogFormat)
	var fe *wkdb.ChannelLogFormatError
	require.ErrorAs(t, err, &fe)
	require.Equal(t, testKey, fe.Key)
	require.Equal(t, uint64(5), fe.Index)
	require.Equal(t, wkdb.ChannelLogStreamNoWithoutSetting, fe.Reason)
	// The entries before it are still served on their own.
	logs, err = st.GetLogs(testKey, 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, logs, 4)
}
