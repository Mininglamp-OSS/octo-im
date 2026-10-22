package channel

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// GF-8b: a ChannelId or ChannelType column that differs from the key is
// emitted as stored, survives the round trip and is counted.
func TestGetLogsEmitsKeyInconsistentColumnsAsStored(t *testing.T) {
	db := retryDB(t)
	st := newStorage(db, nil)
	key := wkutil.ChannelToKey("retry", 2)
	m := retryMessage(1, "a", "c")
	m.Framer = wkproto.Framer{FrameType: wkproto.RECV}
	m.MessageSeq = 1
	m.Term = 1
	m.ChannelID = "other"
	m.ChannelType = 9
	require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{m}))

	logs, err := st.GetLogs(key, 1, 2, 0)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), st.inconsistentChannelID.Load())
	require.Equal(t, uint64(1), st.inconsistentChannelType.Load())

	follower := newStorage(retryDB(t), nil)
	require.NoError(t, follower.AppendLogs(key, logs, nil))
	got, err := follower.db.LoadMsg("retry", 2, 1)
	require.NoError(t, err)
	require.Equal(t, "other", got.ChannelID)
	require.Equal(t, uint8(9), got.ChannelType)
}

// Every rejection is counted and returned; the error log for the same entry is
// rate-limited because each follower retry hits it again.
func TestGetLogsFormatErrorCountedAndRateLimited(t *testing.T) {
	db := retryDB(t)
	st := newStorage(db, nil)
	key := wkutil.ChannelToKey("retry", 2)
	m := retryMessage(1, "a", "c")
	m.Framer = wkproto.Framer{FrameType: wkproto.RECV}
	m.MessageSeq = 1
	m.Term = 1
	m.Topic = "topic" // without SettingTopic
	require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{m}))

	for i := 0; i < 3; i++ {
		logs, err := st.GetLogs(key, 1, 2, 0)
		require.Nil(t, logs)
		require.ErrorIs(t, err, wkdb.ErrChannelLogFormat)
	}
	require.Equal(t, uint64(3), st.formatErrorCount(wkdb.ChannelLogTopicWithoutSetting))
	st.formatLogMu.Lock()
	defer st.formatLogMu.Unlock()
	state := st.formatLogged[formatLogKey{key: key, index: 1, reason: wkdb.ChannelLogTopicWithoutSetting}]
	require.NotNil(t, state)
	require.Equal(t, uint64(2), state.suppressed)
}
