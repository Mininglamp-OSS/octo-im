package wkdb_test

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// GF-11: GetLogs batches are budgeted at the size they are emitted with. A
// stored StreamNo is emitted in a frame v4 body, which is 11+len(StreamNo)
// bytes larger than the v5 body, so it must be counted.
func TestLoadNextRangeMsgsForSizeBudgetsEmittedStreamNo(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Open())
	defer func() { require.NoError(t, db.Close()) }()

	const n = 64
	for _, stream := range []bool{true, false} {
		channelID := fmt.Sprintf("budget-%v", stream)
		msgs := make([]wkdb.Message, 0, n)
		for i := 1; i <= n; i++ {
			m := wkdb.Message{RecvPacket: wkproto.RecvPacket{
				Framer: wkproto.Framer{FrameType: wkproto.RECV}, MessageID: int64(i), MessageSeq: uint32(i),
				ClientMsgNo: "c", ChannelID: channelID, ChannelType: 2, FromUID: "u", Payload: []byte("hi"), Timestamp: 1,
			}, Term: 1}
			if stream {
				m.Setting = wkproto.SettingStream
				m.StreamNo = fmt.Sprintf("stream-%04d", i)
			}
			msgs = append(msgs, m)
		}
		require.NoError(t, db.AppendMessages(channelID, 2, msgs))

		emitted := func(m *wkdb.Message) uint64 {
			if m.StreamNo != "" {
				return uint64(m.SizeWithProtoVersion(4))
			}
			return uint64(m.Size())
		}
		maxEntry := emitted(&msgs[0])
		for _, limit := range []uint64{1, 200, 480, 1000, 2048} {
			start := uint64(1)
			var batches []int
			for start <= n {
				got, err := db.LoadNextRangeMsgsForSize(channelID, 2, start, n+1, limit)
				require.NoError(t, err)
				require.NotEmpty(t, got)
				var sum uint64
				for i := range got {
					sum += emitted(&got[i])
				}
				require.Less(t, sum, limit+maxEntry, "stream=%v limit=%d start=%d: %d entries", stream, limit, start, len(got))
				batches = append(batches, len(got))
				start += uint64(len(got))
			}
			if !stream {
				// Without StreamNo the budget is m.Size(), so boundaries are unchanged.
				size := uint64(msgs[0].Size())
				per := int((limit + size - 1) / size)
				for i, b := range batches[:len(batches)-1] {
					require.Equal(t, per, b, "limit=%d batch %d", limit, i)
				}
			}
		}
	}
}
