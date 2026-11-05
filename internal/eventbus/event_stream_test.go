package eventbus

import (
	"bytes"
	"testing"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestForwardStreamEventPreservesPacketAndBatchBoundary(t *testing.T) {
	for _, size := range []int{1, 100, 127, 128, 16384} {
		p := &wkproto.SendPacket{Setting: wkproto.SettingStream, ClientMsgNo: "stream-key", ChannelID: "room", ChannelType: 2, Expire: 17, Payload: bytes.Repeat([]byte("x"), size)}
		original := EventBatch{&Event{Type: EventChannelDistribute, MessageId: 42, MessageSeq: 3, Frame: p}, &Event{Type: EventChannelWebhook, MessageId: 43, Frame: p}}
		data, err := original.Encode()
		require.NoError(t, err)
		var got EventBatch
		require.NoError(t, got.Decode(data))
		require.Len(t, got, 2)
		for i, e := range got {
			require.Equal(t, original[i].MessageId, e.MessageId)
			packet, ok := e.Frame.(*wkproto.SendPacket)
			require.True(t, ok)
			require.Equal(t, p.Setting, packet.Setting)
			require.Equal(t, p.Payload, packet.Payload)
			require.Equal(t, p.ClientMsgNo, packet.ClientMsgNo)
			require.Equal(t, p.Expire, packet.Expire)
		}
	}
}
