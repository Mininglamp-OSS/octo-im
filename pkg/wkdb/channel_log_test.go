package wkdb_test

import (
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// channelLogEntry is the common fixture entry: Header 0x50, Setting 0x02,
// Expire 0, MessageId 1, MessageSeq 7, ClientMsgNo "c", StreamNo "s1",
// Timestamp 1, ChannelId "g", ChannelType 2, Topic "", FromUid "u",
// Payload "hi", Term 3.
func channelLogEntry() wkdb.Message {
	return wkdb.Message{
		RecvPacket: wkproto.RecvPacket{
			Framer:      wkproto.Framer{FrameType: wkproto.RECV},
			Setting:     wkproto.SettingStream,
			MessageID:   1,
			MessageSeq:  7,
			ClientMsgNo: "c",
			StreamNo:    "s1",
			Timestamp:   1,
			ChannelID:   "g",
			ChannelType: 2,
			FromUID:     "u",
			Payload:     []byte("hi"),
		},
		Term: 3,
	}
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.NewReplacer(" ", "", "|", "", "\n", "").Replace(s))
	require.NoError(t, err)
	return b
}

const (
	// GV-1: the stream carrier emission (frame v4), 64 bytes.
	gv1 = "68 00 00000032 | 50 30 | 02 0000 0001 75 0001 67 02 | 00000000 | 0001 63 | 00 0002 7331 0000000000000000 | 0000000000000001 00000007 00000001 | 6869 | 0000000000000003"
	// GV-0: the frame v5 emission of the same columns, 51 bytes. StreamNo is lost.
	gv0 = "69 00 00000025 | 50 23 | 02 0000 000175 000167 02 00000000 000163 | 0000000000000001 00000007 00000001 6869 | 0000000000000003"
)

// legacyChannelLogData frames m at version in the old (version <= 100,
// int16-prefixed frame) or new (version+100, data version, u32 length) wrapper.
func legacyChannelLogData(t *testing.T, m wkdb.Message, version uint8, oldWrapper bool) []byte {
	t.Helper()
	frame, err := wkproto.New().EncodeFrame(&m.RecvPacket, version)
	require.NoError(t, err)
	enc := wkproto.NewEncoder()
	defer enc.End()
	if oldWrapper {
		enc.WriteUint8(version)
		enc.WriteBinary(frame)
	} else {
		enc.WriteUint8(version + 100)
		enc.WriteUint8(0)
		enc.WriteUint32(uint32(len(frame)))
		enc.WriteBytes(frame)
	}
	enc.WriteUint64(m.Term)
	return enc.Bytes()
}

func TestChannelLogGoldenVectors(t *testing.T) {
	m := channelLogEntry()

	got, err := wkdb.EncodeChannelLogData(&m)
	require.NoError(t, err)
	require.Equal(t, mustHex(t, gv1), got, "GV-1")
	require.Len(t, got, 64)

	v5, err := m.Marshal()
	require.NoError(t, err)
	require.Equal(t, mustHex(t, gv0), v5, "GV-0")
	require.Len(t, v5, 51)

	// GV-2: the v2 original has RemainingLength 44 (no Expire).
	v2 := legacyChannelLogData(t, m, 2, false)
	require.Equal(t, byte(0x2C), v2[7], "GV-2 RemainingLength")

	// Budget values: 35 at v5, 48 at the carrier, difference 11+len("s1").
	require.Equal(t, 35, m.Size())
	require.Equal(t, 48, wkdb.ChannelLogBodySize(&m))
	require.Equal(t, uint8(4), wkdb.ChannelLogEmitVersion(&m))
	require.Equal(t, 1, wkdb.ChannelLogEmitter)
}

// GF-1 and GF-7 (current decoder): every legacy framing of the entry decodes
// to the same message, and the emission of that message is GV-1, which
// decodes back to the same message.
func TestChannelLogLegacyFramingsEmitGV1(t *testing.T) {
	want := channelLogEntry()
	for _, version := range []uint8{2, 3, 4} {
		for _, old := range []bool{true, false} {
			var m wkdb.Message
			require.NoError(t, m.Unmarshal(legacyChannelLogData(t, want, version, old)))
			require.Equal(t, "s1", m.StreamNo, "v%d old=%v", version, old)
			got, err := wkdb.EncodeChannelLogData(&m)
			require.NoError(t, err)
			require.Equal(t, mustHex(t, gv1), got, "v%d old=%v", version, old)
		}
	}
	var back wkdb.Message
	require.NoError(t, back.Unmarshal(mustHex(t, gv1)))
	require.Equal(t, want.RecvPacket.StreamNo, back.StreamNo)
	require.Equal(t, want.Setting, back.Setting)
	require.Equal(t, want.ClientMsgNo, back.ClientMsgNo)
	require.Equal(t, want.Payload, back.Payload)
	require.Equal(t, want.Term, back.Term)
	require.Equal(t, want.MessageSeq, back.MessageSeq)
}

// GF-4 and GF-10: for StreamNo == "" the emission equals Marshal byte for
// byte; with StreamNo set it round-trips every emitted field and is a fixed point.
func TestChannelLogFlagMatrix(t *testing.T) {
	for header := 0; header < 16; header++ {
		for _, topic := range []bool{false, true} {
			for _, expire := range []uint32{0, 7} {
				for _, stream := range []string{"", "s1"} {
					m := channelLogEntry()
					m.StreamNo = stream
					m.Framer = wkproto.FramerFromUint8(uint8(0x50 | header))
					m.Expire = expire
					if topic {
						m.Topic = "t"
						m.Setting.Set(wkproto.SettingTopic)
					}
					got, err := wkdb.EncodeChannelLogData(&m)
					require.NoError(t, err)
					if stream == "" {
						v5, err := m.Marshal()
						require.NoError(t, err)
						require.Equal(t, v5, got)
						continue
					}
					var back wkdb.Message
					require.NoError(t, back.Unmarshal(got))
					require.Equal(t, wkproto.ToFixHeaderUint8(&m.RecvPacket), wkproto.ToFixHeaderUint8(&back.RecvPacket))
					require.Equal(t, m.Setting, back.Setting)
					require.Equal(t, m.Expire, back.Expire)
					require.Equal(t, m.Topic, back.Topic)
					require.Equal(t, m.StreamNo, back.StreamNo)
					again, err := wkdb.EncodeChannelLogData(&back)
					require.NoError(t, err)
					require.Equal(t, got, again, "second hop")
				}
			}
		}
	}

	// SettingStream with an empty StreamNo stays at v5.
	m := channelLogEntry()
	m.StreamNo = ""
	got, err := wkdb.EncodeChannelLogData(&m)
	require.NoError(t, err)
	v5, err := m.Marshal()
	require.NoError(t, err)
	require.Equal(t, v5, got)
	require.Equal(t, m.Size(), wkdb.ChannelLogBodySize(&m))
}

// GF-8: every rejection returns the matching reason, emits nothing and does
// not panic.
func TestChannelLogRejectsLossyInput(t *testing.T) {
	long := strings.Repeat("x", math.MaxInt16+1)
	cases := map[string]func(m *wkdb.Message){
		wkdb.ChannelLogHeaderNotRecv:          func(m *wkdb.Message) { m.Framer = wkproto.FramerFromUint8(0x30) },
		wkdb.ChannelLogStreamNoWithoutSetting: func(m *wkdb.Message) { m.Setting = 0 },
		wkdb.ChannelLogTopicWithoutSetting:    func(m *wkdb.Message) { m.Topic = "t" },
		wkdb.ChannelLogFrameTooLarge:          func(m *wkdb.Message) { m.Payload = make([]byte, wkproto.MaxRemaingLength) },
		"string_too_long/FromUID":             func(m *wkdb.Message) { m.FromUID = long },
		"string_too_long/ChannelID":           func(m *wkdb.Message) { m.ChannelID = long },
		"string_too_long/ClientMsgNo":         func(m *wkdb.Message) { m.ClientMsgNo = long },
		"string_too_long/StreamNo":            func(m *wkdb.Message) { m.StreamNo = long },
		"string_too_long/Topic":               func(m *wkdb.Message) { m.Topic = long; m.Setting.Set(wkproto.SettingTopic) },
		"string_too_long/MsgKey":              func(m *wkdb.Message) { m.MsgKey = long },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			m := channelLogEntry()
			mutate(&m)
			var got []byte
			var err error
			require.NotPanics(t, func() { got, err = wkdb.EncodeChannelLogData(&m) })
			require.Nil(t, got)
			require.ErrorIs(t, err, wkdb.ErrChannelLogFormat)
			var fe *wkdb.ChannelLogFormatError
			require.True(t, errors.As(err, &fe))
			require.Equal(t, strings.Split(name, "/")[0], fe.Reason)
			require.Equal(t, uint64(7), fe.Index)
		})
	}

	// Non-stream frame_too_large: a v2-shaped entry whose v5 body exceeds
	// MaxRemaingLength because v5 adds Expire. Today the leader emits it and
	// the follower fails to decode it; now the leader rejects it.
	m := channelLogEntry()
	m.StreamNo = ""
	m.Setting = 0
	m.Payload = nil
	m.Payload = make([]byte, int(wkproto.MaxRemaingLength)-m.SizeWithProtoVersion(2))
	require.Equal(t, int(wkproto.MaxRemaingLength), m.SizeWithProtoVersion(2))
	require.Equal(t, int(wkproto.MaxRemaingLength)+4, m.Size())
	today, err := m.Marshal()
	require.NoError(t, err, "Marshal does not enforce MaxRemaingLength")
	var follower wkdb.Message
	require.Error(t, follower.Unmarshal(today), "the follower rejects today's emission")
	_, err = wkdb.EncodeChannelLogData(&m)
	var fe *wkdb.ChannelLogFormatError
	require.True(t, errors.As(err, &fe))
	require.Equal(t, wkdb.ChannelLogFrameTooLarge, fe.Reason)
}
