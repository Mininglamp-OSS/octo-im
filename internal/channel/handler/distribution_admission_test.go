package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type snapshotAdmission interface {
	acceptDistribution([]byte) proto.Status
}

// distributionTestBody encodes an immutable per-recipient-node version envelope.
func distributionTestBody(t *testing.T, target uint64, key string, ids ...int64) []byte {
	t.Helper()
	events := eventbus.EventBatch{}
	for _, id := range ids {
		events = append(events, &eventbus.Event{
			Type: eventbus.EventChannelDistribute, MessageId: id, TagKey: key,
			Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{},
		})
	}
	data, err := events.Encode()
	require.NoError(t, err)
	e := wkproto.NewEncoder()
	e.WriteUint8(1)
	e.WriteUint64(1)
	e.WriteUint64(target)
	e.WriteString("channel")
	e.WriteUint8(wkproto.ChannelTypeData)
	e.WriteString(key)
	e.WriteUint32(1)
	e.WriteString("fixed")
	e.WriteBinary(data)
	return append([]byte(nil), e.Bytes()...)
}

func TestDistributionAdmissionDeduplicatesLostAcknowledgement(t *testing.T) {
	versionTestFixture(t)
	old := eventbus.Channel
	t.Cleanup(func() { eventbus.Channel = old })
	sink := &forwardChannel{}
	eventbus.RegisterChannel(sink)
	a, ok := any(NewHandler()).(snapshotAdmission)
	require.True(t, ok, "distribution requires acknowledged immutable-snapshot admission")
	body := distributionTestBody(t, 2, "A", 101, 102)
	require.Equal(t, proto.StatusOK, a.acceptDistribution(body))
	require.Equal(t, proto.StatusOK, a.acceptDistribution(body), "replay after lost response must be harmless")
	require.Len(t, sink.events, 2)
	require.Equal(t, int64(101), sink.events[0].MessageId)
	require.Equal(t, int64(102), sink.events[1].MessageId)
	NewHandler().distributeCommon(&eventbus.ChannelContext{
		ChannelId: "channel", ChannelType: wkproto.ChannelTypeData, SlotLeaderId: 1,
		Events: sink.events,
	})
}

func TestDistributionAdmissionRejectsWrongTargetAndVersionConflict(t *testing.T) {
	versionTestFixture(t)
	old := eventbus.Channel
	t.Cleanup(func() { eventbus.Channel = old })
	sink := &forwardChannel{}
	eventbus.RegisterChannel(sink)
	a, ok := any(NewHandler()).(snapshotAdmission)
	require.True(t, ok, "distribution requires acknowledged snapshot admission")
	require.NotEqual(t, proto.StatusOK, a.acceptDistribution(distributionTestBody(t, 3, "A", 101)))
	require.Empty(t, sink.events)
	require.Equal(t, proto.StatusOK, a.acceptDistribution(distributionTestBody(t, 2, "A", 101)))
	require.NotEqual(t, proto.StatusOK, a.acceptDistribution(distributionTestBody(t, 2, "B", 101)))
	require.Len(t, sink.events, 1)
}
