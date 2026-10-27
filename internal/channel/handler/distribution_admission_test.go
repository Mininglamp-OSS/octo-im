package handler

import (
	"testing"
	"time"

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
	m, p := versionTestFixture(t)
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
	m.RemoveTag("A") // Queue ownership must outlive cache eviction.
	NewHandler().distributeCommon(&eventbus.ChannelContext{
		ChannelId: "channel", ChannelType: wkproto.ChannelTypeData, SlotLeaderId: 1,
		Events: sink.events,
	})
	require.Len(t, p.events, 2)
	for _, event := range p.events {
		require.Equal(t, "fixed", event.ToUid)
	}
}

func TestDistributionAdmissionBoundsReceiptsAndExpiresThem(t *testing.T) {
	versionTestFixture(t)
	old := eventbus.Channel
	t.Cleanup(func() { eventbus.Channel = old })
	sink := &forwardChannel{}
	eventbus.RegisterChannel(sink)
	h := NewHandler()
	h.distributionLimit = 2
	now := time.Now()
	h.distributionNow = func() time.Time { return now }
	require.Equal(t, proto.StatusOK, h.acceptDistribution(distributionTestBody(t, 2, "A", 101, 102)))
	require.NotEqual(t, proto.StatusOK, h.acceptDistribution(distributionTestBody(t, 2, "A", 103)))
	require.Len(t, sink.events, 2, "capacity rejection must not partially enqueue")
	require.Len(t, h.distributionReceipts, 2)
	now = now.Add(distributionAdmissionTTL)
	require.Equal(t, proto.StatusOK, h.acceptDistribution(distributionTestBody(t, 2, "A", 103)))
	require.Len(t, h.distributionReceipts, 1)
	require.Len(t, sink.events, 3)
}

func TestDistributionAdmissionBackpressuresUntilSnapshotReleased(t *testing.T) {
	versionTestFixture(t)
	old := eventbus.Channel
	t.Cleanup(func() { eventbus.Channel = old })
	sink := &forwardChannel{}
	eventbus.RegisterChannel(sink)
	h := NewHandler()
	h.distributionByteLimit = 1024
	require.Equal(t, proto.StatusOK, h.acceptDistribution(distributionTestBody(t, 2, "A", 101)))
	require.NotEqual(t, proto.StatusOK, h.acceptDistribution(distributionTestBody(t, 2, "A", 102)))
	require.Len(t, sink.events, 1)
	h.distributeCommon(&eventbus.ChannelContext{ChannelId: "channel", ChannelType: wkproto.ChannelTypeData, SlotLeaderId: 1, Events: sink.events})
	require.Equal(t, proto.StatusOK, h.acceptDistribution(distributionTestBody(t, 2, "A", 102)), "finishing admitted work must release snapshot capacity")
	require.Len(t, sink.events, 2)
}

func TestDistributionAdmissionRejectsTruncation(t *testing.T) {
	versionTestFixture(t)
	old := eventbus.Channel
	t.Cleanup(func() { eventbus.Channel = old })
	sink := &forwardChannel{}
	eventbus.RegisterChannel(sink)
	h := NewHandler()
	body := distributionTestBody(t, 2, "A", 101)
	for n := 0; n < len(body); n++ {
		require.NotEqual(t, proto.StatusOK, h.acceptDistribution(body[:n]), "length=%d", n)
	}
	require.NotEqual(t, proto.StatusOK, h.acceptDistribution(append(body, 1)))
	require.Empty(t, sink.events)
}

func TestDistributionAdmissionPreservesDistinctStreamEvents(t *testing.T) {
	versionTestFixture(t)
	old := eventbus.Channel
	t.Cleanup(func() { eventbus.Channel = old })
	sink := &forwardChannel{}
	eventbus.RegisterChannel(sink)
	r := &distributionRequest{source: 1, target: 2, channel: "channel", channelType: wkproto.ChannelTypeData, key: "A", uids: []string{"fixed"}}
	for _, id := range []string{"delta-1", "delta-2"} {
		r.events = append(r.events, &eventbus.Event{Type: eventbus.EventChannelDistribute, TagKey: "A", MessageId: 0,
			Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.EventPacket{Id: id, Type: "stream.delta", Data: []byte("fragment")}})
	}
	body, err := r.encode()
	require.NoError(t, err)
	h := NewHandler()
	require.Equal(t, proto.StatusOK, h.acceptDistribution(body))
	require.Equal(t, proto.StatusOK, h.acceptDistribution(body))
	require.Len(t, sink.events, 2, "two deltas of the same parent message are distinct, replay is not")
}

func TestDistributionAdmissionRejectsWrongTargetAndCoalescesVersionRetry(t *testing.T) {
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
	require.Equal(t, proto.StatusOK, a.acceptDistribution(distributionTestBody(t, 2, "B", 101)))
	require.Len(t, sink.events, 1)
}
