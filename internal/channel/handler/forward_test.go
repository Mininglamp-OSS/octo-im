package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type forwardChannelCluster struct {
	icluster.ICluster
	leader uint64
}

func (c *forwardChannelCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) {
	return c.leader, nil
}

type forwardChannel struct {
	eventbus.IChannel
	events []*eventbus.Event
}

func (c *forwardChannel) AddEvent(_ string, _ uint8, e *eventbus.Event) {
	c.events = append(c.events, e)
}
func (c *forwardChannel) Advance(string, uint8) {}

func TestChannelForwardAdmissionFollowsAuthority(t *testing.T) {
	oldOptions, oldCluster, oldChannel := options.G, service.Cluster, eventbus.Channel
	t.Cleanup(func() { options.G, service.Cluster, eventbus.Channel = oldOptions, oldCluster, oldChannel })
	options.G = options.New()
	options.G.Cluster.NodeId = 3
	c := &forwardChannelCluster{leader: 4}
	service.Cluster = c
	sink := &forwardChannel{}
	eventbus.RegisterChannel(sink)
	e := &eventbus.Event{Type: eventbus.EventChannelOnSend, Conn: &eventbus.Conn{Uid: "u", NodeId: 1}, Frame: &wkproto.SendPacket{ClientMsgNo: "stable"}}
	req := &forwardChannelEventReq{channelId: "room", channelType: 2, events: eventbus.EventBatch{e}}
	body, err := req.encode()
	require.NoError(t, err)
	data, _, err := forward.Envelope(body, []*eventbus.Event{e})
	require.NoError(t, err)
	h := NewHandler()
	require.Equal(t, forward.StatusRetry, h.acceptForward(data))
	require.Empty(t, sink.events)
	c.leader = 3
	require.Equal(t, proto.StatusOK, h.acceptForward(data))
	require.Len(t, sink.events, 1)
	require.Greater(t, sink.events[0].ForwardDeadline, int64(0))
}

func TestChannelForwardAdmissionRejectsWrongFrameAndMixedBatch(t *testing.T) {
	oldOptions, oldCluster, oldChannel := options.G, service.Cluster, eventbus.Channel
	t.Cleanup(func() { options.G, service.Cluster, eventbus.Channel = oldOptions, oldCluster, oldChannel })
	options.G = options.New()
	options.G.Cluster.NodeId = 3
	service.Cluster = &forwardChannelCluster{leader: 3}
	eventbus.RegisterChannel(&forwardChannel{})
	h := NewHandler()
	conn := &eventbus.Conn{Uid: "u", NodeId: 1}

	for _, events := range []eventbus.EventBatch{
		{{Type: eventbus.EventChannelOnSend, Conn: conn, Frame: &wkproto.PingPacket{}}},
		{
			{Type: eventbus.EventChannelOnSend, Conn: conn, Frame: &wkproto.SendPacket{}},
			{Type: eventbus.EventChannelDistribute, Conn: conn, Frame: &wkproto.SendPacket{}},
		},
	} {
		req := &forwardChannelEventReq{channelId: "room", channelType: 2, events: events}
		body, err := req.encode()
		require.NoError(t, err)
		data, _, err := forward.Envelope(body, events)
		require.NoError(t, err)
		require.Equal(t, forward.StatusInvalid, h.acceptForward(data))
	}
}
