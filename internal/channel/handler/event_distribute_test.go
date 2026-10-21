package handler

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// committingChannel commits every proposal under its own id.
type committingChannel struct {
	icluster.Channel
}

func (committingChannel) ProposeBatchUntilAppliedTimeout(_ context.Context, _ string, _ uint8, reqs rafttypes.ProposeReqSet) (rafttypes.ProposeRespSet, error) {
	resps := make(rafttypes.ProposeRespSet, 0, len(reqs))
	for i, req := range reqs {
		resps = append(resps, &rafttypes.ProposeResp{Id: req.Id, Index: uint64(i + 1), CanonicalID: req.Id})
	}
	return resps, nil
}

type noPlugins struct{ service.IPluginManager }

func (noPlugins) Plugins(...types.PluginMethod) []types.Plugin { return nil }

type capturedChannel struct {
	events []*eventbus.Event
}

func (c *capturedChannel) AddEvent(_ string, _ uint8, e *eventbus.Event) {
	c.events = append(c.events, e)
}
func (c *capturedChannel) Advance(string, uint8) {}

func (c *capturedChannel) ofType(t eventbus.EventType) []*eventbus.Event {
	var out []*eventbus.Event
	for _, e := range c.events {
		if e.Type == t {
			out = append(out, e)
		}
	}
	return out
}

func forwardedEvents(t *testing.T, msgs []*proto.Message) []*eventbus.Event {
	t.Helper()
	var out []*eventbus.Event
	for _, m := range msgs {
		if m.MsgType != uint32(msgForwardChannelEvent) {
			continue
		}
		req := &forwardChannelEventReq{}
		require.NoError(t, req.decode(m.Content))
		out = append(out, req.events...)
	}
	return out
}

// An API send accepted on node 1 is persisted on slot leader 2. The leader
// must still distribute it to node 1, which owns the recipient, together with
// an unrelated message in the same batch.
func TestSlotLeaderDistributesAPISendToAcceptingNode(t *testing.T) {
	c := setupSendResult(t, 2)
	oldStore, oldPlugins, oldChannel, oldPresence := service.Store, service.PluginManager, eventbus.Channel, service.Presence
	t.Cleanup(func() {
		service.Store, service.PluginManager, eventbus.Channel, service.Presence = oldStore, oldPlugins, oldChannel, oldPresence
	})
	service.Store = store.New(store.NewOptions(store.WithChannel(committingChannel{})))
	service.PluginManager = noPlugins{}
	service.Presence = nil
	options.G.Webhook.HTTPAddr = "http://127.0.0.1:1"
	bus := &capturedChannel{}
	eventbus.RegisterChannel(bus)

	api := apiSendEvent(101, 1)
	client := &eventbus.Event{
		Type:       eventbus.EventChannelOnSend,
		Conn:       &eventbus.Conn{Uid: "carol", DeviceId: "phone", NodeId: 2},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "other"},
		MessageId:  102,
		ReasonCode: wkproto.ReasonSuccess,
	}
	h := NewHandler()
	h.persist(&eventbus.ChannelContext{ChannelId: "group", ChannelType: wkproto.ChannelTypeGroup, SlotLeaderId: 2, Events: []*eventbus.Event{api, client}})

	// The accepting node still receives the persistence result.
	require.Len(t, c.sent[1], 1)
	require.Equal(t, uint32(msgSendResult), c.sent[1][0].MsgType)
	c.sent = nil

	distribute := bus.ofType(eventbus.EventChannelDistribute)
	require.Len(t, distribute, 2)
	tag := &types.Tag{Key: "tag", Nodes: []*types.Node{{LeaderId: 1, Uids: []string{"bob"}}}}
	h.distributeByTag(2, tag, "group", wkproto.ChannelTypeGroup, distribute)

	got := forwardedEvents(t, c.sent[1])
	require.Len(t, got, 2, "both messages of the batch reach the recipient's node")
	require.ElementsMatch(t, []int64{101, 102}, []int64{got[0].MessageId, got[1].MessageId})

	webhook := bus.ofType(eventbus.EventChannelWebhook)
	require.Len(t, webhook, 2)
	for _, e := range append(distribute, webhook...) {
		require.Zero(t, e.SourceNodeId, "send-result provenance must not leak into delivery")
	}
}

// Clearing API provenance must not disable the existing guard for an event
// that really originates from the target node.
func TestDistributeToNodeKeepsSourceNodeGuard(t *testing.T) {
	c := setupSendResult(t, 2)
	h := NewHandler()
	e := &eventbus.Event{Type: eventbus.EventChannelDistribute, Conn: &eventbus.Conn{Uid: "carol"}, Frame: &wkproto.SendPacket{}, SourceNodeId: 1}
	h.distributeToNode(1, "group", wkproto.ChannelTypeGroup, []*eventbus.Event{e})
	require.Empty(t, c.sent[1])

	e.SourceNodeId = 0
	h.distributeToNode(1, "group", wkproto.ChannelTypeGroup, []*eventbus.Event{e})
	require.Len(t, forwardedEvents(t, c.sent[1]), 1)
}
