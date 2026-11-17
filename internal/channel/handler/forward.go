package handler

import (
	"strings"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func (h *Handler) SetForwardRoutes() {
	cl := h.cluster()
	if cl == nil {
		return
	}
	cl.Route(forward.ChannelPath, func(c *wkserver.Context) { c.WriteStatus(h.acceptForward(c.Body())) })
}

func (h *Handler) AcceptForward(data []byte) proto.Status {
	return h.acceptForward(data)
}

func (h *Handler) acceptForward(data []byte) proto.Status {
	body, deadline, hops, err := forward.Decode(data)
	if err != nil {
		return forward.StatusInvalid
	}
	req := &forwardChannelEventReq{}
	if err := req.decode(body); err != nil || req.channelId == "" || len(req.events) == 0 {
		return forward.StatusInvalid
	}
	eventType := req.events[0].Type
	for _, e := range req.events {
		if e.Type != eventType || !validChannelForwardEvent(e) {
			return forward.StatusInvalid
		}
		if !h.notForwardToLeader(e.Type) || (e.Type == eventbus.EventChannelDistribute && strings.TrimSpace(e.TagKey) == "") {
			cl := h.cluster()
			if cl == nil {
				return forward.StatusRetry
			}
			leader, err := cl.SlotLeaderIdOfChannel(req.channelId, req.channelType)
			if err != nil || leader == 0 || !h.isLocalNode(leader) {
				return forward.StatusRetry
			}
		}
	}
	chBus := h.channelBus()
	if chBus == nil {
		return forward.StatusRetry
	}
	for _, e := range req.events {
		e.ForwardDeadline, e.ForwardHops = deadline, hops
		if req.fromNode != 0 && e.Type == eventbus.EventChannelOnSend && e.MessageSeq > 0 {
			// The persisting ingress node already ran node-local PluginPersistAfter and EventChannelWebhook;
			// record that fact on the receiving slot leader so cross-node retries across leadership handoffs
			// never duplicate plugin/webhook side effects.
			_ = h.recordPluginWebhookSideEffect(req.channelId, req.channelType, e.MessageSeq)
		}
		chBus.AddEvent(req.channelId, req.channelType, e)
	}
	chBus.Advance(req.channelId, req.channelType)
	return proto.StatusOK
}

func validChannelForwardEvent(e *eventbus.Event) bool {
	if e.Conn == nil {
		return false
	}
	switch e.Type {
	case eventbus.EventChannelOnSend, eventbus.EventChannelWebhook, eventbus.EventChannelDistribute:
		_, ok := e.Frame.(*wkproto.SendPacket)
		return ok
	default:
		return false
	}
}
