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
	forward.RegisterCapabilityRouteOn(cl)
	cl.Route(forward.ChannelPath, func(c *wkserver.Context) { c.WriteStatus(h.acceptForward(c.Body())) })
}

func (h *Handler) AcceptForward(data []byte) proto.Status {
	return h.acceptForward(data)
}

func isDurableForwardEvent(e *eventbus.Event) bool {
	if e == nil {
		return false
	}
	if e.MessageSeq > 0 {
		return true
	}
	if e.Type == eventbus.EventChannelOnSend {
		if sp, ok := e.Frame.(*wkproto.SendPacket); ok && sp != nil && !sp.NoPersist {
			return true
		}
	}
	return false
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
		if e == nil || e.Type != eventType || !validChannelForwardEvent(e) {
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

	var durableIdx []int
	var durableEvs []*eventbus.Event
	var transientIdx []int
	var transientEvs []*eventbus.Event
	for i, e := range req.events {
		if isDurableForwardEvent(e) {
			durableIdx = append(durableIdx, i)
			durableEvs = append(durableEvs, e)
		} else {
			transientIdx = append(transientIdx, i)
			transientEvs = append(transientEvs, e)
		}
	}

	var durableTickets []*eventbus.PostCommitTicket
	if len(durableEvs) > 0 {
		var ok bool
		durableTickets, ok = h.postCommitBudget().TryReserve(req.channelId, durableEvs)
		if !ok {
			return forward.StatusRetry
		}
	}
	var transientTickets []*eventbus.PostCommitTicket
	if len(transientEvs) > 0 {
		var ok bool
		transientTickets, ok = h.transientBudget().TryReserve(req.channelId, transientEvs)
		if !ok {
			for _, tk := range durableTickets {
				tk.Release()
			}
			return forward.StatusRetry
		}
	}

	for i, idx := range durableIdx {
		req.events[idx].Ticket = durableTickets[i]
	}
	for i, idx := range transientIdx {
		req.events[idx].Ticket = transientTickets[i]
	}

	for _, e := range req.events {
		e.ForwardDeadline, e.ForwardHops = deadline, hops
		if req.fromNode != 0 && e.Type == eventbus.EventChannelOnSend && e.MessageSeq > 0 {
			if !e.PluginDone && !e.WebhookDone {
				// BaseV1 / pre-PartA sender ran local side effects before forwarding without explicit wire flags.
				e.PluginDone = true
				e.WebhookDone = true
			}
			if e.PluginDone {
				_ = h.recordPluginSideEffect(req.channelId, req.channelType, e.MessageSeq)
			}
			if e.WebhookDone {
				_ = h.recordWebhookSideEffect(req.channelId, req.channelType, e.MessageSeq)
			}
		}
	}

	if !h.tryAddBatchToChannelBus(req.channelId, req.channelType, req.events) {
		for _, e := range req.events {
			if e != nil && e.Ticket != nil {
				e.Ticket.Release()
				e.Ticket = nil
			}
		}
		return forward.StatusRetry
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
