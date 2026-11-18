package handler

import (
	"fmt"
	"strings"
	"sync"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

const maxForwardAdmittedDedupEntries = 4096

type forwardAdmittedDedupCache struct {
	mu      sync.Mutex
	order   []string
	entries map[string]struct{}
}

func (c *forwardAdmittedDedupCache) checkAndRecord(keys []string) []bool {
	out := make([]bool, len(keys))
	if len(keys) == 0 {
		return out
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]struct{}, 256)
	}
	for i, k := range keys {
		if k == "" {
			continue
		}
		if _, exists := c.entries[k]; exists {
			out[i] = true
		}
	}
	return out
}

func (c *forwardAdmittedDedupCache) commitRecorded(keys []string) {
	if len(keys) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]struct{}, 256)
	}
	for _, k := range keys {
		if k == "" {
			continue
		}
		if _, exists := c.entries[k]; exists {
			continue
		}
		if len(c.order) >= maxForwardAdmittedDedupEntries {
			evict := c.order[0]
			c.order = c.order[1:]
			delete(c.entries, evict)
		}
		c.entries[k] = struct{}{}
		c.order = append(c.order, k)
	}
}

func forwardEventDedupKey(channelId string, channelType uint8, e *eventbus.Event) string {
	if e == nil || e.MessageSeq == 0 {
		return ""
	}
	// Only deduplicate across remote RPC retries (fromNode/SourceNodeId or explicit stage meta / target mask)
	// when the event carries a persisted MessageSeq > 0.
	return fmt.Sprintf("%s#%d#%d#%d#%d#%v#%v",
		channelId,
		channelType,
		e.MessageSeq,
		e.MessageId,
		e.Type,
		e.HasTargetSlotMask,
		e.TargetSlotMask,
	)
}

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
	return ClassifyEventDurability(e) != DurabilityTransient
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

	// B5: Filter out events in req.events that were already admitted by an earlier RPC whose ACK was lost.
	dedupKeys := make([]string, len(req.events))
	for i, e := range req.events {
		if req.fromNode != 0 && e != nil && e.MessageSeq > 0 {
			dedupKeys[i] = forwardEventDedupKey(req.channelId, req.channelType, e)
		}
	}
	alreadyAdmitted := h.forwardDedup.checkAndRecord(dedupKeys)

	var pendingEvents []*eventbus.Event
	var pendingDedupKeys []string
	for i, e := range req.events {
		if alreadyAdmitted[i] {
			continue
		}
		pendingEvents = append(pendingEvents, e)
		if dedupKeys[i] != "" {
			pendingDedupKeys = append(pendingDedupKeys, dedupKeys[i])
		}
	}
	if len(pendingEvents) == 0 {
		return proto.StatusOK
	}

	var durableIdx []int
	var durableEvs []*eventbus.Event
	var transientIdx []int
	var transientEvs []*eventbus.Event
	for i, e := range pendingEvents {
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
		pendingEvents[idx].Ticket = durableTickets[i]
	}
	for i, idx := range transientIdx {
		pendingEvents[idx].Ticket = transientTickets[i]
	}

	for _, e := range pendingEvents {
		e.ForwardDeadline, e.ForwardHops = deadline, hops
		if req.fromNode != 0 && e.Type == eventbus.EventChannelOnSend && e.MessageSeq > 0 {
			if e.HasStageMeta {
				// A2: PartAV2 sender explicitly provided stage statuses and owner node.
				// Never infer completion from (!PluginDone && !WebhookDone).
				switch e.PluginStageStatus {
				case eventbus.StageStatusCompleted, eventbus.StageStatusNotRequired:
					e.PluginDone = true
					_ = h.recordPluginSideEffect(req.channelId, req.channelType, e.MessageSeq)
				case eventbus.StageStatusRetryOwned:
					if e.StageOwnerNodeId != 0 && !h.isLocalNode(e.StageOwnerNodeId) {
						// Ingress node owns the Stage 0 retry; do not double-execute on leader.
						e.PluginDone = true
					} else {
						e.PluginDone = false
					}
				default:
					e.PluginDone = false
				}

				switch e.WebhookStageStatus {
				case eventbus.StageStatusCompleted, eventbus.StageStatusNotRequired:
					e.WebhookDone = true
					_ = h.recordWebhookSideEffect(req.channelId, req.channelType, e.MessageSeq)
				case eventbus.StageStatusRetryOwned:
					if e.StageOwnerNodeId != 0 && !h.isLocalNode(e.StageOwnerNodeId) {
						e.WebhookDone = true
					} else {
						e.WebhookDone = false
					}
				default:
					e.WebhookDone = false
				}
			} else {
				if !e.PluginDone && !e.WebhookDone {
					// Genuine BaseV1 (21cd8f88) frame without WKST stage metadata ran local side effects before forwarding.
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
	}

	if !h.tryAddBatchToChannelBus(req.channelId, req.channelType, pendingEvents) {
		for _, e := range pendingEvents {
			if e != nil && e.Ticket != nil {
				e.Ticket.Release()
				e.Ticket = nil
			}
		}
		return forward.StatusRetry
	}
	h.forwardDedup.commitRecorded(pendingDedupKeys)
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
