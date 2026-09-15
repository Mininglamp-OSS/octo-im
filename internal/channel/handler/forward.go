package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
)

func (h *Handler) SetForwardRoutes() {
	service.Cluster.Route(forward.ChannelPath, func(c *wkserver.Context) { c.WriteStatus(h.acceptForward(c.Body())) })
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
	for _, e := range req.events {
		if (e.Type != eventbus.EventChannelOnSend && !h.notForwardToLeader(e.Type)) || e.Conn == nil || e.Frame == nil {
			return forward.StatusInvalid
		}
		if !h.notForwardToLeader(e.Type) {
			leader, err := service.Cluster.SlotLeaderIdOfChannel(req.channelId, req.channelType)
			if err != nil || leader == 0 || !options.G.IsLocalNode(leader) {
				return forward.StatusRetry
			}
		}
	}
	for _, e := range req.events {
		e.ForwardDeadline, e.ForwardHops = deadline, hops
		eventbus.Channel.AddEvent(req.channelId, req.channelType, e)
	}
	eventbus.Channel.Advance(req.channelId, req.channelType)
	return proto.StatusOK
}
