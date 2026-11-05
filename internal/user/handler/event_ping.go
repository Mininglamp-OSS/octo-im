package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// Answer network-origin heartbeats at their physical owner. A user leader can
// change while its transport retains an earlier PING; waiting for that peer's
// reconnect would make a healthy client socket appear dead. Presence refreshes
// logical sessions independently, and writeLocalFrame still fences the PONG
// against a closed or reused physical socket.
func (h *Handler) handleLocalPings(ctx *eventbus.UserContext) {
	var remaining []*eventbus.Event
	for i, event := range ctx.Events {
		localPing := event != nil && event.Conn != nil &&
			options.G.IsLocalNode(event.Conn.NodeId) &&
			event.SourceNodeId == options.G.Cluster.NodeId
		if localPing {
			_, localPing = event.Frame.(*wkproto.PingPacket)
		}
		if !localPing {
			if remaining != nil {
				remaining = append(remaining, event)
			}
			continue
		}
		if remaining == nil {
			remaining = make([]*eventbus.Event, 0, len(ctx.Events)-1)
			remaining = append(remaining, ctx.Events[:i]...)
		}
		conn := event.Conn
		if !conn.Auth || conn.Uid != ctx.Uid || service.ConnManager == nil {
			continue
		}
		raw := service.ConnManager.GetConn(conn.ConnId)
		if raw == nil {
			continue
		}
		physical, ok := raw.Context().(*eventbus.Conn)
		if !ok || physical == nil || !physical.Auth || !sessionDescriptorMatches(physical, conn) {
			continue
		}
		event.Conn = physical
		h.ping(event)
	}
	if remaining != nil {
		ctx.Events = remaining
	}
}

func (h *Handler) ping(event *eventbus.Event) {
	conn := event.Conn

	trace.GlobalTrace.Metrics.App().PingCountAdd(1)
	trace.GlobalTrace.Metrics.App().PingBytesAdd(1) // ping的大小就是1

	trace.GlobalTrace.Metrics.App().PongCountAdd(1)
	trace.GlobalTrace.Metrics.App().PongBytesAdd(1)
	eventbus.User.ConnWrite(event.ReqId, conn, &wkproto.PongPacket{})
}
