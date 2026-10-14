package handler

import (
	"bytes"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func (h *Handler) SetForwardRoutes() {
	service.Cluster.Route(forward.UserPath, func(c *wkserver.Context) { c.WriteStatus(h.acceptForward(c.Body())) })
}

func (h *Handler) acceptForward(data []byte) proto.Status {
	body, deadline, hops, err := forward.Decode(data)
	if err != nil {
		return forward.StatusInvalid
	}
	req := &forwardUserEventReq{}
	if err := req.decode(body); err != nil || req.uid == "" || len(req.events) == 0 {
		return forward.StatusInvalid
	}
	// Validate the whole batch before admission; partial admission cannot be retried safely.
	eventType := req.events[0].Type
	for _, e := range req.events {
		if e.Type != eventType || e.Conn == nil || e.Conn.Uid != req.uid || !validUserForwardEvent(e) {
			return forward.StatusInvalid
		}
		if h.notForwardToLeader(e.Type) {
			if e.Type != eventbus.EventConnRemove && !options.G.IsLocalNode(e.Conn.NodeId) {
				return forward.StatusInvalid
			}
		} else if !options.G.IsLocalNode(h.userLeaderNodeId(req.uid)) {
			return forward.StatusRetry
		}
	}
	for _, e := range req.events {
		e.ForwardDeadline, e.ForwardHops = deadline, hops
		// CONNACK contains the newly authenticated descriptor. Replacing it with
		// the pending local descriptor would discard authentication and AES keys.
		if e.Type != eventbus.EventConnack {
			if conn := eventbus.User.ConnById(req.uid, e.Conn.NodeId, e.Conn.ConnId); conn != nil {
				if sameForwardedConn(conn, e.Conn) {
					e.Conn = conn
				} else {
					eventbus.User.TouchConn(req.uid, e.Conn.NodeId, e.Conn.ConnId)
				}
			}
		}
		eventbus.User.AddEvent(req.uid, e)
	}
	eventbus.User.Advance(req.uid)
	return proto.StatusOK
}

func validUserForwardEvent(e *eventbus.Event) bool {
	switch e.Type {
	case eventbus.EventConnect:
		_, ok := e.Frame.(*wkproto.ConnectPacket)
		return ok
	case eventbus.EventConnack:
		_, ok := e.Frame.(*wkproto.ConnackPacket)
		return ok
	case eventbus.EventOnSend:
		switch e.Frame.(type) {
		case *wkproto.SendPacket, *wkproto.RecvackPacket, *wkproto.PingPacket:
			return true
		default:
			return false
		}
	case eventbus.EventConnWriteFrame:
		return e.Frame != nil
	case eventbus.EventConnClose, eventbus.EventConnRemove, eventbus.EventConnLeaderRemove:
		return true
	default:
		return false
	}
}

// Compare the complete immutable wire descriptor before reusing a cached
// connection. This also preserves generation fields added by session recovery.
func sameForwardedConn(cached, incoming *eventbus.Conn) bool {
	if cached == nil || incoming == nil {
		return false
	}
	left, err := cached.Encode()
	if err != nil {
		return false
	}
	right, err := incoming.Encode()
	return err == nil && bytes.Equal(left, right)
}
