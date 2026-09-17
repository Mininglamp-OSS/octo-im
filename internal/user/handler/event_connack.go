package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

func (h *Handler) connack(ctx *eventbus.UserContext) {
	for _, event := range ctx.Events {
		conn := event.Conn
		frame := event.Frame
		if conn == nil {
			h.Error("processConnack: conn is nil")
			continue
		}
		uid := conn.Uid
		if conn.NodeId == 0 {
			h.Error("processConnack: from node is 0", zap.String("uid", uid))
			continue
		}
		if frame == nil {
			h.Error("processConnack: frame is nil", zap.String("uid", uid))
			continue
		}
		connack, ok := frame.(*wkproto.ConnackPacket)
		if !ok {
			h.Error("processConnack: unexpected frame", zap.String("uid", uid), zap.String("frameType", frame.GetFrameType().String()))
			continue
		}
		if connack.ReasonCode == wkproto.ReasonSuccess {
			// 设置连接最大空闲时间
			if options.G.IsLocalNode(conn.NodeId) {
				if service.Presence != nil && !service.Presence.Authenticate(conn) {
					current := service.Presence.LocalSession(conn)
					h.Warn("physical session rejected",
						zap.String("uid", conn.Uid),
						zap.Int64("connId", conn.ConnId),
						zap.String("ownerBootId", conn.OwnerBootID),
						zap.String("sessionId", conn.SessionID),
						zap.Uint64("rejectionCount", service.Presence.RejectedCount()))
					if current != nil {
						connack.ReasonCode = wkproto.ReasonAuthFail
						connack.NodeId = options.G.Cluster.NodeId
						eventbus.User.ConnWrite(event.ReqId, current, connack)
					}
					continue
				}
				realConn := service.ConnManager.GetConn(conn.ConnId)
				if realConn != nil {
					realConn.SetMaxIdle(options.G.ConnIdleTime)
					realConn.SetContext(conn)
				}
			}
			connack.NodeId = options.G.Cluster.NodeId
			// 更新连接
			eventbus.User.UpdateConn(conn)
		}
		eventbus.User.ConnWrite(event.ReqId, conn, connack)
	}

}
