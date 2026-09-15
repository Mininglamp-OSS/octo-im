package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

type Handler struct {
	wklog.Log
	forwardCapabilities forward.CapabilityCache
	forwardGate         *forward.Gate
}

func NewHandler() *Handler {
	workerCount := 1
	if options.G != nil {
		workerCount = options.G.Poller.UserGoroutine
	}
	h := &Handler{
		Log:         wklog.NewWKLog("handler"),
		forwardGate: forward.NewGate(workerCount),
	}
	h.routes()
	return h
}

func (h *Handler) routes() {
	// 连接事件
	eventbus.RegisterUserHandlers(eventbus.EventConnect, h.connect)
	// 连接回执
	eventbus.RegisterUserHandlers(eventbus.EventConnack, h.connack)
	// 发送事件
	eventbus.RegisterUserHandlers(eventbus.EventOnSend, h.onSend)
	// 连接写事件
	eventbus.RegisterUserHandlers(eventbus.EventConnWriteFrame, h.writeFrame)
	// 连接关闭
	eventbus.RegisterUserHandlers(eventbus.EventConnClose, h.closeConn)
	// 移除连接
	eventbus.RegisterUserHandlers(eventbus.EventConnRemove, h.removeConn)
	// 移除leader节点上的连接
	eventbus.RegisterUserHandlers(eventbus.EventConnLeaderRemove, h.connLeaderRemove)

}

// 收到消息
func (h *Handler) OnMessage(m *proto.Message) {
	switch msgType(m.MsgType) {
	case msgForwardUserEvent:
		h.onForwardUserEvent(m)
	}
}

// 收到事件
func (h *Handler) OnEvent(ctx *eventbus.UserContext) {
	slotLeaderId := h.userLeaderNodeId(ctx.Uid)

	// 统计
	h.totalIn(ctx)

	// 如果本节点的事件则执行，非本节点事件转发到leader节点
	if options.G.IsLocalNode(slotLeaderId) ||
		h.notForwardToLeader(ctx.EventType) {
		// 执行本地事件
		eventbus.ExecuteUserEvent(ctx)
	} else {
		h.forwardsToNode(slotLeaderId, ctx.Uid, ctx.Events)
	}
}

// 统计输入
func (h *Handler) totalIn(ctx *eventbus.UserContext) {
	// 统计
	for _, event := range ctx.Events {
		if event.Type == eventbus.EventOnSend {
			frameType := event.Frame.GetFrameType()
			// 统计
			conn := event.Conn
			conn.InPacketCount.Add(1)
			conn.InPacketByteCount.Add(event.Frame.GetFrameSize())
			if frameType == wkproto.SEND {
				conn.InMsgCount.Add(1)
				conn.InMsgByteCount.Add(event.Frame.GetFrameSize())
			}
		}
	}
}

func (h *Handler) totalOut(conn *eventbus.Conn, frame wkproto.Frame) {
	frameType := frame.GetFrameType()
	// 统计
	conn.OutPacketCount.Add(1)
	conn.OutPacketByteCount.Add(frame.GetFrameSize())
	if frameType == wkproto.RECV {
		conn.OutMsgCount.Add(1)
		conn.OutMsgByteCount.Add(frame.GetFrameSize())
	}
}

// 不需要转发给领导的事件
func (h *Handler) notForwardToLeader(eventType eventbus.EventType) bool {
	switch eventType {
	case eventbus.EventConnClose,
		eventbus.EventConnack,
		eventbus.EventConnWriteFrame,
		eventbus.EventConnRemove:
		return true
	}
	return false

}

// 获得用户的leader节点
func (h *Handler) userLeaderNodeId(uid string) uint64 {
	slotId := service.Cluster.GetSlotId(uid)
	leaderId := service.Cluster.SlotLeaderId(slotId)
	return leaderId
}

func (h *Handler) forwardsToNode(nodeId uint64, uid string, events []*eventbus.Event) {
	if len(events) == 0 {
		return
	}

	req := &forwardUserEventReq{
		uid:      uid,
		fromNode: options.G.Cluster.NodeId,
		events:   events,
	}
	data, err := req.encode()
	if err != nil {
		h.Error("forwardToLeader: encode failed", zap.Error(err))
		forward.Fail(events, err)
		return
	}
	legacy := func(targetNode uint64) error {
		return h.sendToNode(targetNode, &proto.Message{MsgType: uint32(msgForwardUserEvent), Content: data})
	}
	if !h.forwardGate.TryAcquire() {
		if forward.OnlyRetryableSends(events) {
			err = forward.ErrUnavailable
		} else {
			// Fixed-destination lifecycle/write events cannot ask a client to
			// retry. Before any v1 attempt, preserve them in the reconnect-aware
			// transport queue instead of dropping the dequeued batch.
			err = legacy(nodeId)
		}
		if err != nil {
			h.Error("user forwarding concurrency limit reached", zap.Error(err), zap.String("uid", uid))
			forward.Fail(events, err)
		}
		return
	}
	defer h.forwardGate.Release()
	target := func() uint64 { return nodeId }
	if !h.notForwardToLeader(events[0].Type) {
		target = func() uint64 { return h.userLeaderNodeId(uid) }
	}
	err = forward.RequestCompatible(forward.UserPath, data, events, target, options.G.Cluster.NodeId, h.acceptForward, &h.forwardCapabilities, legacy)
	if err != nil {
		h.Error("user forwarding failed", zap.Error(err), zap.String("uid", uid))
		forward.Fail(events, err)
	}
}

func (h *Handler) forwardToNode(nodeId uint64, uid string, event *eventbus.Event) {
	h.forwardsToNode(nodeId, uid, []*eventbus.Event{event})
}

func (h *Handler) sendToNode(toNodeId uint64, msg *proto.Message) error {
	err := service.Cluster.Send(toNodeId, msg)
	return err
}

// 收到转发用户事件
func (h *Handler) onForwardUserEvent(m *proto.Message) {
	req := &forwardUserEventReq{}
	err := req.decode(m.Content)
	if err != nil {
		h.Error("onForwardUserEvent: decode failed", zap.Error(err))
		return
	}
	slotLeaderId := h.userLeaderNodeId(req.uid)
	if slotLeaderId == 0 {
		h.Error("OnEvent: get slotLeaderId is 0")
		forward.Fail(req.events, forward.ErrUnavailable)
		return
	}

	isSlotLeader := options.G.IsLocalNode(slotLeaderId)

	for _, e := range req.events {
		if !h.notForwardToLeader(e.Type) {
			if !isSlotLeader {
				h.Error("onForwardUserEvent: event type is not EventConnWriteFrame, but not slot leader", zap.String("uid", req.uid), zap.Uint64("slotLeaderId", slotLeaderId))
				forward.Fail([]*eventbus.Event{e}, forward.ErrUnavailable)
				continue
			}
		}

		// 替换成本地的连接
		if e.Conn != nil {
			conn := eventbus.User.ConnById(e.Conn.Uid, e.Conn.NodeId, e.Conn.ConnId)
			if conn != nil {
				e.Conn = conn
			}

		}
		eventbus.User.AddEvent(req.uid, e)
	}
	eventbus.User.Advance(req.uid)

}
