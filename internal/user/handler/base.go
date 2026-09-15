package handler

import (
	"context"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

const (
	verificationBatchBudget = 250 * time.Millisecond
	verificationFailureTTL  = time.Second
	verificationFailureCap  = 4096
)

type verificationSessionKey struct {
	uid         string
	nodeID      uint64
	connID      int64
	ownerBootID string
	sessionID   string
}

type verificationResult struct {
	conn   *eventbus.Conn
	failed bool
}

type Handler struct {
	wklog.Log
	verificationFailures struct {
		sync.Mutex
		entries map[verificationSessionKey]time.Time
	}
}

func NewHandler() *Handler {
	h := &Handler{
		Log: wklog.NewWKLog("handler"),
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
	if slotLeaderId == 0 {
		h.Error("OnEvent: get slotLeaderId is 0")
		return
	}

	// 统计
	h.totalIn(ctx)

	// 如果本节点的事件则执行，非本节点事件转发到leader节点
	if options.G.IsLocalNode(slotLeaderId) ||
		h.notForwardToLeader(ctx.EventType) {
		// A forwarded descriptor is only a claim until its physical owner verifies it.
		if ctx.EventType == eventbus.EventOnSend && service.Presence != nil {
			ctx.Events = h.verifyOnSendEvents(ctx.Uid, ctx.Events)
			if len(ctx.Events) == 0 {
				return
			}
		}
		eventbus.ExecuteUserEvent(ctx)
	} else {
		if slotLeaderId != 0 {
			// 转发到leader节点
			h.forwardsToNode(slotLeaderId, ctx.Uid, ctx.Events)
		} else {
			h.Error("user: OnEvent: slotLeaderId is 0", zap.String("uid", ctx.Uid), zap.Uint64("slotLeaderId", slotLeaderId))
		}
	}
}

func (h *Handler) verifyOnSendEvents(uid string, events []*eventbus.Event) []*eventbus.Event {
	verifiedEvents := make([]*eventbus.Event, 0, len(events))
	results := make(map[verificationSessionKey]verificationResult)
	batchCtx, cancelBatch := context.WithTimeout(context.Background(), verificationBatchBudget)
	defer cancelBatch()
	budgetWarned := false
	for _, event := range events {
		if event == nil || event.Conn == nil || event.Frame == nil {
			h.Warn("skip malformed on-send event", zap.String("uid", uid))
			continue
		}
		known := eventbus.User.ConnById(uid, event.Conn.NodeId, event.Conn.ConnId)
		if known != nil && known.Auth && known.SameSession(event.Conn) {
			event.Conn = known
			verifiedEvents = append(verifiedEvents, event)
			continue
		}
		key := verificationKey(event.Conn)
		if result, ok := results[key]; ok {
			if result.failed {
				verifiedEvents = h.handleUnverifiedOnSend(verifiedEvents, event)
				continue
			}
			event.Conn = result.conn
			verifiedEvents = append(verifiedEvents, event)
			continue
		}
		if h.verificationFailedRecently(key, time.Now()) {
			results[key] = verificationResult{failed: true}
			verifiedEvents = h.handleUnverifiedOnSend(verifiedEvents, event)
			continue
		}
		if err := batchCtx.Err(); err != nil {
			if !budgetWarned {
				h.Warn("physical session verification budget exhausted", zap.Error(err), zap.String("uid", uid))
				budgetWarned = true
			}
			results[key] = verificationResult{failed: true}
			verifiedEvents = h.handleUnverifiedOnSend(verifiedEvents, event)
			continue
		}
		verified, err := service.Presence.Verify(batchCtx, event.Conn)
		if err != nil {
			results[key] = verificationResult{failed: true}
			h.rememberVerificationFailure(key, time.Now())
			h.Warn("physical session verification failed",
				zap.Error(err),
				zap.String("uid", event.Conn.Uid),
				zap.Uint64("nodeId", event.Conn.NodeId),
				zap.Int64("connId", event.Conn.ConnId))
			verifiedEvents = h.handleUnverifiedOnSend(verifiedEvents, event)
			continue
		}
		results[key] = verificationResult{conn: verified}
		h.clearVerificationFailure(key)
		event.Conn = verified
		eventbus.User.UpdateConn(verified)
		verifiedEvents = append(verifiedEvents, event)
	}
	return verifiedEvents
}

func verificationKey(conn *eventbus.Conn) verificationSessionKey {
	return verificationSessionKey{
		uid: conn.Uid, nodeID: conn.NodeId, connID: conn.ConnId,
		ownerBootID: conn.OwnerBootID, sessionID: conn.SessionID,
	}
}

func (h *Handler) handleUnverifiedOnSend(verifiedEvents []*eventbus.Event, event *eventbus.Event) []*eventbus.Event {
	switch packet := event.Frame.(type) {
	case *wkproto.SendPacket:
		eventbus.User.ConnWrite(event.ReqId, event.Conn, &wkproto.SendackPacket{
			Framer:      packet.Framer,
			MessageID:   event.MessageId,
			ClientSeq:   packet.ClientSeq,
			ClientMsgNo: packet.ClientMsgNo,
			ReasonCode:  wkproto.ReasonNodeNotMatch,
		})
		eventbus.User.Advance(event.Conn.Uid)
	case *wkproto.PingPacket, *wkproto.RecvackPacket:
		// These frames are idempotent. Let the physical write/session fence
		// protect PONG delivery, and let recvack validate the retry's session.
		verifiedEvents = append(verifiedEvents, event)
	}
	return verifiedEvents
}

func (h *Handler) verificationFailedRecently(key verificationSessionKey, now time.Time) bool {
	h.verificationFailures.Lock()
	defer h.verificationFailures.Unlock()
	until, ok := h.verificationFailures.entries[key]
	if !ok {
		return false
	}
	if !now.Before(until) {
		delete(h.verificationFailures.entries, key)
		return false
	}
	return true
}

func (h *Handler) rememberVerificationFailure(key verificationSessionKey, now time.Time) {
	h.verificationFailures.Lock()
	defer h.verificationFailures.Unlock()
	if h.verificationFailures.entries == nil {
		h.verificationFailures.entries = make(map[verificationSessionKey]time.Time)
	}
	if len(h.verificationFailures.entries) >= verificationFailureCap {
		for cached := range h.verificationFailures.entries {
			delete(h.verificationFailures.entries, cached)
			break
		}
	}
	h.verificationFailures.entries[key] = now.Add(verificationFailureTTL)
}

func (h *Handler) clearVerificationFailure(key verificationSessionKey) {
	h.verificationFailures.Lock()
	delete(h.verificationFailures.entries, key)
	h.verificationFailures.Unlock()
}

// 统计输入
func (h *Handler) totalIn(ctx *eventbus.UserContext) {
	// 统计
	for _, event := range ctx.Events {
		if event != nil && event.Type == eventbus.EventOnSend && event.Frame != nil && event.Conn != nil {
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

	for _, e := range events {
		if e.SourceNodeId != 0 && e.SourceNodeId == nodeId {
			h.Error("forwardsToNode: event source node id is equal to nodeId,end forward", zap.Uint64("sourceNodeId", e.SourceNodeId), zap.Uint64("nodeId", nodeId), zap.String("uid", uid), zap.String("eventType", e.Type.String()))
			return
		}
	}

	req := &forwardUserEventReq{
		uid:      uid,
		fromNode: options.G.Cluster.NodeId,
		events:   events,
	}
	data, err := req.encode()
	if err != nil {
		h.Error("forwardToLeader: encode failed", zap.Error(err))
		return
	}
	msg := &proto.Message{
		MsgType: uint32(msgForwardUserEvent),
		Content: data,
	}
	err = h.sendToNode(nodeId, msg)
	if err != nil {
		h.Error("user:forwardToLeader: send failed", zap.Error(err), zap.Uint64("nodeId", nodeId), zap.String("uid", uid))
		return
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
		return
	}

	isSlotLeader := options.G.IsLocalNode(slotLeaderId)

	for _, e := range req.events {
		if !h.notForwardToLeader(e.Type) {
			if !isSlotLeader {
				h.Error("onForwardUserEvent: event type is not EventConnWriteFrame, but not slot leader", zap.String("uid", req.uid), zap.Uint64("slotLeaderId", slotLeaderId))
				continue
			}
		}

		// 替换成本地的连接
		if e.Conn != nil {
			conn := eventbus.User.ConnById(e.Conn.Uid, e.Conn.NodeId, e.Conn.ConnId)
			if e.Type != eventbus.EventConnack && conn != nil && conn.SameSession(e.Conn) {
				e.Conn = conn
			}

		}
		eventbus.User.AddEvent(req.uid, e)
	}
	eventbus.User.Advance(req.uid)

}
