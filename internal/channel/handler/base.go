package handler

import (
	"context"

	"github.com/WuKongIM/WuKongIM/internal/common"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"go.uber.org/zap"
)

type Handler struct {
	wklog.Log
	client              *ingress.Client
	commonService       *common.Service
	forwardCapabilities forward.CapabilityCache
	forwardGate         *forward.Gate
}

func NewHandler() *Handler {
	workerCount := 1
	if options.G != nil {
		workerCount = options.G.Poller.ChannelGoroutine
	}
	h := &Handler{
		Log:           wklog.NewWKLog("handler"),
		client:        ingress.NewClient(),
		commonService: common.NewService(),
		forwardGate:   forward.NewGate(workerCount),
	}
	h.routes()
	return h
}

func (h *Handler) routes() {

	// 发送消息
	eventbus.RegisterChannelHandlers(eventbus.EventChannelOnSend, h.onSend)
	// webhook
	eventbus.RegisterChannelHandlers(eventbus.EventChannelWebhook, h.webhook)
	// 分发消息
	eventbus.RegisterChannelHandlers(eventbus.EventChannelDistribute, h.distribute)

}

// 收到消息
func (h *Handler) OnMessage(m *proto.Message) {
	switch msgType(m.MsgType) {
	case msgForwardChannelEvent:
		h.onForwardChannelEvent(m)
	}
}

// 收到事件
func (h *Handler) OnEvent(ctx *eventbus.ChannelContext) {
	if options.G.IsLocalNode(ctx.SlotLeaderId) || h.notForwardToLeader(ctx.EventType) {
		// 执行本地事件 ,频道永远在自己的槽领导节点上执行逻辑。
		eventbus.ExecuteChannelEvent(ctx)
	} else {
		h.forwardsToNode(ctx.SlotLeaderId, ctx.ChannelId, ctx.ChannelType, ctx.Events)
	}
}

// 不需要转发给领导的事件
func (h *Handler) notForwardToLeader(eventType eventbus.EventType) bool {
	switch eventType {
	case eventbus.EventChannelWebhook,
		eventbus.EventChannelDistribute:
		return true
	}
	return false

}

func (h *Handler) forwardsToNode(nodeId uint64, channelId string, channelType uint8, events []*eventbus.Event) {

	req := &forwardChannelEventReq{
		channelId:   channelId,
		channelType: channelType,
		fromNode:    options.G.Cluster.NodeId,
		events:      events,
	}
	data, err := req.encode()
	if err != nil {
		h.Error("forwardToLeader: encode failed", zap.Error(err))
		forward.Fail(events, err)
		return
	}
	legacy := func(targetNode uint64) error {
		return h.sendToNode(targetNode, &proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: data})
	}
	if !h.forwardGate.TryAcquire() {
		if forward.OnlyRetryableSends(events) {
			err = forward.ErrUnavailable
		} else {
			// Post-persistence distribution/webhook events have no client-side
			// retry path. Preserve them in the reconnect-aware transport queue.
			err = legacy(nodeId)
		}
		if err != nil {
			h.Error("channel forwarding concurrency limit reached", zap.Error(err), zap.String("channelId", channelId))
			forward.Fail(events, err)
		}
		return
	}
	defer h.forwardGate.Release()
	target := func() uint64 {
		if len(events) > 0 && h.notForwardToLeader(events[0].Type) {
			return nodeId
		}
		leader, err := service.Cluster.SlotLeaderIdOfChannel(channelId, channelType)
		if err != nil {
			return 0
		}
		return leader
	}
	err = forward.RequestCompatible(forward.ChannelPath, data, events, target, options.G.Cluster.NodeId, h.acceptForward, &h.forwardCapabilities, legacy)
	if err != nil {
		h.Error("channel forwarding failed", zap.Error(err), zap.String("channelId", channelId))
		forward.Fail(events, err)
	}
}

// func (h *Handler) forwardToNode(nodeId uint64, channelId string, channelType uint8, event *eventbus.Event) {
// 	h.forwardsToNode(nodeId, channelId, channelType, []*eventbus.Event{event})
// }

func (h *Handler) sendToNode(toNodeId uint64, msg *proto.Message) error {
	err := service.Cluster.Send(toNodeId, msg)
	return err
}

// 收到转发用户事件
func (h *Handler) onForwardChannelEvent(m *proto.Message) {
	req := &forwardChannelEventReq{}
	err := req.decode(m.Content)
	if err != nil {
		h.Error("onForwardChannelEvent: decode failed", zap.Error(err))
		return
	}

	for _, e := range req.events {
		eventbus.Channel.AddEvent(req.channelId, req.channelType, e)
	}
	eventbus.Channel.Advance(req.channelId, req.channelType)

}

func (h *Handler) WithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), options.G.Channel.ProcessTimeout)

}
