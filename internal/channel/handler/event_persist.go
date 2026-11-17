package handler

import (
	"fmt"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

// 消息持久化
func (h *Handler) persist(ctx *eventbus.ChannelContext) {
	// 记录消息轨迹
	events := ctx.Events
	for _, e := range events {
		e.Track.Record(track.PositionChannelPersist)
	}

	// ========== 存储消息 ==========
	unpersistedEvents := make([]*eventbus.Event, 0, len(events))
	alreadyPersisted := false
	skipDistribute := make(map[*eventbus.Event]bool)
	for _, e := range events {
		if e != nil && e.MessageSeq > 0 && e.ReasonCode == wkproto.ReasonSuccess {
			alreadyPersisted = true
			if h.recordPluginWebhookSideEffect(ctx.ChannelId, ctx.ChannelType, e.MessageSeq) {
				e.PersistedDuplicate = true
			}
			if h.recordDistributeSideEffect(ctx.ChannelId, ctx.ChannelType, e.MessageSeq) {
				skipDistribute[e] = true
			}
			continue
		}
		unpersistedEvents = append(unpersistedEvents, e)
	}
	persists := h.toPersistMessages(ctx.ChannelId, ctx.ChannelType, unpersistedEvents)
	if len(persists) > 0 {

		timeoutCtx, cancel := h.WithTimeout()
		defer cancel()
		results, err := h.store().AppendMessages(timeoutCtx, ctx.ChannelId, ctx.ChannelType, persists)
		if err != nil {
			h.Error("store message failed", zap.Error(err), zap.Int("events", len(persists)), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
			markPersistFailure(unpersistedEvents, err)
		}

		if err == nil {
			err = applyPersistResults(unpersistedEvents, persists, results)
			if err != nil {
				h.Error("invalid persistence response", zap.Error(err))
				markPersistFailure(unpersistedEvents, err)
			} else {
				for _, e := range unpersistedEvents {
					if e != nil && e.MessageSeq > 0 && e.ReasonCode == wkproto.ReasonSuccess && !e.PersistedDuplicate {
						_ = h.recordPluginWebhookSideEffect(ctx.ChannelId, ctx.ChannelType, e.MessageSeq)
						_ = h.recordDistributeSideEffect(ctx.ChannelId, ctx.ChannelType, e.MessageSeq)
					}
				}
			}
		}
	}
	if len(persists) > 0 || alreadyPersisted {
		allPersisted := h.toPersistMessages(ctx.ChannelId, ctx.ChannelType, events)
		h.pluginInvokePersistAfter(ctx.ChannelId, ctx.ChannelType, newlyPersistedMessages(events, allPersisted))
	}

	if options.G.Logger.TraceOn {
		for _, e := range events {
			h.Trace("message persistence result", "persist", zap.Int64("messageId", e.MessageId),
				zap.Uint64("messageSeq", e.MessageSeq), zap.String("channelId", ctx.ChannelId),
				zap.Uint8("channelType", ctx.ChannelType), zap.String("reason", e.ReasonCode.String()))
		}
	}

	chBus := h.channelBus()
	// ========== webhook ==========
	if options.G.WebhookOn(types.EventMsgNotify) && chBus != nil {
		for _, e := range events {
			sendPacket := e.Frame.(*wkproto.SendPacket)
			if shouldRunPersistSideEffects(e) && !sendPacket.NoPersist {
				cloneEvent := e.Clone()
				cloneEvent.ForwardDeadline, cloneEvent.ForwardHops = 0, 0
				cloneEvent.Type = eventbus.EventChannelWebhook
				chBus.AddEvent(ctx.ChannelId, ctx.ChannelType, cloneEvent)
			}
		}
	}

	// ========== 分发 ==========
	if chBus != nil {
		for _, e := range events {
			if skipDistribute[e] || !shouldDistributePersistResult(e) {
				continue
			}
			cloneEvent := e.Clone()
			cloneEvent.ForwardDeadline, cloneEvent.ForwardHops = 0, 0
			cloneEvent.Type = eventbus.EventChannelDistribute
			chBus.AddEvent(ctx.ChannelId, ctx.ChannelType, cloneEvent)
		}

		chBus.Advance(ctx.ChannelId, ctx.ChannelType)
	}

}

func (h *Handler) executePersistedLocalSideEffects(channelId string, channelType uint8, events []*eventbus.Event) {
	freshEvents := make([]*eventbus.Event, 0, len(events))
	for _, e := range events {
		if e == nil || e.MessageSeq == 0 || e.ReasonCode != wkproto.ReasonSuccess {
			continue
		}
		if h.recordPluginWebhookSideEffect(channelId, channelType, e.MessageSeq) {
			e.PersistedDuplicate = true
			continue
		}
		freshEvents = append(freshEvents, e)
	}
	if len(freshEvents) == 0 {
		return
	}
	allPersisted := h.toPersistMessages(channelId, channelType, freshEvents)
	h.pluginInvokePersistAfter(channelId, channelType, newlyPersistedMessages(freshEvents, allPersisted))
	chBus := h.channelBus()
	if options.G != nil && options.G.WebhookOn(types.EventMsgNotify) && chBus != nil {
		queued := false
		for _, e := range freshEvents {
			sendPacket, ok := e.Frame.(*wkproto.SendPacket)
			if ok && sendPacket != nil && shouldRunPersistSideEffects(e) && !sendPacket.NoPersist {
				cloneEvent := e.Clone()
				cloneEvent.ForwardDeadline, cloneEvent.ForwardHops = 0, 0
				cloneEvent.Type = eventbus.EventChannelWebhook
				chBus.AddEvent(channelId, channelType, cloneEvent)
				queued = true
			}
		}
		if queued {
			chBus.Advance(channelId, channelType)
		}
	}
}

func markPersistFailure(events []*eventbus.Event, err error) {
	retryable := channel.IsRetryableSendError(err)
	ambiguous := channel.IsAmbiguousSendError(err)
	for _, event := range events {
		packet, ok := event.Frame.(*wkproto.SendPacket)
		if !ok || packet == nil || packet.NoPersist || event.ReasonCode != wkproto.ReasonSuccess {
			continue
		}
		event.ReasonCode = wkproto.ReasonSystemError
		if !retryable {
			continue
		}
		if ambiguous && packet.ClientMsgNo == "" {
			event.PersistenceOutcomeUnknown = true
			continue
		}
		event.ReasonCode = wkproto.ReasonNodeNotMatch
	}
}

func shouldRunPersistSideEffects(event *eventbus.Event) bool {
	return event.ReasonCode == wkproto.ReasonSuccess && !event.PersistedDuplicate
}

func shouldDistributePersistResult(event *eventbus.Event) bool {
	// Duplicate proves persistence, not that the earlier process reached the
	// in-memory distribution stage. Redistribute the canonical message so a
	// commit followed by a crash cannot strand it; recipients deduplicate by
	// the canonical message identity.
	return event.ReasonCode == wkproto.ReasonSuccess
}

func (h *Handler) pluginInvokePersistAfter(channelId string, channelType uint8, msgs []wkdb.Message) {
	if len(msgs) == 0 || service.PluginManager == nil {
		return
	}
	plugins := service.PluginManager.Plugins(types.PluginPersistAfter)
	if len(plugins) == 0 {
		return
	}

	// 构建插件消息
	pluginMessages := make([]*pluginproto.Message, 0, len(msgs))
	for _, msg := range msgs {
		pluginMessages = append(pluginMessages, &pluginproto.Message{
			MessageId:   msg.MessageID,
			MessageSeq:  uint64(msg.MessageSeq),
			ClientMsgNo: msg.ClientMsgNo,
			StreamNo:    msg.StreamNo,
			StreamId:    msg.StreamId,
			Timestamp:   uint32(msg.Timestamp),
			From:        msg.FromUID,
			ChannelId:   msg.ChannelID,
			Topic:       msg.Topic,
			ChannelType: uint32(msg.ChannelType),
			Payload:     msg.Payload,
		})
	}

	msgBatch := &pluginproto.MessageBatch{
		Messages: pluginMessages,
	}

	// 获取频道领导节点ID
	leaderId, err := h.cluster().LeaderIdOfChannel(channelId, channelType)
	if err != nil {
		h.Error("pluginInvokePersistAfter: get channel leader failed", zap.Error(err), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		// 如果获取领导节点失败，直接在本地执行
		h.executePluginPersistAfterLocal(channelId, channelType, msgBatch)
		return
	}

	// 如果当前节点是频道领导节点，直接执行
	if h.isLocalNode(leaderId) {
		h.executePluginPersistAfterLocal(channelId, channelType, msgBatch)
		return
	}

	// 当前节点非频道领导节点，转发请求到领导节点执行
	h.forwardPersistAfterToLeader(leaderId, channelId, channelType, msgBatch)
}

func newlyPersistedMessages(events []*eventbus.Event, messages []wkdb.Message) []wkdb.Message {
	result := make([]wkdb.Message, 0, len(messages))
	messageIndex := 0
	for _, e := range events {
		packet, ok := e.Frame.(*wkproto.SendPacket)
		if !ok || packet == nil || packet.NoPersist || e.ReasonCode != wkproto.ReasonSuccess {
			continue
		}
		if !e.PersistedDuplicate {
			result = append(result, messages[messageIndex])
		}
		messageIndex++
	}
	return result
}

// executePluginPersistAfterLocal 在本地执行插件PersistAfter调用
func (h *Handler) executePluginPersistAfterLocal(channelId string, channelType uint8, msgBatch *pluginproto.MessageBatch) {
	if service.PluginManager == nil {
		return
	}
	plugins := service.PluginManager.Plugins(types.PluginPersistAfter)
	if len(plugins) == 0 {
		return
	}

	timeoutCtx, cancel := h.WithTimeout()
	defer cancel()

	for _, pg := range plugins {
		err := pg.PersistAfter(timeoutCtx, msgBatch)
		if err != nil {
			h.Error("plugin persist after error", zap.Error(err), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		}
	}
}

// forwardPersistAfterToLeader 转发PersistAfter请求到频道领导节点
func (h *Handler) forwardPersistAfterToLeader(leaderId uint64, channelId string, channelType uint8, msgBatch *pluginproto.MessageBatch) {
	// 序列化消息批次
	msgData, err := msgBatch.Marshal()
	if err != nil {
		h.Error("forwardPersistAfterToLeader: marshal message batch failed", zap.Error(err), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		return
	}

	// 使用 ingress.Client 转发请求
	err = h.client.RequestPersistAfter(leaderId, channelId, channelType, msgData)
	if err != nil {
		h.Error("forwardPersistAfterToLeader: request failed", zap.Error(err), zap.Uint64("leaderId", leaderId), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
	}
}

// 转换成存储消息
func (h *Handler) toPersistMessages(channelId string, channelType uint8, events []*eventbus.Event) []wkdb.Message {
	persists := make([]wkdb.Message, 0, len(events))
	for _, e := range events {
		sendPacket := e.Frame.(*wkproto.SendPacket)
		if sendPacket.NoPersist || e.ReasonCode != wkproto.ReasonSuccess {
			continue
		}

		msg := wkdb.Message{
			RecvPacket: wkproto.RecvPacket{
				Framer: wkproto.Framer{
					RedDot:    sendPacket.Framer.RedDot,
					SyncOnce:  sendPacket.Framer.SyncOnce,
					NoPersist: sendPacket.Framer.NoPersist,
				},
				Setting:     sendPacket.Setting,
				MessageID:   e.MessageId,
				MessageSeq:  uint32(e.MessageSeq),
				ClientMsgNo: sendPacket.ClientMsgNo,
				ClientSeq:   sendPacket.ClientSeq,
				FromUID:     e.Conn.Uid,
				ChannelID:   channelId,
				ChannelType: channelType,
				Expire:      sendPacket.Expire,
				Timestamp:   int32(time.Now().Unix()),
				Topic:       sendPacket.Topic,
				StreamNo:    sendPacket.StreamNo,
				Payload:     sendPacket.Payload,
			},
		}
		persists = append(persists, msg)
	}
	return persists
}

// applyPersistResults validates the entire response before changing an event.
// Positional mapping supports several attempts of the same logical message in
// one batch while leaving non-persistent and permission-denied events alone.
func applyPersistResults(events []*eventbus.Event, messages []wkdb.Message, results rafttypes.ProposeRespSet) error {
	if len(messages) != len(results) {
		return fmt.Errorf("incomplete persistence result")
	}
	eligible := make([]*eventbus.Event, 0, len(messages))
	for _, e := range events {
		if packet, ok := e.Frame.(*wkproto.SendPacket); ok && packet != nil && !packet.NoPersist && e.ReasonCode == wkproto.ReasonSuccess {
			eligible = append(eligible, e)
		}
	}
	if len(eligible) != len(results) {
		return fmt.Errorf("persistence event count mismatch")
	}
	for i, r := range results {
		if r == nil || r.Id != uint64(eligible[i].MessageId) || r.Id != uint64(messages[i].MessageID) || r.CanonicalID == 0 || r.Index == 0 || r.Index > uint64(^uint32(0)) {
			return fmt.Errorf("invalid persistence result")
		}
	}
	for i, r := range results {
		eligible[i].MessageId = int64(r.CanonicalID)
		eligible[i].MessageSeq = r.Index
		eligible[i].PersistedDuplicate = r.Duplicate
		messages[i].MessageID = int64(r.CanonicalID)
		messages[i].MessageSeq = uint32(r.Index)
	}
	return nil
}
