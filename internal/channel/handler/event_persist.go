package handler

import (
	"errors"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

func (h *Handler) ensureEventHasTicket(channelId string, e *eventbus.Event) *eventbus.PostCommitTicket {
	if e == nil {
		return nil
	}
	if e.Ticket != nil && !e.Ticket.IsReleased() {
		return e.Ticket
	}
	if h == nil {
		return nil
	}
	tks, ok := h.postCommitBudget().TryReserve(channelId, []*eventbus.Event{e})
	if !ok || len(tks) == 0 {
		return nil
	}
	e.Ticket = tks[0]
	return e.Ticket
}

// 消息持久化
func (h *Handler) persist(ctx *eventbus.ChannelContext) {
	events := ctx.Events
	for _, e := range events {
		if e != nil {
			e.Track.Record(track.PositionChannelPersist)
		}
	}

	wmKey, hasWMKey := MakeChannelWatermarkKey(ctx.ChannelId, ctx.ChannelType)

	// Separate pre-persisted events from unpersisted events, reserving PostCommitTickets
	// BEFORE Raft AppendMessages for any unpersisted persistent SEND that lacks a pre-attached ticket.
	unpersistedEvents := make([]*eventbus.Event, 0, len(events))
	for _, e := range events {
		if e == nil {
			continue
		}
		if e.MessageSeq > 0 && e.ReasonCode == wkproto.ReasonSuccess {
			continue
		}
		sp, ok := e.Frame.(*wkproto.SendPacket)
		if !ok || sp == nil {
			if e.Ticket != nil {
				e.Ticket.Release()
				e.Ticket = nil
			}
			continue
		}
		if e.ReasonCode != wkproto.ReasonSuccess {
			if e.Ticket != nil {
				e.Ticket.Release()
				e.Ticket = nil
			}
			continue
		}
		if !sp.NoPersist && e.Ticket == nil {
			tks, okReserve := h.postCommitBudget().TryReserve(ctx.ChannelId, []*eventbus.Event{e})
			if !okReserve {
				e.ReasonCode = wkproto.ReasonNodeNotMatch
				continue
			}
			e.Ticket = tks[0]
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
		} else {
			if err = applyPersistResults(unpersistedEvents, persists, results); err != nil {
				h.Error("invalid persistence response", zap.Error(err))
				markPersistFailure(unpersistedEvents, err)
			}
		}
	}

	// Release tickets on any unpersisted event that failed AppendMessages.
	for _, e := range unpersistedEvents {
		if e == nil {
			continue
		}
		if sp, ok := e.Frame.(*wkproto.SendPacket); ok && sp != nil && !sp.NoPersist {
			if e.MessageSeq == 0 || e.ReasonCode != wkproto.ReasonSuccess {
				if e.Ticket != nil {
					e.Ticket.Release()
					e.Ticket = nil
				}
			}
		}
	}

	// Stage 0: PluginPersistAfter (commit watermark ONLY on err == nil; retain ticket in RecoveryManager on error or capacity retry).
	h.runStage0PluginPersistAfter(ctx.ChannelId, ctx.ChannelType, wmKey, hasWMKey, events)

	opts := h.opts()
	if opts != nil && opts.Logger.TraceOn {
		for _, e := range events {
			if e != nil {
				h.Trace("message persistence result", "persist", zap.Int64("messageId", e.MessageId),
					zap.Uint64("messageSeq", e.MessageSeq), zap.String("channelId", ctx.ChannelId),
					zap.Uint8("channelType", ctx.ChannelType), zap.String("reason", e.ReasonCode.String()))
			}
		}
	}

	chBus := h.channelBus()
	webhookEnabled := opts != nil && opts.WebhookOn(types.EventMsgNotify)
	queuedToBus := false

	// Stage 1: WebhookNotify & Stage 2/3: Distribute (each gets an explicit Acquired handle on e.Ticket's entry).
	for _, e := range events {
		if e == nil {
			continue
		}
		sp, ok := e.Frame.(*wkproto.SendPacket)
		if !ok || sp == nil || e.ReasonCode != wkproto.ReasonSuccess {
			if e.Ticket != nil {
				e.Ticket.Release()
				e.Ticket = nil
			}
			continue
		}

		// Stage 1: EventChannelWebhook
		if !webhookEnabled {
			e.HasStageMeta = true
			e.WebhookStageStatus = eventbus.StageStatusNotRequired
			e.WebhookDone = true
		} else if shouldRunPersistSideEffects(e) && !sp.NoPersist && !e.WebhookDone {
			alreadyWebhook := hasWMKey && h.seqTracker.HasCommitted(wmKey, StageWebhookNotify, e.MessageSeq)
			if alreadyWebhook {
				e.HasStageMeta = true
				e.WebhookStageStatus = eventbus.StageStatusCompleted
				e.WebhookDone = true
			} else {
				whClone := e.Clone()
				whClone.ForwardDeadline, whClone.ForwardHops = 0, 0
				whClone.Type = eventbus.EventChannelWebhook
				whClone.Ticket = retainTicketOrNil(e.Ticket)
				if chBus != nil && h.tryAddEventToChannelBus(ctx.ChannelId, ctx.ChannelType, whClone) {
					queuedToBus = true
				} else if whClone.MessageSeq > 0 {
					_ = h.ensureEventHasTicket(ctx.ChannelId, whClone)
					if !h.recoveryMgr.Enqueue(ctx.ChannelId, ctx.ChannelType, RecoveryStageWebhook, eventbus.SlotBitset{}, eventbus.SlotBitset{}, whClone) && whClone.Ticket != nil {
						whClone.Ticket.Release()
						whClone.Ticket = nil
					}
				} else if whClone.Ticket != nil {
					whClone.Ticket.Release()
					whClone.Ticket = nil
				}
			}
		}

		// Stage 2/3: EventChannelDistribute
		if shouldDistributePersistResult(e) {
			alreadyDist := e.MessageSeq > 0 && hasWMKey &&
				(h.seqTracker.HasCommitted(wmKey, StageChannelDistribute, e.MessageSeq) ||
					h.seqTracker.HasCommitted(wmKey, StagePersistEnqueued, e.MessageSeq))
			if !alreadyDist {
				distClone := e.Clone()
				distClone.ForwardDeadline, distClone.ForwardHops = 0, 0
				distClone.Type = eventbus.EventChannelDistribute
				distClone.Ticket = retainTicketOrNil(e.Ticket)
				dispatched := false
				if chBus != nil && h.tryAddEventToChannelBus(ctx.ChannelId, ctx.ChannelType, distClone) {
					queuedToBus = true
					dispatched = true
				} else if distClone.MessageSeq > 0 {
					if !sp.NoPersist {
						_ = h.ensureEventHasTicket(ctx.ChannelId, distClone)
					}
					dispatched = h.recoveryMgr.Enqueue(ctx.ChannelId, ctx.ChannelType, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, distClone)
					if !dispatched && distClone.Ticket != nil {
						distClone.Ticket.Release()
						distClone.Ticket = nil
					}
				} else if distClone.Ticket != nil {
					distClone.Ticket.Release()
					distClone.Ticket = nil
				}
				if dispatched && e.MessageSeq > 0 && hasWMKey {
					_ = h.seqTracker.RecordCommittedForce(wmKey, StagePersistEnqueued, e.MessageSeq)
				}
			}
		}

		if e.Ticket != nil {
			e.Ticket.Release()
			e.Ticket = nil
		}
	}

	if queuedToBus && chBus != nil {
		chBus.Advance(ctx.ChannelId, ctx.ChannelType)
	}
}

func (h *Handler) hasPersistAfterPlugins() bool {
	pm := h.pluginManager()
	if pm == nil {
		return false
	}
	return len(pm.Plugins(types.PluginPersistAfter)) > 0
}

func (h *Handler) enqueueStage0PluginRecovery(channelId string, channelType uint8, e *eventbus.Event) {
	if h == nil || h.recoveryMgr == nil || e == nil || e.MessageSeq == 0 {
		return
	}
	_ = h.ensureEventHasTicket(channelId, e)
	recEv := e.Clone()
	recEv.PluginDone = false
	recEv.HasStageMeta = true
	recEv.PluginStageStatus = eventbus.StageStatusPending
	recEv.Ticket = retainTicketOrNil(e.Ticket)
	if !h.recoveryMgr.Enqueue(channelId, channelType, RecoveryStagePluginAfter, eventbus.SlotBitset{}, eventbus.SlotBitset{}, recEv) && recEv.Ticket != nil {
		recEv.Ticket.Release()
		recEv.Ticket = nil
	}
	// Mark Stage 0 as RetryOwned by this node so a downstream remote slot leader does not execute Stage 0 concurrently (A2).
	e.HasStageMeta = true
	e.PluginStageStatus = eventbus.StageStatusRetryOwned
	e.StageOwnerNodeId = h.localNodeId()
	e.StageOwnerGen++
	e.PluginDone = true
}

func (h *Handler) runStage0PluginPersistAfter(
	channelId string,
	channelType uint8,
	wmKey ChannelWatermarkKey,
	hasWMKey bool,
	events []*eventbus.Event,
) {
	hasPlugins := h.hasPersistAfterPlugins()
	var leasedEvents []*eventbus.Event
	for _, e := range events {
		if e == nil || e.MessageSeq == 0 || e.ReasonCode != wkproto.ReasonSuccess || e.PersistedDuplicate {
			continue
		}
		if e.PluginDone || e.PluginStageStatus == eventbus.StageStatusCompleted || e.PluginStageStatus == eventbus.StageStatusNotRequired {
			e.HasStageMeta = true
			if e.PluginStageStatus == eventbus.StageStatusPending {
				e.PluginStageStatus = eventbus.StageStatusCompleted
			}
			e.PluginDone = true
			continue
		}
		if e.PluginStageStatus == eventbus.StageStatusRetryOwned && e.StageOwnerNodeId != 0 && !h.isLocalNode(e.StageOwnerNodeId) {
			e.HasStageMeta = true
			e.PluginDone = true
			continue
		}
		sp, ok := e.Frame.(*wkproto.SendPacket)
		if !ok || sp == nil || sp.NoPersist {
			continue
		}
		if !hasPlugins {
			if hasWMKey {
				_ = h.seqTracker.RecordCommittedForce(wmKey, StagePluginPersistAfter, e.MessageSeq)
			}
			e.HasStageMeta = true
			e.PluginStageStatus = eventbus.StageStatusNotRequired
			e.PluginDone = true
			continue
		}
		if !hasWMKey {
			leasedEvents = append(leasedEvents, e)
			continue
		}
		status := h.seqTracker.BeginStageChecked(wmKey, StagePluginPersistAfter, e.MessageSeq)
		switch status {
		case StageBeginAlreadyCommitted:
			e.HasStageMeta = true
			e.PluginStageStatus = eventbus.StageStatusCompleted
			e.PluginDone = true
		case StageBeginInFlight, StageBeginCapacityRetry:
			h.enqueueStage0PluginRecovery(channelId, channelType, e)
		case StageBeginLeased:
			e.HasStageMeta = true
			e.PluginStageStatus = eventbus.StageStatusRunning
			leasedEvents = append(leasedEvents, e)
		}
	}
	if len(leasedEvents) == 0 {
		return
	}
	msgs := h.toPersistMessages(channelId, channelType, leasedEvents)
	err := h.pluginInvokePersistAfterChecked(channelId, channelType, msgs)
	if err != nil {
		for _, e := range leasedEvents {
			if hasWMKey {
				h.seqTracker.AbortStage(wmKey, StagePluginPersistAfter, e.MessageSeq)
			}
			h.enqueueStage0PluginRecovery(channelId, channelType, e)
		}
		return
	}
	for _, e := range leasedEvents {
		if hasWMKey {
			h.seqTracker.CommitStage(wmKey, StagePluginPersistAfter, e.MessageSeq)
		}
		e.HasStageMeta = true
		e.PluginStageStatus = eventbus.StageStatusCompleted
		e.PluginDone = true
	}
}

func (h *Handler) executePersistedLocalSideEffects(channelId string, channelType uint8, events []*eventbus.Event) {
	wmKey, hasWMKey := MakeChannelWatermarkKey(channelId, channelType)
	h.runStage0PluginPersistAfter(channelId, channelType, wmKey, hasWMKey, events)

	chBus := h.channelBus()
	opts := h.opts()
	webhookEnabled := opts != nil && opts.WebhookOn(types.EventMsgNotify)
	if !webhookEnabled {
		for _, e := range events {
			if e != nil && e.MessageSeq > 0 && e.ReasonCode == wkproto.ReasonSuccess && !e.PersistedDuplicate {
				e.HasStageMeta = true
				e.WebhookStageStatus = eventbus.StageStatusNotRequired
				e.WebhookDone = true
			}
		}
		return
	}

	queued := false
	for _, e := range events {
		if e == nil || e.MessageSeq == 0 || e.ReasonCode != wkproto.ReasonSuccess || e.PersistedDuplicate {
			continue
		}
		if e.WebhookDone || e.WebhookStageStatus == eventbus.StageStatusCompleted || e.WebhookStageStatus == eventbus.StageStatusNotRequired {
			e.HasStageMeta = true
			if e.WebhookStageStatus == eventbus.StageStatusPending {
				e.WebhookStageStatus = eventbus.StageStatusCompleted
			}
			e.WebhookDone = true
			continue
		}
		if e.WebhookStageStatus == eventbus.StageStatusRetryOwned && e.StageOwnerNodeId != 0 && !h.isLocalNode(e.StageOwnerNodeId) {
			e.HasStageMeta = true
			e.WebhookDone = true
			continue
		}
		sp, ok := e.Frame.(*wkproto.SendPacket)
		if !ok || sp == nil || sp.NoPersist {
			continue
		}
		if hasWMKey && h.seqTracker.HasCommitted(wmKey, StageWebhookNotify, e.MessageSeq) {
			e.HasStageMeta = true
			e.WebhookStageStatus = eventbus.StageStatusCompleted
			e.WebhookDone = true
			continue
		}
		cloneEvent := e.Clone()
		cloneEvent.ForwardDeadline, cloneEvent.ForwardHops = 0, 0
		cloneEvent.Type = eventbus.EventChannelWebhook
		cloneEvent.WebhookDone = false
		cloneEvent.WebhookStageStatus = eventbus.StageStatusPending
		cloneEvent.Ticket = retainTicketOrNil(e.Ticket)
		// Mark Stage 1 as RetryOwned by this node on the outgoing forwarded event so remote leader does not duplicate Stage 1 (A2).
		e.HasStageMeta = true
		e.WebhookStageStatus = eventbus.StageStatusRetryOwned
		if e.StageOwnerNodeId == 0 {
			e.StageOwnerNodeId = h.localNodeId()
		}
		e.StageOwnerGen++
		e.WebhookDone = true
		if chBus != nil && h.tryAddEventToChannelBus(channelId, channelType, cloneEvent) {
			queued = true
		} else {
			_ = h.ensureEventHasTicket(channelId, cloneEvent)
			if !h.recoveryMgr.Enqueue(channelId, channelType, RecoveryStageWebhook, eventbus.SlotBitset{}, eventbus.SlotBitset{}, cloneEvent) && cloneEvent.Ticket != nil {
				cloneEvent.Ticket.Release()
				cloneEvent.Ticket = nil
			}
		}
	}
	if queued && chBus != nil {
		chBus.Advance(channelId, channelType)
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
	return event.ReasonCode == wkproto.ReasonSuccess
}

func (h *Handler) pluginInvokePersistAfter(channelId string, channelType uint8, msgs []wkdb.Message) {
	_ = h.pluginInvokePersistAfterChecked(channelId, channelType, msgs)
}

func (h *Handler) pluginInvokePersistAfterChecked(channelId string, channelType uint8, msgs []wkdb.Message) error {
	pm := h.pluginManager()
	if len(msgs) == 0 || pm == nil {
		return nil
	}
	plugins := pm.Plugins(types.PluginPersistAfter)
	if len(plugins) == 0 {
		return nil
	}

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

	cl := h.cluster()
	if cl == nil {
		return h.executePluginPersistAfterLocal(channelId, channelType, msgBatch)
	}

	leaderId, err := resolvePluginChannelLeader(cl, channelId, channelType)
	if err != nil || leaderId == 0 {
		h.Error("pluginInvokePersistAfter: get channel leader failed", zap.Error(err), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		return h.executePluginPersistAfterLocal(channelId, channelType, msgBatch)
	}

	if h.isLocalNode(leaderId) {
		return h.executePluginPersistAfterLocal(channelId, channelType, msgBatch)
	}

	return h.forwardPersistAfterToLeader(leaderId, channelId, channelType, msgBatch)
}

func resolvePluginChannelLeader(cl icluster.ICluster, channelId string, channelType uint8) (leaderId uint64, err error) {
	if cl == nil {
		return 0, errors.New("cluster is nil")
	}
	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		leaderId, err = cl.LeaderIdOfChannel(channelId, channelType)
	}()
	if panicked {
		defer func() {
			if recover() != nil {
				leaderId = 0
				err = errors.New("SlotLeaderIdOfChannel panicked")
			}
		}()
		return cl.SlotLeaderIdOfChannel(channelId, channelType)
	}
	return leaderId, err
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

func (h *Handler) executePluginPersistAfterLocal(channelId string, channelType uint8, msgBatch *pluginproto.MessageBatch) error {
	pm := h.pluginManager()
	if pm == nil {
		return nil
	}
	plugins := pm.Plugins(types.PluginPersistAfter)
	if len(plugins) == 0 {
		return nil
	}

	timeoutCtx, cancel := h.WithTimeout()
	defer cancel()

	var errs []error
	for _, pg := range plugins {
		err := pg.PersistAfter(timeoutCtx, msgBatch)
		if err != nil {
			h.Error("plugin persist after error", zap.Error(err), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (h *Handler) forwardPersistAfterToLeader(leaderId uint64, channelId string, channelType uint8, msgBatch *pluginproto.MessageBatch) error {
	msgData, err := msgBatch.Marshal()
	if err != nil {
		h.Error("forwardPersistAfterToLeader: marshal message batch failed", zap.Error(err), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		return err
	}

	err = h.client.RequestPersistAfter(leaderId, channelId, channelType, msgData)
	if err != nil {
		h.Error("forwardPersistAfterToLeader: request failed", zap.Error(err), zap.Uint64("leaderId", leaderId), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		return err
	}
	return nil
}

func (h *Handler) toPersistMessages(channelId string, channelType uint8, events []*eventbus.Event) []wkdb.Message {
	persists := make([]wkdb.Message, 0, len(events))
	for _, e := range events {
		if e == nil {
			continue
		}
		sendPacket, ok := e.Frame.(*wkproto.SendPacket)
		if !ok || sendPacket == nil || sendPacket.NoPersist || e.ReasonCode != wkproto.ReasonSuccess {
			continue
		}
		fromUID := ""
		if e.Conn != nil {
			fromUID = e.Conn.Uid
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
				FromUID:     fromUID,
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
