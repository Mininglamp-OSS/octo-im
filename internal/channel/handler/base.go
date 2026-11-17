package handler

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/common"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

type NodeDeps struct {
	NodeId       uint64
	ChannelBus   eventbus.IChannel
	Cluster      icluster.ICluster
	Store        *store.Store
	Permission   *service.PermissionService
	TagManager   service.ITagManager
	Conversation service.IConversationManager
	UserBus      eventbus.IUser
	PusherBus    eventbus.IPusher

	PostCommitBudget    *eventbus.PostCommitBudget
	LegacyInboundBudget *eventbus.PostCommitBudget
	TransientBudget     *eventbus.PostCommitBudget
}

type handlerGlobals struct {
	opts      *options.Options
	cluster   icluster.ICluster
	store     *store.Store
	perm      *service.PermissionService
	tagMgr    service.ITagManager
	convMgr   service.IConversationManager
	pluginMgr service.IPluginManager
	webhook   service.IWebhook
}

type Handler struct {
	wklog.Log
	client              *ingress.Client
	commonService       *common.Service
	forwardCapabilities forward.CapabilityCache
	forwardGate         *forward.Gate
	boundNodeId         uint64
	boundChannelBus     eventbus.IChannel
	boundCluster        icluster.ICluster
	boundStore          *store.Store
	boundPermission     *service.PermissionService
	boundTagManager     service.ITagManager
	boundConversation   service.IConversationManager
	boundUser           eventbus.IUser
	boundPusher         eventbus.IPusher

	boundPostCommitBudget    *eventbus.PostCommitBudget
	boundLegacyInboundBudget *eventbus.PostCommitBudget
	boundTransientBudget     *eventbus.PostCommitBudget

	latchedGlobals atomic.Pointer[handlerGlobals]

	seqTracker  StageSeqTracker
	recoveryMgr *RecoveryManager
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
	h.latchGlobals()
	h.recoveryMgr = NewRecoveryManager(h.executeRecoveredItem)
	h.routes()
	return h
}

func (h *Handler) latchGlobals() {
	if h == nil {
		return
	}
	h.latchedGlobals.Store(&handlerGlobals{
		opts:      options.G,
		cluster:   service.Cluster,
		store:     service.Store,
		perm:      service.Permission,
		tagMgr:    service.TagManager,
		convMgr:   service.ConversationManager,
		pluginMgr: service.PluginManager,
		webhook:   service.Webhook,
	})
}

func (h *Handler) RegisterGlobalPartAHooks() {
	if h == nil {
		return
	}
	eventbus.RegisterPartATelemetryProvider(h.SnapshotTelemetry)
	eventbus.RegisterPersistedRecoveryFallback(func(channelID string, channelType uint8, ev *eventbus.Event) bool {
		if ev == nil || ev.MessageSeq == 0 {
			return false
		}
		stage := RecoveryStageChannelForward
		if ev.Type == eventbus.EventChannelWebhook {
			stage = RecoveryStageWebhook
		} else if ev.Type == eventbus.EventChannelDistribute {
			stage = RecoveryStageDistribute
		}
		return h.recoveryMgr.Enqueue(channelID, channelType, stage, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
	})
}

func (h *Handler) SnapshotTelemetry() eventbus.PartATelemetrySnapshot {
	pcB, pcI, pcRej := h.postCommitBudget().Stats()
	lgB, lgI, _ := h.legacyInboundBudget().Stats()
	trB, trI, _ := h.transientBudget().Stats()
	recCh, recItems, cooldownProbes := h.recoveryMgr.Inspect()
	stMetrics := h.seqTracker.Metrics()
	return eventbus.PartATelemetrySnapshot{
		PostCommitInflightItems:       pcI,
		PostCommitInflightBytes:       pcB,
		LegacyInboundInflightItems:    lgI,
		LegacyInboundInflightBytes:    lgB,
		TransientInflightItems:        trI,
		TransientInflightBytes:        trB,
		PostCommitBackpressureRejects: pcRej,
		LegacyForwardDroppedTotal:     eventbus.LegacyForwardDroppedTotal.Load(),
		RecoveryChannelsActive:        recCh,
		RecoveryItemsActive:           recItems,
		RecoveryCooldownProbesTotal:   cooldownProbes,
		SeqTrackerActiveOOOWindows:    stMetrics.ActiveOOOWindows,
		SeqTrackerActiveSlotExts:      stMetrics.ActiveSlotExts,
		SeqTrackerExtraSlotBytes:      stMetrics.ExtraSlotBytes,
		SeqTrackerShardSaturatedTotal: stMetrics.ShardSaturatedTotal,
		SeqTrackerEvictedChannels:     stMetrics.EvictedChannels,
	}
}

func (h *Handler) BindNode(nodeId uint64, chBus eventbus.IChannel) {
	if h == nil {
		return
	}
	h.boundNodeId = nodeId
	h.boundChannelBus = chBus
}

func (h *Handler) BindNodeDeps(deps NodeDeps) {
	if h == nil {
		return
	}
	h.boundNodeId = deps.NodeId
	h.boundChannelBus = deps.ChannelBus
	h.boundCluster = deps.Cluster
	h.boundStore = deps.Store
	h.boundPermission = deps.Permission
	h.boundTagManager = deps.TagManager
	h.boundConversation = deps.Conversation
	h.boundUser = deps.UserBus
	h.boundPusher = deps.PusherBus
	h.boundPostCommitBudget = deps.PostCommitBudget
	h.boundLegacyInboundBudget = deps.LegacyInboundBudget
	h.boundTransientBudget = deps.TransientBudget
	if h.client != nil && deps.Cluster != nil {
		h.client.BindCluster(deps.Cluster)
	}
	if h.commonService != nil {
		h.commonService.BindDeps(deps.NodeId, deps.Cluster, deps.TagManager, h.client)
	}
}

func (h *Handler) postCommitBudget() *eventbus.PostCommitBudget {
	if h != nil && h.boundPostCommitBudget != nil {
		return h.boundPostCommitBudget
	}
	return eventbus.DefaultPostCommitBudget
}

func (h *Handler) legacyInboundBudget() *eventbus.PostCommitBudget {
	if h != nil && h.boundLegacyInboundBudget != nil {
		return h.boundLegacyInboundBudget
	}
	return eventbus.DefaultLegacyInboundBudget
}

func (h *Handler) transientBudget() *eventbus.PostCommitBudget {
	if h != nil && h.boundTransientBudget != nil {
		return h.boundTransientBudget
	}
	return eventbus.DefaultTransientBudget
}

func (h *Handler) opts() *options.Options {
	if h != nil {
		if g := h.latchedGlobals.Load(); g != nil && g.opts != nil {
			return g.opts
		}
	}
	return options.G
}

func (h *Handler) localNodeId() uint64 {
	if h != nil && h.boundNodeId != 0 {
		return h.boundNodeId
	}
	if opts := h.opts(); opts != nil {
		return opts.Cluster.NodeId
	}
	return 0
}

func (h *Handler) isLocalNode(nodeId uint64) bool {
	if h != nil && h.boundNodeId != 0 {
		return h.boundNodeId == nodeId
	}
	opts := h.opts()
	return opts != nil && opts.IsLocalNode(nodeId)
}

func (h *Handler) channelBus() eventbus.IChannel {
	if h != nil && h.boundChannelBus != nil {
		return h.boundChannelBus
	}
	if eventbus.Channel != nil {
		return eventbus.Channel
	}
	return nil
}

func (h *Handler) tryAddEventToChannelBus(channelId string, channelType uint8, ev *eventbus.Event) bool {
	chBus := h.channelBus()
	if chBus == nil {
		return false
	}
	if ta, ok := chBus.(interface {
		TryAddEvent(string, uint8, *eventbus.Event) bool
	}); ok {
		return ta.TryAddEvent(channelId, channelType, ev)
	}
	chBus.AddEvent(channelId, channelType, ev)
	return true
}

func (h *Handler) tryAddBatchToChannelBus(channelId string, channelType uint8, events []*eventbus.Event) bool {
	chBus := h.channelBus()
	if chBus == nil {
		return false
	}
	if ta, ok := chBus.(interface {
		TryAddBatch(string, uint8, []*eventbus.Event) bool
	}); ok {
		return ta.TryAddBatch(channelId, channelType, events)
	}
	for _, ev := range events {
		chBus.AddEvent(channelId, channelType, ev)
	}
	return true
}

func (h *Handler) cluster() icluster.ICluster {
	if h != nil {
		if h.boundCluster != nil {
			return h.boundCluster
		}
		if g := h.latchedGlobals.Load(); g != nil && g.cluster != nil {
			return g.cluster
		}
	}
	return service.Cluster
}

func (h *Handler) store() *store.Store {
	if h != nil {
		if h.boundStore != nil {
			return h.boundStore
		}
		if g := h.latchedGlobals.Load(); g != nil && g.store != nil {
			return g.store
		}
	}
	return service.Store
}

func (h *Handler) permissionService() *service.PermissionService {
	if h != nil {
		if h.boundPermission != nil {
			return h.boundPermission
		}
		if g := h.latchedGlobals.Load(); g != nil && g.perm != nil {
			return g.perm
		}
	}
	return service.Permission
}

func (h *Handler) tagManager() service.ITagManager {
	if h != nil {
		if h.boundTagManager != nil {
			return h.boundTagManager
		}
		if g := h.latchedGlobals.Load(); g != nil && g.tagMgr != nil {
			return g.tagMgr
		}
	}
	return service.TagManager
}

func (h *Handler) conversationManager() service.IConversationManager {
	if h != nil {
		if h.boundConversation != nil {
			return h.boundConversation
		}
		if g := h.latchedGlobals.Load(); g != nil && g.convMgr != nil {
			return g.convMgr
		}
	}
	return service.ConversationManager
}

func (h *Handler) pluginManager() service.IPluginManager {
	if h != nil {
		if g := h.latchedGlobals.Load(); g != nil {
			return g.pluginMgr
		}
	}
	return service.PluginManager
}

func (h *Handler) webhookService() service.IWebhook {
	if h != nil {
		if g := h.latchedGlobals.Load(); g != nil {
			return g.webhook
		}
	}
	return service.Webhook
}

func (h *Handler) userAuthedConns(uid string) []*eventbus.Conn {
	if h != nil && h.boundUser != nil {
		return h.boundUser.AuthedConnsByUid(uid)
	}
	if eventbus.User != nil {
		return eventbus.User.AuthedConnsByUid(uid)
	}
	return nil
}

func (h *Handler) pushEvents(events []*eventbus.Event, advance bool) {
	if len(events) == 0 {
		return
	}
	if h != nil && h.boundPusher != nil {
		id := h.boundPusher.RandHandlerId()
		for _, ev := range events {
			h.boundPusher.AddEvent(id, ev)
		}
		if advance {
			h.boundPusher.Advance(id)
		}
		return
	}
	if eventbus.Pusher != nil {
		id := eventbus.Pusher.AddEvents(events)
		if advance {
			eventbus.Pusher.Advance(id)
		}
	}
}

func (h *Handler) ensureForwardableConn(ev *eventbus.Event) {
	if ev == nil || ev.Conn != nil {
		return
	}
	devID := ""
	if opts := h.opts(); opts != nil {
		devID = opts.SystemDeviceId
	}
	ev.Conn = &eventbus.Conn{
		DeviceId: devID,
	}
}

func retainTicketOrNil(tk *eventbus.PostCommitTicket) *eventbus.PostCommitTicket {
	if tk == nil {
		return nil
	}
	return tk.Retain()
}

func (h *Handler) recordPluginSideEffect(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	k, ok := MakeChannelWatermarkKey(channelId, channelType)
	if !ok {
		return false
	}
	return h.seqTracker.RecordCommittedForce(k, StagePluginPersistAfter, messageSeq)
}

func (h *Handler) recordWebhookSideEffect(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	k, ok := MakeChannelWatermarkKey(channelId, channelType)
	if !ok {
		return false
	}
	return h.seqTracker.RecordCommittedForce(k, StageWebhookNotify, messageSeq)
}

func (h *Handler) recordDistributeSideEffect(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	k, ok := MakeChannelWatermarkKey(channelId, channelType)
	if !ok {
		return false
	}
	return h.seqTracker.RecordCommittedForce(k, StageChannelDistribute, messageSeq)
}

func (h *Handler) hasDistributed(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	k, ok := MakeChannelWatermarkKey(channelId, channelType)
	if !ok {
		return false
	}
	return h.seqTracker.HasCommitted(k, StageChannelDistribute, messageSeq)
}

func (h *Handler) InspectDistributeWatermark(channelId string, channelType uint8) (contiguous uint64, aboveLen int, spanCount int, channelCount int) {
	if h == nil {
		return 0, 0, 0, 0
	}
	contig, above, spans, total := h.seqTracker.InspectChannelStage(channelId, channelType, StageChannelDistribute)
	if contig > 0 || spans > 0 {
		return contig, above, spans, total
	}
	return h.seqTracker.InspectChannelStage(channelId, channelType, StagePersistEnqueued)
}

func (h *Handler) InspectRecoveryQueue() (channels int, inMemoryEvents int, compactedEvents int, dormantChannels int) {
	if h == nil || h.recoveryMgr == nil {
		return 0, 0, 0, 0
	}
	ch, items, _ := h.recoveryMgr.Inspect()
	return ch, items, 0, 0
}

func (h *Handler) NotifyRecoveryWakeup() {
	if h == nil {
		return
	}
	h.forwardCapabilities.InvalidateAll()
	if h.recoveryMgr != nil {
		h.recoveryMgr.NotifyExternalWakeup()
	}
}

func (h *Handler) TriggerPersistedRecovery() {
	h.NotifyRecoveryWakeup()
}

func (h *Handler) EnqueuePersistedRecovery(channelId string, channelType uint8, ev *eventbus.Event) bool {
	if h == nil || h.recoveryMgr == nil || ev == nil || ev.MessageSeq == 0 {
		if ev != nil && ev.Ticket != nil {
			ev.Ticket.Release()
			ev.Ticket = nil
		}
		return false
	}
	stage := RecoveryStageChannelForward
	if ev.Type == eventbus.EventChannelWebhook {
		stage = RecoveryStageWebhook
	} else if ev.Type == eventbus.EventChannelDistribute {
		stage = RecoveryStageDistribute
	}
	return h.recoveryMgr.Enqueue(channelId, channelType, stage, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
}

func (h *Handler) DrainAndStop(timeout time.Duration) {
	if h == nil || h.recoveryMgr == nil {
		return
	}
	h.recoveryMgr.DrainAndStop(timeout)
}

func (h *Handler) Stop() {
	h.DrainAndStop(0)
}

func (h *Handler) routes() {
	eventbus.RegisterChannelHandlers(eventbus.EventChannelOnSend, h.onSend)
	eventbus.RegisterChannelHandlers(eventbus.EventChannelWebhook, h.webhook)
	eventbus.RegisterChannelHandlers(eventbus.EventChannelDistribute, h.distribute)
}

func (h *Handler) OnMessage(m *proto.Message) {
	switch msgType(m.MsgType) {
	case msgForwardChannelEvent:
		h.onForwardChannelEvent(m)
	}
}

func (h *Handler) OnEvent(ctx *eventbus.ChannelContext) {
	h.latchGlobals()
	if h.isLocalNode(ctx.SlotLeaderId) || h.notForwardToLeader(ctx.EventType) {
		if h.boundNodeId != 0 {
			switch ctx.EventType {
			case eventbus.EventChannelOnSend:
				h.onSend(ctx)
			case eventbus.EventChannelWebhook:
				h.webhook(ctx)
			case eventbus.EventChannelDistribute:
				h.distribute(ctx)
			default:
				eventbus.ExecuteChannelEvent(ctx)
			}
			return
		}
		eventbus.ExecuteChannelEvent(ctx)
	} else {
		h.forwardsToNode(ctx.SlotLeaderId, ctx.ChannelId, ctx.ChannelType, ctx.Events)
	}
}

func (h *Handler) notForwardToLeader(eventType eventbus.EventType) bool {
	switch eventType {
	case eventbus.EventChannelWebhook,
		eventbus.EventChannelDistribute:
		return true
	}
	return false
}

func canForwardChannelSendBatch(events []*eventbus.Event) bool {
	if forward.OnlyRetryableSends(events) {
		return true
	}
	if len(events) == 0 {
		return false
	}
	for _, event := range events {
		if event == nil || event.Conn == nil || event.Type != eventbus.EventChannelOnSend {
			return false
		}
		packet, ok := event.Frame.(*wkproto.SendPacket)
		if !ok || packet == nil || strings.TrimSpace(packet.ClientMsgNo) == "" {
			return false
		}
	}
	return true
}

func hasPersistedEvents(events []*eventbus.Event) bool {
	for _, event := range events {
		if event != nil && event.MessageSeq > 0 {
			return true
		}
	}
	return false
}

func releaseEventTickets(events []*eventbus.Event) {
	for _, ev := range events {
		if ev != nil && ev.Ticket != nil {
			ev.Ticket.Release()
			ev.Ticket = nil
		}
	}
}

func (h *Handler) retainPersistedForwardFailures(channelId string, channelType uint8, events []*eventbus.Event) {
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if ev.MessageSeq == 0 {
			if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
			continue
		}
		if h.hasDistributed(channelId, channelType, ev.MessageSeq) {
			if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
			continue
		}
		stage := RecoveryStageChannelForward
		if ev.Type == eventbus.EventChannelDistribute && strings.TrimSpace(ev.TagKey) != "" {
			stage = RecoveryStageDistribute
		}
		_ = h.recoveryMgr.Enqueue(channelId, channelType, stage, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
	}
}

// executeRecoveredItem runs inside RecoveryManager worker pool without putting unacked items back onto blind queues.
func (h *Handler) executeRecoveredItem(
	ctx context.Context,
	channelID string,
	channelType uint8,
	item RecoveryItem,
) (remainingSlots eventbus.SlotBitset, allSlots eventbus.SlotBitset, updatedEv *eventbus.Event, done bool) {
	ev := item.Event
	if ev == nil || ev.MessageSeq == 0 {
		return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true
	}
	wmKey, ok := MakeChannelWatermarkKey(channelID, channelType)
	if !ok {
		return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true
	}
	h.ensureForwardableConn(ev)

	switch item.Stage {
	case RecoveryStagePluginAfter:
		if !h.hasPersistAfterPlugins() {
			_ = h.seqTracker.RecordCommittedForce(wmKey, StagePluginPersistAfter, ev.MessageSeq)
			ev.PluginDone = true
			return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true
		}
		skip, leased := h.seqTracker.BeginStage(wmKey, StagePluginPersistAfter, ev.MessageSeq)
		if skip {
			if h.seqTracker.HasCommitted(wmKey, StagePluginPersistAfter, ev.MessageSeq) {
				ev.PluginDone = true
				return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true
			}
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		if !leased {
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		msgs := h.toPersistMessages(channelID, channelType, []*eventbus.Event{ev})
		if err := h.pluginInvokePersistAfterChecked(channelID, channelType, msgs); err != nil {
			h.seqTracker.AbortStage(wmKey, StagePluginPersistAfter, ev.MessageSeq)
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		h.seqTracker.CommitStage(wmKey, StagePluginPersistAfter, ev.MessageSeq)
		ev.PluginDone = true
		return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true

	case RecoveryStageWebhook:
		opts := h.opts()
		whSvc := h.webhookService()
		if opts == nil || !opts.WebhookOn(types.EventMsgNotify) || whSvc == nil {
			_ = h.seqTracker.RecordCommittedForce(wmKey, StageWebhookNotify, ev.MessageSeq)
			ev.WebhookDone = true
			return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true
		}
		skip, leased := h.seqTracker.BeginStage(wmKey, StageWebhookNotify, ev.MessageSeq)
		if skip {
			if h.seqTracker.HasCommitted(wmKey, StageWebhookNotify, ev.MessageSeq) {
				ev.WebhookDone = true
				return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true
			}
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		if !leased {
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		msgs := h.toPersistMessages(channelID, channelType, []*eventbus.Event{ev})
		if err := whSvc.AppendMessageOfNotifyQueue(msgs); err != nil {
			h.seqTracker.AbortStage(wmKey, StageWebhookNotify, ev.MessageSeq)
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		h.seqTracker.CommitStage(wmKey, StageWebhookNotify, ev.MessageSeq)
		ev.WebhookDone = true
		return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true

	case RecoveryStageChannelForward:
		if h.hasDistributed(channelID, channelType, ev.MessageSeq) {
			return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true
		}
		cl := h.cluster()
		if cl == nil {
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		slotLeaderID, err := cl.SlotLeaderIdOfChannel(channelID, channelType)
		if err != nil || slotLeaderID == 0 {
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		if h.isLocalNode(slotLeaderID) {
			rem, all, okDone := h.executeSingleEventDistributeRecovery(ctx, slotLeaderID, channelID, channelType, ev, item.PendingSlots, item.AllRecipientSlots)
			return rem, all, ev, okDone
		}
		clone := ev.Clone()
		clone.ForwardHops = 0
		clone.ForwardDeadline = time.Now().Add(forward.Budget).UnixMilli()
		if err := h.forwardPersistedBatchToSlotLeader(ctx, slotLeaderID, channelID, channelType, []*eventbus.Event{clone}); err != nil {
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true

	case RecoveryStageDistribute:
		cl := h.cluster()
		if cl == nil {
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		slotLeaderID, err := cl.SlotLeaderIdOfChannel(channelID, channelType)
		if err != nil || slotLeaderID == 0 {
			return item.PendingSlots, item.AllRecipientSlots, ev, false
		}
		rem, all, okDone := h.executeSingleEventDistributeRecovery(ctx, slotLeaderID, channelID, channelType, ev, item.PendingSlots, item.AllRecipientSlots)
		return rem, all, ev, okDone
	}
	return eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev, true
}

func (h *Handler) forwardPersistedBatchToSlotLeader(
	ctx context.Context,
	nodeId uint64,
	channelId string,
	channelType uint8,
	events []*eventbus.Event,
) error {
	cl := h.cluster()
	localNode := h.localNodeId()
	permSvc := h.permissionService()

	wireEvents := make([]*eventbus.Event, 0, len(events))
	persistedLegacySlotLeader := permSvc != nil && permSvc.CheckOrProbeLegacyPeer(nodeId, channelId, channelType)

	for _, ev := range events {
		if ev == nil || !shouldDistributePersistResult(ev) || h.hasDistributed(channelId, channelType, ev.MessageSeq) {
			continue
		}
		clone := ev.Clone()
		h.ensureForwardableConn(clone)
		clone.ForwardHops = 0
		clone.ForwardDeadline = time.Now().Add(forward.Budget).UnixMilli()
		if persistedLegacySlotLeader {
			clone.Type = eventbus.EventChannelDistribute
			clone.SourceNodeId = 0
			clone.TagKey = ""
		}
		wireEvents = append(wireEvents, clone)
	}
	if len(wireEvents) == 0 {
		return nil
	}
	req := &forwardChannelEventReq{
		channelId:   channelId,
		channelType: channelType,
		fromNode:    localNode,
		events:      wireEvents,
	}
	data, err := req.encode()
	if err != nil {
		return err
	}
	legacy := func(targetNode uint64) error {
		return h.sendToNode(targetNode, &proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: data})
	}
	target := func() uint64 {
		if cl == nil {
			return 0
		}
		leader, err := cl.SlotLeaderIdOfChannel(channelId, channelType)
		if err != nil {
			return 0
		}
		return leader
	}
	err = forward.RequestOnCluster(cl, forward.ChannelPath, data, wireEvents, target, localNode, h.acceptForward, &h.forwardCapabilities, legacy)
	if err != nil {
		return err
	}
	if persistedLegacySlotLeader {
		for _, ev := range wireEvents {
			if ev != nil && ev.MessageSeq > 0 {
				_ = h.recordDistributeSideEffect(channelId, channelType, ev.MessageSeq)
			}
		}
	}
	return nil
}

func (h *Handler) forwardsToNode(nodeId uint64, channelId string, channelType uint8, events []*eventbus.Event) {
	persistedBatch := hasPersistedEvents(events)
	for _, ev := range events {
		if ev != nil && ev.MessageSeq > 0 {
			h.ensureForwardableConn(ev)
		}
	}
	canForwardSend := canForwardChannelSendBatch(events)
	localNode := h.localNodeId()
	cl := h.cluster()

	// Transient (MessageSeq == 0) non-SEND events (e.g. NoPersist EventChannelDistribute / online CMD)
	// preserve the exact retained transport contract (0 v1 requests, 1 Send).
	if !persistedBatch && !canForwardSend {
		req := &forwardChannelEventReq{
			channelId:   channelId,
			channelType: channelType,
			fromNode:    localNode,
			events:      events,
		}
		data, err := req.encode()
		if err != nil {
			h.Error("forwardToLeader: encode failed", zap.Error(err))
			releaseEventTickets(events)
			forward.Fail(events, err)
			return
		}
		if err = h.sendToNode(nodeId, &proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: data}); err != nil {
			h.Error("channel forwarding through retained transport failed", zap.Error(err), zap.String("channelId", channelId))
			forward.Fail(events, err)
		}
		releaseEventTickets(events)
		return
	}

	if !h.forwardGate.TryAcquire() {
		err := forward.ErrUnavailable
		h.Error("channel forwarding concurrency limit reached", zap.Error(err), zap.String("channelId", channelId))
		forward.Fail(events, err)
		if persistedBatch {
			if len(events) > 0 && events[0].Type == eventbus.EventChannelOnSend {
				h.executePersistedLocalSideEffects(channelId, channelType, events)
			}
			h.retainPersistedForwardFailures(channelId, channelType, events)
		} else {
			releaseEventTickets(events)
		}
		return
	}
	defer h.forwardGate.Release()

	if persistedBatch && len(events) > 0 && events[0].Type == eventbus.EventChannelOnSend {
		h.executePersistedLocalSideEffects(channelId, channelType, events)
	}

	if persistedBatch {
		err := h.forwardPersistedBatchToSlotLeader(context.Background(), nodeId, channelId, channelType, events)
		if err != nil {
			h.Error("persisted channel forwarding failed", zap.Error(err), zap.String("channelId", channelId))
			h.retainPersistedForwardFailures(channelId, channelType, events)
			return
		}
		releaseEventTickets(events)
		return
	}

	// Pre-persistence retryable SEND batch (MessageSeq == 0).
	req := &forwardChannelEventReq{
		channelId:   channelId,
		channelType: channelType,
		fromNode:    localNode,
		events:      events,
	}
	data, err := req.encode()
	if err != nil {
		h.Error("forwardToLeader: encode failed", zap.Error(err))
		releaseEventTickets(events)
		forward.Fail(events, err)
		return
	}
	legacy := func(targetNode uint64) error {
		return h.sendToNode(targetNode, &proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: data})
	}
	target := func() uint64 {
		if len(events) > 0 && h.notForwardToLeader(events[0].Type) {
			return nodeId
		}
		if cl == nil {
			return 0
		}
		leader, err := cl.SlotLeaderIdOfChannel(channelId, channelType)
		if err != nil {
			return 0
		}
		return leader
	}
	err = forward.RequestOnCluster(cl, forward.ChannelPath, data, events, target, localNode, h.acceptForward, &h.forwardCapabilities, legacy)
	if err != nil {
		h.Error("channel forwarding failed", zap.Error(err), zap.String("channelId", channelId))
		forward.Fail(events, err)
	}
	releaseEventTickets(events)
}

func (h *Handler) sendToNode(toNodeId uint64, msg *proto.Message) error {
	return h.cluster().Send(toNodeId, msg)
}

// onForwardChannelEvent handles fire-and-forget msgForwardChannelEvent (3002) packets,
// including rolling-upgrade 21cd8f88 HTTP/Plugin sends and legacy distributions.
func (h *Handler) onForwardChannelEvent(m *proto.Message) {
	req := &forwardChannelEventReq{}
	if err := req.decode(m.Content); err != nil {
		h.Error("onForwardChannelEvent: decode failed", zap.Error(err))
		return
	}
	chBus := h.channelBus()
	if chBus == nil {
		return
	}
	admitted := false
	for _, e := range req.events {
		if e == nil {
			continue
		}
		if isDurableForwardEvent(e) {
			tks, ok := h.postCommitBudget().TryReserve(req.channelId, []*eventbus.Event{e})
			if !ok {
				tks, ok = h.legacyInboundBudget().TryReserve(req.channelId, []*eventbus.Event{e})
			}
			if !ok {
				dropped := eventbus.LegacyForwardDroppedTotal.Add(1)
				h.Error("legacy 21cd8f88 inbound event dropped: 24MiB post-commit + legacy spillover budget exhausted",
					zap.String("channelId", req.channelId),
					zap.Uint8("channelType", req.channelType),
					zap.Uint64("fromNode", req.fromNode),
					zap.Uint64("messageSeq", e.MessageSeq),
					zap.Uint64("legacyDroppedTotal", dropped))
				continue
			}
			e.Ticket = tks[0]
			if h.tryAddEventToChannelBus(req.channelId, req.channelType, e) {
				admitted = true
				continue
			}
			if e.MessageSeq > 0 && h.EnqueuePersistedRecovery(req.channelId, req.channelType, e) {
				continue
			}
			e.Ticket.Release()
			e.Ticket = nil
			continue
		}

		tks, ok := h.transientBudget().TryReserve(req.channelId, []*eventbus.Event{e})
		if !ok {
			continue
		}
		e.Ticket = tks[0]
		if h.tryAddEventToChannelBus(req.channelId, req.channelType, e) {
			admitted = true
		} else {
			e.Ticket.Release()
			e.Ticket = nil
		}
	}
	if admitted {
		chBus.Advance(req.channelId, req.channelType)
	}
}

func (h *Handler) WithTimeout() (context.Context, context.CancelFunc) {
	timeout := 5 * time.Second
	if opts := h.opts(); opts != nil && opts.Channel.ProcessTimeout > 0 {
		timeout = opts.Channel.ProcessTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

var _ = wkdb.Message{}
