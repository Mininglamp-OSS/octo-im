package handler

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/common"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

const (
	maxSpansPerChannel              = 64
	maxTrackedWatermarkChannels     = 16384
	maxRecoveryChannels             = 1024
	maxInMemoryRecoveryPerChannel   = 64
	maxTotalInMemoryRecoveryEvents  = 2048
	maxActiveForwardRecoveryRetries = 32
)

type channelWatermarkKey struct {
	channelId   string
	channelType uint8
}

type seqSpan struct {
	lo uint64
	hi uint64
}

type channelSeqWatermark struct {
	contiguous uint64
	above      map[uint64]struct{}
	spans      []seqSpan
	lastAccess uint64
}

type seqWatermarkTracker struct {
	mu        sync.Mutex
	accessSeq uint64
	channels  map[channelWatermarkKey]*channelSeqWatermark
}

func (t *seqWatermarkTracker) hasSeen(channelId string, channelType uint8, messageSeq uint64) bool {
	if t == nil || messageSeq == 0 {
		return false
	}
	k := channelWatermarkKey{channelId: channelId, channelType: channelType}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.channels == nil {
		return false
	}
	wm := t.channels[k]
	if wm == nil {
		return false
	}
	for _, sp := range wm.spans {
		if messageSeq < sp.lo {
			break
		}
		if messageSeq <= sp.hi {
			return true
		}
	}
	if wm.above != nil {
		_, exists := wm.above[messageSeq]
		return exists
	}
	return false
}

func (t *seqWatermarkTracker) evictOldestChannelLocked() {
	var (
		oldestKey channelWatermarkKey
		oldestSeq uint64
		found     bool
	)
	for k, wm := range t.channels {
		if !found || wm.lastAccess < oldestSeq {
			oldestKey = k
			oldestSeq = wm.lastAccess
			found = true
		}
	}
	if found {
		delete(t.channels, oldestKey)
	}
}

func insertAndMergeSpan(spans []seqSpan, seq uint64) []seqSpan {
	n := len(spans)
	if n == 0 {
		return []seqSpan{{lo: seq, hi: seq}}
	}
	// Fast path: extends or follows the highest span.
	last := &spans[n-1]
	if seq >= last.lo && seq <= last.hi {
		return spans
	}
	if seq == last.hi+1 {
		last.hi = seq
		return spans
	}
	if seq > last.hi+1 {
		spans = append(spans, seqSpan{lo: seq, hi: seq})
		if len(spans) > maxSpansPerChannel {
			spans = coalesceClosestSpanPair(spans)
		}
		return spans
	}

	// Binary search insertion position for out-of-order / historical sequence.
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		if spans[mid].lo < seq {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo > 0 && seq <= spans[lo-1].hi {
		return spans
	}
	if lo < n && seq >= spans[lo].lo && seq <= spans[lo].hi {
		return spans
	}
	mergeLeft := lo > 0 && spans[lo-1].hi+1 == seq
	mergeRight := lo < n && seq+1 == spans[lo].lo
	switch {
	case mergeLeft && mergeRight:
		spans[lo-1].hi = spans[lo].hi
		spans = append(spans[:lo], spans[lo+1:]...)
	case mergeLeft:
		spans[lo-1].hi = seq
	case mergeRight:
		spans[lo].lo = seq
	default:
		spans = append(spans, seqSpan{})
		copy(spans[lo+1:], spans[lo:])
		spans[lo] = seqSpan{lo: seq, hi: seq}
		if len(spans) > maxSpansPerChannel {
			spans = coalesceClosestSpanPair(spans)
		}
	}
	return spans
}

func coalesceClosestSpanPair(spans []seqSpan) []seqSpan {
	if len(spans) <= 1 {
		return spans
	}
	bestIdx := 0
	bestGap := spans[1].lo - spans[0].hi
	for i := 1; i < len(spans)-1; i++ {
		gap := spans[i+1].lo - spans[i].hi
		if gap < bestGap {
			bestGap = gap
			bestIdx = i
		}
	}
	spans[bestIdx].hi = spans[bestIdx+1].hi
	return append(spans[:bestIdx+1], spans[bestIdx+2:]...)
}

func (t *seqWatermarkTracker) record(channelId string, channelType uint8, messageSeq uint64) bool {
	if t == nil || messageSeq == 0 {
		return false
	}
	k := channelWatermarkKey{channelId: channelId, channelType: channelType}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.channels == nil {
		t.channels = make(map[channelWatermarkKey]*channelSeqWatermark, 256)
	}
	wm := t.channels[k]
	if wm == nil {
		if len(t.channels) >= maxTrackedWatermarkChannels {
			t.evictOldestChannelLocked()
		}
		wm = &channelSeqWatermark{}
		t.channels[k] = wm
	}
	t.accessSeq++
	wm.lastAccess = t.accessSeq

	for _, sp := range wm.spans {
		if messageSeq < sp.lo {
			break
		}
		if messageSeq <= sp.hi {
			return true
		}
	}
	wm.spans = insertAndMergeSpan(wm.spans, messageSeq)
	wm.contiguous = wm.spans[len(wm.spans)-1].hi
	wm.above = nil
	return false
}

func (t *seqWatermarkTracker) inspect(channelId string, channelType uint8) (contiguous uint64, aboveLen int, spanCount int, channelCount int) {
	if t == nil {
		return 0, 0, 0, 0
	}
	k := channelWatermarkKey{channelId: channelId, channelType: channelType}
	t.mu.Lock()
	defer t.mu.Unlock()
	channelCount = len(t.channels)
	if t.channels == nil {
		return 0, 0, 0, 0
	}
	wm := t.channels[k]
	if wm == nil {
		return 0, 0, 0, channelCount
	}
	return wm.contiguous, len(wm.above), len(wm.spans), channelCount
}

type channelForwardRecoveryEntry struct {
	channelId        string
	channelType      uint8
	events           []*eventbus.Event
	seenSeqs         map[uint64]struct{}
	minCompactedSeq  uint64
	maxCompactedSeq  uint64
	compactedCount   int
	compactedEvType  eventbus.EventType
	attempts         uint32
	dormant          bool
	lastUpdatedUnixM int64
}

type persistedForwardRecoveryQueue struct {
	mu          sync.Mutex
	entries     map[channelWatermarkKey]*channelForwardRecoveryEntry
	totalEvents int
	timer       *time.Timer
	stopped     bool
}

type NodeDeps struct {
	NodeId        uint64
	ChannelBus    eventbus.IChannel
	Cluster       icluster.ICluster
	Store         *store.Store
	Permission    *service.PermissionService
	TagManager    service.ITagManager
	Conversation  service.IConversationManager
	UserBus       eventbus.IUser
	PusherBus     eventbus.IPusher
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

	pluginWebhookTracker   seqWatermarkTracker
	distributeTracker      seqWatermarkTracker
	localDistributeTracker seqWatermarkTracker
	recovery               persistedForwardRecoveryQueue
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
	if h.client != nil && deps.Cluster != nil {
		h.client.BindCluster(deps.Cluster)
	}
	if h.commonService != nil {
		h.commonService.BindDeps(deps.NodeId, deps.Cluster, deps.TagManager, h.client)
	}
}

func (h *Handler) localNodeId() uint64 {
	if h != nil && h.boundNodeId != 0 {
		return h.boundNodeId
	}
	if options.G != nil {
		return options.G.Cluster.NodeId
	}
	return 0
}

func (h *Handler) isLocalNode(nodeId uint64) bool {
	if h != nil && h.boundNodeId != 0 {
		return h.boundNodeId == nodeId
	}
	return options.G != nil && options.G.IsLocalNode(nodeId)
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

func (h *Handler) cluster() icluster.ICluster {
	if h != nil && h.boundCluster != nil {
		return h.boundCluster
	}
	return service.Cluster
}

func (h *Handler) store() *store.Store {
	if h != nil && h.boundStore != nil {
		return h.boundStore
	}
	return service.Store
}

func (h *Handler) permissionService() *service.PermissionService {
	if h != nil && h.boundPermission != nil {
		return h.boundPermission
	}
	return service.Permission
}

func (h *Handler) tagManager() service.ITagManager {
	if h != nil && h.boundTagManager != nil {
		return h.boundTagManager
	}
	return service.TagManager
}

func (h *Handler) conversationManager() service.IConversationManager {
	if h != nil && h.boundConversation != nil {
		return h.boundConversation
	}
	return service.ConversationManager
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

func (h *Handler) recordPluginWebhookSideEffect(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	return h.pluginWebhookTracker.record(channelId, channelType, messageSeq)
}

func (h *Handler) recordDistributeSideEffect(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	return h.distributeTracker.record(channelId, channelType, messageSeq)
}

func (h *Handler) hasDistributed(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	return h.distributeTracker.hasSeen(channelId, channelType, messageSeq)
}

func (h *Handler) recordLocalDistributeSideEffect(channelId string, channelType uint8, messageSeq uint64) bool {
	if h == nil || messageSeq == 0 {
		return false
	}
	_ = h.distributeTracker.record(channelId, channelType, messageSeq)
	return h.localDistributeTracker.record(channelId, channelType, messageSeq)
}

func (h *Handler) InspectDistributeWatermark(channelId string, channelType uint8) (contiguous uint64, aboveLen int, spanCount int, channelCount int) {
	if h == nil {
		return 0, 0, 0, 0
	}
	return h.distributeTracker.inspect(channelId, channelType)
}

func (h *Handler) InspectRecoveryQueue() (channels int, inMemoryEvents int, compactedEvents int, dormantChannels int) {
	if h == nil {
		return 0, 0, 0, 0
	}
	h.recovery.mu.Lock()
	defer h.recovery.mu.Unlock()
	channels = len(h.recovery.entries)
	inMemoryEvents = h.recovery.totalEvents
	for _, entry := range h.recovery.entries {
		compactedEvents += entry.compactedCount
		if entry.dormant {
			dormantChannels++
		}
	}
	return channels, inMemoryEvents, compactedEvents, dormantChannels
}

func (h *Handler) Stop() {
	if h == nil {
		return
	}
	h.recovery.mu.Lock()
	h.recovery.stopped = true
	if h.recovery.timer != nil {
		h.recovery.timer.Stop()
		h.recovery.timer = nil
	}
	h.recovery.mu.Unlock()
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
	if h.isLocalNode(ctx.SlotLeaderId) || h.notForwardToLeader(ctx.EventType) {
		// 执行本地事件 ,频道永远在自己的槽领导节点上执行逻辑。
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

// 不需要转发给领导的事件
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

func (h *Handler) requeuePersistedForwardFailures(channelId string, channelType uint8, events []*eventbus.Event, countHop bool) {
	chBus := h.channelBus()
	if chBus == nil {
		return
	}
	nowMs := time.Now().UnixMilli()
	defaultDeadline := time.Now().Add(forward.Budget).UnixMilli()
	var fastRequeue []*eventbus.Event
	var overflowRecovery []*eventbus.Event
	for _, ev := range events {
		if ev == nil || ev.MessageSeq == 0 {
			continue
		}
		if h.hasDistributed(channelId, channelType, ev.MessageSeq) {
			continue
		}
		nextHops := ev.ForwardHops
		if countHop {
			nextHops++
		}
		deadline := ev.ForwardDeadline
		if deadline == 0 {
			deadline = defaultDeadline
		}
		clone := ev.Clone()
		if nextHops < forward.MaxHops && deadline > nowMs {
			clone.ForwardHops = nextHops
			clone.ForwardDeadline = deadline
			fastRequeue = append(fastRequeue, clone)
		} else {
			clone.ForwardHops = 0
			clone.ForwardDeadline = defaultDeadline
			overflowRecovery = append(overflowRecovery, clone)
		}
	}
	if len(fastRequeue) > 0 {
		for _, clone := range fastRequeue {
			chBus.AddEvent(channelId, channelType, clone)
		}
		time.AfterFunc(25*time.Millisecond, func() {
			if bus := h.channelBus(); bus != nil {
				bus.Advance(channelId, channelType)
			}
		})
	}
	if len(overflowRecovery) > 0 {
		h.enqueueBoundedForwardRecovery(channelId, channelType, overflowRecovery)
	}
}

func (h *Handler) enqueueBoundedForwardRecovery(channelId string, channelType uint8, events []*eventbus.Event) {
	if h == nil || len(events) == 0 {
		return
	}
	k := channelWatermarkKey{channelId: channelId, channelType: channelType}
	nowMs := time.Now().UnixMilli()

	h.recovery.mu.Lock()
	defer h.recovery.mu.Unlock()
	if h.recovery.stopped {
		return
	}
	if h.recovery.entries == nil {
		h.recovery.entries = make(map[channelWatermarkKey]*channelForwardRecoveryEntry, 64)
	}
	entry := h.recovery.entries[k]
	if entry == nil {
		if len(h.recovery.entries) >= maxRecoveryChannels {
			var oldestKey channelWatermarkKey
			var oldestTs int64
			found := false
			for ck, ce := range h.recovery.entries {
				if !found || ce.lastUpdatedUnixM < oldestTs {
					oldestKey = ck
					oldestTs = ce.lastUpdatedUnixM
					found = true
				}
			}
			if found {
				evicted := h.recovery.entries[oldestKey]
				h.recovery.totalEvents -= len(evicted.events)
				delete(h.recovery.entries, oldestKey)
			}
		}
		entry = &channelForwardRecoveryEntry{
			channelId:   channelId,
			channelType: channelType,
			seenSeqs:    make(map[uint64]struct{}),
		}
		h.recovery.entries[k] = entry
	}
	entry.lastUpdatedUnixM = nowMs

	for _, ev := range events {
		if ev == nil || ev.MessageSeq == 0 {
			continue
		}
		if _, dup := entry.seenSeqs[ev.MessageSeq]; dup {
			continue
		}
		if len(entry.seenSeqs) < maxInMemoryRecoveryPerChannel*4 {
			entry.seenSeqs[ev.MessageSeq] = struct{}{}
		}
		if len(entry.events) < maxInMemoryRecoveryPerChannel && h.recovery.totalEvents < maxTotalInMemoryRecoveryEvents {
			entry.events = append(entry.events, ev)
			h.recovery.totalEvents++
		} else {
			if entry.minCompactedSeq == 0 || ev.MessageSeq < entry.minCompactedSeq {
				entry.minCompactedSeq = ev.MessageSeq
			}
			if ev.MessageSeq > entry.maxCompactedSeq {
				entry.maxCompactedSeq = ev.MessageSeq
			}
			entry.compactedCount++
			entry.compactedEvType = ev.Type
		}
	}

	entry.attempts++
	if entry.attempts > maxActiveForwardRecoveryRetries {
		entry.dormant = true
		return
	}
	h.scheduleRecoveryTimerLocked(entry.attempts)
}

func (h *Handler) scheduleRecoveryTimerLocked(attempts uint32) {
	if h.recovery.stopped || h.recovery.timer != nil {
		return
	}
	shift := attempts
	if shift > 5 {
		shift = 5
	}
	delay := time.Duration(25*(1<<shift)) * time.Millisecond
	if delay > 800*time.Millisecond {
		delay = 800 * time.Millisecond
	}
	h.recovery.timer = time.AfterFunc(delay, h.drainBoundedForwardRecovery)
}

func (h *Handler) TriggerPersistedRecovery() {
	if h == nil {
		return
	}
	h.recovery.mu.Lock()
	for _, entry := range h.recovery.entries {
		entry.dormant = false
		entry.attempts = 0
	}
	hasActive := len(h.recovery.entries) > 0
	if hasActive {
		h.scheduleRecoveryTimerLocked(0)
	}
	h.recovery.mu.Unlock()
}

func (h *Handler) drainBoundedForwardRecovery() {
	if h == nil {
		return
	}
	type channelBatch struct {
		channelId       string
		channelType     uint8
		events          []*eventbus.Event
		minCompactedSeq uint64
		maxCompactedSeq uint64
		compactedEvType eventbus.EventType
	}
	var batches []channelBatch
	var nextAttempts uint32
	hasRemainingActive := false

	h.recovery.mu.Lock()
	h.recovery.timer = nil
	if h.recovery.stopped {
		h.recovery.mu.Unlock()
		return
	}
	for k, entry := range h.recovery.entries {
		if entry.dormant {
			continue
		}
		b := channelBatch{
			channelId:       entry.channelId,
			channelType:     entry.channelType,
			events:          entry.events,
			minCompactedSeq: entry.minCompactedSeq,
			maxCompactedSeq: entry.maxCompactedSeq,
			compactedEvType: entry.compactedEvType,
		}
		h.recovery.totalEvents -= len(entry.events)
		entry.events = nil
		entry.minCompactedSeq = 0
		entry.maxCompactedSeq = 0
		entry.compactedCount = 0
		entry.seenSeqs = make(map[uint64]struct{})
		if entry.attempts >= maxActiveForwardRecoveryRetries {
			entry.dormant = true
		} else {
			delete(h.recovery.entries, k)
		}
		batches = append(batches, b)
	}
	for _, entry := range h.recovery.entries {
		if !entry.dormant && (len(entry.events) > 0 || entry.compactedCount > 0) {
			hasRemainingActive = true
			if entry.attempts > nextAttempts {
				nextAttempts = entry.attempts
			}
		}
	}
	if hasRemainingActive {
		h.scheduleRecoveryTimerLocked(nextAttempts)
	}
	h.recovery.mu.Unlock()

	chBus := h.channelBus()
	if chBus == nil {
		return
	}
	st := h.store()
	defaultDeadline := time.Now().Add(forward.Budget).UnixMilli()
	for _, b := range batches {
		queued := false
		for _, ev := range b.events {
			if ev == nil || h.hasDistributed(b.channelId, b.channelType, ev.MessageSeq) {
				continue
			}
			ev.ForwardHops = 0
			ev.ForwardDeadline = defaultDeadline
			chBus.AddEvent(b.channelId, b.channelType, ev)
			queued = true
		}
		if b.minCompactedSeq > 0 && b.maxCompactedSeq >= b.minCompactedSeq && st != nil {
			evType := b.compactedEvType
			if evType == 0 {
				evType = eventbus.EventChannelOnSend
			}
			for seq := b.minCompactedSeq; seq <= b.maxCompactedSeq; seq++ {
				if h.hasDistributed(b.channelId, b.channelType, seq) {
					continue
				}
				msg, err := st.LoadMsg(b.channelId, b.channelType, seq)
				if err != nil || (msg.MessageID == 0 && msg.MessageSeq == 0) {
					continue
				}
				devId := ""
				if options.G != nil {
					devId = options.G.SystemDeviceId
				}
				reconstructed := &eventbus.Event{
					Type: evType,
					Conn: &eventbus.Conn{
						Uid:      msg.FromUID,
						DeviceId: devId,
					},
					Frame: &wkproto.SendPacket{
						Framer: wkproto.Framer{
							RedDot:    msg.RedDot,
							SyncOnce:  msg.SyncOnce,
							NoPersist: msg.NoPersist,
						},
						Setting:     msg.Setting,
						Expire:      msg.Expire,
						ClientMsgNo: msg.ClientMsgNo,
						ClientSeq:   msg.ClientSeq,
						ChannelID:   b.channelId,
						ChannelType: b.channelType,
						Topic:       msg.Topic,
						StreamNo:    msg.StreamNo,
						Payload:     msg.Payload,
					},
					MessageId:       msg.MessageID,
					MessageSeq:      uint64(msg.MessageSeq),
					ReasonCode:      wkproto.ReasonSuccess,
					ForwardDeadline: defaultDeadline,
				}
				chBus.AddEvent(b.channelId, b.channelType, reconstructed)
				queued = true
			}
		}
		if queued {
			chBus.Advance(b.channelId, b.channelType)
		}
	}
}

func (h *Handler) forwardsToNode(nodeId uint64, channelId string, channelType uint8, events []*eventbus.Event) {
	persistedBatch := hasPersistedEvents(events)
	canForwardSend := canForwardChannelSendBatch(events)
	localNode := h.localNodeId()

	if !canForwardSend {
		req := &forwardChannelEventReq{
			channelId:   channelId,
			channelType: channelType,
			fromNode:    localNode,
			events:      events,
		}
		data, err := req.encode()
		if err != nil {
			h.Error("forwardToLeader: encode failed", zap.Error(err))
			forward.Fail(events, err)
			return
		}
		// Distribution and webhook work has no client-side retry path. Keep it
		// out of the v1 ambiguity window and retain it in the base transport.
		if err = h.sendToNode(nodeId, &proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: data}); err != nil {
			h.Error("channel forwarding through retained transport failed", zap.Error(err), zap.String("channelId", channelId))
			forward.Fail(events, err)
			if persistedBatch {
				h.requeuePersistedForwardFailures(channelId, channelType, events, true)
			}
		}
		return
	}

	if !h.forwardGate.TryAcquire() {
		err := forward.ErrUnavailable
		h.Error("channel forwarding concurrency limit reached", zap.Error(err), zap.String("channelId", channelId))
		forward.Fail(events, err)
		if persistedBatch {
			h.requeuePersistedForwardFailures(channelId, channelType, events, false)
		}
		return
	}
	defer h.forwardGate.Release()

	wireEvents := events
	persistedLegacySlotLeader := false
	permSvc := h.permissionService()
	if persistedBatch && len(events) > 0 && events[0].Type == eventbus.EventChannelOnSend {
		h.executePersistedLocalSideEffects(channelId, channelType, events)
		if permSvc != nil && permSvc.CheckOrProbeLegacyPeer(nodeId, channelId, channelType) {
			persistedLegacySlotLeader = true
			distEvents := make([]*eventbus.Event, 0, len(events))
			for _, ev := range events {
				if ev == nil || !shouldDistributePersistResult(ev) || h.hasDistributed(channelId, channelType, ev.MessageSeq) {
					continue
				}
				clone := ev.Clone()
				clone.Type = eventbus.EventChannelDistribute
				clone.SourceNodeId = 0
				distEvents = append(distEvents, clone)
			}
			if len(distEvents) == 0 {
				return
			}
			wireEvents = distEvents
		}
	}

	req := &forwardChannelEventReq{
		channelId:   channelId,
		channelType: channelType,
		fromNode:    localNode,
		events:      wireEvents,
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
	target := func() uint64 {
		if len(events) > 0 && h.notForwardToLeader(events[0].Type) && !persistedLegacySlotLeader {
			return nodeId
		}
		cl := h.cluster()
		if cl == nil {
			return 0
		}
		leader, err := cl.SlotLeaderIdOfChannel(channelId, channelType)
		if err != nil {
			return 0
		}
		return leader
	}
	err = forward.RequestCompatible(forward.ChannelPath, data, events, target, localNode, h.acceptForward, &h.forwardCapabilities, legacy)
	if err != nil {
		h.Error("channel forwarding failed", zap.Error(err), zap.String("channelId", channelId))
		forward.Fail(events, err)
		if persistedBatch {
			h.requeuePersistedForwardFailures(channelId, channelType, events, true)
		}
		return
	}
	if persistedLegacySlotLeader {
		for _, ev := range wireEvents {
			if ev != nil && ev.MessageSeq > 0 {
				_ = h.recordDistributeSideEffect(channelId, channelType, ev.MessageSeq)
			}
		}
	}
}

// func (h *Handler) forwardToNode(nodeId uint64, channelId string, channelType uint8, event *eventbus.Event) {
// 	h.forwardsToNode(nodeId, channelId, channelType, []*eventbus.Event{event})
// }

func (h *Handler) sendToNode(toNodeId uint64, msg *proto.Message) error {
	return h.cluster().Send(toNodeId, msg)
}

// 收到转发用户事件
func (h *Handler) onForwardChannelEvent(m *proto.Message) {
	req := &forwardChannelEventReq{}
	err := req.decode(m.Content)
	if err != nil {
		h.Error("onForwardChannelEvent: decode failed", zap.Error(err))
		return
	}

	chBus := h.channelBus()
	if chBus == nil {
		return
	}
	for _, e := range req.events {
		chBus.AddEvent(req.channelId, req.channelType, e)
	}
	chBus.Advance(req.channelId, req.channelType)

}

func (h *Handler) WithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), options.G.Channel.ProcessTimeout)

}
