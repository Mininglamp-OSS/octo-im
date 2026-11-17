package handler

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

// 跳过会话更新的频道类型
var skipConversationUpdateChannelTypes = []uint8{wkproto.ChannelTypeData, wkproto.ChannelTypeTemp, wkproto.ChannelTypeLive}

func safeGetSlotID(cl icluster.ICluster, v string) uint32 {
	if cl == nil {
		return 0
	}
	var slotID uint32
	func() {
		defer func() { _ = recover() }()
		slotID = cl.GetSlotId(v)
	}()
	return slotID
}

func safeVerifyLocalUserSlot(cl icluster.ICluster, slotID uint32, localNode uint64) bool {
	if cl == nil || localNode == 0 {
		return true
	}
	leaderID := localNode
	panicked := false
	func() {
		defer func() {
			if recover() != nil {
				panicked = true
			}
		}()
		leaderID = cl.SlotLeaderId(slotID)
	}()
	if panicked {
		return true
	}
	return leaderID == localNode
}

// 消息分发
func (h *Handler) distribute(ctx *eventbus.ChannelContext) {
	events := ctx.Events
	for _, event := range events {
		if event != nil {
			event.Track.Record(track.PositionChannelDistribute)
		}
	}

	if options.G.IsOnlineCmdChannel(ctx.ChannelId) {
		h.distributeOnlineCmd(ctx)
	} else {
		h.distributeCommon(ctx)
	}
}

// 普通消息分发
func (h *Handler) distributeCommon(ctx *eventbus.ChannelContext) {
	if !h.isLocalNode(ctx.SlotLeaderId) && len(ctx.Events) > 0 && strings.TrimSpace(ctx.Events[0].TagKey) == "" {
		if ctx.SlotLeaderId != 0 {
			h.forwardsToNode(ctx.SlotLeaderId, ctx.ChannelId, ctx.ChannelType, ctx.Events)
		} else {
			h.retainPersistedForwardFailures(ctx.ChannelId, ctx.ChannelType, ctx.Events)
		}
		return
	}

	tag, err := h.getCommonTag(ctx)
	if err != nil || tag == nil {
		if err != nil {
			h.Error("distributeCommon: get or make tag failed", zap.Error(err), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
		} else {
			h.Error("distributeCommon: get or make tag failed, tag is nil", zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
		}
		for _, ev := range ctx.Events {
			if ev == nil {
				continue
			}
			if ev.MessageSeq > 0 {
				_ = h.recoveryMgr.Enqueue(ctx.ChannelId, ctx.ChannelType, RecoveryStageDistribute, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
			} else if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
		}
		return
	}

	for _, event := range ctx.Events {
		if event != nil {
			event.TagKey = tag.Key
		}
	}
	h.distributeByTag(ctx.SlotLeaderId, tag, ctx.ChannelId, ctx.ChannelType, ctx.Events)
}

// cmd消息分发
func (h *Handler) distributeOnlineCmd(ctx *eventbus.ChannelContext) {
	tagKeyEvents := h.groupEventsByTagKey(ctx.Events)
	var err error
	for tagKey, events := range tagKeyEvents {
		if tagKey == "" {
			h.Warn("distributeOnlineCmd: tagKey is nil", zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
			releaseEventTickets(events)
			continue
		}
		var tag *types.Tag
		if h.isLocalNode(ctx.SlotLeaderId) {
			tag = h.tagManager().Get(tagKey)
		} else if ctx.SlotLeaderId != 0 {
			tag, err = h.requestTag(ctx.SlotLeaderId, tagKey)
			if err != nil {
				h.Error("distributeOnlineCmd: request tag failed", zap.Error(err), zap.String("tagKey", tagKey), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
				for _, ev := range events {
					if ev != nil && ev.MessageSeq > 0 {
						_ = h.recoveryMgr.Enqueue(ctx.ChannelId, ctx.ChannelType, RecoveryStageDistribute, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
					} else if ev != nil && ev.Ticket != nil {
						ev.Ticket.Release()
						ev.Ticket = nil
					}
				}
				continue
			}
		}
		if tag == nil {
			h.Error("distributeOnlineCmd: tag not found", zap.String("tagKey", tagKey), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
			releaseEventTickets(events)
			continue
		}
		h.distributeByTag(ctx.SlotLeaderId, tag, ctx.ChannelId, ctx.ChannelType, events)
	}
}

func (h *Handler) groupEventsByTagKey(events []*eventbus.Event) map[string][]*eventbus.Event {
	tagKeyEvents := make(map[string][]*eventbus.Event)
	for _, e := range events {
		if e != nil {
			tagKeyEvents[e.TagKey] = append(tagKeyEvents[e.TagKey], e)
		}
	}
	return tagKeyEvents
}

func computeTagSlotTopology(cl icluster.ICluster, tag *types.Tag) (allSlots eventbus.SlotBitset, nodeMasks map[uint64]eventbus.SlotBitset) {
	nodeMasks = make(map[uint64]eventbus.SlotBitset, len(tag.Nodes))
	for _, node := range tag.Nodes {
		if node == nil {
			continue
		}
		var nm eventbus.SlotBitset
		for _, uid := range node.Uids {
			if uid == "" || (options.G != nil && options.G.IsSystemUid(uid)) {
				continue
			}
			sid := safeGetSlotID(cl, uid)
			nm.Set(sid)
			allSlots.Set(sid)
		}
		nodeMasks[node.LeaderId] = nm
	}
	return allSlots, nodeMasks
}

func (h *Handler) distributePersistedToRemoteNode(
	ctx context.Context,
	toNodeID uint64,
	channelID string,
	channelType uint8,
	events []*eventbus.Event,
	targetMask eventbus.SlotBitset,
) error {
	if len(events) == 0 || toNodeID == 0 {
		return forward.ErrUnavailable
	}
	cl := h.cluster()
	if cl == nil {
		return forward.ErrUnavailable
	}
	mode := h.forwardCapabilities.ProbeOn(ctx, cl, toNodeID)
	if mode != forward.PeerCapPartAV2 && mode != forward.PeerCapBaseV1 {
		return forward.ErrUnavailable
	}

	wireEvents := make([]*eventbus.Event, 0, len(events))
	for _, ev := range events {
		if ev == nil {
			continue
		}
		clone := ev.Clone()
		h.ensureForwardableConn(clone)
		clone.Type = eventbus.EventChannelDistribute
		clone.ForwardHops = 0
		clone.ForwardDeadline = time.Now().Add(forward.Budget).UnixMilli()
		if mode == forward.PeerCapPartAV2 {
			clone.HasTargetSlotMask = true
			clone.TargetSlotMask = targetMask.Clone()
		} else {
			clone.HasTargetSlotMask = false
			clone.TargetSlotMask = eventbus.SlotBitset{}
		}
		wireEvents = append(wireEvents, clone)
	}
	req := &forwardChannelEventReq{
		channelId:   channelID,
		channelType: channelType,
		fromNode:    h.localNodeId(),
		events:      wireEvents,
		hasSlotMask: mode == forward.PeerCapPartAV2,
		slotMask:    targetMask,
	}
	body, err := req.encodeWithTrailer(mode == forward.PeerCapPartAV2)
	if err != nil {
		return err
	}
	target := func() uint64 { return toNodeID }
	return forward.RequestOnCluster(cl, forward.ChannelPath, body, wireEvents, target, h.localNodeId(), h.acceptForward, nil, nil)
}

// deliverLocalUsersForMask delivers events strictly to local node.Uids whose user slot is in allowedMask
// and still owned by localNode (returning succeededMask and migratedAwayMask per V11-2).
func (h *Handler) deliverLocalUsersForMask(
	cl icluster.ICluster,
	localNode uint64,
	tag *types.Tag,
	channelID string,
	channelType uint8,
	events []*eventbus.Event,
	allowedMask eventbus.SlotBitset,
) (succeededMask eventbus.SlotBitset, migratedAwayMask eventbus.SlotBitset) {
	if len(events) == 0 || allowedMask.IsZero() {
		return eventbus.SlotBitset{}, eventbus.SlotBitset{}
	}

	var targetUIDs []string
	for _, node := range tag.Nodes {
		if node == nil || node.LeaderId != localNode {
			continue
		}
		for _, uid := range node.Uids {
			if uid == "" || (options.G != nil && options.G.IsSystemUid(uid)) {
				continue
			}
			sid := safeGetSlotID(cl, uid)
			if !allowedMask.Has(sid) {
				continue
			}
			if !safeVerifyLocalUserSlot(cl, sid, localNode) {
				migratedAwayMask.Set(sid)
				continue
			}
			succeededMask.Set(sid)
			targetUIDs = append(targetUIDs, uid)
		}
	}

	// Any bit in allowedMask that had no matching local UID in tag.Nodes migrated out of localNode's tag slice.
	unmatched := allowedMask.AndNot(succeededMask).AndNot(migratedAwayMask)
	if !unmatched.IsZero() {
		migratedAwayMask = migratedAwayMask.Or(unmatched)
	}

	if len(targetUIDs) == 0 {
		return succeededMask, migratedAwayMask
	}

	var offlineUids []string
	var pushOnlineEvents []*eventbus.Event
	appendOnline := func(uid string, capacity int) {
		for _, event := range events {
			if pushOnlineEvents == nil {
				pushOnlineEvents = make([]*eventbus.Event, 0, len(events)*capacity)
			}
			cloneMsg := event.Clone()
			cloneMsg.ToUid = uid
			cloneMsg.ChannelId = channelID
			cloneMsg.ChannelType = channelType
			cloneMsg.Type = eventbus.EventPushOnline
			pushOnlineEvents = append(pushOnlineEvents, cloneMsg)
		}
	}
	flushOnline := func() {
		if len(pushOnlineEvents) == 0 {
			return
		}
		h.pushEvents(pushOnlineEvents, true)
		pushOnlineEvents = nil
	}

	delivered := make(map[string]bool, len(targetUIDs))
	if service.Presence != nil {
		unknownUids := make([]string, 0, len(targetUIDs))
		for _, uid := range targetUIDs {
			isOnline, _ := h.deviceOnlineStatus(uid)
			if isOnline {
				appendOnline(uid, len(targetUIDs))
				delivered[uid] = true
			} else {
				unknownUids = append(unknownUids, uid)
			}
		}
		flushOnline()
		if len(unknownUids) > 0 {
			recCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			if err := service.Presence.Recover(recCtx, unknownUids); err != nil {
				h.Warn("recipient authority recovery incomplete; delivering to known sessions",
					zap.Error(err), zap.Int("recipientCount", len(targetUIDs)))
			}
			cancel()
		}
	}

	for _, uid := range targetUIDs {
		isOnline, masterIsOnline := h.deviceOnlineStatus(uid)
		if !masterIsOnline && channelType != wkproto.ChannelTypeAgent {
			if offlineUids == nil {
				offlineUids = make([]string, 0, len(targetUIDs))
			}
			offlineUids = append(offlineUids, uid)
		}
		if !isOnline || delivered[uid] {
			continue
		}
		appendOnline(uid, len(targetUIDs))
	}

	if !h.isSkipConversationUpdate(channelType) {
		h.conversationForUsers(channelID, channelType, tag.Key, targetUIDs, events)
	}

	flushOnline()
	if len(offlineUids) > 0 {
		offlineEvents := make([]*eventbus.Event, 0, len(events))
		for _, event := range events {
			if event.Frame == nil || event.Frame.GetFrameType() == wkproto.EVENT {
				continue
			}
			fromUID := ""
			if event.Conn != nil {
				fromUID = event.Conn.Uid
			}
			filteredOfflineUids := make([]string, 0, len(offlineUids))
			for _, offlineUid := range offlineUids {
				if offlineUid != fromUID {
					filteredOfflineUids = append(filteredOfflineUids, offlineUid)
				}
			}
			filteredOfflineUids = wkutil.RemoveRepeatedElement(filteredOfflineUids)
			cloneEvent := event.Clone()
			cloneEvent.OfflineUsers = filteredOfflineUids
			cloneEvent.Type = eventbus.EventPushOffline
			offlineEvents = append(offlineEvents, cloneEvent)
		}
		h.pushEvents(offlineEvents, false)
	}

	return succeededMask, migratedAwayMask
}

func (h *Handler) distributeByTag(slotLeaderId uint64, tag *types.Tag, channelId string, channelType uint8, events []*eventbus.Event) {
	if slotLeaderId == 0 {
		h.Error("distributeByTag: leaderId is 0", zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		for _, ev := range events {
			if ev != nil && ev.MessageSeq > 0 {
				_ = h.recoveryMgr.Enqueue(channelId, channelType, RecoveryStageDistribute, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
			} else if ev != nil && ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
		}
		return
	}

	var transientEvents []*eventbus.Event
	var persistedEvents []*eventbus.Event
	for _, ev := range events {
		if ev == nil {
			continue
		}
		if ev.MessageSeq == 0 {
			transientEvents = append(transientEvents, ev)
		} else {
			persistedEvents = append(persistedEvents, ev)
		}
	}

	if len(transientEvents) > 0 {
		h.distributeTransientByTag(slotLeaderId, tag, channelId, channelType, transientEvents)
		releaseEventTickets(transientEvents)
	}
	if len(persistedEvents) == 0 {
		return
	}

	cl := h.cluster()
	localNode := h.localNodeId()
	wmKey, hasWMKey := MakeChannelWatermarkKey(channelId, channelType)
	allRecipientSlots, nodeSlotMasks := computeTagSlotTopology(cl, tag)

	if allRecipientSlots.IsZero() {
		for _, ev := range persistedEvents {
			if hasWMKey {
				if h.isLocalNode(slotLeaderId) {
					_ = h.seqTracker.RecordCommittedForce(wmKey, StageChannelDistribute, ev.MessageSeq)
				}
				_ = h.seqTracker.RecordCommittedForce(wmKey, StageLocalDistribute, ev.MessageSeq)
			}
			if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
		}
		return
	}

	if h.isLocalNode(slotLeaderId) {
		h.distributePersistedOnChannelLeader(context.Background(), cl, localNode, wmKey, hasWMKey, tag, allRecipientSlots, nodeSlotMasks, channelId, channelType, persistedEvents)
		return
	}
	h.distributePersistedOnRecipientNode(cl, localNode, wmKey, hasWMKey, tag, allRecipientSlots, nodeSlotMasks, channelId, channelType, persistedEvents)
}

func (h *Handler) distributePersistedOnChannelLeader(
	ctx context.Context,
	cl icluster.ICluster,
	localNode uint64,
	wmKey ChannelWatermarkKey,
	hasWMKey bool,
	tag *types.Tag,
	allRecipientSlots eventbus.SlotBitset,
	nodeSlotMasks map[uint64]eventbus.SlotBitset,
	channelID string,
	channelType uint8,
	events []*eventbus.Event,
) {
	type leasedDistEvent struct {
		ev          *eventbus.Event
		deliverMask eventbus.SlotBitset
		succeeded   eventbus.SlotBitset
		leased      bool
	}
	active := make([]leasedDistEvent, 0, len(events))
	for _, ev := range events {
		reqMask := allRecipientSlots
		if ev.HasTargetSlotMask && !ev.TargetSlotMask.IsZero() {
			reqMask = ev.TargetSlotMask.And(allRecipientSlots)
		}
		if reqMask.IsZero() {
			if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
			continue
		}
		if !hasWMKey {
			active = append(active, leasedDistEvent{ev: ev, deliverMask: reqMask})
			continue
		}
		deliv, leased := h.seqTracker.BeginSlotStage(wmKey, StageChannelDistribute, ev.MessageSeq, reqMask)
		if !leased || deliv.IsZero() {
			if h.seqTracker.HasCommitted(wmKey, StageChannelDistribute, ev.MessageSeq) {
				if ev.Ticket != nil {
					ev.Ticket.Release()
					ev.Ticket = nil
				}
			} else {
				_ = h.recoveryMgr.Enqueue(channelID, channelType, RecoveryStageDistribute, reqMask, allRecipientSlots, ev)
			}
			continue
		}
		active = append(active, leasedDistEvent{ev: ev, deliverMask: deliv, leased: true})
	}
	if len(active) == 0 {
		return
	}

	// 1. Remote user-slot leaders: commit remote slot bits ONLY after /wk/forward/channel/v1 returns StatusOK.
	for _, node := range tag.Nodes {
		if node == nil || node.LeaderId == localNode || node.LeaderId == 0 {
			continue
		}
		remoteMask := nodeSlotMasks[node.LeaderId]
		if remoteMask.IsZero() {
			continue
		}
		var batchEvs []*eventbus.Event
		var batchIdx []int
		var unionTargetMask eventbus.SlotBitset
		for i := range active {
			evMask := active[i].deliverMask.And(remoteMask)
			if evMask.IsZero() {
				continue
			}
			if active[i].ev.SourceNodeId != 0 && active[i].ev.SourceNodeId == node.LeaderId {
				active[i].succeeded = active[i].succeeded.Or(evMask)
				continue
			}
			batchEvs = append(batchEvs, active[i].ev)
			batchIdx = append(batchIdx, i)
			unionTargetMask = unionTargetMask.Or(evMask)
		}
		if len(batchEvs) == 0 {
			continue
		}
		if err := h.distributePersistedToRemoteNode(ctx, node.LeaderId, channelID, channelType, batchEvs, unionTargetMask); err == nil {
			for _, idx := range batchIdx {
				evMask := active[idx].deliverMask.And(remoteMask)
				active[idx].succeeded = active[idx].succeeded.Or(evMask)
			}
		} else {
			h.Warn("distributePersistedToRemoteNode failed; retaining failed slot mask for recovery",
				zap.Error(err),
				zap.Uint64("toNode", node.LeaderId),
				zap.String("channelId", channelID),
				zap.Int("batchLen", len(batchEvs)))
		}
	}

	// 2. Local user-slot delivery on channel leader.
	localMask := nodeSlotMasks[localNode]
	if !localMask.IsZero() {
		for i := range active {
			evLocalReq := active[i].deliverMask.And(localMask)
			if evLocalReq.IsZero() {
				continue
			}
			localDeliv := evLocalReq
			localLeased := false
			if hasWMKey {
				localDeliv, localLeased = h.seqTracker.BeginSlotStage(wmKey, StageLocalDistribute, active[i].ev.MessageSeq, evLocalReq)
				if !localLeased || localDeliv.IsZero() {
					if h.seqTracker.HasCommitted(wmKey, StageLocalDistribute, active[i].ev.MessageSeq) {
						active[i].succeeded = active[i].succeeded.Or(evLocalReq)
					}
					continue
				}
			}
			succLocal, _ := h.deliverLocalUsersForMask(cl, localNode, tag, channelID, channelType, []*eventbus.Event{active[i].ev}, localDeliv)
			if localLeased && hasWMKey {
				h.seqTracker.FinishSlotStage(wmKey, StageLocalDistribute, active[i].ev.MessageSeq, localDeliv, succLocal, localMask)
			}
			// Already-completed local bits in evLocalReq \ localDeliv also count toward Stage 2 completion.
			alreadyDoneLocal := evLocalReq.AndNot(localDeliv)
			active[i].succeeded = active[i].succeeded.Or(succLocal).Or(alreadyDoneLocal)
		}
	}

	// 3. Finalize Stage 2 per event and retain ONLY failed slots in RecoveryManager.
	for i := range active {
		item := &active[i]
		if item.leased && hasWMKey {
			h.seqTracker.FinishSlotStage(wmKey, StageChannelDistribute, item.ev.MessageSeq, item.deliverMask, item.succeeded, allRecipientSlots)
		}
		failed := item.deliverMask.AndNot(item.succeeded)
		if failed.IsZero() {
			if item.ev.Ticket != nil {
				item.ev.Ticket.Release()
				item.ev.Ticket = nil
			}
		} else {
			// Refresh tag on failure in case user slots migrated across nodes.
			if tm := h.tagManager(); tm != nil {
				tm.RemoveTag(tag.Key)
			}
			_ = h.recoveryMgr.Enqueue(channelID, channelType, RecoveryStageDistribute, failed, allRecipientSlots, item.ev)
		}
	}
}

func (h *Handler) distributePersistedOnRecipientNode(
	cl icluster.ICluster,
	localNode uint64,
	wmKey ChannelWatermarkKey,
	hasWMKey bool,
	tag *types.Tag,
	allRecipientSlots eventbus.SlotBitset,
	nodeSlotMasks map[uint64]eventbus.SlotBitset,
	channelID string,
	channelType uint8,
	events []*eventbus.Event,
) {
	localMask := nodeSlotMasks[localNode]
	for _, ev := range events {
		reqMask := localMask
		var migratedOutMask eventbus.SlotBitset
		if ev.HasTargetSlotMask && !ev.TargetSlotMask.IsZero() {
			reqMask = ev.TargetSlotMask.And(localMask)
			migratedOutMask = ev.TargetSlotMask.AndNot(localMask)
		}
		var succLocal eventbus.SlotBitset
		if !reqMask.IsZero() {
			localDeliv := reqMask
			localLeased := false
			if hasWMKey {
				localDeliv, localLeased = h.seqTracker.BeginSlotStage(wmKey, StageLocalDistribute, ev.MessageSeq, reqMask)
			}
			if !hasWMKey || (localLeased && !localDeliv.IsZero()) {
				var migLocal eventbus.SlotBitset
				succLocal, migLocal = h.deliverLocalUsersForMask(cl, localNode, tag, channelID, channelType, []*eventbus.Event{ev}, localDeliv)
				migratedOutMask = migratedOutMask.Or(migLocal)
				if localLeased && hasWMKey {
					h.seqTracker.FinishSlotStage(wmKey, StageLocalDistribute, ev.MessageSeq, localDeliv, succLocal, localMask)
				}
			} else if hasWMKey && !h.seqTracker.HasCommitted(wmKey, StageLocalDistribute, ev.MessageSeq) {
				migratedOutMask = migratedOutMask.Or(reqMask)
			}
		}

		if migratedOutMask.IsZero() {
			if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
		} else {
			if tm := h.tagManager(); tm != nil {
				tm.RemoveTag(tag.Key)
			}
			ev.TagKey = ""
			ev.HasTargetSlotMask = true
			ev.TargetSlotMask = migratedOutMask
			_ = h.recoveryMgr.Enqueue(channelID, channelType, RecoveryStageDistribute, migratedOutMask, allRecipientSlots, ev)
		}
	}
}

// executeSingleEventDistributeRecovery re-executes distribution for a RecoveryItem directly from RecoveryManager.
func (h *Handler) executeSingleEventDistributeRecovery(
	ctx context.Context,
	slotLeaderID uint64,
	channelID string,
	channelType uint8,
	ev *eventbus.Event,
	pendingSlots eventbus.SlotBitset,
	knownAllSlots eventbus.SlotBitset,
) (remainingSlots eventbus.SlotBitset, allSlots eventbus.SlotBitset, done bool) {
	if !h.isLocalNode(slotLeaderID) && strings.TrimSpace(ev.TagKey) == "" {
		clone := ev.Clone()
		clone.Type = eventbus.EventChannelDistribute
		if !pendingSlots.IsZero() {
			clone.HasTargetSlotMask = true
			clone.TargetSlotMask = pendingSlots.Clone()
		}
		if err := h.distributePersistedToRemoteNode(ctx, slotLeaderID, channelID, channelType, []*eventbus.Event{clone}, pendingSlots); err != nil {
			return pendingSlots, knownAllSlots, false
		}
		return eventbus.SlotBitset{}, knownAllSlots, true
	}

	// Build a fresh tag on the channel slot leader so any user-slot migration is reflected immediately.
	var tag *types.Tag
	var err error
	if h.isLocalNode(slotLeaderID) {
		if tm := h.tagManager(); tm != nil {
			if oldKey := tm.GetChannelTag(channelID, channelType); oldKey != "" {
				tm.RemoveTag(oldKey)
			}
		}
		tag, err = h.getOrMakeTagForLeader(channelID, channelType)
	} else {
		chCtx := &eventbus.ChannelContext{
			ChannelId:    channelID,
			ChannelType:  channelType,
			EventType:    eventbus.EventChannelDistribute,
			SlotLeaderId: slotLeaderID,
			Events:       []*eventbus.Event{ev},
		}
		tag, err = h.getCommonTag(chCtx)
	}
	if err != nil || tag == nil {
		return pendingSlots, knownAllSlots, false
	}
	ev.TagKey = tag.Key

	cl := h.cluster()
	localNode := h.localNodeId()
	wmKey, hasWMKey := MakeChannelWatermarkKey(channelID, channelType)
	allRecipientSlots, nodeSlotMasks := computeTagSlotTopology(cl, tag)
	if allRecipientSlots.IsZero() {
		if hasWMKey {
			if h.isLocalNode(slotLeaderID) {
				_ = h.seqTracker.RecordCommittedForce(wmKey, StageChannelDistribute, ev.MessageSeq)
			}
			_ = h.seqTracker.RecordCommittedForce(wmKey, StageLocalDistribute, ev.MessageSeq)
		}
		return eventbus.SlotBitset{}, allRecipientSlots, true
	}

	reqMask := allRecipientSlots
	if !pendingSlots.IsZero() {
		reqMask = pendingSlots.And(allRecipientSlots)
	}
	if reqMask.IsZero() {
		return eventbus.SlotBitset{}, allRecipientSlots, true
	}

	if !h.isLocalNode(slotLeaderID) {
		localMask := nodeSlotMasks[localNode]
		localReq := reqMask.And(localMask)
		remOutside := reqMask.AndNot(localMask)
		if !localReq.IsZero() {
			localDeliv := localReq
			localLeased := false
			if hasWMKey {
				localDeliv, localLeased = h.seqTracker.BeginSlotStage(wmKey, StageLocalDistribute, ev.MessageSeq, localReq)
			}
			if !hasWMKey || (localLeased && !localDeliv.IsZero()) {
				succLocal, migLocal := h.deliverLocalUsersForMask(cl, localNode, tag, channelID, channelType, []*eventbus.Event{ev}, localDeliv)
				if localLeased && hasWMKey {
					h.seqTracker.FinishSlotStage(wmKey, StageLocalDistribute, ev.MessageSeq, localDeliv, succLocal, localMask)
				}
				remOutside = remOutside.Or(migLocal).Or(localDeliv.AndNot(succLocal))
			} else if hasWMKey && !h.seqTracker.HasCommitted(wmKey, StageLocalDistribute, ev.MessageSeq) {
				remOutside = remOutside.Or(localReq)
			}
		}
		if remOutside.IsZero() {
			return eventbus.SlotBitset{}, allRecipientSlots, true
		}
		// Remaining migrated slots must be re-routed via channel slot leader.
		clone := ev.Clone()
		clone.Type = eventbus.EventChannelDistribute
		clone.TagKey = ""
		clone.HasTargetSlotMask = true
		clone.TargetSlotMask = remOutside.Clone()
		if err := h.distributePersistedToRemoteNode(ctx, slotLeaderID, channelID, channelType, []*eventbus.Event{clone}, remOutside); err != nil {
			return remOutside, allRecipientSlots, false
		}
		return eventbus.SlotBitset{}, allRecipientSlots, true
	}

	// Local node IS channel slot leader:
	deliverMask := reqMask
	leased := false
	if hasWMKey {
		deliverMask, leased = h.seqTracker.BeginSlotStage(wmKey, StageChannelDistribute, ev.MessageSeq, reqMask)
		if !leased || deliverMask.IsZero() {
			if h.seqTracker.HasCommitted(wmKey, StageChannelDistribute, ev.MessageSeq) {
				return eventbus.SlotBitset{}, allRecipientSlots, true
			}
			return reqMask, allRecipientSlots, false
		}
	}

	var succeeded eventbus.SlotBitset
	for _, node := range tag.Nodes {
		if node == nil || node.LeaderId == localNode || node.LeaderId == 0 {
			continue
		}
		remoteMask := nodeSlotMasks[node.LeaderId]
		evMask := deliverMask.And(remoteMask)
		if evMask.IsZero() {
			continue
		}
		if ev.SourceNodeId != 0 && ev.SourceNodeId == node.LeaderId {
			succeeded = succeeded.Or(evMask)
			continue
		}
		if err := h.distributePersistedToRemoteNode(ctx, node.LeaderId, channelID, channelType, []*eventbus.Event{ev}, evMask); err == nil {
			succeeded = succeeded.Or(evMask)
		}
	}

	localMask := nodeSlotMasks[localNode]
	evLocalReq := deliverMask.And(localMask)
	if !evLocalReq.IsZero() {
		localDeliv := evLocalReq
		localLeased := false
		if hasWMKey {
			localDeliv, localLeased = h.seqTracker.BeginSlotStage(wmKey, StageLocalDistribute, ev.MessageSeq, evLocalReq)
		}
		if !hasWMKey || (localLeased && !localDeliv.IsZero()) {
			succLocal, _ := h.deliverLocalUsersForMask(cl, localNode, tag, channelID, channelType, []*eventbus.Event{ev}, localDeliv)
			if localLeased && hasWMKey {
				h.seqTracker.FinishSlotStage(wmKey, StageLocalDistribute, ev.MessageSeq, localDeliv, succLocal, localMask)
			}
			succeeded = succeeded.Or(succLocal).Or(evLocalReq.AndNot(localDeliv))
		} else if hasWMKey && h.seqTracker.HasCommitted(wmKey, StageLocalDistribute, ev.MessageSeq) {
			succeeded = succeeded.Or(evLocalReq)
		}
	}

	if leased && hasWMKey {
		h.seqTracker.FinishSlotStage(wmKey, StageChannelDistribute, ev.MessageSeq, deliverMask, succeeded, allRecipientSlots)
	}
	failed := deliverMask.AndNot(succeeded)
	return failed, allRecipientSlots, failed.IsZero()
}

// distributeTransientByTag handles MessageSeq == 0 (NoPersist / online CMD) events using the base transport.
func (h *Handler) distributeTransientByTag(slotLeaderId uint64, tag *types.Tag, channelId string, channelType uint8, events []*eventbus.Event) {
	localNode := h.localNodeId()
	if h.isLocalNode(slotLeaderId) {
		for _, node := range tag.Nodes {
			if node.LeaderId == localNode {
				continue
			}
			h.distributeToNode(node.LeaderId, channelId, channelType, events)
		}
	}
	cl := h.cluster()
	_, nodeMasks := computeTagSlotTopology(cl, tag)
	localMask := nodeMasks[localNode]
	if !localMask.IsZero() {
		_, _ = h.deliverLocalUsersForMask(cl, localNode, tag, channelId, channelType, events, localMask)
	}
}

func (h *Handler) isSkipConversationUpdate(channelType uint8) bool {
	for _, t := range skipConversationUpdateChannelTypes {
		if t == channelType {
			return true
		}
	}
	return false
}

func (h *Handler) distributeToNode(leaderId uint64, channelId string, channelType uint8, events []*eventbus.Event) {
	for _, event := range events {
		if event.SourceNodeId != 0 && event.SourceNodeId == leaderId {
			h.Foucs("distributeToNode: sourceNode is forward node, not distribute", zap.Uint64("sourceNodeId", event.SourceNodeId), zap.Uint64("leaderId", leaderId), zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
			return
		}
	}
	h.forwardsToNode(leaderId, channelId, channelType, events)
}

func (h *Handler) getCommonTag(ctx *eventbus.ChannelContext) (*types.Tag, error) {
	if h.isLocalNode(ctx.SlotLeaderId) {
		return h.getOrMakeTagForLeader(ctx.ChannelId, ctx.ChannelType)
	}
	tagKey := ctx.Events[0].TagKey
	tm := h.tagManager()
	if tm != nil {
		oldTagKey := tm.GetChannelTag(ctx.ChannelId, ctx.ChannelType)
		if oldTagKey != "" && oldTagKey != tagKey {
			tm.RemoveTag(oldTagKey)
		}
		if tagKey != "" {
			if tag := tm.Get(tagKey); tag != nil {
				tm.SetChannelTag(ctx.ChannelId, ctx.ChannelType, tagKey)
				return tag, nil
			}
		}
	}
	tag, err := h.commonService.GetOrRequestAndMakeTagWithLocal(ctx.ChannelId, ctx.ChannelType, tagKey)
	if err != nil {
		h.Error("processDiffuse: get tag failed", zap.Error(err), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType), zap.String("tagKey", tagKey))
		return nil, err
	}
	if tag == nil {
		h.Error("processDiffuse: tag not found", zap.String("tagKey", tagKey), zap.String("channelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
		return nil, nil
	}
	return tag, nil
}

func (h *Handler) requestTag(leaderId uint64, tagKey string) (*types.Tag, error) {
	tagResp, err := h.client.RequestTag(leaderId, &ingress.TagReq{
		TagKey: tagKey,
		NodeId: h.localNodeId(),
	})
	if err != nil {
		h.Error("requestTag: get tag failed", zap.Error(err), zap.Uint64("leaderId", leaderId))
		return nil, err
	}
	tag, err := h.tagManager().MakeTagNotCacheWithTagKey(tagKey, tagResp.Uids)
	if err != nil {
		h.Error("requestTag: MakeTagNotCacheWithTagKey failed", zap.Error(err))
		return nil, err
	}
	return tag, nil
}

func (h *Handler) getOrMakeTagForLeader(fakeChannelId string, channelType uint8) (*types.Tag, error) {
	var (
		tag *types.Tag
		err error
	)
	tm := h.tagManager()
	tagKey := tm.GetChannelTag(fakeChannelId, channelType)
	if tagKey != "" {
		tag = tm.Get(tagKey)
	}
	if tag == nil {
		tag, err = h.makeChannelTag(fakeChannelId, channelType)
		if err != nil {
			h.Error("processMakeTag: makeTag failed", zap.Error(err), zap.String("tagKey", tagKey))
			return nil, err
		}
	}
	return tag, nil
}

func (h *Handler) makeChannelTag(fakeChannelId string, channelType uint8) (*types.Tag, error) {
	var subscribers []string
	if channelType == wkproto.ChannelTypePerson {
		orgFakeChannelId := fakeChannelId
		if options.G.IsCmdChannel(fakeChannelId) {
			orgFakeChannelId = options.G.CmdChannelConvertOrginalChannel(fakeChannelId)
		}
		u1, u2 := options.GetFromUIDAndToUIDWith(orgFakeChannelId)
		subscribers = append(subscribers, u1, u2)
	} else if channelType == wkproto.ChannelTypeAgent {
		u1, agent := options.GetUidAndAgentUIDWith(fakeChannelId)
		subscribers = append(subscribers, u1, agent)
	} else {
		if options.G.IsCmdChannel(fakeChannelId) {
			var err error
			subscribers, err = h.getCmdSubscribers(fakeChannelId, channelType)
			if err != nil {
				h.Error("processMakeTag: getCmdSubscribers failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
				return nil, err
			}
		} else {
			var err error
			subscribers, err = h.getSubscribers(fakeChannelId, channelType)
			if err != nil {
				h.Error("processMakeTag: getSubscribers failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
				return nil, err
			}
		}
	}
	tm := h.tagManager()
	tag, err := tm.MakeTag(subscribers)
	if err != nil {
		h.Error("processMakeTag: makeTag failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	tm.SetChannelTag(fakeChannelId, channelType, tag.Key)
	return tag, nil
}

func (h *Handler) getSubscribers(fakeChannelId string, channelType uint8) ([]string, error) {
	members, err := h.store().GetSubscribers(fakeChannelId, channelType)
	if err != nil {
		h.Error("processMakeTag: getSubscribers failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	subscribers := make([]string, 0, len(members))
	for _, member := range members {
		subscribers = append(subscribers, member.Uid)
	}
	if channelType == wkproto.ChannelTypeCustomerService {
		visitorId, _ := options.G.GetCustomerServiceVisitorUID(fakeChannelId)
		if visitorId != "" {
			subscribers = append(subscribers, visitorId)
		}
	} else if channelType == wkproto.ChannelTypeVisitors {
		subscribers = append(subscribers, fakeChannelId)
	}
	return subscribers, nil
}

func (h *Handler) getCmdSubscribers(channelId string, channelType uint8) ([]string, error) {
	orgFakeChannelId := options.G.CmdChannelConvertOrginalChannel(channelId)
	leaderNode, err := h.cluster().LeaderOfChannel(orgFakeChannelId, channelType)
	if err != nil {
		h.Error("processMakeTag: get leaderNode failed", zap.Error(err), zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	if leaderNode == nil {
		h.Error("processMakeTag: leaderNode is nil", zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		return nil, errors.New("leaderNode is nil")
	}
	leaderId := leaderNode.Id
	var subscribers []string
	if h.isLocalNode(leaderId) {
		members, err := h.store().GetSubscribers(orgFakeChannelId, channelType)
		if err != nil {
			h.Error("processMakeTag: getSubscribers failed", zap.Error(err), zap.String("orgFakeChannelId", orgFakeChannelId), zap.Uint8("channelType", channelType))
			return nil, err
		}
		for _, member := range members {
			subscribers = append(subscribers, member.Uid)
		}
	} else {
		subscribers, err = h.client.RequestSubscribers(leaderId, orgFakeChannelId, channelType)
		if err != nil {
			h.Error("processMakeTag: requestSubscribers failed", zap.Error(err), zap.String("orgFakeChannelId", orgFakeChannelId), zap.Uint8("channelType", channelType))
			return nil, err
		}
	}
	return subscribers, nil
}

func (h *Handler) deviceOnlineStatus(uid string) (bool, bool) {
	toConns := h.userAuthedConns(uid)
	masterIsOnline := false
	for _, conn := range toConns {
		if conn.DeviceLevel == wkproto.DeviceLevelMaster {
			masterIsOnline = true
			break
		}
	}
	return len(toConns) > 0, masterIsOnline
}
