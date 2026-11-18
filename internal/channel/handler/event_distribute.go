package handler

import (
	"context"
	"errors"
	"fmt"
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

type SlotOwnershipStatus uint8

const (
	SlotOwnerUnknown SlotOwnershipStatus = iota
	SlotOwnerLocal
	SlotOwnerRemote
)

// lookupUserSlotID resolves uid -> slotID and fails closed on nil cluster or panic,
// never conflating valid slot 0 with a lookup failure (B1).
func lookupUserSlotID(cl icluster.ICluster, v string) (slotID uint32, err error) {
	if cl == nil {
		return 0, errors.New("cluster is nil")
	}
	defer func() {
		if r := recover(); r != nil {
			slotID = 0
			err = fmt.Errorf("GetSlotId panicked for %q: %v", v, r)
		}
	}()
	return cl.GetSlotId(v), nil
}

// verifyUserSlotOwnership checks slot leadership and fails closed (SlotOwnerUnknown) on nil cluster,
// localNode == 0, leaderID == 0, or panic — never defaulting to local ownership (B1).
func verifyUserSlotOwnership(cl icluster.ICluster, slotID uint32, localNode uint64) (status SlotOwnershipStatus, leaderID uint64, err error) {
	if cl == nil || localNode == 0 {
		return SlotOwnerUnknown, 0, errors.New("cluster or localNode unavailable")
	}
	defer func() {
		if r := recover(); r != nil {
			status = SlotOwnerUnknown
			leaderID = 0
			err = fmt.Errorf("SlotLeaderId panicked for slot %d: %v", slotID, r)
		}
	}()
	leaderID = cl.SlotLeaderId(slotID)
	if leaderID == 0 {
		return SlotOwnerUnknown, 0, fmt.Errorf("slot %d leader is 0", slotID)
	}
	if leaderID == localNode {
		return SlotOwnerLocal, leaderID, nil
	}
	return SlotOwnerRemote, leaderID, nil
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

func (h *Handler) enqueueDistributeRecoveryWithTicket(
	channelID string,
	channelType uint8,
	pendingSlots eventbus.SlotBitset,
	allRecipientSlots eventbus.SlotBitset,
	ev *eventbus.Event,
) {
	if h == nil || ev == nil || ev.MessageSeq == 0 {
		if ev != nil && ev.Ticket != nil {
			ev.Ticket.Release()
			ev.Ticket = nil
		}
		return
	}
	_ = h.ensureEventHasTicket(channelID, ev)
	if !h.recoveryMgr.Enqueue(channelID, channelType, RecoveryStageDistribute, pendingSlots, allRecipientSlots, ev) && ev.Ticket != nil {
		ev.Ticket.Release()
		ev.Ticket = nil
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
				h.enqueueDistributeRecoveryWithTicket(ctx.ChannelId, ctx.ChannelType, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
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
						h.enqueueDistributeRecoveryWithTicket(ctx.ChannelId, ctx.ChannelType, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
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

func computeTagSlotTopologyChecked(cl icluster.ICluster, tag *types.Tag) (allSlots eventbus.SlotBitset, nodeMasks map[uint64]eventbus.SlotBitset, hasUnknown bool) {
	if tag == nil {
		return eventbus.SlotBitset{}, nil, true
	}
	nodeMasks = make(map[uint64]eventbus.SlotBitset, len(tag.Nodes))
	for _, node := range tag.Nodes {
		if node == nil {
			continue
		}
		var nm eventbus.SlotBitset
		hasRecipientUID := false
		for _, uid := range node.Uids {
			if uid == "" || (options.G != nil && options.G.IsSystemUid(uid)) {
				continue
			}
			hasRecipientUID = true
			sid, err := lookupUserSlotID(cl, uid)
			if err != nil {
				hasUnknown = true
				continue
			}
			nm.Set(sid)
			allSlots.Set(sid)
		}
		if hasRecipientUID && node.LeaderId == 0 {
			hasUnknown = true
		}
		nodeMasks[node.LeaderId] = nodeMasks[node.LeaderId].Or(nm)
	}
	return allSlots, nodeMasks, hasUnknown
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
		return forward.ErrPeerNoAck
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
// and verified as locally owned (B1 + V11-2).
// If GetSlotId or SlotLeaderId panics, is nil, or returns leader 0 for any UID in a slot, that slot is treated
// conservatively as Unknown (0 pushes/unread updates for that slot, excluded from succeededMask, retained in retryMask).
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

	type slotUIDGroup struct {
		sid  uint32
		uids []string
	}
	var slotOrder []uint32
	slotUIDs := make(map[uint32][]string)
	var unknownSlots eventbus.SlotBitset
	hadSlotIDLookupError := false
	isTransientNoCluster := cl == nil && !hasPersistedEvents(events)

	for _, node := range tag.Nodes {
		if node == nil || node.LeaderId != localNode {
			continue
		}
		for _, uid := range node.Uids {
			if uid == "" || (options.G != nil && options.G.IsSystemUid(uid)) {
				continue
			}
			if isTransientNoCluster {
				if allowedMask.Has(0) {
					if _, exists := slotUIDs[0]; !exists {
						slotOrder = append(slotOrder, 0)
					}
					slotUIDs[0] = append(slotUIDs[0], uid)
				}
				continue
			}
			sid, err := lookupUserSlotID(cl, uid)
			if err != nil {
				h.Error("deliverLocalUsersForMask: GetSlotId lookup failed; failing closed for batch",
					zap.Error(err), zap.String("uid", uid), zap.String("channelId", channelID))
				hadSlotIDLookupError = true
				continue
			}
			if !allowedMask.Has(sid) {
				continue
			}
			ownerStatus, _, ownerErr := verifyUserSlotOwnership(cl, sid, localNode)
			switch ownerStatus {
			case SlotOwnerUnknown:
				h.Error("deliverLocalUsersForMask: slot ownership unknown; retaining slot for recovery",
					zap.Error(ownerErr), zap.Uint32("slotId", sid), zap.String("uid", uid), zap.String("channelId", channelID))
				unknownSlots.Set(sid)
			case SlotOwnerRemote:
				migratedAwayMask.Set(sid)
			case SlotOwnerLocal:
				if _, exists := slotUIDs[sid]; !exists {
					slotOrder = append(slotOrder, sid)
				}
				slotUIDs[sid] = append(slotUIDs[sid], uid)
			}
		}
	}

	// If any UID's slot ID could not even be resolved, fail closed for the entire allowedMask so no UID is silently skipped.
	if hadSlotIDLookupError {
		return eventbus.SlotBitset{}, allowedMask.Clone()
	}

	var validSucceededSlots eventbus.SlotBitset
	var targetUIDs []string
	for _, sid := range slotOrder {
		// B1: If any UID on sid had Unknown ownership or migrated away, do not partially commit sid.
		if unknownSlots.Has(sid) || migratedAwayMask.Has(sid) {
			continue
		}
		validSucceededSlots.Set(sid)
		targetUIDs = append(targetUIDs, slotUIDs[sid]...)
	}

	// Any bit in allowedMask that had no matching local UID in tag.Nodes (or had Unknown ownership) must be retried/re-routed.
	unmatched := allowedMask.AndNot(validSucceededSlots).AndNot(migratedAwayMask)
	if !unmatched.IsZero() {
		migratedAwayMask = migratedAwayMask.Or(unmatched)
	}

	if len(targetUIDs) == 0 {
		return validSucceededSlots, migratedAwayMask
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

	// Mark validSucceededSlots only after local push/conversation handoff has completed.
	succeededMask = validSucceededSlots
	return succeededMask, migratedAwayMask
}

func (h *Handler) distributeByTag(slotLeaderId uint64, tag *types.Tag, channelId string, channelType uint8, events []*eventbus.Event) {
	if slotLeaderId == 0 {
		h.Error("distributeByTag: leaderId is 0", zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		for _, ev := range events {
			if ev != nil && ev.MessageSeq > 0 {
				h.enqueueDistributeRecoveryWithTicket(channelId, channelType, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
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
	allRecipientSlots, nodeSlotMasks, hasTopologyUnknown := computeTagSlotTopologyChecked(cl, tag)

	// B1: If topology resolution encountered any GetSlotId failure/panic or leader=0, do NOT treat empty allRecipientSlots as "0 recipients -> done"!
	if hasTopologyUnknown && allRecipientSlots.IsZero() {
		for _, ev := range persistedEvents {
			h.enqueueDistributeRecoveryWithTicket(channelId, channelType, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
		}
		return
	}

	if allRecipientSlots.IsZero() {
		for _, ev := range persistedEvents {
			committed := true
			if hasWMKey {
				if h.isLocalNode(slotLeaderId) {
					if !h.seqTracker.RecordCommittedForce(wmKey, StageChannelDistribute, ev.MessageSeq) {
						committed = false
					}
				}
				if !h.seqTracker.RecordCommittedForce(wmKey, StageLocalDistribute, ev.MessageSeq) {
					committed = false
				}
			}
			if !committed {
				h.enqueueDistributeRecoveryWithTicket(channelId, channelType, ev.TargetSlotMask, eventbus.SlotBitset{}, ev)
				continue
			}
			if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
		}
		return
	}

	if h.isLocalNode(slotLeaderId) {
		h.distributePersistedOnChannelLeader(context.Background(), cl, localNode, wmKey, hasWMKey, tag, allRecipientSlots, nodeSlotMasks, hasTopologyUnknown, channelId, channelType, persistedEvents)
		return
	}
	h.distributePersistedOnRecipientNode(cl, localNode, wmKey, hasWMKey, tag, allRecipientSlots, nodeSlotMasks, hasTopologyUnknown, channelId, channelType, persistedEvents)
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
	hasTopologyUnknown bool,
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
			if hasTopologyUnknown {
				h.enqueueDistributeRecoveryWithTicket(channelID, channelType, ev.TargetSlotMask, allRecipientSlots, ev)
				continue
			}
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
		deliv, status := h.seqTracker.BeginSlotStageChecked(wmKey, StageChannelDistribute, ev.MessageSeq, reqMask)
		switch status {
		case StageBeginAlreadyCommitted:
			if hasTopologyUnknown {
				h.enqueueDistributeRecoveryWithTicket(channelID, channelType, reqMask, allRecipientSlots, ev)
			} else if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
		case StageBeginInFlight, StageBeginCapacityRetry:
			h.enqueueDistributeRecoveryWithTicket(channelID, channelType, reqMask, allRecipientSlots, ev)
		case StageBeginLeased:
			active = append(active, leasedDistEvent{ev: ev, deliverMask: deliv, leased: true})
		}
	}
	if len(active) == 0 {
		return
	}

	// 1. Remote user-slot leaders: group by (node.LeaderId, exactMask) so events with different masks
	// never get widened to a batch union mask (A2).
	type exactMaskBatch struct {
		mask eventbus.SlotBitset
		evs  []*eventbus.Event
		idxs []int
	}
	for _, node := range tag.Nodes {
		if node == nil || node.LeaderId == localNode || node.LeaderId == 0 {
			continue
		}
		remoteMask := nodeSlotMasks[node.LeaderId]
		if remoteMask.IsZero() {
			continue
		}
		var groups []exactMaskBatch
		for i := range active {
			evMask := active[i].deliverMask.And(remoteMask)
			if evMask.IsZero() {
				continue
			}
			if active[i].ev.SourceNodeId != 0 && active[i].ev.SourceNodeId == node.LeaderId {
				active[i].succeeded = active[i].succeeded.Or(evMask)
				continue
			}
			placed := false
			for gIdx := range groups {
				if groups[gIdx].mask.Equal(evMask) {
					groups[gIdx].evs = append(groups[gIdx].evs, active[i].ev)
					groups[gIdx].idxs = append(groups[gIdx].idxs, i)
					placed = true
					break
				}
			}
			if !placed {
				groups = append(groups, exactMaskBatch{
					mask: evMask.Clone(),
					evs:  []*eventbus.Event{active[i].ev},
					idxs: []int{i},
				})
			}
		}
		for _, grp := range groups {
			if err := h.distributePersistedToRemoteNode(ctx, node.LeaderId, channelID, channelType, grp.evs, grp.mask); err == nil {
				for _, idx := range grp.idxs {
					active[idx].succeeded = active[idx].succeeded.Or(grp.mask)
				}
			} else {
				h.Warn("distributePersistedToRemoteNode failed; retaining failed slot mask for recovery",
					zap.Error(err),
					zap.Uint64("toNode", node.LeaderId),
					zap.String("channelId", channelID),
					zap.Int("batchLen", len(grp.evs)))
			}
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
				var st StageBeginStatus
				localDeliv, st = h.seqTracker.BeginSlotStageChecked(wmKey, StageLocalDistribute, active[i].ev.MessageSeq, evLocalReq)
				if st == StageBeginAlreadyCommitted {
					active[i].succeeded = active[i].succeeded.Or(evLocalReq)
					continue
				}
				if st != StageBeginLeased || localDeliv.IsZero() {
					continue
				}
				localLeased = true
			}
			succLocal, _ := h.deliverLocalUsersForMask(cl, localNode, tag, channelID, channelType, []*eventbus.Event{active[i].ev}, localDeliv)
			if localLeased && hasWMKey {
				h.seqTracker.FinishSlotStage(wmKey, StageLocalDistribute, active[i].ev.MessageSeq, localDeliv, succLocal, localMask)
			}
			alreadyDoneLocal := evLocalReq.AndNot(localDeliv)
			active[i].succeeded = active[i].succeeded.Or(succLocal).Or(alreadyDoneLocal)
		}
	}

	// 3. Finalize Stage 2 per event and retain ONLY failed slots in RecoveryManager.
	for i := range active {
		item := &active[i]
		effectiveAllSlots := allRecipientSlots
		if hasTopologyUnknown {
			// Prevent full-sequence completion if some recipient UID's slot could not be resolved.
			effectiveAllSlots = eventbus.SlotBitset{}
		}
		if item.leased && hasWMKey {
			h.seqTracker.FinishSlotStage(wmKey, StageChannelDistribute, item.ev.MessageSeq, item.deliverMask, item.succeeded, effectiveAllSlots)
		}
		failed := item.deliverMask.AndNot(item.succeeded)
		if failed.IsZero() && !hasTopologyUnknown {
			if item.ev.Ticket != nil {
				item.ev.Ticket.Release()
				item.ev.Ticket = nil
			}
		} else {
			if tm := h.tagManager(); tm != nil {
				tm.RemoveTag(tag.Key)
			}
			h.enqueueDistributeRecoveryWithTicket(channelID, channelType, failed, allRecipientSlots, item.ev)
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
	hasTopologyUnknown bool,
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
			var st StageBeginStatus = StageBeginLeased
			if hasWMKey {
				localDeliv, st = h.seqTracker.BeginSlotStageChecked(wmKey, StageLocalDistribute, ev.MessageSeq, reqMask)
				localLeased = st == StageBeginLeased
			}
			if !hasWMKey || (localLeased && !localDeliv.IsZero()) {
				var migLocal eventbus.SlotBitset
				succLocal, migLocal = h.deliverLocalUsersForMask(cl, localNode, tag, channelID, channelType, []*eventbus.Event{ev}, localDeliv)
				migratedOutMask = migratedOutMask.Or(migLocal)
				if localLeased && hasWMKey {
					effectiveLocalAll := localMask
					if hasTopologyUnknown {
						effectiveLocalAll = eventbus.SlotBitset{}
					}
					h.seqTracker.FinishSlotStage(wmKey, StageLocalDistribute, ev.MessageSeq, localDeliv, succLocal, effectiveLocalAll)
				}
			} else if st != StageBeginAlreadyCommitted {
				migratedOutMask = migratedOutMask.Or(reqMask)
			}
		}

		if migratedOutMask.IsZero() && !hasTopologyUnknown {
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
			h.enqueueDistributeRecoveryWithTicket(channelID, channelType, migratedOutMask, allRecipientSlots, ev)
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
	allRecipientSlots, nodeSlotMasks, hasTopologyUnknown := computeTagSlotTopologyChecked(cl, tag)
	if hasTopologyUnknown && allRecipientSlots.IsZero() {
		return pendingSlots, knownAllSlots, false
	}
	if allRecipientSlots.IsZero() {
		if hasWMKey {
			if h.isLocalNode(slotLeaderID) {
				if !h.seqTracker.RecordCommittedForce(wmKey, StageChannelDistribute, ev.MessageSeq) {
					return pendingSlots, knownAllSlots, false
				}
			}
			if !h.seqTracker.RecordCommittedForce(wmKey, StageLocalDistribute, ev.MessageSeq) {
				return pendingSlots, knownAllSlots, false
			}
		}
		return eventbus.SlotBitset{}, allRecipientSlots, true
	}

	reqMask := allRecipientSlots
	if !pendingSlots.IsZero() {
		reqMask = pendingSlots.And(allRecipientSlots)
	}
	if reqMask.IsZero() {
		if hasTopologyUnknown {
			return pendingSlots, allRecipientSlots, false
		}
		return eventbus.SlotBitset{}, allRecipientSlots, true
	}

	if !h.isLocalNode(slotLeaderID) {
		localMask := nodeSlotMasks[localNode]
		localReq := reqMask.And(localMask)
		remOutside := reqMask.AndNot(localMask)
		if !localReq.IsZero() {
			localDeliv := localReq
			localLeased := false
			var st StageBeginStatus = StageBeginLeased
			if hasWMKey {
				localDeliv, st = h.seqTracker.BeginSlotStageChecked(wmKey, StageLocalDistribute, ev.MessageSeq, localReq)
				localLeased = st == StageBeginLeased
			}
			if !hasWMKey || (localLeased && !localDeliv.IsZero()) {
				succLocal, migLocal := h.deliverLocalUsersForMask(cl, localNode, tag, channelID, channelType, []*eventbus.Event{ev}, localDeliv)
				if localLeased && hasWMKey {
					effectiveLocalAll := localMask
					if hasTopologyUnknown {
						effectiveLocalAll = eventbus.SlotBitset{}
					}
					h.seqTracker.FinishSlotStage(wmKey, StageLocalDistribute, ev.MessageSeq, localDeliv, succLocal, effectiveLocalAll)
				}
				remOutside = remOutside.Or(migLocal).Or(localDeliv.AndNot(succLocal))
			} else if st != StageBeginAlreadyCommitted {
				remOutside = remOutside.Or(localReq)
			}
		}
		if remOutside.IsZero() && !hasTopologyUnknown {
			return eventbus.SlotBitset{}, allRecipientSlots, true
		}
		clone := ev.Clone()
		clone.Type = eventbus.EventChannelDistribute
		clone.TagKey = ""
		clone.HasTargetSlotMask = true
		clone.TargetSlotMask = remOutside.Clone()
		if err := h.distributePersistedToRemoteNode(ctx, slotLeaderID, channelID, channelType, []*eventbus.Event{clone}, remOutside); err != nil {
			return remOutside, allRecipientSlots, false
		}
		return eventbus.SlotBitset{}, allRecipientSlots, !hasTopologyUnknown
	}

	// Local node IS channel slot leader:
	deliverMask := reqMask
	leased := false
	if hasWMKey {
		var st StageBeginStatus
		deliverMask, st = h.seqTracker.BeginSlotStageChecked(wmKey, StageChannelDistribute, ev.MessageSeq, reqMask)
		if st == StageBeginAlreadyCommitted && !hasTopologyUnknown {
			return eventbus.SlotBitset{}, allRecipientSlots, true
		}
		if st != StageBeginLeased || deliverMask.IsZero() {
			return reqMask, allRecipientSlots, false
		}
		leased = true
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
		var st StageBeginStatus = StageBeginLeased
		if hasWMKey {
			localDeliv, st = h.seqTracker.BeginSlotStageChecked(wmKey, StageLocalDistribute, ev.MessageSeq, evLocalReq)
			localLeased = st == StageBeginLeased
		}
		if !hasWMKey || (localLeased && !localDeliv.IsZero()) {
			succLocal, _ := h.deliverLocalUsersForMask(cl, localNode, tag, channelID, channelType, []*eventbus.Event{ev}, localDeliv)
			if localLeased && hasWMKey {
				effectiveLocalAll := localMask
				if hasTopologyUnknown {
					effectiveLocalAll = eventbus.SlotBitset{}
				}
				h.seqTracker.FinishSlotStage(wmKey, StageLocalDistribute, ev.MessageSeq, localDeliv, succLocal, effectiveLocalAll)
			}
			succeeded = succeeded.Or(succLocal).Or(evLocalReq.AndNot(localDeliv))
		} else if st == StageBeginAlreadyCommitted {
			succeeded = succeeded.Or(evLocalReq)
		}
	}

	if leased && hasWMKey {
		effectiveAll := allRecipientSlots
		if hasTopologyUnknown {
			effectiveAll = eventbus.SlotBitset{}
		}
		h.seqTracker.FinishSlotStage(wmKey, StageChannelDistribute, ev.MessageSeq, deliverMask, succeeded, effectiveAll)
	}
	failed := deliverMask.AndNot(succeeded)
	return failed, allRecipientSlots, failed.IsZero() && !hasTopologyUnknown
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
	_, nodeMasks, _ := computeTagSlotTopologyChecked(cl, tag)
	localMask := nodeMasks[localNode]
	if cl == nil {
		localMask.Set(0)
	}
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
