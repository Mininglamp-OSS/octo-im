package handler

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

// 跳过会话更新的频道类型
var skipConversationUpdateChannelTypes = []uint8{wkproto.ChannelTypeData, wkproto.ChannelTypeTemp, wkproto.ChannelTypeLive}

// 消息分发按连续的成员版本处理。领导节点固定快照，远端在确认接收前
// 持有自己的收件人快照；旧协议消息仍按原 key 查询，不回退当前成员。
func (h *Handler) distribute(ctx *eventbus.ChannelContext) {

	// 记录消息轨迹
	events := ctx.Events
	for _, event := range events {
		event.Track.Record(track.PositionChannelDistribute)
	}

	// 消息分发
	if options.G.IsOnlineCmdChannel(ctx.ChannelId) {
		// 分发在线cmd消息
		h.distributeOnlineCmd(ctx)
	} else {
		// 分发普通消息
		h.distributeCommon(ctx)
	}
}

// 普通消息分发
func (h *Handler) distributeCommon(ctx *eventbus.ChannelContext) {
	h.distributeVersions(ctx, false)
}

// distributeVersions preserves contiguous version runs, including A/B/A order.
func (h *Handler) distributeVersions(ctx *eventbus.ChannelContext, onlineCmd bool) {
	for start := 0; start < len(ctx.Events); {
		end := start + 1
		first := ctx.Events[start]
		for end < len(ctx.Events) && ctx.Events[end].TagKey == first.TagKey {
			end++
		}
		events := ctx.Events[start:end]
		part := &eventbus.ChannelContext{SlotLeaderId: ctx.SlotLeaderId, ChannelId: ctx.ChannelId, ChannelType: ctx.ChannelType, Events: events}
		var tag *types.Tag
		var err error
		if first.Distribution != nil {
			tag = first.Distribution.Snapshot
		}
		if tag == nil {
			if onlineCmd {
				if first.TagKey == "" {
					err = errInvalidDistribution
				} else if options.G.IsLocalNode(ctx.SlotLeaderId) {
					tag = service.TagManager.Get(first.TagKey)
				} else {
					tag, err = h.requestTag(ctx.SlotLeaderId, first.TagKey)
				}
			} else {
				tag, err = h.getCommonTag(part)
			}
		}
		if err == nil && tag == nil {
			err = errInvalidDistribution
		}
		if err == nil {
			now := h.distributionTime()
			for _, event := range events {
				event.TagKey = tag.Key
				if event.Distribution == nil {
					event.Distribution = &eventbus.DistributionState{}
				}
				event.Distribution.Snapshot = tag
				if event.Distribution.Deadline.IsZero() {
					event.Distribution.Deadline = now.Add(distributionRetryTimeout)
				}
			}
			err = h.deliverVersion(ctx, tag, events)
		}
		if err != nil {
			h.retainDistribution(ctx, events, ctx.Events[end:], err)
			return
		}
		for _, event := range events {
			finishDistribution(event)
		}
		start = end
	}
}

// deliverVersion records partial progress so retries never replay local fan-out
// or acknowledged destinations. Receipt snapshots survive manager eviction.
func (h *Handler) deliverVersion(ctx *eventbus.ChannelContext, tag *types.Tag, events []*eventbus.Event) error {
	local := make([]*eventbus.Event, 0, len(events))
	for _, event := range events {
		if !event.Distribution.LocalDone {
			local = append(local, event)
		}
	}
	if len(local) > 0 {
		h.distributeByTag(options.G.Cluster.NodeId, tag, ctx.ChannelId, ctx.ChannelType, local)
		for _, event := range local {
			event.Distribution.LocalDone = true
		}
	}
	for _, node := range tag.Nodes {
		if node.LeaderId == options.G.Cluster.NodeId || len(node.Uids) == 0 {
			continue
		}
		pending := make([]*eventbus.Event, 0, len(events))
		for _, event := range events {
			if event.SourceNodeId != 0 && event.SourceNodeId == node.LeaderId {
				continue // Legacy recipient tags can also contain the forwarding node.
			}
			if _, done := event.Distribution.DeliveredNodes[node.LeaderId]; !done {
				pending = append(pending, event)
			}
		}
		if len(pending) == 0 {
			continue
		}
		if err := h.forwardDistribution(node.LeaderId, tag, ctx.ChannelId, ctx.ChannelType, pending); err != nil {
			return &distributionTargetError{node: node.LeaderId, err: err}
		}
		for _, event := range pending {
			if event.Distribution.DeliveredNodes == nil {
				event.Distribution.DeliveredNodes = make(map[uint64]struct{})
			}
			event.Distribution.DeliveredNodes[node.LeaderId] = struct{}{}
		}
	}
	return nil
}

type distributionTargetError struct {
	node uint64
	err  error
}

func (e *distributionTargetError) Error() string { return e.err.Error() }
func (e *distributionTargetError) Unwrap() error { return e.err }

func (h *Handler) retainDistribution(ctx *eventbus.ChannelContext, failed, tail []*eventbus.Event, cause error) {
	now := h.distributionTime()
	delay := 50 * time.Millisecond
	target := ctx.SlotLeaderId
	var targetError *distributionTargetError
	if errors.As(cause, &targetError) {
		target = targetError.node
	}
	for _, event := range failed {
		if event.Distribution == nil {
			event.Distribution = &eventbus.DistributionState{}
		}
		state := event.Distribution
		if state.Deadline.IsZero() {
			state.Deadline = now.Add(distributionRetryTimeout)
		}
		state.Attempts++
		fields := []zap.Field{zap.Error(cause), zap.Int64("messageId", event.MessageId), zap.String("tagKey", event.TagKey), zap.String("channelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType), zap.Uint64("nodeId", options.G.Cluster.NodeId), zap.Uint64("sourceNodeId", event.SourceNodeId), zap.Uint64("targetNodeId", target), zap.Int("attempt", state.Attempts)}
		if state.Attempts >= distributionMaxAttempts || !now.Before(state.Deadline) || errors.Is(cause, errInvalidDistribution) {
			h.Error("distribution retry exhausted", fields...)
			finishDistribution(event)
			continue
		}
		retryDelay := time.Duration(50<<min(state.Attempts-1, 4)) * time.Millisecond
		if retryDelay > delay {
			delay = retryDelay
		}
		h.Warn("distribution retained for retry", fields...)
		ctx.RetryEvents = append(ctx.RetryEvents, event)
	}
	ctx.RetryEvents = append(ctx.RetryEvents, tail...)
	ctx.RetryAt = now.Add(delay)
}

func finishDistribution(event *eventbus.Event) {
	if state := event.Distribution; state != nil {
		state.Finish()
	}
}

// cmd消息分发
func (h *Handler) distributeOnlineCmd(ctx *eventbus.ChannelContext) {
	h.distributeVersions(ctx, true)
}

func (h *Handler) distributeByTag(slotLeaderId uint64, tag *types.Tag, channelId string, channelType uint8, events []*eventbus.Event) {
	if slotLeaderId == 0 {
		h.Error("distributeByTag: leaderId is 0", zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		return
	}

	// 本地分发 （分发本节点上的用户）
	var offlineUids []string // 需要推离线的用户
	var pubshEvents []*eventbus.Event
	appendOnline := func(uid string, capacity int) {
		for _, event := range events {
			if pubshEvents == nil {
				pubshEvents = make([]*eventbus.Event, 0, len(events)*capacity)
			}
			cloneMsg := event.Clone()
			cloneMsg.Distribution = nil
			cloneMsg.ToUid = uid
			cloneMsg.ChannelId = channelId
			cloneMsg.ChannelType = channelType
			cloneMsg.Type = eventbus.EventPushOnline
			pubshEvents = append(pubshEvents, cloneMsg)
		}
	}
	flushOnline := func() {
		if len(pubshEvents) == 0 {
			return
		}
		id := eventbus.Pusher.AddEvents(pubshEvents)
		eventbus.Pusher.Advance(id)
		pubshEvents = nil
	}
	localHasEvent := false
	for _, node := range tag.Nodes {
		if node.LeaderId != options.G.Cluster.NodeId {
			continue
		}
		if len(node.Uids) > 0 {
			localHasEvent = true
		}
		delivered := make(map[string]bool, len(node.Uids))
		if service.Presence != nil {
			unknownUids := make([]string, 0, len(node.Uids))
			for _, uid := range node.Uids {
				if options.G.IsSystemUid(uid) {
					continue
				}
				isOnline, _ := h.deviceOnlineStatus(uid)
				if isOnline {
					appendOnline(uid, len(node.Uids))
					delivered[uid] = true
				} else {
					unknownUids = append(unknownUids, uid)
				}
			}
			// Do not hold known-live delivery behind cold recovery RPCs.
			flushOnline()
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			err := service.Presence.Recover(ctx, unknownUids)
			cancel()
			if err != nil {
				h.Warn("recipient authority recovery incomplete; delivering to known sessions",
					zap.Error(err), zap.Int("recipientCount", len(node.Uids)))
			}
		}
		for _, uid := range node.Uids {
			if options.G.IsSystemUid(uid) {
				continue
			}
			isOnline, masterIsOnline := h.deviceOnlineStatus(uid)
			if !masterIsOnline && channelType != wkproto.ChannelTypeAgent { // agent不需要触发离线的webhook
				if offlineUids == nil {
					offlineUids = make([]string, 0, len(node.Uids))
				}
				offlineUids = append(offlineUids, uid)
			}
			if !isOnline || delivered[uid] {
				continue
			}
			appendOnline(uid, len(node.Uids))
		}
	}

	if localHasEvent {
		// 更新最近会话
		if !h.isSkipConversationUpdate(channelType) {
			h.conversation(channelId, channelType, tag.Key, events)
		}
	}

	flushOnline()
	if len(offlineUids) > 0 {
		offlineEvents := make([]*eventbus.Event, 0, len(events))
		for _, event := range events {
			if event.Frame == nil || event.Frame.GetFrameType() == wkproto.EVENT {
				continue // 不处理event类型的事件
			}
			// 过滤发送者
			filteredOfflineUids := make([]string, 0, len(offlineUids))
			for _, offlineUid := range offlineUids {
				if offlineUid != event.Conn.Uid {
					filteredOfflineUids = append(filteredOfflineUids, offlineUid)
				}
			}
			// 移除重复的离线用户
			filteredOfflineUids = wkutil.RemoveRepeatedElement(filteredOfflineUids)

			cloneEvent := event.Clone()
			cloneEvent.Distribution = nil
			cloneEvent.OfflineUsers = filteredOfflineUids
			cloneEvent.Type = eventbus.EventPushOffline
			offlineEvents = append(offlineEvents, cloneEvent)
		}
		_ = eventbus.Pusher.AddEvents(offlineEvents)
		// eventbus.Pusher.Advance(id) // 不需要推进，因为是离线消息
	}

}

// 是否跳过会话更新
func (h *Handler) isSkipConversationUpdate(channelType uint8) bool {
	for _, t := range skipConversationUpdateChannelTypes {
		if t == channelType {
			return true
		}
	}
	return false
}

func (h *Handler) getCommonTag(ctx *eventbus.ChannelContext) (*types.Tag, error) {

	tagKey := ctx.Events[0].TagKey
	if tagKey == "" && options.G.IsLocalNode(ctx.SlotLeaderId) {
		return h.getOrMakeTagForLeader(ctx.ChannelId, ctx.ChannelType)
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

// 请求tag
func (h *Handler) requestTag(leaderId uint64, tagKey string) (*types.Tag, error) {
	// 去领导节点请求
	tagResp, err := h.client.RequestTag(leaderId, &ingress.TagReq{
		TagKey: tagKey,
		NodeId: options.G.Cluster.NodeId,
	})
	if err != nil {
		h.Error("requestTag: get tag failed", zap.Error(err), zap.Uint64("leaderId", leaderId))
		return nil, err
	}
	tag, err := service.TagManager.MakeTagNotCacheWithTagKey(tagKey, tagResp.Uids)
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

	tagKey := service.TagManager.GetChannelTag(fakeChannelId, channelType)
	if tagKey != "" {
		tag = service.TagManager.Get(tagKey)
	}
	if tag == nil {
		// 如果没有则制作tag
		tag, err = h.makeChannelTag(fakeChannelId, channelType)
		if err != nil {
			h.Error("processMakeTag: makeTag failed", zap.Error(err), zap.String("tagKey", tagKey))
			return nil, err
		}

	}
	return tag, nil
}

func (h *Handler) makeChannelTag(fakeChannelId string, channelType uint8) (*types.Tag, error) {

	var (
		subscribers []string
	)

	if channelType == wkproto.ChannelTypePerson { // 个人频道
		var orgFakeChannelId = fakeChannelId
		if options.G.IsCmdChannel(fakeChannelId) {
			// 处理命令频道
			orgFakeChannelId = options.G.CmdChannelConvertOrginalChannel(fakeChannelId)
		}
		u1, u2 := options.GetFromUIDAndToUIDWith(orgFakeChannelId)
		subscribers = append(subscribers, u1, u2)
	} else if channelType == wkproto.ChannelTypeAgent { // agent频道
		u1, agent := options.GetUidAndAgentUIDWith(fakeChannelId)
		subscribers = append(subscribers, u1, agent)
	} else {
		// 如果是cmd频道需要去对应的源频道获取订阅者来制作tag
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
	tag, err := service.TagManager.MakeTag(subscribers)
	if err != nil {
		h.Error("processMakeTag: makeTag failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	service.TagManager.SetChannelTag(fakeChannelId, channelType, tag.Key)
	return tag, nil
}

func (h *Handler) getSubscribers(fakeChannelId string, channelType uint8) ([]string, error) {
	members, err := service.Store.GetSubscribers(fakeChannelId, channelType)
	if err != nil {
		h.Error("processMakeTag: getSubscribers failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	var subscribers = make([]string, 0, len(members))
	for _, member := range members {
		subscribers = append(subscribers, member.Uid)
	}

	// 如果是客服频道，则从频道id中获取访客id
	if channelType == wkproto.ChannelTypeCustomerService {
		// 访客id
		visitorId, _ := options.G.GetCustomerServiceVisitorUID(fakeChannelId)
		if visitorId != "" {
			subscribers = append(subscribers, visitorId)
		}
	} else if channelType == wkproto.ChannelTypeVisitors { // 如果是访客频道，那么频道id就是访客的uid，所以这里需要加访客加入到订阅者里
		subscribers = append(subscribers, fakeChannelId)
	}
	return subscribers, nil
}

// 获取cmd频道的订阅者
func (h *Handler) getCmdSubscribers(channelId string, channelType uint8) ([]string, error) {
	// 原频道id
	orgFakeChannelId := options.G.CmdChannelConvertOrginalChannel(channelId)
	// 获取原频道的领导节点id
	leaderNode, err := service.Cluster.LeaderOfChannel(orgFakeChannelId, channelType)
	if err != nil {
		h.Error("processMakeTag: get leaderNode failed", zap.Error(err), zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	if leaderNode == nil {
		h.Error("processMakeTag: leaderNode is nil", zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		return nil, errors.New("leaderNode is nil")
	}
	leaderId := leaderNode.Id
	// 如果是本地节点，则直接获取订阅者
	var subscribers []string
	if options.G.IsLocalNode(leaderId) {
		members, err := service.Store.GetSubscribers(orgFakeChannelId, channelType)
		if err != nil {
			h.Error("processMakeTag: getSubscribers failed", zap.Error(err), zap.String("orgFakeChannelId", orgFakeChannelId), zap.Uint8("channelType", channelType))
			return nil, err
		}
		for _, member := range members {
			subscribers = append(subscribers, member.Uid)
		}
	} else {
		// 如果不是本地节点，则去请求领导节点获取订阅者
		subscribers, err = h.client.RequestSubscribers(leaderId, orgFakeChannelId, channelType)
		if err != nil {
			h.Error("processMakeTag: requestSubscribers failed", zap.Error(err), zap.String("orgFakeChannelId", orgFakeChannelId), zap.Uint8("channelType", channelType))
			return nil, err
		}
	}
	return subscribers, nil
}

// 用户的设备在线状态
func (h *Handler) deviceOnlineStatus(uid string) (bool, bool) {
	toConns := eventbus.User.AuthedConnsByUid(uid)
	masterIsOnline := false
	for _, conn := range toConns {
		if conn.DeviceLevel == wkproto.DeviceLevelMaster {
			masterIsOnline = true
			break
		}
	}
	return len(toConns) > 0, masterIsOnline
}
