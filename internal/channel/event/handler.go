package event

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type channelHandler struct {
	wklog.Log
	channelId    string
	channelType  uint8
	channelKey   string
	slotLeaderId uint64 // 频道的槽领导节点Id
	lastActive   uint64 // 最后活跃时间
	pending      struct {
		sync.RWMutex
		eventQueue *eventbus.EventQueue
	}
	poller  *poller
	handler eventbus.ChannelEventHandler
	// 处理中的下标位置
	processingIndex uint64
	// 是否正在处理
	processing           atomic.Bool
	leaderRetryScheduled atomic.Bool
}

func newChannelHandler(channelId string, channelType uint8, poller *poller) *channelHandler {

	channelKey := wkutil.ChannelToKey(channelId, channelType)
	uh := &channelHandler{
		channelKey:  channelKey,
		channelId:   channelId,
		channelType: channelType,
		poller:      poller,
		handler:     poller.eventPool.handler,
		lastActive:  fasttime.UnixTimestamp(),
		Log:         wklog.NewWKLog(fmt.Sprintf("channelHandler[%s]", channelKey)),
	}
	uh.pending.eventQueue = eventbus.NewEventQueue(fmt.Sprintf("channel:%s", channelKey))
	return uh
}

func isPersistedChannelEvent(event *eventbus.Event) bool {
	return event != nil && (event.MessageSeq > 0 || event.Type == eventbus.EventChannelDistribute || event.Type == eventbus.EventChannelWebhook)
}

func (c *channelHandler) tryAddEvent(event *eventbus.Event) bool {
	if event == nil {
		return true
	}
	c.pending.Lock()
	defer c.pending.Unlock()
	if !c.pending.eventQueue.CanAppend(event) {
		return false
	}
	event.Index = c.pending.eventQueue.LastIndex() + 1
	c.pending.eventQueue.Append(event)
	c.lastActive = fasttime.UnixTimestamp()
	return true
}

func (c *channelHandler) tryAddBatch(events []*eventbus.Event) bool {
	if len(events) == 0 {
		return true
	}
	nonNil := 0
	persisted := true
	for _, ev := range events {
		if ev == nil {
			continue
		}
		nonNil++
		if !isPersistedChannelEvent(ev) {
			persisted = false
		}
	}
	if nonNil == 0 {
		return true
	}
	c.pending.Lock()
	defer c.pending.Unlock()
	if !c.pending.eventQueue.CanAppendCount(nonNil, persisted) {
		return false
	}
	for _, ev := range events {
		if ev == nil {
			continue
		}
		ev.Index = c.pending.eventQueue.LastIndex() + 1
		c.pending.eventQueue.Append(ev)
	}
	c.lastActive = fasttime.UnixTimestamp()
	return true
}

func (c *channelHandler) addEvent(event *eventbus.Event) {
	if event == nil {
		return
	}
	if c.tryAddEvent(event) {
		return
	}
	if event.MessageSeq > 0 && eventbus.EnqueuePersistedRecoveryFallback(c.channelId, c.channelType, event) {
		c.Warn("addEvent: channel queue full, spilled persisted event into recovery manager",
			zap.String("channelId", c.channelId),
			zap.Uint8("channelType", c.channelType),
			zap.Uint64("messageSeq", event.MessageSeq))
		return
	}
	if event.Ticket != nil {
		event.Ticket.Release()
		event.Ticket = nil
	}
	c.Warn("addEvent: channel event queue full, dropping event",
		zap.String("channelId", c.channelId),
		zap.Uint8("channelType", c.channelType),
		zap.Int("queueLen", c.pendingQueueLen()))
}

func (c *channelHandler) hasEvent() bool {
	c.pending.RLock()
	defer c.pending.RUnlock()
	if c.processing.Load() {
		return false
	}
	return c.processingIndex < c.pending.eventQueue.LastIndex()
}

func (c *channelHandler) pendingQueueLen() int {
	c.pending.RLock()
	defer c.pending.RUnlock()
	if c.pending.eventQueue == nil {
		return 0
	}
	return c.pending.eventQueue.Len()
}

func (u *channelHandler) events() []*eventbus.Event {
	u.pending.Lock()
	defer u.pending.Unlock()
	events := u.pending.eventQueue.SliceWithSize(u.processingIndex+1, u.pending.eventQueue.LastIndex()+1, options.G.Poller.ChannelEventMaxSizePerBatch)
	if len(events) == 0 {
		return nil
	}
	u.lastActive = fasttime.UnixTimestamp()
	return events
}

func (u *channelHandler) completeEvents(eventLastIndex uint64) {
	u.pending.Lock()
	defer u.pending.Unlock()
	if eventLastIndex > u.processingIndex {
		u.pending.eventQueue.TruncateTo(eventLastIndex + 1)
		u.processingIndex = eventLastIndex
	}
	u.lastActive = fasttime.UnixTimestamp()
}

// 推进事件
func (c *channelHandler) advanceEvents(events []*eventbus.Event) {

	c.processing.Store(true)
	defer func() {
		c.pending.Lock()
		c.lastActive = fasttime.UnixTimestamp()
		c.pending.Unlock()
		c.processing.Store(false)
	}()
	if len(events) == 0 {
		return
	}
	eventLastIndex := events[len(events)-1].Index

	// 检查和更新leaderId
	err := c.checkAndUpdateLeaderIdChange()
	if err != nil {
		c.Error("advanceEvents: checkAndUpdateLeaderIdChange failed", zap.Error(err))
		c.slotLeaderId = 0
	}

	var deferredSends []*eventbus.Event
	// 按类型分组
	group := c.groupByType(events)
	// 处理事件
	for eventType, grpEvents := range group {
		if c.slotLeaderId == 0 && (eventType == eventbus.EventChannelOnSend || eventType == eventbus.EventChannelDistribute) {
			deferredSends = append(deferredSends, grpEvents...)
			continue
		}
		// 从对象池中获取上下文
		ctx := c.poller.getContext()
		ctx.ChannelId = c.channelId
		ctx.ChannelType = c.channelType
		ctx.EventType = eventType
		ctx.Events = grpEvents
		ctx.SlotLeaderId = c.slotLeaderId
		// 处理事件
		c.handler.OnEvent(ctx)

		// 释放上下文
		c.poller.putContext(ctx)
	}

	c.completeEvents(eventLastIndex)

	if len(deferredSends) > 0 {
		nowMs := time.Now().UnixMilli()
		defaultDeadline := time.Now().Add(5 * time.Second).UnixMilli()
		requeued := false
		maxDeferredHops := uint8(0)
		for _, ev := range deferredSends {
			if ev == nil {
				continue
			}
			// Already-persisted messages (MessageSeq > 0) and distribution events must never be dropped
			// during transient leader-0 election windows; retain them with bounded backoff until a slot leader is elected.
			if ev.MessageSeq == 0 && ev.Type == eventbus.EventChannelOnSend {
				if ev.ForwardDeadline == 0 {
					ev.ForwardDeadline = defaultDeadline
				}
				if nowMs >= ev.ForwardDeadline {
					if ev.Ticket != nil {
						ev.Ticket.Release()
						ev.Ticket = nil
					}
					forward.Fail([]*eventbus.Event{ev}, forward.ErrUnavailable)
					continue
				}
			} else {
				if ev.ForwardHops < 32 {
					ev.ForwardHops++
				}
				if ev.ForwardHops > maxDeferredHops {
					maxDeferredHops = ev.ForwardHops
				}
			}
			if c.tryAddEvent(ev) {
				requeued = true
				continue
			}
			if ev.MessageSeq > 0 && eventbus.EnqueuePersistedRecoveryFallback(c.channelId, c.channelType, ev) {
				c.Warn("advanceEvents: channel queue full during leader-0 defer, routed persisted event to recovery manager",
					zap.String("channelId", c.channelId),
					zap.Uint8("channelType", c.channelType),
					zap.Uint64("messageSeq", ev.MessageSeq))
				continue
			}
			if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
			forward.Fail([]*eventbus.Event{ev}, forward.ErrUnavailable)
		}
		if requeued {
			if c.leaderRetryScheduled.CompareAndSwap(false, true) {
				shift := maxDeferredHops
				if shift > 5 {
					shift = 5
				}
				delay := time.Duration(25*(1<<shift)) * time.Millisecond
				if delay > 800*time.Millisecond {
					delay = 800 * time.Millisecond
				}
				time.AfterFunc(delay, func() {
					c.leaderRetryScheduled.Store(false)
					if c.poller != nil {
						c.poller.advance()
					}
				})
			}
			return
		}
	}

	// 推进事件
	c.pending.RLock()
	hasMore := c.processingIndex < c.pending.eventQueue.LastIndex()
	c.pending.RUnlock()
	if hasMore || (c.poller != nil && c.poller.needAdvance.Swap(false)) {
		c.poller.advance()
	}
}

// checkAndUpdateLeaderIdChange 检查并更新leaderId变化
func (c *channelHandler) checkAndUpdateLeaderIdChange() error {
	var notifyWakeup bool
	c.pending.Lock()
	prevLeader := c.slotLeaderId
	cl := service.Cluster
	if c.poller != nil && c.poller.eventPool != nil {
		cl = c.poller.eventPool.cluster()
	}
	if cl == nil {
		c.pending.Unlock()
		return errors.New("checkAndUpdateLeaderIdChange: cluster is nil")
	}
	slotLeaderId, err := cl.SlotLeaderIdOfChannel(c.channelId, c.channelType)
	if err != nil {
		c.pending.Unlock()
		c.Error("checkLeaderIdChange: get leader id failed", zap.Error(err), zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType))
		return err
	}
	if slotLeaderId == 0 {
		c.pending.Unlock()
		c.Warn("checkLeaderIdChange: leader id is 0", zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType))
		return errors.New("checkAndUpdateLeaderIdChange： leader id is 0")
	}
	c.slotLeaderId = slotLeaderId
	notifyWakeup = prevLeader != slotLeaderId
	c.pending.Unlock()
	if notifyWakeup && c.poller != nil && c.poller.eventPool != nil {
		c.poller.eventPool.notifyLeaderWakeup()
	}
	return nil
}

// isTimeout 判断用户是否超时
func (c *channelHandler) isTimeout() bool {
	if c.processing.Load() {
		return false
	}
	c.pending.RLock()
	hasPending := c.pending.eventQueue != nil && (c.processingIndex < c.pending.eventQueue.LastIndex() || c.pending.eventQueue.Len() > 0)
	lastActive := c.lastActive
	c.pending.RUnlock()
	if hasPending {
		return false
	}
	now := fasttime.UnixTimestamp()
	// Avoid unsigned underflow when the wall clock moves backwards.
	return now > lastActive && now-lastActive > uint64(options.G.Poller.ChannelTimeout.Seconds())
}

// groupByType 将待处理事件按照事件类型分组
func (c *channelHandler) groupByType(events []*eventbus.Event) map[eventbus.EventType][]*eventbus.Event {
	group := make(map[eventbus.EventType][]*eventbus.Event)
	for _, event := range events {
		group[event.Type] = append(group[event.Type], event)
	}
	return group
}
