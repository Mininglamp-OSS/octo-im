package event

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
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
		eventQueue  *eventbus.EventQueue
		retryEvents []*eventbus.Event // retryEvents is a bounded previously dequeued batch.
		retryAt     time.Time         // retryAt schedules retries before reading newer work.
	}
	poller  *poller
	handler eventbus.ChannelEventHandler
	// 处理中的下标位置
	processingIndex uint64
	// 是否正在处理
	processing atomic.Bool
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

func (c *channelHandler) addEvent(event *eventbus.Event) {
	c.pending.Lock()
	defer c.pending.Unlock()
	event.Index = c.pending.eventQueue.LastIndex() + 1
	c.pending.eventQueue.Append(event)

	c.lastActive = fasttime.UnixTimestamp()
}

func (c *channelHandler) hasEvent() bool {
	c.pending.RLock()
	defer c.pending.RUnlock()
	if c.processing.Load() {
		return false
	}
	if len(c.pending.retryEvents) > 0 {
		// Preserve channel order: newer work waits behind the bounded retry batch,
		// including its backoff, trading queue fairness for delivery ordering.
		return !time.Now().Before(c.pending.retryAt)
	}
	return c.processingIndex < c.pending.eventQueue.LastIndex()
}

func (u *channelHandler) events() []*eventbus.Event {
	u.pending.Lock()
	defer u.pending.Unlock()
	if len(u.pending.retryEvents) > 0 {
		if time.Now().Before(u.pending.retryAt) {
			return nil
		}
		events := u.pending.retryEvents
		u.pending.retryEvents = nil
		return events
	}
	events := u.pending.eventQueue.SliceWithSize(u.processingIndex+1, u.pending.eventQueue.LastIndex()+1, options.G.Poller.ChannelEventMaxSizePerBatch)
	if len(events) == 0 {
		return nil
	}
	eventLastIndex := events[len(events)-1].Index

	// 截取掉之前的事件
	u.pending.eventQueue.TruncateTo(eventLastIndex + 1)
	u.processingIndex = eventLastIndex
	return events
}

// 推进事件
func (c *channelHandler) advanceEvents(events []*eventbus.Event) {

	c.processing.Store(true)
	defer func() {
		c.processing.Store(false)
	}()

	// 检查和更新leaderId
	err := c.checkAndUpdateLeaderIdChange()
	if err != nil {
		c.Error("advanceEvents: checkAndUpdateLeaderIdChange failed", zap.Error(err))
		c.slotLeaderId = 0
	}

	// 按类型分组
	group := c.groupByType(events)
	// 处理事件
	for eventType, events := range group {
		// 从对象池中获取上下文
		ctx := c.poller.getContext()
		ctx.ChannelId = c.channelId
		ctx.ChannelType = c.channelType
		ctx.EventType = eventType
		ctx.Events = events
		ctx.SlotLeaderId = c.slotLeaderId
		// 处理事件
		c.handler.OnEvent(ctx)
		if len(ctx.RetryEvents) > 0 {
			c.pending.Lock()
			c.pending.retryEvents = append(c.pending.retryEvents, ctx.RetryEvents...)
			if ctx.RetryAt.After(c.pending.retryAt) {
				c.pending.retryAt = ctx.RetryAt
			}
			c.pending.Unlock()
		}

		// 释放上下文
		c.poller.putContext(ctx)
	}

	// 推进事件
	c.pending.RLock()
	hasPending := c.pending.eventQueue.Len() > 0 || len(c.pending.retryEvents) > 0
	c.pending.RUnlock()
	if hasPending {
		c.poller.advance()
	}
}

// checkAndUpdateLeaderIdChange 检查并更新leaderId变化
func (c *channelHandler) checkAndUpdateLeaderIdChange() error {
	c.pending.Lock()
	defer c.pending.Unlock()
	slotLeaderId, err := service.Cluster.SlotLeaderIdOfChannel(c.channelId, c.channelType)
	if err != nil {
		c.Error("checkLeaderIdChange: get leader id failed", zap.Error(err), zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType))
		return err
	}
	if slotLeaderId == 0 {
		c.Warn("checkLeaderIdChange: leader id is 0", zap.String("channelId", c.channelId), zap.Uint8("channelType", c.channelType))
		return errors.New("checkAndUpdateLeaderIdChange： leader id is 0")
	}
	c.slotLeaderId = slotLeaderId
	return nil
}

// isTimeout 判断用户是否超时
func (c *channelHandler) isTimeout() bool {
	c.pending.RLock()
	defer c.pending.RUnlock()
	if c.processing.Load() || len(c.pending.retryEvents) > 0 || (c.pending.eventQueue != nil && c.processingIndex < c.pending.eventQueue.LastIndex()) {
		return false
	}
	now, lastActive := fasttime.UnixTimestamp(), c.lastActive
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
