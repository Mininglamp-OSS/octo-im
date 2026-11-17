package eventbus

import "context"

func RegisterChannel(channel IChannel) {
	Channel = newChannelPlus(channel)
}

var Channel *channelPlus

type IChannel interface {
	// AddEvent 添加事件
	AddEvent(channelId string, channelType uint8, event *Event)
	// Advance 推进事件（让事件池不需要等待直接执行下一轮事件）
	Advance(channelId string, channelType uint8)
}

type channelCapacityWaiter interface {
	WaitCapacity(ctx context.Context, channelId string, channelType uint8, batchSize int) error
}

type channelBatchTryAdder interface {
	TryAddEvent(channelId string, channelType uint8, event *Event) bool
	TryAddBatch(channelId string, channelType uint8, events []*Event) bool
}

type channelPlus struct {
	channel IChannel
}

func newChannelPlus(channel IChannel) *channelPlus {
	return &channelPlus{
		channel: channel,
	}
}

func (c *channelPlus) WaitCapacity(ctx context.Context, channelId string, channelType uint8, batchSize int) error {
	if c == nil || c.channel == nil {
		return nil
	}
	if w, ok := c.channel.(channelCapacityWaiter); ok && w != nil {
		return w.WaitCapacity(ctx, channelId, channelType, batchSize)
	}
	return nil
}

// TryAddEvent atomically admits a single event if channel queue capacity permits.
func (c *channelPlus) TryAddEvent(channelId string, channelType uint8, event *Event) bool {
	if c == nil || c.channel == nil {
		return false
	}
	if ta, ok := c.channel.(channelBatchTryAdder); ok && ta != nil {
		return ta.TryAddEvent(channelId, channelType, event)
	}
	c.channel.AddEvent(channelId, channelType, event)
	return true
}

// TryAddBatch atomically admits all events in the slice or none at all.
func (c *channelPlus) TryAddBatch(channelId string, channelType uint8, events []*Event) bool {
	if c == nil || c.channel == nil {
		return false
	}
	if len(events) == 0 {
		return true
	}
	if ta, ok := c.channel.(channelBatchTryAdder); ok && ta != nil {
		return ta.TryAddBatch(channelId, channelType, events)
	}
	for _, ev := range events {
		c.channel.AddEvent(channelId, channelType, ev)
	}
	return true
}

// AddEvent 添加事件
func (c *channelPlus) AddEvent(channelId string, channelType uint8, event *Event) {
	c.channel.AddEvent(channelId, channelType, event)
}

// AddEvents 添加事件
func (c *channelPlus) AddEvents(channelId string, channelType uint8, events []*Event) {
	for _, event := range events {
		c.channel.AddEvent(channelId, channelType, event)
	}
}

// Advance 无等待快速推进事件
func (c *channelPlus) Advance(channelId string, channelType uint8) {
	c.channel.Advance(channelId, channelType)
}

// SendMessage 发送消息
func (c *channelPlus) SendMessage(channelId string, channelType uint8, event *Event) {
	c.channel.AddEvent(channelId, channelType, event)
}

// TrySendMessage 发送消息（带背压原子入队）
func (c *channelPlus) TrySendMessage(channelId string, channelType uint8, event *Event) bool {
	return c.TryAddEvent(channelId, channelType, event)
}
