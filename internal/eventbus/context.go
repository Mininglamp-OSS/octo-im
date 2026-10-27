package eventbus

import "time"

type UserContext struct {
	Uid       string
	EventType EventType
	Events    []*Event
	AddConn   func(conn *Conn)
}

func (c *UserContext) Reset() {
	c.EventType = EventUnknown
	c.Events = nil
}

type UserHandlerFunc func(ctx *UserContext)

type ChannelContext struct {
	RetryEvents  []*Event  // RetryEvents retains only unfinished distribution work.
	RetryAt      time.Time // RetryAt prevents a failed batch from busy-spinning.
	SlotLeaderId uint64    // 频道的槽领导节点Id
	ChannelId    string
	ChannelType  uint8
	EventType    EventType
	Events       []*Event
}

func (c *ChannelContext) Reset() {
	c.RetryEvents = nil
	c.RetryAt = time.Time{}
	c.EventType = EventUnknown
	c.Events = nil
	c.ChannelId = ""
	c.ChannelType = 0
}

type ChannelHandlerFunc func(ctx *ChannelContext)

type PushContext struct {
	Id        int
	EventType EventType
	Events    []*Event
}

func (p *PushContext) Reset() {
	p.Id = 0
	p.EventType = EventUnknown
	p.Events = nil
}

type PusherHandlerFunc func(ctx *PushContext)
