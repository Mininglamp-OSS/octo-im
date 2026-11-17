package event

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/lni/goutils/syncutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type poller struct {
	waitlist *linkedList
	wklog.Log
	sync.RWMutex

	tmpHandlers []*channelHandler
	stopper     *syncutil.Stopper
	handlePool  *ants.Pool
	index       int

	// tick次数
	tickCount int
	eventPool *EventPool

	contextPool *sync.Pool
	advanceC    chan struct{}
	needAdvance atomic.Bool
	rrCursor    int
}

func newPoller(index int, eventPool *EventPool) *poller {

	p := &poller{
		waitlist:  newLinkedList(),
		stopper:   syncutil.NewStopper(),
		index:     index,
		eventPool: eventPool,
		Log:       wklog.NewWKLog(fmt.Sprintf("poller[%d]", index)),
		contextPool: &sync.Pool{
			New: func() interface{} {
				return &eventbus.ChannelContext{}
			},
		},
		advanceC: make(chan struct{}, 1),
	}

	var err error
	p.handlePool, err = ants.NewPool(options.G.Poller.ChannelGoroutine, ants.WithNonblocking(true), ants.WithPanicHandler(func(i interface{}) {
		p.Panic("channel handle panic", zap.Any("panic", i), zap.Stack("stack"))
	}))
	if err != nil {
		p.Panic("new ants pool failed", zap.String("error", err.Error()))
	}
	return p
}

func (p *poller) start() error {
	p.stopper.RunWorker(p.loopEvent)
	return nil
}

func (p *poller) stop() {
	p.stopper.Stop()
}

func (p *poller) addEvent(channelId string, channelType uint8, event *eventbus.Event) {
	p.Lock()
	h := p.handler(channelId, channelType)
	if h == nil {
		h = newChannelHandler(channelId, channelType, p)
		p.waitlist.push(h)
	}
	p.Unlock()
	h.addEvent(event)
}

func (p *poller) tryAddEvent(channelId string, channelType uint8, event *eventbus.Event) bool {
	p.Lock()
	h := p.handler(channelId, channelType)
	if h == nil {
		h = newChannelHandler(channelId, channelType, p)
		p.waitlist.push(h)
	}
	p.Unlock()
	return h.tryAddEvent(event)
}

func (p *poller) tryAddBatch(channelId string, channelType uint8, events []*eventbus.Event) bool {
	p.Lock()
	h := p.handler(channelId, channelType)
	if h == nil {
		h = newChannelHandler(channelId, channelType, p)
		p.waitlist.push(h)
	}
	p.Unlock()
	return h.tryAddBatch(events)
}

func (p *poller) loopEvent() {
	tk := time.NewTicker(options.G.Poller.IntervalTick)
	defer tk.Stop()
	for {
		p.handleEvents()
		select {
		case <-tk.C:
			p.tick()
		case <-p.advanceC:
		case <-p.stopper.ShouldStop():
			return
		}
	}
}

func (p *poller) tick() {
	p.tickCount++
	if p.tickCount%options.G.Poller.ClearIntervalTick == 0 {
		p.tickCount = 0
		p.clear()
	}
}

func (p *poller) clear() {
	p.Lock()
	defer p.Unlock()
	p.waitlist.readHandlers(&p.tmpHandlers)
	for _, h := range p.tmpHandlers {
		if h.isTimeout() {
			p.waitlist.remove(h.channelId, h.channelType)
		}
	}

}

func (p *poller) handleEvents() {
	p.waitlist.readHandlers(&p.tmpHandlers)
	n := len(p.tmpHandlers)
	if n > 0 {
		start := p.rrCursor % n
		var err error
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			h := p.tmpHandlers[idx]
			if h.hasEvent() && h.processing.CompareAndSwap(false, true) {
				err = p.handlePool.Submit(func() {
					events := h.events()
					h.advanceEvents(events)
				})
				if err != nil {
					h.processing.Store(false)
					p.needAdvance.Store(true)
					p.rrCursor = idx
					break
				}
				p.rrCursor = (idx + 1) % n
			}
		}
	}
	for i := range p.tmpHandlers {
		p.tmpHandlers[i] = nil
	}
	p.tmpHandlers = p.tmpHandlers[:0]
	if cap(p.tmpHandlers) > 1024 {
		p.tmpHandlers = nil
	}
}

func (p *poller) advance() {
	select {
	case p.advanceC <- struct{}{}:
	default:
	}
}

func (p *poller) handler(channelId string, channelType uint8) *channelHandler {
	return p.waitlist.get(channelId, channelType)
}

func (p *poller) pendingQueueLen(channelId string, channelType uint8) int {
	p.RLock()
	h := p.handler(channelId, channelType)
	p.RUnlock()
	if h == nil {
		return 0
	}
	return h.pendingQueueLen()
}

func (p *poller) getContext() *eventbus.ChannelContext {
	return p.contextPool.Get().(*eventbus.ChannelContext)
}

func (p *poller) putContext(ctx *eventbus.ChannelContext) {
	ctx.Reset()
	p.contextPool.Put(ctx)
}
