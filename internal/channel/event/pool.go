package event

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
)

type EventPool struct {
	pollers      []*poller
	wklog.Log
	handler      eventbus.ChannelEventHandler
	boundCluster icluster.ICluster
}

func (e *EventPool) BindCluster(c icluster.ICluster) {
	if e == nil {
		return
	}
	e.boundCluster = c
}

func (e *EventPool) cluster() icluster.ICluster {
	if e != nil && e.boundCluster != nil {
		return e.boundCluster
	}
	return service.Cluster
}

func (e *EventPool) PendingQueueLen(channelId string, channelType uint8) int {
	if e == nil || len(e.pollers) == 0 {
		return 0
	}
	p := e.pollerByChannel(channelId, channelType)
	return p.pendingQueueLen(channelId, channelType)
}

func (e *EventPool) WaitCapacity(ctx context.Context, channelId string, channelType uint8, batchSize int) error {
	if e == nil || len(e.pollers) == 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = 1
	}
	for {
		qLen := e.PendingQueueLen(channelId, channelType)
		if qLen+batchSize <= eventbus.MaxEventQueueSize {
			return nil
		}
		e.Advance(channelId, channelType)
		select {
		case <-ctx.Done():
			return fmt.Errorf("channel %s/%d event queue backpressure timeout (len=%d): %w", channelId, channelType, qLen, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func NewEventPool(handler eventbus.ChannelEventHandler) *EventPool {
	u := &EventPool{
		handler: handler,
		Log:     wklog.NewWKLog("EventPool"),
	}
	for i := 0; i < options.G.Poller.ChannelCount; i++ {
		p := newPoller(i, u)
		u.pollers = append(u.pollers, p)
	}
	return u
}

// Start 启动
func (e *EventPool) Start() error {
	for _, p := range e.pollers {
		err := p.start()
		if err != nil {
			e.Error("start poller failed", zap.Error(err))
			return err
		}
	}
	return nil
}

// Stop 停止
func (e *EventPool) Stop() {
	for _, p := range e.pollers {
		p.stop()
	}
}

// AddEvent 添加事件
func (e *EventPool) AddEvent(channelId string, channelType uint8, event *eventbus.Event) {
	p := e.pollerByChannel(channelId, channelType)
	p.addEvent(channelId, channelType, event)
}

// Advance 推进事件
func (e *EventPool) Advance(channelId string, channelType uint8) {
	p := e.pollerByChannel(channelId, channelType)
	p.advance()
}

func (e *EventPool) pollerByChannel(channelId string, _ uint8) *poller {
	h := fnv.New32a()
	h.Write([]byte(channelId))
	i := h.Sum32() % uint32(len(e.pollers))
	return e.pollers[i]
}
