package event

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWorkerRejectionRetainsChannelEvents(t *testing.T) {
	old := options.G
	t.Cleanup(func() { options.G = old })
	options.G = options.New()
	options.G.Poller.ChannelCount = 1
	pool := NewEventPool(nil)
	defer pool.Stop()
	p := pool.pollerByChannel("room", 2)
	p.handlePool.Release()
	e := &eventbus.Event{Type: eventbus.EventChannelOnSend}
	pool.AddEvent("room", 2, e)
	p.handleEvents()
	h := p.handler("room", 2)
	require.False(t, h.processing.Load())
	require.True(t, h.hasEvent(), "a rejected worker must leave the batch queued")
	require.Equal(t, []*eventbus.Event{e}, h.events())
}
