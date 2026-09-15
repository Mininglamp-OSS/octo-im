package event

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWorkerRejectionRetainsUserEvents(t *testing.T) {
	old := options.G
	t.Cleanup(func() { options.G = old })
	options.G = options.New()
	options.G.Poller.UserCount = 1
	pool := NewEventPool(&mockUserEventHandler{})
	defer pool.Stop()
	p := pool.pollerByUid("u")
	p.handlePool.Release()
	e := &eventbus.Event{Type: eventbus.EventConnRemove}
	pool.AddEvent("u", e)
	p.handleEvents()
	h := p.handler("u")
	require.False(t, h.processing.Load())
	require.True(t, h.hasEvent(), "a rejected worker must not consume already admitted events")
	require.Equal(t, []*eventbus.Event{e}, h.events())
}
