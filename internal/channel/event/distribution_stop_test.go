package event

import (
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/stretchr/testify/require"
)

func TestDistributionStopReleasesQueuedAndRetainedReservations(t *testing.T) {
	previous := options.G
	options.G = options.New()
	options.G.Poller.ChannelCount = 1
	options.G.Poller.ChannelGoroutine = 1
	t.Cleanup(func() { options.G = previous })
	pool := NewEventPool(nil)
	var released atomic.Int32
	event := func() *eventbus.Event {
		return &eventbus.Event{Type: eventbus.EventChannelDistribute, Distribution: &eventbus.DistributionState{Release: func() { released.Add(1) }}}
	}
	pool.AddEvent("room", 2, event())
	handler := pool.pollerByChannel("room", 2).handler("room", 2)
	handler.pending.retryEvents = []*eventbus.Event{event()}
	pool.Stop()
	require.EqualValues(t, 2, released.Load(), "stopping a pool must release both unread and retained snapshots")
}
