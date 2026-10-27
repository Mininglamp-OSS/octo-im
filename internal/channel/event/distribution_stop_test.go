package event

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
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

type stoppingDistributionHandler struct {
	entered chan struct{}
	proceed chan struct{}
}
type stoppingDistributionCluster struct{ icluster.ICluster }

func (stoppingDistributionCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) {
	return 1, nil
}
func (h *stoppingDistributionHandler) OnEvent(ctx *eventbus.ChannelContext) {
	close(h.entered)
	<-h.proceed
	ctx.RetryEvents = ctx.Events
}

func TestDistributionStopRejectsLateRetriesAndAdmissions(t *testing.T) {
	previous, previousCluster := options.G, service.Cluster
	options.G = options.New()
	options.G.Poller.ChannelCount = 1
	options.G.Poller.ChannelGoroutine = 1
	service.Cluster = stoppingDistributionCluster{}
	t.Cleanup(func() { options.G, service.Cluster = previous, previousCluster })
	handler := &stoppingDistributionHandler{entered: make(chan struct{}), proceed: make(chan struct{})}
	pool := NewEventPool(handler)
	require.NoError(t, pool.Start())
	var released atomic.Int32
	makeEvent := func() *eventbus.Event {
		return &eventbus.Event{Type: eventbus.EventChannelDistribute, Distribution: &eventbus.DistributionState{Release: func() { released.Add(1) }}}
	}
	pool.AddEvent("room", 2, makeEvent())
	pool.Advance("room", 2)
	<-handler.entered
	stopped := make(chan struct{})
	go func() { pool.Stop(); close(stopped) }()
	t.Cleanup(func() { close(handler.proceed); <-stopped })
	p := pool.pollerByChannel("room", 2)
	require.Eventually(t, func() bool { p.RLock(); defer p.RUnlock(); return p.stopped }, 500*time.Millisecond, time.Millisecond,
		"a stopping pool must reject work before waiting for active workers")
	pool.AddEvent("room", 2, makeEvent())
	require.EqualValues(t, 1, released.Load(), "late admissions must release immediately")
}
