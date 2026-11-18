package event

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestXIN2857_YP102_PollerAdmissionAndClearRetirementRace(t *testing.T) {
	oldOpts, oldCluster := options.G, service.Cluster
	t.Cleanup(func() {
		options.G = oldOpts
		service.Cluster = oldCluster
	})

	opts := options.New()
	opts.Cluster.NodeId = 1
	opts.Poller.ChannelTimeout = 1 * time.Second
	opts.Poller.ChannelEventMaxSizePerBatch = 64 * 1024
	options.G = opts

	clusterStub := &round2PollerCluster{}
	clusterStub.slotLeader.Store(1)
	service.Cluster = clusterStub

	budget := eventbus.NewPostCommitBudget(64*1024*1024, 65536)

	var admittedTotal atomic.Int32
	var consumedTotal atomic.Int32
	stressSink := &funcChannelSink{
		fn: func(ctx *eventbus.ChannelContext) {
			for _, ev := range ctx.Events {
				if ev.Ticket != nil {
					ev.Ticket.Release()
					ev.Ticket = nil
				}
				consumedTotal.Add(1)
			}
		},
	}
	stressPool := NewEventPool(stressSink)
	sp := newPoller(1, stressPool)

	// Deterministic window: producer enters tryAddBatch, passes p.Lock()/p.Unlock(),
	// and while it is between p.Unlock() and c.pending.Lock(), sp.clear() sweeps the poller.
	chDet := "ch_det_yp102"
	hDet := newChannelHandler(chDet, 2, sp)
	hDet.lastActive = fasttime.UnixTimestamp() - 100
	sp.Lock()
	sp.waitlist.push(hDet)

	detEv := &eventbus.Event{
		Type:      eventbus.EventChannelOnSend,
		MessageId: 1,
		Conn:      &eventbus.Conn{Uid: "u1", DeviceId: opts.SystemDeviceId},
		Frame:     &wkproto.SendPacket{ClientMsgNo: "det-1", Payload: []byte("x")},
	}
	detTks, ok := budget.TryReserve(chDet, []*eventbus.Event{detEv})
	require.True(t, ok)
	detEv.Ticket = detTks[0]

	bigBatch := make([]*eventbus.Event, 5000000)
	bigBatch[len(bigBatch)-1] = detEv

	// Waiter #1 on sp.Lock(): producer calling tryAddBatch
	w1Ready := make(chan struct{})
	doneDet := make(chan bool, 1)
	go func() {
		close(w1Ready)
		doneDet <- sp.tryAddBatch(chDet, 2, bigBatch)
	}()
	<-w1Ready
	time.Sleep(5 * time.Millisecond)

	// Waiter #2 on sp.Lock(): sp.clear() directly queued behind Waiter #1 in mutex FIFO!
	w2Ready := make(chan struct{})
	w2Done := make(chan struct{})
	go func() {
		close(w2Ready)
		sp.clear()
		close(w2Done)
	}()
	<-w2Ready
	time.Sleep(5 * time.Millisecond)

	// Release sp.Lock() -> Waiter #1 runs p.handler(chDet, 2) & p.Unlock() -> hands sp.Lock() directly to Waiter #2 (sp.clear())!
	sp.Unlock()
	<-w2Done
	// In loopEvent(), handleEvents() runs every tick; run one cycle before Waiter #1 finishes scanning bigBatch:
	sp.handleEvents()
	require.True(t, <-doneDet)
	require.NotNil(t, sp.handler(chDet, 2), "channelHandler with in-flight admission must not be removed from waitlist by clear()")
	admittedTotal.Add(1)

	// Concurrent stress loop across tryAddEvent, tryAddBatch, and addEvent
	const rounds = 200
	for r := 0; r < rounds; r++ {
		chID := fmt.Sprintf("ch_race_%d", r)
		h := newChannelHandler(chID, 2, sp)
		h.lastActive = fasttime.UnixTimestamp() - 100
		sp.Lock()
		sp.waitlist.push(h)

		ev := &eventbus.Event{
			Type:      eventbus.EventChannelOnSend,
			MessageId: int64(r + 2),
			Conn:      &eventbus.Conn{Uid: "u1", DeviceId: opts.SystemDeviceId},
			Frame:     &wkproto.SendPacket{ClientMsgNo: "s", Payload: []byte("x")},
		}
		tks, ok := budget.TryReserve(chID, []*eventbus.Event{ev})
		require.True(t, ok)
		ev.Ticket = tks[0]

		batch := make([]*eventbus.Event, 4096)
		batch[4095] = ev

		var wg sync.WaitGroup
		wg.Add(2)
		go func(iter int) {
			defer wg.Done()
			var okAdd bool
			switch iter % 3 {
			case 0:
				okAdd = sp.tryAddBatch(chID, 2, batch)
			case 1:
				okAdd = sp.tryAddEvent(chID, 2, ev)
			case 2:
				sp.addEvent(chID, 2, ev)
				okAdd = true
			}
			if okAdd {
				admittedTotal.Add(1)
			} else if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
		}(r)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			sp.clear()
		}()
		sp.Unlock()
		wg.Wait()
	}

	// Drain all handlers on waitlist
	drainDeadline := time.Now().Add(500 * time.Millisecond)
	for consumedTotal.Load() < admittedTotal.Load() && time.Now().Before(drainDeadline) {
		sp.handleEvents()
		time.Sleep(2 * time.Millisecond)
	}
	require.Equal(t, admittedTotal.Load(), consumedTotal.Load(), "every admitted event must be consumed without orphaning")
	usedBytes, usedItems, _ := budget.Stats()
	require.Zero(t, usedItems, "all reserved PostCommitTickets must be released")
	require.Zero(t, usedBytes, "all reserved PostCommitBudget bytes must be released")
}

type funcChannelSink struct {
	fn func(ctx *eventbus.ChannelContext)
}

func (f *funcChannelSink) OnEvent(ctx *eventbus.ChannelContext) {
	if f.fn != nil {
		f.fn(ctx)
	}
}
