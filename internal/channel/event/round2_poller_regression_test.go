package event

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type round2PollerCluster struct {
	icluster.ICluster
	slotLeader atomic.Uint64
}

func (c *round2PollerCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) {
	id := c.slotLeader.Load()
	if id == 0 {
		return 0, errors.New("slot leader temporarily unavailable")
	}
	return id, nil
}

type round2SinkHandler struct {
	delivered atomic.Int32
}

func (s *round2SinkHandler) OnEvent(ctx *eventbus.ChannelContext) {
	s.delivered.Add(int32(len(ctx.Events)))
}

func TestRound2_BFR2_4_PollerDoesNotEvictBackloggedChannelOrDropOnTransientZeroLeader(t *testing.T) {
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
	service.Cluster = clusterStub

	sink := &round2SinkHandler{}
	pool := NewEventPool(sink)
	p := newPoller(0, pool)

	// 1. Enqueue 10 EventChannelOnSend events while slotLeaderId is 0 (transient handoff)
	for i := 0; i < 10; i++ {
		p.addEvent("ch_test", 2, &eventbus.Event{
			Type:      eventbus.EventChannelOnSend,
			MessageId: int64(i + 1),
			Conn:      &eventbus.Conn{Uid: "u1", DeviceId: opts.SystemDeviceId},
			Frame:     &wkproto.SendPacket{ClientMsgNo: "cmn"},
		})
	}
	h := p.handler("ch_test", 2)
	require.NotNil(t, h)

	// Force lastActive into the past beyond ChannelTimeout; isTimeout() must still return false while pending events exist
	h.pending.Lock()
	h.lastActive = fasttime.UnixTimestamp() - 100
	h.pending.Unlock()
	require.False(t, h.isTimeout(), "channelHandler with unconsumed pending events must never time out")
	p.clear()
	require.NotNil(t, p.handler("ch_test", 2), "poller.clear() must not evict channelHandler with pending events")

	// 2. Run advanceEvents while slotLeader is 0: events must NOT be truncated or dropped
	batch := h.events()
	require.Len(t, batch, 10)
	h.advanceEvents(batch)
	require.Equal(t, int32(0), sink.delivered.Load(), "events must not be dispatched with SlotLeaderId=0")
	require.True(t, h.hasEvent(), "pending events must remain in eventQueue after transient leader resolution failure")

	// 3. Once slotLeader resolves to node 1, the retained events are delivered without loss
	clusterStub.slotLeader.Store(1)
	batch2 := h.events()
	require.Len(t, batch2, 10)
	h.advanceEvents(batch2)
	require.Equal(t, int32(10), sink.delivered.Load(), "all 10 retained events must be delivered once slot leader resolves")
	require.False(t, h.hasEvent())
}
