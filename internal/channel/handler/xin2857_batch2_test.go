package handler

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type barrierChannelBus struct {
	eventbus.IChannel
	mu          sync.Mutex
	enteredCh   chan struct{}
	releaseCh   chan struct{}
	blockFirst  atomic.Bool
	rejectAll   atomic.Bool
	batchCalls  atomic.Int32
	admittedEvs []*eventbus.Event
}

func (b *barrierChannelBus) TryAddBatch(_ string, _ uint8, events []*eventbus.Event) bool {
	b.batchCalls.Add(1)
	if b.blockFirst.CompareAndSwap(true, false) {
		close(b.enteredCh)
		<-b.releaseCh
	}
	if b.rejectAll.Load() {
		return false
	}
	b.mu.Lock()
	b.admittedEvs = append(b.admittedEvs, events...)
	b.mu.Unlock()
	return true
}

func (b *barrierChannelBus) Advance(string, uint8) {}

func TestXIN2857_YP103_AtomicForwardDedupInFlightRollbackAndCommit(t *testing.T) {
	oldOpts := options.G
	t.Cleanup(func() { options.G = oldOpts })
	opts := options.New()
	opts.Cluster.NodeId = 1002
	options.G = opts

	budget := eventbus.NewPostCommitBudget(16*1024*1024, 16384)
	bus := &barrierChannelBus{
		enteredCh: make(chan struct{}),
		releaseCh: make(chan struct{}),
	}

	h := NewHandler()
	defer h.DrainAndStop(0)
	h.BindNodeDeps(NodeDeps{
		NodeId:           1002,
		ChannelBus:       bus,
		Cluster:          &forwardChannelCluster{leader: 1002},
		PostCommitBudget: budget,
	})

	ev := &eventbus.Event{
		Type:       eventbus.EventChannelOnSend,
		Conn:       &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "yp103-1", ChannelID: "ch_yp103", ChannelType: wkproto.ChannelTypeGroup, Payload: []byte("dedup")},
		MessageId:  9301,
		MessageSeq: 77,
		ReasonCode: wkproto.ReasonSuccess,
	}
	fReq := &forwardChannelEventReq{
		fromNode:    1001,
		channelId:   "ch_yp103",
		channelType: wkproto.ChannelTypeGroup,
		events:      eventbus.EventBatch{ev},
	}
	raw, err := fReq.encodeWithTrailer(true)
	require.NoError(t, err)
	env, _, err := forward.Envelope(raw, []*eventbus.Event{ev})
	require.NoError(t, err)

	// 1. Rollback on queue rejection: when TryAddBatch returns false, inFlight key and ticket must roll back.
	bus.rejectAll.Store(true)
	require.Equal(t, forward.StatusRetry, h.AcceptForward(env), "Y-P1-03: rejected admission must return StatusRetry")
	_, usedAfterReject, _ := budget.Stats()
	require.Zero(t, usedAfterReject, "Y-P1-03: rejected admission must release reserved ticket")

	// 2. Concurrent in-flight deduplication: block first RPC at TryAddBatch barrier and send concurrent retry RPC.
	bus.rejectAll.Store(false)
	bus.blockFirst.Store(true)

	firstResCh := make(chan proto.Status, 1)
	go func() {
		firstResCh <- h.AcceptForward(env)
	}()
	<-bus.enteredCh

	// While first RPC is in flight (not yet committed), a concurrent retry MUST see inFlight and return StatusRetry
	// without reserving a second ticket or calling TryAddBatch a second time!
	retryStatus := h.AcceptForward(env)
	close(bus.releaseCh)
	firstStatus := <-firstResCh

	require.Equal(t, proto.StatusOK, firstStatus)
	require.Equal(t, forward.StatusRetry, retryStatus, "Y-P1-03: concurrent retry while first admission is in-flight must return StatusRetry, not duplicate admission")
	require.Equal(t, int32(2), bus.batchCalls.Load(), "Y-P1-03: TryAddBatch must only be called once for the rejected attempt and once for the winning in-flight attempt")

	bus.mu.Lock()
	require.Len(t, bus.admittedEvs, 1, "Y-P1-03: ChannelBus must receive the event exactly once")
	admittedTicket := bus.admittedEvs[0].Ticket
	bus.mu.Unlock()

	_, usedInFlight, _ := budget.Stats()
	require.Equal(t, int64(1), usedInFlight, "Y-P1-03: only 1 PostCommitTicket must be reserved")

	// 3. Subsequent retry after commit returns StatusOK idempotently without re-enqueuing:
	require.Equal(t, proto.StatusOK, h.AcceptForward(env))
	require.Equal(t, int32(2), bus.batchCalls.Load(), "Y-P1-03: committed key must short-circuit before TryAddBatch")

	if admittedTicket != nil {
		admittedTicket.Release()
	}
	_, usedFinal, _ := budget.Stats()
	require.Zero(t, usedFinal)
}
