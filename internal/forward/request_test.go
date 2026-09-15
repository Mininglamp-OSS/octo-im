package forward

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type requestCluster struct {
	icluster.ICluster
	request func(context.Context, uint64, string, []byte) (*proto.Response, error)
}

type failUser struct {
	eventbus.IUser
	events   []*eventbus.Event
	advances int
}

func (u *failUser) AddEvent(_ string, event *eventbus.Event) { u.events = append(u.events, event) }
func (u *failUser) Advance(string)                           { u.advances++ }

func (c *requestCluster) RequestWithContext(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	return c.request(ctx, node, path, body)
}

func TestRequestDoesNotReplayAfterAmbiguousSend(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	leader := uint64(2)
	var nodes []uint64
	var bodies [][]byte
	service.Cluster = &requestCluster{request: func(_ context.Context, node uint64, _ string, body []byte) (*proto.Response, error) {
		nodes = append(nodes, node)
		bodies = append(bodies, append([]byte(nil), body...))
		if len(nodes) == 1 {
			leader = 3
			return nil, errors.New("response lost after admission")
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}}
	events := []*eventbus.Event{{Frame: &wkproto.SendPacket{ClientMsgNo: "stable"}}}
	require.ErrorIs(t, Request(UserPath, []byte("immutable encrypted packet"), events, func() uint64 { return leader }, 1, nil), ErrOutcomeUnknown)
	require.Equal(t, []uint64{2}, nodes)
	require.Len(t, bodies, 1)
}

func TestGateRejectsWithoutBlockingWhenForwardCapacityIsBusy(t *testing.T) {
	gate := NewGate(4)
	require.True(t, gate.TryAcquire())
	defer gate.Release()

	start := time.Now()
	require.False(t, gate.TryAcquire())
	require.Less(t, time.Since(start), 100*time.Millisecond)
}

func TestAmbiguousResponseCannotContradictPeerSuccess(t *testing.T) {
	oldOptions, oldCluster, oldUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = oldOptions, oldCluster, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	u := &failUser{}
	eventbus.RegisterUser(u)
	conn := &eventbus.Conn{Uid: "u", NodeId: 1}
	event := &eventbus.Event{Type: eventbus.EventOnSend, Conn: conn, Frame: &wkproto.SendPacket{ClientSeq: 7, ClientMsgNo: "stable"}}
	service.Cluster = &requestCluster{request: func(_ context.Context, _ uint64, _ string, _ []byte) (*proto.Response, error) {
		// The remote side admitted the event and its canonical success ACK raced
		// with the lost admission response.
		eventbus.User.ConnWrite("peer", conn, &wkproto.SendackPacket{ClientSeq: 7, ClientMsgNo: "stable", ReasonCode: wkproto.ReasonSuccess})
		eventbus.User.Advance(conn.Uid)
		return nil, errors.New("response lost after admission")
	}}

	err := Request(UserPath, nil, []*eventbus.Event{event}, func() uint64 { return 2 }, 1, nil)
	require.ErrorIs(t, err, ErrOutcomeUnknown)
	Fail([]*eventbus.Event{event}, err)

	require.Len(t, u.events, 1)
	ack := u.events[0].Frame.(*wkproto.SendackPacket)
	require.Equal(t, wkproto.ReasonSuccess, ack.ReasonCode)
	require.Equal(t, 1, u.advances)
}

func TestRequestReResolvesAfterExplicitRetry(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	leader := uint64(2)
	var nodes []uint64
	service.Cluster = &requestCluster{request: func(_ context.Context, node uint64, _ string, _ []byte) (*proto.Response, error) {
		nodes = append(nodes, node)
		if len(nodes) == 1 {
			leader = 3
			return &proto.Response{Status: StatusRetry}, nil
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}}
	events := []*eventbus.Event{{Frame: &wkproto.SendPacket{ClientMsgNo: "stable"}}}
	require.NoError(t, Request(UserPath, nil, events, func() uint64 { return leader }, 1, nil))
	require.Equal(t, []uint64{2, 3}, nodes)
}

func TestRequestDoesNotReplayUnkeyedOrTransientSend(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	for _, packet := range []*wkproto.SendPacket{{}, {Framer: wkproto.Framer{NoPersist: true}, ClientMsgNo: "key"}} {
		calls := 0
		service.Cluster = &requestCluster{request: func(context.Context, uint64, string, []byte) (*proto.Response, error) {
			calls++
			return nil, context.DeadlineExceeded
		}}
		require.Error(t, Request(UserPath, nil, []*eventbus.Event{{Frame: packet}}, func() uint64 { return 2 }, 1, nil))
		require.Equal(t, 1, calls)
	}
}

func TestRequestBudgetAndPermanentRejection(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	calls := 0
	service.Cluster = &requestCluster{request: func(context.Context, uint64, string, []byte) (*proto.Response, error) {
		calls++
		return &proto.Response{Status: proto.StatusNotFound}, nil
	}}
	event := &eventbus.Event{Frame: &wkproto.SendPacket{ClientMsgNo: "key"}}
	require.Error(t, Request(UserPath, nil, []*eventbus.Event{event}, func() uint64 { return 2 }, 1, nil))
	require.Equal(t, 1, calls, "mixed-version endpoint must fail explicitly without legacy fallback")
	event.ForwardDeadline = time.Now().Add(60 * time.Millisecond).UnixMilli()
	start := time.Now()
	require.Error(t, Request(UserPath, nil, []*eventbus.Event{event}, func() uint64 { return 0 }, 1, nil))
	require.Less(t, time.Since(start), time.Second)
	event.ForwardDeadline = time.Now().Add(Budget).UnixMilli()
	event.ForwardHops = maxHops
	_, _, err := Envelope(nil, []*eventbus.Event{event})
	require.Error(t, err)
}

func TestRequestCompatibleFallsBackBeforeAdmission(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	var paths []string
	service.Cluster = &requestCluster{request: func(_ context.Context, _ uint64, path string, _ []byte) (*proto.Response, error) {
		paths = append(paths, path)
		return nil, context.DeadlineExceeded
	}}
	legacyNode := uint64(0)
	err := RequestCompatible(UserPath, nil, []*eventbus.Event{{Frame: &wkproto.SendPacket{ClientMsgNo: "stable"}}}, func() uint64 { return 2 }, 1, nil, &CapabilityCache{}, func(node uint64) error {
		legacyNode = node
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{CapabilityPath}, paths)
	require.Equal(t, uint64(2), legacyNode)
}

func TestCapabilityProbeCannotOverrunEnvelopeBudget(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	service.Cluster = &requestCluster{request: func(ctx context.Context, _ uint64, _ string, _ []byte) (*proto.Response, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	event := &eventbus.Event{Frame: &wkproto.SendPacket{ClientMsgNo: "stable"}, ForwardDeadline: time.Now().Add(40 * time.Millisecond).UnixMilli()}
	legacyCalls := 0
	start := time.Now()
	err := RequestCompatible(UserPath, nil, []*eventbus.Event{event}, func() uint64 { return 2 }, 1, nil, &CapabilityCache{}, func(uint64) error {
		legacyCalls++
		return nil
	})
	require.Error(t, err)
	require.Zero(t, legacyCalls)
	require.Less(t, time.Since(start), 200*time.Millisecond)
}

func TestRequestCompatibleDoesNotFallBackAfterAmbiguousV1Attempt(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	leader := uint64(2)
	legacyCalls := 0
	service.Cluster = &requestCluster{request: func(_ context.Context, node uint64, path string, _ []byte) (*proto.Response, error) {
		if path == CapabilityPath {
			if node == 2 {
				return &proto.Response{Status: proto.StatusOK}, nil
			}
			return nil, context.DeadlineExceeded
		}
		leader = 3
		return nil, context.DeadlineExceeded
	}}
	err := RequestCompatible(UserPath, nil, []*eventbus.Event{{Frame: &wkproto.SendPacket{ClientMsgNo: "stable"}}}, func() uint64 { return leader }, 1, nil, &CapabilityCache{}, func(uint64) error {
		legacyCalls++
		return nil
	})
	require.ErrorIs(t, err, ErrOutcomeUnknown)
	require.Zero(t, legacyCalls)
}

func TestDecodeUsesRelativeBudgetAndClampsFutureValues(t *testing.T) {
	data := make([]byte, 9)
	binary.BigEndian.PutUint64(data, uint64((Budget+time.Minute)/time.Millisecond))
	data[8] = 1
	start := time.Now()
	_, deadline, hops, err := Decode(data)
	require.NoError(t, err)
	require.Equal(t, uint8(1), hops)
	require.WithinDuration(t, start.Add(Budget), time.UnixMilli(deadline), 100*time.Millisecond)

	data[8] = maxHops + 1
	_, _, _, err = Decode(data)
	require.Error(t, err)
}

func TestFailLeavesAmbiguousOutcomeUnansweredAndSkipsPostCommitEvents(t *testing.T) {
	oldOptions, oldUser := options.G, eventbus.User
	t.Cleanup(func() { options.G, eventbus.User = oldOptions, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	u := &failUser{}
	eventbus.RegisterUser(u)
	conn := &eventbus.Conn{Uid: "u", NodeId: 1}
	unkeyed := &eventbus.Event{Type: eventbus.EventOnSend, Conn: conn, Frame: &wkproto.SendPacket{ClientSeq: 7}}

	Fail([]*eventbus.Event{unkeyed}, ErrOutcomeUnknown)

	require.Empty(t, u.events)
	require.Zero(t, u.advances)

	u.events = nil
	keyed := &eventbus.Event{Type: eventbus.EventOnSend, Conn: conn, Frame: &wkproto.SendPacket{ClientSeq: 8, ClientMsgNo: "stable"}}
	Fail([]*eventbus.Event{keyed}, ErrOutcomeUnknown)
	require.Empty(t, u.events)
	require.Zero(t, u.advances)

	Fail([]*eventbus.Event{unkeyed}, ErrUnavailable)
	require.Len(t, u.events, 1)
	require.Equal(t, wkproto.ReasonNodeNotMatch, u.events[0].Frame.(*wkproto.SendackPacket).ReasonCode)
	require.Equal(t, 1, u.advances)

	u.events = nil
	Fail([]*eventbus.Event{{Type: eventbus.EventChannelDistribute, Conn: conn, Frame: &wkproto.SendPacket{ClientSeq: 7}}}, ErrUnavailable)
	require.Empty(t, u.events)

	conn.DeviceId = options.G.SystemDeviceId
	Fail([]*eventbus.Event{{Type: eventbus.EventChannelOnSend, Conn: conn, Frame: &wkproto.SendPacket{ClientSeq: 7}}}, ErrUnavailable)
	require.Empty(t, u.events)
}
