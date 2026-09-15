package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type forwardCluster struct {
	icluster.ICluster
	leader    uint64
	calls     int
	sendCalls int
	sendErr   error
	supportV1 bool
}

func (c *forwardCluster) GetSlotId(string) uint32    { return 19 }
func (c *forwardCluster) SlotLeaderId(uint32) uint64 { return c.leader }
func (c *forwardCluster) RequestWithContext(_ context.Context, _ uint64, path string, _ []byte) (*proto.Response, error) {
	c.calls++
	if path == forward.CapabilityPath && c.supportV1 {
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	return nil, errors.New("connection unavailable")
}
func (c *forwardCluster) Send(uint64, *proto.Message) error {
	c.sendCalls++
	return c.sendErr
}

type forwardUser struct {
	eventbus.IUser
	events  []*eventbus.Event
	cached  *eventbus.Conn
	touches int
}

func (u *forwardUser) AddEvent(_ string, e *eventbus.Event)          { u.events = append(u.events, e) }
func (u *forwardUser) Advance(string)                                {}
func (u *forwardUser) ConnById(string, uint64, int64) *eventbus.Conn { return u.cached }
func (u *forwardUser) TouchConn(string, uint64, int64) bool {
	u.touches++
	return u.cached != nil
}

func TestForwardRejectsFormerLeaderAndReportsFailure(t *testing.T) {
	oldOptions, oldCluster, oldUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = oldOptions, oldCluster, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 3
	c := &forwardCluster{leader: 4, sendErr: errors.New("legacy send unavailable")}
	service.Cluster = c
	u := &forwardUser{}
	eventbus.RegisterUser(u)
	h := NewHandler()
	e := &eventbus.Event{Type: eventbus.EventOnSend, Conn: &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7}, Frame: &wkproto.SendPacket{ClientMsgNo: "stable", ClientSeq: 42}}
	r := &forwardUserEventReq{uid: "u", fromNode: 1, events: eventbus.EventBatch{e}}
	body, err := r.encode()
	require.NoError(t, err)
	data, _, err := forward.Envelope(body, []*eventbus.Event{e})
	require.NoError(t, err)
	require.Equal(t, forward.StatusRetry, h.acceptForward(data))
	require.Empty(t, u.events)
	options.G.Cluster.NodeId = 4
	require.Equal(t, proto.StatusOK, h.acceptForward(data))
	require.Len(t, u.events, 1)
	require.Equal(t, "stable", u.events[0].Frame.(*wkproto.SendPacket).ClientMsgNo)
	options.G.Cluster.NodeId = 1
	u.events = nil
	e.ForwardDeadline = time.Now().Add(60 * time.Millisecond).UnixMilli()
	h.OnEvent(&eventbus.UserContext{Uid: "u", EventType: eventbus.EventOnSend, Events: []*eventbus.Event{e}})
	require.Greater(t, c.calls, 0)
	require.Len(t, u.events, 1)
	ack := u.events[0].Frame.(*wkproto.SendackPacket)
	require.Equal(t, wkproto.ReasonNodeNotMatch, ack.ReasonCode)
	require.Equal(t, uint64(42), ack.ClientSeq)
	require.Equal(t, "stable", ack.ClientMsgNo)
	// A legacy message already addressed to an old leader also gets a failure ACK.
	options.G.Cluster.NodeId = 3
	u.events = nil
	h.onForwardUserEvent(&proto.Message{Content: body})
	require.Len(t, u.events, 1)
}

func TestForwardReuseRequiresTheCompleteAuthenticatedDescriptor(t *testing.T) {
	cached := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7}
	incoming := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, Auth: true, AesKey: []byte("new-key")}
	require.False(t, sameForwardedConn(cached, incoming))
	data, err := incoming.Encode()
	require.NoError(t, err)
	require.NoError(t, cached.Decode(data))
	require.True(t, sameForwardedConn(cached, incoming))
	incoming.AesKey = []byte("changed-key")
	require.False(t, sameForwardedConn(cached, incoming))
}

func TestAmbiguousUnkeyedSendWaitsForPeerOrClientTimeout(t *testing.T) {
	oldOptions, oldCluster, oldUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = oldOptions, oldCluster, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = &forwardCluster{leader: 2, supportV1: true}
	u := &forwardUser{}
	eventbus.RegisterUser(u)
	h := NewHandler()
	e := &eventbus.Event{Type: eventbus.EventOnSend, Conn: &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7}, Frame: &wkproto.SendPacket{ClientSeq: 42}}

	h.OnEvent(&eventbus.UserContext{Uid: "u", EventType: eventbus.EventOnSend, Events: []*eventbus.Event{e}})

	require.Empty(t, u.events)
}

func TestForwardConcurrencyGateFailsFastBeforeNetworkWait(t *testing.T) {
	oldOptions, oldCluster, oldUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = oldOptions, oldCluster, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	c := &forwardCluster{leader: 2, supportV1: true}
	service.Cluster = c
	u := &forwardUser{}
	eventbus.RegisterUser(u)
	h := NewHandler()
	h.forwardGate = forward.NewGate(4)
	require.True(t, h.forwardGate.TryAcquire())
	defer h.forwardGate.Release()
	e := &eventbus.Event{Type: eventbus.EventOnSend, Conn: &eventbus.Conn{Uid: "u", NodeId: 1}, Frame: &wkproto.SendPacket{ClientSeq: 42, ClientMsgNo: "stable"}}

	start := time.Now()
	h.OnEvent(&eventbus.UserContext{Uid: "u", EventType: eventbus.EventOnSend, Events: []*eventbus.Event{e}})

	require.Less(t, time.Since(start), 100*time.Millisecond)
	require.Zero(t, c.calls)
	require.Len(t, u.events, 1)
	require.Equal(t, wkproto.ReasonNodeNotMatch, u.events[0].Frame.(*wkproto.SendackPacket).ReasonCode)
}

func TestForwardConcurrencyGatePreservesNonSendInTransportQueue(t *testing.T) {
	oldOptions, oldCluster := options.G, service.Cluster
	t.Cleanup(func() { options.G, service.Cluster = oldOptions, oldCluster })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	c := &forwardCluster{leader: 2}
	service.Cluster = c
	h := NewHandler()
	h.forwardGate = forward.NewGate(4)
	require.True(t, h.forwardGate.TryAcquire())
	defer h.forwardGate.Release()
	e := &eventbus.Event{Type: eventbus.EventConnWriteFrame, Conn: &eventbus.Conn{Uid: "u", NodeId: 2}, Frame: &wkproto.PongPacket{}}

	h.forwardsToNode(2, "u", []*eventbus.Event{e})

	require.Zero(t, c.calls)
	require.Equal(t, 1, c.sendCalls)
}

func TestWriteFrameBatchesRemoteEventsByDestination(t *testing.T) {
	oldOptions, oldCluster := options.G, service.Cluster
	t.Cleanup(func() { options.G, service.Cluster = oldOptions, oldCluster })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	c := &forwardCluster{leader: 2}
	service.Cluster = c
	h := NewHandler()
	conn := &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7}

	h.writeFrame(&eventbus.UserContext{Events: []*eventbus.Event{
		{Type: eventbus.EventConnWriteFrame, Conn: conn, Frame: &wkproto.PongPacket{}},
		{Type: eventbus.EventConnWriteFrame, Conn: conn, Frame: &wkproto.PongPacket{}},
	}})

	require.Equal(t, 1, c.sendCalls)
}

func TestForwardAdmissionValidatesBatchAndRefreshesCachedActivity(t *testing.T) {
	oldOptions, oldCluster, oldUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = oldOptions, oldCluster, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 3
	service.Cluster = &forwardCluster{leader: 3}
	cached := &eventbus.Conn{Uid: "u", NodeId: 3, ConnId: 7}
	u := &forwardUser{cached: cached}
	eventbus.RegisterUser(u)
	h := NewHandler()

	incoming := &eventbus.Conn{Uid: "u", NodeId: 3, ConnId: 7, Auth: true}
	e := &eventbus.Event{Type: eventbus.EventConnWriteFrame, Conn: incoming, Frame: &wkproto.PongPacket{}}
	req := &forwardUserEventReq{uid: "u", events: eventbus.EventBatch{e}}
	body, err := req.encode()
	require.NoError(t, err)
	data, _, err := forward.Envelope(body, req.events)
	require.NoError(t, err)
	require.Equal(t, proto.StatusOK, h.acceptForward(data))
	require.Equal(t, 1, u.touches)
	require.NotSame(t, cached, u.events[0].Conn)
	require.True(t, u.events[0].Conn.Auth)

	bad := &eventbus.Event{Type: eventbus.EventOnSend, Conn: incoming}
	req.events = eventbus.EventBatch{bad}
	body, err = req.encode()
	require.NoError(t, err)
	data, _, err = forward.Envelope(body, req.events)
	require.NoError(t, err)
	require.Equal(t, forward.StatusInvalid, h.acceptForward(data))

	mixed := &eventbus.Event{Type: eventbus.EventConnRemove, Conn: incoming}
	req.events = eventbus.EventBatch{e, mixed}
	body, err = req.encode()
	require.NoError(t, err)
	data, _, err = forward.Envelope(body, req.events)
	require.NoError(t, err)
	require.Equal(t, forward.StatusInvalid, h.acceptForward(data))
}
