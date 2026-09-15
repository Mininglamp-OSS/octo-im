package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/presence"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type verificationPresence struct {
	service.IPresence
	verify func(*eventbus.Conn) (*eventbus.Conn, error)
}

func (p *verificationPresence) Verify(_ context.Context, conn *eventbus.Conn) (*eventbus.Conn, error) {
	return p.verify(conn)
}

type handlerUsers struct {
	eventbus.IUser
	known   *eventbus.Conn
	events  []*eventbus.Event
	updated []*eventbus.Conn
	advance int
}

func (u *handlerUsers) ConnById(string, uint64, int64) *eventbus.Conn { return u.known }
func (u *handlerUsers) AddEvent(_ string, event *eventbus.Event) {
	u.events = append(u.events, event)
}
func (u *handlerUsers) UpdateConn(conn *eventbus.Conn) { u.updated = append(u.updated, conn) }
func (u *handlerUsers) Advance(string)                 { u.advance++ }

type handlerCluster struct{ icluster.ICluster }

func (c *handlerCluster) GetSlotId(string) uint32    { return 1 }
func (c *handlerCluster) SlotLeaderId(uint32) uint64 { return 1 }

func TestVerifyOnSendRejectsOnlyTheInvalidEvent(t *testing.T) {
	oldOptions, oldPresence, oldCluster, oldUser := options.G, service.Presence, service.Cluster, eventbus.User
	t.Cleanup(func() {
		options.G, service.Presence, service.Cluster, eventbus.User = oldOptions, oldPresence, oldCluster, oldUser
		NewHandler()
	})
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = &handlerCluster{}
	good := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 1, Auth: true, OwnerBootID: "boot", SessionID: "good"}
	bad := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 2, Auth: true, OwnerBootID: "boot", SessionID: "bad"}
	users := &handlerUsers{known: good}
	eventbus.RegisterUser(users)
	service.Presence = &verificationPresence{verify: func(*eventbus.Conn) (*eventbus.Conn, error) {
		return nil, errors.New("stale session")
	}}
	h := NewHandler()
	var executed []*eventbus.Event
	eventbus.RegisterUserHandlers(eventbus.EventOnSend, func(ctx *eventbus.UserContext) {
		executed = append(executed, ctx.Events...)
	})
	events := []*eventbus.Event{
		{Type: eventbus.EventOnSend, Conn: bad, Frame: &wkproto.SendPacket{Framer: wkproto.Framer{NoPersist: true}, ClientSeq: 4, ClientMsgNo: "bad"}, MessageId: 88},
		{Type: eventbus.EventOnSend, Conn: good, Frame: &wkproto.PingPacket{}},
		{Type: eventbus.EventOnSend, Conn: good, Frame: &wkproto.RecvackPacket{MessageID: 9}},
		{Type: eventbus.EventOnSend, Conn: nil, Frame: &wkproto.PingPacket{}},
	}

	h.OnEvent(&eventbus.UserContext{Uid: "u", EventType: eventbus.EventOnSend, Events: events})
	require.Len(t, executed, 2)
	require.IsType(t, &wkproto.PingPacket{}, executed[0].Frame)
	require.IsType(t, &wkproto.RecvackPacket{}, executed[1].Frame)
	require.Len(t, users.events, 1)
	ack, ok := users.events[0].Frame.(*wkproto.SendackPacket)
	require.True(t, ok)
	require.Equal(t, int64(88), ack.MessageID)
	require.Equal(t, uint64(4), ack.ClientSeq)
	require.Equal(t, "bad", ack.ClientMsgNo)
	require.True(t, ack.NoPersist)
	require.Equal(t, wkproto.ReasonNodeNotMatch, ack.ReasonCode)
	require.Equal(t, 1, users.advance)
}

type handlerSocket struct {
	wknet.Conn
	id      int64
	ctx     interface{}
	maxIdle time.Duration
}

func (c *handlerSocket) ID() int64                { return c.id }
func (c *handlerSocket) SetContext(v interface{}) { c.ctx = v }
func (c *handlerSocket) Context() interface{}     { return c.ctx }
func (c *handlerSocket) SetMaxIdle(v time.Duration) {
	c.maxIdle = v
}

type handlerConnManager struct {
	service.IConnManager
	conn wknet.Conn
}

func (m *handlerConnManager) GetConn(int64) wknet.Conn { return m.conn }

func TestConnackBindsLegacyDescriptorToPreparedSocket(t *testing.T) {
	oldOptions, oldPresence, oldManager, oldUser := options.G, service.Presence, service.ConnManager, eventbus.User
	t.Cleanup(func() {
		options.G, service.Presence, service.ConnManager, eventbus.User = oldOptions, oldPresence, oldManager, oldUser
	})
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	manager := presence.New(1)
	raw := &handlerSocket{id: 7}
	manager.Track(raw)
	prepared := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: wkproto.APP, Uptime: 11}
	manager.Prepare(raw, prepared)
	legacy := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: wkproto.APP, Uptime: 11, Auth: true, AesIV: []byte("iv"), AesKey: []byte("key")}
	service.Presence = manager
	service.ConnManager = &handlerConnManager{conn: raw}
	users := &handlerUsers{}
	eventbus.RegisterUser(users)

	NewHandler().connack(&eventbus.UserContext{Uid: "u", Events: []*eventbus.Event{{Conn: legacy, Frame: &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonSuccess}}}})
	require.True(t, legacy.HasSessionIdentity())
	require.Same(t, legacy, raw.Context())
	require.Equal(t, options.G.ConnIdleTime, raw.maxIdle)
	require.Len(t, users.updated, 1)
	require.Len(t, users.events, 1)
	require.Equal(t, wkproto.ReasonSuccess, users.events[0].Frame.(*wkproto.ConnackPacket).ReasonCode)
}

func TestConnackReturnsAuthFailureForSessionMismatch(t *testing.T) {
	oldOptions, oldPresence, oldUser := options.G, service.Presence, eventbus.User
	t.Cleanup(func() { options.G, service.Presence, eventbus.User = oldOptions, oldPresence, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	manager := presence.New(1)
	raw := &handlerSocket{id: 7}
	manager.Track(raw)
	prepared := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: wkproto.APP, Uptime: 11}
	manager.Prepare(raw, prepared)
	service.Presence = manager
	users := &handlerUsers{}
	eventbus.RegisterUser(users)
	stale := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: wkproto.APP, Uptime: 11, Auth: true, OwnerBootID: prepared.OwnerBootID, SessionID: "stale"}

	NewHandler().connack(&eventbus.UserContext{Uid: "u", Events: []*eventbus.Event{{Conn: stale, Frame: &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonSuccess}}}})
	require.Empty(t, users.updated)
	require.Len(t, users.events, 1)
	require.Equal(t, wkproto.ReasonAuthFail, users.events[0].Frame.(*wkproto.ConnackPacket).ReasonCode)
	require.True(t, users.events[0].Conn.SameSession(prepared))
	require.Equal(t, uint64(1), manager.RejectedCount())
}

func TestFinalizeRecvPacketUsesOnlyPhysicalSessionCrypto(t *testing.T) {
	packet := &wkproto.RecvPacket{Payload: []byte("plain"), MessageID: 7, MessageSeq: 3}
	conn := &eventbus.Conn{AesKey: []byte("0123456789abcdef"), AesIV: []byte("abcdef0123456789")}
	finalized, err := finalizeRecvPacket(packet, conn)
	require.NoError(t, err)
	require.Equal(t, []byte("plain"), packet.Payload)
	require.NotEqual(t, packet.Payload, finalized.Payload)
	require.NotEmpty(t, finalized.MsgKey)
}
