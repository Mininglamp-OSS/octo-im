package handler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/presence"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	serverproto "github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type verificationPresence struct {
	service.IPresence
	verify        func(*eventbus.Conn) (*eventbus.Conn, error)
	verifyContext func(context.Context, *eventbus.Conn) (*eventbus.Conn, error)
}

func (p *verificationPresence) Verify(ctx context.Context, conn *eventbus.Conn) (*eventbus.Conn, error) {
	if p.verifyContext != nil {
		return p.verifyContext(ctx, conn)
	}
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
	verifyCalls := 0
	service.Presence = &verificationPresence{verify: func(*eventbus.Conn) (*eventbus.Conn, error) {
		verifyCalls++
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
		{Type: eventbus.EventOnSend, Conn: bad, Frame: &wkproto.PingPacket{}},
		{Type: eventbus.EventOnSend, Conn: bad, Frame: &wkproto.RecvackPacket{MessageID: 10}},
		{Type: eventbus.EventOnSend, Conn: nil, Frame: &wkproto.PingPacket{}},
	}

	h.OnEvent(&eventbus.UserContext{Uid: "u", EventType: eventbus.EventOnSend, Events: events})
	require.Len(t, executed, 4)
	require.IsType(t, &wkproto.PingPacket{}, executed[0].Frame)
	require.IsType(t, &wkproto.RecvackPacket{}, executed[1].Frame)
	require.IsType(t, &wkproto.PingPacket{}, executed[2].Frame)
	require.IsType(t, &wkproto.RecvackPacket{}, executed[3].Frame)
	require.Len(t, users.events, 1)
	ack, ok := users.events[0].Frame.(*wkproto.SendackPacket)
	require.True(t, ok)
	require.Equal(t, int64(88), ack.MessageID)
	require.Equal(t, uint64(4), ack.ClientSeq)
	require.Equal(t, "bad", ack.ClientMsgNo)
	require.True(t, ack.NoPersist)
	require.Equal(t, wkproto.ReasonNodeNotMatch, ack.ReasonCode)
	require.Equal(t, 1, users.advance)
	require.Equal(t, 1, verifyCalls)
}

func TestVerifyOnSendCachesFailuresAcrossBatchesBySession(t *testing.T) {
	oldOptions, oldPresence, oldUser := options.G, service.Presence, eventbus.User
	t.Cleanup(func() { options.G, service.Presence, eventbus.User = oldOptions, oldPresence, oldUser })
	options.G = options.New()
	users := &handlerUsers{}
	eventbus.RegisterUser(users)
	verifyCalls := 0
	service.Presence = &verificationPresence{verify: func(*eventbus.Conn) (*eventbus.Conn, error) {
		verifyCalls++
		return nil, errors.New("owner unavailable")
	}}
	h := NewHandler()
	event := func(seq uint64, session string) *eventbus.Event {
		return &eventbus.Event{
			Type:  eventbus.EventOnSend,
			Conn:  &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, OwnerBootID: "boot", SessionID: session},
			Frame: &wkproto.SendPacket{ClientSeq: seq},
		}
	}

	h.verifyOnSendEvents("u", []*eventbus.Event{event(1, "session")})
	h.verifyOnSendEvents("u", []*eventbus.Event{event(2, "session")})
	h.verifyOnSendEvents("u", []*eventbus.Event{event(3, "replacement")})

	require.Equal(t, 2, verifyCalls)
	require.Len(t, users.events, 3)
}

func TestVerifyOnSendHasOneBatchDeadlineForDistinctSessions(t *testing.T) {
	oldOptions, oldPresence, oldUser := options.G, service.Presence, eventbus.User
	t.Cleanup(func() { options.G, service.Presence, eventbus.User = oldOptions, oldPresence, oldUser })
	options.G = options.New()
	users := &handlerUsers{}
	eventbus.RegisterUser(users)
	verifyCalls := 0
	service.Presence = &verificationPresence{verifyContext: func(ctx context.Context, _ *eventbus.Conn) (*eventbus.Conn, error) {
		verifyCalls++
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	h := NewHandler()
	events := make([]*eventbus.Event, 0, 32)
	for i := 0; i < 32; i++ {
		events = append(events, &eventbus.Event{
			Type:  eventbus.EventOnSend,
			Conn:  &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: int64(i + 1), OwnerBootID: "boot", SessionID: fmt.Sprintf("session-%d", i)},
			Frame: &wkproto.SendPacket{ClientSeq: uint64(i + 1)},
		})
	}

	start := time.Now()
	verified := h.verifyOnSendEvents("u", events)

	require.Empty(t, verified)
	require.Equal(t, 1, verifyCalls)
	require.Len(t, users.events, len(events))
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestVerifyOnSendRestoresCryptoForRecoveredSession(t *testing.T) {
	oldOptions, oldPresence, oldUser := options.G, service.Presence, eventbus.User
	t.Cleanup(func() { options.G, service.Presence, eventbus.User = oldOptions, oldPresence, oldUser })
	options.G = options.New()
	known := &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "session"}
	claim := &eventbus.Conn{
		Uid: "u", NodeId: 2, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "session",
		AesKey: []byte("0123456789abcdef"), AesIV: []byte("abcdef0123456789"),
	}
	users := &handlerUsers{known: known}
	eventbus.RegisterUser(users)
	presence := &verificationPresence{verify: func(*eventbus.Conn) (*eventbus.Conn, error) {
		t.Fatal("matching recovered session must not require a snapshot RPC")
		return nil, nil
	}}
	service.Presence = presence
	packet := &wkproto.SendPacket{ClientSeq: 1, ClientMsgNo: "m", ChannelID: "c", ChannelType: 2}
	var err error
	packet.Payload, err = wkutil.AesEncryptPkcs7Base64([]byte("plain"), claim.AesKey, claim.AesIV)
	require.NoError(t, err)
	signature, err := wkutil.AesEncryptPkcs7Base64([]byte(packet.VerityString()), claim.AesKey, claim.AesIV)
	require.NoError(t, err)
	packet.MsgKey = wkutil.MD5Bytes(signature)

	events := NewHandler().verifyOnSendEvents("u", []*eventbus.Event{{Type: eventbus.EventOnSend, Conn: claim, Frame: packet}})

	require.Len(t, events, 1)
	require.Equal(t, claim.AesIV, events[0].Conn.AesIV)
	require.Equal(t, claim.AesKey, events[0].Conn.AesKey)
	require.Empty(t, users.updated)
	plain, err := NewHandler().decryptPayload(packet, events[0].Conn)
	require.NoError(t, err)
	require.Equal(t, []byte("plain"), plain)
}

func TestLegacyForwardKeepsCryptoOnRecoveredSessionEvent(t *testing.T) {
	oldOptions, oldPresence, oldCluster, oldUser := options.G, service.Presence, service.Cluster, eventbus.User
	t.Cleanup(func() {
		options.G, service.Presence, service.Cluster, eventbus.User = oldOptions, oldPresence, oldCluster, oldUser
	})
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = &handlerCluster{}
	service.Presence = &verificationPresence{}
	known := &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "session"}
	claim := &eventbus.Conn{
		Uid: "u", NodeId: 2, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "session",
		AesKey: []byte("0123456789abcdef"), AesIV: []byte("abcdef0123456789"),
	}
	users := &handlerUsers{known: known}
	eventbus.RegisterUser(users)
	request := &forwardUserEventReq{
		fromNode: 2,
		uid:      "u",
		events:   eventbus.EventBatch{{Type: eventbus.EventOnSend, Conn: claim, Frame: &wkproto.SendPacket{ClientSeq: 1}}},
	}
	data, err := request.encode()
	require.NoError(t, err)

	NewHandler().onForwardUserEvent(&serverproto.Message{MsgType: uint32(msgForwardUserEvent), Content: data})

	require.Len(t, users.events, 1)
	require.Equal(t, claim.AesIV, users.events[0].Conn.AesIV)
	require.Equal(t, claim.AesKey, users.events[0].Conn.AesKey)
	require.Empty(t, users.updated)
}

type recvackRetryManager struct {
	service.RetryMgr
	msg     *types.RetryMessage
	removed bool
}

func (m *recvackRetryManager) RetryMessage(uint64, int64, int64) *types.RetryMessage {
	return m.msg
}
func (m *recvackRetryManager) RemoveRetry(uint64, int64, int64) error {
	m.removed = true
	return nil
}

func TestRecvackCannotClearRetryFromReusedSession(t *testing.T) {
	oldRetry := service.RetryManager
	t.Cleanup(func() { service.RetryManager = oldRetry })
	retry := &recvackRetryManager{msg: &types.RetryMessage{
		Uid: "u", FromNode: 1, ConnId: 7, OwnerBootID: "old-boot", SessionID: "old-session",
	}}
	service.RetryManager = retry
	conn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, OwnerBootID: "new-boot", SessionID: "new-session"}

	NewHandler().recvack(&eventbus.Event{Conn: conn, Frame: &wkproto.RecvackPacket{MessageID: 9}})

	require.False(t, retry.removed)
}

type handlerSocket struct {
	wknet.Conn
	id      int64
	ctx     interface{}
	maxIdle time.Duration
	uptime  time.Time
}

func (c *handlerSocket) ID() int64                { return c.id }
func (c *handlerSocket) SetContext(v interface{}) { c.ctx = v }
func (c *handlerSocket) Context() interface{}     { return c.ctx }
func (c *handlerSocket) Uptime() time.Time {
	if c.uptime.IsZero() {
		return time.Unix(0, c.id)
	}
	return c.uptime
}
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
	legacy := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: wkproto.APP, Uptime: prepared.Uptime, Auth: true, AesIV: []byte("iv"), AesKey: []byte("key")}
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
	stale := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: wkproto.APP, Uptime: prepared.Uptime, Auth: true, OwnerBootID: prepared.OwnerBootID, SessionID: "stale"}

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
