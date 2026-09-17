package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestForwardAdmissionPreservesSessionVerification(t *testing.T) {
	oldOptions, oldPresence, oldCluster, oldUser := options.G, service.Presence, service.Cluster, eventbus.User
	t.Cleanup(func() {
		options.G, service.Presence, service.Cluster, eventbus.User = oldOptions, oldPresence, oldCluster, oldUser
		NewHandler()
	})
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = &forwardCluster{leader: 1}
	current := &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "current"}
	stale := &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "stale"}
	users := &handlerUsers{known: current}
	eventbus.RegisterUser(users)
	verifyCalls := 0
	service.Presence = &verificationPresence{verify: func(conn *eventbus.Conn) (*eventbus.Conn, error) {
		verifyCalls++
		require.Equal(t, "stale", conn.SessionID)
		return nil, service.ErrPresenceSessionNotFound
	}}
	h := NewHandler()
	require.NotNil(t, h.forwardGate)
	var executed []*eventbus.Event
	eventbus.RegisterUserHandlers(eventbus.EventOnSend, func(ctx *eventbus.UserContext) {
		executed = append(executed, ctx.Events...)
	})
	events := eventbus.EventBatch{
		{Type: eventbus.EventOnSend, Conn: current, Frame: &wkproto.SendPacket{ClientSeq: 1, ClientMsgNo: "current"}},
		{Type: eventbus.EventOnSend, Conn: stale, Frame: &wkproto.SendPacket{ClientSeq: 2, ClientMsgNo: "stale"}},
	}
	body, err := (&forwardUserEventReq{uid: "u", fromNode: 2, events: events}).encode()
	require.NoError(t, err)
	envelope, _, err := forward.Envelope(body, events)
	require.NoError(t, err)

	// PR48 admits the whole v1 batch; PR49 must still verify each session
	// before execution, even when a stale session reuses the current socket ID.
	require.Equal(t, proto.StatusOK, h.acceptForward(envelope))
	require.Len(t, users.events, 2)
	admitted := users.events
	users.events = nil
	h.OnEvent(&eventbus.UserContext{Uid: "u", EventType: eventbus.EventOnSend, Events: admitted})

	require.Len(t, executed, 1)
	require.Same(t, current, executed[0].Conn)
	require.Equal(t, "current", executed[0].Frame.(*wkproto.SendPacket).ClientMsgNo)
	require.Positive(t, executed[0].ForwardDeadline)
	require.Equal(t, uint8(1), executed[0].ForwardHops)
	require.Equal(t, 1, verifyCalls)
	require.Len(t, users.events, 1)
	ack, ok := users.events[0].Frame.(*wkproto.SendackPacket)
	require.True(t, ok)
	require.Equal(t, "stale", ack.ClientMsgNo)
	require.Equal(t, wkproto.ReasonNodeNotMatch, ack.ReasonCode)
	require.Empty(t, users.updated)
}
