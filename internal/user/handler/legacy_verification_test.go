package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/presence"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestColdLegacySessionIsUnknownAndDoesNotPoisonVerificationCache(t *testing.T) {
	oldOptions, oldPresence, oldCluster, oldUser := options.G, service.Presence, service.Cluster, eventbus.User
	t.Cleanup(func() {
		options.G, service.Presence, service.Cluster, eventbus.User = oldOptions, oldPresence, oldCluster, oldUser
		NewHandler()
	})
	options.G = options.New()
	service.Presence = presence.New(3)
	service.Cluster = nil // No owner proof exists for an identity-less claim.
	users := &handlerUsers{}
	eventbus.RegisterUser(users)
	h := NewHandler()
	conn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: wkproto.APP, Uptime: 1_700_000_000, Auth: true}
	event := &eventbus.Event{Type: eventbus.EventOnSend, Conn: conn, Frame: &wkproto.SendPacket{ClientSeq: 1, ClientMsgNo: "legacy"}}

	for i := 0; i < 2; i++ {
		require.Empty(t, h.verifyOnSendEvents(conn.Uid, []*eventbus.Event{event}))
	}
	require.Len(t, users.events, 2)
	for _, event := range users.events {
		ack, ok := event.Frame.(*wkproto.SendackPacket)
		require.True(t, ok)
		require.Equal(t, wkproto.ReasonSystemError, ack.ReasonCode)
	}
	require.Empty(t, h.verificationFailures.entries)
	require.Empty(t, users.updated)

	// The next matching authoritative connection publication must work
	// immediately, without waiting for a spurious stale-session cache entry.
	users.known = conn
	require.Len(t, h.verifyOnSendEvents(conn.Uid, []*eventbus.Event{event}), 1)
}

func TestLegacyForwardDoesNotReplaceAStaleSocketBirth(t *testing.T) {
	oldOptions, oldCluster, oldUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = oldOptions, oldCluster, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = &handlerCluster{}
	current := &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, Uptime: 102, Auth: true}
	stale := &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, Uptime: 101, Auth: true}
	users := &handlerUsers{known: current}
	eventbus.RegisterUser(users)
	body, err := (&forwardUserEventReq{uid: "u", fromNode: 2, events: eventbus.EventBatch{
		{Type: eventbus.EventOnSend, Conn: stale, Frame: &wkproto.SendPacket{ClientSeq: 1}},
		{Type: eventbus.EventOnSend, Conn: current, Frame: &wkproto.SendPacket{ClientSeq: 2}},
	}}).encode()
	require.NoError(t, err)
	NewHandler().onForwardUserEvent(&proto.Message{Content: body})
	require.Len(t, users.events, 2)
	require.Equal(t, stale.Uptime, users.events[0].Conn.Uptime)
	require.NotSame(t, current, users.events[0].Conn)
	require.Same(t, current, users.events[1].Conn)
}
