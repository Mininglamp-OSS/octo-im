package handler

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type heartbeatCluster struct {
	forwardCluster
	targets  []uint64
	messages []*proto.Message
}

func (c *heartbeatCluster) Send(node uint64, message *proto.Message) error {
	c.targets = append(c.targets, node)
	c.messages = append(c.messages, message)
	// A disconnected node's transport accepts and retains this message.
	return nil
}

type heartbeatConnManager struct {
	service.IConnManager
	socket *handlerSocket
}

func (m *heartbeatConnManager) GetConn(id int64) wknet.Conn {
	if m.socket != nil && m.socket.id == id {
		return m.socket
	}
	return nil
}

func heartbeatFixture(t *testing.T) (*Handler, *heartbeatCluster, *forwardUser, *eventbus.Conn, *handlerSocket) {
	t.Helper()
	oldOptions, oldCluster, oldUser := options.G, service.Cluster, eventbus.User
	oldManager, oldPresence, oldTrace := service.ConnManager, service.Presence, trace.GlobalTrace
	t.Cleanup(func() {
		options.G, service.Cluster, eventbus.User = oldOptions, oldCluster, oldUser
		service.ConnManager, service.Presence = oldManager, oldPresence
		trace.SetGlobalTrace(oldTrace)
		NewHandler()
	})
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	cluster := &heartbeatCluster{forwardCluster: forwardCluster{leader: 2}}
	service.Cluster = cluster
	service.Presence = nil
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	conn := &eventbus.Conn{
		Uid: "heartbeat-user", NodeId: 1, ConnId: 7, Auth: true,
		DeviceId: "web", DeviceFlag: wkproto.APP, Uptime: 100,
		OwnerBootID: "boot", SessionID: "live", IsJsonRpc: true,
	}
	raw := &handlerSocket{id: conn.ConnId, ctx: conn}
	service.ConnManager = &heartbeatConnManager{socket: raw}
	users := &forwardUser{cached: conn}
	eventbus.RegisterUser(users)
	return NewHandler(), cluster, users, conn, raw
}

func TestLocalHeartbeatSurvivesUnavailableUserLeader(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		leader uint64
	}{{"old leader", 2}, {"no leader", 0}, {"new leader", 3}} {
		t.Run(scenario.name, func(t *testing.T) {
			h, cluster, users, conn, _ := heartbeatFixture(t)
			cluster.leader = scenario.leader
			event := &eventbus.Event{
				Type: eventbus.EventOnSend, SourceNodeId: 1,
				Conn: conn, Frame: &wkproto.PingPacket{}, ReqId: "pending-heartbeat",
			}

			h.OnEvent(&eventbus.UserContext{Uid: conn.Uid, EventType: eventbus.EventOnSend, Events: []*eventbus.Event{event}})

			require.Len(t, users.events, 1, "a live physical socket must receive PONG without waiting for user-slot authority")
			require.IsType(t, &wkproto.PongPacket{}, users.events[0].Frame)
			require.Equal(t, "pending-heartbeat", users.events[0].ReqId)
			require.Same(t, conn, users.events[0].Conn)
			require.Empty(t, cluster.targets, "the heartbeat must not enter a disconnected peer's retained queue")
			require.Zero(t, cluster.calls)
		})
	}
}

func TestLocalHeartbeatPreservesOtherEventsInMixedBatch(t *testing.T) {
	h, cluster, users, conn, _ := heartbeatFixture(t)
	send := &eventbus.Event{Type: eventbus.EventOnSend, SourceNodeId: 1, Conn: conn,
		Frame: &wkproto.SendPacket{ClientSeq: 3, ClientMsgNo: "do-not-replay"}}
	ping := &eventbus.Event{Type: eventbus.EventOnSend, SourceNodeId: 1, Conn: conn,
		Frame: &wkproto.PingPacket{}, ReqId: "heartbeat"}
	recvack := &eventbus.Event{Type: eventbus.EventOnSend, SourceNodeId: 1, Conn: conn,
		Frame: &wkproto.RecvackPacket{MessageID: 9}}

	h.OnEvent(&eventbus.UserContext{Uid: conn.Uid, EventType: eventbus.EventOnSend,
		Events: []*eventbus.Event{send, ping, recvack}})

	require.Len(t, users.events, 1)
	require.IsType(t, &wkproto.PongPacket{}, users.events[0].Frame)
	require.Equal(t, []uint64{2}, cluster.targets)
	req := &forwardUserEventReq{}
	require.NoError(t, req.decode(cluster.messages[0].Content))
	require.Len(t, req.events, 2)
	require.Equal(t, "do-not-replay", req.events[0].Frame.(*wkproto.SendPacket).ClientMsgNo)
	require.Equal(t, int64(9), req.events[1].Frame.(*wkproto.RecvackPacket).MessageID)
}

func TestLocalHeartbeatRejectsStaleOrUnauthenticatedSocket(t *testing.T) {
	for _, scenario := range []string{"reused ID", "closed", "physical unauthenticated", "claim unauthenticated", "different UID"} {
		t.Run(scenario, func(t *testing.T) {
			h, cluster, users, physical, raw := heartbeatFixture(t)
			data, err := physical.Encode()
			require.NoError(t, err)
			claim := &eventbus.Conn{}
			require.NoError(t, claim.Decode(data))
			switch scenario {
			case "reused ID":
				claim.SessionID = "previous-session"
			case "closed":
				service.ConnManager = &heartbeatConnManager{}
			case "physical unauthenticated":
				physical.Auth = false
			case "claim unauthenticated":
				claim.Auth = false
			case "different UID":
				claim.Uid = "another-user"
			}
			event := &eventbus.Event{Type: eventbus.EventOnSend, SourceNodeId: 1,
				Conn: claim, Frame: &wkproto.PingPacket{}, ReqId: "stale-heartbeat"}

			h.OnEvent(&eventbus.UserContext{Uid: claim.Uid, EventType: eventbus.EventOnSend, Events: []*eventbus.Event{event}})

			require.Empty(t, users.events, "a stale or unauthenticated claim must not receive a local PONG")
			require.Empty(t, cluster.targets, "invalid local PING must not be forwarded for later replay")
			require.Same(t, physical, raw.Context())
		})
	}
}

func TestForwardedHeartbeatRetainsExistingRouting(t *testing.T) {
	h, cluster, users, _, _ := heartbeatFixture(t)
	remote := &eventbus.Conn{Uid: "remote-user", NodeId: 3, ConnId: 8, Auth: true}
	event := &eventbus.Event{Type: eventbus.EventOnSend, SourceNodeId: 3,
		Conn: remote, Frame: &wkproto.PingPacket{}}

	h.OnEvent(&eventbus.UserContext{Uid: remote.Uid, EventType: eventbus.EventOnSend, Events: []*eventbus.Event{event}})

	require.Empty(t, users.events)
	require.Equal(t, []uint64{2}, cluster.targets)
}

func TestLocalHeartbeatSupportsBinaryAndLegacySessions(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "session identity"
		if legacy {
			name = "legacy identity"
		}
		t.Run(name, func(t *testing.T) {
			h, cluster, users, conn, _ := heartbeatFixture(t)
			conn.IsJsonRpc = false
			if legacy {
				conn.OwnerBootID, conn.SessionID = "", ""
			}
			event := &eventbus.Event{Type: eventbus.EventOnSend, SourceNodeId: 1,
				Conn: conn, Frame: &wkproto.PingPacket{}}

			h.OnEvent(&eventbus.UserContext{Uid: conn.Uid, EventType: eventbus.EventOnSend, Events: []*eventbus.Event{event}})

			require.Len(t, users.events, 1)
			require.IsType(t, &wkproto.PongPacket{}, users.events[0].Frame)
			require.False(t, users.events[0].Conn.IsJsonRpc)
			require.Empty(t, cluster.targets)
		})
	}
}

func TestLocalHeartbeatUsesCurrentPhysicalDescriptor(t *testing.T) {
	h, cluster, users, physical, _ := heartbeatFixture(t)
	data, err := physical.Encode()
	require.NoError(t, err)
	claim := &eventbus.Conn{}
	require.NoError(t, claim.Decode(data))
	claim.IsJsonRpc = false
	event := &eventbus.Event{Type: eventbus.EventOnSend, SourceNodeId: 1,
		Conn: claim, Frame: &wkproto.PingPacket{}, ReqId: "physical-protocol"}

	h.OnEvent(&eventbus.UserContext{Uid: claim.Uid, EventType: eventbus.EventOnSend, Events: []*eventbus.Event{event}})

	require.Len(t, users.events, 1)
	require.Same(t, physical, users.events[0].Conn)
	require.True(t, users.events[0].Conn.IsJsonRpc)
	require.Equal(t, "physical-protocol", users.events[0].ReqId)
	require.Empty(t, cluster.targets)
}

func TestUserLeaderStillAnswersHeartbeatForwardedByOlderOwner(t *testing.T) {
	h, cluster, users, _, _ := heartbeatFixture(t)
	cluster.leader = 1
	remote := &eventbus.Conn{Uid: "remote-user", NodeId: 3, ConnId: 8, Auth: true}
	event := &eventbus.Event{Type: eventbus.EventOnSend, SourceNodeId: 3,
		Conn: remote, Frame: &wkproto.PingPacket{}, ReqId: "older-owner"}

	h.OnEvent(&eventbus.UserContext{Uid: remote.Uid, EventType: eventbus.EventOnSend, Events: []*eventbus.Event{event}})

	require.Len(t, users.events, 1)
	require.Equal(t, uint64(3), users.events[0].Conn.NodeId)
	require.Equal(t, "older-owner", users.events[0].ReqId)
	require.IsType(t, &wkproto.PongPacket{}, users.events[0].Frame)
	require.Empty(t, cluster.targets)
}
