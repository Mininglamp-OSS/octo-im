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
	leader uint64
	calls  int
}

func (c *forwardCluster) GetSlotId(string) uint32    { return 19 }
func (c *forwardCluster) SlotLeaderId(uint32) uint64 { return c.leader }
func (c *forwardCluster) RequestWithContext(context.Context, uint64, string, []byte) (*proto.Response, error) {
	c.calls++
	return nil, errors.New("connection unavailable")
}

type forwardUser struct {
	eventbus.IUser
	events []*eventbus.Event
}

func (u *forwardUser) AddEvent(_ string, e *eventbus.Event)          { u.events = append(u.events, e) }
func (u *forwardUser) Advance(string)                                {}
func (u *forwardUser) ConnById(string, uint64, int64) *eventbus.Conn { return nil }

func TestForwardRejectsFormerLeaderAndReportsFailure(t *testing.T) {
	oldOptions, oldCluster, oldUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = oldOptions, oldCluster, oldUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 3
	c := &forwardCluster{leader: 4}
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
