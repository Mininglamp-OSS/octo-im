package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type failingRecovery struct {
	service.IPresence
	uids            []string
	deliveredBefore int
	pusher          *distributionPusher
}

func (f *failingRecovery) Recover(_ context.Context, uids []string) error {
	f.uids = append([]string(nil), uids...)
	if f.pusher != nil {
		f.deliveredBefore = len(f.pusher.events)
	}
	return errors.New("owner unavailable")
}
func (f *failingRecovery) IsReady(string) bool { return false }

type distributionUsers struct {
	eventbus.IUser
	conns map[string][]*eventbus.Conn
}

func (u *distributionUsers) AuthedConnsByUid(uid string) []*eventbus.Conn {
	return append([]*eventbus.Conn(nil), u.conns[uid]...)
}

type distributionPusher struct {
	eventbus.IPusher
	events []*eventbus.Event
}

func (p *distributionPusher) RandHandlerId() int { return 1 }
func (p *distributionPusher) AddEvent(_ int, event *eventbus.Event) {
	p.events = append(p.events, event)
}
func (p *distributionPusher) Advance(int) {}

func TestDistributionKeepsKnownOnlineDeliveryWhenRecoveryFails(t *testing.T) {
	oldOptions, oldPresence, oldUser, oldPusher := options.G, service.Presence, eventbus.User, eventbus.Pusher
	t.Cleanup(func() {
		options.G, service.Presence, eventbus.User, eventbus.Pusher = oldOptions, oldPresence, oldUser, oldPusher
	})
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	users := &distributionUsers{conns: map[string][]*eventbus.Conn{
		"known": {{Uid: "known", NodeId: 1, ConnId: 7, Auth: true, DeviceLevel: wkproto.DeviceLevelMaster}},
	}}
	eventbus.RegisterUser(users)
	pusher := &distributionPusher{}
	eventbus.RegisterPusher(pusher)
	recovery := &failingRecovery{pusher: pusher}
	service.Presence = recovery

	h := NewHandler()
	h.distributeByTag(1, &types.Tag{Key: "tag", Nodes: []*types.Node{{LeaderId: 1, Uids: []string{"known", "unknown"}}}}, "channel", wkproto.ChannelTypeData, []*eventbus.Event{{
		Type:  eventbus.EventChannelDistribute,
		Conn:  &eventbus.Conn{Uid: "sender", NodeId: 1, ConnId: 9},
		Frame: &wkproto.SendPacket{},
	}})

	require.Len(t, pusher.events, 2)
	require.Equal(t, eventbus.EventPushOnline, pusher.events[0].Type)
	require.Equal(t, "known", pusher.events[0].ToUid)
	require.Equal(t, eventbus.EventPushOffline, pusher.events[1].Type)
	require.Equal(t, []string{"unknown"}, pusher.events[1].OfflineUsers)
	require.Equal(t, []string{"unknown"}, recovery.uids)
	require.Equal(t, 1, recovery.deliveredBefore)
}
