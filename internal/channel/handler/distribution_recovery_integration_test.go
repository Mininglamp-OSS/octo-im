package handler_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	channelevent "github.com/WuKongIM/WuKongIM/internal/channel/event"
	"github.com/WuKongIM/WuKongIM/internal/channel/handler"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type recoveryCluster struct {
	icluster.ICluster
	requests atomic.Int32
}

func (*recoveryCluster) GetSlotId(string) uint32                             { return 1 }
func (*recoveryCluster) SlotLeaderId(uint32) uint64                          { return 2 }
func (*recoveryCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) { return 1, nil }
func (c *recoveryCluster) RequestWithContext(context.Context, uint64, string, []byte) (*proto.Response, error) {
	if c.requests.Add(1) == 1 {
		return nil, errors.New("temporary tag lookup failure")
	}
	e := wkproto.NewEncoder()
	e.WriteString("A")
	e.WriteUint32(1)
	e.WriteString("fixed")
	return &proto.Response{Status: proto.StatusOK, Body: e.Bytes()}, nil
}

type recoveryUsers struct{ eventbus.IUser }

func (recoveryUsers) AuthedConnsByUid(uid string) []*eventbus.Conn {
	return []*eventbus.Conn{{Uid: uid, NodeId: 2, ConnId: 1, Auth: true, DeviceLevel: wkproto.DeviceLevelMaster}}
}

type recoveryPusher struct {
	eventbus.IPusher
	delivered chan *eventbus.Event
}

func (*recoveryPusher) RandHandlerId() int                  { return 1 }
func (*recoveryPusher) Advance(int)                         {}
func (p *recoveryPusher) AddEvent(_ int, e *eventbus.Event) { p.delivered <- e }

func TestDistributionQueueRecoversTagLookupFailure(t *testing.T) {
	oldOptions, oldCluster, oldTags := options.G, service.Cluster, service.TagManager
	oldPresence, oldUsers, oldPusher, oldChannel := service.Presence, eventbus.User, eventbus.Pusher, eventbus.Channel
	t.Cleanup(func() {
		options.G, service.Cluster, service.TagManager = oldOptions, oldCluster, oldTags
		service.Presence, eventbus.User, eventbus.Pusher, eventbus.Channel = oldPresence, oldUsers, oldPusher, oldChannel
	})
	options.G = options.New()
	options.G.Cluster.NodeId = 2
	options.G.Poller.ChannelCount = 1
	options.G.Poller.ChannelGoroutine = 4
	options.G.Poller.IntervalTick = time.Millisecond
	c := &recoveryCluster{}
	service.Cluster = c
	service.TagManager = manager.NewTagManager(4, func() uint64 { return 0 })
	service.Presence = nil
	eventbus.RegisterUser(recoveryUsers{})
	pusher := &recoveryPusher{delivered: make(chan *eventbus.Event, 4)}
	eventbus.RegisterPusher(pusher)
	pool := channelevent.NewEventPool(handler.NewHandler())
	eventbus.RegisterChannel(pool)
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}
	defer pool.Stop()
	pool.AddEvent("channel", wkproto.ChannelTypeData, &eventbus.Event{
		Type: eventbus.EventChannelDistribute, MessageId: 101, TagKey: "A",
		Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{},
	})
	pool.Advance("channel", wkproto.ChannelTypeData)
	select {
	case got := <-pusher.delivered:
		if got.MessageId != 101 || got.ToUid != "fixed" {
			t.Fatalf("unexpected delivery: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("failed distribution disappeared from the channel queue; lookup attempts=%d", c.requests.Load())
	}
	if c.requests.Load() != 2 {
		t.Fatalf("lookup attempts=%d, want 2", c.requests.Load())
	}
}
