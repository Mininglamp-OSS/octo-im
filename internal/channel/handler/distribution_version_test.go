package handler

import (
	"reflect"
	"sort"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// versionTestCluster places every recipient on the receiving node without network IO.
type versionTestCluster struct{ icluster.ICluster }

func (versionTestCluster) GetSlotId(string) uint32    { return 1 }
func (versionTestCluster) SlotLeaderId(uint32) uint64 { return 2 }

// versionTestUsers exposes authenticated sessions for the three fixture recipients.
type versionTestUsers struct{ eventbus.IUser }

func (versionTestUsers) AuthedConnsByUid(uid string) []*eventbus.Conn {
	return []*eventbus.Conn{{Uid: uid, NodeId: 2, ConnId: 1, Auth: true, DeviceLevel: wkproto.DeviceLevelMaster}}
}

// versionTestPusher records the recipients selected by the real distribution handler.
type versionTestPusher struct {
	eventbus.IPusher
	events []*eventbus.Event // events contains the output of ordinary distribution.
}

func (*versionTestPusher) RandHandlerId() int { return 1 }
func (*versionTestPusher) Advance(int)        {}
func (p *versionTestPusher) AddEvent(_ int, e *eventbus.Event) {
	p.events = append(p.events, e)
}

func versionTestFixture(t *testing.T) (*manager.TagManager, *versionTestPusher) {
	t.Helper()
	oldOptions, oldCluster, oldTags := options.G, service.Cluster, service.TagManager
	oldPresence, oldUsers, oldPusher := service.Presence, eventbus.User, eventbus.Pusher
	t.Cleanup(func() {
		options.G, service.Cluster, service.TagManager = oldOptions, oldCluster, oldTags
		service.Presence, eventbus.User, eventbus.Pusher = oldPresence, oldUsers, oldPusher
	})
	options.G = options.New()
	options.G.Cluster.NodeId = 2
	service.Cluster = versionTestCluster{}
	service.Presence = nil
	m := manager.NewTagManager(4, func() uint64 { return 0 })
	service.TagManager = m
	if _, err := m.MakeTagWithTagKey("A", []string{"fixed", "removed"}); err != nil {
		t.Fatal(err)
	}
	m.SetChannelTag("channel", wkproto.ChannelTypeData, "A")
	eventbus.RegisterUser(versionTestUsers{})
	p := &versionTestPusher{}
	eventbus.RegisterPusher(p)
	return m, p
}

// This control covers the immutable in-process snapshot fixed by PR #73.
func TestDistributionHeldSnapshotSurvivesChange(t *testing.T) {
	m, _ := versionTestFixture(t)
	held := m.Get("A")
	if err := m.RemoveUsers("A", []string{"removed"}); err != nil {
		t.Fatal(err)
	}
	if got := held.GetUsers(); !reflect.DeepEqual(got, []string{"fixed", "removed"}) {
		t.Errorf("held snapshot changed: got %v", got)
	}
}

func versionTestDistributeBatch(t *testing.T, secondKey string) {
	t.Helper()
	m, p := versionTestFixture(t)
	if _, err := m.MakeTagWithTagKey("B", []string{"fixed", "new"}); err != nil {
		t.Fatal(err)
	}
	ctx := &eventbus.ChannelContext{
		ChannelId: "channel", ChannelType: wkproto.ChannelTypeData, SlotLeaderId: 1,
		Events: []*eventbus.Event{
			{Type: eventbus.EventChannelDistribute, MessageId: 101, TagKey: "A", Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{}},
			{Type: eventbus.EventChannelDistribute, MessageId: 102, TagKey: secondKey, Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{}},
		},
	}
	NewHandler().distributeCommon(ctx)
	got := map[int64][]string{}
	for _, e := range p.events {
		if e.Type == eventbus.EventPushOnline {
			got[e.MessageId] = append(got[e.MessageId], e.ToUid)
		}
	}
	for id := range got {
		sort.Strings(got[id])
	}
	want := map[int64][]string{101: {"fixed", "removed"}, 102: {"fixed", "removed"}}
	if secondKey == "B" {
		want[102] = []string{"fixed", "new"}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("recipients by message: got %v; want %v; second event key after distribution=%q", got, want, ctx.Events[1].TagKey)
	}
}

func TestDistributionHomogeneousBatch(t *testing.T) { versionTestDistributeBatch(t, "A") }
func TestDistributionMixedTagBatchKeepsRecipients(t *testing.T) {
	versionTestDistributeBatch(t, "B")
}
