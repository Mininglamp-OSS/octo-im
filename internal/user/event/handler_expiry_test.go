package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
	"github.com/stretchr/testify/require"
)

func TestUserHandlerTimeoutClockRegression(t *testing.T) {
	old := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = old })
	timeout := int64(options.G.Poller.UserTimeout.Seconds())

	for _, tc := range []struct {
		name    string
		offset  int64
		expired bool
	}{
		{name: "fresh"},
		{name: "one_second_ahead", offset: 1},
		{name: "clock_rollback", offset: 600},
		{name: "before_timeout", offset: -timeout + 1},
		{name: "at_timeout", offset: -timeout},
		{name: "past_timeout", offset: -timeout - 1, expired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Check exact boundaries within one cached-clock tick. Retry only
			// when its background ticker advanced during the observation.
			for attempt := 0; attempt < 10; attempt++ {
				now := fasttime.UnixTimestamp()
				u := &userHandler{lastActive: uint64(int64(now) + tc.offset)}
				got := u.isTimeout()
				if fasttime.UnixTimestamp() != now {
					continue
				}
				require.Equal(t, tc.expired, got)
				return
			}
			t.Fatal("cached clock changed during every timeout observation")
		})
	}
}

type expiryCluster struct{ icluster.ICluster }

func (*expiryCluster) GetSlotId(string) uint32    { return 1 }
func (*expiryCluster) SlotLeaderId(uint32) uint64 { return 2 }

func TestUserPollerClearClockRegression(t *testing.T) {
	oldOptions, oldCluster := options.G, service.Cluster
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	// A remote slot leader exercises eviction without requiring local metrics.
	service.Cluster = &expiryCluster{}
	t.Cleanup(func() { options.G, service.Cluster = oldOptions, oldCluster })

	for _, tc := range []struct {
		name   string
		offset int64
		remove bool
	}{
		{name: "fresh"},
		{name: "clock_rollback", offset: 600},
		{name: "genuinely_idle", offset: -int64(options.G.Poller.UserTimeout.Seconds()) - 1, remove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Drive the real sweep synchronously, without starting workers.
			p := &poller{waitlist: newLinkedList(), eventPool: &EventPool{}}
			u := newUserHandler("expiry-user", p)
			var conn *eventbus.Conn
			if !tc.remove {
				conn = &eventbus.Conn{
					Uid: u.Uid, NodeId: 1, ConnId: 7, Auth: true,
					OwnerBootID: "boot", SessionID: "session",
				}
				u.addConn(conn)
			}
			p.waitlist.push(u)
			// Assign after addConn, which refreshes the handler timestamp.
			u.lastActive = uint64(int64(fasttime.UnixTimestamp()) + tc.offset)
			if conn != nil {
				conn.LastActive = u.lastActive
			}
			// Run both connection expiry and handler expiry in this tick.
			u.tickCount = 9
			p.tickCount = options.G.Poller.ClearIntervalTick - 1
			p.tick()

			if tc.remove {
				require.Nil(t, p.handler(u.Uid), "an idle handler must still be removed")
				require.Zero(t, p.allUserCount())
				return
			}
			require.Same(t, u, p.handler(u.Uid), "the owning handler must survive the sweep")
			require.Same(t, conn, p.connById(u.Uid, conn.NodeId, conn.ConnId))
			got := p.authedConnsByUid(u.Uid)
			require.Len(t, got, 1, "the session must remain visible to delivery")
			require.Same(t, conn, got[0])
			require.Zero(t, u.pending.eventQueue.Len(), "no connection removal should be queued")
			require.Equal(t, 1, p.allUserCount())
		})
	}
}
