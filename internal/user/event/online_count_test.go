package event

import (
	"fmt"
	"hash/crc32"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
	"github.com/stretchr/testify/require"
)

type onlineCountCluster struct {
	icluster.ICluster
	leaders [64]atomic.Uint64
}

func (*onlineCountCluster) GetSlotId(uid string) uint32       { return crc32.ChecksumIEEE([]byte(uid)) % 64 }
func (c *onlineCountCluster) SlotLeaderId(slot uint32) uint64 { return c.leaders[slot].Load() }
func (c *onlineCountCluster) setLeaders(leader uint64) {
	for i := range c.leaders {
		c.leaders[i].Store(leader)
	}
}

func newOnlineCountPool(t testing.TB) (*EventPool, *onlineCountCluster) {
	t.Helper()
	oldOptions, oldCluster := options.G, service.Cluster
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	options.G.Poller.UserCount = 2
	cluster := &onlineCountCluster{}
	cluster.setLeaders(1)
	service.Cluster = cluster
	pool := NewEventPool(&mockUserEventHandler{})
	t.Cleanup(func() {
		pool.Stop()
		for _, p := range pool.pollers {
			p.handlePool.Release()
		}
		options.G, service.Cluster = oldOptions, oldCluster
	})
	return pool, cluster
}

func onlineCountConn(uid string, node uint64, id int64, auth bool) *eventbus.Conn {
	return &eventbus.Conn{Uid: uid, NodeId: node, ConnId: id, Auth: auth,
		OwnerBootID: "boot", SessionID: fmt.Sprintf("session-%d-%d", node, id),
		LastActive: fasttime.UnixTimestamp()}
}

func TestOnlineUserCountTracksLeadershipWithoutEvents(t *testing.T) {
	pool, cluster := newOnlineCountPool(t)
	pool.UpdateConn(onlineCountConn("online", 3, 1, true))
	require.EqualValues(t, 1, pool.OnlineUserCount())
	for i := 0; i < 10; i++ {
		cluster.setLeaders(2)
		require.Zero(t, pool.OnlineUserCount(), "a former leader must stop contributing even with a retained handler")
		cluster.setLeaders(1)
		require.EqualValues(t, 1, pool.OnlineUserCount(), "promotion must not wait for a user event")
	}
	cluster.setLeaders(0)
	require.Zero(t, pool.OnlineUserCount(), "an unknown leader must not claim the UID")
	service.Cluster = nil
	require.Zero(t, pool.OnlineUserCount(), "startup before cluster initialization is empty")
}

func TestOnlineUserCountDeduplicatesDevicesAcrossOwners(t *testing.T) {
	pool, _ := newOnlineCountPool(t)
	first := onlineCountConn("same-uid", 2, 1, true)
	second := onlineCountConn("same-uid", 3, 1, true)
	pool.UpdateConn(first)
	pool.UpdateConn(second)
	require.Equal(t, 2, pool.AllConnCount())
	require.EqualValues(t, 1, pool.OnlineUserCount())
	pool.RemoveConn(first)
	require.EqualValues(t, 1, pool.OnlineUserCount())
	pool.RemoveConn(second)
	require.Zero(t, pool.OnlineUserCount(), "the last disconnect must not wait for handler expiration")
	require.Equal(t, 1, pool.AllUserCount(), "an empty cached handler is not an online UID")
}

func TestOnlineUserCountExcludesEmptyAndUnauthenticatedHandlers(t *testing.T) {
	pool, _ := newOnlineCountPool(t)
	pool.AddEvent("empty", &eventbus.Event{Type: eventbus.EventOnSend})
	p := pool.pollerByUid("empty")
	h := p.handler("empty")
	h.advanceEvents(h.events())
	unauthenticated := onlineCountConn("auth-pending", 1, 2, false)
	pool.UpdateConn(unauthenticated)
	require.Equal(t, 2, pool.AllUserCount())
	require.Zero(t, pool.OnlineUserCount())
	pool.UpdateConn(onlineCountConn("auth-pending", 1, 2, true))
	require.EqualValues(t, 1, pool.OnlineUserCount())
}

func TestOnlineUserCountIncludesRecoveredSessionsAndFencesStaleClose(t *testing.T) {
	pool, _ := newOnlineCountPool(t)
	current := onlineCountConn("recovered", 3, 7, true)
	pool.UpdateConnRecovered(current)
	require.EqualValues(t, 1, pool.OnlineUserCount(), "presence recovery alone must update the exported count")
	stale := onlineCountConn("recovered", 3, 7, true)
	stale.SessionID = "previous-session"
	pool.RemoveConn(stale)
	require.EqualValues(t, 1, pool.OnlineUserCount())
	pool.RemoveConn(current)
	require.Zero(t, pool.OnlineUserCount())
}

func TestOnlineUserCountHandlerExpiryHasNoResidualContribution(t *testing.T) {
	pool, cluster := newOnlineCountPool(t)
	conn := onlineCountConn("expiring", 3, 1, true)
	pool.UpdateConn(conn)
	require.EqualValues(t, 1, pool.OnlineUserCount())
	cluster.setLeaders(2)
	pool.RemoveConn(conn)
	p := pool.pollerByUid(conn.Uid)
	h := p.handler(conn.Uid)
	h.lastActive = fasttime.UnixTimestamp() - uint64(options.G.Poller.UserTimeout.Seconds()) - 1
	p.tickCount = options.G.Poller.ClearIntervalTick - 1
	p.tick()
	require.Nil(t, p.handler(conn.Uid))
	require.Zero(t, pool.OnlineUserCount())
	cluster.setLeaders(1)
	require.Zero(t, pool.OnlineUserCount(), "deleting an uncounted/promoted handler cannot make the count negative")
}

func TestOnlineUserCountConcurrentCollectionsAndSessionChanges(t *testing.T) {
	pool, cluster := newOnlineCountPool(t)
	const users, iterations = 64, 300
	for i := 0; i < users; i++ {
		pool.UpdateConn(onlineCountConn(fmt.Sprintf("user-%d", i), 1, 1, true))
	}
	tasks := []func(){
		func() {
			for i := 0; i < iterations; i++ {
				conn := onlineCountConn(fmt.Sprintf("user-%d", i%users), 2, 2, true)
				pool.UpdateConnRecovered(conn)
				pool.RemoveConn(conn)
			}
		},
		func() {
			for i := 0; i < iterations; i++ {
				cluster.setLeaders(uint64(1 + i%2))
			}
		},
		func() {
			// Exercise user-list insertion and removal while metrics traverse it.
			p := pool.pollerByUid("transient")
			for i := 0; i < iterations; i++ {
				pool.UpdateConn(onlineCountConn("transient", 1, 3, true))
				p.waitlist.remove("transient")
			}
		},
	}
	for i := 0; i < 4; i++ {
		tasks = append(tasks, func() {
			for i := 0; i < iterations; i++ {
				if got := pool.OnlineUserCount(); got < 0 || got > users+1 {
					t.Errorf("online count outside possible UID bounds: %d", got)
					return
				}
			}
		})
	}
	runConnectionStatsTasks(tasks...)
	cluster.setLeaders(1)
	require.EqualValues(t, users, pool.OnlineUserCount())
}

func BenchmarkOnlineUserCount(b *testing.B) {
	for _, users := range []int{126, 100000} {
		b.Run(fmt.Sprintf("users=%d", users), func(b *testing.B) {
			pool, _ := newOnlineCountPool(b)
			// Build fixture lists directly; repeated linked-list lookups are not
			// part of the collection operation being measured.
			for i := 0; i < users; i++ {
				uid := fmt.Sprintf("bench-%d", i)
				p := pool.pollerByUid(uid)
				h := newUserHandler(uid, p)
				h.addConn(onlineCountConn(uid, 1, 1, true))
				p.waitlist.push(h)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := pool.OnlineUserCount(); got != int64(users) {
					b.Fatalf("online count = %d, want %d", got, users)
				}
			}
		})
	}
}
