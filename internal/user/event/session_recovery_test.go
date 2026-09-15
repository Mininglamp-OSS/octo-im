package event

import (
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/stretchr/testify/require"
)

func TestRecoveredSessionCreatesUserAndSurvivesStaleClose(t *testing.T) {
	old := options.G
	t.Cleanup(func() { options.G = old })
	options.G = options.New()
	options.G.Poller.UserCount = 1
	pool := NewEventPool(&mockUserEventHandler{})
	defer pool.Stop()
	current := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "new"}
	pool.UpdateConn(current)
	require.Len(t, pool.AuthedConnsByUid("u"), 1)
	pool.RemoveConn(&eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, OwnerBootID: "boot", SessionID: "old"})
	require.Len(t, pool.AuthedConnsByUid("u"), 1)
	pool.RemoveConn(current)
	require.Empty(t, pool.AuthedConnsByUid("u"))
}

func TestUpdateRecoveredSessionPreservesRuntimeState(t *testing.T) {
	old := options.G
	t.Cleanup(func() { options.G = old })
	options.G = options.New()
	options.G.Poller.UserCount = 1
	pool := NewEventPool(&mockUserEventHandler{})
	defer pool.Stop()
	current := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "session", AesIV: []byte("iv"), AesKey: []byte("key"), LastActive: 9}
	current.InPacketCount.Store(4)
	pool.UpdateConn(current)
	pool.UpdateConn(&eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "session", LastActive: 2})
	got := pool.ConnsByUid("u")
	require.Len(t, got, 1)
	require.Equal(t, int64(4), got[0].InPacketCount.Load())
	require.Equal(t, []byte("iv"), got[0].AesIV)
	require.Equal(t, []byte("key"), got[0].AesKey)
	require.Equal(t, uint64(9), got[0].LastActive)
}

func TestConnsByUidReturnsSnapshotDuringConcurrentUpdates(t *testing.T) {
	old := options.G
	t.Cleanup(func() { options.G = old })
	options.G = options.New()
	options.G.Poller.UserCount = 1
	pool := NewEventPool(&mockUserEventHandler{})
	defer pool.Stop()

	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				conn := &eventbus.Conn{Uid: "u", NodeId: uint64(worker + 1), ConnId: int64(i % 8), Auth: true, OwnerBootID: "boot", SessionID: "session"}
				pool.UpdateConn(conn)
				if i%3 == 0 {
					pool.RemoveConn(conn)
				}
				for range pool.ConnsByUid("u") {
				}
			}
		}(worker)
	}
	wg.Wait()
}

func TestLegacyRemovalMatchesTheSamePreparedSocket(t *testing.T) {
	old := options.G
	t.Cleanup(func() { options.G = old })
	options.G = options.New()
	options.G.Poller.UserCount = 1
	pool := NewEventPool(&mockUserEventHandler{})
	defer pool.Stop()
	current := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: 1, Uptime: 9, Auth: true, OwnerBootID: "boot", SessionID: "session"}
	pool.UpdateConn(current)
	pool.RemoveConn(&eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, DeviceId: "web", DeviceFlag: 1, Uptime: 9})
	require.Empty(t, pool.ConnsByUid("u"))
}

type untouchedPhysicalManager struct {
	service.IConnManager
	t *testing.T
}

func (m *untouchedPhysicalManager) GetConn(int64) wknet.Conn {
	m.t.Fatal("logical removal must not look up a physical socket by numeric ID")
	return nil
}
func (m *untouchedPhysicalManager) RemoveConn(wknet.Conn) {
	m.t.Fatal("logical removal must not delete a physical socket")
}

func TestRecoveredSessionRemovalDoesNotTouchPhysicalManager(t *testing.T) {
	oldOptions, oldManager := options.G, service.ConnManager
	t.Cleanup(func() { options.G, service.ConnManager = oldOptions, oldManager })
	options.G = options.New()
	options.G.Poller.UserCount = 1
	service.ConnManager = &untouchedPhysicalManager{t: t}
	pool := NewEventPool(&mockUserEventHandler{})
	defer pool.Stop()
	for _, node := range []uint64{1, 2} {
		conn := &eventbus.Conn{Uid: "u", NodeId: node, ConnId: 7, Auth: true, OwnerBootID: "boot", SessionID: "session"}
		pool.UpdateConn(conn)
		pool.RemoveConn(conn)
		require.Empty(t, pool.ConnsByUid("u"))
	}
}
