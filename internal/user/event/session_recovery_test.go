package event

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/stretchr/testify/require"
	"testing"
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
