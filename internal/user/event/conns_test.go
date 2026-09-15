package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/stretchr/testify/require"
)

func TestConnsTouchRefreshesRegisteredDescriptor(t *testing.T) {
	connections := newConns()
	conn := &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, LastActive: 1}
	connections.add(conn)

	require.True(t, connections.touch(2, 7))
	require.Greater(t, conn.LastActive, uint64(1))
	require.False(t, connections.touch(2, 8))
}
