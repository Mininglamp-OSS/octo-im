package wkserver

import (
	"testing"

	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/require"
)

type peerUIDConn struct {
	gnet.Conn
	context interface{}
}

func (c *peerUIDConn) Context() interface{} { return c.context }

func TestContextPeerUIDRequiresTransportConnectContext(t *testing.T) {
	require.Empty(t, NewContext(nil).PeerUID())
	conn := &peerUIDConn{}
	require.Empty(t, NewContext(conn).PeerUID())
	conn.context = "2"
	require.Empty(t, NewContext(conn).PeerUID())
	conn.context = (*connContext)(nil)
	require.Empty(t, NewContext(conn).PeerUID())
	conn.context = newConnContext("2")
	require.Equal(t, "2", NewContext(conn).PeerUID())
}
