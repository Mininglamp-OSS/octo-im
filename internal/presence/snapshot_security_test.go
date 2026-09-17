package presence

import (
	"encoding/json"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/stretchr/testify/require"
)

func TestPreparedSnapshotDoesNotDiscloseAuthenticationIdentity(t *testing.T) {
	owner, _, _, _, _, _ := fixture(t)
	conn := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 8, DeviceId: "mobile"}
	owner.Prepare(&testSocket{id: 8}, conn)
	response, err := owner.snapshot([]string{"u"})
	require.NoError(t, err)
	require.Len(t, response.PreparedSessions, 1)
	marker := &eventbus.Conn{}
	require.NoError(t, marker.Decode(response.PreparedSessions[0]))
	require.NotEqual(t, conn.SessionID, marker.SessionID)
	require.NotContains(t, string(response.PreparedSessions[0]), conn.SessionID)
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), conn.SessionID)
	require.False(t, marker.Auth)
	require.Empty(t, marker.AesKey)
	require.Empty(t, marker.AesIV)
	require.Zero(t, marker.Uptime)
	marker.Auth = true
	require.False(t, owner.Authenticate(marker), "a snapshot marker must never authenticate the pending socket")
	conn.Auth = true
	require.True(t, owner.Authenticate(conn), "the original CONNECT/CONNACK identity must still work")
}

func TestPreparedDigestIsBoundToTheEntireSessionIdentity(t *testing.T) {
	original := &eventbus.Conn{Uid: "u", NodeId: 1, ConnId: 7, OwnerBootID: "boot", SessionID: "session"}
	marker := copyConn(original)
	marker.SessionID = preparedSessionID(original)
	require.True(t, preparedSessionMatches(original, marker))
	for _, change := range []func(*eventbus.Conn){
		func(c *eventbus.Conn) { c.Uid = "other" },
		func(c *eventbus.Conn) { c.NodeId++ },
		func(c *eventbus.Conn) { c.ConnId++ },
		func(c *eventbus.Conn) { c.OwnerBootID = "new-boot" },
		func(c *eventbus.Conn) { c.SessionID = "replacement" },
	} {
		other := copyConn(original)
		change(other)
		require.False(t, preparedSessionMatches(other, marker))
	}
	marker.Auth = true
	require.False(t, preparedSessionMatches(original, marker))
}
