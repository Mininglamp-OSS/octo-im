package presence

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

type snapshotRouteCluster struct {
	*testCluster
	server *wkserver.Server
}

func (c *snapshotRouteCluster) Route(path string, handler wkserver.Handler) {
	c.server.Route(path, handler)
}
func (c *snapshotRouteCluster) GetSlotId(uid string) uint32 {
	if uid == "foreign" {
		return 20
	}
	return 19
}
func (c *snapshotRouteCluster) SlotLeaderId(slot uint32) uint64 {
	if slot == 20 {
		return 3
	}
	return 2
}

func TestSnapshotRouteRestrictsEveryUIDToItsCurrentLeader(t *testing.T) {
	// Production configures logging before starting transport goroutines.
	// Initialize its lazy fallback here too, before client/server run together.
	wklog.Info("starting presence snapshot transport test")
	owner, _, cluster, _, _, _ := fixture(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	server := wkserver.New("tcp://" + address)
	cluster.nodes = []*types.Node{{Id: 1, Online: true}, {Id: 2, Online: true}, {Id: 3, Online: true}}
	service.Cluster = &snapshotRouteCluster{testCluster: cluster, server: server}
	owner.SetRoutes()
	require.NoError(t, server.Start())
	defer server.Stop()

	for _, tc := range []struct {
		name, peer string
		uids       []string
		status     proto.Status
	}{
		{"current leader", "2", []string{"u"}, proto.StatusOK},
		{"former leader", "1", []string{"u"}, proto.StatusError},
		{"different member", "3", []string{"u"}, proto.StatusError},
		{"unknown member", "99", []string{"u"}, proto.StatusError},
		{"non-node peer", "client", []string{"u"}, proto.StatusError},
		{"noncanonical node", "02", []string{"u"}, proto.StatusError},
		{"mixed authority batch", "2", []string{"u", "foreign"}, proto.StatusError},
		{"empty batch", "2", nil, proto.StatusError},
		{"empty uid", "2", []string{""}, proto.StatusError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := client.New("tcp://"+address, client.WithUid(tc.peer))
			require.NoError(t, peer.Start())
			defer peer.Stop()
			require.Eventually(t, peer.IsAuthed, 5*time.Second, 10*time.Millisecond)
			body, err := json.Marshal(tc.uids)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			response, err := peer.RequestWithContext(ctx, snapshotPath, body)
			require.NoError(t, err)
			require.Equal(t, tc.status, response.Status)
			if tc.status != proto.StatusOK {
				require.NotContains(t, string(response.Body), "Sessions")
				require.NotContains(t, string(response.Body), owner.boot)
			} else {
				var snapshot snapshotResponse
				require.NoError(t, json.Unmarshal(response.Body, &snapshot))
				require.Len(t, snapshot.Sessions, 1)
			}
		})
	}

	t.Run("request before transport CONNECT", func(t *testing.T) {
		socket, err := net.DialTimeout("tcp", address, time.Second)
		require.NoError(t, err)
		defer socket.Close()
		require.NoError(t, socket.SetDeadline(time.Now().Add(time.Second)))
		request := &proto.Request{Id: 7, Path: snapshotPath, Body: []byte(`["u"]`)}
		body, err := request.Marshal()
		require.NoError(t, err)
		wire, err := proto.New().Encode(body, proto.MsgTypeRequest)
		require.NoError(t, err)
		_, err = socket.Write(wire)
		require.NoError(t, err)
		header := make([]byte, proto.MagicNumberStartLength+proto.MsgTypeLength+proto.MsgContentLength)
		_, err = io.ReadFull(socket, header)
		require.NoError(t, err)
		require.Equal(t, proto.MagicNumberStart, header[:proto.MagicNumberStartLength])
		require.Equal(t, byte(proto.MsgTypeResp), header[proto.MagicNumberStartLength])
		length := binary.BigEndian.Uint32(header[proto.MagicNumberStartLength+proto.MsgTypeLength:])
		require.Less(t, length, uint32(1024))
		body = make([]byte, length)
		_, err = io.ReadFull(socket, body)
		require.NoError(t, err)
		var response proto.Response
		require.NoError(t, response.Unmarshal(body))
		require.Equal(t, proto.StatusError, response.Status)
		require.NotContains(t, string(response.Body), owner.boot)
	})
}
