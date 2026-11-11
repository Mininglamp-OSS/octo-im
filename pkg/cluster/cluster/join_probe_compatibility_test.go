package cluster

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestMembershipProbeDoesNotFallBackToLegacyMutatingJoin(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	legacy := wkserver.New("tcp://" + addr)
	var joins atomic.Uint64
	// This wire-compatible server exposes only the legacy join route. It is
	// an endpoint compatibility fixture, not an older complete IM binary.
	legacy.Route("/rpc/cluster/join", func(c *wkserver.Context) {
		joins.Add(1)
		enc := wkproto.NewEncoder()
		defer enc.End()
		enc.WriteUint16(1)
		enc.WriteUint64(2)
		enc.WriteString(addr)
		c.Write(enc.Bytes()) // historical response has no recovery boundary
	})
	require.NoError(t, legacy.Start())
	t.Cleanup(legacy.Stop)
	opts := NewOptions()
	opts.SendQueueLength = 32
	s := &Server{nodeManager: newNodeManager(opts)}
	peer := NewImprovedNode(2, "probe-compatibility", addr, opts)
	s.nodeManager.addNode(peer)
	peer.Start()
	t.Cleanup(peer.Stop)
	require.Eventually(t, peer.client.IsAuthed, 3*time.Second, 10*time.Millisecond)
	rpc := newRpcClient(s)
	req := &ClusterJoinReq{NodeId: 1, ServerAddr: "127.0.0.1:1001", Role: nodetypes.NodeRole_NodeRoleReplica}
	response, err := rpc.RequestClusterJoin(2, req)
	require.NoError(t, err, "the legacy connection and ordinary join route are usable")
	require.Zero(t, response.JoinVersion, "a legacy reply cannot establish the new recovery boundary")
	require.Nil(t, response.Config)
	require.Equal(t, uint64(1), joins.Load())

	req.Probe = true
	_, err = rpc.RequestClusterJoin(2, req)
	require.Error(t, err, "an old peer does not implement the read-only membership probe")
	require.Equal(t, uint64(1), joins.Load(), "periodic probes must not silently invoke the legacy mutating join")
	req.Probe = false
	response, err = rpc.RequestClusterJoin(2, req)
	require.NoError(t, err, "the failed probe is a capability limitation, not a broken peer connection")
	require.Zero(t, response.JoinVersion)
	require.Equal(t, uint64(2), joins.Load())
}
