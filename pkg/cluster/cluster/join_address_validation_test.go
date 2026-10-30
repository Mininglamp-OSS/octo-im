package cluster

import (
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

func TestJoinRPCRejectsInvalidAddressBeforeMembership(t *testing.T) {
	for _, tc := range []struct {
		name, addr, reason string
	}{
		{"empty", "", "required"},
		{"blank", "   ", "required"},
		{"nonnumeric", "127.0.0.1:abc", "port"},
		{"zero", "127.0.0.1:0", "port"},
		{"negative", "127.0.0.1:-1", "port"},
		{"overflow", "127.0.0.1:65536", "port"},
		{"space", "127.0.0.1: 11110", "whitespace"},
		{"trailing_space", "127.0.0.1:11110 ", "whitespace"},
		{"bind_all", "0.0.0.0:11110", "bind-all"},
		{"scoped_bind_all", "[::%lo]:11110", "bind-all"},
		{"scoped_expanded_bind_all", "tcp://[0:0:0:0:0:0:0:0%eth0]:11110", "bind-all"},
		{"scoped_mapped_bind_all", "[::ffff:0.0.0.0%lo]:11110", "bind-all"},
		{"malformed_zone", "[::%]:11110", "invalid IP literal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			servers, ctx := newConfigRPCServersWithSlots(t, 1, 1)
			s := servers[0]
			cli := client.New(s.opts.Addr, client.WithUid("1009"))
			require.NoError(t, cli.Start())
			t.Cleanup(cli.Stop)
			require.Eventually(t, cli.IsAuthed, 5*time.Second, 20*time.Millisecond)
			request := &ClusterJoinReq{NodeId: 1009, ServerAddr: tc.addr}
			payload, err := request.Marshal()
			require.NoError(t, err)
			response, err := cli.RequestWithContext(ctx, "/rpc/cluster/join", payload)
			require.NoError(t, err)
			require.Equal(t, proto.StatusError, response.Status)
			require.Contains(t, string(response.Body), tc.reason)
			require.Nil(t, s.cfgServer.Node(request.NodeId), "rejected address must never enter membership")
		})
	}
}

func TestJoinRPCFollowerRejectsBeforeForwarding(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 2, 1)
	var leader, follower *Server
	require.Eventually(t, func() bool {
		leader, follower = nil, nil
		for _, server := range servers {
			if server.cfgServer.IsLeader() {
				leader = server
			} else {
				follower = server
			}
		}
		return leader != nil && follower != nil && follower.cfgServer.LeaderId() == leader.opts.ConfigOptions.NodeId
	}, 5*time.Second, 20*time.Millisecond)
	var forwarded atomic.Int32
	leader.Route("/rpc/cluster/join", func(c *wkserver.Context) {
		forwarded.Add(1)
		c.WriteErr(errors.New("invalid join reached the leader"))
	})
	cli := client.New(follower.opts.Addr, client.WithUid("1009"))
	require.NoError(t, cli.Start())
	t.Cleanup(cli.Stop)
	require.Eventually(t, cli.IsAuthed, 5*time.Second, 20*time.Millisecond)
	for _, addr := range []string{"", "127.0.0.1:abc", "[::%lo]:11110", "[::ffff:0.0.0.0%lo]:11110", "[::%]:11110"} {
		request := &ClusterJoinReq{NodeId: 1009, ServerAddr: addr}
		payload, err := request.Marshal()
		require.NoError(t, err)
		response, err := cli.RequestWithContext(ctx, "/rpc/cluster/join", payload)
		require.NoError(t, err)
		require.Equal(t, proto.StatusError, response.Status)
		require.NotContains(t, string(response.Body), "reached the leader")
		require.Zero(t, forwarded.Load(), "invalid requests must be rejected before forwarding")
		require.Nil(t, leader.cfgServer.Node(request.NodeId))
	}
}

func TestJoinRPCResponseDuringAddressBackfill(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 1, 1)
	s := servers[0]
	cfg := s.cfgServer.GetClusterConfig()
	address := cfg.Nodes[0].ClusterAddr
	require.NotEmpty(t, address)
	cfg.Nodes[0].ClusterAddr = ""
	require.NoError(t, s.cfgServer.ProposeConfig(cfg))
	require.Empty(t, s.cfgServer.Node(1).ClusterAddr)

	cli := client.New(s.opts.Addr, client.WithUid("1009"))
	require.NoError(t, cli.Start())
	t.Cleanup(cli.Stop)
	require.Eventually(t, cli.IsAuthed, 5*time.Second, 20*time.Millisecond)
	payload, err := (&ClusterJoinReq{NodeId: 1009, ServerAddr: "127.0.0.1:11119", Role: types.NodeRole_NodeRoleReplica}).Marshal()
	require.NoError(t, err)

	backfilled := make(chan error, 1)
	go func() {
		backfilled <- s.cfgServer.ProposeClusterAddr(1, address)
	}()
	readResponse := func() string {
		t.Helper()
		response, err := cli.RequestWithContext(ctx, "/rpc/cluster/join", payload)
		require.NoError(t, err)
		require.Equal(t, proto.StatusOK, response.Status, string(response.Body))
		joined := &ClusterJoinResp{}
		require.NoError(t, joined.Unmarshal(response.Body))
		for _, node := range joined.Nodes {
			if node.NodeId == 1 {
				return node.ServerAddr
			}
		}
		t.Fatal("join response omitted the existing member")
		return ""
	}
	for i := 0; i < 8; i++ {
		observed := readResponse()
		require.True(t, observed == "" || observed == address, "join response contains a partial address: %q", observed)
	}
	select {
	case err := <-backfilled:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Equal(t, address, readResponse(), "joins after backfill must receive the committed address")
}

func TestJoinWithWildcardListenerAndAdvertisedAddress(t *testing.T) {
	servers, ctx := newConfigRPCServersWithSlots(t, 1, 1)
	leader := servers[0]
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	listenAddr := listener.Addr().String()
	_, port, err := net.SplitHostPort(listenAddr)
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	advertised := net.JoinHostPort("127.0.0.1", port)
	seed := "1@" + leader.cfgServer.Node(1).ClusterAddr
	cfg := newTestOptions(t, 2, nil, clusterconfig.WithSeed(seed), clusterconfig.WithServerAddr(advertised),
		clusterconfig.WithSlotCount(1), clusterconfig.WithSlotMaxReplicaCount(1))
	joining := New(NewOptions(WithAddr("tcp://"+listenAddr), WithServerAddr(advertised), WithSeed(seed),
		WithConfigOptions(cfg), WithDataDir(t.TempDir()), WithDBWKDbShardNum(1), WithDBSlotShardNum(1),
		WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
	require.NoError(t, joining.Start())
	t.Cleanup(joining.Stop)
	joining.configReconciler.stop()
	for _, server := range []*Server{leader, joining} {
		require.Eventually(t, func() bool {
			node := server.cfgServer.Node(2)
			return node != nil && node.ClusterAddr == advertised && node.Status != types.NodeStatus_NodeStatusWillJoin
		}, 10*time.Second, 20*time.Millisecond, "membership must advertise the reachable address, not the wildcard bind")
	}
	conn, err := net.DialTimeout("tcp", joining.cfgServer.Node(2).ClusterAddr, time.Second)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, leader.cfgServer.ProposeApiServerAddr(2, "http://127.0.0.1:5002"))
	require.Eventually(t, func() bool {
		node := joining.cfgServer.Node(2)
		return node != nil && node.ApiServerAddr == "http://127.0.0.1:5002"
	}, 5*time.Second, 20*time.Millisecond, "the joined peer must receive a later committed config command")
	require.NoError(t, ctx.Err())
}
