package cluster

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

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
	for _, addr := range []string{"", "127.0.0.1:abc"} {
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
