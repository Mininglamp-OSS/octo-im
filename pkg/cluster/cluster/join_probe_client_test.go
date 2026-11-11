package cluster

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/lni/goutils/syncutil"
	"github.com/stretchr/testify/require"
)

type probeGuardPeer struct {
	stop                  func()
	stopPeerOnce          func()
	server                *Server
	peer                  *ImprovedNode
	probes, legacy, joins atomic.Uint64
	invalidJoin           atomic.Bool
}

func writeProbeGuardReply(t *testing.T, c *wkserver.Context, reply *ClusterJoinResp) {
	t.Helper()
	data, err := reply.Marshal()
	if err != nil {
		t.Errorf("encode mock peer reply: %v", err)
		c.WriteErr(err)
		return
	}
	c.Write(data)
}

// Exercise the real TCP client and response decoder. The local config server
// deliberately has no running Raft: rejected replies and formal-member probes
// must never reach ResumeJoin or install any local recovery configuration.
func newProbeGuardPeer(t *testing.T, probe wkserver.Handler, joined *ClusterJoinResp, legacyOnly bool) *probeGuardPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	fixture := &probeGuardPeer{}
	remote := wkserver.New("tcp://" + addr)
	if !legacyOnly {
		remote.Route("/rpc/cluster/join/probe/v2", func(c *wkserver.Context) {
			fixture.probes.Add(1)
			probe(c)
		})
	}
	remote.Route("/rpc/cluster/join/probe/v1", func(c *wkserver.Context) {
		fixture.legacy.Add(1)
		c.WriteErr(errors.New("legacy probe could admit an unknown member"))
	})
	remote.Route("/rpc/cluster/join", func(c *wkserver.Context) {
		fixture.joins.Add(1)
		var req ClusterJoinReq
		if err := req.Unmarshal(c.Body()); err != nil || req.Probe || req.NodeId != 1 || req.ServerAddr != "127.0.0.1:19001" {
			fixture.invalidJoin.Store(true)
		}
		if joined == nil {
			c.WriteErr(errors.New("unexpected ordinary join"))
			return
		}
		writeProbeGuardReply(t, c, joined)
	})
	require.NoError(t, remote.Start())
	t.Cleanup(remote.Stop)
	opts := NewOptions(WithServerAddr("127.0.0.1:19001"))
	opts.ConfigOptions = clusterconfig.NewOptions(clusterconfig.WithNodeId(1), clusterconfig.WithConfigPath(filepath.Join(t.TempDir(), "config.json")))
	opts.SendQueueLength = 32
	s := &Server{opts: opts, cfgServer: clusterconfig.New(opts.ConfigOptions), nodeManager: newNodeManager(opts), stopper: syncutil.NewStopper()}
	s.rpcClient = newRpcClient(s)
	fixture.server = s
	fixture.stop = sync.OnceFunc(s.stopper.Stop)
	t.Cleanup(fixture.stop)
	peer := NewImprovedNode(2, "probe-client-guard", addr, opts)
	fixture.peer = peer
	s.nodeManager.addNode(peer)
	peer.Start()
	fixture.stopPeerOnce = sync.OnceFunc(peer.Stop)
	t.Cleanup(fixture.stopPeerOnce)
	require.Eventually(t, peer.client.IsAuthed, 3*time.Second, 10*time.Millisecond)
	return fixture
}

func probeGuardAbsent() *ClusterJoinResp {
	return &ClusterJoinResp{LeaderID: 2, Config: &nodetypes.Config{Version: 10,
		Nodes: []*nodetypes.Node{{Id: 2, AllowVote: true, Status: nodetypes.NodeStatus_NodeStatusJoined}}}}
}

func TestMembershipProbeClientRejectsUnprovenAbsence(t *testing.T) {
	for _, kind := range []string{"rpc-error", "malformed", "join-boundary", "zero-version", "zero-leader", "missing-leader", "empty-membership"} {
		t.Run(kind, func(t *testing.T) {
			reply := probeGuardAbsent()
			switch kind {
			case "join-boundary":
				reply.JoinVersion = reply.Config.Version
			case "zero-version":
				reply.Config.Version = 0
			case "zero-leader":
				reply.LeaderID = 0
			case "missing-leader":
				reply.LeaderID = 3
			case "empty-membership":
				reply.Config.Nodes = nil
			}
			f := newProbeGuardPeer(t, func(c *wkserver.Context) {
				switch kind {
				case "rpc-error":
					c.WriteErr(errors.New("leader changed"))
				case "malformed":
					c.Write([]byte{1})
				default:
					writeProbeGuardReply(t, c, reply)
				}
			}, nil, false)
			require.Error(t, f.server.probeMembership(2))
			require.Equal(t, uint64(1), f.probes.Load())
			require.Zero(t, f.joins.Load(), "an error or incomplete snapshot cannot authorize an ordinary join")
			require.Zero(t, f.legacy.Load())
			require.Zero(t, f.server.cfgServer.Snapshot().Version)
		})
	}
}

func TestMembershipProbeClientKeepsFormalMembersReadOnly(t *testing.T) {
	for _, kind := range []string{"nodes-only", "joined-snapshot", "joining-snapshot"} {
		t.Run(kind, func(t *testing.T) {
			reply := probeGuardAbsent()
			if kind == "nodes-only" {
				reply.Config, reply.LeaderID = nil, 0
			} else {
				status := nodetypes.NodeStatus_NodeStatusJoined
				if kind == "joining-snapshot" {
					status = nodetypes.NodeStatus_NodeStatusJoining
				}
				reply.Config.Nodes = append(reply.Config.Nodes, &nodetypes.Node{Id: 1, AllowVote: true, Status: status})
			}
			f := newProbeGuardPeer(t, func(c *wkserver.Context) { writeProbeGuardReply(t, c, reply) }, nil, false)
			require.NoError(t, f.server.probeMembership(2))
			require.Equal(t, uint64(1), f.probes.Load())
			require.Zero(t, f.joins.Load())
			require.Zero(t, f.legacy.Load())
			require.Zero(t, f.server.cfgServer.Snapshot().Version)
		})
	}
}

func TestMembershipProbeClientRequiresExplicitJoinBoundary(t *testing.T) {
	for _, kind := range []string{"rpc-error", "no-config", "zero-boundary", "stale-snapshot", "missing-self", "zero-leader", "missing-leader", "stopped"} {
		t.Run(kind, func(t *testing.T) {
			joined := &ClusterJoinResp{JoinVersion: 11, LeaderID: 2, Config: &nodetypes.Config{Version: 11,
				Nodes: []*nodetypes.Node{{Id: 1, Status: nodetypes.NodeStatus_NodeStatusWillJoin}, {Id: 2, AllowVote: true}}}}
			switch kind {
			case "rpc-error":
				joined = nil
			case "no-config":
				joined.Config = nil
			case "zero-boundary":
				joined.JoinVersion = 0
			case "stale-snapshot":
				joined.Config.Version = joined.JoinVersion - 1
			case "missing-self":
				joined.Config.Nodes = joined.Config.Nodes[1:]
			case "zero-leader":
				joined.LeaderID = 0
			case "missing-leader":
				joined.LeaderID = 3
			}
			f := newProbeGuardPeer(t, func(c *wkserver.Context) { writeProbeGuardReply(t, c, probeGuardAbsent()) }, joined, false)
			if kind == "stopped" {
				f.stop()
				require.NoError(t, f.server.probeMembership(2))
				require.Zero(t, f.joins.Load(), "shutdown must not begin a new membership write")
			} else {
				require.Error(t, f.server.probeMembership(2))
				require.Equal(t, uint64(1), f.joins.Load(), "only an explicit successful absence probe triggers ordinary join")
			}
			require.Equal(t, uint64(1), f.probes.Load())
			require.Zero(t, f.legacy.Load())
			require.False(t, f.invalidJoin.Load(), "ordinary join must use the local identity, address, and Probe=false")
			require.Zero(t, f.server.cfgServer.Snapshot().Version)
			require.False(t, f.server.joinReady.Load())
		})
	}
}

func TestMembershipProbeClientDoesNotFallBackToV1(t *testing.T) {
	f := newProbeGuardPeer(t, nil, nil, true)
	require.Error(t, f.server.probeMembership(2), "a v1-only peer does not prove read-only discovery support")
	require.Zero(t, f.probes.Load())
	require.Zero(t, f.legacy.Load(), "the older mutating probe must not be called")
	require.Zero(t, f.joins.Load(), "an unsupported route is not evidence of missing membership")
}

func TestMembershipProbeClientDisconnectedPeerDoesNotJoin(t *testing.T) {
	f := newProbeGuardPeer(t, func(c *wkserver.Context) { writeProbeGuardReply(t, c, probeGuardAbsent()) }, nil, false)
	f.stopPeerOnce()
	require.Eventually(t, func() bool { return !f.peer.client.IsAuthed() }, time.Second, time.Millisecond)
	require.Error(t, f.server.probeMembership(2))
	require.Zero(t, f.probes.Load())
	require.Zero(t, f.legacy.Load())
	require.Zero(t, f.joins.Load(), "a disconnected peer cannot establish missing membership")
}

func TestMembershipProbeClientRechecksNewerLocalMembership(t *testing.T) {
	for _, containsSelf := range []bool{true, false} {
		name := "still-absent"
		if containsSelf {
			name = "already-present"
		}
		t.Run(name, func(t *testing.T) {
			// Load a real persisted startup snapshot newer than the remote
			// absence response. This tests response ordering, not a forged
			// production Apply or a direct Raft state-machine event.
			local := &nodetypes.Config{Version: 11, Nodes: []*nodetypes.Node{{Id: 2, AllowVote: true}}}
			if containsSelf {
				local.Nodes = append(local.Nodes, &nodetypes.Node{Id: 1, AllowVote: true, Status: nodetypes.NodeStatus_NodeStatusJoined})
			}
			f := newProbeGuardPeer(t, func(c *wkserver.Context) { writeProbeGuardReply(t, c, probeGuardAbsent()) }, &ClusterJoinResp{}, false)
			data, err := json.Marshal(local)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(f.server.opts.ConfigOptions.ConfigPath, data, 0600))
			f.server.cfgServer = clusterconfig.New(f.server.opts.ConfigOptions)
			require.Equal(t, uint64(11), f.server.cfgServer.Snapshot().Version)

			err = f.server.probeMembership(2)
			t.Logf("probe snapshot=10 local snapshot=11 contains self=%t ordinary joins=%d", containsSelf, f.joins.Load())
			if containsSelf {
				require.Zero(t, f.joins.Load(), "an older absence response must not rejoin an already present member")
				require.NoError(t, err)
			} else {
				require.Equal(t, uint64(1), f.joins.Load(), "a newer snapshot without self does not establish membership")
				require.Error(t, err, "the mock's empty join boundary is still rejected")
			}
			require.Equal(t, uint64(1), f.probes.Load())
			require.Zero(t, f.legacy.Load())
			require.Equal(t, uint64(11), f.server.cfgServer.Snapshot().Version)
		})
	}
}
