package cluster

import (
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	serverproto "github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// configuredProbeVoters follows configToRaftConfig's published membership rule.
// This is derived from the applied snapshot, not a read of private Raft state.
func configuredProbeVoters(cfg *nodetypes.Config) []uint64 {
	var voters []uint64
	for _, node := range cfg.Nodes {
		if node.AllowVote && node.Role == nodetypes.NodeRole_NodeRoleReplica && !slices.Contains(cfg.Learners, node.Id) {
			voters = append(voters, node.Id)
		}
	}
	slices.Sort(voters)
	return voters
}

func TestUnknownMembershipProbeDoesNotAdmitNodeOverTCP(t *testing.T) {
	for _, forwarding := range []bool{false, true} {
		t.Run(map[bool]string{false: "leader", true: "follower"}[forwarding], func(t *testing.T) {
			servers, ctx := newConfigRPCServersWithSlots(t, 3, 1)
			var leader, target *Server
			require.Eventually(t, func() bool {
				leader, target = nil, nil
				for _, s := range servers {
					if s.cfgServer.IsLeader() {
						leader = s
					} else if forwarding {
						target = s
					}
				}
				if leader == nil {
					return false
				}
				if !forwarding {
					target = leader
				}
				return target != nil && target.cfgServer.LeaderId() == leader.opts.ConfigOptions.NodeId
			}, 5*time.Second, 10*time.Millisecond)
			// Let real startup planning and configuration replication settle
			// before comparing versions; all control loops remain running.
			var lastVersion uint64
			stableSince := time.Now()
			require.Eventually(t, func() bool {
				cfg := leader.cfgServer.Snapshot()
				if cfg.Version != lastVersion {
					lastVersion, stableSince = cfg.Version, time.Now()
				}
				for _, s := range servers {
					if !proto.Equal(cfg, s.cfgServer.Snapshot()) {
						return false
					}
				}
				return time.Since(stableSince) >= 250*time.Millisecond
			}, 3*time.Second, 10*time.Millisecond)
			before := leader.cfgServer.Snapshot()
			require.Len(t, before.Nodes, 3)
			require.Empty(t, before.Learners)
			require.Equal(t, []uint64{1, 2, 3}, configuredProbeVoters(before))

			advertised, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, advertised.Close()) })
			// The caller is not a cluster peer and supplies no credential. Its
			// CONNECT UID is unrelated to both the requested ID and real peers.
			cli := client.New(target.opts.Addr, client.WithUid("unregistered-membership-observer"))
			require.NoError(t, cli.Start())
			t.Cleanup(cli.Stop)
			require.Eventually(t, cli.IsAuthed, 3*time.Second, 10*time.Millisecond)
			const unknownID uint64 = 1009
			request := &ClusterJoinReq{NodeId: unknownID, ServerAddr: advertised.Addr().String(), Role: nodetypes.NodeRole_NodeRoleReplica, Probe: true}
			payload, err := request.Marshal()
			require.NoError(t, err)
			read := func(t *testing.T, path string, body []byte) ClusterJoinResp {
				t.Helper()
				wire, err := cli.RequestWithContext(ctx, path, body)
				require.NoError(t, err)
				require.Equal(t, serverproto.StatusOK, wire.Status, string(wire.Body))
				var response ClusterJoinResp
				require.NoError(t, response.Unmarshal(wire.Body), "membership discovery must return a parseable response")
				return response
			}
			for _, version := range []int{1, 2} {
				t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
					path := fmt.Sprintf("/rpc/cluster/join/probe/v%d", version)
					for attempt := 1; attempt <= 2; attempt++ {
						response := read(t, path, payload)
						after := leader.cfgServer.Snapshot()
						t.Logf("path=%s attempt=%d target=%d leader=%d beforeVersion=%d afterVersion=%d unknown=%v learners=%v derivedVoters=%v joinVersion=%d",
							path, attempt, target.opts.ConfigOptions.NodeId, leader.opts.ConfigOptions.NodeId, before.Version, after.Version,
							leader.cfgServer.Node(unknownID), after.Learners, configuredProbeVoters(after), response.JoinVersion)
						assert.Zero(t, response.JoinVersion, "a probe must not claim a committed join")
						assert.Equal(t, before.Version, after.Version, "a probe must not append a membership command")
						assert.Nil(t, leader.cfgServer.Node(unknownID), "an unknown probe must not admit a learner")
						assert.Equal(t, before.Learners, after.Learners)
						assert.Equal(t, before.MigrateFrom, after.MigrateFrom)
						assert.Equal(t, before.MigrateTo, after.MigrateTo)
						assert.Equal(t, configuredProbeVoters(before), configuredProbeVoters(after))
						assert.True(t, proto.Equal(before, after), fmt.Sprintf("probe attempt %d changed the applied configuration", attempt))
						require.NotNil(t, response.Config, "absence must be established by the read-only member snapshot")
						assert.True(t, proto.Equal(before, response.Config))
						assert.Equal(t, leader.opts.ConfigOptions.NodeId, response.LeaderID)
					}
				})
			}

			// A separate explicit join is allowed to create a learner. Discovery
			// of that already accepted learner must preserve its original task.
			joined := read(t, "/rpc/cluster/join", payload)
			require.Greater(t, joined.JoinVersion, before.Version)
			pending := leader.cfgServer.Snapshot()
			require.Equal(t, []uint64{unknownID}, pending.Learners)
			require.Equal(t, nodetypes.NodeStatus_NodeStatusWillJoin, leader.cfgServer.Node(unknownID).Status)
			request.ServerAddr, request.Role = "", nodetypes.NodeRole_NodeRoleProxy
			stale, err := request.Marshal()
			require.NoError(t, err)
			for _, path := range []string{"/rpc/cluster/join/probe/v1", "/rpc/cluster/join/probe/v2"} {
				response := read(t, path, stale)
				assert.Zero(t, response.JoinVersion)
				assert.Equal(t, leader.opts.ConfigOptions.NodeId, response.LeaderID)
				assert.True(t, proto.Equal(pending, response.Config), "an accepted learner still receives its passive catch-up configuration")
				assert.True(t, proto.Equal(pending, leader.cfgServer.Snapshot()), "stale probe role/address must not rewrite the pending admission")
			}
		})
	}
}
