package event

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// joiningTestTransport connects actual configuration Raft servers without sockets.
type joiningTestTransport struct {
	servers map[uint64]*clusterconfig.Server
}

func (t *joiningTestTransport) Send(e rafttypes.Event) {
	if target := t.servers[e.To]; target != nil {
		target.StepRaftEvent(e)
	}
}

func TestHandleNodeJoiningCompletesWithoutSlotMigration(t *testing.T) {
	cfg := joiningTestConfig(true)
	s := newJoiningTestServer(t, cfg)
	before := s.cfgServer.GetClusterConfig().Clone()
	for range 2 {
		require.NoError(t, s.handleNodeJoining())
	}
	for _, node := range s.cfgServer.Nodes() {
		require.Equal(t, types.NodeStatus_NodeStatusJoined, node.Status, "node %d must complete rejoin", node.Id)
	}
	require.Equal(t, before.Slots, s.cfgServer.Slots(), "an existing assignment must remain unchanged")
	version := s.cfgServer.GetClusterConfig().Version
	require.NoError(t, s.handleNodeJoining())
	require.Equal(t, version, s.cfgServer.GetClusterConfig().Version, "completed joins must not generate more proposals")
}

func TestHandleNodeJoiningStillAllocatesMissingSlotReplicas(t *testing.T) {
	cfg := joiningTestConfig(false)
	s := newJoiningTestServer(t, cfg)
	require.NoError(t, s.handleNodeJoining())
	require.Equal(t, types.NodeStatus_NodeStatusJoined, s.cfgServer.Node(1002).Status)
	for _, slot := range s.cfgServer.Slots() {
		require.Equal(t, []uint64{1001, 1003}, slot.Replicas)
		require.Equal(t, []uint64{1002}, slot.Learners)
		require.Equal(t, uint64(1002), slot.MigrateFrom)
		require.Equal(t, uint64(1002), slot.MigrateTo)
	}
}

func TestHandleNodeJoiningReelectsInheritedSlotLeaders(t *testing.T) {
	cfg := joiningTestConfig(true)
	cfg.Slots[0].Leader = 1002
	s := newJoiningTestServer(t, cfg)
	require.NoError(t, s.handleNodeJoining())
	require.Equal(t, types.NodeStatus_NodeStatusJoined, s.cfgServer.Node(1002).Status)
	for _, slot := range s.cfgServer.Slots() {
		require.Equal(t, []uint64{1001, 1002, 1003}, slot.Replicas)
		require.Empty(t, slot.Learners)
		if slot.Id == 0 {
			require.Equal(t, types.SlotStatus_SlotStatusCandidate, slot.Status)
			require.Equal(t, slot.Leader, slot.ExpectLeader)
		} else {
			require.Equal(t, types.SlotStatus_SlotStatusNormal, slot.Status)
		}
	}
}

func joiningTestConfig(full bool) *types.Config {
	cfg := &types.Config{SlotCount: 64, SlotReplicaCount: 3, ChannelReplicaCount: 3}
	for _, id := range []uint64{1001, 1002, 1003} {
		status := types.NodeStatus_NodeStatusJoined
		if id == 1002 || full && id == 1003 {
			status = types.NodeStatus_NodeStatusJoining
		}
		cfg.Nodes = append(cfg.Nodes, &types.Node{Id: id, AllowVote: true, Online: true, Status: status})
	}
	for id := uint32(0); id < 64; id++ {
		replicas := []uint64{1001, 1003}
		if full {
			replicas = []uint64{1001, 1002, 1003}
		}
		cfg.Slots = append(cfg.Slots, &types.Slot{Id: id, Leader: 1001, Term: 1, Replicas: replicas})
	}
	return cfg
}

func newJoiningTestServer(t *testing.T, cfg *types.Config) *Server {
	t.Helper()
	transport := &joiningTestTransport{servers: make(map[uint64]*clusterconfig.Server)}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	for _, id := range []uint64{1001, 1002, 1003} {
		path := filepath.Join(t.TempDir(), "config.json")
		require.NoError(t, os.WriteFile(path, data, 0600))
		opts := clusterconfig.NewOptions(clusterconfig.WithNodeId(id), clusterconfig.WithConfigPath(path), clusterconfig.WithTransport(transport))
		transport.servers[id] = clusterconfig.New(opts)
	}
	for _, id := range []uint64{1001, 1002, 1003} {
		server := transport.servers[id]
		require.NoError(t, server.Start())
		t.Cleanup(server.Stop)
	}
	var leader *clusterconfig.Server
	require.Eventually(t, func() bool {
		for _, id := range []uint64{1001, 1002, 1003} {
			if transport.servers[id].IsLeader() {
				leader = transport.servers[id]
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	return &Server{cfgServer: leader, cfgOptions: leader.Options()}
}
