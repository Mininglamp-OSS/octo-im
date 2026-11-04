package event

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// joiningTestTransport connects actual configuration Raft servers without sockets.
type joiningTestTransport struct {
	servers       map[uint64]*clusterconfig.Server
	mu            sync.RWMutex
	beforePropose func(rafttypes.Event)
}

func (t *joiningTestTransport) Send(e rafttypes.Event) {
	if e.Type == rafttypes.SendPropose {
		t.mu.RLock()
		before := t.beforePropose
		t.mu.RUnlock()
		if before != nil {
			before(e)
		}
	}
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
	slots := s.cfgServer.Slots()
	require.Len(t, slots, len(before.Slots))
	for i, slot := range slots {
		require.True(t, slot.Equal(before.Slots[i]), "an existing assignment must remain unchanged")
	}
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

func TestHandleNodeJoiningRecoversDuringPendingMigration(t *testing.T) {
	for _, pending := range []string{"learner", "leader-transfer", "election"} {
		t.Run(pending, func(t *testing.T) {
			cfg := joiningTestConfig(true)
			cfg.Nodes[2].Status = types.NodeStatus_NodeStatusJoined
			cfg.Slots[0].Leader = 1002
			st := cfg.Slots[0]
			switch pending {
			case "learner":
				cfg.Nodes = append(cfg.Nodes, &types.Node{Id: 1004, Online: true, Status: types.NodeStatus_NodeStatusJoined})
				st.Learners, st.MigrateFrom, st.MigrateTo = []uint64{1004}, 1003, 1004
			case "leader-transfer":
				st.MigrateFrom, st.MigrateTo = 1002, 1003
			case "election":
				st.Status = types.SlotStatus_SlotStatusCandidate
			}
			s := newJoiningTestServer(t, cfg)
			require.NoError(t, s.handleNodeJoining())
			require.Equal(t, types.NodeStatus_NodeStatusJoined, s.cfgServer.Node(1002).Status)
			got := s.cfgServer.Slot(0)
			require.Equal(t, types.SlotStatus_SlotStatusCandidate, got.Status)
			require.Equal(t, uint64(1002), got.ExpectLeader)
			require.Equal(t, st.Learners, got.Learners, "recovery must retain the persisted migration")
			require.Equal(t, st.MigrateFrom, got.MigrateFrom)
			require.Equal(t, st.MigrateTo, got.MigrateTo)
			for _, slot := range s.cfgServer.Slots()[1:] {
				require.True(t, slot.Equal(cfg.Slots[slot.Id]), "do not schedule ordinary balancing while a task is pending")
			}
		})
	}
}

func TestNewNodeWaitsForPendingBalanceBeforeJoined(t *testing.T) {
	cfg := joiningTestConfig(true)
	for _, node := range cfg.Nodes {
		node.Status = types.NodeStatus_NodeStatusJoined
	}
	cfg.Nodes = append(cfg.Nodes,
		&types.Node{Id: 1004, Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusJoining},
		&types.Node{Id: 1005, Online: true, Status: types.NodeStatus_NodeStatusJoined})
	cfg.Slots[0].Learners = []uint64{1005}
	cfg.Slots[0].MigrateFrom, cfg.Slots[0].MigrateTo = 1003, 1005
	s := newJoiningTestServer(t, cfg)
	require.NoError(t, s.handleNodeJoining())
	require.Equal(t, types.NodeStatus_NodeStatusJoining, s.cfgServer.Node(1004).Status,
		"a new node must retain its pending balance instead of completing with no slots")
	completed := cfg.Slots[0].Clone()
	completed.Replicas = []uint64{1001, 1002, 1005}
	completed.Learners, completed.MigrateFrom, completed.MigrateTo = nil, 0, 0
	require.NoError(t, s.cfgServer.ProposeSlots([]*types.Slot{completed}))
	require.NoError(t, s.handleNodeJoining())
	require.Equal(t, types.NodeStatus_NodeStatusJoined, s.cfgServer.Node(1004).Status)
	assigned := 0
	for _, slot := range s.cfgServer.Slots() {
		if slot.MigrateTo == 1004 {
			assigned++
		}
	}
	require.Positive(t, assigned, "balancing must resume after the previous task completes")
}

func TestNewNodeWaitsForBalanceAfterDeadLearnerCleanup(t *testing.T) {
	cfg := joiningTestConfig(true)
	for _, node := range cfg.Nodes {
		node.Status = types.NodeStatus_NodeStatusJoined
	}
	cfg.Nodes = append(cfg.Nodes,
		&types.Node{Id: 1004, Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusJoining},
		&types.Node{Id: 1005, Online: false, Status: types.NodeStatus_NodeStatusJoined})
	cfg.Slots[0].Learners = []uint64{1005}
	cfg.Slots[0].MigrateFrom, cfg.Slots[0].MigrateTo = 1003, 1005
	s := newJoiningTestServer(t, cfg)
	require.NoError(t, s.handleNodeJoining())
	require.Empty(t, s.cfgServer.Slot(0).Learners)
	require.Zero(t, s.cfgServer.Slot(0).MigrateTo)
	require.Equal(t, types.NodeStatus_NodeStatusJoining, s.cfgServer.Node(1004).Status,
		"cleaning another node's dead learner is not this node's slot allocation")
	require.NoError(t, s.handleNodeJoining())
	require.Equal(t, types.NodeStatus_NodeStatusJoined, s.cfgServer.Node(1004).Status)
	assigned := 0
	for _, slot := range s.cfgServer.Slots() {
		if slot.MigrateTo == 1004 {
			assigned++
		}
	}
	require.Positive(t, assigned, "the next pass must allocate slots to the new node")
}

func TestReturningReplicaPrecedesNewNodeWaitingForBalance(t *testing.T) {
	cfg := joiningTestConfig(true)
	cfg.Nodes[2].Status = types.NodeStatus_NodeStatusJoined
	cfg.Nodes = append([]*types.Node{{Id: 1004, Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusJoining}}, cfg.Nodes...)
	cfg.Nodes = append(cfg.Nodes, &types.Node{Id: 1005, Online: true, Status: types.NodeStatus_NodeStatusJoined})
	cfg.Slots[0].Leader = 1002
	cfg.Slots[0].Learners = []uint64{1005}
	cfg.Slots[0].MigrateFrom, cfg.Slots[0].MigrateTo = 1003, 1005
	s := newJoiningTestServer(t, cfg)
	require.NoError(t, s.handleNodeJoining())
	require.Equal(t, types.NodeStatus_NodeStatusJoined, s.cfgServer.Node(1002).Status)
	require.Equal(t, types.NodeStatus_NodeStatusJoining, s.cfgServer.Node(1004).Status)
	got := s.cfgServer.Slot(0)
	require.Equal(t, types.SlotStatus_SlotStatusCandidate, got.Status)
	require.Equal(t, uint64(1002), got.ExpectLeader)
	require.Equal(t, cfg.Slots[0].Learners, got.Learners)
	require.Equal(t, cfg.Slots[0].MigrateFrom, got.MigrateFrom)
	require.Equal(t, cfg.Slots[0].MigrateTo, got.MigrateTo)
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

func TestReturningConfigurationLeaderMarksItselfOnline(t *testing.T) {
	cfg := joiningTestConfig(true)
	for _, n := range cfg.Nodes {
		n.Status = types.NodeStatus_NodeStatusJoined
	}
	base := newJoiningTestServer(t, cfg)
	leader := base.cfgOptions.NodeId
	require.NoError(t, base.cfgServer.ProposeNodeOnlineStatus(leader, false))
	require.False(t, base.cfgServer.NodeIsOnline(leader))
	s := NewServer(nil, base.cfgOptions, base.cfgServer)
	s.tick()
	require.True(t, base.cfgServer.NodeIsOnline(leader), "a leader cannot wait for a heartbeat from itself after restart")
}

func TestOfflinePromotedNodeDoesNotBlockNextAdmission(t *testing.T) {
	cfg := joiningTestConfig(false)
	cfg.Nodes[1].Online = false
	s := newJoiningTestServer(t, cfg)
	require.NoError(t, s.handleNodeJoining())
	require.Equal(t, types.NodeStatus_NodeStatusJoined, s.cfgServer.Node(1002).Status)
	require.False(t, s.cfgServer.NodeIsOnline(1002))
	for _, st := range s.cfgServer.Slots() {
		require.NotContains(t, st.Learners, uint64(1002))
		require.NotContains(t, st.Replicas, uint64(1002))
	}
}
