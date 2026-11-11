package slot

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func persistedSlotGenerationNode(t *testing.T) *clusterconfig.Server {
	t.Helper()
	cfg := &nodetypes.Config{Version: 91, Term: 1, SlotCount: 2, SlotReplicaCount: 1,
		Nodes: []*nodetypes.Node{{Id: 1, Online: true, AllowVote: true, Status: nodetypes.NodeStatus_NodeStatusJoined}},
		Slots: []*nodetypes.Slot{{Id: 1, Term: 7, Leader: 1, Replicas: []uint64{1},
			Learners: []uint64{2}, MigrateFrom: 1, MigrateTo: 2, Generation: 37}}}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	configPath := filepath.Join(t.TempDir(), "cluster.json")
	require.NoError(t, os.WriteFile(configPath, data, 0600))
	node := clusterconfig.New(clusterconfig.NewOptions(clusterconfig.WithNodeId(1), clusterconfig.WithConfigPath(configPath)))
	require.NoError(t, node.Start())
	t.Cleanup(node.Stop)
	// No listener is registered: startup must recover the persisted slot's
	// generation before any versioned controller notification arrives.
	require.Equal(t, uint64(37), node.Slot(1).Generation)
	return node
}

func startSlotGenerationServer(t *testing.T, node *clusterconfig.Server, dataDir string) *Server {
	t.Helper()
	s := NewServer(NewOptions(WithNodeId(1), WithDataDir(dataDir), WithNode(node), WithSlotDbShardNum(1),
		WithTransport(&recoveryTestTransport{peers: map[uint64]*Server{}}),
		WithOnApply(func(uint32, []types.Log) error { return nil })))
	require.NoError(t, s.Start())
	return s
}

func TestSlotGenerationRestoredOnStartAndRestart(t *testing.T) {
	node := persistedSlotGenerationNode(t)
	dataDir := t.TempDir()
	s := startSlotGenerationServer(t, node, dataDir)
	stopped := false
	defer func() {
		if !stopped {
			s.Stop()
		}
	}()
	state := waitRuntimeSlot(t, s, 7, true)
	require.Equal(t, uint64(37), state.ConfigVersion)
	_, err := s.ProposeUntilApplied(1, []byte("persisted-before-restart"))
	require.NoError(t, err)
	s.Stop()
	stopped = true

	restarted := startSlotGenerationServer(t, node, dataDir)
	defer restarted.Stop()
	state = waitRuntimeSlot(t, restarted, 7, true)
	require.Equal(t, uint64(37), state.ConfigVersion)
	require.Equal(t, uint64(1), state.LeaderID)
	last, err := restarted.LastLog(1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), last.Index)
	require.Equal(t, []byte("persisted-before-restart"), last.Data)
	applied, err := restarted.AppliedIndex(1)
	require.NoError(t, err)
	require.Equal(t, last.Index, applied)

	// The reloaded active A->D assignment can consume a formal cancellation
	// and continue durable writes without another restart or term change.
	cancelled := node.Snapshot()
	cancelled.Version = 92
	cancelled.Slots[0].Generation = 92
	cancelled.Slots[0].Learners = nil
	cancelled.Slots[0].MigrateFrom, cancelled.Slots[0].MigrateTo = 0, 0
	restarted.OnConfigChange(cancelled)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		state, err := restarted.ReadLeaderState(ctx, 1)
		return err == nil && state.Ready && state.ConfigVersion == 92 && state.LeaderID == 1 && state.Term == 7
	}, time.Second, 5*time.Millisecond)
	_, err = restarted.ProposeUntilApplied(1, []byte("persisted-after-restart"))
	require.NoError(t, err)
	logs, err := restarted.GetLogsInReverseOrder(1, 1, 3, 0)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.Equal(t, []byte("persisted-after-restart"), logs[0].Data)
	require.Equal(t, last.Data, logs[1].Data)
}

func TestSlotGenerationRefreshesSameAssignmentOnly(t *testing.T) {
	node := persistedSlotGenerationNode(t)
	s := startSlotGenerationServer(t, node, t.TempDir())
	defer s.Stop()
	state := waitRuntimeSlot(t, s, 7, true)
	require.Equal(t, uint64(37), state.ConfigVersion)

	// A restarted task can have identical membership, leader and term. Its
	// higher generation must still install a new owner configuration.
	fresh := node.Snapshot()
	fresh.Version = 92
	fresh.Slots[0].Generation = 92
	s.OnConfigChange(fresh)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.Eventually(t, func() bool {
		state, err := s.ReadLeaderState(ctx, 1)
		return err == nil && state.Ready && state.ConfigVersion == 92 && state.LeaderID == 1 && state.Term == 7
	}, time.Second, 5*time.Millisecond)

	unrelated := fresh.Clone()
	unrelated.Version++
	unrelated.Nodes[0].ApiServerAddr = "http://127.0.0.1:15001"
	s.OnConfigChange(unrelated)
	// The following read runs after the owner's next event pass, including
	// any ConfChange incorrectly queued by the unrelated notification.
	require.NoError(t, s.raftGroup.Do(ctx, SlotIdToKey(1), func(raftgroup.IRaft) error { return nil }))
	state, err := s.ReadLeaderState(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(92), state.ConfigVersion)
	require.True(t, state.Ready)
	_, err = s.ProposeUntilApplied(1, []byte("same-task-after-global-refresh"))
	require.NoError(t, err)
	last, err := s.LastLog(1)
	require.NoError(t, err)
	require.Equal(t, []byte("same-task-after-global-refresh"), last.Data)
}
