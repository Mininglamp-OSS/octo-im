package clusterconfig

import (
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestGHR24_7_ConcurrentSlotAndNodeSnapshotRaceSafety(t *testing.T) {
	cfg := NewConfig(NewOptions(WithConfigPath(t.TempDir() + "/cluster.json")))
	defer cfg.cfgFile.Close()

	cfg.addOrUpdateNode(&types.Node{
		Id:          1001,
		ClusterAddr: "127.0.0.1:11110",
		Status:      types.NodeStatus_NodeStatusJoined,
		AllowVote:   true,
		Online:      true,
	})
	cfg.addSlot(&types.Slot{
		Id:       1,
		Leader:   1001,
		Term:     1,
		Replicas: []uint64{1001, 1002},
		Status:   types.SlotStatus_SlotStatusNormal,
	})

	var wg sync.WaitGroup
	// Concurrent mutators modifying slot/node fields in-place under cfg.mu:
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			if i%2 == 0 {
				cfg.updateSlotStatus(1, types.SlotStatus_SlotStatusCandidate)
				cfg.updateSlotMigrate(1, 1001, 1003)
				cfg.updateNodeStatus(1001, types.NodeStatus_NodeStatusJoining)
			} else {
				cfg.updateSlotStatus(1, types.SlotStatus_SlotStatusNormal)
				cfg.updateNodeStatus(1001, types.NodeStatus_NodeStatusJoined)
			}
		}
	}()

	// Concurrent readers inspecting returned Slot / Node snapshots outside cfg.mu:
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				for _, st := range cfg.slots() {
					_ = st.Status
					_ = st.MigrateFrom
					_ = len(st.Learners)
				}
				if s1 := cfg.slot(1); s1 != nil {
					_ = s1.Status
					_ = len(s1.Replicas)
				}
				for _, nd := range cfg.nodes() {
					_ = nd.Status
					_ = nd.ClusterAddr
				}
			}
		}()
	}
	wg.Wait()
	require.NotNil(t, cfg.slot(1))
}
