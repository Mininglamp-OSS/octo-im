package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func newNodeVersionTestServer(t testing.TB, slotCount int) *Server {
	t.Helper()
	cfg := &types.Config{
		Version:   42,
		SlotCount: uint32(slotCount),
		Nodes:     []*types.Node{{Id: 1}, {Id: 2}, {Id: 3}},
	}
	for i := 0; i < slotCount; i++ {
		cfg.Slots = append(cfg.Slots, &types.Slot{
			Id: uint32(i), Leader: 1, Term: 1, Replicas: []uint64{1, 2, 3},
		})
	}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "cluster.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	return &Server{cfgServer: clusterconfig.New(clusterconfig.NewOptions(clusterconfig.WithConfigPath(path)))}
}

func TestNodeVersionZeroAllocations(t *testing.T) {
	for _, slotCount := range []int{64, 1024} {
		t.Run(fmt.Sprintf("slots_%d", slotCount), func(t *testing.T) {
			s := newNodeVersionTestServer(t, slotCount)
			require.Equal(t, uint64(42), s.NodeVersion())
			allocs := testing.AllocsPerRun(1000, func() {
				if s.NodeVersion() != 42 {
					panic("unexpected node version")
				}
			})
			require.Zero(t, allocs)
		})
	}
}

var nodeVersionBenchmarkResult uint64

func BenchmarkNodeVersion(b *testing.B) {
	for _, slotCount := range []int{64, 1024} {
		b.Run(fmt.Sprintf("slots_%d", slotCount), func(b *testing.B) {
			s := newNodeVersionTestServer(b, slotCount)
			b.ReportAllocs()
			b.ResetTimer()
			var version uint64
			for i := 0; i < b.N; i++ {
				version = s.NodeVersion()
			}
			nodeVersionBenchmarkResult = version
		})
	}
}
