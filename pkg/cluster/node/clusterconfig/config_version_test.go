package clusterconfig

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func configVersionTestConfig(slotCount int) *pb.Config {
	cfg := &pb.Config{
		Version:   42,
		SlotCount: uint32(slotCount),
		Nodes: []*pb.Node{
			{Id: 1, ClusterAddr: "127.0.0.1:11110", Online: true, AllowVote: true},
			{Id: 2, ClusterAddr: "127.0.0.1:11111", Online: true, AllowVote: true},
			{Id: 3, ClusterAddr: "127.0.0.1:11112", Online: true, AllowVote: true},
		},
	}
	for i := 0; i < slotCount; i++ {
		cfg.Slots = append(cfg.Slots, &pb.Slot{
			Id: uint32(i), Leader: 1, Term: 1, Replicas: []uint64{1, 2, 3},
		})
	}
	return cfg
}

func TestConfigVersion(t *testing.T) {
	s := &Server{config: &Config{cfg: &pb.Config{}}}
	require.Zero(t, s.ConfigVersion())

	s.config.update(configVersionTestConfig(64))
	require.Equal(t, uint64(42), s.ConfigVersion())

	s.config.setVersionAndTerm(43, 7)
	require.Equal(t, uint64(43), s.ConfigVersion())
	require.Equal(t, uint32(7), s.config.term())

	s.config.versionInc()
	require.Equal(t, uint64(44), s.ConfigVersion())

	cfg := configVersionTestConfig(1024)
	cfg.Version = 45
	s.config.update(cfg)
	require.Equal(t, uint64(45), s.ConfigVersion())
}

func TestConfigVersionZeroAllocations(t *testing.T) {
	for _, slotCount := range []int{64, 1024} {
		t.Run(fmt.Sprintf("slots_%d", slotCount), func(t *testing.T) {
			s := &Server{config: &Config{cfg: configVersionTestConfig(slotCount)}}
			allocs := testing.AllocsPerRun(1000, func() {
				if s.ConfigVersion() != 42 {
					panic("unexpected configuration version")
				}
			})
			require.Zero(t, allocs)
		})
	}
}

func TestConfigVersionConcurrentUpdates(t *testing.T) {
	s := &Server{config: &Config{cfg: &pb.Config{}}}
	const updates = 1000
	stopReaders := readConfigVersionsConcurrently(t, s, updates)
	defer stopReaders()
	for version := uint64(1); version <= updates; version++ {
		if version%2 == 0 {
			s.config.update(&pb.Config{Version: version, SlotCount: 64})
		} else {
			s.config.setVersionAndTerm(version, 1)
		}
		runtime.Gosched()
	}
	require.Equal(t, uint64(updates), s.ConfigVersion())
}

func TestConfigVersionConcurrentApply(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/cluster.json")))
	t.Cleanup(func() { require.NoError(t, s.config.cfgFile.Close()) })
	s.config.update(&pb.Config{Nodes: []*pb.Node{{Id: 1}}})
	payload, err := EncodeApiServerAddrChange(1, "http://127.0.0.1:5001")
	require.NoError(t, err)
	data, err := NewCMD(CMDTypeConfigApiServerAddrChange, payload).Marshal()
	require.NoError(t, err)

	const updates = 100
	stopReaders := readConfigVersionsConcurrently(t, s, updates)
	defer stopReaders()
	for version := uint64(1); version <= updates; version++ {
		require.NoError(t, s.applyLog(rafttypes.Log{Index: version, Term: 7, Data: data}))
	}
	require.Equal(t, uint64(updates), s.ConfigVersion())
	require.Equal(t, uint32(7), s.config.term())
}

func readConfigVersionsConcurrently(t *testing.T, s *Server, maxVersion uint64) func() {
	t.Helper()
	done := make(chan struct{})
	var ready, readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		ready.Add(1)
		readers.Add(1)
		go func() {
			defer readers.Done()
			last := s.ConfigVersion()
			ready.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				version := s.ConfigVersion()
				if version < last || version > maxVersion {
					t.Errorf("configuration version changed from %d to %d (maximum %d)", last, version, maxVersion)
					return
				}
				last = version
			}
		}()
	}
	ready.Wait()
	return func() {
		close(done)
		readers.Wait()
	}
}

var configVersionBenchmarkResult uint64

func BenchmarkConfigVersion(b *testing.B) {
	for _, slotCount := range []int{64, 1024} {
		s := &Server{config: &Config{cfg: configVersionTestConfig(slotCount)}}
		b.Run(fmt.Sprintf("slots_%d/Version", slotCount), func(b *testing.B) {
			b.ReportAllocs()
			var version uint64
			for i := 0; i < b.N; i++ {
				version = s.ConfigVersion()
			}
			configVersionBenchmarkResult = version
		})
		b.Run(fmt.Sprintf("slots_%d/SnapshotClone", slotCount), func(b *testing.B) {
			b.ReportAllocs()
			var version uint64
			for i := 0; i < b.N; i++ {
				version = s.GetClusterConfig().Version
			}
			configVersionBenchmarkResult = version
		})
	}
}
