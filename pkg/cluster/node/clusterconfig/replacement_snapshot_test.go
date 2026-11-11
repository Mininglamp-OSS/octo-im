package clusterconfig

import (
	"runtime"
	"sync"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestReplacementJoinPublishesCoherentSnapshots(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/config.json")))
	defer s.config.cfgFile.Close()
	s.config.cfg.Nodes = []*pb.Node{
		{Id: 1, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
		{Id: 2, AllowVote: true, OfflineCount: 1, Status: pb.NodeStatus_NodeStatusJoined},
		{Id: 3, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
	}
	prior := s.GetClusterConfig()
	bad := make(chan string, 1)
	report := func(reason string) {
		select {
		case bad <- reason:
		default:
		}
	}
	stop, started := make(chan struct{}), make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
			}
			cfg := s.GetClusterConfig()
			var joined bool
			for _, node := range cfg.Nodes {
				joined = joined || node.Id == 4
			}
			if len(cfg.Learners) > 0 {
				if len(cfg.Learners) != 1 || cfg.Learners[0] != 4 || !joined || cfg.MigrateTo != 4 || (cfg.MigrateFrom != 2 && cfg.MigrateFrom != 4) {
					report("snapshot has partially published replacement join")
				}
			} else if cfg.MigrateFrom != 0 || cfg.MigrateTo != 0 {
				report("snapshot has migration markers without learner")
			}
			runtime.Gosched()
		}
	}()
	<-started
	for i := 0; i < 500; i++ {
		s.config.joinNode(&pb.Node{Id: 4, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusWillJoin}, true)
		runtime.Gosched()
		s.config.updateNodeJoining(4, true)
	}
	close(stop)
	readers.Wait()
	select {
	case reason := <-bad:
		t.Fatal(reason)
	default:
	}
	require.Len(t, prior.Nodes, 3)
	require.True(t, prior.Nodes[1].AllowVote, "snapshots predating replacement remain unchanged")
	require.Empty(t, prior.Learners)
	require.False(t, s.Node(2).AllowVote)
	require.ElementsMatch(t, []uint64{1, 3, 4}, s.configToRaftConfig(s.config).Replicas)
}
