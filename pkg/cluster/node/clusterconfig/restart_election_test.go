package clusterconfig_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type restartElectionTransport struct {
	mu    sync.Mutex
	nodes map[uint64]*clusterconfig.Server
	votes map[uint64]map[uint64]int
}

func (tr *restartElectionTransport) Send(e types.Event) {
	tr.mu.Lock()
	s := tr.nodes[e.To]
	if e.Type == types.VoteReq {
		if tr.votes[e.From] == nil {
			tr.votes[e.From] = map[uint64]int{}
		}
		tr.votes[e.From][e.ConfigVersion]++
	}
	tr.mu.Unlock()
	if s != nil {
		s.StepRaftEvent(e)
	}
}
func (tr *restartElectionTransport) set(id uint64, s *clusterconfig.Server) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.nodes[id] = s
}

// Membership is unchanged. Only a normal API address log is applied before
// one follower restarts and the original leader becomes unavailable.
func TestRestartedVoterCanElectWithLivePeer(t *testing.T) {
	tr := &restartElectionTransport{nodes: map[uint64]*clusterconfig.Server{}, votes: map[uint64]map[uint64]int{}}
	nodes := map[uint64]*clusterconfig.Server{}
	active := map[uint64]bool{}
	opts := map[uint64]*clusterconfig.Options{}
	t.Cleanup(func() {
		for id, s := range nodes {
			if active[id] {
				tr.set(id, nil)
				s.Stop()
			}
		}
	})
	for id := uint64(1); id <= 3; id++ {
		opts[id] = newTestOptions(t, id, map[uint64]string{1: "", 2: "", 3: ""}, clusterconfig.WithTransport(tr))
		s := clusterconfig.New(opts[id])
		nodes[id] = s
		tr.set(id, s)
		require.NoError(t, s.Start())
		active[id] = true
	}
	var leaderID uint64
	require.Eventually(t, func() bool {
		for id, s := range nodes {
			if s.IsLeader() {
				leaderID = id
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	leader := nodes[leaderID]
	cfg := &pb.Config{Nodes: []*pb.Node{}}
	for id := uint64(1); id <= 3; id++ {
		cfg.Nodes = append(cfg.Nodes, &pb.Node{Id: id, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: pb.NodeStatus_NodeStatusJoined})
	}
	require.NoError(t, leader.ProposeConfig(cfg))
	require.NoError(t, leader.ProposeApiServerAddr(leaderID, "http://restart.example"))
	require.Eventually(t, func() bool {
		for _, s := range nodes {
			n := s.Node(leaderID)
			if n == nil || n.ApiServerAddr != "http://restart.example" {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	restartID := leaderID%3 + 1
	tr.set(restartID, nil)
	nodes[restartID].Stop()
	active[restartID] = false
	restarted := clusterconfig.New(opts[restartID])
	nodes[restartID] = restarted
	tr.set(restartID, restarted)
	require.NoError(t, restarted.Start())
	active[restartID] = true
	require.Eventually(t, func() bool { return restarted.LeaderId() == leaderID }, 5*time.Second, 20*time.Millisecond)
	tr.mu.Lock()
	tr.votes = map[uint64]map[uint64]int{}
	tr.mu.Unlock()
	tr.set(leaderID, nil)
	leader.Stop()
	active[leaderID] = false
	t.Logf("stopped leader=%d, restarted follower=%d, persisted application version=%d", leaderID, restartID, restarted.GetClusterConfig().Version)
	elected := false
	deadline := time.Now().Add(9 * time.Second)
	for time.Now().Before(deadline) {
		for id, s := range nodes {
			if active[id] && s.IsLeader() {
				elected = true
				break
			}
		}
		if elected {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	tr.mu.Lock()
	detail := fmt.Sprint(tr.votes)
	tr.mu.Unlock()
	t.Logf("surviving voters' outgoing election ConfigVersion counts: %s", detail)
	require.True(t, elected, "two live voters with identical membership must elect after one restarts")
}
