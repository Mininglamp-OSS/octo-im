package clusterconfig_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type expansionTransport struct {
	mu    sync.RWMutex
	peers map[uint64]*clusterconfig.Server
}

func (t *expansionTransport) Send(e rafttypes.Event) {
	t.mu.RLock()
	peer := t.peers[e.To]
	t.mu.RUnlock()
	if peer != nil {
		peer.StepRaftEvent(e)
	}
}
func (t *expansionTransport) set(id uint64, s *clusterconfig.Server) {
	t.mu.Lock()
	t.peers[id] = s
	t.mu.Unlock()
}

type expansionObserver struct {
	mu     sync.Mutex
	voters [][]uint64
}

func (o *expansionObserver) OnConfigChange(cfg *pb.Config) {
	var voters []uint64
	for _, n := range cfg.Nodes {
		learner := false
		for _, id := range cfg.Learners {
			learner = learner || id == n.Id
		}
		if n.AllowVote && !learner {
			voters = append(voters, n.Id)
		}
	}
	o.mu.Lock()
	o.voters = append(o.voters, voters)
	o.mu.Unlock()
}

func TestExpansionRetiresOfflineVoterBeforePromotion(t *testing.T) {
	for _, killLeader := range []bool{false, true} {
		t.Run(fmt.Sprintf("leader_failure_%t", killLeader), func(t *testing.T) {
			transport := &expansionTransport{peers: map[uint64]*clusterconfig.Server{}}
			servers := map[uint64]*clusterconfig.Server{}
			active := map[uint64]bool{}
			t.Cleanup(func() {
				for id, s := range servers {
					if active[id] {
						transport.set(id, nil)
						s.Stop()
					}
				}
			})
			for id := uint64(1); id <= 3; id++ {
				s := clusterconfig.New(newTestOptions(t, id, map[uint64]string{1: "", 2: "", 3: ""}, clusterconfig.WithTransport(transport)))
				servers[id], active[id] = s, true
				transport.set(id, s)
				require.NoError(t, s.Start())
			}
			var leader *clusterconfig.Server
			findLeader := func() bool {
				for id, s := range servers {
					if active[id] && s.IsLeader() {
						leader = s
						return true
					}
				}
				return false
			}
			require.Eventually(t, findLeader, 5*time.Second, 10*time.Millisecond)
			cfg := &pb.Config{SlotCount: 64, SlotReplicaCount: 3, ChannelReplicaCount: 3}
			for id := uint64(1); id <= 3; id++ {
				cfg.Nodes = append(cfg.Nodes, &pb.Node{Id: id, Online: true, AllowVote: true, Status: pb.NodeStatus_NodeStatusJoined})
			}
			require.NoError(t, leader.ProposeConfig(cfg))
			victim := leader.Options().NodeId
			if !killLeader {
				for id := uint64(1); id <= 3; id++ {
					if id != victim {
						victim = id
						break
					}
				}
			}
			transport.set(victim, nil)
			servers[victim].Stop()
			active[victim] = false
			require.Eventually(t, findLeader, 5*time.Second, 10*time.Millisecond)
			require.NoError(t, leader.ProposeNodeOnlineStatus(victim, false))
			observer := &expansionObserver{}
			leader.AddEventListener(observer)
			d := clusterconfig.New(newTestOptions(t, 4, nil, clusterconfig.WithTransport(transport), clusterconfig.WithSeed(fmt.Sprintf("%d@127.0.0.1:1", leader.Options().NodeId))))
			servers[4], active[4] = d, true
			transport.set(4, d)
			require.NoError(t, d.Start())
			joinVersion, err := leader.ProposeJoinWithVersion(&pb.Node{Id: 4, ClusterAddr: "127.0.0.1:11114", Online: true, AllowVote: true, Join: true, Status: pb.NodeStatus_NodeStatusWillJoin})
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				n := d.Snapshot()
				for _, node := range n.Nodes {
					if node.Id == 4 {
						return node.Status == pb.NodeStatus_NodeStatusJoining && len(n.Learners) == 0 && len(n.Nodes) == 3
					}
				}
				return false
			}, 8*time.Second, 10*time.Millisecond)
			require.Nil(t, leader.Node(victim))
			observer.mu.Lock()
			sawTwo := false
			for _, voters := range observer.voters {
				require.LessOrEqual(t, len(voters), 3)
				sawTwo = sawTwo || len(voters) == 2
			}
			observer.mu.Unlock()
			require.True(t, sawTwo, "removal must be applied before adding D's voting seat")
			logs, err := leader.GetLogsByLimit(joinVersion, 0, 100)
			require.NoError(t, err)
			var removed, promoted uint64
			for _, log := range logs {
				var cmd clusterconfig.CMD
				require.NoError(t, cmd.Unmarshal(log.Data))
				if cmd.CmdType == clusterconfig.CMDTypeNodeRemove {
					removed = log.Index
				}
				if cmd.CmdType == clusterconfig.CMDTypeNodeJoining {
					promoted = log.Index
				}
			}
			require.Greater(t, removed, joinVersion)
			require.Greater(t, promoted, removed)
			// Stop another original voter. The remaining original voter and D
			// must still commit a real write under the repaired three-member set.
			var second uint64
			for id := range servers {
				if active[id] && id != 4 && id != leader.Options().NodeId {
					second = id
					break
				}
			}
			require.NotZero(t, second)
			transport.set(second, nil)
			servers[second].Stop()
			active[second] = false
			require.NoError(t, leader.ProposeApiServerAddr(4, "http://after-recovery"))
			require.Eventually(t, func() bool {
				for _, n := range d.Snapshot().Nodes {
					if n.Id == 4 {
						return n.ApiServerAddr == "http://after-recovery"
					}
				}
				return false
			}, 3*time.Second, 10*time.Millisecond)
		})
	}
}
