package clusterconfig_test

import (
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type promotionSkewTransport struct {
	mu                       sync.Mutex
	nodes                    map[uint64]*clusterconfig.Server
	leader, lagging, ceiling uint64
	hold                     bool
	exchanges                int
}

func (tr *promotionSkewTransport) Send(e types.Event) {
	tr.mu.Lock()
	target := tr.nodes[e.To]
	if tr.nodes[e.From] == nil {
		target = nil
	}
	if tr.hold && e.From == tr.leader && e.To == tr.lagging {
		if e.Type == types.ConfigResp {
			target = nil
		}
		if e.Type == types.Ping || e.Type == types.SyncResp {
			e.CommittedIndex = min(e.CommittedIndex, tr.ceiling)
		}
	}
	if e.Type == types.ConfigResp && e.Reason == types.ReasonOnlySync {
		tr.exchanges++
	}
	tr.mu.Unlock()
	if target != nil {
		target.StepRaftEvent(e)
	}
}
func TestPromotedVoterCanElectBeforeOtherVoterAppliesPromotion(t *testing.T) {
	tr := &promotionSkewTransport{nodes: map[uint64]*clusterconfig.Server{}}
	nodes := map[uint64]*clusterconfig.Server{}
	active := map[uint64]bool{}
	t.Cleanup(func() {
		for id, s := range nodes {
			if active[id] {
				tr.mu.Lock()
				tr.nodes[id] = nil
				tr.mu.Unlock()
				s.Stop()
			}
		}
	})
	for id := uint64(1); id <= 3; id++ {
		init := map[uint64]string{1: "", 2: ""}
		opts := []clusterconfig.Option{clusterconfig.WithTransport(tr)}
		if id == 3 {
			init = nil
			opts = append(opts, clusterconfig.WithSeed("1@127.0.0.1:11110"))
		}
		s := clusterconfig.New(newTestOptions(t, id, init, opts...))
		nodes[id] = s
		tr.mu.Lock()
		tr.nodes[id] = s
		tr.mu.Unlock()
		require.NoError(t, s.Start())
		active[id] = true
	}
	var leaderID uint64
	require.Eventually(t, func() bool {
		for id, s := range nodes {
			if id != 3 && s.IsLeader() {
				leaderID = id
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	leader := nodes[leaderID]
	lagging := uint64(3) - leaderID
	cfg := &pb.Config{Learners: []uint64{3}}
	for id := uint64(1); id <= 3; id++ {
		status := pb.NodeStatus_NodeStatusJoined
		if id == 3 {
			status = pb.NodeStatus_NodeStatusJoining
		} // explicit promotion below; no automatic WillJoin callback
		cfg.Nodes = append(cfg.Nodes, &pb.Node{Id: id, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Status: status})
	}
	require.NoError(t, leader.ProposeConfig(cfg))
	require.Eventually(t, func() bool {
		for _, s := range nodes {
			applied, err := s.AppliedLogIndex()
			if err != nil || applied < 1 || len(s.GetClusterConfig().Learners) != 1 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	ceiling := leader.GetClusterConfig().Version
	tr.mu.Lock()
	tr.leader = leaderID
	tr.lagging = lagging
	tr.ceiling = ceiling
	tr.hold = true
	tr.mu.Unlock()
	require.NoError(t, leader.ProposeJoining(3))
	require.Eventually(t, func() bool {
		applied, err := nodes[3].AppliedLogIndex()
		return err == nil && applied > ceiling && len(nodes[3].GetClusterConfig().Learners) == 0
	}, 5*time.Second, 20*time.Millisecond)
	stored, err := nodes[lagging].LastLogIndex()
	require.NoError(t, err)
	require.Greater(t, stored, ceiling)
	applied, err := nodes[lagging].AppliedLogIndex()
	require.NoError(t, err)
	require.Equal(t, ceiling, applied)
	require.Equal(t, []uint64{3}, nodes[lagging].GetClusterConfig().Learners)
	tr.mu.Lock()
	tr.nodes[leaderID] = nil
	tr.mu.Unlock()
	leader.Stop()
	active[leaderID] = false
	t.Logf("leader %d stopped: voter %d stored promotion but applied=%d; promoted voter 3 applied it", leaderID, lagging, applied)
	var elected *clusterconfig.Server
	require.Eventually(t, func() bool {
		for id, s := range nodes {
			if active[id] && s.IsLeader() {
				elected = s
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, elected.ProposeApiServerAddr(3, "http://after-promotion.example"))
	require.Eventually(t, func() bool {
		for id, s := range nodes {
			if active[id] && (len(s.GetClusterConfig().Learners) != 0 || s.Node(3).ApiServerAddr != "http://after-promotion.example") {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	tr.mu.Lock()
	exchanges := tr.exchanges
	tr.mu.Unlock()
	require.Positive(t, exchanges, "membership must come from applied config, never from counting a learner vote")
}
