package raftgroup_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// The leader has one voting replica so these tests isolate proposal forwarding
// and the Apply boundary. Both ends use real Raft nodes and event loops. Replica
// catch-up and the API's channel/user-slot mismatch are covered by cluster smoke.
type proposalTransport struct {
	groups   map[uint64]*raftgroup.RaftGroup
	drop     bool
	requests atomic.Int32
}

func (p *proposalTransport) Send(key string, event types.Event) {
	if event.Type != types.SendPropose && event.Type != types.SendProposeResp {
		return
	}
	if event.Type == types.SendPropose {
		p.requests.Add(1)
	}
	if !p.drop {
		if group := p.groups[event.To]; group != nil {
			group.AddEvent(key, event)
			group.Advance()
		}
	}
}

type heldApplyStorage struct {
	*testStorage
	entered  chan []types.Log
	release  chan struct{}
	finished chan struct{}
	once     sync.Once
}

func (s *heldApplyStorage) Apply(_ string, logs []types.Log) error {
	s.entered <- append([]types.Log(nil), logs...)
	<-s.release
	s.finished <- struct{}{}
	return nil
}

func (s *heldApplyStorage) unblock() { s.once.Do(func() { close(s.release) }) }

func proposalGroup(t *testing.T, transport *proposalTransport, node raftgroup.IRaft, storage raftgroup.IStorage) *raftgroup.RaftGroup {
	t.Helper()
	g := raftgroup.New(raftgroup.NewOptions(raftgroup.WithTransport(transport), raftgroup.WithStorage(storage),
		raftgroup.WithTickInterval(time.Hour), raftgroup.WithProposeTimeout(time.Second)))
	g.AddRaft(node)
	transport.groups[node.NodeId()] = g
	return g
}

func startProposalGroups(t *testing.T, groups ...*raftgroup.RaftGroup) {
	t.Helper()
	for _, group := range groups {
		require.NoError(t, group.Start())
		t.Cleanup(group.Stop)
	}
}

func TestAsyncProposalForwardsBeforeApply(t *testing.T) {
	for _, role := range []string{"follower", "learner"} {
		for _, method := range []string{"Propose", "ProposeTimeout", "ProposeBatch", "ProposeBatchTimeout"} {
			t.Run(role+"/"+method, func(t *testing.T) {
				transport := &proposalTransport{groups: make(map[uint64]*raftgroup.RaftGroup)}
				storage := &heldApplyStorage{testStorage: newTestStorage(), entered: make(chan []types.Log, 1), release: make(chan struct{}), finished: make(chan struct{}, 1)}
				leader := newTestRaftNode("user-slot", 1, 0, types.RaftState{}, raft.WithReplicas([]uint64{1}))
				follower := newTestRaftNode("user-slot", 2, 0, types.RaftState{}, raft.WithReplicas([]uint64{1}))
				if role == "learner" {
					follower.BecomeLearner(1, 1)
				} else {
					follower.BecomeFollower(1, 1)
				}
				leaderGroup := proposalGroup(t, transport, leader, storage)
				followerGroup := proposalGroup(t, transport, follower, newTestStorage())
				startProposalGroups(t, leaderGroup, followerGroup)
				t.Cleanup(storage.unblock)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				reqs := types.ProposeReqSet{{Id: 101, Data: []byte("cleanup-a")}, {Id: 102, Data: []byte("cleanup-b")}}
				var resps types.ProposeRespSet
				var err error
				switch method {
				case "Propose", "ProposeTimeout":
					reqs = reqs[:1]
					var resp *types.ProposeResp
					if method == "Propose" {
						resp, err = followerGroup.Propose("user-slot", reqs[0].Id, reqs[0].Data)
					} else {
						resp, err = followerGroup.ProposeTimeout(ctx, "user-slot", reqs[0].Id, reqs[0].Data)
					}
					resps = types.ProposeRespSet{resp}
				case "ProposeBatch":
					resps, err = followerGroup.ProposeBatch("user-slot", reqs)
				case "ProposeBatchTimeout":
					resps, err = followerGroup.ProposeBatchTimeout(ctx, "user-slot", reqs)
				}
				require.NoError(t, err, "admission must finish while Apply is held")
				require.Equal(t, int32(1), transport.requests.Load())
				require.Len(t, resps, len(reqs))
				select {
				case logs := <-storage.entered:
					require.Len(t, logs, len(reqs), "leader must actually store and apply the submitted logs")
					for i, log := range logs {
						require.Equal(t, reqs[i].Id, log.Id)
						require.Equal(t, reqs[i].Data, log.Data)
						require.Equal(t, log.Index, resps[i].Index)
					}
				case <-time.After(time.Second):
					t.Fatal("leader never reached Apply")
				}
				storage.unblock()
				select {
				case <-storage.finished:
				case <-time.After(time.Second):
					t.Fatal("Apply did not complete after release")
				}
			})
		}
	}
}

type demotingNode struct{ *raft.Node }

func (n *demotingNode) Step(event types.Event) error {
	if event.Type == types.Propose {
		// Deterministically change role after all ingress checks, on the event
		// loop itself, rather than racing test-side writes with Raft.
		n.BecomeFollower(2, 3)
	}
	return n.Node.Step(event)
}

func TestAsyncProposalRejectsLeaderChangeBeforeStep(t *testing.T) {
	for _, remote := range []bool{false, true} {
		name := "local"
		if remote {
			name = "forwarded"
		}
		t.Run(name, func(t *testing.T) {
			transport := &proposalTransport{groups: make(map[uint64]*raftgroup.RaftGroup)}
			leader := &demotingNode{newTestRaftNode("slot", 1, 0, types.RaftState{}, raft.WithReplicas([]uint64{1}))}
			leaderGroup := proposalGroup(t, transport, leader, newTestStorage())
			follower := newTestRaftNode("slot", 2, 0, types.RaftState{}, raft.WithReplicas([]uint64{1}))
			follower.BecomeFollower(1, 1)
			followerGroup := proposalGroup(t, transport, follower, newTestStorage())
			startProposalGroups(t, leaderGroup, followerGroup)
			group := leaderGroup
			if remote {
				group = followerGroup
			}
			resp, err := group.Propose("slot", 1, []byte("cleanup"))
			require.Error(t, err)
			require.Nil(t, resp)
			if !remote {
				require.ErrorIs(t, err, types.ErrNotLeader)
			}
		})
	}
}

func TestAsyncProposalUnavailableLeader(t *testing.T) {
	transport := &proposalTransport{groups: make(map[uint64]*raftgroup.RaftGroup), drop: true}
	node := newTestRaftNode("slot", 2, 0, types.RaftState{}, raft.WithReplicas([]uint64{1}))
	group := proposalGroup(t, transport, node, newTestStorage())
	// No event loop is needed: unknown leaders and expired contexts must not
	// enqueue a local proposal, and the transport deliberately drops forwards.
	resp, err := group.Propose("slot", 1, []byte("cleanup"))
	require.ErrorIs(t, err, types.ErrNotLeader)
	require.Nil(t, resp)
	require.Zero(t, transport.requests.Load())
	node.BecomeFollower(1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = group.ProposeTimeout(ctx, "slot", 1, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, transport.requests.Load())
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_, err = group.ProposeTimeout(ctx, "slot", 1, []byte("cleanup"))
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		// Reusing the ID when no request was delivered also verifies the prior
		// timed-out waiter was removed (Wait.Register otherwise panics).
	}
	require.Equal(t, int32(2), transport.requests.Load())
	_, err = group.ProposeBatchTimeout(context.Background(), "slot", nil)
	require.Error(t, err)
	group.Stop()
	_, err = group.Propose("slot", 2, nil)
	require.ErrorIs(t, err, raftgroup.ErrGroupStopped)
}
