package raftgroup_test

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// The XIN-2020 index-reuse cases with C0-1 expectations. The event loop is not
// started until both proposals are queued, which deterministically models a
// slow loop: A is admitted to the queue but its caller times out, then B
// allocates the same index range from the unchanged LastLogIndex.
//
// C0-1 only guarantees B fails closed. A's queued ("ghost") entry is still
// applied even though A's caller saw a timeout; removing that is C0-2.

func failClosedSetup(t *testing.T) (*raftgroup.RaftGroup, *raft.Node, *testStorage) {
	storage := newTestStorage()
	g := raftgroup.New(raftgroup.NewOptions(raftgroup.WithTransport(&testTransport{groups: map[uint64]*raftgroup.RaftGroup{}}),
		raftgroup.WithStorage(storage), raftgroup.WithTickInterval(time.Hour), raftgroup.WithProposeTimeout(5*time.Second)))
	node := newTestRaftNode("slot-1", 1, 0, types.RaftState{}, raft.WithReplicas([]uint64{1}))
	g.AddRaft(node)
	require.True(t, node.IsLeader())
	return g, node, storage
}

func failClosedPropose(g *raftgroup.RaftGroup, ctx context.Context, ids ...uint64) (types.ProposeRespSet, error) {
	reqs := types.ProposeReqSet{}
	for _, id := range ids {
		reqs = append(reqs, types.ProposeReq{Id: id, Data: []byte{byte(id)}})
	}
	return g.ProposeBatchUntilAppliedTimeout(ctx, "slot-1", reqs)
}

func appliedIDs(s *testStorage, want int) []uint64 {
	var ids []uint64
	deadline := time.After(2 * time.Second)
	for len(ids) < want {
		select {
		case logs := <-s.applyC:
			for _, l := range logs {
				ids = append(ids, l.Id)
			}
		case <-deadline:
			return ids
		}
	}
	// Nothing else may be applied afterwards.
	select {
	case logs := <-s.applyC:
		for _, l := range logs {
			ids = append(ids, l.Id)
		}
	case <-time.After(200 * time.Millisecond):
	}
	return ids
}

func storedIDs(s *testStorage) []uint64 {
	s.Lock()
	defer s.Unlock()
	var ids []uint64
	for _, l := range s.logs["slot-1"] {
		ids = append(ids, l.Id)
	}
	return ids
}

func runStaleSecondProposal(t *testing.T, ghost, second []uint64) (*raftgroup.RaftGroup, *raft.Node, *testStorage, error) {
	g, node, storage := failClosedSetup(t)
	expired, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	_, errA := failClosedPropose(g, expired, ghost...)
	require.ErrorIs(t, errA, context.DeadlineExceeded)

	bC := make(chan error, 1)
	go func() {
		ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		resps, err := failClosedPropose(g, ctx, second...)
		if err == nil && len(resps) == 0 {
			t.Errorf("success without responses")
		}
		bC <- err
	}()
	time.Sleep(50 * time.Millisecond) // B has read LastLogIndex()==0 and queued index 1
	require.NoError(t, g.Start())
	return g, node, storage, <-bC
}

// R1: duplicate one-entry proposal after timeout.
func TestProposalFailClosedDuplicateIndex(t *testing.T) {
	g, node, storage, errB := runStaleSecondProposal(t, []uint64{100}, []uint64{200})
	defer g.Stop()
	require.ErrorIs(t, errB, types.ErrLeaderLocalOverlap, "B must not report success")
	require.Equal(t, []uint64{100}, appliedIDs(storage, 1), "ghost A applied at index 1; B never applied")
	require.Equal(t, []uint64{100}, storedIDs(storage))
	overlap, _ := node.ProposeRejectCounts()
	require.Equal(t, uint64(1), overlap)
}

// R2: partial overlap (A one entry, B two entries) no longer truncates A.
func TestProposalFailClosedPartialOverlap(t *testing.T) {
	g, _, storage, errB := runStaleSecondProposal(t, []uint64{100}, []uint64{200, 201})
	defer g.Stop()
	require.ErrorIs(t, errB, types.ErrLeaderLocalOverlap)
	require.Equal(t, []uint64{100}, appliedIDs(storage, 1), "A's entry is not truncated and B is not appended")
	require.Equal(t, []uint64{100}, storedIDs(storage))
}

// R3: a shorter second batch no longer succeeds on the first batch's data.
func TestProposalFailClosedShorterSecondBatch(t *testing.T) {
	g, _, storage, errB := runStaleSecondProposal(t, []uint64{100, 101}, []uint64{200})
	defer g.Stop()
	require.ErrorIs(t, errB, types.ErrLeaderLocalOverlap, "B must not be told index 1 holds its entry")
	require.Equal(t, []uint64{100, 101}, appliedIDs(storage, 2))
	require.Equal(t, []uint64{100, 101}, storedIDs(storage))
}

// After a rejection the next proposal is admitted at the correct index and its
// response maps to its own entry.
func TestProposalFailClosedNextProposalSucceeds(t *testing.T) {
	g, _, storage, errB := runStaleSecondProposal(t, []uint64{100}, []uint64{200})
	defer g.Stop()
	require.ErrorIs(t, errB, types.ErrLeaderLocalOverlap)
	require.Equal(t, []uint64{100}, appliedIDs(storage, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resps, err := failClosedPropose(g, ctx, 300)
	require.NoError(t, err)
	require.Equal(t, uint64(2), resps[0].Index)
	require.Equal(t, uint64(300), resps[0].Id)
	require.Equal(t, []uint64{300}, appliedIDs(storage, 1))
	require.Equal(t, []uint64{100, 300}, storedIDs(storage))
}
