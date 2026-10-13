package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func admitLogs(ids ...uint64) []types.Log {
	logs := make([]types.Log, 0, len(ids))
	for _, id := range ids {
		logs = append(logs, types.Log{Id: id, Data: []byte{byte(id)}})
	}
	return logs
}

func fences(n *Node) []uint32 {
	var terms []uint32
	for _, e := range n.events {
		if e.Type == types.ProposalFence {
			terms = append(terms, e.Term)
		}
	}
	return terms
}

// Admit assigns the next contiguous indexes and the current term and returns
// that range; successive admissions never share an index.
func TestAdmitAssignsContiguousRange(t *testing.T) {
	n := newLeaderNode(t, 3)
	first := admitLogs(100, 101)
	adm, err := n.Admit(first)
	require.NoError(t, err)
	require.Equal(t, types.Admission{Term: 3, FirstIndex: 1, LastIndex: 2}, adm)
	require.Equal(t, uint64(1), first[0].Index)
	require.Equal(t, uint32(3), first[1].Term)

	adm, err = n.Admit(admitLogs(200))
	require.NoError(t, err)
	require.Equal(t, types.Admission{Term: 3, FirstIndex: 3, LastIndex: 3}, adm)
	require.Equal(t, []uint64{100, 101, 200}, queueIDs(n.queue))
	overlap, staleTerm := n.ProposeRejectCounts()
	require.Zero(t, overlap)
	require.Zero(t, staleTerm)
}

func TestAdmitRejectsWithoutChangingQueue(t *testing.T) {
	unconfigured := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1)))
	_, err := unconfigured.Admit(admitLogs(1))
	require.ErrorIs(t, err, types.ErrNotLeader)

	follower := newLeaderNode(t, 1)
	follower.BecomeFollower(2, 2)
	_, err = follower.Admit(admitLogs(1))
	require.ErrorIs(t, err, types.ErrNotLeader)
	require.Zero(t, follower.queue.lastLogIndex)

	stopped := newLeaderNode(t, 1)
	stopped.stopPropose = true
	_, err = stopped.Admit(admitLogs(1))
	require.ErrorIs(t, err, types.ErrProposalDropped)
	require.Zero(t, stopped.queue.lastLogIndex)

	_, err = newLeaderNode(t, 1).Admit(nil)
	require.Error(t, err)
}

// A fence is emitted exactly when the node stops leading a term.
func TestProposalFenceOnLeadershipEnd(t *testing.T) {
	n := newLeaderNode(t, 1)
	require.Empty(t, fences(n), "becoming leader ends no led term")
	n.BecomeLeader(1)
	require.Empty(t, fences(n), "still leading the same term")

	n.BecomeFollower(2, 2)
	require.Equal(t, []uint32{1}, fences(n))
	n.BecomeFollower(3, 2)
	require.Equal(t, []uint32{1}, fences(n), "a follower led no term")

	n = newLeaderNode(t, 1)
	n.BecomeLeader(2)
	require.Equal(t, []uint32{1}, fences(n), "the led term changed")

	n = newLeaderNode(t, 4)
	n.BecomeLearner(4, 2)
	require.Equal(t, []uint32{4}, fences(n))
}

// A configuration naming another leader fences even when the role handler
// is kept.
func TestProposalFenceOnConfigLeaderChange(t *testing.T) {
	n := newLeaderNode(t, 1)
	cfg := n.cfg.Clone()
	cfg.Version++
	cfg.Leader = 2
	cfg.Role = types.RoleUnknown
	require.NoError(t, n.switchConfig(cfg))
	require.False(t, n.IsLeader())
	require.Contains(t, fences(n), uint32(1))

	n = newLeaderNode(t, 1)
	cfg = n.cfg.Clone()
	cfg.Version++
	require.NoError(t, n.switchConfig(cfg))
	require.Empty(t, fences(n), "same leader and term")
}
