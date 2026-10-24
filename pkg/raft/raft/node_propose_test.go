package raft

import (
	"math/rand"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func newLeaderNode(t *testing.T, term uint32) *Node {
	t.Helper()
	n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2}), WithAdvance(func() {})))
	n.BecomeLeader(term)
	require.True(t, n.IsLeader())
	return n
}

func proposeEvent(term uint32, first uint64, ids ...uint64) types.Event {
	logs := make([]types.Log, 0, len(ids))
	for i, id := range ids {
		logs = append(logs, types.Log{Id: id, Term: term, Index: first + uint64(i), Data: []byte{byte(id)}})
	}
	return types.Event{Type: types.Propose, Logs: logs}
}

type queueSnapshot struct {
	logs                                 []types.Log
	storedIndex, lastLogIndex, committed uint64
}

func snapshotQueue(q *queue) queueSnapshot {
	return queueSnapshot{logs: append([]types.Log(nil), q.logs...), storedIndex: q.storedIndex,
		lastLogIndex: q.lastLogIndex, committed: q.committedIndex}
}

func queueIDs(q *queue) []uint64 {
	ids := make([]uint64, 0, len(q.logs))
	for _, l := range q.logs {
		ids = append(ids, l.Id)
	}
	return ids
}

// R15: strict compatibility mode for leader-local Propose.
func TestLeaderLocalProposeStrictAdmission(t *testing.T) {
	cases := []struct {
		name    string
		event   types.Event
		wantErr error
		overlap uint64
		stale   uint64
	}{
		{"full overlap", proposeEvent(2, 1, 900, 901), types.ErrLeaderLocalOverlap, 1, 0},
		{"repeat last entry", proposeEvent(2, 2, 900), types.ErrLeaderLocalOverlap, 1, 0},
		{"partial overlap", proposeEvent(2, 2, 900, 901), types.ErrLeaderLocalOverlap, 1, 0},
		{"gap", proposeEvent(2, 4, 900), types.ErrLeaderLocalOverlap, 1, 0},
		{"stale term", proposeEvent(1, 3, 900), types.ErrProposalStaleTerm, 0, 1},
		{"future term", proposeEvent(3, 3, 900), types.ErrProposalStaleTerm, 0, 1},
		{"mixed term in batch", types.Event{Type: types.Propose, Logs: []types.Log{
			{Id: 900, Term: 2, Index: 3}, {Id: 901, Term: 1, Index: 4}}}, types.ErrProposalStaleTerm, 0, 1},
		{"internal gap", types.Event{Type: types.Propose, Logs: []types.Log{
			{Id: 900, Term: 2, Index: 3}, {Id: 901, Term: 2, Index: 5}}}, types.ErrLeaderLocalOverlap, 1, 0},
		{"internal repeat", types.Event{Type: types.Propose, Logs: []types.Log{
			{Id: 900, Term: 2, Index: 3}, {Id: 901, Term: 2, Index: 3}}}, types.ErrLeaderLocalOverlap, 1, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newLeaderNode(t, 2)
			require.NoError(t, n.Step(proposeEvent(2, 1, 100, 101)), "exact next index and current term")
			before := snapshotQueue(n.queue)

			err := n.Step(tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, before, snapshotQueue(n.queue), "rejected proposal must leave the queue unchanged")
			overlap, stale := n.ProposeRejectCounts()
			require.Equal(t, tc.overlap, overlap)
			require.Equal(t, tc.stale, stale)

			// The leader keeps accepting the correct next proposal.
			require.NoError(t, n.Step(proposeEvent(2, 3, 102)))
			require.Equal(t, []uint64{100, 101, 102}, queueIDs(n.queue))
		})
	}
}

func TestLeaderLocalProposeEmptyIsNoop(t *testing.T) {
	n := newLeaderNode(t, 1)
	before := snapshotQueue(n.queue)
	require.NoError(t, n.Step(types.Event{Type: types.Propose}))
	require.Equal(t, before, snapshotQueue(n.queue))
	overlap, stale := n.ProposeRejectCounts()
	require.Zero(t, overlap+stale)
}

func TestLeaderLocalProposeNewProposeCompatibility(t *testing.T) {
	n := newLeaderNode(t, 1)
	first := n.NewPropose([]byte("a"))
	stale := n.NewPropose([]byte("b")) // built before first is admitted: same index
	require.NoError(t, n.Step(first))
	require.ErrorIs(t, n.Step(stale), types.ErrLeaderLocalOverlap)
	require.Equal(t, uint64(1), n.LastLogIndex())
	require.Equal(t, []byte("a"), n.queue.logs[0].Data)
	require.NoError(t, n.Step(n.NewPropose([]byte("c"))))
	require.Equal(t, uint64(2), n.LastLogIndex())

	// A proposal built in an earlier term is rejected after re-election.
	old := n.NewPropose([]byte("d"))
	n.BecomeLeader(2)
	require.ErrorIs(t, n.Step(old), types.ErrProposalStaleTerm)
	require.Equal(t, uint64(2), n.LastLogIndex())
}

// R14: an overlapping proposal after a StoreReq was issued neither truncates
// the in-memory queue nor rewinds what storage already holds.
func TestLeaderLocalProposeOverlapAfterStoreReq(t *testing.T) {
	n := newLeaderNode(t, 1)
	require.NoError(t, n.Step(proposeEvent(1, 1, 100, 101)))
	events := n.Ready()
	var store *types.Event
	for i := range events {
		if events[i].Type == types.StoreReq {
			store = &events[i]
		}
	}
	require.NotNil(t, store, "StoreReq issued for 1..2")
	require.Len(t, store.Logs, 2)
	require.True(t, n.queue.appending)
	before := snapshotQueue(n.queue)

	// Ghost-style overlap: a later caller computed index 2 before 1..2 was admitted.
	require.ErrorIs(t, n.Step(proposeEvent(1, 2, 200, 201)), types.ErrLeaderLocalOverlap)
	require.Equal(t, before, snapshotQueue(n.queue))
	require.True(t, n.queue.appending, "in-flight store state untouched")

	require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Index: 2, Reason: types.ReasonOk}))
	require.Equal(t, uint64(2), n.queue.storedIndex)
	require.Equal(t, uint64(2), n.LastLogIndex())

	// Overlap with already stored entries does not rewind storedIndex.
	require.ErrorIs(t, n.Step(proposeEvent(1, 1, 300)), types.ErrLeaderLocalOverlap)
	require.Equal(t, uint64(2), n.queue.storedIndex)
	require.Equal(t, uint64(2), n.LastLogIndex())
	require.NoError(t, n.Step(proposeEvent(1, 3, 102)))
	require.Equal(t, []uint64{102}, queueIDs(n.queue))
}

// R16: follower and learner catch-up keep the overlap/truncate semantics of
// queue.append and never go through the leader-local strict path.
func TestFollowerAndLearnerSyncRespUnchanged(t *testing.T) {
	for _, role := range []types.Role{types.RoleFollower, types.RoleLearner} {
		t.Run(role.String(), func(t *testing.T) {
			n := NewNode(0, types.RaftState{}, NewOptions(WithNodeId(1), WithAdvance(func() {})))
			if role == types.RoleFollower {
				n.BecomeFollower(1, 2)
			} else {
				n.BecomeLearner(1, 2)
			}
			sync := func(logs ...types.Log) error {
				return n.Step(types.Event{Type: types.SyncResp, From: 2, Term: 1, Reason: types.ReasonOk, Logs: logs})
			}
			require.NoError(t, sync(types.Log{Id: 1, Index: 1, Term: 1}, types.Log{Id: 2, Index: 2, Term: 1}, types.Log{Id: 3, Index: 3, Term: 1}))
			require.Equal(t, uint64(3), n.LastLogIndex())

			// Full overlap replay is accepted and ignored.
			require.NoError(t, sync(types.Log{Id: 2, Index: 2, Term: 1}, types.Log{Id: 3, Index: 3, Term: 1}))
			require.Equal(t, []uint64{1, 2, 3}, queueIDs(n.queue))

			// Partial overlap truncates and re-appends.
			require.NoError(t, sync(types.Log{Id: 22, Index: 2, Term: 1}, types.Log{Id: 33, Index: 3, Term: 1}, types.Log{Id: 44, Index: 4, Term: 1}))
			require.Equal(t, []uint64{1, 22, 33, 44}, queueIDs(n.queue))
			require.Equal(t, uint64(4), n.LastLogIndex())

			// A gap is still an error and changes nothing.
			require.Error(t, sync(types.Log{Id: 9, Index: 9, Term: 1}))
			require.Equal(t, uint64(4), n.LastLogIndex())

			// TruncateResp still truncates.
			require.NoError(t, n.Step(types.Event{Type: types.TruncateResp, From: 2, Term: 1, Index: 2, Reason: types.ReasonOk}))
			require.Equal(t, uint64(2), n.LastLogIndex())
			require.Equal(t, []uint64{1, 22}, queueIDs(n.queue))

			// Local proposals on a non-leader are still rejected as before.
			require.ErrorIs(t, n.Step(proposeEvent(1, 3, 500)), types.ErrNotLeader)

			overlap, stale := n.ProposeRejectCounts()
			require.Zero(t, overlap+stale, "strict leader-local path not used")
		})
	}
}

// Property: with proposers that allocate indexes from a possibly stale view of
// the log, accepted ranges never overlap, accepted entries are never replaced,
// and every success maps to the proposer's own entries.
func TestLeaderLocalProposeInterleavingProperty(t *testing.T) {
	const seed = 20301
	rng := rand.New(rand.NewSource(seed))
	n := newLeaderNode(t, 1)

	type accepted struct{ first, last, id uint64 }
	var (
		wins   []accepted
		views  []uint64 // lastLogIndex observed by proposers that have not stepped yet
		nextID uint64   = 1
		owner           = map[uint64]uint64{} // log index -> proposal id
	)
	for step := 0; step < 5000; step++ {
		switch op := rng.Intn(10); {
		case op < 4: // a proposer reads lastLogIndex (index allocation outside the loop)
			views = append(views, n.LastLogIndex())
		case op < 9 && len(views) > 0: // a proposer's event reaches the loop
			i := rng.Intn(len(views))
			view := views[i]
			views = append(views[:i], views[i+1:]...)
			id := nextID
			nextID++
			size := 1 + rng.Intn(3)
			logs := make([]types.Log, size)
			for k := range logs {
				logs[k] = types.Log{Id: id, Term: 1, Index: view + 1 + uint64(k)}
			}
			err := n.Step(types.Event{Type: types.Propose, Logs: logs})
			if err == nil {
				wins = append(wins, accepted{view + 1, view + uint64(size), id})
				for k := view + 1; k <= view+uint64(size); k++ {
					_, taken := owner[k]
					require.False(t, taken, "seed %d: index %d accepted twice", seed, k)
					owner[k] = id
				}
			} else {
				require.ErrorIs(t, err, types.ErrLeaderLocalOverlap, "seed %d", seed)
			}
		default: // storage catches up
			if n.queue.hasNextStoreLogs() {
				logs := n.queue.nextStoreLogs(0)
				require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Index: logs[len(logs)-1].Index, Reason: types.ReasonOk}))
			}
		}
		// Every unstored entry still belongs to the proposer that won its index.
		for _, l := range n.queue.logs {
			require.Equal(t, owner[l.Index], l.Id, "seed %d: index %d replaced", seed, l.Index)
		}
	}
	require.NotEmpty(t, wins)
	overlap, _ := n.ProposeRejectCounts()
	require.NotZero(t, overlap, "seed %d must exercise rejections", seed)
	require.Equal(t, uint64(len(owner)), n.LastLogIndex(), "accepted ranges are contiguous and disjoint")
	for _, w := range wins {
		for k := w.first; k <= w.last; k++ {
			require.Equal(t, w.id, owner[k])
		}
	}
}
