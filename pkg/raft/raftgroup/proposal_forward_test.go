package raftgroup

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// R11: forwarded proposals between two real groups. Node 1 leads alone;
// node 2 follows it and replicates through the same transport.

type forwardTransport struct {
	mu     sync.Mutex
	groups map[uint64]*RaftGroup
	// holdResps keeps SendProposeResp events instead of delivering them.
	holdResps atomic.Bool
	held      []types.Event
	heldKeys  []string
	// forgeIndex, if set, rewrites the index of delivered propose responses.
	forgeIndex atomic.Uint64
}

func (tr *forwardTransport) Send(key string, e types.Event) {
	if e.Type == types.SendProposeResp {
		if idx := tr.forgeIndex.Load(); idx != 0 {
			var resps types.ProposeRespSet
			if err := resps.Unmarshal(e.Logs[0].Data); err == nil && len(resps) == 1 {
				resps[0].Index = idx
				data, _ := resps.Marshal()
				e.Logs = []types.Log{{Data: data}}
			}
		}
		if tr.holdResps.Load() {
			tr.mu.Lock()
			tr.held, tr.heldKeys = append(tr.held, e), append(tr.heldKeys, key)
			tr.mu.Unlock()
			return
		}
	}
	tr.mu.Lock()
	g := tr.groups[e.To]
	tr.mu.Unlock()
	if g != nil {
		g.AddEvent(key, e)
		g.Advance()
	}
}

func (tr *forwardTransport) release() {
	tr.mu.Lock()
	held, keys := tr.held, tr.heldKeys
	tr.held, tr.heldKeys = nil, nil
	tr.mu.Unlock()
	for i, e := range held {
		tr.groups[e.To].AddEvent(keys[i], e)
		tr.groups[e.To].Advance()
	}
}

type forwardPair struct {
	transport                      *forwardTransport
	leader, follower               *RaftGroup
	leaderStorage, followerStorage *memStorage
	leaderHook                     atomic.Pointer[func(uint64)]
}

func newForwardPair(t *testing.T, leaderProposeTimeout time.Duration) *forwardPair {
	t.Helper()
	fp := &forwardPair{transport: &forwardTransport{groups: map[uint64]*RaftGroup{}},
		leaderStorage: &memStorage{}, followerStorage: &memStorage{}}
	mk := func(id uint64, storage *memStorage, timeout time.Duration) *RaftGroup {
		g := New(NewOptions(WithTransport(fp.transport), WithStorage(storage), WithTickInterval(10*time.Millisecond),
			WithProposeTimeout(timeout)))
		node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(admKey), raft.WithNodeId(id), raft.WithReplicas([]uint64{1})))
		if id != 1 {
			node.BecomeFollower(1, 1)
		}
		g.AddRaft(node)
		fp.transport.groups[id] = g
		t.Cleanup(func() {
			g.Stop()
			g.goPool.Release()
		})
		return g
	}
	fp.leader = mk(1, fp.leaderStorage, leaderProposeTimeout)
	fp.follower = mk(2, fp.followerStorage, 5*time.Second)
	fp.leader.beforeAdmit = func(seq uint64) {
		if f := fp.leaderHook.Load(); f != nil {
			(*f)(seq)
		}
	}
	require.NoError(t, fp.leader.Start())
	require.NoError(t, fp.follower.Start())
	return fp
}

func (fp *forwardPair) setLeaderHook(f func(uint64)) { fp.leaderHook.Store(&f) }

func (fp *forwardPair) forwardUntilApplied(ctx context.Context, id uint64) (types.ProposeRespSet, error) {
	return fp.follower.ProposeBatchUntilAppliedTimeout(ctx, admKey, types.ProposeReqSet{{Id: id, Data: []byte("forwarded")}})
}

func TestForwardedProposalAppliedOnFollower(t *testing.T) {
	fp := newForwardPair(t, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resps, err := fp.forwardUntilApplied(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(1), resps[0].Index)
	require.Equal(t, []uint64{100}, fp.followerStorage.waitApplied(t, 1))
	require.Zero(t, fp.follower.wait.pending())
}

// R11 (a): the follower's caller gives up before the leader answers. The
// leader may have admitted it, so the outcome is unknown, not "not admitted".
func TestForwardedCallerTimeoutIsUnknown(t *testing.T) {
	fp := newForwardPair(t, 5*time.Second)
	fp.transport.holdResps.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := fp.forwardUntilApplied(ctx, 100)
	require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, types.ErrProposalNotAdmitted)
	require.Equal(t, []uint64{100}, fp.leaderStorage.waitApplied(t, 1), "the leader did admit it")
	fp.transport.release() // the late answer finds no waiter
	require.Zero(t, fp.follower.wait.pending())
}

// R11 (b): the entry is applied on the follower before its receipt exists.
// Success is decided by the stored Id, not by the applied index.
func TestForwardedAppliedBeforeRegistration(t *testing.T) {
	fp := newForwardPair(t, 5*time.Second)
	fp.transport.holdResps.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c := make(chan proposeResult, 1)
	go func() {
		resps, err := fp.forwardUntilApplied(ctx, 100)
		c <- proposeResult{resps, err}
	}()
	require.Equal(t, []uint64{100}, fp.followerStorage.waitApplied(t, 1))
	fp.transport.release()
	r := recv(t, c)
	require.NoError(t, r.err)
	require.Equal(t, uint64(1), r.resps[0].Index)
	require.Zero(t, fp.follower.wait.pending())
}

// R11 (b), negative: an already applied index holding another entry does not
// complete the forwarded receipt.
func TestForwardedAppliedIndexHoldsAnotherEntry(t *testing.T) {
	fp := newForwardPair(t, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := fp.forwardUntilApplied(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, []uint64{100}, fp.followerStorage.waitApplied(t, 1))

	fp.transport.holdResps.Store(true)
	fp.transport.forgeIndex.Store(1) // answer 200 with index 1, which holds 100
	c := make(chan proposeResult, 1)
	go func() {
		resps, err := fp.forwardUntilApplied(ctx, 200)
		c <- proposeResult{resps, err}
	}()
	require.Equal(t, []uint64{100, 200}, fp.followerStorage.waitApplied(t, 2))
	fp.transport.release()
	r := recv(t, c)
	require.ErrorIs(t, r.err, types.ErrProposalSuperseded)
	require.Zero(t, fp.follower.wait.pending())
}

// R11 (c): the leader's own deadline passes while it admits the proposal.
// Admission happened, so the follower gets ReasonOk and the right index.
func TestForwardedLeaderDeadlineAfterAdmission(t *testing.T) {
	fp := newForwardPair(t, 20*time.Millisecond)
	fp.setLeaderHook(func(uint64) { time.Sleep(60 * time.Millisecond) })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resps, err := fp.follower.ProposeBatchTimeout(ctx, admKey, types.ProposeReqSet{{Id: 100}})
	require.NoError(t, err)
	require.Equal(t, uint64(1), resps[0].Index)
	require.Equal(t, []uint64{100}, fp.leaderStorage.waitApplied(t, 1))
}

// R11 (d, e): the leader's deadline passes before admission. The leader
// answers ReasonError and never applies it; the follower reports
// ErrForwardRejected, which does not claim non-admission.
func TestForwardedLeaderDeadlineBeforeAdmission(t *testing.T) {
	fp := newForwardPair(t, 20*time.Millisecond)
	release := make(chan struct{})
	var once sync.Once
	fp.setLeaderHook(func(uint64) { once.Do(func() { <-release }) })
	// A local proposal holds the leader's admission semaphore.
	localC := make(chan error, 1)
	go func() {
		_, err := fp.leader.ProposeBatchTimeout(context.Background(), admKey, types.ProposeReqSet{{Id: 100}})
		localC <- err
	}()
	waitFor(t, "local admitting", func() bool { return len(fp.leader.admissionSem(admKey)) == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := fp.follower.ProposeBatchTimeout(ctx, admKey, types.ProposeReqSet{{Id: 200}})
	require.ErrorIs(t, err, types.ErrForwardRejected)
	require.NotErrorIs(t, err, types.ErrProposalNotAdmitted)
	require.NotErrorIs(t, err, types.ErrProposalOutcomeUnknown)
	close(release)
	require.NoError(t, <-localC)
	require.Equal(t, []uint64{100}, fp.leaderStorage.waitApplied(t, 1), "the rejected proposal is never applied")
	require.Equal(t, []uint64{100}, fp.leaderStorage.storedIDs())
}
