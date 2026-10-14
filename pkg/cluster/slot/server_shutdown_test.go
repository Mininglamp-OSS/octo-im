package slot

import (
	"context"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// verifyGateStorage wraps the real Pebble storage. When armed it blocks the
// receipt-verification point read (one index, no size limit) and records
// whether any storage call overlapped or followed Close.
type verifyGateStorage struct {
	*PebbleShardLogStorage
	armed    atomic.Bool
	entered  chan struct{}
	release  chan struct{}
	inflight atomic.Int64
	closed   atomic.Bool
	reads    atomic.Int64
	applied  chan struct{}
	once     sync.Once
	mu       sync.Mutex
	errs     []error
	late     []string
}

func (s *verifyGateStorage) track(op string) func(error) {
	s.inflight.Add(1)
	if s.closed.Load() {
		s.note("started after close: " + op)
	}
	return func(err error) {
		if s.closed.Load() {
			s.note("finished after close: " + op)
		}
		if err != nil {
			s.mu.Lock()
			s.errs = append(s.errs, err)
			s.mu.Unlock()
		}
		s.inflight.Add(-1)
	}
}

func (s *verifyGateStorage) note(v string) {
	s.mu.Lock()
	s.late = append(s.late, v)
	s.mu.Unlock()
}

func (s *verifyGateStorage) GetLogs(key string, start, end, limit uint64) ([]types.Log, error) {
	done := s.track("GetLogs")
	if limit == 0 && end == start+1 && s.armed.CompareAndSwap(true, false) {
		s.reads.Add(1)
		close(s.entered)
		<-s.release
	}
	logs, err := s.PebbleShardLogStorage.GetLogs(key, start, end, limit)
	done(err)
	return logs, err
}

func (s *verifyGateStorage) AppendLogs(key string, logs []types.Log, info *types.TermStartIndexInfo) error {
	done := s.track("AppendLogs")
	err := s.PebbleShardLogStorage.AppendLogs(key, logs, info)
	done(err)
	return err
}

func (s *verifyGateStorage) Apply(key string, logs []types.Log) error {
	done := s.track("Apply")
	err := s.PebbleShardLogStorage.Apply(key, logs)
	done(err)
	s.once.Do(func() { close(s.applied) })
	return err
}

type slotPairTransport struct {
	mu     sync.Mutex
	groups map[uint64]*raftgroup.RaftGroup
	hold   atomic.Bool
	held   []types.Event
}

func (tr *slotPairTransport) Send(key string, e types.Event) {
	tr.mu.Lock()
	if e.Type == types.SendProposeResp && tr.hold.Load() {
		tr.held = append(tr.held, e)
		tr.mu.Unlock()
		return
	}
	g := tr.groups[e.To]
	tr.mu.Unlock()
	if g != nil {
		g.AddEvent(key, e)
		g.Advance()
	}
}

func (tr *slotPairTransport) releaseHeld() {
	tr.mu.Lock()
	held := tr.held
	tr.held = nil
	tr.hold.Store(false)
	tr.mu.Unlock()
	for _, e := range held {
		tr.groups[e.To].AddEvent("1", e)
		tr.groups[e.To].Advance()
	}
}

// newOwnedSlotServer builds a production slot Server whose RaftGroup reads
// and writes the Server-owned Pebble storage through the gate wrapper.
func newOwnedSlotServer(t *testing.T, id uint64, tr *slotPairTransport) (*Server, *verifyGateStorage) {
	s := NewServer(NewOptions(WithNodeId(id), WithDataDir(t.TempDir()), WithSlotDbShardNum(1)))
	require.NoError(t, s.storage.Open())
	gate := &verifyGateStorage{PebbleShardLogStorage: s.storage, entered: make(chan struct{}), release: make(chan struct{}), applied: make(chan struct{})}
	s.raftGroup = raftgroup.New(raftgroup.NewOptions(raftgroup.WithLogPrefix("slot"), raftgroup.WithStorage(gate), raftgroup.WithTransport(tr),
		raftgroup.WithTickInterval(10*time.Millisecond), raftgroup.WithProposeTimeout(5*time.Second)))
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey("1"), raft.WithNodeId(id), raft.WithReplicas([]uint64{1})))
	if id != 1 {
		node.BecomeFollower(1, 1)
	}
	s.raftGroup.AddRaft(node)
	tr.mu.Lock()
	tr.groups[id] = s.raftGroup
	tr.mu.Unlock()
	require.NoError(t, s.raftGroup.Start())
	return s, gate
}

// A forwarded receipt verification read is blocked inside the real slot owner.
// Stop settles the caller promptly; the bounded handoff times out and keeps
// Pebble open; the late read completes against open storage without
// replacing the caller's outcome; only then does the owner close storage.
func TestSlotServerStopJoinsVerificationBeforeClose(t *testing.T) {
	tr := &slotPairTransport{groups: map[uint64]*raftgroup.RaftGroup{}}
	leader, leaderGate := newOwnedSlotServer(t, 1, tr)
	follower, gate := newOwnedSlotServer(t, 2, tr)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, follower.StopWithContext(ctx))
		require.NoError(t, leader.StopWithContext(ctx))
	})

	// Hold the leader response until the follower has applied, so receipt
	// registration dispatches the asynchronous verification point read.
	tr.hold.Store(true)
	gate.armed.Store(true)
	type result struct {
		resps types.ProposeRespSet
		err   error
	}
	out := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resps, err := follower.raftGroup.ProposeBatchUntilAppliedTimeout(ctx, "1", types.ProposeReqSet{{Id: 3100, Data: []byte("owner")}})
		out <- result{resps, err}
	}()
	select {
	case <-gate.applied:
	case <-time.After(5 * time.Second):
		t.Fatal("follower apply not reached")
	}
	// didApply follows Storage.Apply in the same pool task; same settle
	// margin as the raftgroup waitApplied fixture.
	time.Sleep(50 * time.Millisecond)
	tr.releaseHeld()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("verification read not reached")
	}

	var err error
	for i := 0; i < 2; i++ {
		short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		started := time.Now()
		err = follower.StopWithContext(short)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded, "the handoff must time out while the read is blocked")
		t.Logf("bounded handoff %d returned after %s: %v", i+1, time.Since(started), err)
	}
	var r result
	select {
	case r = <-out:
	case <-time.After(time.Second):
		t.Fatal("caller was not settled by Stop")
	}
	require.ErrorIs(t, r.err, types.ErrProposalOutcomeUnknown)
	require.ErrorIs(t, r.err, raftgroup.ErrGroupStopped)
	require.False(t, gate.closed.Load())
	// Ownership is retained: storage is still open and usable.
	_, err = follower.storage.GetLogs("1", 1, 2, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, gate.inflight.Load(), "only the blocked verification read remains")

	release()
	ctx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	require.NoError(t, follower.StopWithContext(ctx))
	require.Zero(t, gate.inflight.Load())
	gate.closed.Store(true)
	require.NoError(t, follower.StopWithContext(ctx), "repeated handoff keeps its result")
	require.NoError(t, leader.StopWithContext(ctx))
	leaderGate.closed.Store(true)
	require.Zero(t, leaderGate.inflight.Load())

	require.EqualValues(t, 1, gate.reads.Load())
	gate.mu.Lock()
	require.Empty(t, gate.errs, "no storage call may observe a closed Pebble")
	require.Empty(t, gate.late)
	gate.mu.Unlock()
	ok, notAdmitted, unknown, superseded := follower.raftGroup.ProposalOutcomeCounts()
	require.Equal(t, []uint64{0, 0, 1, 0}, []uint64{ok, notAdmitted, unknown, superseded}, "the late read cannot replace the outcome")
	t.Logf("cleanup: inflight storage calls=0 verification reads=%d caller=%v", gate.reads.Load(), r.err)
}

// hardStateGate blocks the first hard-state write of a node on the event loop,
// the synchronous storage write that is not a pool task.
type hardStateGate struct {
	entered, release chan struct{}
	calls            atomic.Int64
}

func newHardStateGate() *hardStateGate {
	return &hardStateGate{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *hardStateGate) wrap(save func(types.HardState) error) func(types.HardState) error {
	return func(st types.HardState) error {
		if g.calls.Add(1) == 1 {
			close(g.entered)
			<-g.release
		}
		return save(st)
	}
}

// The production slot owner with its event loop blocked in a Pebble hard-state
// write: a 30 ms handoff returns at its deadline and keeps Pebble open and
// owned; after the write returns, a later call finishes the same lifecycle,
// which closes Pebble exactly once with the write persisted.
func TestSlotServerStopBoundsBlockedHardState(t *testing.T) {
	dir := t.TempDir()
	s := NewServer(NewOptions(WithNodeId(1), WithDataDir(dir), WithSlotDbShardNum(1)))
	require.NoError(t, s.storage.Open())
	gate := newHardStateGate()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(release)
	save := gate.wrap(func(st types.HardState) error { return s.storage.SaveHardState("1", st) })
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey("1"), raft.WithNodeId(1), raft.WithReplicas([]uint64{1}), raft.WithSaveHardState(save)))
	s.raftGroup.AddRaft(node)
	require.NoError(t, s.raftGroup.Start())
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("hard-state write not reached")
	}

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		started := time.Now()
		err := s.StopWithContext(ctx)
		elapsed := time.Since(started)
		cancel()
		require.ErrorIs(t, err, raftgroup.ErrIncompleteQuiescence)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, time.Second, "the handoff must return at its deadline, not when the write returns")
		t.Logf("bounded handoff %d returned after %s: %v", i+1, elapsed, err)
	}
	// Ownership is retained: storage is still open and usable.
	_, err := s.storage.GetState("1")
	require.NoError(t, err)
	select {
	case <-s.stopDone:
		t.Fatal("storage closed while the event loop was inside it")
	default:
	}

	release()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.StopWithContext(ctx))
	require.NoError(t, s.StopWithContext(ctx), "repeated handoff keeps its result")
	require.EqualValues(t, 1, gate.calls.Load())

	// Close released the Pebble lock, and the blocked write was persisted.
	reopened := NewPebbleShardLogStorage(s, path.Join(dir, "logdb"), 1)
	require.NoError(t, reopened.Open())
	defer func() { require.NoError(t, reopened.Close()) }()
	st, err := reopened.hardState("1")
	require.NoError(t, err)
	require.EqualValues(t, 1, st.Term)
}
