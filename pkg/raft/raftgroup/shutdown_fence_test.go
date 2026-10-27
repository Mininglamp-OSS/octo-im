package raftgroup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// fenceStorage counts storage calls so a test can assert that none happens
// once shutdown has begun.
type fenceStorage struct {
	*memStorage
	calls atomic.Int64
}

func (s *fenceStorage) AppendLogs(key string, logs []types.Log, termStart *types.TermStartIndexInfo) error {
	s.calls.Add(1)
	return s.memStorage.AppendLogs(key, logs, termStart)
}

func (s *fenceStorage) GetLogs(key string, start, end, limit uint64) ([]types.Log, error) {
	s.calls.Add(1)
	return s.memStorage.GetLogs(key, start, end, limit)
}

// probeNode records every IsLeader call, which a SendPropose pool task makes
// before anything else.
type probeNode struct {
	*raft.Node
	isLeaderCalls atomic.Int64
}

func (n *probeNode) IsLeader() bool {
	n.isLeaderCalls.Add(1)
	return n.Node.IsLeader()
}

// stepGateNode blocks Step of a ConfChange so a test can hold the event loop
// after it claimed a waited event.
type stepGateNode struct {
	*raft.Node
	entered chan struct{}
	release chan struct{}
}

func (n *stepGateNode) Step(e types.Event) error {
	if e.Type == types.ConfChange {
		close(n.entered)
		<-n.release
	}
	return n.Node.Step(e)
}

// blockedLoopGroup starts a group whose event loop is held inside the first
// SaveHardState, a synchronous storage write on the loop itself.
func blockedLoopGroup(t *testing.T, storage IStorage) (g *RaftGroup, node *probeNode, release func()) {
	t.Helper()
	entered, gate := make(chan struct{}), make(chan struct{})
	var saves atomic.Int64
	save := func(types.HardState) error {
		if saves.Add(1) == 1 {
			close(entered)
			<-gate
		}
		return nil
	}
	g = New(NewOptions(WithTransport(dropTransport{}), WithStorage(storage), WithTickInterval(time.Hour)))
	t.Cleanup(g.goPool.Release)
	node = &probeNode{Node: raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(admKey), raft.WithNodeId(1), raft.WithReplicas([]uint64{1}), raft.WithSaveHardState(save)))}
	require.True(t, node.Node.IsLeader())
	g.AddRaft(node)
	require.NoError(t, g.Start())
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("hard-state write not reached")
	}
	var once atomic.Bool
	release = func() {
		if once.CompareAndSwap(false, true) {
			close(gate)
		}
	}
	t.Cleanup(release)
	return g, node, release
}

func queuedEvents(g *RaftGroup) int {
	g.mq.mu.Lock()
	defer g.mq.mu.Unlock()
	return len(g.mq.nodrop)
}

func acceptedPoolTasks(g *RaftGroup) int {
	g.verificationMu.Lock()
	defer g.verificationMu.Unlock()
	return g.poolTasks
}

func quiesce(t *testing.T, g *RaftGroup) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, g.WaitQuiescence(ctx))
}

// An AddEventWait accepted into the queue while the loop is inside storage
// I/O is never processed once shutdown begins. Its caller must be settled
// with ErrGroupStopped, and the event must not be stepped afterwards.
func TestAddEventWaitQueuedWhileLoopBlockedSettlesOnStop(t *testing.T) {
	g, node, release := blockedLoopGroup(t, &memStorage{})
	before := node.Config()
	waitErr := make(chan error, 1)
	go func() {
		cfg := before
		cfg.Version += 100
		waitErr <- g.AddEventWait(admKey, types.Event{Type: types.ConfChange, Config: cfg})
	}()
	require.Eventually(t, func() bool { return queuedEvents(g) == 1 }, 5*time.Second, time.Millisecond)

	g.BeginStop()
	select {
	case err := <-waitErr:
		require.ErrorIs(t, err, ErrGroupStopped)
	case <-time.After(time.Second):
		t.Fatal("AddEventWait still blocked after shutdown began")
	}

	release()
	quiesce(t, g)
	require.Equal(t, before.Version, node.Config().Version, "an event reported as stopped must not be applied")
}

// Once shutdown has begun nothing will process the queue, so AddEventWait
// refuses without enqueueing.
func TestAddEventWaitAfterStopRefuses(t *testing.T) {
	g := New(NewOptions(WithTransport(dropTransport{}), WithStorage(&memStorage{}), WithTickInterval(time.Hour)))
	defer g.goPool.Release()
	require.NoError(t, g.Start())
	g.Stop()
	waitErr := make(chan error, 1)
	go func() { waitErr <- g.AddEventWait(admKey, types.Event{Type: types.ConfChange}) }()
	select {
	case err := <-waitErr:
		require.ErrorIs(t, err, ErrGroupStopped)
	case <-time.After(time.Second):
		t.Fatal("AddEventWait after Stop blocked")
	}
	require.Zero(t, queuedEvents(g))
}

// An event the loop already claimed keeps its actual result: shutdown must
// not report it as stopped while it is being, or has been, applied.
func TestAddEventWaitKeepsResultClaimedBeforeStop(t *testing.T) {
	g := New(NewOptions(WithTransport(dropTransport{}), WithStorage(&memStorage{}), WithTickInterval(time.Hour)))
	defer g.goPool.Release()
	node := &stepGateNode{
		Node:    raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(admKey), raft.WithNodeId(1))),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	g.AddRaft(node)
	require.NoError(t, g.Start())
	cfg := types.Config{Leader: 1, Term: 2, Version: 7, Role: types.RoleLeader, Replicas: []uint64{1}}
	waitErr := make(chan error, 1)
	go func() { waitErr <- g.AddEventWait(admKey, types.Event{Type: types.ConfChange, Config: cfg}) }()
	select {
	case <-node.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("ConfChange not stepped")
	}

	g.BeginStop()
	select {
	case err := <-waitErr:
		t.Fatalf("claimed event settled before its step finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(node.release)
	select {
	case err := <-waitErr:
		require.NoError(t, err, "the applied result wins over shutdown")
	case <-time.After(5 * time.Second):
		t.Fatal("claimed event never settled")
	}
	require.EqualValues(t, 7, node.Config().Version)
	quiesce(t, g)
}

// Shutdown fences every storage-task submission path as soon as it begins,
// before the event loop is joined: direct submission, SendPropose and receipt
// verification are refused, and no storage call happens after BeginStop.
func TestBeginStopFencesStorageSubmissionBeforeLoopJoin(t *testing.T) {
	storage := &fenceStorage{memStorage: &memStorage{}}
	g, node, release := blockedLoopGroup(t, storage)

	g.BeginStop()
	callsAtStop := storage.calls.Load()
	select {
	case <-g.loopDone:
		t.Fatal("lifecycle joined a loop that is blocked in storage")
	default:
	}

	var ran atomic.Bool
	require.ErrorIs(t, g.submit(func() { ran.Store(true) }), ErrGroupStopped, "submission accepted after shutdown began")

	data, err := types.ProposeReqSet{{Id: 1, Data: []byte("late")}}.Marshal()
	require.NoError(t, err)
	callsBefore := node.isLeaderCalls.Load()
	g.AddEvent(admKey, types.Event{Type: types.SendPropose, From: 2, Logs: []types.Log{{Data: data}}})
	require.Zero(t, acceptedPoolTasks(g), "SendPropose task accepted after shutdown began")
	require.Never(t, func() bool { return node.isLeaderCalls.Load() != callsBefore }, 100*time.Millisecond, 5*time.Millisecond,
		"SendPropose task ran after shutdown began")

	var reads atomic.Int64
	g.wait.readLog = func(string, uint64) (types.Log, bool, error) { reads.Add(1); return types.Log{}, false, nil }
	w, _ := g.wait.register(receipt{raftKey: admKey, seq: 99, first: 1, last: 1, logs: []types.Log{{Id: 7, Index: 1}}, forwarded: true})
	g.verifyReceiptAsync(w)
	select {
	case err := <-w.doneC:
		require.ErrorIs(t, err, ErrGroupStopped)
		require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
	case <-time.After(time.Second):
		t.Fatal("receipt verification after shutdown began did not settle")
	}

	// The loop still owns its in-flight write; the handoff stays incomplete.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	require.ErrorIs(t, g.WaitQuiescence(ctx), ErrIncompleteQuiescence)
	cancel()

	release()
	quiesce(t, g)
	g.Stop()
	require.False(t, ran.Load())
	require.Zero(t, reads.Load())
	require.Equal(t, callsAtStop, storage.calls.Load(), "storage touched after shutdown began")
	require.Zero(t, g.verificationWorkers.Load())
	require.Zero(t, g.wait.pending())
}

// A task accepted before the fence stays owned: Stop returns after joining the
// loop, but WaitQuiescence only succeeds once that task has left Storage.
func TestTaskAcceptedBeforeFenceIsJoinedByWaitQuiescence(t *testing.T) {
	g := New(NewOptions(WithTransport(dropTransport{}), WithStorage(&memStorage{}), WithTickInterval(time.Hour)))
	defer g.goPool.Release()
	require.NoError(t, g.Start())
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	require.NoError(t, g.submit(func() { close(entered); <-release; close(exited) }))
	<-entered
	g.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	err := g.WaitQuiescence(ctx)
	cancel()
	require.ErrorIs(t, err, ErrIncompleteQuiescence)
	require.ErrorContains(t, err, "storage workers still running")
	close(release)
	quiesce(t, g)
	select {
	case <-exited:
	default:
		t.Fatal("WaitQuiescence returned before the accepted task exited")
	}
}
