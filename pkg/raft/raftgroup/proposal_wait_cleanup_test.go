package raftgroup

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// Isolate waiter ownership from replication and storage. The proposal still
// enters the real event queue and uses ProposeBatchUntilAppliedTimeout.
type waitTestRaft struct {
	IRaft
	mu sync.Mutex
}

func (*waitTestRaft) Key() string          { return "slot" }
func (*waitTestRaft) IsLeader() bool       { return true }
func (*waitTestRaft) LeaderId() uint64     { return 1 }
func (*waitTestRaft) AppliedIndex() uint64 { return 0 }
func (*waitTestRaft) LastLogIndex() uint64 { return 0 }
func (*waitTestRaft) LastTerm() uint32     { return 1 }
func (*waitTestRaft) KeepAlive()           {}
func (r *waitTestRaft) Lock()              { r.mu.Lock() }
func (r *waitTestRaft) Unlock()            { r.mu.Unlock() }

func newWaitTestGroup(t *testing.T) *RaftGroup {
	t.Helper()
	g := New(NewOptions())
	g.AddRaft(&waitTestRaft{})
	t.Cleanup(func() {
		select {
		case <-g.stopper.ShouldStop():
		default:
			g.Stop()
		}
		g.goPool.Release()
	})
	return g
}

func admitWaitTestProposal(t *testing.T, g *RaftGroup) {
	t.Helper()
	select {
	case <-g.advanceC:
	case <-time.After(time.Second):
		t.Fatal("proposal did not enter event queue")
	}
	events := g.mq.Get()
	if len(events) != 1 || events[0].Event.Type != types.Propose {
		t.Fatalf("unexpected proposal events: %+v", events)
	}
	events[0].WaitC <- nil
}

func assertNoPendingWaiters(t *testing.T, g *RaftGroup) {
	t.Helper()
	b := g.wait.buckets[g.wait.bucketIndex("slot")]
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.progresses) != 0 {
		t.Fatalf("finished proposal retained %d apply waiters", len(b.progresses))
	}
}

func TestProposeUntilAppliedCancellationReleasesWaiter(t *testing.T) {
	g := newWaitTestGroup(t)
	for iteration := 0; iteration < 100; iteration++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := g.ProposeBatchUntilAppliedTimeout(ctx, "slot", types.ProposeReqSet{{Id: 1}})
			done <- err
		}()
		admitWaitTestProposal(t, g)
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected canceled proposal, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("canceled proposal did not return")
		}
		assertNoPendingWaiters(t, g)
	}
}

func TestProposeUntilAppliedStopReleasesWaiter(t *testing.T) {
	g := newWaitTestGroup(t)
	done := make(chan error, 1)
	go func() {
		_, err := g.ProposeBatchUntilAppliedTimeout(context.Background(), "slot", types.ProposeReqSet{{Id: 1}})
		done <- err
	}()
	admitWaitTestProposal(t, g)
	g.Stop()
	select {
	case err := <-done:
		if !errors.Is(err, ErrGroupStopped) {
			t.Fatalf("expected stopped proposal, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stopped proposal did not return")
	}
	assertNoPendingWaiters(t, g)
}

// Make acknowledgement and deadline ready during the same select evaluation.
// The first Done call belongs to admission; the second belongs to apply wait.
type applyDeadlineContext struct {
	context.Context
	calls int
	done  chan struct{}
	apply func()
}

func (c *applyDeadlineContext) Done() <-chan struct{} {
	c.calls++
	if c.calls == 1 {
		return nil
	}
	if c.calls == 2 {
		c.apply()
		close(c.done)
	}
	return c.done
}

func (*applyDeadlineContext) Err() error { return context.DeadlineExceeded }

func TestProposeUntilAppliedCompletedAtDeadlineSucceeds(t *testing.T) {
	g := newWaitTestGroup(t)
	falseTimeouts := 0
	for iteration := 0; iteration < 100; iteration++ {
		ctx := &applyDeadlineContext{Context: context.Background(), done: make(chan struct{}),
			apply: func() { g.wait.didApply("slot", 1) }}
		done := make(chan error, 1)
		go func() {
			_, err := g.ProposeBatchUntilAppliedTimeout(ctx, "slot", types.ProposeReqSet{{Id: 1}})
			done <- err
		}()
		admitWaitTestProposal(t, g)
		select {
		case err := <-done:
			if err != nil {
				falseTimeouts++
			}
		case <-time.After(time.Second):
			t.Fatal("completed proposal did not return")
		}
		assertNoPendingWaiters(t, g)
	}
	if falseTimeouts != 0 {
		t.Fatalf("%d already-applied proposals falsely reported a deadline", falseTimeouts)
	}
}
