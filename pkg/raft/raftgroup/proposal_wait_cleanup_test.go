package raftgroup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// Isolate receipt ownership from replication and storage. The proposal still
// enters the real event queue and uses ProposeBatchUntilAppliedTimeout; the
// test plays the event loop with admitWaitTestProposal.
type waitTestRaft struct {
	IRaft
	mu   sync.Mutex
	last uint64
}

func (*waitTestRaft) Key() string          { return "slot" }
func (*waitTestRaft) IsLeader() bool       { return true }
func (*waitTestRaft) LeaderId() uint64     { return 1 }
func (*waitTestRaft) NodeId() uint64       { return 1 }
func (*waitTestRaft) AppliedIndex() uint64 { return 0 }
func (*waitTestRaft) KeepAlive()           {}
func (r *waitTestRaft) Admit(logs []types.Log) (types.Admission, error) {
	first := r.last + 1
	for i := range logs {
		logs[i].Index, logs[i].Term = first+uint64(i), 1
	}
	r.last += uint64(len(logs))
	return types.Admission{Term: 1, FirstIndex: first, LastIndex: r.last}, nil
}

// waitTestStorage serves point reads for forwarded receipts.
type waitTestStorage struct {
	IStorage
	logs map[uint64]types.Log
}

func (s *waitTestStorage) GetLogs(_ string, start, _, _ uint64) ([]types.Log, error) {
	if log, ok := s.logs[start]; ok {
		return []types.Log{log}, nil
	}
	return nil, nil
}

func newWaitTestGroup(t *testing.T, options ...Option) *RaftGroup {
	t.Helper()
	g := New(NewOptions(options...))
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

type waitTestFollower struct {
	*waitTestRaft
	applied uint64
}

func (*waitTestFollower) IsLeader() bool         { return false }
func (*waitTestFollower) NodeId() uint64         { return 2 }
func (r *waitTestFollower) AppliedIndex() uint64 { return r.applied }

type waitTestTransport struct{ group *RaftGroup }

func (tr *waitTestTransport) Send(_ string, event types.Event) {
	var reqs types.ProposeReqSet
	if err := reqs.Unmarshal(event.Logs[0].Data); err != nil {
		panic(err)
	}
	responses := types.ProposeRespSet{{Id: reqs[len(reqs)-1].Id, Index: 1}}
	tr.group.fowardProposeWait.Trigger(fmt.Sprintf("%d", responses[0].Id), responses)
}

func TestProposeUntilAppliedInvalidAdmissionHasNoWaiter(t *testing.T) {
	g := newWaitTestGroup(t)
	for _, key := range []string{"missing", "slot"} {
		if _, err := g.ProposeBatchUntilAppliedTimeout(context.Background(), key, nil); err == nil {
			t.Fatalf("expected invalid proposal %q to fail", key)
		}
		assertNoPendingWaiters(t, g)
	}
}

// R11: whether the forwarded entry is applied before or after the receipt is
// registered, success requires the stored Id to match.
func TestProposeUntilAppliedForwardedCompletionAndDeadline(t *testing.T) {
	for _, applied := range []uint64{0, 1} {
		for _, storedID := range []uint64{1, 2} {
			t.Run(fmt.Sprintf("applied=%d/storedId=%d", applied, storedID), func(t *testing.T) {
				transport := &waitTestTransport{}
				storage := &waitTestStorage{logs: map[uint64]types.Log{1: {Index: 1, Term: 1, Id: storedID}}}
				g := newWaitTestGroup(t, WithTransport(transport), WithStorage(storage), WithNotNeedApplied(true))
				transport.group = g
				g.RemoveRaft(g.GetRaft("slot"))
				g.AddRaft(&waitTestFollower{waitTestRaft: &waitTestRaft{}, applied: applied})
				for iteration := 0; iteration < 100; iteration++ {
					ctx := &applyDeadlineContext{Context: context.Background(), done: make(chan struct{}), at: 2,
						apply: func() { g.wait.didApply("slot", 1, nil) }}
					responses, err := g.ProposeBatchUntilAppliedTimeout(ctx, "slot", types.ProposeReqSet{{Id: 1}})
					if storedID == 1 && (err != nil || len(responses) != 1 || responses[0].Index != 1) {
						t.Fatalf("forwarded completion lost: responses=%v err=%v", responses, err)
					}
					if storedID != 1 && !errors.Is(err, types.ErrProposalSuperseded) {
						t.Fatalf("forwarded receipt completed by another entry: responses=%v err=%v", responses, err)
					}
					assertNoPendingWaiters(t, g)
				}
			})
		}
	}
}

// admitWaitTestProposal plays one event-loop admission.
func admitWaitTestProposal(t *testing.T, g *RaftGroup) {
	t.Helper()
	select {
	case <-g.advanceC:
	case <-time.After(time.Second):
		t.Fatal("proposal did not enter event queue")
	}
	events := g.mq.Get()
	if len(events) != 1 || events[0].Event.Type != types.Propose || events[0].Proposal == nil {
		t.Fatalf("unexpected proposal events: %+v", events)
	}
	g.admitProposal(g.GetRaft("slot"), events[0])
}

func assertNoPendingWaiters(t *testing.T, g *RaftGroup) {
	t.Helper()
	if n := g.wait.pending(); n != 0 {
		t.Fatalf("finished proposal retained %d receipts", n)
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
			if !errors.Is(err, types.ErrProposalOutcomeUnknown) || !errors.Is(err, context.Canceled) {
				t.Fatalf("expected unknown outcome of a canceled admitted proposal, got %v", err)
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
		if !errors.Is(err, ErrGroupStopped) || !errors.Is(err, types.ErrProposalOutcomeUnknown) {
			t.Fatalf("expected stopped proposal with unknown outcome, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stopped proposal did not return")
	}
	assertNoPendingWaiters(t, g)
}

// Make completion and deadline ready during the same select evaluation: the
// at-th Done call (the apply wait) completes the receipt and closes done.
type applyDeadlineContext struct {
	context.Context
	mu    sync.Mutex
	calls int
	at    int
	done  chan struct{}
	apply func()
}

func (c *applyDeadlineContext) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls == c.at {
		c.apply()
		close(c.done)
	}
	return c.done
}

func (c *applyDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func TestProposeUntilAppliedCompletedAtDeadlineSucceeds(t *testing.T) {
	g := newWaitTestGroup(t)
	falseTimeouts := 0
	for iteration := 0; iteration < 100; iteration++ {
		index := uint64(iteration + 1)
		// Done calls: semaphore, admission, apply wait.
		ctx := &applyDeadlineContext{Context: context.Background(), done: make(chan struct{}), at: 3,
			apply: func() { g.wait.didApply("slot", index, []types.Log{{Index: index, Term: 1, Id: 1}}) }}
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
