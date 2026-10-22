package raftgroup

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// C0-2 admission tests: real raft.Node and event loop, in-memory storage.

const admKey = "slot-1"

type memStorage struct {
	mu        sync.Mutex
	logs      []types.Log // logs[i] has Index i+1
	termStart types.TermStartIndexInfo
	applied   []types.Log
	// failApply makes that many Apply calls fail before they succeed; a
	// negative value fails all of them.
	failApply  int
	applyCalls int
	// appendGate, if set, is received from before every AppendLogs.
	appendGate chan struct{}
	onAppend   func(logs []types.Log)
}

func (s *memStorage) AppendLogs(_ string, logs []types.Log, termStart *types.TermStartIndexInfo) error {
	if s.appendGate != nil {
		<-s.appendGate
	}
	if s.onAppend != nil {
		s.onAppend(logs)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = append(s.logs[:logs[0].Index-1], logs...)
	if termStart != nil {
		s.termStart = *termStart
	}
	return nil
}

func (s *memStorage) GetLogs(_ string, start, end, _ uint64) ([]types.Log, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []types.Log
	for _, log := range s.logs {
		if log.Index >= start && (end == 0 || log.Index < end) {
			out = append(out, log)
		}
	}
	return out, nil
}

func (s *memStorage) Apply(_ string, logs []types.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyCalls++
	if s.failApply != 0 {
		if s.failApply > 0 {
			s.failApply--
		}
		return errors.New("injected apply failure")
	}
	s.applied = append(s.applied, logs...)
	return nil
}

func (s *memStorage) TruncateLogTo(_ string, index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < uint64(len(s.logs)) {
		s.logs = s.logs[:index]
	}
	return nil
}

func (s *memStorage) GetTermStartIndex(string, uint32) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.termStart.Index, nil
}

func (s *memStorage) LeaderLastLogTerm(string) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.logs) == 0 {
		return 0, nil
	}
	return s.logs[len(s.logs)-1].Term, nil
}

func (s *memStorage) LeaderTermGreaterEqThan(_ string, term uint32) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.termStart.Term >= term {
		return s.termStart.Term, nil
	}
	return 0, nil
}

func (s *memStorage) DeleteLeaderTermStartIndexGreaterThanTerm(string, uint32) error { return nil }
func (s *memStorage) SaveConfig(string, types.Config) error                          { return nil }

func ids(logs []types.Log) []uint64 {
	out := []uint64{}
	for _, log := range logs {
		out = append(out, log.Id)
	}
	return out
}

func (s *memStorage) storedIDs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ids(s.logs)
}

func (s *memStorage) appliedLogs() []types.Log {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]types.Log(nil), s.applied...)
}

// waitApplied waits for n applied entries, then checks nothing else applies.
func (s *memStorage) waitApplied(t *testing.T, n int) []uint64 {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for len(s.appliedLogs()) < n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	return ids(s.appliedLogs())
}

// dropTransport records what the node sends and delivers nothing.
type dropTransport struct{}

func (dropTransport) Send(string, types.Event) {}

type admissionGroup struct {
	*RaftGroup
	node    *raft.Node
	storage *memStorage
	hook    atomic.Pointer[func(seq uint64)]
}

// setBeforeAdmit changes the loop's beforeAdmit behaviour; the hook itself
// is installed before the loop starts.
func (g *admissionGroup) setBeforeAdmit(f func(seq uint64)) { g.hook.Store(&f) }

// newAdmissionGroup builds a group whose node leads term 1. With one replica
// entries commit as soon as they are stored; with three they never commit.
func newAdmissionGroup(t *testing.T, replicas []uint64, notNeedApplied bool, opts ...Option) *admissionGroup {
	t.Helper()
	storage := &memStorage{}
	g := New(NewOptions(append([]Option{WithTransport(dropTransport{}), WithStorage(storage), WithTickInterval(time.Hour),
		WithProposeTimeout(5 * time.Second), WithNotNeedApplied(notNeedApplied)}, opts...)...))
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(admKey), raft.WithNodeId(1), raft.WithReplicas(replicas)))
	if !node.IsLeader() {
		node.BecomeLeader(1) // before the loop runs
	}
	g.AddRaft(node)
	t.Cleanup(func() {
		select {
		case <-g.stopper.ShouldStop():
		default:
			g.Stop()
		}
		g.goPool.Release()
	})
	ag := &admissionGroup{RaftGroup: g, node: node, storage: storage}
	g.beforeAdmit = func(seq uint64) {
		if f := ag.hook.Load(); f != nil {
			(*f)(seq)
		}
	}
	return ag
}

func (g *admissionGroup) propose(ctx context.Context, ids ...uint64) (types.ProposeRespSet, error) {
	reqs := types.ProposeReqSet{}
	for _, id := range ids {
		reqs = append(reqs, types.ProposeReq{Id: id, Data: []byte(fmt.Sprintf("data-%d", id))})
	}
	return g.ProposeBatchUntilAppliedTimeout(ctx, admKey, reqs)
}

// proposeAsync starts a proposal and returns its result channel.
type proposeResult struct {
	resps types.ProposeRespSet
	err   error
}

func (g *admissionGroup) proposeAsync(ctx context.Context, ids ...uint64) chan proposeResult {
	c := make(chan proposeResult, 1)
	go func() {
		resps, err := g.propose(ctx, ids...)
		c <- proposeResult{resps, err}
	}()
	return c
}

func recv(t *testing.T, c chan proposeResult) proposeResult {
	t.Helper()
	select {
	case r := <-c:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("proposal did not return")
		return proposeResult{}
	}
}

func (g *admissionGroup) queued() int {
	g.mq.mu.Lock()
	defer g.mq.mu.Unlock()
	return len(g.mq.nodrop)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func requireOwnIndexes(t *testing.T, resps types.ProposeRespSet, applied []types.Log) {
	t.Helper()
	byIndex := map[uint64]uint64{}
	for _, log := range applied {
		byIndex[log.Index] = log.Id
	}
	for _, resp := range resps {
		require.Equal(t, resp.Id, byIndex[resp.Index], "index %d must hold the caller's own entry", resp.Index)
	}
}

func requireNotAdmitted(t *testing.T, err error, cause error) {
	t.Helper()
	require.ErrorIs(t, err, types.ErrProposalNotAdmitted)
	require.ErrorIs(t, err, cause)
	require.NotErrorIs(t, err, types.ErrProposalOutcomeUnknown)
}

var overlapCases = []struct {
	name     string
	a, b     []uint64
	bIndexes []uint64 // B's indexes when A was skipped
}{
	{"R1-duplicate", []uint64{100}, []uint64{200}, []uint64{1}},
	{"R2-partial-overlap", []uint64{100}, []uint64{200, 201}, []uint64{1, 2}},
	{"R3-shorter-second", []uint64{100, 101}, []uint64{200}, []uint64{1}},
}

// R1–R3, A cancelled before admission (the XIN-2020 schedule: the loop is
// stalled while A is queued and A's caller times out, then B proposes).
// A is definitely not executed and B gets its own range.
func TestAdmissionCancelledBeforeAdmission(t *testing.T) {
	for _, tc := range overlapCases {
		t.Run(tc.name, func(t *testing.T) {
			g := newAdmissionGroup(t, []uint64{1}, false)
			ctxA, cancelA := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancelA()
			_, errA := g.propose(ctxA, tc.a...)
			requireNotAdmitted(t, errA, context.DeadlineExceeded)
			require.Equal(t, 1, g.queued(), "A's event is still queued, abandoned")

			ctxB, cancelB := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelB()
			bC := g.proposeAsync(ctxB, tc.b...)
			waitFor(t, "B queued", func() bool { return g.queued() == 2 })
			require.NoError(t, g.Start())

			b := recv(t, bC)
			require.NoError(t, b.err)
			for i, resp := range b.resps {
				require.Equal(t, tc.b[i], resp.Id)
				require.Equal(t, tc.bIndexes[i], resp.Index)
			}
			require.Equal(t, tc.b, g.storage.waitApplied(t, len(tc.b)), "A never applies")
			require.Equal(t, tc.b, g.storage.storedIDs(), "A never stored")
			requireOwnIndexes(t, b.resps, g.storage.appliedLogs())
			require.Zero(t, g.wait.pending())
			ok, notAdmitted, unknown, superseded := g.ProposalOutcomeCounts()
			require.Equal(t, []uint64{1, 1, 0, 0}, []uint64{ok, notAdmitted, unknown, superseded})
		})
	}
}

// R1–R3, A already admitting when its caller expires: A reports Unknown, and
// B gets its own, distinct range after A's.
func TestAdmissionAdmittedBeforeCallerExpired(t *testing.T) {
	for _, tc := range overlapCases {
		t.Run(tc.name, func(t *testing.T) {
			g := newAdmissionGroup(t, []uint64{1}, false)
			g.storage.appendGate = make(chan struct{}) // hold apply until A returned
			aExpired := make(chan struct{})
			var once sync.Once
			g.setBeforeAdmit(func(uint64) { once.Do(func() { <-aExpired }) })
			require.NoError(t, g.Start())

			ctxA, cancelA := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancelA()
			aC := g.proposeAsync(ctxA, tc.a...)
			<-ctxA.Done()
			time.Sleep(20 * time.Millisecond) // A's caller found the proposal admitting
			close(aExpired)
			a := recv(t, aC)
			require.ErrorIs(t, a.err, types.ErrProposalOutcomeUnknown)
			require.ErrorIs(t, a.err, context.DeadlineExceeded)

			ctxB, cancelB := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancelB()
			bC := g.proposeAsync(ctxB, tc.b...)
			close(g.storage.appendGate)
			b := recv(t, bC)
			require.NoError(t, b.err)
			for i, resp := range b.resps {
				require.Equal(t, uint64(len(tc.a)+i+1), resp.Index, "B's range follows A's")
			}
			want := append(append([]uint64{}, tc.a...), tc.b...)
			require.Equal(t, want, g.storage.waitApplied(t, len(want)), "nothing truncated, nothing duplicated")
			requireOwnIndexes(t, b.resps, g.storage.appliedLogs())
			require.Zero(t, g.wait.pending())
		})
	}
}

// R5: the leader steps down before admission.
func TestAdmissionLeaderStepsDownBeforeAdmission(t *testing.T) {
	g := newAdmissionGroup(t, []uint64{1, 2, 3}, false)
	g.setBeforeAdmit(func(uint64) { g.node.BecomeFollower(2, 2) }) // on the loop
	require.NoError(t, g.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := g.propose(ctx, 100)
	requireNotAdmitted(t, err, types.ErrNotLeader)
	require.Empty(t, g.storage.storedIDs())
	require.Zero(t, g.wait.pending())
}

// stepDown makes the node follow node 2 in term 2.
func (g *admissionGroup) stepDown() {
	g.AddEvent(admKey, types.Event{Type: types.Ping, From: 2, To: 1, Term: 2})
	g.Advance()
}

// R5: the leader steps down after admission; the fence reports Unknown.
func TestAdmissionLeaderStepsDownAfterAdmission(t *testing.T) {
	g := newAdmissionGroup(t, []uint64{1, 2, 3}, false)
	require.NoError(t, g.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	aC := g.proposeAsync(ctx, 100)
	waitFor(t, "A stored", func() bool { return len(g.storage.storedIDs()) == 1 })
	require.Equal(t, 1, g.wait.pending())
	start := time.Now()
	g.stepDown()
	a := recv(t, aC)
	require.ErrorIs(t, a.err, types.ErrProposalOutcomeUnknown)
	require.Less(t, time.Since(start), time.Second, "the fence completes the receipt, not the deadline")
	require.Zero(t, g.wait.pending())
}

// R6: after a term change another entry replaces the admitted one at the same
// index and is applied. The old receipt never succeeds. Without a fence the
// NotNeedApplied mode could not tell the entries apart.
func TestAdmissionReplacementEntryDoesNotCompleteOldReceipt(t *testing.T) {
	for _, notNeedApplied := range []bool{false, true} {
		t.Run(fmt.Sprintf("notNeedApplied=%v", notNeedApplied), func(t *testing.T) {
			g := newAdmissionGroup(t, []uint64{1, 2, 3}, notNeedApplied)
			require.NoError(t, g.Start())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			aC := g.proposeAsync(ctx, 100)
			waitFor(t, "A stored", func() bool { return len(g.storage.storedIDs()) == 1 })

			g.stepDown()
			// The new leader truncates index 1 and replicates its own entry there.
			g.AddEvent(admKey, types.Event{Type: types.SyncResp, From: 2, To: 1, Term: 2, Reason: types.ReasonTruncate, Index: 0})
			g.Advance()
			waitFor(t, "truncate", func() bool { return len(g.storage.storedIDs()) == 0 })
			g.AddEvent(admKey, types.Event{Type: types.SyncResp, From: 2, To: 1, Term: 2, Reason: types.ReasonOk, Index: 1,
				Logs: []types.Log{{Index: 1, Term: 2, Id: 200, Data: []byte("replacement")}}})
			g.Advance()
			waitFor(t, "replacement stored", func() bool { ids := g.storage.storedIDs(); return len(ids) == 1 && ids[0] == 200 })
			g.AddEvent(admKey, types.Event{Type: types.Ping, From: 2, To: 1, Term: 2, CommittedIndex: 1})
			g.Advance()

			a := recv(t, aC)
			require.Error(t, a.err, "the replacement entry must not complete A")
			require.True(t, errors.Is(a.err, types.ErrProposalOutcomeUnknown) || errors.Is(a.err, types.ErrProposalSuperseded), a.err)
			if !notNeedApplied {
				require.Equal(t, []uint64{200}, g.storage.waitApplied(t, 1))
			}
			require.Zero(t, g.wait.pending())
		})
	}
}

// R7: expired before enqueue and while waiting for the admission semaphore.
func TestAdmissionExpiredBeforeEnqueue(t *testing.T) {
	g := newAdmissionGroup(t, []uint64{1}, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := g.propose(ctx, 100)
	requireNotAdmitted(t, err, context.Canceled)
	require.Zero(t, g.queued(), "no event was queued")

	// B holds the semaphore while the loop is stalled; C waits for it.
	ctxB, cancelB := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelB()
	bC := g.proposeAsync(ctxB, 200)
	waitFor(t, "B queued", func() bool { return g.queued() == 1 })
	ctxC, cancelC := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelC()
	_, err = g.propose(ctxC, 300)
	requireNotAdmitted(t, err, context.DeadlineExceeded)
	require.Equal(t, 1, g.queued(), "C never reached the queue")

	require.NoError(t, g.Start())
	b := recv(t, bC)
	require.NoError(t, b.err)
	require.Equal(t, uint64(1), b.resps[0].Index)
	require.Equal(t, []uint64{200}, g.storage.waitApplied(t, 1))
	require.Equal(t, 0, len(g.admissionSem(admKey)), "semaphore released on every path")
}

// R7: cancelled after enqueue, before the loop steps it. The loop skips the
// event without allocating an index.
func TestAdmissionAbandonedEventIsSkipped(t *testing.T) {
	g := newAdmissionGroup(t, []uint64{1}, false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := g.propose(ctx, 100)
	requireNotAdmitted(t, err, context.DeadlineExceeded)
	events := g.mq.Get()
	require.Len(t, events, 1)
	require.Equal(t, proposalAbandoned, events[0].Proposal.state.Load())
	g.mq.MustAdd(events[0])
	require.NoError(t, g.Start())
	waitFor(t, "queue drained", func() bool { return g.queued() == 0 })
	time.Sleep(20 * time.Millisecond)
	require.Empty(t, g.storage.storedIDs())

	resps, err := g.propose(context.Background(), 300)
	require.NoError(t, err)
	require.Equal(t, uint64(1), resps[0].Index, "the skipped event allocated no index")
	require.Equal(t, []uint64{300}, g.storage.waitApplied(t, 1))
}

// The loop registers the receipt before the admitted range can be stored, so
// apply cannot complete before the waiter exists.
func TestAdmissionReceiptRegisteredBeforeStore(t *testing.T) {
	g := newAdmissionGroup(t, []uint64{1}, true)
	var missing []uint64
	var mu sync.Mutex
	g.storage.onAppend = func(logs []types.Log) {
		if g.wait.pending() == 0 {
			mu.Lock()
			missing = append(missing, logs[len(logs)-1].Index)
			mu.Unlock()
		}
	}
	require.NoError(t, g.Start())
	for i := uint64(1); i <= 200; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resps, err := g.propose(ctx, i)
		cancel()
		require.NoError(t, err)
		require.Equal(t, i, resps[0].Index)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Empty(t, missing, "store dispatched before the receipt was registered")
}

// R12: Stop with a queued, an admitting and an admitted caller. All return in
// bounded time with truthful outcomes and nothing is left behind.
func TestAdmissionStopCompletesQueuedAndInFlight(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	g := newAdmissionGroup(t, []uint64{1, 2, 3}, false)
	require.NoError(t, g.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	admittedC := g.proposeAsync(ctx, 100)
	waitFor(t, "admitted", func() bool { return g.wait.pending() == 1 })

	hold := make(chan struct{})
	g.setBeforeAdmit(func(uint64) {
		select {
		case <-hold:
		case <-g.stopper.ShouldStop():
		}
	})
	admittingC := g.proposeAsync(ctx, 200)
	waitFor(t, "admitting", func() bool { return g.queued() == 0 && len(g.admissionSem(admKey)) == 1 })
	queuedC := g.proposeAsync(ctx, 300) // waits for the semaphore
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	g.Stop()
	admitted, admitting, queued := recv(t, admittedC), recv(t, admittingC), recv(t, queuedC)
	require.Less(t, time.Since(start), 2*time.Second)
	require.ErrorIs(t, admitted.err, types.ErrProposalOutcomeUnknown)
	require.ErrorIs(t, admitted.err, ErrGroupStopped)
	require.Error(t, admitting.err)
	require.ErrorIs(t, admitting.err, ErrGroupStopped)
	requireNotAdmitted(t, queued.err, ErrGroupStopped)
	require.Zero(t, g.wait.pending())
	require.Equal(t, 0, len(g.admissionSem(admKey)))
	close(hold)
	g.goPool.Release()
}

// R12: RemoveRaft completes the raft's receipts; a proposal still queued for
// the removed raft is rejected as not admitted.
func TestAdmissionRemoveRaftCompletesReceipts(t *testing.T) {
	g := newAdmissionGroup(t, []uint64{1, 2, 3}, false)
	require.NoError(t, g.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admittedC := g.proposeAsync(ctx, 100)
	waitFor(t, "admitted", func() bool { return g.wait.pending() == 1 })

	hold := make(chan struct{})
	g.setBeforeAdmit(func(uint64) { <-hold })
	stalledC := g.proposeAsync(ctx, 200) // admitting; the loop waits in beforeAdmit
	waitFor(t, "admitting", func() bool { return g.queued() == 0 && len(g.admissionSem(admKey)) == 1 })
	queuedC := make(chan proposeResult, 1)
	go func() {
		// Queue directly behind the stalled admission, bypassing the semaphore.
		p := newProposal(g.proposalSeq.Add(1), true)
		g.mq.MustAdd(Event{RaftKey: admKey, Event: types.Event{Type: types.Propose, Logs: []types.Log{{Id: 300}}}, Proposal: p})
		r := <-p.admitC
		queuedC <- proposeResult{err: r.err}
	}()
	waitFor(t, "queued", func() bool { return g.queued() == 1 })

	g.RemoveRaft(g.node)
	admitted := recv(t, admittedC)
	require.ErrorIs(t, admitted.err, types.ErrProposalOutcomeUnknown)
	require.ErrorIs(t, admitted.err, ErrRaftNotExist)
	close(hold)
	queued := recv(t, queuedC)
	requireNotAdmitted(t, queued.err, ErrRaftNotExist)
	// Admitted on the node object the loop already held, after the removal:
	// its receipt cannot complete and is reported as unknown at once.
	stalled := recv(t, stalledC)
	require.ErrorIs(t, stalled.err, types.ErrProposalOutcomeUnknown)
	require.ErrorIs(t, stalled.err, ErrRaftNotExist)
	_, err := g.propose(ctx, 400)
	require.Error(t, err)
	require.Zero(t, g.wait.pending())
}

// R13: apply fails and is retried. The receipt stays pending and completes
// once the entry is applied, exactly once. A failed apply is retried on the
// next tick, so these groups tick.
func TestAdmissionApplyRetry(t *testing.T) {
	g := newAdmissionGroup(t, []uint64{1}, false, WithTickInterval(10*time.Millisecond))
	g.storage.failApply = 3
	require.NoError(t, g.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resps, err := g.propose(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(1), resps[0].Index)
	require.Equal(t, []uint64{100}, g.storage.waitApplied(t, 1))
	g.storage.mu.Lock()
	require.Equal(t, 4, g.storage.applyCalls)
	g.storage.mu.Unlock()
}

// R13: apply keeps failing. The caller ends with Unknown at its deadline and
// the entry is applied once after the failures stop.
func TestAdmissionApplyFailingUntilDeadline(t *testing.T) {
	g := newAdmissionGroup(t, []uint64{1}, false, WithTickInterval(10*time.Millisecond))
	g.storage.failApply = -1
	require.NoError(t, g.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := g.propose(ctx, 100)
	require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, g.wait.pending())
	g.storage.mu.Lock()
	g.storage.failApply = 0
	g.storage.mu.Unlock()
	require.Equal(t, []uint64{100}, g.storage.waitApplied(t, 1))
}
