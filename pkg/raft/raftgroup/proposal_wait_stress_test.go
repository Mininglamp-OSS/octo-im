package raftgroup

import (
	"context"
	"errors"
	"fmt"
	wt "github.com/WuKongIM/WuKongIM/pkg/wait"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

// Stress ProposeBatchUntilAppliedTimeout on a real two-node cluster with
// deadlines placed around the typical apply latency. Enable explicitly:
//
//	RAFT_WAIT_STRESS=1 go test -v -count=1 -run TestApplyWaitStress ./pkg/raft/raftgroup
//
// RAFT_WAIT_STRESS_SECONDS sets the load duration (default 5) and
// RAFT_WAIT_STRESS_TIMEOUT_SCALE widens deadlines (default 1).
func skipUnlessApplyWaitStress(t *testing.T) {
	t.Helper()
	if os.Getenv("RAFT_WAIT_STRESS") == "" {
		t.Skip("set RAFT_WAIT_STRESS=1 to run apply-wait stress tests")
	}
}

// stressStorage is a thread-safe in-memory store that records when this node
// applied each proposal id, so callers can compare results with reality.
type stressStorage struct {
	group      *RaftGroup
	receipts   map[uint64]*receiptWaiter
	mu         sync.Mutex
	logs       map[string][]types.Log
	termStarts map[string]types.TermStartIndexInfo
	appliedAt  map[uint64]time.Time
	applyDelay func() time.Duration
}

func newStressStorage(applyDelay func() time.Duration) *stressStorage {
	return &stressStorage{
		receipts:   make(map[uint64]*receiptWaiter),
		logs:       make(map[string][]types.Log),
		termStarts: make(map[string]types.TermStartIndexInfo),
		appliedAt:  make(map[uint64]time.Time),
		applyDelay: applyDelay,
	}
}

func (s *stressStorage) AppendLogs(key string, logs []types.Log, termStartIndexInfo *types.TermStartIndexInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.logs[key]
	if len(logs) > 0 && logs[0].Index <= uint64(len(current)) {
		current = current[:logs[0].Index-1]
	}
	s.logs[key] = append(current, logs...)
	if termStartIndexInfo != nil {
		s.termStarts[key] = *termStartIndexInfo
	}
	return nil
}

func (s *stressStorage) GetTermStartIndex(key string, term uint32) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.termStarts[key].Index, nil
}

func (s *stressStorage) LeaderLastLogTerm(key string) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logs := s.logs[key]
	if len(logs) == 0 {
		return 0, nil
	}
	return logs[len(logs)-1].Term, nil
}

func (s *stressStorage) LeaderTermGreaterEqThan(key string, term uint32) (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.termStarts[key]; ok && info.Term >= term {
		return info.Term, nil
	}
	return 0, nil
}

func (s *stressStorage) GetLogs(key string, start, end, maxSize uint64) ([]types.Log, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logs := s.logs[key]
	if start == 0 || start > uint64(len(logs)) {
		return nil, nil
	}
	last := uint64(len(logs))
	if end > 0 && end-1 < last {
		last = end - 1
	}
	result := make([]types.Log, 0, last-start+1)
	var size uint64
	for i := start; i <= last; i++ {
		log := logs[i-1]
		size += uint64(log.LogSize())
		if maxSize > 0 && size > maxSize && len(result) > 0 {
			break
		}
		result = append(result, log)
	}
	return result, nil
}

func (s *stressStorage) Apply(key string, logs []types.Log) error {
	b := s.group.wait.bucket(key)
	b.mu.Lock()
	s.mu.Lock()
	for _, w := range b.waiters[key] {
		s.receipts[w.lastLogId] = w
	}
	s.mu.Unlock()
	b.mu.Unlock()
	if s.applyDelay != nil {
		time.Sleep(s.applyDelay())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for _, log := range logs {
		if log.Id != 0 {
			s.appliedAt[log.Id] = now
		}
	}
	return nil
}

func (s *stressStorage) applyTime(id uint64) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.appliedAt[id]
	return at, ok
}

func (s *stressStorage) TruncateLogTo(key string, index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if logs := s.logs[key]; index < uint64(len(logs)) {
		s.logs[key] = logs[:index]
	}
	return nil
}

func (s *stressStorage) DeleteLeaderTermStartIndexGreaterThanTerm(key string, term uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, ok := s.termStarts[key]; ok && info.Term > term {
		delete(s.termStarts, key)
	}
	return nil
}

func (s *stressStorage) SaveConfig(key string, cfg types.Config) error { return nil }

type stressTransport struct{ groups map[uint64]*RaftGroup }

func (tr *stressTransport) Send(key string, event types.Event) {
	g := tr.groups[event.To]
	if g == nil {
		return
	}
	g.AddEvent(key, event)
	g.Advance()
}

// stressNode intercepts fixture-only snapshot events on the group's event loop.
// Raw Node state is never read by the test goroutine. Production methods and
// their concurrency are unchanged; this adds no getter-only lock.
type stressNode struct {
	IRaft
	requests chan chan stressSnapshot
}
type stressSnapshot struct {
	leader                   uint64
	term                     uint32
	last, committed, applied uint64
}

func (n *stressNode) Step(e types.Event) error {
	if e.Type == types.Unknown && e.From == ^uint64(0) {
		reply := <-n.requests
		reply <- stressSnapshot{n.IRaft.LeaderId(), n.IRaft.Config().Term, n.IRaft.LastLogIndex(), n.IRaft.CommittedIndex(), n.IRaft.AppliedIndex()}
		return nil
	}
	return n.IRaft.Step(e)
}

// stressForwardWait retains the real registry behavior and counts registrations
// under the same wrapper lock used by Register and Trigger.
type stressForwardWait struct {
	wt.Wait
	mu  sync.Mutex
	ids map[string]bool
}

func (w *stressForwardWait) Register(id string) <-chan interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	ch := w.Wait.Register(id)
	w.ids[id] = true
	return ch
}
func (w *stressForwardWait) Trigger(id string, value interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.Wait.Trigger(id, value)
	delete(w.ids, id)
}
func (w *stressForwardWait) pending() int { w.mu.Lock(); defer w.mu.Unlock(); return len(w.ids) }

type stressCluster struct {
	groups     []*RaftGroup
	stores     []*stressStorage
	keys       []string
	leaders    map[string]uint64
	started    []*RaftGroup
	shutdown   sync.Once
	stopped    chan struct{}
	stopErrors []error
}

func (c *stressCluster) stop(t *testing.T) {
	t.Helper()
	c.shutdown.Do(func() {
		go func() {
			defer close(c.stopped)
			for _, g := range c.started {
				g.Stop()
			}
			// All event loops have joined: no new pool work can be scheduled.
			for _, g := range c.groups {
				if err := g.goPool.ReleaseTimeout(3 * time.Second); err != nil {
					c.stopErrors = append(c.stopErrors, err)
				}
			}
		}()
	})
	select {
	case <-c.stopped:
		for _, err := range c.stopErrors {
			t.Errorf("pool shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("complete Stop and pool join exceeded 10s")
	}
}
func (c *stressCluster) assertCleanup(t *testing.T) {
	t.Helper()
	for i, g := range c.groups {
		local, forwarded := 0, 0
		for _, b := range g.wait.buckets {
			b.mu.Lock()
			for _, entries := range b.waiters {
				for _, w := range entries {
					if w.forwarded {
						forwarded++
					} else {
						local++
					}
				}
			}
			b.mu.Unlock()
		}
		registrations := g.fowardProposeWait.(*stressForwardWait).pending()
		workers := g.goPool.Running()
		t.Logf("cleanup node=%d localReceipts=%d forwardedReceipts=%d forwardRegistrations=%d poolWorkers=%d", i+1, local, forwarded, registrations, workers)
		if local+forwarded+registrations+workers != 0 {
			t.Errorf("node %d did not drain", i+1)
		}
	}
}
func (c *stressCluster) snapshot(t *testing.T, g *RaftGroup, key string) stressSnapshot {
	t.Helper()
	n := g.GetRaft(key).(*stressNode)
	reply := make(chan stressSnapshot, 1)
	n.requests <- reply
	g.AddEvent(key, types.Event{Type: types.Unknown, From: ^uint64(0)})
	g.Advance()
	select {
	case state := <-reply:
		return state
	case <-time.After(3 * time.Second):
		t.Fatal("event-loop snapshot timed out")
	}
	return stressSnapshot{}
}
func newStressCluster(t *testing.T, keyCount int, applyDelay func() time.Duration) *stressCluster {
	t.Helper()
	nodeIDs := []uint64{1, 2}
	tr := &stressTransport{groups: make(map[uint64]*RaftGroup)}
	c := &stressCluster{stopped: make(chan struct{}), leaders: make(map[string]uint64)}
	// Registered before allocation/start: partial setup failures also join workers.
	t.Cleanup(func() { c.stop(t); c.assertCleanup(t) })
	for i := 0; i < keyCount; i++ {
		c.keys = append(c.keys, fmt.Sprintf("slot-%d", i))
	}
	for _, id := range nodeIDs {
		store := newStressStorage(applyDelay)
		g := New(NewOptions(WithTransport(tr), WithStorage(store), WithTickInterval(10*time.Millisecond), WithLogPrefix(fmt.Sprintf("stress-%d", id))))
		g.fowardProposeWait = &stressForwardWait{Wait: wt.New(), ids: make(map[string]bool)}
		store.group = g
		tr.groups[id] = g
		c.groups = append(c.groups, g)
		c.stores = append(c.stores, store)
		for _, key := range c.keys {
			n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithNodeId(id), raft.WithKey(key), raft.WithElectionOn(true), raft.WithElectionInterval(5), raft.WithReplicas(nodeIDs), raft.WithAdvance(g.Advance)))
			g.AddRaft(&stressNode{IRaft: n, requests: make(chan chan stressSnapshot, 1)})
		}
	}
	for _, g := range c.groups {
		if err := g.Start(); err != nil {
			t.Fatal(err)
		}
		c.started = append(c.started, g)
	}
	// Require ten agreeing event-loop snapshots over >=200ms (four election
	// intervals), with unchanged leaders/terms; a single matching poll is insufficient.
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	var previous []stressSnapshot
	stable := 0
	for {
		select {
		case <-timeout.C:
			t.Fatal("election stabilization timed out")
		case <-ticker.C:
		}
		states := make([]stressSnapshot, 0, keyCount*2)
		agree := true
		for _, key := range c.keys {
			a, b := c.snapshot(t, c.groups[0], key), c.snapshot(t, c.groups[1], key)
			agree = agree && a.leader != 0 && a.leader == b.leader && a.term == b.term
			states = append(states, a, b)
		}
		if len(previous) == len(states) {
			for i, state := range states {
				agree = agree && state.leader == previous[i].leader && state.term == previous[i].term
			}
		} else {
			agree = false
		}
		if agree {
			stable++
		} else {
			stable = 0
		}
		previous = states
		if stable >= 10 {
			for i, key := range c.keys {
				c.leaders[key] = states[i*2].leader
			}
			t.Logf("election barrier: 10 stable event-loop rounds, states=%v", states)
			break
		}
	}
	return c
}

func (c *stressCluster) waitQuiescent(t *testing.T) bool {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for {
		idle := true
		for _, g := range c.groups {
			for _, key := range c.keys {
				state := c.snapshot(t, g, key)
				idle = idle && state.applied == state.last && state.committed == state.last
			}
		}
		if idle {
			return true
		}
		select {
		case <-timeout.C:
			return false
		case <-ticker.C:
		}
	}
}

// remainingWaiters counts entries still held by the apply-wait buckets.
func (c *stressCluster) remainingWaiters() int {
	total := 0
	for _, g := range c.groups {
		total += g.wait.pending()
	}
	return total
}

type stressStats struct {
	calls, ok, deadline, canceled, stopped, other                                  int64
	notAdmitted, unknown, rejected, superseded                                     int64
	leaderCalls, forwardedCalls, deadlineCalls, cancelCalls                        int64
	leaderOK, forwardedOK                                                          int64
	businessAppliedUnpublished, businessAppliedPublished, businessAppliedNoReceipt int64
	// Business Apply precedes expiry; this alone does not prove receipt completion.
	appliedBeforeExpiryButFailed int64
	// Success was returned although this node had not applied the batch yet.
	okButNotApplied int64
	// The call returned more than 500ms after its context expired.
	lateReturn int64

	mu          sync.Mutex
	notApplied  []stressProposal
	otherErrors map[string]int
}

type stressProposal struct {
	node int
	key  string
	id   uint64
	resp []*types.ProposeResp
}

func (s *stressStats) String() string {
	return fmt.Sprintf("calls=%d ok=%d deadline=%d canceled=%d stopped=%d other=%d appliedBeforeExpiryButFailed=%d okButNotApplied=%d lateReturn=%d",
		s.calls, s.ok, s.deadline, s.canceled, s.stopped, s.other, s.appliedBeforeExpiryButFailed, s.okButNotApplied, s.lateReturn) +
		fmt.Sprintf(" notAdmitted=%d unknown=%d rejected=%d superseded=%d leaderCalls=%d forwardedCalls=%d deadlineCalls=%d cancelCalls=%d businessAppliedUnpublished=%d businessAppliedPublished=%d businessAppliedNoReceipt=%d leaderOK=%d forwardedOK=%d", s.notAdmitted, s.unknown, s.rejected, s.superseded, s.leaderCalls, s.forwardedCalls, s.deadlineCalls, s.cancelCalls, s.businessAppliedUnpublished, s.businessAppliedPublished, s.businessAppliedNoReceipt, s.leaderOK, s.forwardedOK)
}

type stressLoad struct {
	workersPerNode int
	stop           <-chan struct{}
	// newContext returns the call context and a function reporting when it
	// expired (zero while it has not).
	newContext func(r *rand.Rand) (context.Context, context.CancelFunc, func() time.Time)
	onSuccess  func(time.Duration)
	started    chan<- struct{}
}

func timeoutContext(d time.Duration) (context.Context, context.CancelFunc, func() time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	deadline, _ := ctx.Deadline()
	return ctx, cancel, func() time.Time {
		if ctx.Err() == nil {
			return time.Time{}
		}
		return deadline
	}
}

func cancelAfterContext(d time.Duration) (context.Context, context.CancelFunc, func() time.Time) {
	ctx, cancel := context.WithCancel(context.Background())
	var firedAt atomic.Int64
	done := make(chan struct{})
	timer := time.AfterFunc(d, func() {
		defer close(done)
		firedAt.Store(time.Now().UnixNano())
		cancel()
	})
	return ctx, func() {
			if !timer.Stop() {
				<-done
			}
			cancel()
		}, func() time.Time {
			if at := firedAt.Load(); at != 0 {
				return time.Unix(0, at)
			}
			return time.Time{}
		}
}

var stressProposalID atomic.Uint64

func (c *stressCluster) run(t *testing.T, load stressLoad) *stressStats {
	stats := &stressStats{otherErrors: make(map[string]int)}
	var wg sync.WaitGroup
	for node := range c.groups {
		for w := 0; w < load.workersPerNode; w++ {
			wg.Add(1)
			go func(node int, seed int64) {
				defer wg.Done()
				r := rand.New(rand.NewSource(seed))
				g, store := c.groups[node], c.stores[node]
				firstCall := true
				for {
					select {
					case <-load.stop:
						return
					default:
					}
					reqs := make(types.ProposeReqSet, 1+r.Intn(3))
					for i := range reqs {
						reqs[i] = types.ProposeReq{Id: stressProposalID.Add(1), Data: []byte("stress")}
					}
					key := c.keys[r.Intn(len(c.keys))]
					ctx, cancel, expiredAt := load.newContext(r)
					if uint64(node+1) == c.leaders[key] {
						atomic.AddInt64(&stats.leaderCalls, 1)
					} else {
						atomic.AddInt64(&stats.forwardedCalls, 1)
					}
					if _, hasDeadline := ctx.Deadline(); hasDeadline {
						atomic.AddInt64(&stats.deadlineCalls, 1)
					} else {
						atomic.AddInt64(&stats.cancelCalls, 1)
					}
					if load.started != nil && firstCall {
						firstCall = false
						select {
						case load.started <- struct{}{}:
						default:
						}
					}
					start := time.Now()
					resps, err := g.ProposeBatchUntilAppliedTimeout(ctx, key, reqs)
					returned := time.Now()
					lastID := reqs[len(reqs)-1].Id
					appliedAt, applied := store.applyTime(lastID)
					expired := expiredAt()
					cancel()

					atomic.AddInt64(&stats.calls, 1)
					if !expired.IsZero() && returned.Sub(expired) > 500*time.Millisecond {
						atomic.AddInt64(&stats.lateReturn, 1)
					}
					if err != nil {
						switch {
						case errors.Is(err, types.ErrProposalNotAdmitted):
							atomic.AddInt64(&stats.notAdmitted, 1)
						case errors.Is(err, types.ErrProposalOutcomeUnknown):
							atomic.AddInt64(&stats.unknown, 1)
						case errors.Is(err, types.ErrForwardRejected):
							atomic.AddInt64(&stats.rejected, 1)
						case errors.Is(err, types.ErrProposalSuperseded):
							atomic.AddInt64(&stats.superseded, 1)
						default:
							atomic.AddInt64(&stats.other, 1)
						}
					}
					switch {
					case err == nil:
						atomic.AddInt64(&stats.ok, 1)
						if uint64(node+1) == c.leaders[key] {
							atomic.AddInt64(&stats.leaderOK, 1)
						} else {
							atomic.AddInt64(&stats.forwardedOK, 1)
						}
						valid := forwardRespsMatch(reqs, resps)
						if valid {
							store.mu.Lock()
							for _, resp := range resps {
								logs := store.logs[key]
								if resp.Index > uint64(len(logs)) || logs[resp.Index-1].Id != resp.Id {
									valid = false
								}
							}
							store.mu.Unlock()
						}
						for _, req := range reqs {
							at, exists := store.applyTime(req.Id)
							valid = valid && exists && !at.After(returned)
						}
						if !valid {
							atomic.AddInt64(&stats.okButNotApplied, 1)
							stats.mu.Lock()
							stats.notApplied = append(stats.notApplied, stressProposal{node, key, lastID, resps})
							stats.mu.Unlock()
						}
						if load.onSuccess != nil {
							load.onSuccess(returned.Sub(start))
						}
					case errors.Is(err, context.DeadlineExceeded):
						atomic.AddInt64(&stats.deadline, 1)
					case errors.Is(err, context.Canceled):
						atomic.AddInt64(&stats.canceled, 1)
					case errors.Is(err, ErrGroupStopped):
						atomic.AddInt64(&stats.stopped, 1)
						return
					default:
						stats.mu.Lock()
						stats.otherErrors[err.Error()]++
						stats.mu.Unlock()
					}
					if err != nil && applied && !expired.IsZero() && appliedAt.Before(expired) {
						atomic.AddInt64(&stats.appliedBeforeExpiryButFailed, 1)
						store.mu.Lock()
						w := store.receipts[lastID]
						store.mu.Unlock()
						if w == nil {
							atomic.AddInt64(&stats.businessAppliedNoReceipt, 1)
						} else {
							b := g.wait.bucket(key)
							b.mu.Lock()
							// On an error return, successful publication would have won detach.
							// A detached waiter has done=true, nil err, and no published channel value.
							published := w.done && (w.err != nil || len(w.doneC) > 0)
							b.mu.Unlock()
							if published {
								atomic.AddInt64(&stats.businessAppliedPublished, 1)
							} else {
								atomic.AddInt64(&stats.businessAppliedUnpublished, 1)
							}
						}
					}
				}
			}(node, int64(node*1000+w+1))
		}
	}
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(20 * time.Second):
		t.Fatal("load callers exceeded 20s bounded join")
	}
	return stats
}

func stressApplyDelay(max time.Duration) func() time.Duration {
	var mu sync.Mutex
	r := rand.New(rand.NewSource(42))
	return func() time.Duration {
		mu.Lock()
		defer mu.Unlock()
		return time.Duration(r.Int63n(int64(max)))
	}
}

func stopAfter(d time.Duration) <-chan struct{} {
	stop := make(chan struct{})
	time.AfterFunc(d, func() { close(stop) })
	return stop
}

// Deadlines and cancellations land around the apply latency, so completion,
// expiry and waiter release race constantly on leader and forwarded paths.
func TestApplyWaitStressDeadlinesNearApply(t *testing.T) {
	skipUnlessApplyWaitStress(t)
	c := newStressCluster(t, 4, stressApplyDelay(500*time.Microsecond))

	// Calibrate: measure successful latency under the same concurrency.
	var mu sync.Mutex
	var latencies []time.Duration
	calibration := c.run(t, stressLoad{
		workersPerNode: 32,
		stop:           stopAfter(time.Second),
		newContext: func(*rand.Rand) (context.Context, context.CancelFunc, func() time.Time) {
			return timeoutContext(2 * time.Second)
		},
		onSuccess: func(d time.Duration) {
			mu.Lock()
			latencies = append(latencies, d)
			mu.Unlock()
		},
	})
	calibration.assert(t, false)
	if calibration.leaderOK == 0 || calibration.forwardedOK == 0 {
		t.Fatal("calibration missing local/forwarded successes")
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) == 0 {
		t.Fatal("calibration produced no successful proposal")
	}
	p50 := latencies[len(latencies)/2]
	t.Logf("calibration: %d ok, p50=%v p90=%v", len(latencies), p50, latencies[len(latencies)*9/10])

	duration := 5 * time.Second
	if v := os.Getenv("RAFT_WAIT_STRESS_SECONDS"); v != "" {
		d, err := time.ParseDuration(v + "s")
		if err != nil || d < time.Second || d > 10*time.Second {
			t.Fatal("stress duration must be between 1s and 10s")
		}
		duration = d
	}
	// RAFT_WAIT_STRESS_TIMEOUT_SCALE widens deadlines so that fewer calls expire.
	timeoutScale := 1
	if v := os.Getenv("RAFT_WAIT_STRESS_TIMEOUT_SCALE"); v != "" {
		if n, err := fmt.Sscanf(v, "%d", &timeoutScale); err != nil || n != 1 || timeoutScale < 1 || timeoutScale > 10 {
			t.Fatal("timeout scale must be between 1 and 10")
		}
	}
	var okLatencies []time.Duration
	stats := c.run(t, stressLoad{
		workersPerNode: 32,
		stop:           stopAfter(duration),
		onSuccess: func(d time.Duration) {
			mu.Lock()
			okLatencies = append(okLatencies, d)
			mu.Unlock()
		},
		newContext: func(r *rand.Rand) (context.Context, context.CancelFunc, func() time.Time) {
			d := (p50/4 + time.Duration(r.Int63n(int64(p50*2)))) * time.Duration(timeoutScale)
			if r.Intn(5) == 0 {
				return cancelAfterContext(d)
			}
			return timeoutContext(d)
		},
	})
	// Every caller has returned, so no waiter may remain even if replicas
	// stalled; waiting for apply only lets late notifications drain.
	applied := c.waitQuiescent(t)
	remaining := c.remainingWaiters()
	t.Logf("load %v: %s", duration, stats)
	if n := len(okLatencies); n > 0 {
		sort.Slice(okLatencies, func(i, j int) bool { return okLatencies[i] < okLatencies[j] })
		t.Logf("load success latency: p50=%v p99=%v", okLatencies[n/2], okLatencies[n*99/100])
	}
	t.Logf("after load: allLogsApplied=%v remainingWaiters=%d", applied, remaining)
	c.explainNotApplied(t, stats)

	stats.assert(t, true)
	if !applied {
		t.Error("replicas did not converge")
	}
	if remaining != 0 {
		t.Errorf("%d apply waiters remain after every proposal returned", remaining)
	}
	if stats.lateReturn != 0 {
		t.Errorf("%d proposals returned more than 500ms after their deadline", stats.lateReturn)
	}
}

// Stop the groups while many callers wait for slow applies.
func TestApplyWaitStressStopDuringLoad(t *testing.T) {
	skipUnlessApplyWaitStress(t)
	c := newStressCluster(t, 4, func() time.Duration { return 5 * time.Millisecond })
	stop := make(chan struct{})
	started := make(chan struct{}, 64)
	done := make(chan *stressStats, 1)
	go func() {
		done <- c.run(t, stressLoad{
			workersPerNode: 32,
			stop:           stop,
			started:        started,
			newContext: func(*rand.Rand) (context.Context, context.CancelFunc, func() time.Time) {
				return timeoutContext(10 * time.Second)
			},
		})
	}()
	// Wait for all 64 callers to enter the workload, then for a real pending
	// receipt. No arbitrary warm-up sleep, and Stop itself is inside the bound.
	timer := time.NewTimer(3 * time.Second)
	for i := 0; i < 64; i++ {
		select {
		case <-started:
		case <-timer.C:
			t.Fatal("load did not start")
		}
	}
	timer.Stop()
	pendingDeadline := time.NewTimer(3 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	for c.remainingWaiters() == 0 {
		select {
		case <-ticker.C:
		case <-pendingDeadline.C:
			t.Fatal("no in-flight receipts")
		}
	}
	ticker.Stop()
	pendingDeadline.Stop()
	c.stop(t)
	close(stop)
	var stats *stressStats
	select {
	case stats = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("callers did not join after Stop")
	}
	stats.assert(t, false)
	if stats.stopped == 0 {
		t.Error("Stop did not interrupt a caller")
	}
	c.assertCleanup(t)
	remaining := c.remainingWaiters()
	t.Logf("stop during load: %s", stats)
	t.Logf("after Stop: remainingWaiters=%d", remaining)
	if remaining != 0 {
		t.Errorf("%d apply waiters remain after Stop", remaining)
	}
	if stats.okButNotApplied != 0 {
		t.Errorf("%d proposals reported success before local apply", stats.okButNotApplied)
	}
}

// explainNotApplied splits success-before-apply results, once the cluster is
// idle, into batches applied later and batches whose log index was reused by
// another proposal (the reported index now holds a different id).
func (c *stressCluster) explainNotApplied(t *testing.T, stats *stressStats) {
	t.Helper()
	later, lost := 0, 0
	for i, p := range stats.notApplied {
		_, applied := c.stores[p.node].applyTime(p.id)
		if applied {
			later++
		} else {
			lost++
		}
		if i < 3 {
			var index uint64
			if len(p.resp) > 0 && p.resp[len(p.resp)-1] != nil {
				index = p.resp[len(p.resp)-1].Index
			}
			store := c.stores[p.node]
			store.mu.Lock()
			var at types.Log
			if logs := store.logs[p.key]; index > 0 && index <= uint64(len(logs)) {
				at = logs[index-1]
			}
			store.mu.Unlock()
			t.Logf("  sample: node=%d key=%s id=%d reportedIndex=%d appliedLater=%v idAtThatIndex=%d",
				p.node+1, p.key, p.id, index, applied, at.Id)
		}
	}
	t.Logf("success-before-apply breakdown: appliedLater=%d neverApplied=%d; other errors=%v", later, lost, stats.otherErrors)
}

func (s *stressStats) assert(t *testing.T, deadlines bool) {
	t.Helper()
	if s.calls != s.ok+s.notAdmitted+s.unknown+s.rejected+s.superseded+s.other {
		t.Errorf("outcome conservation failed: %s", s)
	}
	if s.calls != s.leaderCalls+s.forwardedCalls || s.calls != s.deadlineCalls+s.cancelCalls {
		t.Errorf("coverage conservation failed: %s", s)
	}
	if s.okButNotApplied != 0 || s.other != 0 || s.lateReturn != 0 {
		t.Errorf("invalid outcomes: %s", s)
	}
	if s.leaderCalls == 0 || s.forwardedCalls == 0 {
		t.Errorf("missing local/forwarded success coverage: %s", s)
	}
	if deadlines && (s.leaderOK == 0 || s.forwardedOK == 0 || s.deadline == 0 || s.canceled == 0 || s.deadlineCalls == 0 || s.cancelCalls == 0) {
		t.Errorf("missing expiry/cancellation coverage: %s", s)
	}
	if s.appliedBeforeExpiryButFailed != s.businessAppliedUnpublished+s.businessAppliedPublished+s.businessAppliedNoReceipt {
		t.Errorf("business apply correlation did not conserve: %s", s)
	}
}
