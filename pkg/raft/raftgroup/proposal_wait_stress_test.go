package raftgroup

import (
	"context"
	"errors"
	"fmt"
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
	mu         sync.Mutex
	logs       map[string][]types.Log
	termStarts map[string]types.TermStartIndexInfo
	appliedAt  map[uint64]time.Time
	applyDelay func() time.Duration
}

func newStressStorage(applyDelay func() time.Duration) *stressStorage {
	return &stressStorage{
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

type stressCluster struct {
	groups []*RaftGroup
	stores []*stressStorage
	keys   []string
}

func newStressCluster(t *testing.T, keyCount int, applyDelay func() time.Duration) *stressCluster {
	t.Helper()
	nodeIDs := []uint64{1, 2}
	tr := &stressTransport{groups: make(map[uint64]*RaftGroup)}
	c := &stressCluster{}
	for i := 0; i < keyCount; i++ {
		c.keys = append(c.keys, fmt.Sprintf("slot-%d", i))
	}
	for _, id := range nodeIDs {
		store := newStressStorage(applyDelay)
		g := New(NewOptions(WithTransport(tr), WithStorage(store), WithTickInterval(10*time.Millisecond),
			WithLogPrefix(fmt.Sprintf("stress-%d", id))))
		tr.groups[id] = g
		c.groups = append(c.groups, g)
		c.stores = append(c.stores, store)
	}
	for i, id := range nodeIDs {
		g := c.groups[i]
		if err := g.Start(); err != nil {
			t.Fatal(err)
		}
		for _, key := range c.keys {
			g.AddRaft(raft.NewNode(0, types.RaftState{}, raft.NewOptions(
				raft.WithNodeId(id), raft.WithKey(key), raft.WithElectionOn(true),
				raft.WithElectionInterval(5), raft.WithReplicas(nodeIDs), raft.WithAdvance(g.Advance))))
		}
	}
	t.Cleanup(func() {
		for _, g := range c.groups {
			select {
			case <-g.stopper.ShouldStop():
			default:
				g.Stop()
			}
			g.goPool.Release()
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for _, key := range c.keys {
		for {
			l1, l2 := c.groups[0].GetRaft(key).LeaderId(), c.groups[1].GetRaft(key).LeaderId()
			if l1 != 0 && l1 == l2 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("no stable leader for %s", key)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return c
}

// waitQuiescent waits until every replica has applied everything it stored.
// It reports false when replicas stop making progress.
func (c *stressCluster) waitQuiescent(t *testing.T) bool {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		idle := true
		for _, g := range c.groups {
			for _, key := range c.keys {
				r := g.GetRaft(key)
				if r.AppliedIndex() != r.LastLogIndex() || r.CommittedIndex() != r.LastLogIndex() {
					idle = false
				}
			}
		}
		if idle {
			time.Sleep(50 * time.Millisecond) // let trailing didApply calls finish
			return true
		}
		if time.Now().After(deadline) {
			for i, g := range c.groups {
				for _, key := range c.keys {
					r := g.GetRaft(key)
					t.Logf("  node=%d %s last=%d committed=%d applied=%d", i+1, key, r.LastLogIndex(), r.CommittedIndex(), r.AppliedIndex())
				}
			}
			return false
		}
		time.Sleep(10 * time.Millisecond)
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
	calls, ok, deadline, canceled, stopped, other int64
	// This node applied the batch before the context expired, yet the call
	// reported a timeout or cancellation.
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
		s.calls, s.ok, s.deadline, s.canceled, s.stopped, s.other, s.appliedBeforeExpiryButFailed, s.okButNotApplied, s.lateReturn)
}

type stressLoad struct {
	workersPerNode int
	stop           <-chan struct{}
	// newContext returns the call context and a function reporting when it
	// expired (zero while it has not).
	newContext func(r *rand.Rand) (context.Context, context.CancelFunc, func() time.Time)
	onSuccess  func(time.Duration)
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
	timer := time.AfterFunc(d, func() {
		firedAt.Store(time.Now().UnixNano())
		cancel()
	})
	return ctx, func() { timer.Stop(); cancel() }, func() time.Time {
		if at := firedAt.Load(); at != 0 {
			return time.Unix(0, at)
		}
		return time.Time{}
	}
}

var stressProposalID atomic.Uint64

func (c *stressCluster) run(load stressLoad) *stressStats {
	stats := &stressStats{otherErrors: make(map[string]int)}
	var wg sync.WaitGroup
	for node := range c.groups {
		for w := 0; w < load.workersPerNode; w++ {
			wg.Add(1)
			go func(node int, seed int64) {
				defer wg.Done()
				r := rand.New(rand.NewSource(seed))
				g, store := c.groups[node], c.stores[node]
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
					switch {
					case err == nil:
						atomic.AddInt64(&stats.ok, 1)
						if !applied || appliedAt.After(returned) || len(resps) != len(reqs) {
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
						atomic.AddInt64(&stats.other, 1)
						stats.mu.Lock()
						stats.otherErrors[err.Error()]++
						stats.mu.Unlock()
					}
					if err != nil && applied && !expired.IsZero() && appliedAt.Before(expired) {
						atomic.AddInt64(&stats.appliedBeforeExpiryButFailed, 1)
					}
				}
			}(node, int64(node*1000+w+1))
		}
	}
	wg.Wait()
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
	c.run(stressLoad{
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
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) == 0 {
		t.Fatal("calibration produced no successful proposal")
	}
	p50 := latencies[len(latencies)/2]
	t.Logf("calibration: %d ok, p50=%v p90=%v", len(latencies), p50, latencies[len(latencies)*9/10])

	duration := 5 * time.Second
	if v := os.Getenv("RAFT_WAIT_STRESS_SECONDS"); v != "" {
		if d, err := time.ParseDuration(v + "s"); err == nil {
			duration = d
		}
	}
	// RAFT_WAIT_STRESS_TIMEOUT_SCALE widens deadlines so that fewer calls expire.
	timeoutScale := 1
	if v := os.Getenv("RAFT_WAIT_STRESS_TIMEOUT_SCALE"); v != "" {
		fmt.Sscanf(v, "%d", &timeoutScale)
	}
	var okLatencies []time.Duration
	stats := c.run(stressLoad{
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
	done := make(chan *stressStats, 1)
	go func() {
		done <- c.run(stressLoad{
			workersPerNode: 32,
			stop:           stop,
			newContext: func(*rand.Rand) (context.Context, context.CancelFunc, func() time.Time) {
				return timeoutContext(10 * time.Second)
			},
		})
	}()
	time.Sleep(500 * time.Millisecond)
	for _, g := range c.groups {
		g.Stop()
	}
	close(stop)
	var stats *stressStats
	select {
	case stats = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("callers did not return after Stop")
	}
	time.Sleep(100 * time.Millisecond) // let in-flight applies notify
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
			index := p.resp[len(p.resp)-1].Index
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
