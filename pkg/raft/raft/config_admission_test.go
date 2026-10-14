package raft

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// Adapted from the accepted XIN-2082 scheduling driver. Channels and mutex
// rendezvous establish ordering; deadlines only bound deadlocks.
type studyResult struct {
	resps []*types.ProposeResp
	err   error
}
type studyStorage struct {
	memStorage
	applied   []types.Log
	terms     map[uint32]uint64
	applyGate chan struct{}
}

func (s *studyStorage) AppendLogs(logs []types.Log, term *types.TermStartIndexInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = append(s.logs, logs...)
	if term != nil {
		s.terms[term.Term] = term.Index
	}
	return nil
}
func (s *studyStorage) GetLogs(start, end, limit uint64) ([]types.Log, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []types.Log
	for _, l := range s.logs {
		if l.Index >= start && l.Index < end {
			out = append(out, l)
		}
	}
	return out, nil
}
func (s *studyStorage) GetTermStartIndex(term uint32) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.terms[term], nil
}
func (s *studyStorage) LeaderLastTerm() (uint32, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var max uint32
	for term := range s.terms {
		if term > max {
			max = term
		}
	}
	return max, nil
}
func (s *studyStorage) LeaderTermGreaterEqThan(term uint32) (uint32, error) { return term, nil }
func (s *studyStorage) Apply(logs []types.Log) error {
	if s.applyGate != nil {
		<-s.applyGate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, logs...)
	return nil
}
func (s *studyStorage) snapshot() ([]types.Log, []types.Log) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]types.Log(nil), s.logs...), append([]types.Log(nil), s.applied...)
}

type studyTransport struct{ c chan types.Event }

func (s *studyTransport) Send(e types.Event) { s.c <- e }

type studyCluster struct {
	holdFollowerApply bool
	heldApply         *stepReq
	t                 *testing.T
	rr                []*Raft
	ss                []*studyStorage
	tr                *studyTransport
}

func studyNew(t *testing.T, multi bool) *studyCluster {
	c := &studyCluster{t: t, tr: &studyTransport{c: make(chan types.Event, 4096)}}
	replicas := []uint64{1}
	if multi {
		replicas = []uint64{1, 2, 3}
	}
	for _, id := range replicas {
		s := &studyStorage{terms: make(map[uint32]uint64)}
		r := New(NewOptions(WithNodeId(id), WithReplicas(replicas), WithStorage(s), WithTransport(c.tr)))
		if multi {
			if id == 1 {
				r.BecomeLeader(1)
			} else {
				r.BecomeFollower(1, 1)
			}
		}
		c.rr = append(c.rr, r)
		c.ss = append(c.ss, s)
	}
	t.Cleanup(func() {
		for _, s := range c.ss {
			if s.applyGate != nil {
				select {
				case <-s.applyGate:
				default:
					close(s.applyGate)
				}
			}
		}
		for _, r := range c.rr {
			select {
			case <-r.stopper.ShouldStop():
			default:
				r.Stop()
			}
			require.NoError(t, r.pool.ReleaseTimeout(5*time.Second))
			require.Zero(t, r.pool.Running())
		}
	})
	return c
}
func studyRecv[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		if result, ok := any(v).(studyResult); ok {
			t.Logf("caller result error=%v response_count=%d", result.err, len(result.resps))
			for _, resp := range result.resps {
				t.Logf("response id=%d index=%d", resp.Id, resp.Index)
			}
		}
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("barrier not reached")
		var zero T
		return zero
	}
}
func studyCount(r *Raft) int {
	r.wait.mu.RLock()
	defer r.wait.mu.RUnlock()
	return len(r.wait.progresses)
}
func studyCall(r *Raft, c context.Context, id uint64) <-chan studyResult {
	out := make(chan studyResult, 1)
	go func() {
		resp, err := r.ProposeBatchUntilAppliedTimeout(c, types.ProposeReqSet{{Id: id, Data: []byte(fmt.Sprintf("request-%d", id))}})
		out <- studyResult{resp, err}
	}()
	return out
}

// The driver is test-only loop instrumentation: it uses the production queue,
// Node.Step, readyEvents and storage/apply handlers, replacing only clock ticks
// and select scheduling. No node is also started on a background loop.
func (c *studyCluster) step(i int, req stepReq) error {
	return c.rr[i].handleStep(req)
}
func (c *studyCluster) pump(target <-chan struct{}, hold types.EventType) *types.Event {
	c.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if target != nil {
			select {
			case <-target:
				return nil
			default:
			}
		}
		for _, r := range c.rr {
			r.readyEvents()
		}
		var q2, q3 <-chan stepReq
		if len(c.rr) > 1 {
			q2 = c.rr[1].stepC
			q3 = c.rr[2].stepC
		}
		select {
		case <-target:
			return nil
		case req := <-c.rr[0].stepC:
			c.step(0, req)
		case req := <-q2:
			if c.holdFollowerApply && req.event.Type == types.ApplyResp {
				copy := req
				c.heldApply = &copy
			} else {
				c.step(1, req)
			}
		case req := <-q3:
			c.step(2, req)
		case e := <-c.tr.c:
			if e.Type == hold {
				return &e
			}
			if e.To > 0 && int(e.To) <= len(c.rr) {
				c.rr[e.To-1].Step(e)
			}
		case <-deadline:
			c.t.Fatal("driver barrier not reached")
			return nil
		}
	}
}
func (c *studyCluster) settle(index uint64) {
	c.t.Helper()
	if len(c.rr) > 1 {
		c.rr[0].node.sendPing(All)
	}
	// didApply precedes no externally exposed barrier. A queued StepWait after
	// the apply response provides a driver rendezvous; waiter count is locked.
	for i := 0; i < 10000; i++ {
		done := true
		for _, r := range c.rr {
			if r.node.queue.appliedIndex < index || studyCount(r) != 0 {
				done = false
			}
		}
		if done {
			return
		}
		c.pumpOne(index)
	}
	c.t.Fatal("apply did not settle")
}
func (c *studyCluster) pumpOne(index uint64) {
	var wake <-chan struct{}
	for _, r := range c.rr {
		r.wait.mu.RLock()
		if len(r.wait.progresses) > 0 {
			wake = r.wait.progresses[0].waitC
		}
		r.wait.mu.RUnlock()
	}
	if wake == nil {
		done := true
		for _, r := range c.rr {
			if r.node.queue.appliedIndex < index {
				done = false
			}
		}
		if done {
			return
		}
	}
	for _, r := range c.rr {
		r.readyEvents()
	}
	var q2, q3 <-chan stepReq
	if len(c.rr) > 1 {
		q2 = c.rr[1].stepC
		q3 = c.rr[2].stepC
	}
	select {
	case <-wake:
	case req := <-c.rr[0].stepC:
		c.step(0, req)
	case req := <-q2:
		c.step(1, req)
	case req := <-q3:
		c.step(2, req)
	case e := <-c.tr.c:
		if e.To > 0 && int(e.To) <= len(c.rr) {
			c.rr[e.To-1].Step(e)
		}
	case <-time.After(5 * time.Second):
		for i, r := range c.rr {
			c.t.Logf("stalled node=%d applied=%d committed=%d stored=%d wait=%d queue=%d", i+1, r.node.queue.appliedIndex, r.node.queue.committedIndex, r.node.queue.storedIndex, studyCount(r), len(r.stepC))
		}
		c.t.Fatal("settle barrier not reached")
	}
}
func (c *studyCluster) evidence(label string) {
	for i, r := range c.rr {
		stored, applied := c.ss[i].snapshot()
		c.t.Logf("%s node=%d waiters=%d stored=%+v applied=%+v", label, i+1, studyCount(r), stored, applied)
	}
}
func studyIdentity(t *testing.T, resp *types.ProposeResp, logs []types.Log, id uint64, term uint32) {
	t.Helper()
	require.Equal(t, id, resp.Id)
	for _, l := range logs {
		if l.Index == resp.Index {
			require.Equal(t, id, l.Id)
			require.Equal(t, term, l.Term)
			require.Equal(t, []byte(fmt.Sprintf("request-%d", id)), l.Data)
			return
		}
	}
	t.Fatalf("response index %d absent", resp.Index)
}
func TestConfigQueuedCancellation(t *testing.T) {
	for _, second := range []bool{false, true} {
		t.Run(fmt.Sprintf("second=%t", second), func(t *testing.T) {
			c := studyNew(t, false)
			r := c.rr[0]
			ctx, cancel := context.WithCancel(context.Background())
			out := studyCall(r, ctx, 100)
			req := studyRecv(t, r.stepC)
			if os.Getenv("CONFIG_MISPLACE_BARRIER") == "1" {
				c.step(0, req)
			}
			cancel()
			require.ErrorIs(t, studyRecv(t, out).err, context.Canceled)
			require.Zero(t, studyCount(r), "cancel must detach owned wait")
			if os.Getenv("CONFIG_MISPLACE_BARRIER") != "1" {
				c.step(0, req)
			}
			require.Zero(t, r.node.queue.lastLogIndex, "cancelled queued request must not append")
			if second {
				b := studyCall(r, context.Background(), 200)
				c.step(0, studyRecv(t, r.stepC))
				c.settle(1)
				result := studyRecv(t, b)
				require.NoError(t, result.err)
				stored, applied := c.ss[0].snapshot()
				require.Len(t, stored, 1)
				require.Len(t, applied, 1)
				studyIdentity(t, result.resps[0], stored, 200, 1)
				studyIdentity(t, result.resps[0], applied, 200, 1)
			}
		})
	}
}
func TestConfigCancelAdmitted(t *testing.T) {
	c := studyNew(t, false)
	r := c.rr[0]
	ctx, cancel := context.WithCancel(context.Background())
	out := studyCall(r, ctx, 300)
	c.step(0, studyRecv(t, r.stepC))
	cancel()
	require.ErrorIs(t, studyRecv(t, out).err, context.Canceled)
	require.Zero(t, studyCount(r), "admitted cancellation must detach owned wait")
	c.settle(1)
	stored, applied := c.ss[0].snapshot()
	require.Len(t, stored, 1)
	require.Equal(t, stored, applied)
	require.Equal(t, uint64(300), applied[0].Id)
}

func TestConfigRoleTerm(t *testing.T) {
	for _, after := range []bool{false, true} {
		for _, follower := range []bool{false, true} {
			t.Run(fmt.Sprintf("after=%t/follower=%t", after, follower), func(t *testing.T) {
				c := studyNew(t, false)
				r := c.rr[0]
				out := studyCall(r, context.Background(), 400)
				req := studyRecv(t, r.stepC)
				if after {
					c.step(0, req)
				}
				if follower {
					r.BecomeFollower(2, 2)
				} else {
					r.BecomeLeader(2)
				}
				if !after {
					c.step(0, req)
				}
				if after {
					require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
				} else if follower {
					require.ErrorIs(t, studyRecv(t, out).err, types.ErrNotLeader)
				} else {
					// Allocation belongs to admission in the new term, not stale caller state.
					c.settle(1)
					result := studyRecv(t, out)
					require.NoError(t, result.err)
					_, applied := c.ss[0].snapshot()
					studyIdentity(t, result.resps[0], applied, 400, 2)
				}
				require.Zero(t, studyCount(r))
			})
		}
	}
}
func TestConfigStop(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			c := studyNew(t, false)
			r := c.rr[0]
			out := studyCall(r, context.Background(), 600)
			req := studyRecv(t, r.stepC)
			if pending {
				c.step(0, req)
			}
			r.Stop()
			require.ErrorIs(t, studyRecv(t, out).err, types.ErrStopped)
			require.Zero(t, studyCount(r))
			if !pending {
				c.step(0, req)
				require.Zero(t, r.node.queue.lastLogIndex)
			}
		})
	}
}
func TestConfigForward(t *testing.T) {
	for _, boundary := range []string{"before_receipt", "after_receipt", "before_registration", "after_registration", "completion_before_registration", "healthy", "stop_queued", "stop_pending", "fence_queued", "fence_pending"} {
		t.Run(boundary, func(t *testing.T) {
			c := studyNew(t, true)
			r := c.rr[1]
			// Apply is deliberately ungated in healthy and completion-before-registration.
			if boundary == "after_registration" || boundary == "stop_pending" || boundary == "fence_pending" {
				c.ss[1].applyGate = make(chan struct{})
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := studyCall(r, ctx, 500)
			request := studyRecv(t, c.tr.c)
			if boundary == "before_receipt" || boundary == "stop_queued" || boundary == "fence_queued" {
				if boundary == "stop_queued" {
					r.Stop()
				} else if boundary == "fence_queued" {
					r.BecomeFollower(2, 3)
				} else {
					cancel()
				}
				require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
				require.False(t, r.fowardProposeWait.IsRegistered("500"))
				require.Zero(t, studyCount(r))
				// A delayed response cannot resurrect a detached wait.
				resp := types.ProposeRespSet{&types.ProposeResp{Id: 500, Index: 1}}
				data, _ := resp.Marshal()
				r.Step(types.Event{Type: types.SendProposeResp, From: 1, Reason: types.ReasonOk, Logs: []types.Log{{Data: data}}})
				require.Zero(t, studyCount(r))
				return
			}
			c.rr[0].Step(request)
			if boundary == "after_receipt" {
				req := studyRecv(t, c.rr[0].stepC)
				cancel()
				require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
				c.step(0, req)
			}
			response := c.pump(nil, types.SendProposeResp)
			if boundary == "completion_before_registration" {
				c.settle(1)
			}
			if boundary == "before_registration" {
				cancel()
				require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
			}
			r.Step(*response)
			if boundary == "after_registration" || boundary == "stop_pending" || boundary == "fence_pending" {
				// A mutex rendezvous (not a sleep) waits for receipt publication.
				deadline := time.After(5 * time.Second)
				for studyCount(r) == 0 {
					select {
					case <-deadline:
						t.Fatal("registration not reached")
					default:
						runtime.Gosched()
					}
				}
				if boundary == "stop_pending" {
					r.Stop()
				} else if boundary == "fence_pending" {
					r.BecomeFollower(2, 3)
				} else {
					cancel()
				}
				require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
				require.Zero(t, studyCount(r))
				close(c.ss[1].applyGate)
			}
			if boundary == "healthy" || boundary == "completion_before_registration" {
				c.settle(1)
				result := studyRecv(t, out)
				require.NoError(t, result.err)
				for _, s := range c.ss {
					stored, applied := s.snapshot()
					studyIdentity(t, result.resps[0], stored, 500, 1)
					studyIdentity(t, result.resps[0], applied, 500, 1)
				}
			}
			if boundary != "fence_pending" && boundary != "stop_pending" {
				c.settle(1)
			}
			require.False(t, r.fowardProposeWait.IsRegistered("500"))
			require.Zero(t, studyCount(r))
		})
	}
}
func TestConfigCompletionIdentity(t *testing.T) {
	for _, order := range []string{"before", "during", "after"} {
		for _, wrong := range []string{"none", "id", "term", "data"} {
			t.Run(order+"/"+wrong, func(t *testing.T) {
				c := studyNew(t, false)
				r := c.rr[0]
				want := types.Log{Id: 700, Index: 1, Term: 1, Data: []byte("own-data")}
				got := want
				switch wrong {
				case "id":
					got.Id++
				case "term":
					got.Term++
				case "data":
					got.Data = []byte("other-data")
				}
				c.ss[0].AppendLogs([]types.Log{got}, nil)
				c.ss[0].Apply([]types.Log{got})
				var p *progress
				switch order {
				case "before":
					r.wait.appliedLogs([]types.Log{got})
					p = r.wait.registerReceipt([]types.Log{want}, false)
				case "after":
					p = r.wait.registerReceipt([]types.Log{want}, false)
					r.wait.appliedLogs([]types.Log{got})
				case "during":
					r.wait.mu.Lock()
					registered := make(chan *progress, 1)
					applied := make(chan struct{})
					go func() { registered <- r.wait.registerReceipt([]types.Log{want}, false) }()
					go func() { r.wait.appliedLogs([]types.Log{got}); close(applied) }()
					r.wait.mu.Unlock()
					p = studyRecv(t, registered)
					studyRecv(t, applied)
				}
				studyRecv(t, p.waitC)
				if wrong == "none" {
					require.NoError(t, p.err)
				} else {
					require.ErrorIs(t, p.err, types.ErrProposalSuperseded)
				}
				require.Zero(t, studyCount(r))
			})
		}
	}
}
func TestConfigDuplicateForwardID(t *testing.T) {
	c := studyNew(t, true)
	r := c.rr[1]
	ctx, cancel := context.WithCancel(context.Background())
	out := studyCall(r, ctx, 900)
	studyRecv(t, c.tr.c)
	duplicate := studyCall(r, context.Background(), 900)
	require.ErrorIs(t, studyRecv(t, duplicate).err, types.ErrProposalNotAdmitted)
	cancel()
	require.ErrorIs(t, studyRecv(t, out).err, context.Canceled)
	duplicate = studyCall(r, context.Background(), 900)
	require.ErrorIs(t, studyRecv(t, duplicate).err, types.ErrProposalNotAdmitted)
	// An old response for 900 must not satisfy the different live registration.
	freshCtx, freshCancel := context.WithCancel(context.Background())
	defer freshCancel()
	fresh := studyCall(r, freshCtx, 901)
	studyRecv(t, c.tr.c)
	data, _ := types.ProposeRespSet{&types.ProposeResp{Id: 900, Index: 1}}.Marshal()
	r.Step(types.Event{Type: types.SendProposeResp, From: 1, Reason: types.ReasonOk, Logs: []types.Log{{Data: data}}})
	require.True(t, r.fowardProposeWait.IsRegistered("901"))
	freshCancel()
	require.ErrorIs(t, studyRecv(t, fresh).err, context.Canceled)
	require.False(t, r.fowardProposeWait.IsRegistered("900"))
	require.False(t, r.fowardProposeWait.IsRegistered("901"))
}

func TestConfigRealLoop(t *testing.T) {
	for _, multi := range []bool{false, true} {
		t.Run(fmt.Sprintf("multi=%t", multi), func(t *testing.T) {
			c := studyNew(t, multi)
			for _, r := range c.rr {
				require.NoError(t, r.Start())
			}
			routingDone := make(chan struct{})
			routed := make(chan struct{})
			go func() {
				defer close(routed)
				for {
					select {
					case e := <-c.tr.c:
						if e.To > 0 && int(e.To) <= len(c.rr) {
							c.rr[e.To-1].Step(e)
						}
					case <-routingDone:
						return
					}
				}
			}()
			defer func() {
				for _, r := range c.rr {
					r.Stop()
				}
				close(routingDone)
				studyRecv(t, routed)
			}()
			r := c.rr[0]
			if multi {
				r = c.rr[1]
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Two entries prove allocation and applied ownership over an admitted batch.
			reqs := types.ProposeReqSet{{Id: 1000, Data: []byte("request-1000")}, {Id: 1001, Data: []byte("request-1001")}}
			resps, err := r.ProposeBatchUntilAppliedTimeout(ctx, reqs)
			require.NoError(t, err)
			require.Len(t, resps, 2)
			stored, applied := c.ss[r.opts.NodeId-1].snapshot()
			for i, resp := range resps {
				studyIdentity(t, resp, stored, uint64(1000+i), 1)
				studyIdentity(t, resp, applied, uint64(1000+i), 1)
			}
			require.Zero(t, studyCount(r))
			// Explicit role setters must go through the real loop while it is running.
			if !multi {
				r.BecomeLeader(2)
				next, err := r.ProposeUntilAppliedTimeout(ctx, 1002, []byte("request-1002"))
				require.NoError(t, err)
				_, applied = c.ss[0].snapshot()
				studyIdentity(t, next, applied, 1002, 2)
			}
		})
	}
}

func TestConfigRejectionAndOwnership(t *testing.T) {
	c := studyNew(t, false)
	r := c.rr[0]
	good := studyCall(r, context.Background(), 1100)
	c.step(0, studyRecv(t, r.stepC))
	r.node.stopPropose = true
	bad := studyCall(r, context.Background(), 1101)
	c.step(0, studyRecv(t, r.stepC))
	require.ErrorIs(t, studyRecv(t, bad).err, types.ErrProposalDropped)
	require.Equal(t, 1, studyCount(r), "rejecting another request must preserve the admitted owner")
	r.node.stopPropose = false
	c.settle(1)
	result := studyRecv(t, good)
	require.NoError(t, result.err)
	_, applied := c.ss[0].snapshot()
	studyIdentity(t, result.resps[0], applied, 1100, 1)
	require.Zero(t, studyCount(r))
}

// This preserves the accepted missed-notification discovery boundary exactly:
// the worker completed Apply, but the follower's ApplyResp is still withheld.
func TestConfigWorkerCompletionBeforeRegistration(t *testing.T) {
	c := studyNew(t, true)
	r := c.rr[1]
	c.holdFollowerApply = true
	completion := r.wait.waitApply(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := studyCall(r, ctx, 800)
	c.rr[0].Step(studyRecv(t, c.tr.c))
	response := c.pump(nil, types.SendProposeResp)
	c.rr[0].node.sendPing(All)
	c.pump(completion.waitC, types.EventType(0xffff))
	require.Zero(t, r.node.queue.appliedIndex, "ApplyResp must still be withheld")
	require.Zero(t, studyCount(r))
	r.Step(*response)
	result := studyRecv(t, out)
	require.NoError(t, result.err)
	stored, applied := c.ss[1].snapshot()
	studyIdentity(t, result.resps[0], stored, 800, 1)
	studyIdentity(t, result.resps[0], applied, 800, 1)
	require.Zero(t, studyCount(r))
	for c.heldApply == nil {
		req := studyRecv(t, r.stepC)
		if req.event.Type == types.ApplyResp {
			c.heldApply = &req
		} else {
			c.step(1, req)
		}
	}
	c.holdFollowerApply = false
	c.step(1, *c.heldApply)
	c.settle(1)
}

func TestConfigStopPausedLoop(t *testing.T) {
	c := studyNew(t, false)
	r := c.rr[0]
	r.Pause()
	require.NoError(t, r.Start())
	done := make(chan struct{})
	go func() { r.Stop(); close(done) }()
	studyRecv(t, done)
}

func TestConfigForwardResponseValidation(t *testing.T) {
	for _, invalid := range []string{"sender", "id", "index", "length", "reject", "capacity"} {
		t.Run(invalid, func(t *testing.T) {
			c := studyNew(t, true)
			r := c.rr[1]
			if invalid == "capacity" {
				for i := 0; i < configForwardIdentityLimit; i++ {
					r.forwardUsed[fmt.Sprint(i)] = struct{}{}
				}
				require.ErrorIs(t, studyRecv(t, studyCall(r, context.Background(), 99999)).err, types.ErrProposalNotAdmitted)
				require.Empty(t, r.forwardPending)
				return
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out := studyCall(r, ctx, 1200)
			studyRecv(t, c.tr.c)
			resps := types.ProposeRespSet{&types.ProposeResp{Id: 1200, Index: 1}}
			reason := types.ReasonOk
			from := uint64(1)
			switch invalid {
			case "sender":
				from = 3
			case "id":
				resps[0].Id = 1201
			case "index":
				resps[0].Index = 0
			case "length":
				resps = append(types.ProposeRespSet{&types.ProposeResp{Id: 1199, Index: 1}}, resps...)
			case "reject":
				reason = types.ReasonError
			}
			data, _ := resps.Marshal()
			r.Step(types.Event{Type: types.SendProposeResp, From: from, Reason: reason, Logs: []types.Log{{Data: data}}})
			if invalid == "sender" || invalid == "id" {
				require.True(t, r.fowardProposeWait.IsRegistered("1200"))
				cancel()
			}
			require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
			require.False(t, r.fowardProposeWait.IsRegistered("1200"))
			require.Zero(t, studyCount(r))
		})
	}
}

func TestConfigCancelDuringStoredVerification(t *testing.T) {
	c := studyNew(t, false)
	r := c.rr[0]
	want := types.Log{Id: 1300, Index: 1, Term: 1, Data: []byte("request-1300")}
	r.wait.appliedLogs([]types.Log{want})
	reading := make(chan struct{})
	release := make(chan struct{})
	verified := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	r.wait.readLogs = func(uint64, uint64, uint64) ([]types.Log, error) {
		close(reading)
		<-release
		return []types.Log{want}, nil
	}
	// Expose completion of the production verifier on the same bounded pool.
	r.wait.verifyStored = func(p *progress) {
		require.NoError(t, r.pool.Submit(func() { r.wait.readReceipt(p); close(verified) }))
	}
	p := r.wait.registerReceipt([]types.Log{want}, true)
	studyRecv(t, reading)
	_, done := r.wait.detach(p)
	require.False(t, done)
	require.Zero(t, studyCount(r))
	// The later successful read cannot replace the detached terminal result.
	unblock()
	studyRecv(t, verified)
	require.ErrorIs(t, p.err, types.ErrProposalOutcomeUnknown)
	require.Zero(t, studyCount(r))
}
