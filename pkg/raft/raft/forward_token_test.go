package raft

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/bwmarrin/snowflake"
	"github.com/stretchr/testify/require"
)

// forwardRouter runs the real loops of a three-node study cluster and routes
// transport events. While rejecting, SendPropose is diverted to a non-leader
// Raft whose production handler answers with an echoed rejection.
type forwardRouter struct {
	c         *studyCluster
	rejector  *Raft
	rejectTr  *studyTransport
	rejecting atomic.Bool
	legacy    atomic.Bool
	// handlerRejects counts rejections routed while rejecting.
	handlerRejects atomic.Int64
	done           chan struct{}
	exited         chan struct{}
}

func newForwardRouter(t *testing.T) *forwardRouter {
	c := studyNew(t, true)
	rt := &forwardRouter{c: c, rejectTr: &studyTransport{c: make(chan types.Event, 4096)}, done: make(chan struct{}), exited: make(chan struct{})}
	s := &studyStorage{terms: make(map[uint32]uint64)}
	rt.rejector = New(NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2, 3}), WithStorage(s), WithTransport(rt.rejectTr)))
	rt.rejector.BecomeFollower(1, 3)
	for _, r := range c.rr {
		require.NoError(t, r.Start())
	}
	go rt.loop()
	t.Cleanup(func() {
		for _, r := range c.rr {
			r.Stop()
		}
		rt.rejector.Stop()
		close(rt.done)
		<-rt.exited
	})
	return rt
}

func (rt *forwardRouter) loop() {
	defer close(rt.exited)
	for {
		select {
		case e := <-rt.c.tr.c:
			if e.Type == types.SendPropose && e.To == 1 && rt.rejecting.Load() {
				// The production handler answers the first rejections; later ones
				// are echoed directly to keep error logging off the hot loop.
				if rt.handlerRejects.Add(1) <= 1000 {
					rt.rejector.handleSendPropose(e)
				} else {
					rt.c.rr[e.From-1].Step(rejectEcho(e))
				}
				continue
			}
			if e.Type == types.SendProposeResp && rt.legacy.Load() {
				// A leader that predates the token answers with zero header fields.
				e.Logs = []types.Log{{Data: e.Logs[0].Data}}
			}
			if e.To > 0 && int(e.To) <= len(rt.c.rr) {
				rt.c.rr[e.To-1].Step(e)
			}
		case e := <-rt.rejectTr.c:
			rt.c.rr[e.To-1].Step(e)
		case <-rt.done:
			return
		}
	}
}

// rejectEcho mirrors sendProposeRespError without its error log.
func rejectEcho(e types.Event) types.Event {
	var reqs types.ProposeReqSet
	if err := reqs.Unmarshal(e.Logs[0].Data); err != nil {
		panic(err)
	}
	resps := types.ProposeRespSet{}
	for _, req := range reqs {
		resps = append(resps, &types.ProposeResp{Id: req.Id})
	}
	data, err := resps.Marshal()
	if err != nil {
		panic(err)
	}
	return types.Event{Type: types.SendProposeResp, From: 1, To: e.From, Reason: types.ReasonError, Logs: []types.Log{echoForwardToken(e, data)}}
}

func requireForwardQuiescent(t *testing.T, r *Raft) ForwardIdentityStats {
	t.Helper()
	st := r.ForwardIdentityStats()
	require.Zero(t, st.Pending)
	require.Equal(t, st.Registered, st.Retired, "every registration must retire exactly once")
	r.forwardMu.Lock()
	require.Empty(t, r.forwardPending)
	r.forwardMu.Unlock()
	require.Zero(t, studyCount(r))
	return st
}

// More identities than the former 65,536 lifetime ceiling are rejected through
// the production leader handler, then a fresh never-used identity succeeds end
// to end. IDs come from the same snowflake generator production uses.
func TestConfigForwardRecoversBeyondFormerCeiling(t *testing.T) {
	rt := newForwardRouter(t)
	r := rt.c.rr[1]
	node, err := snowflake.NewNode(2)
	require.NoError(t, err)
	const workers = 8
	const total = 65536 + 4464
	rt.rejecting.Store(true)
	var next atomic.Int64
	var wg sync.WaitGroup
	var rejected atomic.Int64
	var failure atomic.Value
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for next.Add(1) <= total {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				id := uint64(node.Generate().Int64())
				_, err := r.ProposeBatchUntilAppliedTimeout(ctx, types.ProposeReqSet{{Id: id, Data: []byte("retry")}})
				cancel()
				if err == nil || !errorsIsAll(err, types.ErrProposalOutcomeUnknown, types.ErrForwardRejected) {
					failure.CompareAndSwap(nil, fmt.Sprintf("id=%d err=%v", id, err))
					return
				}
				rejected.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Nil(t, failure.Load())
	require.EqualValues(t, total, rejected.Load())
	st := requireForwardQuiescent(t, r)
	require.EqualValues(t, total, st.Registered)
	require.LessOrEqual(t, st.PendingHighWater, uint64(workers), "live state is bounded by concurrent callers")
	require.Zero(t, st.LimitRejected)
	require.Equal(t, ProposalOutcomes{Unknown: total}, r.ProposalOutcomes())
	t.Logf("rejected=%d registered=%d retired=%d high_water=%d", rejected.Load(), st.Registered, st.Retired, st.PendingHighWater)

	// The leader recovers; a fresh identity is admitted, applied and verified.
	rt.rejecting.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := uint64(node.Generate().Int64())
	resps, err := r.ProposeBatchUntilAppliedTimeout(ctx, types.ProposeReqSet{{Id: id, Data: []byte(fmt.Sprintf("request-%d", id))}})
	require.NoError(t, err)
	require.Len(t, resps, 1)
	require.Equal(t, id, resps[0].Id)
	stored, applied := rt.c.ss[1].snapshot()
	studyIdentity(t, resps[0], stored, id, 1)
	studyIdentity(t, resps[0], applied, id, 1)
	st = requireForwardQuiescent(t, r)
	require.EqualValues(t, total+1, st.Registered)
	require.Equal(t, ProposalOutcomes{OK: 1, Unknown: total}, r.ProposalOutcomes())
}

func errorsIsAll(err error, targets ...error) bool {
	for _, target := range targets {
		if !errors.Is(err, target) {
			return false
		}
	}
	return true
}

// Stale, foreign, unechoed and wrong-sender responses never settle or retire a
// live registration; only the exact echoed token from the routed leader does.
func TestConfigForwardTokenMismatch(t *testing.T) {
	c := studyNew(t, true)
	r := c.rr[1]
	other := New(NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3}), WithStorage(&studyStorage{terms: make(map[uint32]uint64)}), WithTransport(c.tr)))
	require.NotEqual(t, r.forwardToken.incarnation, other.forwardToken.incarnation, "a second instance with the same NodeId has its own incarnation")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstCtx, firstCancel := context.WithCancel(context.Background())
	first := studyCall(r, firstCtx, 1500)
	older := studyRecv(t, c.tr.c)
	firstCancel()
	require.ErrorIs(t, studyRecv(t, first).err, types.ErrProposalOutcomeUnknown)
	out := studyCall(r, ctx, 1500)
	live := studyRecv(t, c.tr.c)
	require.Equal(t, forwardTokenVersion, live.Logs[0].Term)
	require.Equal(t, r.forwardToken.incarnation, live.Logs[0].Index)
	require.Greater(t, live.Logs[0].Id, older.Logs[0].Id)

	resps := types.ProposeRespSet{&types.ProposeResp{Id: 1500, Index: 1}}
	unechoed := studyResponse(t, live, 1, types.ReasonOk, resps)
	unechoed.Logs[0] = types.Log{Data: unechoed.Logs[0].Data}
	foreign := studyResponse(t, live, 1, types.ReasonOk, resps)
	foreign.Logs[0].Index = other.forwardToken.incarnation
	versioned := studyResponse(t, live, 1, types.ReasonOk, resps)
	versioned.Logs[0].Term = forwardTokenVersion + 1
	for name, e := range map[string]types.Event{
		"unechoed":       unechoed,
		"unknownVersion": versioned,
		"olderAttempt":   studyResponse(t, older, 1, types.ReasonOk, resps),
		"otherInstance":  foreign,
		"wrongSender":    studyResponse(t, live, 3, types.ReasonOk, resps),
		"rejectedOlder":  studyResponse(t, older, 1, types.ReasonError, resps),
	} {
		r.Step(e)
		require.True(t, r.fowardProposeWait.IsRegistered("1500"), name)
		require.EqualValues(t, 1, r.ForwardIdentityStats().Pending, name)
	}
	st := r.ForwardIdentityStats()
	require.EqualValues(t, 2, st.UnechoedResponses)
	require.EqualValues(t, 3, st.StaleResponses)
	require.EqualValues(t, 1, st.SenderMismatches)
	select {
	case result := <-out:
		t.Fatalf("mismatched response settled the caller: %v", result.err)
	default:
	}
	r.Step(studyResponse(t, live, 1, types.ReasonError, resps))
	result := studyRecv(t, out)
	require.ErrorIs(t, result.err, types.ErrProposalOutcomeUnknown)
	require.ErrorIs(t, result.err, types.ErrForwardRejected)
	requireForwardQuiescent(t, r)
	other.Stop()
}

// Each settlement cause retires the registration and frees the ID for a new token.
func TestConfigForwardRetirementCauses(t *testing.T) {
	for _, cause := range []string{"cancel", "deadline", "response", "generation", "term", "stop"} {
		t.Run(cause, func(t *testing.T) {
			c := studyNew(t, true)
			r := c.rr[1]
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if cause == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
			}
			out := studyCall(r, ctx, 1600)
			request := studyRecv(t, c.tr.c)
			switch cause {
			case "cancel":
				cancel()
			case "response":
				r.Step(studyResponse(t, request, 1, types.ReasonError, types.ProposeRespSet{&types.ProposeResp{Id: 1600, Index: 1}}))
			case "generation":
				r.BecomeFollower(1, 3) // same term, new leader
			case "term":
				r.BecomeFollower(2, 1) // same leader, new term
			case "stop":
				r.Stop()
			}
			require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
			st := requireForwardQuiescent(t, r)
			require.EqualValues(t, 1, st.Registered)
			// The retired token cannot settle anything any more.
			r.Step(studyResponse(t, request, request.To, types.ReasonOk, types.ProposeRespSet{&types.ProposeResp{Id: 1600, Index: 1}}))
			if cause == "stop" {
				require.ErrorIs(t, studyRecv(t, studyCall(r, context.Background(), 1600)).err, types.ErrProposalNotAdmitted)
				return
			}
			require.EqualValues(t, 1, r.ForwardIdentityStats().StaleResponses)
			againCtx, againCancel := context.WithCancel(context.Background())
			again := studyCall(r, againCtx, 1600)
			next := studyRecv(t, c.tr.c)
			require.Greater(t, next.Logs[0].Id, request.Logs[0].Id)
			againCancel()
			require.ErrorIs(t, studyRecv(t, again).err, types.ErrProposalOutcomeUnknown)
			requireForwardQuiescent(t, r)
		})
	}
}

// The attempt counter never wraps; exhaustion fails closed before any send.
func TestConfigForwardAttemptExhaustion(t *testing.T) {
	c := studyNew(t, true)
	r := c.rr[1]
	r.forwardMu.Lock()
	r.forwardToken.next = math.MaxUint64
	r.forwardMu.Unlock()
	err := studyRecv(t, studyCall(r, context.Background(), 1700)).err
	require.ErrorIs(t, err, types.ErrProposalNotAdmitted)
	require.Contains(t, err.Error(), "attempt space exhausted")
	require.Empty(t, c.tr.c)
	st := requireForwardQuiescent(t, r)
	require.Zero(t, st.Registered)
	require.EqualValues(t, 1, st.LimitRejected)
}

// A restarted instance on the same storage draws a new incarnation, so an
// in-flight response for the previous incarnation completes nothing even when
// request ID and attempt number collide.
func TestConfigForwardRestartIncarnation(t *testing.T) {
	c := studyNew(t, true)
	old := c.rr[1]
	out := studyCall(old, context.Background(), 1800)
	stale := studyRecv(t, c.tr.c)
	old.Stop()
	require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)

	restarted := New(NewOptions(WithNodeId(2), WithReplicas([]uint64{1, 2, 3}), WithStorage(c.ss[1]), WithTransport(c.tr)))
	restarted.BecomeFollower(1, 1)
	defer restarted.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	live := studyCall(restarted, ctx, 1800)
	request := studyRecv(t, c.tr.c)
	require.Equal(t, stale.Logs[0].Id, request.Logs[0].Id, "attempt numbers restart")
	require.NotEqual(t, stale.Logs[0].Index, request.Logs[0].Index, "incarnations differ")
	restarted.Step(studyResponse(t, stale, 1, types.ReasonOk, types.ProposeRespSet{&types.ProposeResp{Id: 1800, Index: 1}}))
	require.True(t, restarted.fowardProposeWait.IsRegistered("1800"))
	require.EqualValues(t, 1, restarted.ForwardIdentityStats().StaleResponses)
	cancel()
	require.ErrorIs(t, studyRecv(t, live).err, types.ErrProposalOutcomeUnknown)
	requireForwardQuiescent(t, restarted)
}

// Mixed versions: an upgraded follower facing a legacy leader never reports a
// false outcome and ends at its deadline; a legacy follower facing an upgraded
// leader receives the zero header it sent.
func TestConfigForwardMixedVersion(t *testing.T) {
	t.Run("legacy_leader", func(t *testing.T) {
		rt := newForwardRouter(t)
		rt.legacy.Store(true)
		r := rt.c.rr[1]
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, err := r.ProposeBatchUntilAppliedTimeout(ctx, types.ProposeReqSet{{Id: 1900, Data: []byte("mixed")}})
		require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		st := requireForwardQuiescent(t, r)
		require.EqualValues(t, 1, st.UnechoedResponses)
		require.Equal(t, ProposalOutcomes{Unknown: 1}, r.ProposalOutcomes())
	})
	t.Run("legacy_follower", func(t *testing.T) {
		c := studyNew(t, true)
		leader := c.rr[0]
		data, err := types.ProposeReqSet{{Id: 1901, Data: []byte("legacy")}}.Marshal()
		require.NoError(t, err)
		leader.BecomeFollower(1, 3) // answer synchronously through the error path
		leader.handleSendPropose(types.Event{Type: types.SendPropose, From: 2, To: 1, Logs: []types.Log{{Data: data}}})
		resp := studyRecv(t, c.tr.c)
		require.Equal(t, types.SendProposeResp, resp.Type)
		require.Equal(t, types.ReasonError, resp.Reason)
		require.Zero(t, resp.Logs[0].Id)
		require.Zero(t, resp.Logs[0].Index)
		require.Zero(t, resp.Logs[0].Term)
		var resps types.ProposeRespSet
		require.NoError(t, resps.Unmarshal(resp.Logs[0].Data))
		require.EqualValues(t, 1901, resps[0].Id)
		// The token header survives the unchanged event encoding.
		echoed := types.Event{Type: types.SendProposeResp, From: 1, To: 2, Logs: []types.Log{forwardToken{incarnation: 7, attempt: 9}.log(resp.Logs[0].Data)}}
		raw, err := echoed.Marshal()
		require.NoError(t, err)
		var decoded types.Event
		require.NoError(t, decoded.Unmarshal(raw))
		require.Equal(t, echoed.Logs[0].Id, decoded.Logs[0].Id)
		require.Equal(t, echoed.Logs[0].Index, decoded.Logs[0].Index)
		require.Equal(t, echoed.Logs[0].Term, decoded.Logs[0].Term)
	})
}
