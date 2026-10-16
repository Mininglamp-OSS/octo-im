package raftgroup

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// requireAppliedAs fails unless every returned index holds the submitted
// request on the follower: same Id and same payload.
func requireAppliedAs(t *testing.T, s *memStorage, reqs types.ProposeReqSet, resps types.ProposeRespSet) {
	t.Helper()
	require.Len(t, resps, len(reqs))
	logs, err := s.GetLogs(admKey, resps[0].Index, resps[len(resps)-1].Index+1, 0)
	require.NoError(t, err)
	require.Len(t, logs, len(reqs))
	for i, req := range reqs {
		require.Equal(t, resps[i].Index, logs[i].Index)
		require.Equal(t, req.Id, logs[i].Id, "index %d", logs[i].Index)
		require.True(t, bytes.Equal(req.Data, logs[i].Data), "index %d holds %q, submitted %q", logs[i].Index, logs[i].Data, req.Data)
	}
}

// A forwarded attempt times out after the leader admitted it. A new attempt
// reuses the last request ID with different content, and the old response
// arrives first. It must not settle the new attempt: the caller may only
// succeed for the range that holds what it submitted.
func TestForwardedStaleResponseCannotSettleReusedID(t *testing.T) {
	for name, tc := range map[string]struct{ old, next types.ProposeReqSet }{
		"batch_earlier_entry_differs": {
			old:  types.ProposeReqSet{{Id: 400, Data: []byte("old-a")}, {Id: 500, Data: []byte("b")}},
			next: types.ProposeReqSet{{Id: 400, Data: []byte("new-a")}, {Id: 500, Data: []byte("b")}},
		},
		"same_id_other_payload": {
			old:  types.ProposeReqSet{{Id: 600, Data: []byte("old")}},
			next: types.ProposeReqSet{{Id: 600, Data: []byte("new")}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			fp := newForwardPair(t, 5*time.Second)
			fp.transport.holdResps.Store(true)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := fp.follower.ProposeBatchUntilAppliedTimeout(ctx, admKey, tc.old)
			cancel()
			require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
			fp.leaderStorage.waitApplied(t, len(tc.old))

			out := make(chan error, 1)
			var resps types.ProposeRespSet
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var err error
				resps, err = fp.follower.ProposeBatchUntilAppliedTimeout(ctx, admKey, tc.next)
				out <- err
			}()
			// Both responses are held; the stale one is delivered first.
			fp.leaderStorage.waitApplied(t, len(tc.old)+len(tc.next))
			fp.transport.holdResps.Store(false)
			fp.transport.release()
			select {
			case err = <-out:
			case <-time.After(5 * time.Second):
				t.Fatal("forwarded caller did not return")
			}
			if err != nil {
				// Never a false success; an honest non-success is acceptable
				// only if it is classified.
				require.True(t, errors.Is(err, types.ErrProposalOutcomeUnknown) || errors.Is(err, types.ErrProposalSuperseded), "unclassified: %v", err)
				return
			}
			fp.followerStorage.waitApplied(t, len(tc.old)+len(tc.next))
			requireAppliedAs(t, fp.followerStorage, tc.next, resps)
		})
	}
}

// Two live forwards whose batches end in the same request ID must not share
// or crash the correlation; the second is refused before it is sent.
func TestForwardedConcurrentSameLastID(t *testing.T) {
	fp := newForwardPair(t, 5*time.Second)
	fp.transport.holdResps.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := fp.follower.ProposeBatchUntilAppliedTimeout(ctx, admKey, types.ProposeReqSet{{Id: 700, Data: []byte("x")}})
		first <- err
	}()
	fp.leaderStorage.waitApplied(t, 1)
	_, err := fp.follower.ProposeBatchUntilAppliedTimeout(context.Background(), admKey, types.ProposeReqSet{{Id: 700, Data: []byte("y")}})
	require.ErrorIs(t, err, types.ErrProposalNotAdmitted)
	fp.transport.holdResps.Store(false)
	fp.transport.release()
	require.NoError(t, <-first)
}

// A leader that predates the token answers with a zero header: the response
// settles by request ID and completes as before.
func TestForwardedLegacyLeaderCompletes(t *testing.T) {
	fp := newForwardPair(t, 5*time.Second)
	fp.transport.legacy.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reqs := types.ProposeReqSet{{Id: 800, Data: []byte("a")}, {Id: 801, Data: []byte("b")}}
	resps, err := fp.follower.ProposeBatchUntilAppliedTimeout(ctx, admKey, reqs)
	require.NoError(t, err)
	fp.followerStorage.waitApplied(t, 2)
	requireAppliedAs(t, fp.followerStorage, reqs, resps)
	st := fp.follower.ForwardStats()
	require.EqualValues(t, 1, st.LegacyResponses)
	require.EqualValues(t, 1, st.LegacySettled)
	require.Zero(t, st.Pending)
}

// With a legacy leader nothing distinguishes attempts, so a stale response
// can still reach a newer caller that reused the request ID; the per-entry
// receipt check keeps that from becoming a false success.
func TestForwardedLegacyStaleResponseIsNeverFalseSuccess(t *testing.T) {
	fp := newForwardPair(t, 5*time.Second)
	fp.transport.legacy.Store(true)
	fp.transport.holdResps.Store(true)
	old := types.ProposeReqSet{{Id: 400, Data: []byte("old-a")}, {Id: 500, Data: []byte("b")}}
	next := types.ProposeReqSet{{Id: 400, Data: []byte("new-a")}, {Id: 500, Data: []byte("b")}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err := fp.follower.ProposeBatchUntilAppliedTimeout(ctx, admKey, old)
	cancel()
	require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
	out := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := fp.follower.ProposeBatchUntilAppliedTimeout(ctx, admKey, next)
		out <- err
	}()
	fp.leaderStorage.waitApplied(t, 4)
	fp.transport.holdResps.Store(false)
	fp.transport.release()
	require.ErrorIs(t, <-out, types.ErrProposalSuperseded)
}

// Responses that do not carry the live token from the routed leader never
// settle the registration.
func TestForwardedResponseCorrelation(t *testing.T) {
	g := New(NewOptions(WithTransport(dropTransport{}), WithStorage(&memStorage{})))
	resps := types.ProposeRespSet{{Id: 900, Index: 1}}
	data, err := resps.Marshal()
	require.NoError(t, err)
	key := forwardKey(admKey, 900)
	reg, ok := g.registerForward(key, 1)
	require.True(t, ok)
	_, ok = g.registerForward(key, 1)
	require.False(t, ok, "a second live forward for the same ID is refused")
	live := reg.token.log(data)
	older := forwardToken{incarnation: reg.token.incarnation, attempt: reg.token.attempt - 1}.log(data)
	foreign := forwardToken{incarnation: reg.token.incarnation + 1, attempt: reg.token.attempt}.log(data)
	versioned := live
	versioned.Term = forwardTokenVersion + 1
	for name, e := range map[string]types.Event{
		"olderAttempt":   {From: 1, Logs: []types.Log{older}},
		"otherInstance":  {From: 1, Logs: []types.Log{foreign}},
		"unknownVersion": {From: 1, Logs: []types.Log{versioned}},
		"wrongSender":    {From: 3, Logs: []types.Log{live}},
		"legacyWrong":    {From: 3, Logs: []types.Log{{Data: data}}},
		"empty":          {From: 1},
	} {
		e.Type, e.Reason = types.SendProposeResp, types.ReasonOk
		g.handleSendProposeResp(admKey, e)
		require.EqualValues(t, 1, g.ForwardStats().Pending, name)
		select {
		case r := <-reg.resultC:
			t.Fatalf("%s settled the registration: %v", name, r)
		default:
		}
	}
	st := g.ForwardStats()
	require.EqualValues(t, 2, st.StaleResponses)
	require.EqualValues(t, 2, st.SenderMismatches)
	require.EqualValues(t, 1, st.UnknownHeader)
	require.EqualValues(t, 1, st.DuplicateRejected)
	g.handleSendProposeResp(admKey, types.Event{Type: types.SendProposeResp, From: 1, Reason: types.ReasonError, Logs: []types.Log{live}})
	r := <-reg.resultC
	require.ErrorIs(t, r.(error), types.ErrForwardRejected)
	require.ErrorIs(t, r.(error), types.ErrProposalOutcomeUnknown)
	require.Zero(t, g.ForwardStats().Pending)
	g.Stop()
	g.goPool.Release()
}

// The leader echoes the request header on success and on rejection.
func TestForwardedLeaderEchoesToken(t *testing.T) {
	fp := newForwardPair(t, 5*time.Second)
	fp.transport.holdResps.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out := make(chan error, 1)
	go func() { _, err := fp.forwardUntilApplied(ctx, 950); out <- err }()
	require.Eventually(t, func() bool {
		fp.transport.mu.Lock()
		defer fp.transport.mu.Unlock()
		return len(fp.transport.held) == 1
	}, 3*time.Second, time.Millisecond)
	fp.transport.mu.Lock()
	h := fp.transport.held[0].Logs[0]
	fp.transport.mu.Unlock()
	require.Equal(t, forwardTokenVersion, h.Term)
	require.Equal(t, fp.follower.forwardIncarnation, h.Index)
	require.NotZero(t, h.Id)
	fp.transport.holdResps.Store(false)
	fp.transport.release()
	require.NoError(t, <-out)

	// Rejection: the follower now leads nothing, the "leader" is a follower.
	data, err := types.ProposeReqSet{{Id: 951, Data: []byte("x")}}.Marshal()
	require.NoError(t, err)
	tok := forwardToken{incarnation: 7, attempt: 9}.log(data)
	fp.transport.holdResps.Store(true)
	fp.follower.handleSendPropose(admKey, types.Event{Type: types.SendPropose, From: 1, To: 2, Logs: []types.Log{tok}})
	fp.transport.mu.Lock()
	rej := fp.transport.held[len(fp.transport.held)-1]
	fp.transport.mu.Unlock()
	require.Equal(t, types.ReasonError, rej.Reason)
	require.Equal(t, tok.Term, rej.Logs[0].Term)
	require.Equal(t, tok.Index, rej.Logs[0].Index)
	require.Equal(t, tok.Id, rej.Logs[0].Id)
}
