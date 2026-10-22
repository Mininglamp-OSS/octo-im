package raft

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// legacyResponse is what a leader that predates the forwarding token sends:
// the same payload with zero header fields.
func legacyResponse(t *testing.T, request types.Event, from uint64, reason types.Reason, resps types.ProposeRespSet) types.Event {
	e := studyResponse(t, request, from, reason, resps)
	e.Logs[0] = types.Log{Data: e.Logs[0].Data}
	return e
}

func studyWaitReceipt(t *testing.T, r *Raft) {
	t.Helper()
	require.Eventually(t, func() bool { return studyCount(r) == 1 }, 5*time.Second, time.Millisecond)
}

// A follower forwards while its view is still term T; the leader has already
// moved to T+1 and assigns that term to the entry. The forwarded response
// carries no term, so the receipt must not trust the follower's view: Id,
// Index and Data identify the entry.
func TestConfigForwardReceiptLeaderTerm(t *testing.T) {
	const viewTerm, leaderTerm = 1, 2
	for _, path := range []string{"applied_batch", "stored"} {
		t.Run(path, func(t *testing.T) {
			c := studyNew(t, true)
			r := c.rr[1]
			require.EqualValues(t, viewTerm, r.node.cfg.Term)
			applied := types.Log{Id: 2100, Index: 1, Term: leaderTerm, Data: []byte("request-2100")}
			if path == "stored" {
				// Applied before the response arrives: verified from storage.
				require.NoError(t, c.ss[1].AppendLogs([]types.Log{applied}, nil))
				r.wait.appliedLogs([]types.Log{applied})
			}
			out := studyCall(r, context.Background(), 2100)
			request := studyRecv(t, c.tr.c)
			r.Step(studyResponse(t, request, 1, types.ReasonOk, types.ProposeRespSet{{Id: 2100, Index: 1}}))
			if path == "applied_batch" {
				studyWaitReceipt(t, r)
				r.wait.appliedLogs([]types.Log{applied})
			}
			result := studyRecv(t, out)
			require.NoError(t, result.err)
			require.Len(t, result.resps, 1)
			require.EqualValues(t, 1, result.resps[0].Index)
			require.Equal(t, ProposalOutcomes{OK: 1}, r.ProposalOutcomes())
			requireForwardQuiescent(t, r)
		})
	}
}

// The term is not the identity: a different entry at the receipt's index is
// still superseded even though the receipt does not compare terms.
func TestConfigForwardReceiptLeaderTermMismatch(t *testing.T) {
	for name, applied := range map[string]types.Log{
		"id":   {Id: 9999, Index: 1, Term: 2, Data: []byte("request-2101")},
		"data": {Id: 2101, Index: 1, Term: 2, Data: []byte("other")},
	} {
		t.Run(name, func(t *testing.T) {
			c := studyNew(t, true)
			r := c.rr[1]
			out := studyCall(r, context.Background(), 2101)
			request := studyRecv(t, c.tr.c)
			r.Step(studyResponse(t, request, 1, types.ReasonOk, types.ProposeRespSet{{Id: 2101, Index: 1}}))
			studyWaitReceipt(t, r)
			r.wait.appliedLogs([]types.Log{applied})
			require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalSuperseded)
		})
	}
}

// Mixed versions, upgraded follower and legacy leader: the zero-header
// response settles by request ID like the previous release, and the receipt
// is still verified against the applied entry before success is reported.
func TestConfigForwardLegacyLeaderCompletes(t *testing.T) {
	rt := newForwardRouter(t)
	rt.legacy.Store(true)
	r := rt.c.rr[1]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resps, err := r.ProposeBatchUntilAppliedTimeout(ctx, types.ProposeReqSet{{Id: 1900, Data: []byte("request-1900")}})
	require.NoError(t, err)
	require.Len(t, resps, 1)
	stored, applied := rt.c.ss[1].snapshot()
	studyIdentity(t, resps[0], stored, 1900, 1)
	studyIdentity(t, resps[0], applied, 1900, 1)
	st := requireForwardQuiescent(t, r)
	require.EqualValues(t, 1, st.LegacyResponses)
	require.EqualValues(t, 1, st.LegacySettled)
	require.Zero(t, st.UnechoedResponses)
	require.Equal(t, ProposalOutcomes{OK: 1}, r.ProposalOutcomes())
}

func TestConfigForwardLegacyResponses(t *testing.T) {
	t.Run("rejection_is_unknown_without_waiting", func(t *testing.T) {
		c := studyNew(t, true)
		r := c.rr[1]
		out := studyCall(r, context.Background(), 1910)
		request := studyRecv(t, c.tr.c)
		r.Step(legacyResponse(t, request, 1, types.ReasonError, types.ProposeRespSet{{Id: 1910}}))
		err := studyRecv(t, out).err
		require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
		require.ErrorIs(t, err, types.ErrForwardRejected)
		st := requireForwardQuiescent(t, r)
		require.EqualValues(t, 1, st.LegacySettled)
	})
	t.Run("never_a_false_success", func(t *testing.T) {
		c := studyNew(t, true)
		r := c.rr[1]
		out := studyCall(r, context.Background(), 1911)
		request := studyRecv(t, c.tr.c)
		r.Step(legacyResponse(t, request, 1, types.ReasonOk, types.ProposeRespSet{{Id: 1911, Index: 1}}))
		studyWaitReceipt(t, r)
		r.wait.appliedLogs([]types.Log{{Id: 4242, Index: 1, Term: 1, Data: []byte("request-1911")}})
		require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalSuperseded)
		requireForwardQuiescent(t, r)
	})
	t.Run("mismatched_payload", func(t *testing.T) {
		c := studyNew(t, true)
		r := c.rr[1]
		out := studyCall(r, context.Background(), 1912)
		request := studyRecv(t, c.tr.c)
		r.Step(legacyResponse(t, request, 1, types.ReasonOk, types.ProposeRespSet{{Id: 1912, Index: 0}}))
		err := studyRecv(t, out).err
		require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
		require.Contains(t, err.Error(), "mismatched forwarded response")
		requireForwardQuiescent(t, r)
	})
	t.Run("wrong_sender", func(t *testing.T) {
		c := studyNew(t, true)
		r := c.rr[1]
		ctx, cancel := context.WithCancel(context.Background())
		out := studyCall(r, ctx, 1913)
		request := studyRecv(t, c.tr.c)
		r.Step(legacyResponse(t, request, 3, types.ReasonOk, types.ProposeRespSet{{Id: 1913, Index: 1}}))
		require.True(t, r.fowardProposeWait.IsRegistered("1913"))
		require.EqualValues(t, 1, r.ForwardIdentityStats().SenderMismatches)
		cancel()
		require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
		requireForwardQuiescent(t, r)
	})
	// A leader that echoed the token of a live request in the current term is
	// token-aware; a zero-header response attributed to it cannot come from
	// that leader and is dropped. A new term clears the observation, so a
	// rolled-back leader is again served by the compatibility path.
	t.Run("token_aware_leader_until_term_change", func(t *testing.T) {
		c := studyNew(t, true)
		r := c.rr[1]
		// The echo of a live request proves the leader is token-aware.
		probe := studyCall(r, context.Background(), 1913)
		probeReq := studyRecv(t, c.tr.c)
		r.Step(studyResponse(t, probeReq, 1, types.ReasonError, types.ProposeRespSet{{Id: 1913}}))
		require.ErrorIs(t, studyRecv(t, probe).err, types.ErrForwardRejected)
		ctx, cancel := context.WithCancel(context.Background())
		out := studyCall(r, ctx, 1914)
		live := studyRecv(t, c.tr.c)
		resps := types.ProposeRespSet{{Id: 1914, Index: 1}}
		r.Step(legacyResponse(t, live, 1, types.ReasonOk, resps))
		require.True(t, r.fowardProposeWait.IsRegistered("1914"))
		st := r.ForwardIdentityStats()
		require.EqualValues(t, 1, st.UnechoedResponses)
		require.Zero(t, st.LegacySettled)
		cancel()
		require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)

		r.BecomeFollower(2, 1) // same leader, new term
		out = studyCall(r, context.Background(), 1915)
		request := studyRecv(t, c.tr.c)
		r.Step(legacyResponse(t, request, 1, types.ReasonError, types.ProposeRespSet{{Id: 1915}}))
		require.ErrorIs(t, studyRecv(t, out).err, types.ErrForwardRejected)
		st = requireForwardQuiescent(t, r)
		require.EqualValues(t, 1, st.LegacySettled)
	})
	t.Run("unknown_version_is_counted", func(t *testing.T) {
		c := studyNew(t, true)
		r := c.rr[1]
		ctx, cancel := context.WithCancel(context.Background())
		out := studyCall(r, ctx, 1916)
		request := studyRecv(t, c.tr.c)
		e := studyResponse(t, request, 1, types.ReasonOk, types.ProposeRespSet{{Id: 1916, Index: 1}})
		e.Logs[0].Term = forwardTokenVersion + 1
		r.Step(e)
		require.True(t, r.fowardProposeWait.IsRegistered("1916"))
		require.EqualValues(t, 1, r.ForwardIdentityStats().UnechoedResponses)
		cancel()
		require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
		requireForwardQuiescent(t, r)
	})
}

// A leader is rolled back to a build that predates the token and leads again
// in a later term. A version-1 echo of a request retired before the term
// change can still arrive afterwards. It matches no live request, so it must
// not mark the rolled-back leader token-aware for the new term; otherwise its
// zero-header replies are dropped and every forward ends at its deadline as
// unknown instead of being settled.
func TestConfigForwardStaleEchoAfterTermChange(t *testing.T) {
	for _, retired := range []string{"cancel", "term_change"} {
		t.Run(retired, func(t *testing.T) {
			c := studyNew(t, true)
			r := c.rr[1]
			ctxA, cancelA := context.WithCancel(context.Background())
			defer cancelA()
			outA := studyCall(r, ctxA, 2200)
			reqA := studyRecv(t, c.tr.c)
			require.Equal(t, forwardTokenVersion, reqA.Logs[0].Term)
			if retired == "cancel" {
				cancelA()
				require.ErrorIs(t, studyRecv(t, outA).err, types.ErrProposalOutcomeUnknown)
			}
			r.BecomeFollower(2, 1) // the rolled-back leader 1 leads term 2
			if retired == "term_change" {
				require.ErrorIs(t, studyRecv(t, outA).err, types.ErrProposalOutcomeUnknown)
			}
			// A's echo, read off the old connection, arrives in term 2.
			r.Step(studyResponse(t, reqA, 1, types.ReasonOk, types.ProposeRespSet{{Id: 2200, Index: 1}}))
			require.EqualValues(t, 1, r.ForwardIdentityStats().StaleResponses)

			ctxB, cancelB := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancelB()
			outB := studyCall(r, ctxB, 2201)
			reqB := studyRecv(t, c.tr.c)
			r.Step(legacyResponse(t, reqB, 1, types.ReasonError, types.ProposeRespSet{{Id: 2201}}))
			err := studyRecv(t, outB).err
			require.ErrorIs(t, err, types.ErrForwardRejected, "the legacy leader's reply must settle the forward")
			require.NotErrorIs(t, err, context.DeadlineExceeded)
			st := requireForwardQuiescent(t, r)
			require.EqualValues(t, 1, st.LegacySettled)
			require.Zero(t, st.UnechoedResponses)
		})
	}
}
