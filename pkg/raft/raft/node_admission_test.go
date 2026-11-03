package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestTransferDrainsAdmittedWorkBeforeFixingTail(t *testing.T) {
	for _, learner := range []bool{false, true} {
		t.Run(map[bool]string{false: "follower", true: "learner"}[learner], func(t *testing.T) {
			n := transferTestLeader(t, learner)
			require.NoError(t, n.AdmitMessage(100))
			require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3, Index: 13, StoredIndex: 13, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
			require.True(t, n.drainingAdmissions)
			require.Zero(t, n.leaderTransferTo, "queued accepted work must prevent transfer")
			require.ErrorIs(t, n.AdmitMessage(101), types.ErrProposalDropped)
			require.ErrorIs(t, n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Id: 101, Term: 1, Index: 13}}}), types.ErrProposalDropped)
			require.NoError(t, n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Id: 100, Term: 1, Index: 13}}}))
			n.CompleteMessage(100)
			require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3, Index: 14, StoredIndex: 13, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
			require.Zero(t, n.leaderTransferTo, "received tail is not a stored tail")
			require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3, Index: 14, StoredIndex: 14, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
			require.Equal(t, uint64(3), n.leaderTransferTo)
		})
	}
}

func TestAdmissionBoundAndAuthoritativeCancellation(t *testing.T) {
	n := transferTestLeader(t, true)
	for i := 1; i <= maxPendingAdmissions; i++ {
		require.NoError(t, n.AdmitMessage(uint64(i)))
	}
	require.ErrorIs(t, n.AdmitMessage(maxPendingAdmissions+1), types.ErrProposalDropped)
	n.CompleteMessage(1)
	n.CompleteMessage(1) // completion is idempotent
	require.NoError(t, n.AdmitMessage(maxPendingAdmissions+1))
	n.drainingAdmissions = true
	cfg := n.Config().Clone()
	cfg.Version++
	cfg.MigrateFrom, cfg.MigrateTo = 0, 0
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	n.CompleteMessage(2)
	require.NoError(t, n.AdmitMessage(maxPendingAdmissions+2), "only a newer authoritative cancellation reopens admission")
}

func TestPrePublicationDrainTimeoutResumesOldLeader(t *testing.T) {
	n := transferTestLeader(t, true)
	require.NoError(t, n.AdmitMessage(100))
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3, Index: 13, StoredIndex: 13, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
	require.ErrorIs(t, n.AdmitMessage(101), types.ErrProposalDropped)
	// The target disappears before a transfer request was emitted.
	n.CompleteMessage(100)
	for i := 0; i < maxAdmissionDrainTicks; i++ {
		n.tickAdmissionDrain()
	}
	require.NoError(t, n.AdmitMessage(101))
	require.Zero(t, n.leaderTransferTo)
	require.Positive(t, n.admissionDrainCooldown)
}

func TestUnknownPublishedTransferNeverTimesOutIntoTwoWriters(t *testing.T) {
	n := transferTestLeader(t, true)
	e := beginTestTransfer(t, n)
	require.NoError(t, n.Step(types.Event{Type: types.LearnerToLeaderResp, From: 3, Term: e.Term, ConfigVersion: e.ConfigVersion, Reason: types.ReasonError}))
	for i := 0; i < maxAdmissionDrainTicks*3; i++ {
		n.tickAdmissionDrain()
	}
	require.ErrorIs(t, n.AdmitMessage(101), types.ErrProposalDropped)
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("unknown commit"))), types.ErrProposalDropped)
}
