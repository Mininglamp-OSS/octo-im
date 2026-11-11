package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestAdmissionDrainCancellationAndReplacementPreserveAcceptedWork(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "replace"}[replace], func(t *testing.T) {
			n := transferTestLeader(t, true)
			require.NoError(t, n.AdmitMessage(100))
			require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3, Index: 13, StoredIndex: 13, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
			require.True(t, n.drainingAdmissions)
			require.False(t, n.stopPropose, "accepted work must remain writable while draining")
			cfg := n.Config().Clone()
			cfg.Version++
			cfg.MigrateFrom, cfg.MigrateTo, cfg.Learners = 0, 0, nil
			if replace {
				cfg.MigrateFrom, cfg.MigrateTo, cfg.Learners = 1, 4, []uint64{4}
			}
			require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
			require.False(t, n.drainingAdmissions)
			require.False(t, n.stopPropose)
			require.Contains(t, n.messageAdmissions, uint64(100), "cancelling a transfer does not cancel accepted work")
			require.NoError(t, n.AdmitMessage(101))
			if replace {
				require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 4, Index: 13, StoredIndex: 13, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
				require.Equal(t, uint64(4), n.handoff.to)
				require.Zero(t, n.handoff.requestID)
				require.False(t, n.stopPropose)
			}
			require.NoError(t, n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Id: 100, Term: 1, Index: 13}}}))
			n.CompleteMessage(100)
			require.NoError(t, n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Id: 101, Term: 1, Index: 14}}}))
			n.CompleteMessage(101)
			if replace {
				require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 4, Index: 15, StoredIndex: 15, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
				require.True(t, n.handoff.attempted)
				require.True(t, n.stopPropose)
			}
		})
	}
}

func TestAdmissionDrainVersionRefreshDoesNotPauseAcceptedWork(t *testing.T) {
	n := transferTestLeader(t, true)
	require.NoError(t, n.AdmitMessage(100))
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3, Index: 13, StoredIndex: 13, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
	cfg := n.Config().Clone()
	cfg.Version++
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.True(t, n.drainingAdmissions)
	require.False(t, n.stopPropose)
	require.ErrorIs(t, n.AdmitMessage(101), types.ErrProposalDropped)
	require.NoError(t, n.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Id: 100, Term: 1, Index: 13}}}))
	n.CompleteMessage(100)
	require.NoError(t, n.Step(types.Event{Type: types.SyncReq, Term: 1, From: 3, Index: 14, StoredIndex: 14, LastLogTerm: 1, Reason: types.ReasonOnlySync}))
	require.True(t, n.handoff.attempted)
	require.Equal(t, cfg.Version, n.handoff.version)
}

func TestPublishedHandoffRefreshAndDelayedFailureCannotExpireAdmissionFence(t *testing.T) {
	n := transferTestLeader(t, true)
	old := beginTestTransfer(t, n)
	cfg := n.Config().Clone()
	cfg.Version++
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.NoError(t, n.Step(types.Event{Type: types.LearnerToLeaderResp, From: old.From, Term: old.Term,
		ConfigVersion: old.ConfigVersion, RoleChangeID: old.RoleChangeID, Reason: types.ReasonError}))
	for i := 0; i < maxAdmissionDrainTicks*3; i++ {
		n.tickAdmissionDrain()
	}
	require.True(t, n.handoff.attempted)
	require.True(t, n.stopPropose)
	require.ErrorIs(t, n.AdmitMessage(100), types.ErrProposalDropped)
	retry := beginTestTransfer(t, n)
	require.Greater(t, retry.RoleChangeID, old.RoleChangeID)
	require.Equal(t, cfg.Version, retry.ConfigVersion)
	require.NoError(t, n.Step(types.Event{Type: types.LearnerToLeaderResp, From: retry.From, Term: retry.Term,
		ConfigVersion: retry.ConfigVersion, RoleChangeID: retry.RoleChangeID, Reason: types.ReasonError}))
	for i := 0; i < maxAdmissionDrainTicks*3; i++ {
		n.tickAdmissionDrain()
	}
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("unknown saved config"))), types.ErrProposalDropped)
	cfg.Version++
	cfg.MigrateFrom, cfg.MigrateTo, cfg.Learners = 0, 0, nil
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Nil(t, n.handoff)
	require.False(t, n.drainingAdmissions)
	require.NoError(t, n.AdmitMessage(100), "only authoritative cancellation reopens both fences")
}
