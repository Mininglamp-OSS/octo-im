package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestMessageConfirmationAndHTTPAdmissionBothDrainBeforeTransfer(t *testing.T) {
	for _, test := range []struct {
		name     string
		learner  bool
		httpLast bool
	}{
		{name: "follower/confirmation-last"},
		{name: "learner/http-last", learner: true, httpLast: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			n := transferTestLeader(t, test.learner)
			require.NoError(t, n.Step(n.NewPropose([]byte("TCP message"))))
			confirmation := n.BeginMessageConfirmation()
			storeMessageConfirmationTail(t, n)
			require.NoError(t, n.AdmitMessage(100))

			progress := types.Event{Type: types.SyncReq, From: 3, Term: 1, ConfigVersion: n.Config().Version,
				LastLogTerm: 1, Reason: types.ReasonOnlySync}
			progress.Index, progress.StoredIndex = n.LastLogIndex()+1, n.LastLogIndex()+1
			require.NoError(t, n.Step(progress))
			require.True(t, n.drainingAdmissions)
			require.False(t, n.handoff.attempted)
			require.True(t, n.LeaderReadReady(), "accepted proposals still need to finish confirmation")
			require.ErrorIs(t, n.Step(n.NewPropose([]byte("late TCP message"))), types.ErrProposalDropped)
			require.ErrorIs(t, n.AdmitMessage(101), types.ErrProposalDropped)

			httpProposal := n.NewPropose([]byte("already admitted HTTP message"))
			httpProposal.Logs[0].Id = 100
			require.NoError(t, n.Step(httpProposal))
			httpConfirmation := n.BeginMessageConfirmation()
			storeMessageConfirmationTail(t, n)
			n.CompleteMessageConfirmation(httpConfirmation)

			if test.httpLast {
				n.CompleteMessageConfirmation(confirmation)
			} else {
				n.CompleteMessage(100)
			}
			progress.Index = n.LastLogIndex() + 1
			require.NoError(t, n.Step(progress))
			require.False(t, n.handoff.attempted, "either kind of accepted work must hold the transfer")
			require.True(t, n.LeaderReadReady())

			if test.httpLast {
				n.CompleteMessage(100)
			} else {
				n.CompleteMessageConfirmation(confirmation)
			}
			require.False(t, n.handoff.attempted, "completion alone does not prove the target has stored the tail")
			require.NoError(t, n.Step(progress))
			require.False(t, n.handoff.attempted, "the target's received tail is insufficient")
			progress.StoredIndex = progress.Index
			require.NoError(t, n.Step(progress))
			require.True(t, n.handoff.attempted)
			require.Equal(t, uint64(3), n.handoff.to)
			require.NotZero(t, n.handoff.requestID)
			require.True(t, n.stopPropose)
		})
	}
}

func storeMessageConfirmationTail(t *testing.T, n *Node) {
	t.Helper()
	request, ok := findEvent(n.Ready(), types.StoreReq)
	require.True(t, ok)
	require.Equal(t, n.LastLogIndex(), request.Logs[len(request.Logs)-1].Index)
	require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Index: n.LastLogIndex(), Reason: types.ReasonOk}))
	n.Ready()
}

func TestMessageConfirmationFencesSameTermRoleReset(t *testing.T) {
	n := transferTestLeader(t, false)
	require.NoError(t, n.Step(n.NewPropose([]byte("old generation"))))
	old := n.BeginMessageConfirmation()

	cfg := n.Config().Clone()
	cfg.Version++
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.True(t, n.MessageConfirmationActive(old), "a version-only change keeps the active leader")
	n.BecomeFollower(cfg.Term, 2)
	n.BecomeLeader(cfg.Term)
	require.Equal(t, cfg.Term, n.Config().Term)
	require.False(t, n.MessageConfirmationActive(old))
	require.False(t, n.hasPendingHandoffWork())

	n.Ready()
	require.NoError(t, n.Step(n.NewPropose([]byte("new generation"))))
	current := n.BeginMessageConfirmation()
	require.False(t, n.CompleteMessageConfirmation(old))
	old.Abandon()
	old.Abandon()
	require.True(t, n.MessageConfirmationActive(current))
	require.EqualValues(t, 1, n.messageConfirmations.pending.Load(), "late cleanup must not release the new generation")
	n.CompleteMessageConfirmation(current)
	n.CompleteMessageConfirmation(current)
	require.False(t, n.MessageConfirmationActive(current))
	require.False(t, n.hasPendingHandoffWork())
}

func TestAbandonedMessageConfirmationCannotBeCertified(t *testing.T) {
	n := transferTestLeader(t, false)
	confirmation := n.BeginMessageConfirmation()
	confirmation.Abandon()
	confirmation.Abandon()
	require.False(t, n.MessageConfirmationActive(confirmation))
	require.False(t, n.CompleteMessageConfirmation(confirmation))
	require.False(t, n.hasPendingHandoffWork())
}

func TestMessageConfirmationSurvivesAuthoritativeMigrationReplacement(t *testing.T) {
	n := transferTestLeader(t, false)
	require.NoError(t, n.Step(n.NewPropose([]byte("pending confirmation"))))
	confirmation := n.BeginMessageConfirmation()
	storeMessageConfirmationTail(t, n)
	progress := types.Event{Type: types.SyncReq, From: 3, Term: 1, ConfigVersion: n.Config().Version,
		Index: n.LastLogIndex() + 1, StoredIndex: n.LastLogIndex() + 1, LastLogTerm: 1, Reason: types.ReasonOnlySync}
	require.NoError(t, n.Step(progress))
	require.True(t, n.drainingAdmissions)
	require.False(t, n.handoff.attempted)

	cfg := n.Config().Clone()
	cfg.Version++
	cfg.MigrateTo = 2
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Nil(t, n.handoff)
	require.False(t, n.drainingAdmissions)
	require.True(t, n.MessageConfirmationActive(confirmation))

	progress.From, progress.ConfigVersion = cfg.MigrateTo, cfg.Version
	require.NoError(t, n.Step(progress))
	require.True(t, n.drainingAdmissions)
	require.Equal(t, cfg.MigrateTo, n.handoff.to)
	require.False(t, n.handoff.attempted, "the replacement still has to drain accepted work")
	require.True(t, n.CompleteMessageConfirmation(confirmation))
	require.NoError(t, n.Step(progress))
	require.True(t, n.handoff.attempted)
}

func TestMessageConfirmationKeepsRaftAliveUntilCompletion(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	n.opts.AutoDestory = true
	n.opts.DestoryAfterIdleTick = 0
	require.NoError(t, n.Step(n.NewPropose([]byte("awaiting confirmation"))))
	confirmation := n.BeginMessageConfirmation()

	n.Tick()
	_, destroyed := findEvent(n.Ready(), types.Destory)
	require.False(t, destroyed, "pending confirmation owns the runtime")

	n.CompleteMessageConfirmation(confirmation)
	n.Tick()
	_, destroyed = findEvent(n.Ready(), types.Destory)
	require.True(t, destroyed, "completion restores idle cleanup")
}

func TestMessageConfirmationTransferResumesIdleTargetAfterDroppedWake(t *testing.T) {
	for _, test := range []struct {
		name    string
		learner bool
	}{
		{name: "follower"},
		{name: "learner", learner: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			leader := transferTestLeader(t, test.learner)
			leader.opts.AutoSuspend = true
			leader.opts.SuspendAfterEmptySyncTick = 0
			require.NoError(t, leader.Step(leader.NewPropose([]byte("in-flight TCP message"))))
			confirmation := leader.BeginMessageConfirmation()
			tail := leader.LastLogIndex()
			storeMessageConfirmationTail(t, leader)

			target := NewNode(1, types.RaftState{LastLogIndex: tail, LastTerm: 1, AppliedIndex: tail - 1,
				HardState: types.HardState{Term: 1}}, NewOptions(WithNodeId(3), WithAdvance(func() {}), WithAutoSuspend(true)))
			cfg := leader.Config().Clone()
			cfg.Role = types.RoleFollower
			if test.learner {
				cfg.Role = types.RoleLearner
			}
			require.NoError(t, target.Step(types.Event{Type: types.ConfChange, Config: cfg}))
			target.Ready()
			target.suspend, target.onlySync = true, true

			// Lose the initial notification. Periodic ticks alone must wake the target.
			nextMessageConfirmationWake(t, leader)
			require.True(t, target.Suspend())
			request := syncMessageConfirmationTarget(t, target, nextMessageConfirmationWake(t, leader))
			require.NoError(t, leader.Step(request))
			response, ok := findEvent(leader.Ready(), types.SyncResp)
			require.True(t, ok)
			require.NotEqual(t, types.SpeedSuspend, response.Speed, "a draining target must keep synchronizing")
			require.NoError(t, target.Step(response))
			require.False(t, target.Suspend())
			require.False(t, leader.handoff.attempted)

			leader.CompleteMessageConfirmation(confirmation)
			// Lose the completion notification too. Do not tick the target or send any
			// more messages; another leader notification must drive a fresh SyncReq.
			wake, ok := findEvent(leader.Ready(), types.NotifySync)
			require.True(t, ok, "the final completion should wake the transfer target immediately")
			require.Equal(t, uint64(3), wake.To)
			require.False(t, leader.handoff.attempted)
			request = syncMessageConfirmationTarget(t, target, nextMessageConfirmationWake(t, leader))
			require.NoError(t, leader.Step(request))
			require.True(t, leader.handoff.attempted)
			require.Equal(t, uint64(3), leader.handoff.to)
			leader.Ready()
			tickN(leader, leader.opts.SyncIntervalTick*2)
			require.Empty(t, findEventsOfType(leader.Ready(), types.NotifySync), "publication ends the wake loop")
		})
	}
}

func nextMessageConfirmationWake(t *testing.T, leader *Node) types.Event {
	t.Helper()
	for i := 0; i < maxAdmissionDrainTicks/2; i++ {
		leader.Tick()
		for _, event := range leader.Ready() {
			if event.Type == types.NotifySync || event.Type == types.Ping && event.Reason == types.ReasonOnlySync {
				require.Equal(t, uint64(3), event.To, "only the transfer target needs a wake")
				return event
			}
		}
	}
	t.Fatal("ticks did not wake the idle transfer target")
	return types.Event{}
}

func syncMessageConfirmationTarget(t *testing.T, target *Node, wake types.Event) types.Event {
	t.Helper()
	require.NoError(t, target.Step(wake))
	if wake.Type == types.Ping {
		require.False(t, target.Suspend())
		tickN(target, target.opts.SyncIntervalTick)
	}
	request, ok := findEvent(target.Ready(), types.SyncReq)
	require.True(t, ok, "a notified target must send a fresh sync request")
	return request
}

func TestMessageConfirmationDrainTimeoutRespectsPublication(t *testing.T) {
	n := transferTestLeader(t, false)
	require.NoError(t, n.Step(n.NewPropose([]byte("slow confirmation"))))
	confirmation := n.BeginMessageConfirmation()
	progress := types.Event{Type: types.SyncReq, From: 3, Term: 1, ConfigVersion: n.Config().Version,
		LastLogTerm: 1, Reason: types.ReasonOnlySync}
	progress.Index, progress.StoredIndex = n.LastLogIndex()+1, n.LastLogIndex()+1
	require.NoError(t, n.Step(progress))
	require.True(t, n.drainingAdmissions)

	for i := 0; i < maxAdmissionDrainTicks; i++ {
		n.tickAdmissionDrain()
	}
	require.True(t, n.MessageConfirmationActive(confirmation), "timeout must not discard outstanding work")
	require.False(t, n.drainingAdmissions)
	require.Nil(t, n.handoff)
	require.NoError(t, n.Step(n.NewPropose([]byte("resumed before publication"))))
	n.CompleteMessageConfirmation(confirmation)

	for i := 0; i < maxAdmissionDrainTicks; i++ {
		n.tickAdmissionDrain()
	}
	progress.Index, progress.StoredIndex = n.LastLogIndex()+1, n.LastLogIndex()+1
	require.NoError(t, n.Step(progress))
	require.True(t, n.handoff.attempted)
	require.Equal(t, uint64(3), n.handoff.to)
	require.NotZero(t, n.handoff.requestID)
	require.NoError(t, n.Step(types.Event{Type: types.FollowerToLeaderResp, From: 3, Term: 1,
		ConfigVersion: n.Config().Version, RoleChangeID: n.handoff.requestID, Reason: types.ReasonError}))
	for i := 0; i < maxAdmissionDrainTicks*2; i++ {
		n.tickAdmissionDrain()
	}
	require.ErrorIs(t, n.Step(n.NewPropose([]byte("publication outcome unknown"))), types.ErrProposalDropped)
}
