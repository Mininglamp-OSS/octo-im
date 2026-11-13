package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestFailedLeaderTransferBacksOffAndRecoversWithoutWrites(t *testing.T) {
	for _, test := range []struct {
		name            string
		learner         bool
		legacyBystander bool
		lagging         bool
	}{
		{name: "follower"},
		{name: "learner", learner: true},
		{name: "legacy-bystander", legacyBystander: true},
		{name: "lagging-bystander", lagging: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			leader := transferTestLeader(t, test.learner)
			leader.opts.AutoSuspend = true
			target := newLeaderTransferReplica(t, leader, 3)
			bystander := newLeaderTransferReplica(t, leader, 2)
			// #97 replicas ignore the versioned sleep grant during migration.
			bystander.opts.AutoSuspend = !test.legacyBystander
			if test.lagging {
				proposal := leader.NewPropose([]byte("one pending follower append"))
				require.NoError(t, leader.Step(proposal))
				leader.Ready()
				require.NoError(t, leader.Step(types.Event{Type: types.StoreResp, Index: 13, Reason: types.ReasonOk}))
				leader.Ready()
				for _, peer := range []*Node{bystander, target} {
					require.NoError(t, peer.Step(types.Event{Type: types.SyncResp, From: 1, Term: 1,
						Index: 13, Logs: proposal.Logs, Reason: types.ReasonOk}))
					peer.Ready()
				}
				require.NoError(t, target.Step(types.Event{Type: types.StoreResp, Index: 13, Reason: types.ReasonOk}))
				target.Ready()
				require.Equal(t, uint64(12), bystander.queue.storedIndex)
				require.True(t, bystander.queue.appending)
			}

			var attempts []int
			publicationAvailable := false
			published := false
			tick := func(at int) {
				leader.Tick()
				target.Tick()
				bystander.Tick()
				for i := 0; i < 5; i++ {
					for _, peer := range []*Node{target, bystander} {
						for _, event := range peer.Ready() {
							if event.To == 1 {
								require.NoError(t, leader.Step(event))
							}
						}
					}
					for _, event := range leader.Ready() {
						response := types.FollowerToLeaderResp
						switch event.Type {
						case types.LearnerToLeaderReq:
							response = types.LearnerToLeaderResp
						case types.FollowerToLeaderReq:
						default:
							if event.To == 3 {
								require.NoError(t, target.Step(event))
							}
							if event.To == 2 {
								require.NoError(t, bystander.Step(event))
							}
							continue
						}
						attempts = append(attempts, at)
						reason := types.ReasonError
						if publicationAvailable {
							reason = types.ReasonOk
							published = true
						}
						require.NoError(t, leader.Step(types.Event{Type: response, From: event.From,
							Term: event.Term, ConfigVersion: event.ConfigVersion, RoleChangeID: event.RoleChangeID, Reason: reason}))
					}
				}
			}

			const failureTicks = 120
			for i := 0; i < failureTicks; i++ {
				tick(i)
			}
			t.Logf("failed publication attempts over %d ticks: %v", failureTicks, attempts)
			require.Len(t, attempts, maxLeaderTransferFailures)
			require.True(t, target.Suspend(), "the exhausted target must have stopped requesting sync")
			require.Equal(t, !test.legacyBystander && !test.lagging, bystander.Suspend())
			delay := initialLeaderTransferRetryTicks
			for i := 1; i < len(attempts); i++ {
				require.GreaterOrEqual(t, attempts[i]-attempts[i-1], delay)
				delay = min(delay*2, maxLeaderTransferRetryTicks)
			}
			require.ErrorIs(t, leader.Step(leader.NewPropose([]byte("uncertain publication"))), types.ErrProposalDropped)

			publicationAvailable = true
			for i := failureTicks; i < failureTicks+leaderTransferRecoveryTicks+target.opts.SyncIntervalTick+2 && !published; i++ {
				tick(i)
			}
			require.True(t, published, "recover the same migration by probing its suspended target")
			require.GreaterOrEqual(t, attempts[len(attempts)-1]-attempts[len(attempts)-2], leaderTransferRecoveryTicks)
			require.ErrorIs(t, leader.Step(leader.NewPropose([]byte("owner config not applied"))), types.ErrProposalDropped)
		})
	}
}

func TestLeaderTransferBackoffTracksAuthoritativeConfiguration(t *testing.T) {
	n := transferTestLeader(t, false)
	n.opts.AutoSuspend = true
	request := beginTestTransfer(t, n)
	failure := types.Event{Type: types.FollowerToLeaderResp, From: request.From, Term: request.Term,
		ConfigVersion: request.ConfigVersion, RoleChangeID: request.RoleChangeID, Reason: types.ReasonError}
	require.NoError(t, n.Step(failure))
	require.Positive(t, n.handoff.retryTicks)
	require.NoError(t, n.Step(failure))
	require.Equal(t, 1, n.handoff.failures, "a duplicate callback must not consume the retry budget")
	require.Equal(t, initialLeaderTransferRetryTicks, n.handoff.retryTicks)

	cfg := n.Config().Clone()
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Positive(t, n.handoff.retryTicks, "same configuration must not bypass backoff")
	cfg.Version++
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Zero(t, n.handoff.retryTicks)
	require.Equal(t, cfg.Version, beginTestTransfer(t, n).ConfigVersion)
}

func TestLeaderTransferPublishedChannelWaitsForAuthoritativeConfig(t *testing.T) {
	leader := transferTestLeader(t, false)
	leader.opts.AutoSuspend = true
	req := beginTestTransfer(t, leader)
	result := types.Event{Type: types.FollowerToLeaderResp, From: req.From, Term: req.Term,
		ConfigVersion: req.ConfigVersion, RoleChangeID: req.RoleChangeID, Reason: types.ReasonOk}
	require.NoError(t, leader.Step(result))
	result.Reason = types.ReasonError
	require.NoError(t, leader.Step(result))
	require.Zero(t, leader.handoff.failures)
	tickN(leader, maxLeaderTransferRetryTicks)
	require.NoError(t, leader.Step(types.Event{Type: types.SyncReq, From: 3, Term: req.Term,
		Index: 13, StoredIndex: 13, ConfigVersion: req.ConfigVersion, Reason: types.ReasonOnlySync}))
	events := leader.Ready()
	require.Empty(t, findEventsOfType(events, types.FollowerToLeaderReq))
	require.Empty(t, findEventsOfType(events, types.NotifySync))
	require.ErrorIs(t, leader.Step(leader.NewPropose([]byte("awaiting authoritative config"))), types.ErrProposalDropped)

	cfg := leader.Config().Clone()
	cfg.Version++
	require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Greater(t, beginTestTransfer(t, leader).RoleChangeID, req.RoleChangeID)
}

func TestLeaderTransferAfterRetryLimitWaitsForFreshSync(t *testing.T) {
	leader := transferTestLeader(t, false)
	leader.opts.AutoSuspend = true
	for attempt := 1; attempt <= maxLeaderTransferFailures; attempt++ {
		request := beginTestTransfer(t, leader)
		require.NoError(t, leader.Step(types.Event{Type: types.FollowerToLeaderResp, From: request.From,
			Term: request.Term, ConfigVersion: request.ConfigVersion, RoleChangeID: request.RoleChangeID, Reason: types.ReasonError}))
		if attempt < maxLeaderTransferFailures {
			tickN(leader, leader.handoff.retryTicks)
			leader.Ready()
		}
	}

	cfg := leader.Config().Clone()
	require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Equal(t, maxLeaderTransferFailures, leader.handoff.failures)
	tickN(leader, leaderTransferRecoveryTicks-1)
	require.Empty(t, findEventsOfType(leader.Ready(), types.NotifySync))
	leader.Tick()
	events := leader.Ready()
	require.Len(t, findEventsOfType(events, types.NotifySync), 1, "the cooldown enables a single probe")
	require.Empty(t, findEventsOfType(events, types.FollowerToLeaderReq), "a probe is not a durable target report")
	tickN(leader, leaderTransferRecoveryTicks-1)
	require.Empty(t, findEventsOfType(leader.Ready(), types.NotifySync), "lost probe must not cause a wake storm")
	require.ErrorIs(t, leader.AdmitMessage(123), types.ErrProposalDropped)
	require.ErrorIs(t, leader.Step(leader.NewPropose([]byte("publication unresolved"))), types.ErrProposalDropped)
	beginTestTransfer(t, leader)
}

func TestLeaderTransferProbesWhileLocalApplyPreventsQuietEviction(t *testing.T) {
	leader := transferTestLeader(t, false)
	leader.opts.AutoSuspend = true
	leader.opts.AutoDestory = true
	leader.opts.DestoryAfterIdleTick = 150
	leader.pauseLeaderHandoff()
	leader.handoff.failures = maxLeaderTransferFailures
	leader.handoff.quietTicks = leader.opts.DestoryAfterIdleTick + 1
	leader.queue.applying = true

	tickN(leader, leader.opts.SyncIntervalTick+1)
	events := leader.Ready()
	require.Empty(t, findEventsOfType(events, types.Destory))
	for _, wake := range findEventsOfType(events, types.Ping) {
		if wake.To == 3 {
			require.Equal(t, types.ReasonOnlySync, wake.Reason)
			return
		}
	}
	t.Fatal("an active source must still probe its transfer target")
}

func TestRepeatedLeaderTransferFailureLetsIdleRaftsEvict(t *testing.T) {
	for _, learner := range []bool{false, true} {
		t.Run(map[bool]string{false: "follower", true: "learner"}[learner], func(t *testing.T) {
			leader := transferTestLeader(t, learner)
			leader.opts.AutoSuspend = true
			leader.opts.AutoDestory = true
			leader.opts.DestoryAfterIdleTick = 3 * leaderTransferRecoveryTicks
			target := newLeaderTransferReplica(t, leader, 3)
			target.opts.AutoDestory = true
			target.opts.DestoryAfterIdleTick = leader.opts.DestoryAfterIdleTick
			other := newLeaderTransferReplica(t, leader, 2)
			other.opts.AutoDestory = true
			other.opts.DestoryAfterIdleTick = leader.opts.DestoryAfterIdleTick
			peers := []*Node{other, target}

			attempts := 0
			var sourceEvicted bool
			evicted := make(map[uint64]bool)
			for tick := 0; tick < 3*leader.opts.DestoryAfterIdleTick && len(evicted) != len(peers); tick++ {
				if !sourceEvicted {
					leader.Tick()
				}
				for _, peer := range peers {
					if evicted[peer.opts.NodeId] {
						continue
					}
					peer.Tick()
					for _, event := range peer.Ready() {
						if event.Type == types.Destory {
							evicted[peer.opts.NodeId] = true
						} else if event.To == 1 && !sourceEvicted {
							require.NoError(t, leader.Step(event))
						}
					}
				}
				if sourceEvicted {
					continue
				}
				for _, event := range leader.Ready() {
					switch event.Type {
					case types.Destory:
						sourceEvicted = true
					case types.FollowerToLeaderReq, types.LearnerToLeaderReq:
						attempts++
						// The group keeps the instance alive while dispatching
						// role work, including recovery publication attempts.
						leader.KeepAlive()
						response := types.FollowerToLeaderResp
						if event.Type == types.LearnerToLeaderReq {
							response = types.LearnerToLeaderResp
						}
						require.NoError(t, leader.Step(types.Event{Type: response, From: event.From,
							Term: event.Term, ConfigVersion: event.ConfigVersion, RoleChangeID: event.RoleChangeID, Reason: types.ReasonError}))
						require.ErrorIs(t, leader.Step(leader.NewPropose([]byte("unresolved publication"))), types.ErrProposalDropped)
						require.ErrorIs(t, leader.AdmitMessage(100), types.ErrProposalDropped)
					default:
						for _, peer := range peers {
							if event.To == peer.opts.NodeId && !evicted[event.To] {
								require.NoError(t, peer.Step(event))
							}
						}
					}
				}
			}
			t.Logf("failed attempts=%d source evicted=%t peers evicted=%v", attempts, sourceEvicted, evicted)
			require.Greater(t, attempts, maxLeaderTransferFailures, "cold source should probe before eviction")
			require.LessOrEqual(t, attempts, maxLeaderTransferFailures+3, "probes must not extend residency")
			require.True(t, sourceEvicted)
			require.Len(t, evicted, len(peers))
		})
	}
}

func newLeaderTransferReplica(t *testing.T, leader *Node, id uint64) *Node {
	t.Helper()
	target := NewNode(1, types.RaftState{LastLogIndex: 12, LastTerm: 1, AppliedIndex: 12,
		HardState: types.HardState{Term: 1}}, NewOptions(WithNodeId(id), WithAdvance(func() {}), WithAutoSuspend(true)))
	cfg := leader.Config().Clone()
	cfg.Role = types.RoleFollower
	require.NoError(t, target.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	target.Ready()
	target.onlySync = true
	return target
}

func TestLeaderTransferWakeDoesNotWaitOnUnknownLeader(t *testing.T) {
	for _, learner := range []bool{false, true} {
		name := "follower"
		if learner {
			name = "learner"
		}
		t.Run(name, func(t *testing.T) {
			leader := transferTestLeader(t, learner)
			leader.opts.AutoSuspend = true
			target := newLeaderTransferReplica(t, leader, 3)
			target.suspend = true

			newCfg := leader.Config().Clone()
			newCfg.Term++
			newCfg.Version++
			require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: newCfg}))
			leader.Ready()
			for i := 0; i <= leader.opts.SyncIntervalTick; i++ {
				leader.Tick()
			}
			events := leader.Ready()
			require.Empty(t, findEventsOfType(events, types.NotifySync), "a stale old target cannot receive a speculative notification")
			for _, event := range events {
				if event.To == 3 {
					require.Equal(t, types.Ping, event.Type)
					require.Equal(t, types.ReasonOnlySync, event.Reason)
					require.NoError(t, target.Step(event))
				}
			}
			require.Equal(t, uint64(1), target.Config().Leader)
			require.False(t, target.Suspend())
			for i := 0; i <= target.opts.SyncIntervalTick; i++ {
				for _, request := range findEventsOfType(target.Ready(), types.SyncReq) {
					require.Equal(t, uint64(1), request.To)
					require.Equal(t, newCfg.Term, request.Term)
					return
				}
				target.Tick()
			}
			t.Fatal("first valid sync was delayed after the leader heartbeat")
		})
	}
}

func TestLeaderTransferWakeWhenTermIsUnchanged(t *testing.T) {
	leader := transferTestLeader(t, false)
	leader.opts.AutoSuspend = true
	target := newLeaderTransferReplica(t, leader, 3)
	target.suspend = true

	leader.wakeLeaderTransferTarget(3)
	ping, ok := findEvent(leader.Ready(), types.Ping)
	require.True(t, ok)
	require.Equal(t, types.ReasonOnlySync, ping.Reason)
	require.NoError(t, target.Step(ping))
	require.False(t, target.Suspend(), "an updated target can wake on a same-term Ping")

	// Older targets ignore the wake hint, but request config when the version
	// changes. That request proves they can safely receive NotifySync.
	cfg := leader.Config().Clone()
	cfg.Version++
	require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	leader.wakeLeaderTransferTarget(3)
	ping, ok = findEvent(leader.Ready(), types.Ping)
	require.True(t, ok)
	ping.Reason = 0
	target.suspend = true
	require.NoError(t, target.Step(ping))
	require.True(t, target.Suspend())
	req, ok := findEvent(target.Ready(), types.ConfigReq)
	require.True(t, ok)
	require.NoError(t, leader.Step(req))

	cfg.Version++
	require.NoError(t, leader.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	require.Zero(t, leader.replicaSync[3].LastSyncIndex, "the proof must not restore log progress")
	leader.wakeLeaderTransferTarget(3)
	wake, ok := findEvent(leader.Ready(), types.NotifySync)
	require.True(t, ok, "same-term config updates keep proof of the leader")
	require.NoError(t, target.Step(wake))
	sync, ok := findEvent(target.Ready(), types.SyncReq)
	require.True(t, ok)
	require.Equal(t, uint64(1), sync.To)
	require.Equal(t, cfg.Term, sync.Term)
}

func TestHigherTermNotifySyncWithoutKnownLeaderDoesNotStartTimeout(t *testing.T) {
	leader := transferTestLeader(t, false)
	target := newLeaderTransferReplica(t, leader, 3)
	require.NoError(t, target.Step(types.Event{Type: types.NotifySync, From: 1, To: 3, Term: 2}))
	require.Zero(t, target.Config().Leader)
	require.False(t, target.syncing)
	require.Empty(t, findEventsOfType(target.Ready(), types.SyncReq))
}

func TestLeaderHandoffIdleGrantIsLimitedToCurrentPlannedTransfer(t *testing.T) {
	leader := transferTestLeader(t, false)
	target := newLeaderTransferReplica(t, leader, 3)
	grant := types.Event{Type: types.SyncResp, From: 1, To: 3, Term: 1, ConfigVersion: target.cfg.Version,
		Index: 13, Speed: types.SpeedSuspend, Reason: types.ReasonOk}

	stale := grant
	stale.ConfigVersion--
	require.NoError(t, target.Step(stale))
	require.False(t, target.Suspend())
	stale = grant
	stale.Index--
	require.NoError(t, target.Step(stale))
	require.False(t, target.Suspend())

	cfg := target.Config().Clone()
	cfg.Version++
	cfg.MigrateFrom = 2 // follower replacement must keep acknowledging membership
	require.NoError(t, target.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	grant.ConfigVersion = cfg.Version
	require.NoError(t, target.Step(grant))
	require.False(t, target.Suspend())
}
