package slot

import (
	"context"
	"errors"
	"testing"
	"time"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type recoveryTestTransport struct{ peers map[uint64]*Server }

func (tr *recoveryTestTransport) Send(key string, e types.Event) {
	if p := tr.peers[e.To]; p != nil {
		p.AddEvent(key, e)
	}
}

// Run real Raft owner loops, durable storage, replication and apply callbacks.
// Node 2 is deliberately absent: nodes 1 and 3 can form an empty majority.
func recoveryRuntimeServers(t *testing.T, recovering bool) []*Server {
	t.Helper()
	tr := &recoveryTestTransport{peers: map[uint64]*Server{}}
	var servers []*Server
	for _, id := range []uint64{1, 3} {
		s := NewServer(NewOptions(WithNodeId(id), WithRecovering(recovering), WithDataDir(t.TempDir()), WithSlotDbShardNum(1), WithTransport(tr), WithOnApply(func(uint32, []types.Log) error { return nil })))
		require.NoError(t, s.storage.Open())
		tr.peers[id] = s
		servers = append(servers, s)
	}
	for _, s := range servers {
		require.NoError(t, s.raftGroup.Start())
		t.Cleanup(s.Stop)
	}
	return servers
}

func runtimeSlotConfig(leader uint64, term uint32, status nodetypes.SlotStatus) *nodetypes.Config {
	return &nodetypes.Config{Version: uint64(term), Slots: []*nodetypes.Slot{{Id: 1, Leader: leader, Term: term, Status: status, Replicas: []uint64{1, 2, 3}}}}
}

func waitRuntimeSlot(t *testing.T, s *Server, term uint32, ready bool) raftgroup.ReadState {
	t.Helper()
	var state raftgroup.ReadState
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		v, err := s.ReadLeaderState(ctx, 1)
		state = v
		return err == nil && v.Exists && v.Term == term && v.Ready == ready
	}, 2*time.Second, 5*time.Millisecond)
	return state
}

func TestRecoveryRuntimeHistoricalFollowerCannotCommitNewHistory(t *testing.T) {
	servers := recoveryRuntimeServers(t, true)
	for _, cfg := range []*nodetypes.Config{runtimeSlotConfig(2, 1, 0), runtimeSlotConfig(1, 2, 0), runtimeSlotConfig(3, 3, 0), runtimeSlotConfig(1, 4, 0)} {
		for _, s := range servers {
			s.OnConfigChange(cfg)
		}
		for _, s := range servers {
			waitRuntimeSlot(t, s, cfg.Slots[0].Term, false)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := servers[0].ProposeUntilAppliedTimeoutForLocal(ctx, 1, types.ProposeReqSet{{Id: 1, Data: []byte("unsafe-history")}})
	require.Error(t, err, "two empty replacements must reject admission before consulting surviving history")
	// The group API may forward to metadata's historical leader, which maps
	// its refusal to a generic proposal response. A timeout is not a rejection.
	require.True(t, errors.Is(err, types.ErrNotLeader) || err.Error() == "receive propose resp error", "expected immediate proposal rejection, got %v", err)
	for _, s := range servers {
		last, err := s.LastIndex(1)
		require.NoError(t, err)
		require.Zero(t, last)
		applied, err := s.AppliedIndex(1)
		require.NoError(t, err)
		require.Zero(t, applied)
	}
}

func TestRecoveryRuntimeCandidateDemotesExistingLeader(t *testing.T) {
	for _, leader := range []uint64{1, 0} {
		t.Run(map[uint64]string{1: "previous-leader", 0: "zero-leader"}[leader], func(t *testing.T) {
			servers := recoveryRuntimeServers(t, false)
			for _, s := range servers {
				s.OnConfigChange(runtimeSlotConfig(1, 1, 0))
			}
			waitRuntimeSlot(t, servers[0], 1, true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := servers[0].ProposeUntilAppliedTimeoutForLocal(ctx, 1, types.ProposeReqSet{{Id: 1, Data: []byte("before-candidate")}})
			require.NoError(t, err)
			for _, s := range servers {
				s.OnConfigChange(runtimeSlotConfig(leader, 2, nodetypes.SlotStatus_SlotStatusCandidate))
			}
			state := waitRuntimeSlot(t, servers[0], 2, false)
			require.Zero(t, state.LeaderID)
			_, err = servers[0].ProposeUntilAppliedTimeoutForLocal(ctx, 1, types.ProposeReqSet{{Id: 2, Data: []byte("after-candidate")}})
			require.ErrorIs(t, err, types.ErrNotLeader)
			last, err := servers[0].LastIndex(1)
			require.NoError(t, err)
			require.Equal(t, uint64(1), last)
		})
	}
}

func TestRecoveryRuntimeRejoinBoundaryRestoresUnusedSlot(t *testing.T) {
	for _, order := range []string{"response-before-joined", "response-after-joined", "response-after-election"} {
		t.Run(order, func(t *testing.T) {
			servers := recoveryRuntimeServers(t, true)
			historical := runtimeSlotConfig(1, 1, 0)
			historical.Nodes = []*nodetypes.Node{{Id: 1, Status: nodetypes.NodeStatus_NodeStatusJoined}, {Id: 3, Status: nodetypes.NodeStatus_NodeStatusJoined}}
			for _, s := range servers {
				s.OnConfigChange(historical)
			}
			waitRuntimeSlot(t, servers[0], 1, false)
			// A historical Joined snapshot and an older seed's response are insufficient.
			for _, s := range servers {
				s.SetRecoveryJoinVersion(0)
			}
			waitRuntimeSlot(t, servers[0], 1, false)
			if order == "response-before-joined" {
				for _, s := range servers {
					s.SetRecoveryJoinVersion(10)
				}
			}
			joining := runtimeSlotConfig(1, 2, 0)
			joining.Version = 10
			joining.Nodes = []*nodetypes.Node{{Id: 1, Status: nodetypes.NodeStatus_NodeStatusWillJoin}, {Id: 3, Status: nodetypes.NodeStatus_NodeStatusWillJoin}}
			for _, s := range servers {
				s.OnConfigChange(joining)
			}
			waitRuntimeSlot(t, servers[0], 2, false)
			joined := runtimeSlotConfig(1, 3, nodetypes.SlotStatus_SlotStatusCandidate)
			joined.Version = 11
			joined.Nodes = historical.Nodes
			for _, s := range servers {
				s.OnConfigChange(joined)
			}
			if order == "response-after-joined" {
				for _, s := range servers {
					s.SetRecoveryJoinVersion(10)
				}
			}
			waitRuntimeSlot(t, servers[0], 3, false)
			elected := runtimeSlotConfig(1, 4, 0)
			elected.Version = 12
			elected.Nodes = historical.Nodes
			for _, s := range servers {
				s.OnConfigChange(elected)
			}
			if order == "response-after-election" {
				waitRuntimeSlot(t, servers[0], 4, false)
				for _, s := range servers {
					s.SetRecoveryJoinVersion(10)
				}
			}
			waitRuntimeSlot(t, servers[0], 4, true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := servers[0].ProposeUntilAppliedTimeoutForLocal(ctx, 1, types.ProposeReqSet{{Id: 1, Data: []byte("verified-unused-slot")}})
			require.NoError(t, err)
			for _, s := range servers {
				require.Eventually(t, func() bool { index, err := s.AppliedIndex(1); return err == nil && index == 1 }, time.Second, 5*time.Millisecond)
			}
		})
	}
}

func TestRecoveryBoundaryDoesNotAuthorizeQueuedHistoricalConfig(t *testing.T) {
	s, local := replayTestSlot(t)
	// Publishing a response/Joined boundary on the config loop must not release
	// old historical ConfChange events still waiting on the slot owner loop.
	s.recoveryBoundary.Store(20)
	cfg := types.Config{Version: 19, Leader: 1002, Term: 100, Role: types.RoleLeader, Replicas: []uint64{1001, 1002, 1003}}
	require.NoError(t, local.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	local.Ready()
	require.False(t, local.IsLeader())
	cfg.Version = 20
	cfg.Term++
	require.NoError(t, local.Step(types.Event{Type: types.ConfChange, Config: cfg}))
	local.Ready()
	require.True(t, local.IsLeader())
}

func TestRecoveryRuntimeRemoteConfigCannotBypassFence(t *testing.T) {
	s, local := replayTestSlot(t)
	metadata := &nodetypes.Slot{Id: 1, Leader: 1001, Term: 3, Replicas: []uint64{1001, 1002, 1003}}
	require.NoError(t, local.Step(types.Event{Type: types.ConfChange, Config: s.slotRuntimeConfig(metadata, local)}))
	local.Ready()
	require.NoError(t, local.Step(types.Event{Type: types.ConfigResp, From: 1001, Term: 3, Config: types.Config{Version: 10, Term: 3, Leader: 1002, Replicas: metadata.Replicas, Role: types.RoleLeader}}))
	local.Ready()
	require.False(t, local.IsLeader(), "a peer's configuration response is not this boot's recovery decision")
	require.ErrorIs(t, local.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Id: 1, Data: []byte("unverified")}}}), types.ErrNotLeader)
}
