package event

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// Commit a competing command before forwarding the already planned operation.
// This exercises the real Raft apply order without production test hooks.
func interleavedAdmissionServer(t *testing.T, cfg *types.Config, kind clusterconfig.CMDType, before func(*clusterconfig.Server)) (*Server, *clusterconfig.Server) {
	t.Helper()
	leader := newJoiningTestServer(t, cfg).cfgServer
	tr := leader.Options().Transport.(*joiningTestTransport)
	var follower *clusterconfig.Server
	for id, server := range tr.servers {
		if id != leader.Options().NodeId {
			follower = server
			break
		}
	}
	require.Eventually(t, func() bool { return follower.LeaderId() == leader.Options().NodeId }, time.Second, 10*time.Millisecond)
	var once sync.Once
	tr.mu.Lock()
	tr.beforePropose = func(e rafttypes.Event) {
		if e.From != follower.Options().NodeId {
			return
		}
		var requests rafttypes.ProposeReqSet
		require.NoError(t, requests.Unmarshal(e.Logs[0].Data))
		for _, req := range requests {
			var cmd clusterconfig.CMD
			require.NoError(t, cmd.Unmarshal(req.Data))
			if cmd.CmdType == kind {
				once.Do(func() { before(leader) })
			}
		}
	}
	tr.mu.Unlock()
	return &Server{cfgServer: follower, cfgOptions: follower.Options()}, leader
}

func TestExistingVoterRejoinRetriesIfRetirementWins(t *testing.T) {
	cfg := joiningTestConfig(true)
	for _, node := range cfg.Nodes {
		node.Status = types.NodeStatus_NodeStatusJoined
	}
	cfg.Nodes = append(cfg.Nodes, &types.Node{Id: 1004, Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusWillJoin})
	cfg.Learners, cfg.MigrateFrom, cfg.MigrateTo = []uint64{1004}, 1004, 1004
	var victim uint64
	s, leader := interleavedAdmissionServer(t, cfg, clusterconfig.CMDTypeNodeJoining, func(leader *clusterconfig.Server) {
		require.NoError(t, leader.ProposeNodeOnlineStatus(victim, false))
		payload := make([]byte, 16)
		binary.BigEndian.PutUint64(payload, victim)
		binary.BigEndian.PutUint64(payload[8:], 1004)
		data, err := clusterconfig.NewCMD(clusterconfig.CMDTypeNodeRemove, payload).Marshal()
		require.NoError(t, err)
		_, err = leader.ProposeUntilApplied(991001, data)
		require.NoError(t, err)
		require.Nil(t, leader.Node(victim))
	})
	for _, id := range []uint64{1001, 1002, 1003} {
		if id != leader.Options().NodeId && id != s.cfgOptions.NodeId {
			victim = id
		}
	}
	require.NotZero(t, victim)
	node := &types.Node{Id: victim, ClusterAddr: "127.0.0.1:29991", Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusWillJoin}
	version, err := s.cfgServer.ProposeJoinWithVersion(node)
	require.ErrorIs(t, err, clusterconfig.ErrMembershipBusy, "a committed no-op must not acknowledge a removed voter")
	require.Zero(t, version)
	require.Nil(t, s.cfgServer.Node(victim))
	_, err = s.cfgServer.ProposeJoinWithVersion(node)
	require.ErrorIs(t, err, clusterconfig.ErrMembershipBusy, "retry must preserve the other new voter's admission")
	require.Equal(t, []uint64{1004}, s.cfgServer.Snapshot().Learners)
	require.Equal(t, uint64(1004), s.cfgServer.Snapshot().MigrateTo)
}

func TestJoiningCompletionWaitsForAppliedSlotPlan(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "new_allocation"
		if recovery {
			name = "recovery_election"
		}
		t.Run(name, func(t *testing.T) {
			cfg := joiningTestConfig(true)
			for _, node := range cfg.Nodes {
				node.Status = types.NodeStatus_NodeStatusJoined
			}
			joining := uint64(1004)
			if recovery {
				joining = 1002
				cfg.Nodes[1].Status = types.NodeStatus_NodeStatusJoining
				cfg.Slots[0].Leader = joining
			} else {
				cfg.Nodes = append(cfg.Nodes, &types.Node{Id: joining, Online: true, AllowVote: true, Status: types.NodeStatus_NodeStatusJoining})
			}
			s, _ := interleavedAdmissionServer(t, cfg, clusterconfig.CMDTypeSlotUpdateIfCurrent, func(leader *clusterconfig.Server) {
				changed := leader.Snapshot().Slots
				for _, slot := range changed {
					slot.Term++
				}
				require.NoError(t, leader.ProposeSlots(changed))
			})
			require.NoError(t, s.handleNodeJoining())
			require.Equal(t, types.NodeStatus_NodeStatusJoining, s.cfgServer.Node(joining).Status,
				"skipped conditional slot updates must not complete admission")
			for _, slot := range s.cfgServer.Slots() {
				require.Empty(t, slot.Learners)
				require.Equal(t, types.SlotStatus_SlotStatusNormal, slot.Status)
			}
			require.NoError(t, s.handleNodeJoining())
			require.Equal(t, types.NodeStatus_NodeStatusJoined, s.cfgServer.Node(joining).Status)
			if recovery {
				require.Equal(t, types.SlotStatus_SlotStatusCandidate, s.cfgServer.Slot(0).Status)
			} else {
				assigned := 0
				for _, slot := range s.cfgServer.Slots() {
					if slot.MigrateTo == joining {
						assigned++
					}
				}
				require.Positive(t, assigned)
			}
		})
	}
}
