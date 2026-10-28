package slot

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestEmptyRecoveredSlotWaitsForNewElection(t *testing.T) {
	s := &Server{opts: NewOptions(WithNodeId(1002))}
	st := &types.Slot{Id: 1, Leader: 1002, Term: 1, Replicas: []uint64{1001, 1002, 1003}}
	local := &Slot{recoveryTerm: 1}
	for range 3 {
		cfg := s.slotRuntimeConfig(st, local)
		require.Zero(t, cfg.Leader, "replaying the same metadata must not restore empty leadership")
		require.Equal(t, rafttype.RoleFollower, cfg.Role)
	}
	require.Equal(t, uint64(1002), st.Leader, "the local fence must not mutate shared metadata")
	next := st.Clone()
	next.Term++
	cfg := s.slotRuntimeConfig(next, local)
	require.Equal(t, uint64(1002), cfg.Leader)
	require.Equal(t, rafttype.RoleLeader, cfg.Role)
	require.Zero(t, local.recoveryTerm)
}

func TestCandidateSlotFencesItsPreviousLeader(t *testing.T) {
	s := &Server{opts: NewOptions(WithNodeId(1002))}
	cfg := s.slotToConfig(&types.Slot{Leader: 1002, Term: 1, Status: types.SlotStatus_SlotStatusCandidate})
	require.Zero(t, cfg.Leader)
	require.Equal(t, rafttype.RoleFollower, cfg.Role)
}

func TestRecoveredSlotCanFollowSurvivingLeader(t *testing.T) {
	s := &Server{opts: NewOptions(WithNodeId(1002))}
	local := &Slot{recoveryTerm: 1}
	cfg := s.slotRuntimeConfig(&types.Slot{Leader: 1001, Term: 2}, local)
	require.Equal(t, uint64(1001), cfg.Leader)
	require.Equal(t, rafttype.RoleFollower, cfg.Role)
	require.Zero(t, local.recoveryTerm)
}
