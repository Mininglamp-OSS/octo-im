package clusterconfig

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestP2_HandleCmdRejectsUnknownCMDType(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/cluster.json")))
	defer s.config.cfgFile.Close()

	for _, unknownType := range []CMDType{CMDTypeUnknown, CMDType(11), CMDType(12), CMDType(13), CMDType(99)} {
		data, err := NewCMD(unknownType, []byte("payload")).Marshal()
		require.NoError(t, err)
		err = s.applyLog(rafttypes.Log{Index: 1, Term: 1, Data: data})
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown clusterconfig cmd type")
		require.Zero(t, s.config.version())
	}
}

func TestP1_UpdateSlotsFencesStaleTermAndConflictingSameTermLeader(t *testing.T) {
	cfg := NewConfig(NewOptions(WithConfigPath(t.TempDir() + "/cluster.json")))
	defer cfg.cfgFile.Close()

	cfg.addSlot(&types.Slot{
		Id:       1,
		Leader:   1001,
		Term:     5,
		Replicas: []uint64{1001, 1002, 1003},
		Status:   types.SlotStatus_SlotStatusNormal,
	})

	// 1. Stale term (< 5) must be ignored.
	cfg.updateSlots([]*types.Slot{{
		Id:       1,
		Leader:   1002,
		Term:     4,
		Replicas: []uint64{1001, 1002, 1003},
		Status:   types.SlotStatus_SlotStatusNormal,
	}})
	require.Equal(t, uint64(1001), cfg.slot(1).Leader)
	require.Equal(t, uint32(5), cfg.slot(1).Term)

	// 2. Same-term conflicting leader without planned migration must be rejected.
	cfg.updateSlots([]*types.Slot{{
		Id:       1,
		Leader:   1002,
		Term:     5,
		Replicas: []uint64{1001, 1002, 1003},
		Status:   types.SlotStatus_SlotStatusNormal,
	}})
	require.Equal(t, uint64(1001), cfg.slot(1).Leader)

	// 3. When slot is in Candidate status at Term 5, a Normal update with Term <= 5 must be rejected.
	cfg.updateSlotStatus(1, types.SlotStatus_SlotStatusCandidate)
	cfg.updateSlots([]*types.Slot{{
		Id:       1,
		Leader:   1002,
		Term:     5,
		Replicas: []uint64{1001, 1002, 1003},
		Status:   types.SlotStatus_SlotStatusNormal,
	}})
	require.Equal(t, types.SlotStatus_SlotStatusCandidate, cfg.slot(1).Status)
	require.Equal(t, uint64(1001), cfg.slot(1).Leader)

	// 4. Higher term (Term 6) Normal update from election succeeds.
	cfg.updateSlots([]*types.Slot{{
		Id:       1,
		Leader:   1002,
		Term:     6,
		Replicas: []uint64{1001, 1002, 1003},
		Status:   types.SlotStatus_SlotStatusNormal,
	}})
	require.Equal(t, types.SlotStatus_SlotStatusNormal, cfg.slot(1).Status)
	require.Equal(t, uint64(1002), cfg.slot(1).Leader)
	require.Equal(t, uint32(6), cfg.slot(1).Term)
}
