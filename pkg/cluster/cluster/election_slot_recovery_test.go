package cluster

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func recoveryConfigServer(t *testing.T, slots []*types.Slot) *clusterconfig.Server {
	t.Helper()
	data, err := json.Marshal(&types.Config{Nodes: []*types.Node{{Id: 1001}, {Id: 1002}, {Id: 1003}}, Slots: slots})
	require.NoError(t, err)
	configPath := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(configPath, data, 0600))
	return clusterconfig.New(clusterconfig.NewOptions(clusterconfig.WithNodeId(1002), clusterconfig.WithConfigPath(configPath)))
}

func TestSlotRecoveryChoosesDurableLogAndFencesAllTerms(t *testing.T) {
	slot := &types.Slot{Id: 7, Leader: 1002, Term: 3, ExpectLeader: 1002, Status: types.SlotStatus_SlotStatusCandidate, Replicas: []uint64{1001, 1002, 1003}}
	s := &Server{cfgServer: recoveryConfigServer(t, []*types.Slot{slot})}
	infos := map[uint64]map[uint32]SlotInfo{
		1001: {7: {Term: 3, LogTerm: 3, LogIndex: 12}},
		1002: {7: {Term: 20}},
		1003: {7: {Term: 4}},
	}
	for range 100 {
		leader := s.electionSlotLeaderBySlot(infos, slot.Id)
		require.Equal(t, uint64(1001), leader, "runtime term cannot outrank a surviving log")
		next, err := slotElectionConfig(slot, leader, infos)
		require.NoError(t, err)
		require.Equal(t, uint32(21), next.Term)
		require.Equal(t, types.SlotStatus_SlotStatusNormal, next.Status)
		require.Zero(t, next.ExpectLeader)
	}
	infos[1002][7] = SlotInfo{Term: math.MaxUint32}
	_, err := slotElectionConfig(slot, 1001, infos)
	require.Error(t, err, "term overflow cannot publish term zero")
}

func TestSlotRecoveryCancelsSupersededLeadershipTransfer(t *testing.T) {
	slot := &types.Slot{Id: 7, Leader: 1002, Term: 3, ExpectLeader: 1002, Status: types.SlotStatus_SlotStatusCandidate,
		Replicas: []uint64{1001, 1002, 1003}, MigrateFrom: 1002, MigrateTo: 1003}
	next, err := slotElectionConfig(slot, 1001, nil)
	require.NoError(t, err)
	require.Zero(t, next.MigrateFrom)
	require.Zero(t, next.MigrateTo)
	require.Equal(t, uint64(1002), slot.MigrateFrom, "the previous configuration must remain immutable")
	slot.Replicas, slot.Learners = []uint64{1001, 1002}, []uint64{1003}
	next, err = slotElectionConfig(slot, 1001, nil)
	require.NoError(t, err)
	require.Equal(t, slot.MigrateFrom, next.MigrateFrom, "actual learner catch-up must continue")
	require.Equal(t, slot.MigrateTo, next.MigrateTo)
	require.Equal(t, slot.Learners, next.Learners)
}

func TestSlotElectionRetainsPreferredTransferWithEqualLogs(t *testing.T) {
	slot := &types.Slot{Id: 7, Leader: 1001, ExpectLeader: 1003}
	s := &Server{cfgServer: recoveryConfigServer(t, []*types.Slot{slot})}
	infos := map[uint64]map[uint32]SlotInfo{
		1001: {7: {LogTerm: 2, LogIndex: 12}},
		1002: {7: {LogTerm: 2, LogIndex: 12}},
		1003: {7: {LogTerm: 2, LogIndex: 12}},
	}
	for range 100 {
		require.Equal(t, uint64(1003), s.electionSlotLeaderBySlot(infos, slot.Id))
	}
	infos[1001][7] = SlotInfo{LogTerm: 2, LogIndex: 13}
	require.Equal(t, uint64(1001), s.electionSlotLeaderBySlot(infos, slot.Id), "transfer preference cannot discard a newer log")
}

func TestFreshSeededNodeStillJoinsAfterConfigurationReplay(t *testing.T) {
	s := &Server{cfgServer: recoveryConfigServer(t, nil), opts: NewOptions(WithSeed("1001@127.0.0.1:11110")), freshSeededStart: true}
	join, err := s.needJoin()
	require.NoError(t, err)
	require.True(t, join, "configuration replay does not prove this empty node completed rejoin")
	s.freshSeededStart = false
	join, err = s.needJoin()
	require.NoError(t, err)
	require.False(t, join, "a persisted restart does not need another rejoin")
	s.opts.Seed = "invalid"
	_, err = s.needJoin()
	require.Error(t, err)
}
