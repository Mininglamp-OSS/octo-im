package clusterconfig

import (
	"encoding/binary"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func removalTestConfig() *pb.Config {
	cfg := &pb.Config{Learners: []uint64{4}, MigrateFrom: 4, MigrateTo: 4}
	for _, id := range []uint64{1, 2, 3, 4} {
		cfg.Nodes = append(cfg.Nodes, &pb.Node{Id: id, Online: id != 3, AllowVote: true, Status: pb.NodeStatus_NodeStatusJoined})
	}
	cfg.Nodes[3].Status = pb.NodeStatus_NodeStatusWillJoin
	return cfg
}

func TestOfflineVoterRetirementRequiresExternalLearnerAndOldQuorum(t *testing.T) {
	cfg := removalTestConfig()
	require.Equal(t, uint64(3), offlineVoterForJoin(cfg, 4))
	cfg.Learners = nil
	require.Zero(t, offlineVoterForJoin(cfg, 4), "heartbeat loss alone never retires a voter")
	cfg.Learners = []uint64{4}
	cfg.Nodes[1].Online = false
	require.Zero(t, offlineVoterForJoin(cfg, 4), "the learner must not count toward the old majority")
	cfg.Nodes[1].Online, cfg.Nodes[2].Online = true, true
	require.Zero(t, offlineVoterForJoin(cfg, 4), "a recovered member remains eligible")
}

func TestDelayedNodeRemovalCannotDeleteRecoveredMember(t *testing.T) {
	s := New(NewOptions(WithConfigPath(t.TempDir() + "/config.json")))
	t.Cleanup(func() { _ = s.config.cfgFile.Close() })
	s.config.cfg = removalTestConfig()
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data, 3)
	binary.BigEndian.PutUint64(data[8:], 4)
	cmd := NewCMD(CMDTypeNodeRemove, data)
	s.config.cfg.Nodes[2].Online = true
	require.NoError(t, s.handleNodeRemove(cmd))
	require.Len(t, s.Snapshot().Nodes, 4)
	s.config.cfg.Nodes[2].Online = false
	require.NoError(t, s.handleNodeRemove(cmd))
	require.Nil(t, s.Node(3))
	require.NoError(t, s.handleNodeRemove(cmd), "replaying a completed retirement is idempotent")
	require.Equal(t, []uint64{4}, s.Snapshot().Learners)
	require.Equal(t, uint64(4), s.Snapshot().MigrateTo)
}

func TestConditionalSlotUpdateDoesNotOverwriteNewerAssignment(t *testing.T) {
	s := New(NewOptions(WithConfigPath(t.TempDir() + "/config.json")))
	t.Cleanup(func() { _ = s.config.cfgFile.Close() })
	old := &pb.Slot{Id: 0, Leader: 1, Term: 2, Replicas: []uint64{1, 2, 3}, Learners: []uint64{4}, MigrateFrom: 3, MigrateTo: 4}
	next := old.Clone()
	next.Replicas, next.Learners, next.MigrateFrom, next.MigrateTo = []uint64{1, 2, 4}, nil, 0, 0
	// Marshal the real command payload, then apply after another task wins.
	oldData, err := pb.SlotSet{old}.Marshal()
	require.NoError(t, err)
	newData, err := pb.SlotSet{next}.Marshal()
	require.NoError(t, err)
	enc := wkproto.NewEncoder()
	defer enc.End()
	enc.WriteBinary(oldData)
	enc.WriteBinary(newData)
	cmd := NewCMD(CMDTypeSlotUpdateIfCurrent, enc.Bytes())
	newer := old.Clone()
	newer.MigrateTo, newer.Learners = 5, []uint64{5}
	s.config.cfg.Slots = []*pb.Slot{newer}
	require.NoError(t, s.handleSlotUpdateIfCurrent(cmd))
	require.True(t, s.Slot(0).Equal(newer))
	s.config.cfg.Slots = []*pb.Slot{old}
	require.NoError(t, s.handleSlotUpdateIfCurrent(cmd))
	require.True(t, s.Slot(0).Equal(next))
	require.NoError(t, s.handleSlotUpdateIfCurrent(cmd))
	require.True(t, s.Slot(0).Equal(next))
}

func TestExpansionRetiresFailureDetectedAfterAdmission(t *testing.T) {
	cfg := &pb.Config{SlotReplicaCount: 3, ChannelReplicaCount: 3}
	for _, id := range []uint64{1, 2, 3, 4} {
		cfg.Nodes = append(cfg.Nodes, &pb.Node{Id: id, AllowVote: true, Online: id != 3, Status: pb.NodeStatus_NodeStatusJoined})
	}
	require.Equal(t, uint64(3), offlineVoterWithSpareCapacity(cfg), "D already joined before C's timeout")
	cfg.Nodes = cfg.Nodes[:3]
	require.Zero(t, offlineVoterWithSpareCapacity(cfg), "heartbeat alone cannot shrink ABC to AB")
	cfg.Nodes = append(cfg.Nodes, &pb.Node{Id: 4, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoining})
	require.Zero(t, offlineVoterWithSpareCapacity(cfg), "wait for the pending membership change")
	cfg.Nodes[3].Status = pb.NodeStatus_NodeStatusJoined
	cfg.Nodes[2].Online = true
	require.Zero(t, offlineVoterWithSpareCapacity(cfg), "a recovered member must not be retired")
	cfg.Nodes[2].Online = false
	cfg.Nodes[1].Online = false
	require.Zero(t, offlineVoterWithSpareCapacity(cfg), "new capacity cannot substitute for the old quorum")
}

func TestProxyLearnerCannotReplaceOfflineVoter(t *testing.T) {
	cfg := removalTestConfig()
	cfg.Nodes[3].Role = pb.NodeRole_NodeRoleProxy
	cfg.Nodes[3].AllowVote = false
	require.Zero(t, offlineVoterForJoin(cfg, 4))
}
