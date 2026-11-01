package clusterconfig

import (
	"encoding/binary"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func replacementTestCommand(t *testing.T, typ CMDType, data []byte) *CMD {
	t.Helper()
	cmd := NewCMD(typ, data)
	cmd.version = 2
	raw, err := cmd.Marshal()
	require.NoError(t, err)
	decoded := &CMD{}
	require.NoError(t, decoded.Unmarshal(raw))
	require.Equal(t, uint16(2), decoded.version)
	return decoded
}

func TestHistoricalJoinReplayKeepsExpansionSemantics(t *testing.T) {
	for _, version := range []uint16{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			initial := []*pb.Node{
				{Id: 1, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
				{Id: 2, AllowVote: true, OfflineCount: 1, Status: pb.NodeStatus_NodeStatusJoined},
				{Id: 3, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
			}
			data, err := (&pb.Node{Id: 4, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusWillJoin}).Marshal()
			require.NoError(t, err)
			join := NewCMD(CMDTypeNodeJoin, data)
			if version == 2 {
				join.version = 2
			}
			joinBytes, err := join.Marshal()
			require.NoError(t, err)
			require.Equal(t, version, binary.BigEndian.Uint16(joinBytes))
			data = make([]byte, 8)
			binary.BigEndian.PutUint64(data, 4)
			promote := NewCMD(CMDTypeNodeJoining, data)
			if version == 2 {
				promote.version = 2
			}
			promotionBytes, err := promote.Marshal()
			require.NoError(t, err)
			for repeat := 0; repeat < 2; repeat++ {
				s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/config.json")))
				defer s.config.cfgFile.Close()
				s.config.cfg.Nodes = (&pb.Config{Nodes: initial}).Clone().Nodes
				for _, encoded := range [][]byte{joinBytes, promotionBytes} {
					cmd := &CMD{}
					require.NoError(t, cmd.Unmarshal(encoded))
					require.NoError(t, s.handleCmd(cmd))
				}
				want := []uint64{1, 2, 3, 4}
				if version == 2 {
					want = []uint64{1, 3, 4}
				}
				require.ElementsMatch(t, want, s.configToRaftConfig(s.config).Replicas)
			}
		})
	}
}

func TestFreshJoinReplacesConfirmedOfflineVoterAfterCatchup(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/config.json")))
	defer s.config.cfgFile.Close()
	s.config.cfg.Nodes = []*pb.Node{
		{Id: 1, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
		{Id: 2, AllowVote: true, OfflineCount: 1, Status: pb.NodeStatus_NodeStatusJoined},
		{Id: 3, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
	}
	data, err := (&pb.Node{Id: 4, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusWillJoin}).Marshal()
	require.NoError(t, err)
	require.NoError(t, s.handleNodeJoin(replacementTestCommand(t, CMDTypeNodeJoin, data)))
	require.Equal(t, uint64(2), s.config.cfg.MigrateFrom, "a replacement must not grow the voting quorum around a dead member")
	require.Equal(t, uint64(4), s.config.cfg.MigrateTo)
	require.ElementsMatch(t, []uint64{1, 2, 3}, s.configToRaftConfig(s.config).Replicas, "retain the old voters until the learner catches up")
	require.Equal(t, []uint64{4}, s.config.cfg.Learners)
	require.NoError(t, s.handleNodeJoin(replacementTestCommand(t, CMDTypeNodeJoin, data)), "a lost join response can cause the same request to be retried")
	require.Equal(t, uint64(2), s.config.cfg.MigrateFrom)
	require.Equal(t, []uint64{4}, s.config.cfg.Learners)
	s.config.updateNodeJoining(3, true)
	require.True(t, s.Node(2).AllowVote, "an unrelated completion must not retire the old voter")
	promotion := make([]byte, 8)
	binary.BigEndian.PutUint64(promotion, 4)
	require.NoError(t, s.handleNodeJoining(replacementTestCommand(t, CMDTypeNodeJoining, promotion)))
	require.ElementsMatch(t, []uint64{1, 3, 4}, s.configToRaftConfig(s.config).Replicas)
	require.NotNil(t, s.Node(2), "retain address and identity metadata for a returning node")
	require.False(t, s.Node(2).AllowVote)
	require.Empty(t, s.config.cfg.Learners)
	require.Zero(t, s.config.cfg.MigrateFrom)
	require.Zero(t, s.config.cfg.MigrateTo)
	require.NoError(t, s.handleNodeJoining(replacementTestCommand(t, CMDTypeNodeJoining, promotion)))
	require.ElementsMatch(t, []uint64{1, 3, 4}, s.configToRaftConfig(s.config).Replicas, "duplicate completion must be idempotent")
}

func TestFreshJoinDoesNotReplaceHealthyOrUnconfirmedVoter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		online bool
		count  uint32
		joinID uint64
	}{
		{"healthy expansion", true, 1, 4}, {"unknown status", false, 0, 4}, {"same ID recovery", false, 1, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/config.json")))
			defer s.config.cfgFile.Close()
			s.config.cfg.Nodes = []*pb.Node{{Id: 1, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined},
				{Id: 2, AllowVote: true, Online: tc.online, OfflineCount: tc.count, Status: pb.NodeStatus_NodeStatusJoined},
				{Id: 3, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusJoined}}
			data, err := (&pb.Node{Id: tc.joinID, AllowVote: true, Online: true, Status: pb.NodeStatus_NodeStatusWillJoin}).Marshal()
			require.NoError(t, err)
			require.NoError(t, s.handleNodeJoin(replacementTestCommand(t, CMDTypeNodeJoin, data)))
			require.Equal(t, tc.joinID, s.config.cfg.MigrateFrom)
			s.config.updateNodeJoining(tc.joinID, true)
			require.True(t, s.Node(2).AllowVote)
		})
	}
}
