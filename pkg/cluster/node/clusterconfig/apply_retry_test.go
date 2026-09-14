package clusterconfig

import (
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestApplyRetryDoesNotRepeatSavedStateMutation(t *testing.T) {
	path := t.TempDir() + "/cluster.json"
	s := New(NewOptions(WithNodeId(1), WithConfigPath(path)))
	s.config.cfg.Nodes = []*pb.Node{{Id: 1, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica, Online: true}}
	s.config.cfg.Version = 1
	payload, err := EncodeNodeOnlineStatusChange(1, false)
	require.NoError(t, err)
	data, err := NewCMD(CMDTypeNodeOnlineStatusChange, payload).Marshal()
	require.NoError(t, err)
	log := types.Log{Index: 2, Term: 1, Data: data}
	require.NoError(t, s.config.cfgFile.Close()) // fail persistence after the mutation
	require.Error(t, s.applyLog(log))
	require.Equal(t, uint32(1), s.config.cfg.Nodes[0].OfflineCount)
	require.Error(t, s.applyLog(log))
	require.Equal(t, uint32(1), s.config.cfg.Nodes[0].OfflineCount)
	s.config.cfgFile, err = os.OpenFile(path, os.O_RDWR, 0600)
	require.NoError(t, err)
	defer s.config.cfgFile.Close()
	require.NoError(t, s.applyLog(log))
	require.Equal(t, uint32(1), s.config.cfg.Nodes[0].OfflineCount)
	restored := NewConfig(NewOptions(WithConfigPath(path)))
	defer restored.cfgFile.Close()
	require.Equal(t, uint64(2), restored.cfg.Version)
	require.Equal(t, uint32(1), restored.cfg.Nodes[0].OfflineCount)
}
