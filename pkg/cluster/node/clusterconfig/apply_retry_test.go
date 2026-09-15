package clusterconfig

import (
	"context"
	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
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

type applyRetryListener struct{ versions []uint64 }

func (l *applyRetryListener) OnConfigChange(cfg *pb.Config) {
	l.versions = append(l.versions, cfg.Version)
}

func TestApplyRetrySkipsCompletedBatchPrefix(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/cluster.json")))
	s.config.cfg.Nodes = []*pb.Node{{Id: 1, Online: true}}
	listener := &applyRetryListener{}
	s.AddEventListener(listener)
	payload, err := EncodeNodeOnlineStatusChange(1, false)
	require.NoError(t, err)
	data, err := NewCMD(CMDTypeNodeOnlineStatusChange, payload).Marshal()
	require.NoError(t, err)
	logs := []types.Log{{Index: 1, Term: 1, Data: data}, {Index: 2, Term: 1, Data: []byte{0}}}
	require.Error(t, s.applyLogs(logs))
	require.Equal(t, []uint64{1}, listener.versions)
	// A closed file proves that a completed prefix is not rewritten while
	// retrying a later failed command or the batch's applied-index write.
	require.NoError(t, s.config.cfgFile.Close())
	require.NoError(t, s.applyLog(logs[0]))
	require.Error(t, s.applyLogs(logs))
	require.Equal(t, []uint64{1}, listener.versions)
	require.Equal(t, uint32(1), s.config.cfg.Nodes[0].OfflineCount)
}

func TestApplyRetryMembershipDoesNotRewritePersistedConfig(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/cluster.json")))
	require.NoError(t, s.storage.Open())
	defer s.storage.Close()
	require.NoError(t, s.initRaft())
	s.raft.Stop() // StepWait must fail after the config file has been saved.
	listener := &applyRetryListener{}
	s.AddEventListener(listener)
	payload, err := (&pb.Node{Id: 2, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica}).Marshal()
	require.NoError(t, err)
	data, err := NewCMD(CMDTypeNodeJoin, payload).Marshal()
	require.NoError(t, err)
	log := types.Log{Index: 1, Term: 1, Data: data}
	require.ErrorIs(t, s.applyLog(log), types.ErrStopped)
	require.True(t, s.membershipPending)
	require.Empty(t, listener.versions)
	require.NoError(t, s.config.cfgFile.Close())
	// Retry delivery on a live raft; persistence must not be retried through
	// the closed file and listeners must run only once after delivery succeeds.
	require.NoError(t, s.initRaft())
	require.NoError(t, s.raft.Start())
	defer s.raft.Stop()
	require.NoError(t, s.applyLog(log))
	require.False(t, s.membershipPending)
	require.Equal(t, []uint64{1}, listener.versions)
	require.NoError(t, s.applyLog(log))
	require.Equal(t, []uint64{1}, listener.versions)
}

func TestApplyRetryAcceptsMembershipAlreadyAdoptedByRaft(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/cluster.json")))
	require.NoError(t, s.Start())
	defer s.Stop()
	defer s.config.cfgFile.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	newer := types.Config{Version: 10, Replicas: []uint64{1}, Leader: 1, Term: 1}
	require.NoError(t, s.raft.StepWait(ctx, types.Event{Type: types.ConfChange, Config: newer}))
	payload, err := (&pb.Node{Id: 2, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica}).Marshal()
	require.NoError(t, err)
	data, err := NewCMD(CMDTypeNodeJoin, payload).Marshal()
	require.NoError(t, err)
	require.NoError(t, s.applyLog(types.Log{Index: 1, Term: 1, Data: data}))
	require.False(t, s.membershipPending)
	newer.Version = 9
	require.ErrorIs(t, s.raft.StepWait(ctx, types.Event{Type: types.ConfChange, Config: newer}), raft.ErrConfigVersionStale)
}

func TestApplyMalformedCommandReturnsError(t *testing.T) {
	s := New(NewOptions(WithConfigPath(t.TempDir() + "/cluster.json")))
	defer s.config.cfgFile.Close()
	for _, kind := range []CMDType{CMDTypeNodeOnlineStatusChange, CMDTypeNodeJoining} {
		data, err := NewCMD(kind, []byte{0}).Marshal()
		require.NoError(t, err)
		require.NotPanics(t, func() { require.Error(t, s.applyLog(types.Log{Index: 1, Term: 1, Data: data})) })
		require.Zero(t, s.config.version())
	}
}
