package clusterconfig

import (
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestSubscriberProtocolPersistsInConfigAndFencesReplacement(t *testing.T) {
	opts := NewOptions(WithConfigPath(filepath.Join(t.TempDir(), "cluster.json")))
	c := NewConfig(opts)
	c.update(&types.Config{Nodes: []*types.Node{{Id: 1, ClusterAddr: "one", CreatedAt: 10}, {Id: 2, ClusterAddr: "two", CreatedAt: 10}}})
	s := &Server{config: c}
	proof := &types.Config{Nodes: []*types.Node{{Id: 1, ClusterAddr: "one", CreatedAt: 10, SubscriberProtocol: 3}, {Id: 2, ClusterAddr: "two", CreatedAt: 10, SubscriberProtocol: 3}}}
	data, err := proof.Marshal()
	require.NoError(t, err)
	require.NoError(t, s.handleCmd(NewCMD(CMDTypeSubscriberProtocols, data)))
	require.NoError(t, c.saveConfig())
	require.NoError(t, c.cfgFile.Close())
	reopened := NewConfig(opts)
	defer reopened.cfgFile.Close()
	s.config = reopened
	require.EqualValues(t, 3, s.SubscriberNodes()[1].SubscriberProtocol)
	// Config snapshots/replication carry the proof, not just local KV state.
	snapshot, err := reopened.data()
	require.NoError(t, err)
	clone := &types.Config{}
	require.NoError(t, clone.Unmarshal(snapshot))
	require.EqualValues(t, 3, clone.Nodes[0].SubscriberProtocol)
	clone.Nodes[1].CreatedAt++
	clone.Nodes[1].SubscriberProtocol = 0
	reopened.update(clone)
	require.NoError(t, s.handleCmd(NewCMD(CMDTypeSubscriberProtocols, data)))
	require.Zero(t, s.SubscriberNodes()[1].SubscriberProtocol, "delayed old-identity proof must not authorize replacement")
}

func TestSubscriberAtomicProofRejectsLateProtocolFourJoin(t *testing.T) {
	c := NewConfig(NewOptions(WithConfigPath(filepath.Join(t.TempDir(), "cluster.json"))))
	defer c.cfgFile.Close()
	c.update(&types.Config{Nodes: []*types.Node{{Id: 1, SubscriberProtocol: 5}, {Id: 2, SubscriberProtocol: 5}}})
	s := &Server{config: c}
	for _, id := range []uint64{2, 3} {
		data, err := (&types.Node{Id: id, ClusterAddr: "old-node", CreatedAt: 20, SubscriberProtocol: 4}).Marshal()
		require.NoError(t, err)
		require.NoError(t, s.handleNodeJoin(NewCMD(CMDTypeNodeJoin, data)))
		require.Len(t, s.SubscriberNodes(), 2)
		require.EqualValues(t, 5, s.SubscriberNodes()[1].SubscriberProtocol)
	}
	require.True(t, s.subscriberRevisionJoinAllowed(5))
	// A separate, not-yet-activated cluster: committed capability proofs in
	// the cluster above deliberately cannot be downgraded by config refresh.
	partial := NewConfig(NewOptions(WithConfigPath(filepath.Join(t.TempDir(), "partial.json"))))
	defer partial.cfgFile.Close()
	partial.update(&types.Config{Nodes: []*types.Node{{Id: 1, SubscriberProtocol: 5}, {Id: 2, SubscriberProtocol: 4}}})
	s.config = partial
	require.True(t, s.subscriberRevisionJoinAllowed(4))
	require.False(t, s.subscriberRevisionJoinAllowed(3))
}

func TestSubscriberRevisionProofFencesAlreadyProposedOldJoin(t *testing.T) {
	c := NewConfig(NewOptions(WithConfigPath(filepath.Join(t.TempDir(), "cluster.json"))))
	defer c.cfgFile.Close()
	c.update(&types.Config{Nodes: []*types.Node{
		{Id: 1, ClusterAddr: "one", CreatedAt: 10, SubscriberProtocol: 4},
		{Id: 2, ClusterAddr: "two", CreatedAt: 10, SubscriberProtocol: 4},
	}})
	s := &Server{config: c}
	for _, id := range []uint64{2, 3} {
		oldJoin := &types.Node{Id: id, ClusterAddr: "old-replacement", CreatedAt: 20, SubscriberProtocol: 3}
		data, err := oldJoin.Marshal()
		require.NoError(t, err)
		require.NoError(t, s.handleNodeJoin(NewCMD(CMDTypeNodeJoin, data)))
		require.Len(t, s.SubscriberNodes(), 2)
		require.EqualValues(t, 4, s.SubscriberNodes()[1].SubscriberProtocol)
		require.Equal(t, "two", s.SubscriberNodes()[1].ClusterAddr)
		require.Empty(t, c.cfg.Learners)
	}
	require.True(t, s.subscriberRevisionJoinAllowed(4))
	// Partial capability confirmation still permits protocol-3 rolling joins.
	c.update(&types.Config{Nodes: []*types.Node{{Id: 1, SubscriberProtocol: 4}, {Id: 2, SubscriberProtocol: 3}}})
	require.True(t, s.subscriberRevisionJoinAllowed(3))
}
