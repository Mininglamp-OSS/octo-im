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
