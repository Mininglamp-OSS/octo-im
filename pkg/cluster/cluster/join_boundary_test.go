package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterJoinResponseRecoveryBoundaryCodec(t *testing.T) {
	original := ClusterJoinResp{Nodes: []*NodeInfo{{NodeId: 1, ServerAddr: "127.0.0.1:1001"}}, JoinVersion: 42}
	data, err := original.Marshal()
	require.NoError(t, err)
	var decoded ClusterJoinResp
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, original, decoded)
	legacy := data[:len(data)-8]
	require.NoError(t, decoded.Unmarshal(legacy))
	require.Zero(t, decoded.JoinVersion, "legacy responses never reuse a prior recovery proof")
	require.Equal(t, original.Nodes, decoded.Nodes)
	for n := 1; n < 8; n++ {
		require.Error(t, decoded.Unmarshal(data[:len(legacy)+n]), "truncated boundaries must fail closed")
	}
}
