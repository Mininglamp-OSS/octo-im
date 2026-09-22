package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestSubscriberCapabilityRequiresProofBeforeActivation(t *testing.T) {
	nodes := []*types.Node{{Id: 1}, {Id: 2, ClusterAddr: "node2", Online: true}}
	old := func(context.Context, uint64) (bool, error) { return false, nil }
	down := func(context.Context, uint64) (bool, error) { return false, errors.New("unreachable") }
	confirmed, err := probeSubscriberNodes(context.Background(), nodes, 1, old)
	require.ErrorContains(t, err, "upgraded")
	require.Len(t, confirmed, 1, "successful local proof survives another peer's failure")
	_, err = probeSubscriberNodes(context.Background(), nodes, 1, down)
	require.ErrorContains(t, err, "unconfirmed")
	require.False(t, subscriberNodesConfirmed(nodes))
}

func TestSubscriberCapabilitySharedProofNeedsNoReachability(t *testing.T) {
	nodes := []*types.Node{{Id: 1, ClusterAddr: "node1", CreatedAt: 1}, {Id: 2, ClusterAddr: "node2", CreatedAt: 1}, {Id: 3, ClusterAddr: "node3", CreatedAt: 1}}
	confirmed, err := probeSubscriberNodes(context.Background(), nodes, 1, func(context.Context, uint64) (bool, error) { return true, nil })
	require.NoError(t, err)
	data, err := (&types.Config{Nodes: confirmed}).Marshal()
	require.NoError(t, err)
	replicated := &types.Config{}
	require.NoError(t, replicated.Unmarshal(data))
	require.True(t, subscriberNodesConfirmed(replicated.Nodes))
	require.True(t, subscriberProofMatches(nodes, replicated.Nodes))
	// A different slot leader with the replicated proof does no RPC, even if
	// every other peer is currently unreachable. Raft still enforces quorum.
	more, err := probeSubscriberNodes(context.Background(), replicated.Nodes, 2, func(context.Context, uint64) (bool, error) {
		t.Error("confirmed identities must not be re-probed per proposal")
		return false, errors.New("unreachable")
	})
	require.NoError(t, err)
	require.Empty(t, more)
	nodes[2].CreatedAt++
	require.False(t, subscriberProofMatches(nodes, replicated.Nodes), "old proof cannot authorize replacement")
}

func TestSubscriberCapabilityJoinEncodingPreservesLegacyPrefix(t *testing.T) {
	r := ClusterJoinReq{NodeId: 2, ServerAddr: "node2", SubscriberProtocol: subscriberProtocolVersion}
	b, err := r.Marshal()
	require.NoError(t, err)
	var decoded ClusterJoinReq
	require.NoError(t, decoded.Unmarshal(b))
	require.Equal(t, r, decoded)
	var legacy ClusterJoinReq
	require.NoError(t, legacy.Unmarshal(b[:len(b)-4]))
	require.Zero(t, legacy.SubscriberProtocol)
}
