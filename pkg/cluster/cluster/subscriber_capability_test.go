package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
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

func TestSubscriberAtomicRulesRequireAllReplicasAndStrongestBatchProof(t *testing.T) {
	command := func(kind store.CMDType, protocol uint32) rafttypes.ProposeReq {
		data, err := json.Marshal(wkdb.SubscriberOperation{SourceProtocol: protocol})
		require.NoError(t, err)
		wire, err := store.NewCMD(kind, data).Marshal()
		require.NoError(t, err)
		return rafttypes.ProposeReq{Data: wire}
	}
	strict := command(store.CMDSubscriberOperation, subscriberAtomicProtocolVersion)
	legacy := command(store.CMDSubscriberReconcile, 0)
	for _, batch := range []rafttypes.ProposeReqSet{{strict, legacy}, {legacy, strict}} {
		need, err := subscriberProposalProtocol(batch)
		require.NoError(t, err)
		require.EqualValues(t, 5, need)
	}
	nodes := []*types.Node{{Id: 1, SubscriberProtocol: 5}, {Id: 2, SubscriberProtocol: 4}}
	require.False(t, subscriberNodesConfirmedAt(nodes, 5))
	require.EqualValues(t, 4, requiredSubscriberJoinProtocol(nodes))
	_, err := probeSubscriberNodesAt(context.Background(), nodes, 1, 5, func(context.Context, uint64) (bool, error) { return false, nil })
	require.Error(t, err)
	nodes[1].SubscriberProtocol = 5
	require.True(t, subscriberNodesConfirmedAt(nodes, 5))
	require.EqualValues(t, 5, requiredSubscriberJoinProtocol(nodes))
	_, err = subscriberProposalProtocol(rafttypes.ProposeReqSet{command(store.CMDSubscriberReconcile, 6)})
	require.ErrorContains(t, err, "unsupported")
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

func TestSubscriberRevisionNeedsDistinctProofFromProtocolThree(t *testing.T) {
	nodes := []*types.Node{{Id: 1, SubscriberProtocol: 3}, {Id: 2, SubscriberProtocol: 3}}
	require.True(t, subscriberNodesConfirmed(nodes), "ordinary recovery remains available while upgrading")
	require.False(t, subscriberNodesConfirmedAt(nodes, subscriberRevisionProtocolVersion))
	called := false
	proof, err := probeSubscriberNodesAt(context.Background(), nodes, 1, subscriberRevisionProtocolVersion, func(_ context.Context, id uint64) (bool, error) {
		called = true
		require.EqualValues(t, 2, id)
		return false, nil // protocol-3 server has no revision capability endpoint
	})
	require.True(t, called)
	require.Error(t, err)
	require.Len(t, proof, 1)
	require.False(t, subscriberProofMatchesAt(nodes, nodes, subscriberRevisionProtocolVersion))
	proof, err = probeSubscriberNodesAt(context.Background(), nodes, 1, subscriberRevisionProtocolVersion, func(context.Context, uint64) (bool, error) { return true, nil })
	require.NoError(t, err)
	require.True(t, subscriberProofMatchesAt(nodes, proof, subscriberRevisionProtocolVersion))
	proof[1].Online = false
	_, err = probeSubscriberNodesAt(context.Background(), proof, 1, subscriberRevisionProtocolVersion, func(context.Context, uint64) (bool, error) {
		t.Fatal("replicated revision capability should survive peer unavailability")
		return false, nil
	})
	require.NoError(t, err)
	nodes[1].CreatedAt++
	require.False(t, subscriberProofMatchesAt(nodes, proof, subscriberRevisionProtocolVersion), "replacement node needs a new proof")
}

func TestSubscriberJoinDoesNotConfusePartialCapabilityWithActivation(t *testing.T) {
	nodes := []*types.Node{{Id: 1, SubscriberProtocol: 4}, {Id: 2, SubscriberProtocol: 3}}
	require.EqualValues(t, 3, requiredSubscriberJoinProtocol(nodes))
	nodes[1].SubscriberProtocol = 4
	require.EqualValues(t, 4, requiredSubscriberJoinProtocol(nodes))
	// Online status does not erase a committed protocol proof.
	nodes[1].Online = false
	require.EqualValues(t, 4, requiredSubscriberJoinProtocol(nodes))
}
