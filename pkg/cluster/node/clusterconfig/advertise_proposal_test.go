package clusterconfig

import (
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestAddressProposalsRejectInvalidBeforeRaft(t *testing.T) {
	// No running Raft is needed: invalid values must be rejected before admission.
	s := &Server{}
	for _, addr := range []string{"0.0.0.0:11110", "[::]:11110", "node.internal", ":11110", "node.internal:"} {
		t.Run(addr, func(t *testing.T) {
			require.Error(t, s.ProposeClusterAddr(1, addr))
			require.Error(t, s.ProposeJoin(&pb.Node{Id: 2, ClusterAddr: addr}))
			require.Error(t, s.ProposeConfig(&pb.Config{Nodes: []*pb.Node{{Id: 1, ClusterAddr: addr}}}))
		})
	}
}
