package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"

	"github.com/stretchr/testify/require"
)

func TestSeededJoinRetriesAfterRemovalBeforeReplay(t *testing.T) {
	f := newExpansionFixture(t)
	f.start(4, fmt.Sprintf("1@%s", f.addresses[1]), true, false)
	require.Eventually(t, func() bool { return f.servers[1].cfgServer.Node(4) != nil && f.servers[4].joinReady.Load() }, 8*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { return f.servers[f.live()].cfgServer.Node(4) == nil }, 8*time.Second, 20*time.Millisecond, "a learner that never synchronizes must be retired")
	require.Zero(t, f.servers[4].cfgServer.Snapshot().Version, "D has no applied member addresses to probe")
	f.configs[4].pause.Store(false)
	f.waitStable([]uint64{1, 2, 3, 4}, 4)
	f.send(4, 2)
	f.checkMessages([]uint64{1, 2, 3, 4})
}

func TestMembershipProbePeersFallBackToSeed(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *nodetypes.Config
		init map[uint64]string
		seed string
		want []uint64
	}{
		{"applied-members", &nodetypes.Config{Nodes: []*nodetypes.Node{{Id: 4}, {Id: 2}}}, nil, "1@127.0.0.1:11110", []uint64{2}},
		{"bootstrap", &nodetypes.Config{}, map[uint64]string{1: "127.0.0.1:11110", 4: "127.0.0.1:11114"}, "", []uint64{1}},
		{"seed-before-replay", &nodetypes.Config{}, nil, "1@127.0.0.1:11110", []uint64{1}},
		{"self-only", &nodetypes.Config{Nodes: []*nodetypes.Node{{Id: 4}}}, nil, "1@127.0.0.1:11110", []uint64{1}},
		{"invalid-seed", &nodetypes.Config{}, nil, "invalid", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{opts: NewOptions(WithSeed(tc.seed))}
			s.opts.ConfigOptions = clusterconfig.NewOptions(clusterconfig.WithNodeId(4), clusterconfig.WithInitNodes(tc.init))
			require.ElementsMatch(t, tc.want, s.membershipProbePeers(tc.cfg))
		})
	}
}
