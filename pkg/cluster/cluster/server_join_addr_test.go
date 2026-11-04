package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/stretchr/testify/require"
)

func TestJoinServerAddr(t *testing.T) {
	const explicit = "explicit.internal:11110"
	const committed = "committed.internal:11110"
	const bootstrap = "bootstrap.internal:11110"
	for _, tc := range []struct {
		name, serverAddr, committedAddr, bootstrapAddr, want string
		invalid                                              bool
	}{
		{"explicit_wins", explicit, committed, bootstrap, explicit, false},
		{"committed_before_bootstrap", "", committed, bootstrap, committed, false},
		{"legacy_unused_server_addr", "0.0.0.0:11110", committed, bootstrap, committed, false},
		{"bootstrap_without_committed_self", "", "", bootstrap, bootstrap, false},
		{"bootstrap_with_legacy_server_addr", "0.0.0.0:11110", "", bootstrap, bootstrap, false},
		{"invalid_committed_falls_back", "", "[::]:11110", bootstrap, bootstrap, false},
		{"no_usable_address", "0.0.0.0:11110", "[::]:11110", "invalid", "0.0.0.0:11110", true},
		{"missing_self_address", "", "", "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{opts: &Options{ServerAddr: tc.serverAddr, ConfigOptions: &clusterconfig.Options{
				NodeId: 1, InitNodes: map[uint64]string{1: tc.bootstrapAddr},
			}}}
			cfg := &types.Config{Nodes: []*types.Node{{Id: 2, ClusterAddr: "peer.internal:11110"}}}
			if tc.committedAddr != "" {
				cfg.Nodes = append(cfg.Nodes, &types.Node{Id: 1, ClusterAddr: tc.committedAddr})
			}
			addr := s.joinServerAddr(cfg)
			require.Equal(t, tc.want, addr)
			if tc.invalid {
				require.Error(t, types.ValidateJoinAddr(addr), "missing self address must not reuse a peer address or bypass validation")
			} else {
				require.NoError(t, types.ValidateJoinAddr(addr))
			}
		})
	}
}
