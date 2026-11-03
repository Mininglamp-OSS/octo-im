package clusterconfig

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/require"
)

func TestMergedCommandIDsReplayAddressChange(t *testing.T) {
	require.Equal(t, CMDType(11), CMDTypeConfigClusterAddrChange)
	require.Equal(t, CMDType(12), CMDTypeNodeRemove)
	require.Equal(t, CMDType(13), CMDTypeSlotUpdateIfCurrent)
	payload, err := EncodeClusterAddrChange(1, "127.0.0.1:11110")
	require.NoError(t, err)
	// An address-change log already persisted by #89 uses command 11.
	data := append([]byte{0, 1, 0, 11}, payload...)
	var cmd CMD
	require.NoError(t, cmd.Unmarshal(data))
	s := &Server{config: &Config{cfg: &types.Config{Nodes: []*types.Node{{Id: 1}}}, Log: wklog.NewWKLog("test")}}
	require.NoError(t, s.handleCmd(&cmd))
	require.Equal(t, "127.0.0.1:11110", s.Node(1).ClusterAddr)
}
