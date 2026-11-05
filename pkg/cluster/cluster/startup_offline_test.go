package cluster

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStartupOfflineRaftDoesNotFillQueue(t *testing.T) {
	n := NewImprovedNode(2, "offline-test", "127.0.0.1:1", NewOptions())
	defer n.Stop()
	for i := 0; i < 10000; i++ {
		for _, typ := range []uint32{MsgTypeNode, MsgTypeSlot, MsgTypeChannel} {
			require.ErrorIs(t, n.sendRaftMessage(&proto.Message{MsgType: typ}, true), errRaftPeerUnavailable)
		}
	}
	require.Zero(t, n.backpressure.currentCount.Load())
	require.Zero(t, n.backpressure.currentBytes.Load())
	require.False(t, n.hasMessages())
	require.Equal(t, uint64(30000), n.perfMonitor.totalDropped.Load())
	// Once the transport is available, the normal queue path is unchanged.
	n.SetTestMode(true)
	require.NoError(t, n.sendRaftMessage(&proto.Message{MsgType: MsgTypeNode}, true))
	require.Equal(t, int64(1), n.backpressure.currentCount.Load())
}
