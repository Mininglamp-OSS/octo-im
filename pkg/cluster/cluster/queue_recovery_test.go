package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

func TestQueueConcurrentResizeClose(t *testing.T) {
	q := NewAdaptiveSendQueue(2, 512, 1<<20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				_ = q.Send(&proto.Message{Content: []byte("retry")}, false)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if _, ok := q.Receive(ctx); !ok {
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		q.Shrink()
	}
	q.Close()
	q.Close()
	wg.Wait()
	require.ErrorIs(t, q.Send(&proto.Message{}, false), ErrQueueClosed)
	require.Equal(t, uint64(0), q.rl.Get())
}

func TestUnavailablePeerRetainsBoundedQueueAndStops(t *testing.T) {
	opts := NewOptions()
	opts.SendQueueLength = 10
	opts.MaxSendQueueSize = 128
	n := NewImprovedNode(1, "recovery", "127.0.0.1:1", opts)
	msg := &proto.Message{Content: make([]byte, 64)}
	require.NoError(t, n.Send(msg))
	for i := 0; i < 20; i++ {
		n.processBatch(context.Background())
	}
	require.Equal(t, int64(1), n.backpressure.currentCount.Load())
	require.Equal(t, uint64(msg.Size()), n.backpressure.currentBytes.Load())
	require.ErrorIs(t, n.Send(msg), ErrBackpressure)
	done := make(chan struct{})
	go func() { n.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked by disconnected peer")
	}
	require.ErrorIs(t, n.Send(msg), ErrQueueClosed)
	require.Equal(t, int64(0), n.backpressure.currentCount.Load())
}

func TestCommittedRemovalClosesPeerQueueAndPreservesOfflineMembers(t *testing.T) {
	opts := NewOptions(WithConfigOptions(clusterconfig.NewOptions(clusterconfig.WithNodeId(1))))
	s := &Server{opts: opts, nodeManager: newNodeManager(opts), configReconciler: newChannelConfigReconciler(context.Background())}
	defer s.nodeManager.stop()
	cfg := &nodetypes.Config{Nodes: []*nodetypes.Node{{Id: 1, ClusterAddr: "127.0.0.1:1", Online: true}, {Id: 2, ClusterAddr: "127.0.0.1:2", Online: false}}}
	s.OnConfigChange(cfg)
	old := s.nodeManager.node(2)
	require.NotNil(t, old, "offline is still a member until committed retirement")
	require.NoError(t, old.Send(&proto.Message{Content: []byte("pending")}))
	s.addOrUpdateNodes(map[uint64]string{4: "127.0.0.1:4"})
	require.Same(t, old, s.nodeManager.node(2), "partial seed/join maps cannot remove peers")
	s.OnConfigChange(&nodetypes.Config{Nodes: []*nodetypes.Node{{Id: 1, ClusterAddr: "127.0.0.1:1"}, {Id: 4, ClusterAddr: "127.0.0.1:4"}}})
	require.Nil(t, s.nodeManager.node(2))
	require.ErrorIs(t, old.Send(&proto.Message{}), ErrQueueClosed)
	require.Zero(t, old.backpressure.currentCount.Load())
	s.OnConfigChange(&nodetypes.Config{})
	require.Empty(t, s.nodeManager.nodes())
}
