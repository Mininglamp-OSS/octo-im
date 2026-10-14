package cluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// Real TCP forwarding to a separate message leader, with canonical results
// returned across RPC and only one row persisted in Pebble.
func TestMessageRetryRPC(t *testing.T) {
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	addresses := map[uint64]string{}
	for id := uint64(1); id <= 2; id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[id] = l.Addr().String()
		require.NoError(t, l.Close())
	}
	nodes := make([]*Server, 2)
	for i := range nodes {
		id := uint64(i + 1)
		cfg := newTestOptions(t, id, addresses, clusterconfig.WithSlotCount(2), clusterconfig.WithSlotMaxReplicaCount(2))
		s := New(NewOptions(WithAddr("tcp://"+addresses[id]), WithConfigOptions(cfg), WithDataDir(t.TempDir()), WithDBWKDbShardNum(1), WithDBSlotShardNum(1)))
		require.NoError(t, s.Start())
		t.Cleanup(s.Stop)
		nodes[i] = s
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, s := range nodes {
		require.NoError(t, s.WaitAllSlotReady(ctx, 2))
	}
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry-rpc", ChannelType: 2, LeaderId: 2, Replicas: []uint64{2}, ReplicaMaxCount: 1, Term: 1, ConfVersion: 1, Status: wkdb.ChannelClusterStatusNormal}
	for _, s := range nodes {
		require.NoError(t, s.db.SaveChannelClusterConfig(cfg))
	}
	require.NoError(t, nodes[1].channelServer.WakeLeaderIfNeed(cfg))
	m := wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: cfg.ChannelId, ChannelType: 2, FromUID: "sender", ClientMsgNo: "lost-ack", MessageID: 100, Payload: []byte("hello")}}
	first, err := nodes[0].store.AppendMessages(ctx, cfg.ChannelId, 2, []wkdb.Message{m})
	require.NoError(t, err)
	m.MessageID = 200
	retry, err := nodes[0].store.AppendMessages(ctx, cfg.ChannelId, 2, []wkdb.Message{m, m})
	require.NoError(t, err)
	require.Len(t, retry, 2)
	for _, r := range retry {
		require.Equal(t, first[0].Index, r.Index)
		require.Equal(t, uint64(100), r.CanonicalID)
		require.Equal(t, uint64(200), r.Id)
		require.True(t, r.Duplicate)
	}
	seq, _, err := nodes[1].db.GetChannelLastMessageSeq(cfg.ChannelId, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)
	legacy, err := nodes[0].RequestWithContext(ctx, 2, "/rpc/channel/propose", nil)
	require.NoError(t, err)
	require.NotEqual(t, proto.StatusOK, legacy.Status)
	// A legacy/partial response cannot silently discard canonical metadata.
	nodes[1].netServer.Route("/rpc/channel/propose/v2", func(c *wkserver.Context) { c.Write([]byte(`{"version":2,"results":[]}`)) })
	_, err = nodes[0].store.AppendMessages(ctx, cfg.ChannelId, 2, []wkdb.Message{m})
	require.Error(t, err)
}
