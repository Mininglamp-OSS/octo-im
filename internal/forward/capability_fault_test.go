//go:build !race

// The wkserver client and server race in their dial and reconnect paths
// (go test -race ./pkg/wkserver fails on its own tests), so these real
// transport tests run without the race detector. The cache itself is covered
// under -race by capability_test.go.

package forward

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

const faultProbePath = "/wk/channel/sendresult/capability/v1"

// faultPeer is a real wkserver peer whose capability route can be delayed,
// dropped or absent, reached through a real client connection.
type faultPeer struct {
	server *wkserver.Server
	cli    *client.Client
	delay  atomic.Int64 // nanoseconds
	drops  atomic.Int32 // requests left to drop without a response
	served atomic.Int32
}

func startFaultPeer(t *testing.T, addr string, route bool) *faultPeer {
	t.Helper()
	p := &faultPeer{server: wkserver.New(addr)}
	if route {
		p.server.Route(faultProbePath, func(c *wkserver.Context) {
			p.served.Add(1)
			if p.drops.Add(-1) >= 0 {
				return
			}
			time.Sleep(time.Duration(p.delay.Load()))
			c.WriteStatus(proto.StatusOK)
		})
	}
	require.NoError(t, p.server.Start())
	t.Cleanup(p.server.Stop)
	p.cli = client.New(addr, client.WithUid("api-node"))
	require.NoError(t, p.cli.Start())
	t.Cleanup(p.cli.Stop)
	require.Eventually(t, p.cli.IsAuthed, 2*time.Second, 10*time.Millisecond)
	return p
}

func (p *faultPeer) cache() *CapabilityCache {
	return &CapabilityCache{Path: faultProbePath, Policy: testPolicy(), request: func(ctx context.Context, _ uint64, path string) (*proto.Response, error) {
		return p.cli.RequestWithContext(ctx, path, nil)
	}}
}

func TestCapabilityFaultDelayedOrDroppedProbe(t *testing.T) {
	peer := startFaultPeer(t, "tcp://127.0.0.1:19371", true)
	peer.drops.Store(0)

	// A response slower than the first step is accepted by the second.
	peer.delay.Store(int64(400 * time.Millisecond))
	cache := peer.cache()
	v := cache.Check(context.Background(), 2)
	require.True(t, v.Supported, "a 400 ms answer must not yield a 503")

	// One dropped response is retried by the ladder.
	peer.delay.Store(0)
	peer.drops.Store(1)
	cache = peer.cache()
	require.True(t, cache.Check(context.Background(), 3).Supported)

	// Slower than the whole ladder: inconclusive for this caller only.
	peer.delay.Store(int64(1500 * time.Millisecond))
	cache = peer.cache()
	v = cache.Check(context.Background(), 4)
	require.False(t, v.Supported)
	require.False(t, v.Definitive)
	require.Equal(t, CapabilityInconclusive, v.Reason)
	peer.delay.Store(0)
	time.Sleep(1600 * time.Millisecond) // let the late handlers finish
	require.True(t, cache.Check(context.Background(), 4).Supported, "nothing negative was cached")
}

func TestCapabilityFaultSilentOldRoute(t *testing.T) {
	// A build without the send-result route drops the request silently.
	peer := startFaultPeer(t, "tcp://127.0.0.1:19372", false)
	cache := peer.cache()
	for range 2 {
		start := time.Now()
		v := cache.Check(context.Background(), 2)
		elapsed := time.Since(start)
		require.False(t, v.Supported)
		require.False(t, v.Definitive, "silence is never proof of an old build")
		require.Equal(t, ProbeTimeout, v.ErrClass)
		require.InDelta(t, 1250*time.Millisecond, elapsed, float64(250*time.Millisecond))
	}
}

func TestCapabilityFaultReconnect(t *testing.T) {
	peer := startFaultPeer(t, "tcp://127.0.0.1:19373", true)
	cache := peer.cache()
	require.True(t, cache.Check(context.Background(), 2).Supported)

	peer.cli.CloseConn()
	require.False(t, peer.cli.IsAuthed())

	// A known peer keeps its positive across the reconnect window.
	require.Equal(t, CapabilityVerdict{Supported: true, Cached: true}, cache.Check(context.Background(), 2))

	// An unknown peer fails fast and is probed again once re-authed.
	cold := peer.cache()
	start := time.Now()
	v := cold.Check(context.Background(), 3)
	if !v.Supported {
		require.Equal(t, CapabilityInconclusive, v.Reason)
		require.Less(t, time.Since(start), 1500*time.Millisecond)
	}
	require.Eventually(t, peer.cli.IsAuthed, 2*time.Second, 10*time.Millisecond)
	require.True(t, cold.Check(context.Background(), 3).Supported)
}
