package cluster

import (
	"context"
	"encoding/json"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	nt "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rt "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/stretchr/testify/require"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type startupPeerTransport struct {
	s              *Server
	sends, missing atomic.Int64
}

func (p *startupPeerTransport) Send(_ string, e rt.Event) {
	p.sends.Add(1)
	if p.s.nodeManager.node(e.To) == nil {
		p.missing.Add(1)
	}
}
func TestStartupPersistedSlotsHavePeersBeforeSend(t *testing.T) {
	old := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	defer trace.SetGlobalTrace(old)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()
	opts := newTestOptions(t, 1, nil, clusterconfig.WithSlotCount(64))
	cfg := &nt.Config{Version: 1, Term: 1, SlotCount: 64, SlotReplicaCount: 2, Nodes: []*nt.Node{{Id: 1, ClusterAddr: addr, Online: true}, {Id: 2, ClusterAddr: "127.0.0.1:1", Online: true}}}
	for i := 0; i < 64; i++ {
		cfg.Slots = append(cfg.Slots, &nt.Slot{Id: uint32(i), Leader: 2, Term: 1, Replicas: []uint64{1, 2}})
	}
	require.NoError(t, os.MkdirAll(filepath.Dir(opts.ConfigPath), 0700))
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(opts.ConfigPath, raw, 0600))
	tr := &startupPeerTransport{}
	options := NewOptions(WithAddr("tcp://"+addr), WithConfigOptions(opts), WithDataDir(t.TempDir()), WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20))
	options.SlotTransport = tr
	s := New(options)
	tr.s = s
	start := time.Now()
	require.NoError(t, s.Start())
	defer s.Stop()
	require.Less(t, time.Since(start), 5*time.Second, "unavailable peer must not block startup")
	require.Eventually(t, func() bool { return tr.sends.Load() > 0 }, 2*time.Second, 10*time.Millisecond)
	require.Zero(t, tr.missing.Load(), "restored slots dispatched before their transport peers existed")
}
