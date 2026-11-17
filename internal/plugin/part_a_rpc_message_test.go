package plugin

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	extwklog "github.com/WuKongIM/wklog"
	wkrpcclient "github.com/WuKongIM/wkrpc/client"
	wkrpcproto "github.com/WuKongIM/wkrpc/proto"
	"github.com/stretchr/testify/require"
)

type pluginTestChannel struct {
	icluster.Channel
	appendCalls atomic.Int32
	canonicalID uint64
}

func (p *pluginTestChannel) ProposeBatchUntilAppliedTimeout(_ context.Context, _ string, _ uint8, reqs rafttypes.ProposeReqSet) (rafttypes.ProposeRespSet, error) {
	p.appendCalls.Add(1)
	resps := make(rafttypes.ProposeRespSet, len(reqs))
	for i, r := range reqs {
		cid := p.canonicalID
		if cid == 0 {
			cid = r.Id
		}
		resps[i] = &rafttypes.ProposeResp{
			Id:          r.Id,
			CanonicalID: cid,
			Index:       uint64(i + 1),
		}
	}
	return resps, nil
}

type pluginTestSlot struct {
	icluster.Slot
}

func (pluginTestSlot) GetSlotId(string) uint32 { return 0 }

type pluginTestBus struct {
	onAdd func(ev *eventbus.Event)
}

func (b *pluginTestBus) AddEvent(_ string, _ uint8, ev *eventbus.Event) {
	if b.onAdd != nil {
		b.onAdd(ev)
	}
}
func (b *pluginTestBus) Advance(string, uint8) {}

func TestPartA_V11_3_PluginRPCMessageSend_SyncPersistAndBackpressure(t *testing.T) {
	logOpts := wklog.NewOptions()
	logOpts.LogDir = t.TempDir()
	wklog.Configure(logOpts)
	extOpts := extwklog.NewOptions()
	extOpts.LogDir = t.TempDir()
	extwklog.Configure(extOpts)

	oldOpts, oldStore, oldPerm, oldCh := options.G, service.Store, service.Permission, eventbus.Channel
	oldMaxB, oldMaxI := eventbus.DefaultPostCommitBudget.Limits()
	t.Cleanup(func() {
		eventbus.DefaultPostCommitBudget.SetLimits(oldMaxB, oldMaxI)
		options.G = oldOpts
		service.Store = oldStore
		service.Permission = oldPerm
		eventbus.Channel = oldCh
	})

	opts := options.New()
	opts.Cluster.NodeId = 1001
	options.G = opts

	wdb := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(t.TempDir()),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(1),
	))
	require.NoError(t, wdb.Open())
	t.Cleanup(func() { _ = wdb.Close() })

	chSrv := &pluginTestChannel{canonicalID: 777001}
	service.Store = store.New(store.NewOptions(
		store.WithNodeId(1001),
		store.WithDB(wdb),
		store.WithSlot(pluginTestSlot{}),
		store.WithChannel(chSrv),
	))
	service.Permission = nil

	var admittedSeq atomic.Uint64
	eventbus.RegisterChannel(&pluginTestBus{onAdd: func(ev *eventbus.Event) {
		if ev != nil {
			admittedSeq.Store(ev.MessageSeq)
			if ev.Ticket != nil {
				ev.Ticket.Release()
				ev.Ticket = nil
			}
		}
	}})

	tmpDir := t.TempDir()
	sockDir, err := os.MkdirTemp(filepath.Dir(os.TempDir()), "wkp")
	if err != nil {
		sockDir = tmpDir
	} else {
		t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	}
	sockPath := filepath.Join(sockDir, "p.sock")
	ps := NewServer(NewOptions(WithDir(filepath.Join(tmpDir, "plugins")), WithSocketPath(sockPath)))
	require.NoError(t, ps.Start())
	t.Cleanup(func() { ps.Stop() })

	addr, err := getUnixSocket(sockPath)
	require.NoError(t, err)
	cli := wkrpcclient.New(addr, wkrpcclient.WithUid("plugin-cli"))
	require.NoError(t, cli.Start())
	t.Cleanup(func() { cli.Stop() })
	require.Eventually(t, func() bool { return cli.IsAuthed() }, 2*time.Second, 20*time.Millisecond)

	// 1. Exhaust PostCommitBudget -> /message/send must return error with ZERO Raft AppendMessages calls!
	eventbus.DefaultPostCommitBudget.SetLimits(1, 0)
	reqProto := &pluginproto.SendReq{
		FromUid:     "bot1",
		ChannelId:   "grp_plugin",
		ChannelType: 2,
		Payload:     []byte("hello-from-plugin"),
	}
	reqBytes, err := reqProto.Marshal()
	require.NoError(t, err)

	bpResp, bpErr := cli.Request("/message/send", reqBytes)
	if bpErr == nil && bpResp != nil {
		require.NotEqual(t, wkrpcproto.StatusOK, bpResp.Status, "V11-3: plugin /message/send must fail under PostCommitBudget exhaustion")
	}
	require.Zero(t, chSrv.appendCalls.Load(), "V11-3: zero Raft writes when PostCommitBudget is exhausted")

	// 2. Restore PostCommitBudget -> /message/send commits to Raft BEFORE returning canonical MessageId (777001)!
	eventbus.DefaultPostCommitBudget.SetLimits(oldMaxB, oldMaxI)
	okResp, okErr := cli.Request("/message/send", reqBytes)
	require.NoError(t, okErr)
	require.Equal(t, wkrpcproto.StatusOK, okResp.Status)

	var sendResp pluginproto.SendResp
	require.NoError(t, sendResp.Unmarshal(okResp.Body))
	require.Equal(t, int64(777001), sendResp.MessageId, "V11-3: SendResp must return canonical Raft-committed MessageId")
	require.Equal(t, int32(1), chSrv.appendCalls.Load())
	require.Equal(t, uint64(1), admittedSeq.Load())

	usedB, usedI, _ := eventbus.DefaultPostCommitBudget.Stats()
	require.Zero(t, usedB)
	require.Zero(t, usedI)
}
