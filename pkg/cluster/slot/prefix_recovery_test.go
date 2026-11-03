package slot

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type prefixRecoveryTransport struct {
	peers    map[uint64]*Server
	holdSync atomic.Bool
}

func (tr *prefixRecoveryTransport) Send(key string, e types.Event) {
	if e.Type == types.SyncResp && e.To == 2 && tr.holdSync.Load() {
		return
	}
	if s := tr.peers[e.To]; s != nil {
		s.AddEvent(key, e)
	}
}

// Reproduce the physical metadata divergence: index 2 is committed by the new
// leader but the returning follower still stores different, uncommitted bytes
// at that index. Let heartbeats arrive before any alignment response.
func TestSlotRestartRepairsConflictingChannelMetadata(t *testing.T) {
	if trace.GlobalTrace == nil {
		trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	}
	tr := &prefixRecoveryTransport{peers: map[uint64]*Server{}}
	tr.holdSync.Store(true)
	var databases []wkdb.DB
	var stores []*store.Store
	var servers []*Server
	var dbPaths []string
	logFor := func(leader uint64, term uint32, index uint64) types.Log {
		cfg := wkdb.ChannelClusterConfig{ChannelId: "returning-channel", ChannelType: 2, LeaderId: leader, Term: term, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}
		data, err := cfg.Marshal()
		require.NoError(t, err)
		data, err = store.EncodeCMDChannelClusterConfigSave(cfg.ChannelId, cfg.ChannelType, data)
		require.NoError(t, err)
		data, err = store.NewCMD(store.CMDChannelClusterConfigSave, data).Marshal()
		require.NoError(t, err)
		logTerm := uint32(1)
		if leader == 1 {
			logTerm = 2
		}
		return types.Log{Index: index, Term: logTerm, Data: data}
	}
	prefix, canonical, obsolete := logFor(2, 1, 1), logFor(1, 2, 2), logFor(2, 3, 2)
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		for _, s := range servers {
			s.Stop()
		}
		for _, s := range stores {
			s.Stop()
		}
		for _, db := range databases {
			require.NoError(t, db.Close())
		}
	}
	t.Cleanup(stop)
	for _, id := range []uint64{1, 2} {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "db")
		dbPaths = append(dbPaths, dbPath)
		db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dbPath), wkdb.WithShardNum(1), wkdb.WithNodeId(id)))
		require.NoError(t, db.Open())
		databases = append(databases, db)
		stateMachine := store.New(store.NewOptions(store.WithDB(db)))
		require.NoError(t, stateMachine.Start())
		stores = append(stores, stateMachine)
		s := NewServer(NewOptions(WithNodeId(id), WithDataDir(dir), WithSlotDbShardNum(1), WithTransport(tr), WithOnApply(stateMachine.ApplySlotLogs)))
		require.NoError(t, s.storage.Open())
		servers = append(servers, s)
		tr.peers[id] = s
		tail := canonical
		if id == 2 {
			tail = obsolete
		}
		require.NoError(t, s.storage.AppendLogs("59", []types.Log{prefix}, &types.TermStartIndexInfo{Term: 1, Index: 1}))
		require.NoError(t, s.storage.AppendLogs("59", []types.Log{tail}, &types.TermStartIndexInfo{Term: tail.Term, Index: map[uint32]uint64{1: 1, 2: 2}[tail.Term]}))
		logs := []types.Log{prefix}
		if id == 1 {
			logs = append(logs, tail)
		}
		require.NoError(t, s.storage.Apply("59", logs))
	}
	for _, s := range servers {
		require.NoError(t, s.raftGroup.Start())
		s.OnConfigChange(&nodetypes.Config{Version: 2, Slots: []*nodetypes.Slot{{Id: 59, Leader: 1, Term: 2, Replicas: []uint64{1, 2, 3}}}})
	}
	follower := servers[1]
	require.NoError(t, follower.raftGroup.Do(context.Background(), "59", func(r raftgroup.IRaft) error {
		return r.Step(types.Event{Type: types.Ping, From: 1, To: 2, Term: 2, CommittedIndex: 2})
	}))
	var committed uint64
	require.NoError(t, follower.raftGroup.Do(context.Background(), "59", func(r raftgroup.IRaft) error {
		committed = r.(*Slot).CommittedIndex()
		return nil
	}))
	require.Equal(t, uint64(1), committed, "heartbeat cannot commit the divergent old tail")
	tr.holdSync.Store(false)
	require.Eventually(t, func() bool {
		cfg, err := databases[1].GetChannelClusterConfig("returning-channel", 2)
		index, indexErr := follower.storage.AppliedIndex("59")
		return err == nil && indexErr == nil && index == 2 && cfg.LeaderId == 1 && cfg.Term == 2 && cfg.ConfVersion == 2
	}, 5*time.Second, 10*time.Millisecond)
	// A second write must commit through the repaired majority as well.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := servers[0].ProposeUntilAppliedTimeoutForLocal(ctx, 59, types.ProposeReqSet{{Id: 3, Data: canonical.Data}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { v, err := follower.storage.AppliedIndex("59"); return err == nil && v == 3 }, 3*time.Second, 10*time.Millisecond)
	stop()
	for _, path := range dbPaths {
		db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(path), wkdb.WithShardNum(1)))
		require.NoError(t, db.Open())
		cfg, err := db.GetChannelClusterConfig("returning-channel", 2)
		require.NoError(t, err)
		require.Equal(t, uint64(1), cfg.LeaderId)
		require.Equal(t, uint32(2), cfg.Term)
		require.Equal(t, uint64(3), cfg.ConfVersion)
		require.NoError(t, db.Close())
	}
}
