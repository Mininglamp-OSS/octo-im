package slot

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
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

// Each entry changes a different channel, so a later correct entry cannot hide
// a divergent entry which was incorrectly applied before alignment finished.
func TestSlotMultiTermPrefixRecovery(t *testing.T) {
	if trace.GlobalTrace == nil {
		trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	}
	for _, learner := range []bool{false, true} {
		for _, history := range []string{"missing-term", "empty-prefix", "empty-leader"} {
			t.Run(fmt.Sprintf("%s/learner=%v", history, learner), func(t *testing.T) {
				tr := &prefixRecoveryTransport{peers: map[uint64]*Server{}}
				tr.holdSync.Store(true)
				var servers []*Server
				var dbs []wkdb.DB
				var stateMachines []*store.Store
				var dbPaths []string
				stopped := false
				stop := func() {
					if stopped {
						return
					}
					stopped = true
					for _, s := range servers {
						s.Stop()
					}
					for _, s := range stateMachines {
						s.Stop()
					}
					for _, db := range dbs {
						require.NoError(t, db.Close())
					}
				}
				t.Cleanup(stop)
				makeLog := func(index uint64, logTerm uint32, channelLeader uint64) types.Log {
					cfg := wkdb.ChannelClusterConfig{ChannelId: fmt.Sprintf("prefix-%d", index), ChannelType: 2,
						LeaderId: channelLeader, Term: logTerm, ReplicaMaxCount: 3, Replicas: []uint64{1, 2, 3}}
					data, err := cfg.Marshal()
					require.NoError(t, err)
					data, err = store.EncodeCMDChannelClusterConfigSave(cfg.ChannelId, cfg.ChannelType, data)
					require.NoError(t, err)
					data, err = store.NewCMD(store.CMDChannelClusterConfigSave, data).Marshal()
					require.NoError(t, err)
					return types.Log{Id: index, Index: index, Term: logTerm, Data: data}
				}
				var canonical, divergent []types.Log
				var common uint64
				switch history {
				case "missing-term":
					common = 2
					for i := uint64(1); i <= 8; i++ {
						if i <= 6 {
							term := uint32(1)
							if i == 6 {
								term = 3
							}
							canonical = append(canonical, makeLog(i, term, 1))
						}
						term, leader := uint32(2), uint64(2)
						if i <= common {
							term, leader = 1, 1
						}
						divergent = append(divergent, makeLog(i, term, leader))
					}
				case "empty-prefix", "empty-leader":
					for i := uint64(1); i <= 4; i++ {
						if history == "empty-prefix" && i <= 3 {
							canonical = append(canonical, makeLog(i, 3, 1))
						}
						divergent = append(divergent, makeLog(i, 1, 2))
					}
				}
				for _, id := range []uint64{1, 2} {
					dir := t.TempDir()
					path := filepath.Join(dir, "db")
					dbPaths = append(dbPaths, path)
					db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(path), wkdb.WithShardNum(1), wkdb.WithNodeId(id)))
					require.NoError(t, db.Open())
					dbs = append(dbs, db)
					sm := store.New(store.NewOptions(store.WithDB(db)))
					require.NoError(t, sm.Start())
					stateMachines = append(stateMachines, sm)
					s := NewServer(NewOptions(WithNodeId(id), WithDataDir(dir), WithSlotDbShardNum(1), WithTransport(tr), WithOnApply(sm.ApplySlotLogs)))
					require.NoError(t, s.storage.Open())
					servers = append(servers, s)
					tr.peers[id] = s
					logs, applied := canonical, uint64(len(canonical))
					if id == 2 {
						logs, applied = divergent, common
					}
					var previous uint32
					for _, log := range logs {
						var start *types.TermStartIndexInfo
						if log.Term != previous {
							start = &types.TermStartIndexInfo{Term: log.Term, Index: log.Index}
						}
						require.NoError(t, s.storage.AppendLogs("59", []types.Log{log}, start))
						previous = log.Term
					}
					if applied > 0 {
						require.NoError(t, s.storage.Apply("59", logs[:applied]))
					}
				}
				assignment := &nodetypes.Slot{Id: 59, Leader: 1, Term: 3, Replicas: []uint64{1, 2, 3}}
				if learner {
					assignment.Replicas, assignment.Learners = []uint64{1, 3}, []uint64{2}
				}
				for _, s := range servers {
					require.NoError(t, s.raftGroup.Start())
					s.OnConfigChange(&nodetypes.Config{Version: 3, Slots: []*nodetypes.Slot{assignment.Clone()}})
				}
				tr.holdSync.Store(false)
				require.Eventually(t, func() bool {
					state, err := servers[1].storage.GetState("59")
					return err == nil && state.LastLogIndex == uint64(len(canonical)) && state.AppliedIndex == uint64(len(canonical))
				}, 8*time.Second, 10*time.Millisecond, "alignment must converge without committing divergent history")
				if len(canonical) > 0 {
					actual, err := servers[1].storage.GetLogs("59", 1, uint64(len(canonical))+1, 0)
					require.NoError(t, err)
					require.Len(t, actual, len(canonical))
					for i := range canonical {
						if canonical[i].Term != actual[i].Term || !bytes.Equal(canonical[i].Data, actual[i].Data) {
							t.Errorf("divergent stored entry at index %d: canonical term %d, actual term %d", i+1, canonical[i].Term, actual[i].Term)
						}
					}
				}
				if learner {
					assignment.Replicas, assignment.Learners = []uint64{1, 2, 3}, nil
					for _, s := range servers {
						s.OnConfigChange(&nodetypes.Config{Version: 4, Slots: []*nodetypes.Slot{assignment.Clone()}})
					}
				}
				last := uint64(len(canonical)) + 1
				write := makeLog(last, 3, 1)
				canonical = append(canonical, write)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err := servers[0].raftGroup.Do(ctx, "59", func(r raftgroup.IRaft) error {
					return r.Step(r.(*Slot).NewPropose(write.Data))
				})
				require.NoError(t, err, "repaired group must accept another write")
				require.Eventually(t, func() bool { index, err := servers[0].storage.AppliedIndex("59"); return err == nil && index == last }, 3*time.Second, 10*time.Millisecond)
				require.Eventually(t, func() bool { index, err := servers[1].storage.AppliedIndex("59"); return err == nil && index == last }, 3*time.Second, 10*time.Millisecond)
				stop()
				for _, path := range dbPaths {
					db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(path), wkdb.WithShardNum(1)))
					require.NoError(t, db.Open())
					for _, log := range canonical {
						cfg, err := db.GetChannelClusterConfig(fmt.Sprintf("prefix-%d", log.Index), 2)
						require.NoError(t, err)
						require.Equal(t, uint64(1), cfg.LeaderId, "physical metadata at index %d", log.Index)
						require.Equal(t, log.Term, cfg.Term)
						require.Equal(t, log.Index, cfg.ConfVersion)
					}
					require.NoError(t, db.Close())
				}
			})
		}
	}
}
