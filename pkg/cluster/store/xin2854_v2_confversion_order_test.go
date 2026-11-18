package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type delayedOlderVersionDB struct {
	wkdb.DB
	newerArrived sync.Map // key: channelId -> chan struct{}
}

func (d *delayedOlderVersionDB) SaveChannelClusterConfigs(cfgs []wkdb.ChannelClusterConfig) error {
	hasOlder := false
	var waitCh chan struct{}
	for _, cfg := range cfgs {
		chAny, _ := d.newerArrived.LoadOrStore(cfg.ChannelId, make(chan struct{}))
		ch := chAny.(chan struct{})
		if cfg.ConfVersion >= 200 {
			defer func(c chan struct{}) {
				select {
				case <-c:
				default:
					close(c)
				}
			}(ch)
		} else if cfg.ConfVersion == 100 {
			hasOlder = true
			waitCh = ch
		}
	}
	// Deterministically force any competing worker holding ConfVersion=100 to finish AFTER ConfVersion=200
	// if Store dispatched same-channel saves to different concurrent workers!
	if hasOlder && waitCh != nil {
		select {
		case <-waitCh:
			time.Sleep(5 * time.Millisecond)
		case <-time.After(30 * time.Millisecond):
		}
	}
	return d.DB.SaveChannelClusterConfigs(cfgs)
}

func encodeClusterConfigSaveLog(t *testing.T, index uint64, cfg wkdb.ChannelClusterConfig) types.Log {
	t.Helper()
	cfgBytes, err := cfg.Marshal()
	require.NoError(t, err)
	cmdData, err := EncodeCMDChannelClusterConfigSave(cfg.ChannelId, cfg.ChannelType, cfgBytes)
	require.NoError(t, err)
	cmd := NewCMD(CMDChannelClusterConfigSave, cmdData)
	raw, err := cmd.Marshal()
	require.NoError(t, err)
	return types.Log{
		Index: index,
		Term:  1,
		Data:  raw,
	}
}

// TestXIN2854_V2_Defect1_StoreLoopSaveConcurrentSameChannelMonotonic reproduces the exact
// 50-worker unkeyed channelCfgCh race in Store.Start() + ApplySlotLogs where consecutive
// CMDChannelClusterConfigSave entries for the same channel get popped by different workers
// and the older ConfVersion overwrites the newer ConfVersion.
func TestXIN2854_V2_Defect1_StoreLoopSaveConcurrentSameChannelMonotonic(t *testing.T) {
	dir := t.TempDir()
	rawDB := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(dir),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(2),
	))
	require.NoError(t, rawDB.Open())
	t.Cleanup(func() { _ = rawDB.Close() })

	wrappedDB := &delayedOlderVersionDB{DB: rawDB}
	s := New(NewOptions(WithNodeId(1001), WithDB(wrappedDB)))
	require.NoError(t, s.Start())
	t.Cleanup(func() { s.Stop() })

	for i := 0; i < 16; i++ {
		chID := fmt.Sprintf("grp_store_race_%02d", i)
		older := wkdb.ChannelClusterConfig{
			ChannelId:       chID,
			ChannelType:     2,
			ReplicaMaxCount: 3,
			Replicas:        []uint64{1001, 1002},
			Learners:        []uint64{1003},
			LeaderId:        1001,
			Term:            2,
			MigrateFrom:     1003,
			MigrateTo:       1003,
		}
		newer := wkdb.ChannelClusterConfig{
			ChannelId:       chID,
			ChannelType:     2,
			ReplicaMaxCount: 3,
			Replicas:        []uint64{1001, 1002, 1003},
			Learners:        nil,
			LeaderId:        1001,
			Term:            2,
			MigrateFrom:     0,
			MigrateTo:       0,
		}
		// Submit via two separate ApplySlotLogs calls (<10 logs path -> handleChannelClusterConfigSave)
		// in separate goroutines separated by 100us to trigger concurrent worker pickup on unkeyed channelCfgCh:
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			require.NoError(t, s.ApplySlotLogs(1, []types.Log{encodeClusterConfigSaveLog(t, 100, older)}))
		}()
		time.Sleep(100 * time.Microsecond)
		go func() {
			defer wg.Done()
			require.NoError(t, s.ApplySlotLogs(1, []types.Log{encodeClusterConfigSaveLog(t, 200, newer)}))
		}()
		wg.Wait()

		got, err := rawDB.GetChannelClusterConfig(chID, 2)
		require.NoError(t, err)
		require.Equal(t, uint64(200), got.ConfVersion, "channel %s must not regress to older ConfVersion 100", chID)
		require.Equal(t, []uint64{1001, 1002, 1003}, got.Replicas)
		require.Empty(t, got.Learners)
	}
}
