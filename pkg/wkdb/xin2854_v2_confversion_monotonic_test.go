package wkdb_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func openTestWKDBForConfVersion(t *testing.T) wkdb.DB {
	t.Helper()
	dir := t.TempDir()
	db := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(dir),
		wkdb.WithNodeId(1001),
		wkdb.WithShardNum(4),
	))
	require.NoError(t, db.Open())
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

// TestXIN2854_V2_Defect1_OlderConfVersionWriteFinishesLast_PebbleAndCache reproduces
// TC-BASE-003 / TC-S1-001 / TC-P1-004 Sub-A: when an older ConfVersion (e.g. 418 with
// learners=[1002], migrate_to=1002) finishes saving AFTER a newer ConfVersion (e.g. 421
// with replicas=[1001,1002], learners=[]), neither Pebble nor ChannelClusterConfigCache
// may regress below ConfVersion 421.
func TestXIN2854_V2_Defect1_OlderConfVersionWriteFinishesLast_PebbleAndCache(t *testing.T) {
	db := openTestWKDBForConfVersion(t)

	now := time.Now()
	newer421 := wkdb.ChannelClusterConfig{
		ChannelId:       "grp_0063",
		ChannelType:     2,
		ReplicaMaxCount: 3,
		Replicas:        []uint64{1001, 1002},
		Learners:        nil,
		LeaderId:        1001,
		Term:            3,
		MigrateFrom:     0,
		MigrateTo:       0,
		ConfVersion:     421,
		CreatedAt:       &now,
		UpdatedAt:       &now,
	}
	older418 := wkdb.ChannelClusterConfig{
		ChannelId:       "grp_0063",
		ChannelType:     2,
		ReplicaMaxCount: 3,
		Replicas:        []uint64{1001},
		Learners:        []uint64{1002},
		LeaderId:        1001,
		Term:            3,
		MigrateFrom:     1002,
		MigrateTo:       1002,
		ConfVersion:     418,
		CreatedAt:       &now,
		UpdatedAt:       &now,
	}

	// 1. Newer batch commits first, then delayed older batchSaveChannelClusterConfigs arrives last:
	require.NoError(t, db.SaveChannelClusterConfigs([]wkdb.ChannelClusterConfig{newer421}))
	require.NoError(t, db.SaveChannelClusterConfigs([]wkdb.ChannelClusterConfig{older418}))

	gotCached, err := db.GetChannelClusterConfig("grp_0063", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(421), gotCached.ConfVersion, "cached config must not regress from 421 to 418")
	require.Equal(t, []uint64{1001, 1002}, gotCached.Replicas)
	require.Empty(t, gotCached.Learners)
	require.Zero(t, gotCached.MigrateTo)

	ver, err := db.GetChannelClusterConfigVersion("grp_0063", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(421), ver, "Pebble ConfVersion column must not regress from 421 to 418")

	// Scan directly from Pebble iterator (bypassing cache) to prove durable storage was not downgraded:
	allCfgs, err := db.GetChannelClusterConfigs(0, 100)
	require.NoError(t, err)
	require.Len(t, allCfgs, 1)
	require.Equal(t, uint64(421), allCfgs[0].ConfVersion, "Pebble scanned row must remain at ConfVersion 421")
	require.Empty(t, allCfgs[0].Learners)

	// 2. Singular SaveChannelClusterConfig with an even older version (415) or unversioned (0) must also not downgrade 421:
	older415 := older418
	older415.ConfVersion = 415
	require.NoError(t, db.SaveChannelClusterConfig(older415))
	gotAfterSingular, err := db.GetChannelClusterConfig("grp_0063", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(421), gotAfterSingular.ConfVersion)

	// 3. Intra-batch out-of-order entries [425, 422] for the same channel must coalesce to max ConfVersion 425:
	newer425 := newer421
	newer425.Replicas = []uint64{1001, 1002, 1003}
	newer425.ConfVersion = 425
	older422 := newer421
	older422.Learners = []uint64{1003}
	older422.MigrateFrom = 1003
	older422.MigrateTo = 1003
	older422.ConfVersion = 422

	require.NoError(t, db.SaveChannelClusterConfigs([]wkdb.ChannelClusterConfig{newer425, older422}))
	got425, err := db.GetChannelClusterConfig("grp_0063", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(425), got425.ConfVersion)
	require.Equal(t, []uint64{1001, 1002, 1003}, got425.Replicas)
	require.Empty(t, got425.Learners)

	// 4. High-contention concurrent writers + readers across multiple channels must converge to max ConfVersion:
	var wg sync.WaitGroup
	for chIdx := 0; chIdx < 8; chIdx++ {
		chID := fmt.Sprintf("concurrent_grp_%02d", chIdx)
		for v := uint64(1); v <= 20; v++ {
			wg.Add(2)
			ver := v
			go func() {
				defer wg.Done()
				cfg := wkdb.ChannelClusterConfig{
					ChannelId:       chID,
					ChannelType:     2,
					ReplicaMaxCount: 3,
					Replicas:        []uint64{1001, 1002, 1003},
					LeaderId:        1001,
					Term:            uint32(ver),
					ConfVersion:     ver,
				}
				if ver < 20 {
					cfg.Learners = []uint64{1003}
					cfg.Replicas = []uint64{1001, 1002}
				}
				if ver%2 == 0 {
					_ = db.SaveChannelClusterConfigs([]wkdb.ChannelClusterConfig{cfg})
				} else {
					_ = db.SaveChannelClusterConfig(cfg)
				}
			}()
			go func() {
				defer wg.Done()
				_, _ = db.GetChannelClusterConfig(chID, 2)
			}()
		}
	}
	wg.Wait()

	for chIdx := 0; chIdx < 8; chIdx++ {
		chID := fmt.Sprintf("concurrent_grp_%02d", chIdx)
		got, err := db.GetChannelClusterConfig(chID, 2)
		require.NoError(t, err)
		require.Equal(t, uint64(20), got.ConfVersion, "channel %s must converge to max ConfVersion 20", chID)
		require.Empty(t, got.Learners)
	}
}
