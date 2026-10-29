package wkdb

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func newMetadataBatchTestDB(t *testing.T, fs vfs.FS, disabledWALShard ...int) (*wukongDB, func()) {
	t.Helper()
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	wk := NewWukongDB(NewOptions(WithShardNum(2), WithNodeId(1))).(*wukongDB)
	for i := 0; i < 2; i++ {
		opts := &pebble.Options{FS: fs, MemTableSize: 1 << 20}
		if len(disabledWALShard) > 0 && disabledWALShard[0] == i {
			opts.DisableWAL = true
		}
		db, err := pebble.Open(fmt.Sprintf("shard%d", i), opts)
		require.NoError(t, err)
		wk.dbs = append(wk.dbs, db)
		batch := NewBatchDB(i, db)
		batch.Start()
		wk.wkdbs = append(wk.wkdbs, batch)
	}
	parent, err := fs.OpenDir("")
	require.NoError(t, err)
	require.NoError(t, parent.Sync())
	require.NoError(t, parent.Close())
	return wk, func() {
		for _, batch := range wk.wkdbs {
			batch.Stop()
		}
		for _, db := range wk.dbs {
			_ = db.Close()
		}
	}
}

func TestUpdateChannelInfoBatchFieldsIndexesAndDurability(t *testing.T) {
	fs := vfs.NewStrictMem()
	db, closeDB := newMetadataBatchTestDB(t, fs)
	defer func() { closeDB() }()
	created, oldTime, midTime, finalTime := time.Unix(100, 0), time.Unix(200, 0), time.Unix(300, 0), time.Unix(400, 0)
	info := ChannelInfo{ChannelId: "batch-metadata", ChannelType: 2, CreatedAt: &created, UpdatedAt: &oldTime}
	pk, err := db.AddChannel(info)
	require.NoError(t, err)
	require.NoError(t, db.AddSubscribers(info.ChannelId, 2, []Member{{Uid: "existing-member"}}))
	_, err = db.GetChannel(info.ChannelId, 2)
	require.NoError(t, err)
	first := info
	first.Ban, first.Disband, first.UpdatedAt = true, true, &midTime
	last := info
	last.Large, last.SendBan, last.AllowStranger, last.UpdatedAt = true, true, true, &finalTime
	withoutTime := last
	withoutTime.UpdatedAt = nil
	require.NoError(t, db.UpdateChannelInfoBatch([]ChannelInfo{first, last, withoutTime}))
	check := func() {
		got, err := db.GetChannel(info.ChannelId, 2)
		require.NoError(t, err)
		require.Equal(t, created.UnixNano(), got.CreatedAt.UnixNano())
		require.Equal(t, finalTime.UnixNano(), got.UpdatedAt.UnixNano())
		require.False(t, got.Ban || got.Disband)
		require.True(t, got.Large && got.SendBan && got.AllowStranger)
		count, err := db.GetSubscriberCount(info.ChannelId, 2)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		for _, at := range []time.Time{oldTime, midTime} {
			_, closer, err := db.channelDb(info.ChannelId, 2).Get(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.UpdatedAt, uint64(at.UnixNano()), pk))
			if closer != nil {
				_ = closer.Close()
			}
			require.ErrorIs(t, err, pebble.ErrNotFound)
		}
		for _, item := range []struct {
			idx   [2]byte
			value uint64
		}{
			{key.TableChannelInfo.SecondIndex.CreatedAt, uint64(created.UnixNano())},
			{key.TableChannelInfo.SecondIndex.UpdatedAt, uint64(finalTime.UnixNano())},
			{key.TableChannelInfo.SecondIndex.Ban, 0},
		} {
			_, closer, err := db.channelDb(info.ChannelId, 2).Get(key.NewChannelInfoSecondIndexKey(item.idx, item.value, pk))
			require.NoError(t, err)
			require.NoError(t, closer.Close())
		}
	}
	check()
	fs.SetIgnoreSyncs(true)
	closeDB()
	closeDB = func() {}
	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)
	db, closeDB = newMetadataBatchTestDB(t, fs)
	check()
}

type metadataBatchSyncFS struct {
	vfs.FS
	armed   atomic.Bool
	entered [2]chan struct{}
	release [2]chan struct{}
}
type metadataBatchSyncFile struct {
	vfs.File
	fs    *metadataBatchSyncFS
	shard int
}

func (fs *metadataBatchSyncFS) Create(name string) (vfs.File, error) {
	file, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	for shard := 0; shard < 2; shard++ {
		if strings.Contains(name, fmt.Sprintf("shard%d/", shard)) && strings.HasSuffix(name, ".log") {
			return &metadataBatchSyncFile{file, fs, shard}, nil
		}
	}
	return file, nil
}
func (file *metadataBatchSyncFile) Sync() error {
	if file.fs.armed.Load() {
		select {
		case file.fs.entered[file.shard] <- struct{}{}:
		default:
		}
		<-file.fs.release[file.shard]
	}
	return file.File.Sync()
}

// Pebble's syncing wrapper uses SyncData for WAL writes. Intercept both entry
// points so the failure test actually blocks the durability barrier.
func (file *metadataBatchSyncFile) SyncData() error { return file.Sync() }

func TestUpdateChannelInfoBatchWaitsForAllShardsAndPropagatesFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			fs := &metadataBatchSyncFS{FS: vfs.NewStrictMem()}
			for i := 0; i < 2; i++ {
				fs.entered[i] = make(chan struct{}, 1)
				fs.release[i] = make(chan struct{})
			}
			disabled := -1
			if fail {
				disabled = 1
			}
			db, closeDB := newMetadataBatchTestDB(t, fs, disabled)
			defer closeDB()
			defer func() {
				fs.armed.Store(false)
				for _, release := range fs.release {
					select {
					case <-release:
					default:
						close(release)
					}
				}
			}()
			channels := [2]string{}
			for i := 0; channels[0] == "" || channels[1] == ""; i++ {
				ch := fmt.Sprintf("batch-shard-%d", i)
				channels[db.GetChannelShardIndex(ch, 2)] = ch
			}
			var updates []ChannelInfo
			for _, ch := range channels {
				info := ChannelInfo{ChannelId: ch, ChannelType: 2}
				var err error
				if fail && db.GetChannelShardIndex(ch, 2) == 1 {
					// A deliberately WAL-disabled test shard rejects synchronous
					// commits with an error, without Pebble's fatal-I/O exit path.
					pk, e := db.getChannelPrimaryKey(ch, 2)
					require.NoError(t, e)
					batch := db.channelDb(ch, 2).NewBatch()
					require.NoError(t, db.writeChannelInfo(pk, info, batch))
					err = batch.Commit(pebble.NoSync)
					_ = batch.Close()
				} else {
					_, err = db.AddChannel(info)
				}
				require.NoError(t, err)
				_, err = db.GetChannel(ch, 2)
				require.NoError(t, err)
				updates = append(updates, ChannelInfo{ChannelId: ch, ChannelType: 2, Large: true})
			}
			fs.armed.Store(true)
			done := make(chan error, 1)
			go func() { done <- db.UpdateChannelInfoBatch(updates) }()
			for shard, entered := range fs.entered {
				if fail && shard == 1 {
					continue
				}
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("shard commit did not start")
				}
			}
			close(fs.release[1]) // Includes the failed shard; the other shard must still be awaited.
			select {
			case err := <-done:
				t.Fatalf("returned before remaining shard synced: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			close(fs.release[0])
			err := <-done
			if fail {
				require.ErrorContains(t, err, "WAL disabled")
			} else {
				require.NoError(t, err)
			}
			got, err := db.GetChannel(channels[0], 2)
			require.NoError(t, err)
			require.True(t, got.Large, "successfully committed shard must invalidate its old cached value")
		})
	}
}
