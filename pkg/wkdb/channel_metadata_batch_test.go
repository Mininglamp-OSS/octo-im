package wkdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestMetadataBatchRetainsHistoryCountsAndFinalIndexes(t *testing.T) {
	strict := vfs.NewStrictMem()
	db, closeDB := openRecoveryPowerLossDB(t, strict)
	defer func() { closeDB() }()
	created, updated, intermediate, final := time.Unix(100, 0), time.Unix(200, 0), time.Unix(300, 0), time.Unix(400, 0)
	info := ChannelInfo{ChannelId: "metadata", ChannelType: 2, CreatedAt: &created, UpdatedAt: &updated}
	pk, err := db.AddChannel(info)
	require.NoError(t, err)
	require.NoError(t, db.AddSubscribers(info.ChannelId, 2, []Member{{Id: 10, Uid: "member"}}))
	_, err = db.GetChannel(info.ChannelId, 2) // prime a potentially stale cache
	require.NoError(t, err)
	info.Ban, info.Disband, info.UpdatedAt = true, true, &intermediate
	last := info
	last.Ban, last.Disband, last.Large, last.SendBan, last.AllowStranger = false, false, true, true, true
	last.UpdatedAt = &final
	nilTime := last
	nilTime.UpdatedAt = nil
	require.NoError(t, db.UpdateChannelInfoBatch([]ChannelInfo{info, last, nilTime}))
	check := func(db *wukongDB) {
		got, err := db.GetChannel(info.ChannelId, 2)
		require.NoError(t, err)
		require.Equal(t, created.UnixNano(), got.CreatedAt.UnixNano())
		require.Equal(t, final.UnixNano(), got.UpdatedAt.UnixNano())
		require.False(t, got.Ban)
		require.False(t, got.Disband)
		require.True(t, got.Large && got.SendBan && got.AllowStranger)
		count, err := db.GetSubscriberCount(info.ChannelId, 2)
		require.NoError(t, err)
		require.Equal(t, 1, count)
		for _, at := range []time.Time{updated, intermediate} {
			_, closer, err := db.channelDb(info.ChannelId, 2).Get(key.NewChannelInfoSecondIndexKey(key.TableChannelInfo.SecondIndex.UpdatedAt, uint64(at.UnixNano()), pk))
			if closer != nil {
				_ = closer.Close()
			}
			require.ErrorIs(t, err, pebble.ErrNotFound, "obsolete update index")
		}
		for _, item := range []struct {
			index [2]byte
			value uint64
		}{
			{key.TableChannelInfo.SecondIndex.CreatedAt, uint64(created.UnixNano())},
			{key.TableChannelInfo.SecondIndex.UpdatedAt, uint64(final.UnixNano())},
			{key.TableChannelInfo.SecondIndex.Ban, 0},
		} {
			_, closer, err := db.channelDb(info.ChannelId, 2).Get(key.NewChannelInfoSecondIndexKey(item.index, item.value, pk))
			require.NoError(t, err)
			require.NoError(t, closer.Close())
		}
	}
	check(db)
	strict.SetIgnoreSyncs(true)
	closeDB()
	closeDB = func() {}
	strict.ResetToSyncedState()
	strict.SetIgnoreSyncs(false)
	db, closeDB = openRecoveryPowerLossDB(t, strict)
	check(db)
}

func TestMetadataBatchPartialDurabilityNeverExposesIntermediateFlags(t *testing.T) {
	for first := 0; first < 2; first++ {
		t.Run(fmt.Sprint(first), func(t *testing.T) {
			strict := vfs.NewStrictMem()
			fs := &recoverySyncProbeFS{FS: strict}
			for i := range fs.entered {
				fs.entered[i] = make(chan struct{}, 1)
				fs.release[i] = make(chan struct{})
				fs.synced[i] = make(chan struct{}, 1)
			}
			db, closeDB := openRecoveryPowerLossDB(t, fs)
			channels := [2]string{}
			for i := 0; channels[0] == "" || channels[1] == ""; i++ {
				name := fmt.Sprintf("metadata-%d", i)
				channels[db.GetChannelShardIndex(name, 2)] = name
			}
			var updates []ChannelInfo
			for _, channel := range channels {
				_, err := db.AddChannel(ChannelInfo{ChannelId: channel, ChannelType: 2})
				require.NoError(t, err)
				updates = append(updates, ChannelInfo{ChannelId: channel, ChannelType: 2, Ban: true})
				updates = append(updates, ChannelInfo{ChannelId: channel, ChannelType: 2, Large: true})
			}
			done := make(chan error, 1)
			fs.armed.Store(true)
			go func() { done <- db.UpdateChannelInfoBatch(updates) }()
			defer func() {
				for _, release := range fs.release {
					select {
					case <-release:
					default:
						close(release)
					}
				}
				fs.armed.Store(false)
				<-done
				closeDB()
			}()
			for i := range fs.entered {
				select {
				case <-fs.entered[i]:
				case <-time.After(time.Second):
					t.Fatalf("shard %d Sync serialized", i)
				}
			}
			close(fs.release[first])
			select {
			case <-fs.synced[first]:
			case <-time.After(time.Second):
				t.Fatal("Sync did not finish")
			}
			select {
			case err := <-done:
				done <- err
				t.Fatalf("ACK before both shards Sync: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			strict.SetIgnoreSyncs(true)
			close(fs.release[1-first])
			err := <-done
			done <- err
			require.NoError(t, err)
			fs.armed.Store(false)
			closeDB()
			closeDB = func() {}
			strict.ResetToSyncedState()
			strict.SetIgnoreSyncs(false)
			reopened, closeReopened := openRecoveryPowerLossDB(t, strict)
			defer closeReopened()
			for shard, channel := range channels {
				got, err := reopened.GetChannel(channel, 2)
				require.NoError(t, err)
				require.False(t, got.Ban, "a partially durable batch must never expose its intermediate value")
				require.Equal(t, shard == first, got.Large, "the test must actually discard one shard")
			}
			require.NoError(t, reopened.UpdateChannelInfoBatch(updates))
			for _, channel := range channels {
				got, err := reopened.GetChannel(channel, 2)
				require.NoError(t, err)
				require.False(t, got.Ban)
				require.True(t, got.Large)
			}
		})
	}
}
