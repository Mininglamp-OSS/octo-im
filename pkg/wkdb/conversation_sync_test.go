package wkdb

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type recoverySyncProbeFS struct {
	vfs.FS
	armed   atomic.Bool
	entered [2]chan struct{}
	release [2]chan struct{}
	synced  [2]chan struct{}
}

type recoverySyncProbeFile struct {
	vfs.File
	fs    *recoverySyncProbeFS
	shard int
}

func (f *recoverySyncProbeFile) Sync() error {
	if f.fs.armed.Load() {
		select {
		case f.fs.entered[f.shard] <- struct{}{}:
		default:
		}
		<-f.fs.release[f.shard]
	}
	err := f.File.Sync()
	if f.fs.armed.Load() && f.fs.synced[f.shard] != nil {
		select {
		case f.fs.synced[f.shard] <- struct{}{}:
		default:
		}
	}
	return err
}

// Capture a power-loss image after only one side of a cross-shard update has
// synced. No target acknowledgement is allowed at that point. The committed
// Raft entry is replayed after reopening, as it would be with no applied ACK.
func TestConversationCrossShardPartialSyncReplay(t *testing.T) {
	for _, scenario := range []struct{ relationFirst, adoption bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		relationFirst := scenario.relationFirst
		name := "user-first"
		if relationFirst {
			name = "relation-first"
		}
		if scenario.adoption {
			name += "-adoption"
		}
		t.Run(name, func(t *testing.T) {
			strict := vfs.NewStrictMem()
			fs := &recoverySyncProbeFS{FS: strict}
			for i := range fs.entered {
				fs.entered[i] = make(chan struct{}, 1)
				fs.release[i] = make(chan struct{})
				fs.synced[i] = make(chan struct{}, 1)
			}
			db, closeDB := openRecoveryPowerLossDB(t, fs)
			uid, channel := "user", "group"
			for db.shardId(uid) == db.GetChannelShardIndex(channel, 2) {
				channel += "x"
			}
			effect := ConversationEffect{UID: uid, ChannelID: channel, ChannelType: 2,
				Version: 1, ConversationID: 7, CreatedAt: time.Now().UnixNano()}
			if scenario.adoption {
				at := time.Now().Add(-time.Hour)
				require.NoError(t, db.AddOrUpdateConversations([]Conversation{{Id: 3, Uid: uid, ChannelId: channel, ChannelType: 2, Type: ConversationTypeChat, ReadToMsgSeq: 4, CreatedAt: &at, UpdatedAt: &at}}))
				require.NoError(t, db.UpdateConversationDeletedAtMsgSeq(uid, channel, 2, 20))
				// Keep the pre-crash reverse relation absent to observe which
				// shard really became durable in this partial-prefix test.
				require.NoError(t, db.deleteConversationLocalUserRelation(channel, 2, uid))
				effect.PreserveExisting = true
			}
			done := make(chan error, 1)
			fs.armed.Store(true)
			go func() { done <- db.ApplyConversationEffects([]ConversationEffect{effect}) }()
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
					t.Fatalf("shard %d sync was serialized behind the other shard", i)
				}
			}
			first := db.shardId(uid)
			if relationFirst {
				first = 1 - first
			}
			close(fs.release[first])
			select {
			case <-fs.synced[first]:
			case <-time.After(time.Second):
				t.Fatal("first shard failed to sync")
			}
			select {
			case err := <-done:
				done <- err
				t.Fatalf("target ACK before both shards were durable: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			// Freeze the durable image at the simulated crash. Allow the old
			// goroutines to exit, but discard all their subsequent writes/syncs.
			strict.SetIgnoreSyncs(true)
			close(fs.release[1-first])
			err := <-done
			done <- err // cleanup always joins the old operation
			require.NoError(t, err)
			fs.armed.Store(false)
			closeDB()
			closeDB = func() {}
			strict.ResetToSyncedState()
			strict.SetIgnoreSyncs(false)
			reopened, closeReopened := openRecoveryPowerLossDB(t, strict)
			defer closeReopened()
			_, found, err := reopened.ConversationLifecycle(uid, channel, 2)
			require.NoError(t, err)
			require.Equal(t, !relationFirst, found, "must really capture a partial durable prefix")
			relationKey := key.NewConversationLocalUserKey(channel, 2, uid)
			_, closer, err := reopened.channelDb(channel, 2).Get(relationKey)
			if relationFirst {
				require.NoError(t, err)
				require.NoError(t, closer.Close())
			} else {
				require.ErrorIs(t, err, pebble.ErrNotFound)
			}
			require.NoError(t, reopened.ApplyConversationEffects([]ConversationEffect{effect}))
			_, closer, err = reopened.channelDb(channel, 2).Get(relationKey)
			require.NoError(t, err)
			require.NoError(t, closer.Close())
			conversation, err := reopened.GetConversation(uid, channel, 2)
			require.NoError(t, err)
			require.Equal(t, uint64(7), conversation.Id)
			if scenario.adoption {
				require.EqualValues(t, 20, conversation.DeletedAtMsgSeq)
				require.EqualValues(t, 4, conversation.ReadToMsgSeq)
			}
			lifecycle, found, err := reopened.ConversationLifecycle(uid, channel, 2)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, lifecycle.RelationDone)
			// A later removal still fences an old replay after this recovery.
			effect.Version, effect.Deleted = 2, true
			require.NoError(t, reopened.ApplyConversationEffects([]ConversationEffect{effect}))
			effect.Version, effect.Deleted = 1, false
			require.NoError(t, reopened.ApplyConversationEffects([]ConversationEffect{effect}))
			_, err = reopened.GetConversation(uid, channel, 2)
			require.ErrorIs(t, err, ErrNotFound)
			_, _, err = reopened.channelDb(channel, 2).Get(relationKey)
			require.ErrorIs(t, err, pebble.ErrNotFound)
		})
	}
}

func (f *recoverySyncProbeFile) SyncData() error {
	return f.Sync()
}

func (fs *recoverySyncProbeFS) Create(name string) (vfs.File, error) {
	f, err := fs.FS.Create(name)
	if err != nil || !strings.HasSuffix(name, ".log") {
		return f, err
	}
	shard := 0
	if strings.HasPrefix(name, "shard1/") {
		shard = 1
	}
	return &recoverySyncProbeFile{File: f, fs: fs, shard: shard}, nil
}

func TestConversationShardSyncsOverlapButCompletionWaitsForAll(t *testing.T) {
	fs := &recoverySyncProbeFS{FS: vfs.NewMem()}
	for i := range fs.entered {
		fs.entered[i] = make(chan struct{}, 1)
		fs.release[i] = make(chan struct{})
	}
	db, closeDB := openRecoveryPowerLossDB(t, fs)
	batches := map[uint32]*pebble.Batch{0: db.dbs[0].NewBatch(), 1: db.dbs[1].NewBatch()}
	for _, b := range batches {
		require.NoError(t, b.Set([]byte("key"), []byte("durable"), pebble.NoSync))
	}
	done := make(chan error, 1)
	fs.armed.Store(true)
	go func() { done <- commitRecoveryBatches(batches, pebble.Sync, nil) }()
	defer func() {
		for _, release := range fs.release {
			select {
			case <-release:
			default:
				close(release)
			}
		}
		// Always await WAL completion before closing either batch, including
		// when the regression assertion fails against the sequential version.
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("commit did not finish after releasing the test filesystem")
		}
		closeRecoveryBatches(batches)
		closeDB()
	}()
	for i := range fs.entered {
		select {
		case <-fs.entered[i]:
		case <-time.After(time.Second):
			t.Fatalf("shard %d could not start its WAL sync while the other shard was blocked", i)
		}
	}
	close(fs.release[1])
	select {
	case err := <-done:
		// Leave a value for cleanup after checking the premature completion.
		done <- err
		t.Fatalf("completion overtook the still-blocked shard0 sync: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(fs.release[0])
	select {
	case err := <-done:
		done <- err
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("completion did not follow both durable syncs")
	}
}

// A failure in one shard must not let the caller close another shard's batch
// while its durable write is still in flight, or skip that shard's cache flush.
func TestConversationShardFailureWaitsForSuccessfulSibling(t *testing.T) {
	fs := &recoverySyncProbeFS{FS: vfs.NewMem()}
	for i := range fs.entered {
		fs.entered[i] = make(chan struct{}, 1)
		fs.release[i] = make(chan struct{})
	}
	first, err := pebble.Open("shard0", &pebble.Options{FS: fs})
	require.NoError(t, err)
	second, err := pebble.Open("shard1", &pebble.Options{FS: fs})
	require.NoError(t, err)
	require.NoError(t, second.Close())
	second, err = pebble.Open("shard1", &pebble.Options{FS: fs, ReadOnly: true})
	require.NoError(t, err)
	batches := map[uint32]*pebble.Batch{0: first.NewBatch(), 1: second.NewBatch()}
	for _, batch := range batches {
		require.NoError(t, batch.Set([]byte("key"), []byte("value"), pebble.NoSync))
	}
	var invalidated []uint32
	done := make(chan error, 1)
	fs.armed.Store(true)
	go func() {
		done <- commitRecoveryBatches(batches, pebble.Sync, func(shard uint32) {
			invalidated = append(invalidated, shard)
		})
	}()
	defer func() {
		select {
		case <-fs.release[0]:
		default:
			close(fs.release[0])
		}
		close(fs.release[1])
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("commit did not finish after releasing filesystem")
		}
		closeRecoveryBatches(batches)
		require.NoError(t, first.Close())
		require.NoError(t, second.Close())
	}()
	select {
	case <-fs.entered[0]:
	case <-time.After(time.Second):
		t.Fatal("successful sibling did not reach sync")
	}
	select {
	case err := <-done:
		done <- err
		t.Fatalf("failure returned while sibling sync was still blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(fs.release[0])
	select {
	case err := <-done:
		done <- err
		require.ErrorIs(t, err, pebble.ErrReadOnly)
		require.Equal(t, []uint32{0}, invalidated)
	case <-time.After(time.Second):
		t.Fatal("commit did not report sibling failure")
	}
}
