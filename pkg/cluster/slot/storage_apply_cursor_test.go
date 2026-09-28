package slot

import (
	"errors"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestReplaySafeApplyCursorMayLagAfterPowerLoss(t *testing.T) {
	// Use the real business state machine and its durable replay watermark.
	// Only the separate Raft storage loses its unsynced writes in this test;
	// wkdb's strict-filesystem tests cover loss of its own optional markers.
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(2), wkdb.WithMemTableSize(1<<20)))
	require.NoError(t, db.Open())
	defer func() { require.NoError(t, db.Close()) }()
	stateMachine := store.New(store.NewOptions(store.WithDB(db)))
	defer stateMachine.Stop()
	fs := vfs.NewStrictMem()
	open := func() (*PebbleShardLogStorage, func()) {
		logDB, err := pebble.Open("raft", &pebble.Options{FS: fs})
		require.NoError(t, err)
		parent, err := fs.OpenDir("")
		require.NoError(t, err)
		require.NoError(t, parent.Sync())
		require.NoError(t, parent.Close())
		batchDB := wkdb.NewBatchDB(0, logDB)
		batchDB.Start()
		storage := &PebbleShardLogStorage{
			dbs: []*pebble.DB{logDB}, batchDbs: []*wkdb.BatchDB{batchDB},
			shardNum: 1, sync: pebble.Sync, noSync: pebble.NoSync,
			s: &Server{opts: NewOptions(WithReplaySafeOnApply(stateMachine.ApplySlotLogs))},
		}
		var once sync.Once
		closeStorage := func() { once.Do(func() { batchDB.Stop(); require.NoError(t, logDB.Close()) }) }
		t.Cleanup(closeStorage)
		return storage, closeStorage
	}
	storage, closeStorage := open()
	command := func(index uint64, tp store.CMDType, data []byte) types.Log {
		encoded, err := store.NewCMD(tp, data).Marshal()
		require.NoError(t, err)
		return types.Log{Index: index, Term: 1, Data: encoded}
	}
	logs := []types.Log{
		command(1, store.CMDAddDenylist, store.EncodeMembers("group", 2, []wkdb.Member{{Uid: "old"}})),
		command(2, store.CMDRemoveDenylist, store.EncodeChannelUids("group", 2, []string{"old"})),
		command(3, store.CMDAddDenylist, store.EncodeMembers("group", 2, []wkdb.Member{{Uid: "new"}})),
	}
	require.NoError(t, storage.AppendLogs("1", logs, nil))
	require.NoError(t, storage.Apply("1", logs))
	index, err := storage.AppliedIndex("1")
	require.NoError(t, err)
	require.EqualValues(t, 3, index, "truncation must see the current in-process prefix")
	legacy, err := db.SlotAppliedIndex(1)
	require.NoError(t, err)
	require.EqualValues(t, 3, legacy, "business replay protection must precede ACK")
	fs.SetIgnoreSyncs(true)
	closeStorage()
	fs.ResetToSyncedState()
	fs.SetIgnoreSyncs(false)
	storage, closeStorage = open()
	defer closeStorage()
	state, err := storage.GetState("1")
	require.NoError(t, err)
	require.EqualValues(t, 3, state.LastLogIndex, "acknowledged Raft log remains durable")
	require.Zero(t, state.AppliedIndex, "must actually lose the optional applied cursor")
	stateMachine = store.New(store.NewOptions(store.WithDB(db)))
	defer stateMachine.Stop()
	storage.s.opts.OnApply = stateMachine.ApplySlotLogs
	replayed, err := storage.GetLogs("1", 1, 4, 0)
	require.NoError(t, err)
	require.Len(t, replayed, 3)
	// Even an old prefix must not resurrect a superseded business mutation.
	require.NoError(t, storage.Apply("1", replayed[:1]))
	require.NoError(t, storage.Apply("1", replayed))
	members, err := db.GetDenylist("group", 2)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "new", members[0].Uid)
}

func TestReplaySafeApplyFailureDoesNotAdvanceCursor(t *testing.T) {
	storage, batchDB := newStoppedBatchStorage(t)
	batchDB.Start()
	failure := errors.New("business state not durable")
	storage.s = &Server{opts: NewOptions(WithReplaySafeOnApply(func(uint32, []types.Log) error { return failure }))}
	logs := []types.Log{{Index: 1, Term: 1, Data: []byte("command")}}
	require.NoError(t, storage.AppendLogs("1", logs, nil))
	require.ErrorIs(t, storage.Apply("1", logs), failure)
	index, err := storage.AppliedIndex("1")
	require.NoError(t, err)
	require.Zero(t, index)
}

func TestOrdinaryApplyCallbackRestoresSynchronousCursor(t *testing.T) {
	callback := func(uint32, []types.Log) error { return nil }
	opts := NewOptions(WithReplaySafeOnApply(callback), WithOnApply(callback))
	require.False(t, opts.ReplaySafeApply, "replacing a callback must not inherit its replay promise")
}
