package slot

import (
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestRaftMetadataWritesUseGroupCommit(t *testing.T) {
	tests := []struct {
		name  string
		write func(*PebbleShardLogStorage) error
	}{
		{
			name: "append logs",
			write: func(storage *PebbleShardLogStorage) error {
				return storage.AppendLogs("1", []types.Log{{Index: 1, Term: 1, Data: []byte("command")}}, nil)
			},
		},
		{
			name: "set applied index",
			write: func(storage *PebbleShardLogStorage) error {
				return storage.SetAppliedIndex("1", 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage, batchDB := newStoppedBatchStorage(t)
			errCh := make(chan error, 1)
			go func() {
				errCh <- tt.write(storage)
			}()

			select {
			case err := <-errCh:
				t.Fatalf("write completed before the group-commit worker started: %v", err)
			case <-time.After(100 * time.Millisecond):
			}

			batchDB.Start()
			select {
			case err := <-errCh:
				if err != nil {
					t.Fatalf("write error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("write did not complete after the group-commit worker started")
			}
		})
	}
}

func newStoppedBatchStorage(t *testing.T) (*PebbleShardLogStorage, *wkdb.BatchDB) {
	t.Helper()

	db, err := pebble.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("pebble.Open() error = %v", err)
	}
	batchDB := wkdb.NewBatchDB(0, db)
	t.Cleanup(func() {
		batchDB.Stop()
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return &PebbleShardLogStorage{
		dbs:      []*pebble.DB{db},
		batchDbs: []*wkdb.BatchDB{batchDB},
		shardNum: 1,
		sync:     pebble.Sync,
		noSync:   pebble.NoSync,
	}, batchDB
}

func TestApplyDoesNotSerializeDifferentSlotsInSameDBShard(t *testing.T) {
	t.Parallel()

	storage, entered, release := newBlockingApplyStorage(t)

	errCh := make(chan error, 2)
	go func() {
		errCh <- storage.Apply("1", []types.Log{{Index: 1}})
	}()

	waitForApply(t, entered, "first slot")

	go func() {
		errCh <- storage.Apply("2", []types.Log{{Index: 1}})
	}()

	waitForApply(t, entered, "second slot")
	close(release)

	for range 2 {
		if err := <-errCh; !errors.Is(err, errStopBeforeAppliedIndex) {
			t.Fatalf("Apply() error = %v, want %v", err, errStopBeforeAppliedIndex)
		}
	}
}

func TestApplySerializesSameSlot(t *testing.T) {
	t.Parallel()

	storage, entered, release := newBlockingApplyStorage(t)

	errCh := make(chan error, 2)
	go func() {
		errCh <- storage.Apply("1", []types.Log{{Index: 1}})
	}()

	waitForApply(t, entered, "first apply")

	go func() {
		errCh <- storage.Apply("1", []types.Log{{Index: 2}})
	}()

	select {
	case <-entered:
		t.Fatal("second Apply() entered OnApply while the same slot was still applying")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	waitForApply(t, entered, "second apply")

	for range 2 {
		if err := <-errCh; !errors.Is(err, errStopBeforeAppliedIndex) {
			t.Fatalf("Apply() error = %v, want %v", err, errStopBeforeAppliedIndex)
		}
	}
}

func TestAppendCanProgressWhileSameSlotApplyIsBlocked(t *testing.T) {
	storage, entered, release := newBlockingApplyStorage(t)
	// Ensure a failed assertion also releases the blocked callback before close.
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	require.NoError(t, storage.AppendLogs("1", []types.Log{{Index: 1, Term: 1}}, nil))
	applied := make(chan error, 1)
	go func() { applied <- storage.Apply("1", []types.Log{{Index: 1, Term: 1}}) }()
	waitForApply(t, entered, "blocked apply")
	appended := make(chan error, 1)
	go func() { appended <- storage.AppendLogs("1", []types.Log{{Index: 2, Term: 1}}, nil) }()
	select {
	case err := <-appended:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("business Apply prevented the next log from becoming durable")
	}
	index, err := storage.AppliedIndex("1")
	require.NoError(t, err)
	require.Zero(t, index, "append must not acknowledge unfinished business Apply")
	last, err := storage.LastIndex("1")
	require.NoError(t, err)
	require.EqualValues(t, 2, last)
	close(release)
	require.ErrorIs(t, <-applied, errStopBeforeAppliedIndex)
}

func TestTruncateWaitsForApplyAndKeepsItsDurablePrefix(t *testing.T) {
	storage, entered, release := newBlockingApplyStorage(t)
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	applyEntered := make(chan uint32, 1)
	entered = applyEntered
	storage.s.opts.OnApply = func(slot uint32, _ []types.Log) error {
		applyEntered <- slot
		<-release
		return nil
	}
	require.NoError(t, storage.AppendLogs("1", []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}, nil))
	applied := make(chan error, 1)
	go func() { applied <- storage.Apply("1", []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}}) }()
	waitForApply(t, entered, "blocked apply")
	truncated := make(chan error, 1)
	go func() { truncated <- storage.TruncateLogTo("1", 1) }()
	select {
	case err := <-truncated:
		t.Fatalf("truncation overtook pending Apply: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-applied)
	require.NoError(t, <-truncated)
	index, err := storage.AppliedIndex("1")
	require.NoError(t, err)
	require.EqualValues(t, 2, index)
	last, err := storage.LastIndex("1")
	require.NoError(t, err)
	require.EqualValues(t, 3, last, "cannot truncate below the now-durable applied prefix")
	require.NoError(t, storage.TruncateLogTo("1", 2))
	last, err = storage.LastIndex("1")
	require.NoError(t, err)
	require.EqualValues(t, 2, last)
}

var errStopBeforeAppliedIndex = errors.New("stop before writing applied index")

func newBlockingApplyStorage(t *testing.T) (*PebbleShardLogStorage, <-chan uint32, chan struct{}) {
	t.Helper()

	entered := make(chan uint32, 2)
	release := make(chan struct{})
	server := &Server{opts: &Options{
		OnApply: func(slotID uint32, _ []types.Log) error {
			entered <- slotID
			<-release
			return errStopBeforeAppliedIndex
		},
	}}
	storage := NewPebbleShardLogStorage(server, t.TempDir(), 1)
	if err := storage.Open(); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	return storage, entered, release
}

func waitForApply(t *testing.T, entered <-chan uint32, name string) {
	t.Helper()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s to enter OnApply", name)
	}
}
