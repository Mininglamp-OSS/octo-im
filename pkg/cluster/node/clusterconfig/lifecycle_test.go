package clusterconfig

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

// A Server is single-use: once Stop has begun, Start is refused and owns
// nothing. A restart constructs a new Server on the same directory.
func TestServerRestartRequiresNewInstance(t *testing.T) {
	opts := NewOptions(WithNodeId(1), WithInitNodes(map[uint64]string{1: ""}), WithConfigPath(t.TempDir()+"/cluster.json"))
	s := New(opts)
	require.NoError(t, s.Start())
	require.ErrorIs(t, s.Start(), ErrServerLifecycle, "a running Server cannot start twice")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.WaitStopped(ctx))

	require.ErrorIs(t, s.Start(), ErrServerLifecycle)
	require.NoError(t, s.WaitStopped(ctx), "a refused Start leaves the completed result intact")
	// The refused Start reopened nothing: the directory lock is free.
	db, err := pebble.Open(s.storage.(*PebbleShardLogStorage).path, &pebble.Options{})
	require.NoError(t, err, "storage still open after a stopped lifecycle")
	require.NoError(t, db.Close())
	require.NoError(t, s.config.cfgFile.Close())

	restarted := New(opts)
	require.NoError(t, restarted.Start())
	require.NoError(t, restarted.WaitStopped(ctx))
	require.NoError(t, restarted.config.cfgFile.Close())
}

// Start while the close lifecycle is still pending must neither reopen nor
// replace the storage handle that the pending close still owns.
func TestServerStartWhileStopPending(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithInitNodes(map[uint64]string{1: ""}), WithConfigPath(t.TempDir()+"/cluster.json")))
	st := &ownerStorage{PebbleShardLogStorage: s.storage.(*PebbleShardLogStorage), op: "hardstate", entered: make(chan struct{}), release: make(chan struct{})}
	s.storage = st
	require.NoError(t, s.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.raft.StepWait(ctx, types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1}, Term: 1, Leader: 1}}))
	var once sync.Once
	release := func() { once.Do(func() { close(st.release) }) }
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, s.WaitStopped(ctx))
		require.NoError(t, s.config.cfgFile.Close())
	})
	st.blockHardState.Store(true)
	s.StepRaftEvent(types.Event{Type: types.Ping, From: 2, To: 1, Term: 2})
	ownerRecv(t, st.entered)
	s.Stop()

	db := st.db
	require.ErrorIs(t, s.Start(), ErrServerLifecycle)
	require.Same(t, db, st.db, "Start replaced the handle owned by the pending close")
	require.NotNil(t, st.db)
	require.Zero(t, st.closes.Load())

	// Concurrent Start and Stop calls share the single pending lifecycle.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); require.ErrorIs(t, s.Start(), ErrServerLifecycle) }()
		go func() { defer wg.Done(); s.Stop() }()
	}
	wg.Wait()
	release()
	require.NoError(t, s.WaitStopped(ctx))
	require.EqualValues(t, 1, st.closes.Load())
	require.Zero(t, st.afterClose.Load())
}

// Stop before Start ends the lifecycle without owning storage.
func TestServerStopBeforeStart(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithInitNodes(map[uint64]string{1: ""}), WithConfigPath(t.TempDir()+"/cluster.json")))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.WaitStopped(ctx))
	require.ErrorIs(t, s.Start(), ErrServerLifecycle)
	require.Nil(t, s.storage.(*PebbleShardLogStorage).db)
	require.NoError(t, s.config.cfgFile.Close())
}

// Storage Open keeps the existing handle on failure and Close is idempotent.
func TestConfigStorageOpenCloseSafety(t *testing.T) {
	dir := t.TempDir()
	a := NewPebbleShardLogStorage(dir, nil)
	require.NoError(t, a.Close(), "closing a never-opened storage is a no-op")
	require.NoError(t, a.Open())
	db := a.db
	require.Error(t, a.Open(), "a second Open must not replace the live handle")
	require.Same(t, db, a.db)

	b := NewPebbleShardLogStorage(dir, nil)
	require.Error(t, b.Open(), "directory lock is held")
	require.Nil(t, b.db)
	require.NoError(t, b.Close())

	require.NoError(t, a.Close())
	require.NoError(t, a.Close())
}
