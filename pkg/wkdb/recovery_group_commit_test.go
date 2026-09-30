package wkdb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestRecoveryGroupCommitValidation(t *testing.T) {
	valid := RecoveryGroupCommitOptions{Enabled: true, Window: 2 * time.Millisecond, MaxCount: 32, MaxBytes: 1 << 20, OldestAge: 5 * time.Millisecond, QueueHard: 256}
	require.NoError(t, validateRecoveryGroupCommit(valid))
	invalid := valid
	invalid.OldestAge = time.Millisecond
	require.Error(t, validateRecoveryGroupCommit(invalid))
	invalid = valid
	invalid.MaxCount = 129
	require.Error(t, validateRecoveryGroupCommit(invalid))
}

func TestRecoveryShardCoordinatorMergesConcurrentFragments(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), nil)
	require.NoError(t, err)
	c := newRecoveryShardCoordinator(db, RecoveryGroupCommitOptions{Enabled: true, Window: 5 * time.Millisecond, MaxCount: 32, MaxBytes: 1 << 20, OldestAge: 5 * time.Millisecond, QueueHard: 256})
	const count = 16
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		batch := db.NewBatch()
		require.NoError(t, batch.Set([]byte(fmt.Sprintf("k%02d", i)), []byte("v"), nil))
		wg.Add(1)
		go func(batch *pebble.Batch) {
			defer wg.Done()
			require.NoError(t, c.Submit(batch))
			require.NoError(t, batch.Close())
		}(batch)
	}
	wg.Wait()
	c.Close()
	for i := 0; i < count; i++ {
		value, closer, err := db.Get([]byte(fmt.Sprintf("k%02d", i)))
		require.NoError(t, err)
		require.Equal(t, "v", string(value))
		require.NoError(t, closer.Close())
	}
	require.NoError(t, db.Close())
}
