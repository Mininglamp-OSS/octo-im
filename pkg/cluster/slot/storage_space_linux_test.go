package slot

import (
	"bytes"
	"io/fs"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// Exercise the real filesystem: apparent file length excludes Pebble's WAL
// preallocation. A small slot must not reserve hundreds of MiB while warming up.
func TestSlotWALStartupSpaceAndReopen(t *testing.T) {
	dir := t.TempDir()
	storage := NewPebbleShardLogStorage(nil, dir, 1)
	require.NoError(t, storage.Open())
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, storage.Close())
		}
	})
	payload := bytes.Repeat([]byte("log-data"), 1024)
	const count = 1024
	for i := 1; i <= count; i++ {
		require.NoError(t, storage.AppendLogs("1", []types.Log{{Index: uint64(i), Term: 1, Data: payload}}, nil))
	}
	var allocated int64
	require.NoError(t, filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		allocated += info.Sys().(*syscall.Stat_t).Blocks * 512
		return nil
	}))
	// Includes live/recycled WALs and SSTs, measured before Close can flush.
	require.Less(t, allocated, int64(96<<20), "8MiB of logs must fit a bounded shard startup budget")
	require.NoError(t, storage.Close())
	closed = true
	storage = NewPebbleShardLogStorage(nil, dir, 1)
	require.NoError(t, storage.Open())
	closed = false
	logs, err := storage.GetLogs("1", 1, count+1, 0)
	require.NoError(t, err)
	require.Len(t, logs, count)
	for i, log := range logs {
		require.Equal(t, uint64(i+1), log.Index)
		require.Equal(t, payload, log.Data)
	}
}
