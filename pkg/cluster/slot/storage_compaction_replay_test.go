package slot

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/slot/key"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestSlotAppliedPrefixSurvivesTailTruncationAndCompaction(t *testing.T) {
	dir := t.TempDir()
	var seen []uint64
	newStorage := func() *PebbleShardLogStorage {
		p := NewPebbleShardLogStorage(&Server{opts: &Options{OnApply: func(_ uint32, logs []types.Log) error {
			for _, log := range logs {
				seen = append(seen, log.Index)
			}
			return nil
		}}}, dir, 1)
		require.NoError(t, p.Open())
		return p
	}
	p := newStorage()
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	logs := []types.Log{{Index: 1, Term: 1, Data: []byte("one")}, {Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 1, Data: []byte("obsolete-three")}, {Index: 4, Term: 1, Data: []byte("obsolete-four")}}
	require.NoError(t, p.AppendLogs("59", logs, &types.TermStartIndexInfo{Term: 1, Index: 1}))
	require.NoError(t, p.Apply("59", logs[:2]))
	require.ErrorIs(t, p.TruncateLogTo("59", 1), types.ErrCommittedLogConflict)
	require.NoError(t, p.TruncateLogTo("59", 2))
	require.NoError(t, p.shardDB("59").Flush())
	require.NoError(t, p.shardDB("59").Compact(key.NewLogKey("59", 0), key.NewLogKey("59", math.MaxUint64), true))
	retained, err := p.GetLogs("59", 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, retained, 2, "physical compaction removes the uncommitted tail, not the applied prefix")
	require.NoError(t, p.Apply("59", logs[:2]), "retry of the applied prefix remains idempotent after compaction")
	require.Equal(t, []uint64{1, 2}, seen)
	canonical := []types.Log{{Index: 3, Term: 2, Data: []byte("canonical-three")}, {Index: 4, Term: 2, Data: []byte("canonical-four")}}
	require.NoError(t, p.AppendLogs("59", canonical, &types.TermStartIndexInfo{Term: 2, Index: 3}))
	require.Error(t, p.Apply("59", logs), "a retry must not apply bytes replaced in the uncommitted tail")
	require.Equal(t, []uint64{1, 2}, seen)
	replay := append(append([]types.Log{}, logs[:2]...), canonical...)
	require.NoError(t, p.Apply("59", replay))
	require.Equal(t, []uint64{1, 2, 3, 4}, seen)
	require.NoError(t, p.Close())
	p = newStorage()
	state, err := p.GetState("59")
	require.NoError(t, err)
	require.Equal(t, uint64(4), state.AppliedIndex)
	require.Equal(t, uint64(4), state.LastLogIndex)
	require.Equal(t, uint32(2), state.LastTerm)
	require.NoError(t, p.Apply("59", replay))
	require.Equal(t, []uint64{1, 2, 3, 4}, seen, "restart must not reapply the already materialized prefix")
}

func TestSlotApplyAndTruncatePreserveAppliedBoundary(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	p := newPrefixStorage(t, func(uint32, []types.Log) error {
		close(entered)
		<-release
		return nil
	})
	var work sync.WaitGroup
	t.Cleanup(func() { unblock(); work.Wait() })
	logs := []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	require.NoError(t, p.AppendLogs("59", logs, nil))
	applied, truncated := make(chan error, 1), make(chan error, 1)
	work.Add(1)
	go func() { defer work.Done(); applied <- p.Apply("59", logs) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Apply did not enter the real state-machine callback")
	}
	attempted := make(chan struct{})
	work.Add(1)
	go func() {
		defer work.Done()
		close(attempted)
		truncated <- p.TruncateLogTo("59", 1)
	}()
	<-attempted
	select {
	case err := <-truncated:
		t.Fatalf("truncate finished before the in-progress Apply and its marker: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-applied)
	require.ErrorIs(t, <-truncated, types.ErrCommittedLogConflict)
	state, err := p.GetState("59")
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.AppliedIndex)
	require.Equal(t, uint64(2), state.LastLogIndex, "the completed Apply must prevent truncation of its applied tail")
}
