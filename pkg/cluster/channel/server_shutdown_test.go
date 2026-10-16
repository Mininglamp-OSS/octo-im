package channel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

// The production channel owner with a RaftGroup event loop blocked in a
// shared-DB hard-state write: a 30 ms handoff returns at its deadline and the
// shared DB stays open and usable; after the write returns, a later call
// finishes the same lifecycles, and the DB owner then closes it exactly once.
func TestChannelServerStopBoundsBlockedHardState(t *testing.T) {
	dir := t.TempDir()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	s := NewServer(NewOptions(WithNodeId(1), WithDB(db), WithGroupCount(2), WithTransport(nopTransport{})))
	require.NoError(t, s.Start())

	key := wkutil.ChannelToKey("hard-state", 2)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var calls atomic.Int64
	save := func(st types.HardState) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return db.SaveRaftHardState(key, st)
	}
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(key), raft.WithNodeId(1), raft.WithReplicas([]uint64{1}), raft.WithSaveHardState(save)))
	s.getRaftGroup(key).AddRaft(node)
	s.getRaftGroup(key).Advance()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("hard-state write not reached")
	}

	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		started := time.Now()
		err := s.StopWithContext(ctx)
		elapsed := time.Since(started)
		cancel()
		require.ErrorIs(t, err, raftgroup.ErrIncompleteQuiescence)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, time.Second, "the handoff must return at its deadline, not when the write returns")
		t.Logf("bounded handoff %d returned after %s: %v", i+1, elapsed, err)
	}
	// The shared DB is still open and usable while the lifecycle is pending.
	require.NoError(t, db.SaveRaftHardState("probe", types.HardState{Term: 9}))

	releaseOnce.Do(func() { close(release) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.StopWithContext(ctx))
	require.NoError(t, s.StopWithContext(ctx), "repeated handoff keeps its result")
	require.EqualValues(t, 1, calls.Load())
	require.NoError(t, db.Close())

	reopened := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(1)))
	require.NoError(t, reopened.Open())
	defer func() { require.NoError(t, reopened.Close()) }()
	st, err := reopened.RaftHardState(key)
	require.NoError(t, err)
	require.EqualValues(t, 1, st.Term)
}
