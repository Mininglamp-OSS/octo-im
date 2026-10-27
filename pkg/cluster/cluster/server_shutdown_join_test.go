package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

// A reconciler page read is synchronous storage I/O. StopWithContext must not
// join it before honoring ctx, and the shared DB must stay open until it exits.
func TestStopWithContextBoundsBlockedReconcilerJoin(t *testing.T) {
	var armed atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	releaseRead := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseRead)
	s, _ := newConversationConsistencyServer(t, func(s *Server) {
		page := s.configReconciler.page
		s.configReconciler.page = func(offset uint64, limit int) ([]wkdb.ChannelClusterConfig, error) {
			if armed.Load() {
				enterOnce.Do(func() { close(entered) })
				<-release
			}
			return page(offset, limit)
		}
	})
	armed.Store(true)
	s.configReconciler.rescan()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reconciler page read did not start")
	}

	stop := func(timeout time.Duration) (error, time.Duration) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- s.StopWithContext(ctx) }()
		select {
		case err := <-done:
			return err, time.Since(start)
		case <-time.After(2 * time.Second):
			releaseRead()
			<-done
			t.Fatal("StopWithContext joined the blocked reconciler before honoring its deadline")
			return nil, 0
		}
	}
	for i := 0; i < 2; i++ {
		err, elapsed := stop(20 * time.Millisecond)
		require.ErrorIs(t, err, raft.ErrIncompleteQuiescence)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, elapsed, time.Second)
		_, err = s.db.GetChannelClusterConfigs(0, 1)
		require.NoError(t, err, "shared storage stays open while the reconciler can still read it")
	}
	releaseRead()
	err, _ := stop(10 * time.Second)
	require.NoError(t, err, "a later Stop waits on the same join and completes the handoff")
}
