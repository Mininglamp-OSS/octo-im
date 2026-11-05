package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestStartupRetryDeadlineSurvivesFullQueue(t *testing.T) {
	r := newChannelConfigReconciler(context.Background())
	defer r.stop()
	r.capacity = 1
	k := channelConfigKey{"failed", 2}
	require.True(t, r.add(k, false))
	_, rev, ok := r.take()
	require.True(t, ok)
	r.finish(k, rev, errors.New("offline"))
	due := r.cooling[k].due
	for i := 0; i < 100; i++ {
		require.True(t, r.add(k, true))
		_, _, ok = r.take()
		require.False(t, ok)
	}
	require.Equal(t, due, r.cooling[k].due)
	healthy := channelConfigKey{"healthy", 2}
	require.True(t, r.add(healthy, false))
	got, rev, ok := r.take()
	require.True(t, ok)
	require.Equal(t, healthy, got)
	r.finish(got, rev, nil)
	r.cooling[k].due = time.Now().Add(-time.Second)
	got, rev, ok = r.take()
	require.True(t, ok)
	require.Equal(t, k, got)
	r.finish(got, rev, errors.New("offline"))
	require.Equal(t, 2, r.cooling[k].failures)
}
func TestStartupRetryHistoryBoundedAndOverflowCooled(t *testing.T) {
	r := newChannelConfigReconciler(context.Background())
	defer r.stop()
	r.capacity = 1
	for i := 0; i < 5; i++ {
		k := channelConfigKey{fmt.Sprint(i), 2}
		require.True(t, r.add(k, false))
		_, rev, ok := r.take()
		require.True(t, ok)
		r.finish(k, rev, errors.New("offline"))
	}
	require.LessOrEqual(t, len(r.cooling), 4)
	require.True(t, r.overflowDue.After(time.Now()))
	require.True(t, r.add(channelConfigKey{"0", 2}, false))
	require.Empty(t, r.pending)
}

func TestStartupRetryWaveHasAggregateBound(t *testing.T) {
	r := newChannelConfigReconciler(context.Background())
	r.page = func(uint64, int) ([]wkdb.ChannelClusterConfig, error) { return nil, nil }
	r.owned = func(channelConfigKey) bool { return true }
	var attempts atomic.Int64
	r.process = func(context.Context, channelConfigKey) error {
		attempts.Add(1)
		return errors.New("peer not authenticated")
	}
	for i := 0; i < 1000; i++ {
		r.add(channelConfigKey{fmt.Sprint(i), 2}, false)
	}
	r.start()
	time.Sleep(300 * time.Millisecond)
	r.stop()
	require.Positive(t, attempts.Load())
	require.LessOrEqual(t, attempts.Load(), int64(28), "distinct failing channels must not bypass the aggregate failure budget")
}
