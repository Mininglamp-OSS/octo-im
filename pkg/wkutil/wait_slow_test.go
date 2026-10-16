package wkutil

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitReportingSlow(t *testing.T) {
	release := make(chan struct{})
	sentinel := errors.New("close failed")
	var reports atomic.Int32
	var last atomic.Int64
	result := make(chan error, 1)
	go func() {
		result <- WaitReportingSlow(func(ctx context.Context) error {
			require.NoError(t, ctx.Err(), "the wait has no deadline")
			<-release
			return sentinel
		}, 5*time.Millisecond, func(elapsed time.Duration) {
			last.Store(int64(elapsed))
			reports.Add(1)
		})
	}()
	require.Eventually(t, func() bool { return reports.Load() >= 3 }, 5*time.Second, time.Millisecond)
	require.GreaterOrEqual(t, time.Duration(last.Load()), 10*time.Millisecond, "reports carry the elapsed time")
	select {
	case <-result:
		t.Fatal("reporting must not end the wait")
	default:
	}
	close(release)
	require.ErrorIs(t, <-result, sentinel)

	// A wait that finishes before the interval reports nothing.
	reports.Store(0)
	require.NoError(t, WaitReportingSlow(func(context.Context) error { return nil }, time.Hour, func(time.Duration) { reports.Add(1) }))
	require.Zero(t, reports.Load())
}
