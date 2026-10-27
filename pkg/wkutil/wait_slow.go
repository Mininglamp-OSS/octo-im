package wkutil

import (
	"context"
	"time"
)

// WaitReportingSlow runs wait without a deadline and calls slow with the
// elapsed time every interval while it is still pending. It never shortens
// the wait; it only makes a long one visible.
func WaitReportingSlow(wait func(context.Context) error, interval time.Duration, slow func(elapsed time.Duration)) error {
	done := make(chan struct{})
	defer close(done)
	start := time.Now()
	go func() {
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-done:
				return
			case <-tk.C:
				slow(time.Since(start))
			}
		}
	}()
	return wait(context.Background())
}
