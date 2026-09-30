package wkdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

type benchmarkCommitRequest struct {
	batch *pebble.Batch
	done  chan error
}
type benchmarkCommitCoordinator struct {
	db      *pebble.DB
	input   chan benchmarkCommitRequest
	cohorts chan []benchmarkCommitRequest
	wg      sync.WaitGroup
}

func newBenchmarkCommitCoordinator(db *pebble.DB) *benchmarkCommitCoordinator {
	c := &benchmarkCommitCoordinator{db: db, input: make(chan benchmarkCommitRequest, 256), cohorts: make(chan []benchmarkCommitRequest, 8)}
	for i := 0; i < 8; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for cohort := range c.cohorts {
				merged := db.NewBatch()
				var err error
				for _, r := range cohort {
					if err == nil {
						err = merged.Apply(r.batch, nil)
					}
				}
				if err == nil {
					err = merged.Commit(pebble.Sync)
				}
				_ = merged.Close()
				for _, r := range cohort {
					r.done <- err
				}
			}
		}()
	}
	go func() {
		for first := range c.input {
			cohort := []benchmarkCommitRequest{first}
			timer := time.NewTimer(2 * time.Millisecond)
		collect:
			for len(cohort) < 32 {
				select {
				case r := <-c.input:
					cohort = append(cohort, r)
					if len(cohort) >= 8 {
						break collect
					}
				case <-timer.C:
					break collect
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			c.cohorts <- cohort
		}
		close(c.cohorts)
	}()
	return c
}
func (c *benchmarkCommitCoordinator) submit(batch *pebble.Batch) error {
	r := benchmarkCommitRequest{batch: batch, done: make(chan error, 1)}
	c.input <- r
	return <-r.done
}
func (c *benchmarkCommitCoordinator) close() { close(c.input); c.wg.Wait() }

// This retains the rejected prototype as benchmark-only evidence; no production
// option or runtime path depends on it.
func BenchmarkRecoveryCommit(b *testing.B) {
	for _, group := range []bool{false, true} {
		name := "direct_sync"
		if group {
			name = "prototype_group_commit_sync"
		}
		b.Run(name, func(b *testing.B) {
			b.SetParallelism(16)
			root := b.TempDir()
			dbs := make([]*pebble.DB, 8)
			coordinators := make([]*benchmarkCommitCoordinator, 8)
			for i := range dbs {
				dbs[i], _ = pebble.Open(fmt.Sprintf("%s/shard-%d", root, i), &pebble.Options{WALMinSyncInterval: func() time.Duration { return 20 * time.Millisecond }})
				if group {
					coordinators[i] = newBenchmarkCommitCoordinator(dbs[i])
				}
			}
			var sequence atomic.Uint64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					n := sequence.Add(1)
					shard := n % 8
					batch := dbs[shard].NewBatch()
					for key := 0; key < 8; key++ {
						_ = batch.Set([]byte(fmt.Sprintf("%020d/%d", n, key)), make([]byte, 200), nil)
					}
					if group {
						_ = coordinators[shard].submit(batch)
					} else {
						_ = batch.Commit(pebble.Sync)
					}
					_ = batch.Close()
				}
			})
			b.StopTimer()
			for _, c := range coordinators {
				if c != nil {
					c.close()
				}
			}
			for _, db := range dbs {
				_ = db.Close()
			}
		})
	}
}
