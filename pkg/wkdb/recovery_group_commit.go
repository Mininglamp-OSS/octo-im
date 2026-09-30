package wkdb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

type recoveryCommitRequest struct {
	batch *pebble.Batch
	size  int
	done  chan error
}

// recoveryShardCoordinator has one owner goroutine per physical WKDB shard.
// Callers retain per-entry wait-all semantics by submitting once per touched
// shard and waiting for every result in commitRecoveryBatches.
type recoveryShardCoordinator struct {
	db        *pebble.DB
	config    RecoveryGroupCommitOptions
	input     chan recoveryCommitRequest
	stop      chan struct{}
	done      chan struct{}
	once      sync.Once
	submitMu  sync.Mutex
	closed    bool
	queued    atomic.Int64
	inflight  atomic.Int64
	commits   atomic.Uint64
	fragments atomic.Uint64
	bytes     atomic.Uint64
}

type RecoveryGroupCommitStats struct {
	Enabled         bool   `json:"enabled"`
	QueueDepth      int64  `json:"queue_depth"`
	Inflight        int64  `json:"inflight"`
	PhysicalCommits uint64 `json:"physical_commits"`
	Fragments       uint64 `json:"fragments"`
	MutationBytes   uint64 `json:"mutation_bytes"`
}

func validateRecoveryGroupCommit(c RecoveryGroupCommitOptions) error {
	if c.Window < 250*time.Microsecond || c.Window > 5*time.Millisecond {
		return errors.New("subscriberRecovery.groupCommit.window must be between 250us and 5ms")
	}
	if c.MaxCount < 1 || c.MaxCount > 128 {
		return errors.New("subscriberRecovery.groupCommit.maxCount must be between 1 and 128")
	}
	if c.MaxBytes < 64<<10 || c.MaxBytes > 4<<20 {
		return errors.New("subscriberRecovery.groupCommit.maxBytes must be between 64KiB and 4MiB")
	}
	if c.OldestAge < time.Millisecond || c.OldestAge > 10*time.Millisecond || c.OldestAge < c.Window {
		return errors.New("subscriberRecovery.groupCommit.oldestAge must be between 1ms and 10ms and not less than window")
	}
	if c.QueueHard < c.MaxCount {
		return errors.New("subscriberRecovery.groupCommit.queueHard must be at least maxCount")
	}
	return nil
}

func newRecoveryShardCoordinator(db *pebble.DB, config RecoveryGroupCommitOptions) *recoveryShardCoordinator {
	c := &recoveryShardCoordinator{db: db, config: config, input: make(chan recoveryCommitRequest, config.QueueHard), stop: make(chan struct{}), done: make(chan struct{})}
	go c.run()
	return c
}

func (c *recoveryShardCoordinator) Submit(batch *pebble.Batch) error {
	r := recoveryCommitRequest{batch: batch, size: len(batch.Repr()), done: make(chan error, 1)}
	c.submitMu.Lock()
	if c.closed {
		c.submitMu.Unlock()
		return errors.New("recovery group commit is closed")
	}
	c.queued.Add(1)
	c.input <- r
	c.submitMu.Unlock()
	return <-r.done
}

func (c *recoveryShardCoordinator) Close() {
	c.once.Do(func() {
		c.submitMu.Lock()
		c.closed = true
		close(c.stop)
		c.submitMu.Unlock()
		<-c.done
	})
}

func (c *recoveryShardCoordinator) run() {
	defer close(c.done)
	var pending []recoveryCommitRequest
	for {
		var first recoveryCommitRequest
		select {
		case first = <-c.input:
		case <-c.stop:
			c.drain(pending)
			return
		}
		pending = append(pending, first)
		c.queued.Add(-1)
		bytes := first.size
		deadline := time.NewTimer(c.config.Window)
	collect:
		for len(pending) < c.config.MaxCount && bytes < c.config.MaxBytes {
			select {
			case r := <-c.input:
				c.queued.Add(-1)
				if r.size > c.config.MaxBytes || bytes+r.size > c.config.MaxBytes {
					c.flush(pending)
					pending = pending[:0]
					if r.size > c.config.MaxBytes {
						c.flush([]recoveryCommitRequest{r})
						break collect
					}
					pending = append(pending, r)
					bytes = r.size
					continue
				}
				pending = append(pending, r)
				bytes += r.size
			case <-deadline.C:
				break collect
			case <-c.stop:
				if !deadline.Stop() {
					select {
					case <-deadline.C:
					default:
					}
				}
				c.drain(pending)
				return
			}
		}
		if !deadline.Stop() {
			select {
			case <-deadline.C:
			default:
			}
		}
		c.flush(pending)
		pending = pending[:0]
	}
}

func (c *recoveryShardCoordinator) drain(pending []recoveryCommitRequest) {
	for {
		select {
		case r := <-c.input:
			c.queued.Add(-1)
			pending = append(pending, r)
		default:
			c.flush(pending)
			return
		}
	}
}

func (c *recoveryShardCoordinator) flush(cohort []recoveryCommitRequest) {
	if len(cohort) == 0 {
		return
	}
	merged := c.db.NewBatch()
	c.inflight.Add(1)
	defer c.inflight.Add(-1)
	var err error
	for _, r := range cohort {
		if err == nil {
			err = merged.Apply(r.batch, nil)
		}
	}
	if err == nil {
		err = merged.Commit(pebble.Sync)
		c.commits.Add(1)
	}
	if closeErr := merged.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		err = fmt.Errorf("recovery shard group commit: %w", err)
	}
	for _, r := range cohort {
		c.fragments.Add(1)
		c.bytes.Add(uint64(r.size))
		r.done <- err
	}
}
