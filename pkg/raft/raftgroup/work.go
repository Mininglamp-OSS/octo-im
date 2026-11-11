package raftgroup

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

type storageWork struct {
	pending int
	done    chan struct{}
}

// WaitStorage waits for previously submitted local I/O for key. Retire the
// instance on its owner before calling this, and load its replacement only
// after it returns. Never wait on the owner: Apply and SaveConfig can need it.
func (rg *RaftGroup) WaitStorage(ctx context.Context, key string) error {
	rg.workMu.Lock()
	work := rg.storageWork[key]
	closing := rg.closing
	rg.workMu.Unlock()
	if closing {
		return ErrGroupStopped
	}
	if work == nil {
		return ctx.Err()
	}
	select {
	case <-work.done:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case <-rg.stopper.ShouldStop():
		return ErrGroupStopped
	}
}

// submitWork counts accepted workers before publishing them to the pool, so
// Stop cannot close storage under an accepted task. Only local storage work
// participates in the per-key drain: SaveConfig may itself apply a removal of
// this key and must not wait for its own completion through that drain.
func (rg *RaftGroup) submitWork(r IRaft, localStorage bool, fn func()) error {
	rg.workMu.Lock()
	if rg.closing {
		rg.workMu.Unlock()
		return ErrGroupStopped
	}
	if r != nil && rg.GetRaft(r.Key()) != r {
		rg.workMu.Unlock()
		return ErrRaftNotExist
	}
	var work *storageWork
	if localStorage {
		work = rg.storageWork[r.Key()]
		if work == nil {
			work = &storageWork{done: make(chan struct{})}
			rg.storageWork[r.Key()] = work
		}
		work.pending++
	}
	rg.workers.Add(1)
	rg.workMu.Unlock()
	finish := func() {
		if work != nil {
			rg.workMu.Lock()
			work.pending--
			if work.pending == 0 {
				delete(rg.storageWork, r.Key())
				close(work.done)
			}
			rg.workMu.Unlock()
		}
		rg.workers.Done()
	}
	err := rg.goPool.Submit(func() {
		defer finish()
		fn()
	})
	if err != nil {
		finish()
	}
	return err
}

// AddEventFor binds a local event to the instance that produced it. Network
// events still use AddEvent and Raft's term/configuration checks. Check again
// when consuming: an event can already be queued when its instance retires.
func (rg *RaftGroup) AddEventFor(r IRaft, e types.Event) {
	rg.mq.MustAdd(Event{RaftKey: r.Key(), Event: e, instance: r})
	rg.Advance()
}
