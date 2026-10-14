package raftgroup

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestApplyWaitReleaseDetachesPendingWaiter(t *testing.T) {
	w := newWait()
	p := w.waitApply("slot", 1)
	w.put(p)
	b := w.buckets[w.bucketIndex("slot")]
	if len(b.progresses) != 0 {
		t.Fatalf("released waiter retained by bucket: %d entries", len(b.progresses))
	}
	// A late apply must not reach the detached/recycled waiter.
	w.didApply("slot", 1)
}

func TestApplyWaitConcurrentNotificationAndRelease(t *testing.T) {
	w := newWait()
	for index := uint64(1); index <= 200; index++ {
		p := w.waitApply("slot", index)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.didApply("slot", index)
		}()
		<-p.waitC
		w.put(p)
		wg.Wait()
	}
}

func TestApplyWaitLateApplyDoesNotCompleteNewerWaiter(t *testing.T) {
	w := newWait()
	p := w.waitApply("slot", 1)
	w.put(p)
	if len(w.buckets[w.bucketIndex("slot")].progresses) != 0 {
		t.Fatal("failed admission retained a pointer to a recycled waiter")
	}
	next := w.waitApply("slot", 2)
	w.didApply("slot", 1)
	select {
	case <-next.waitC:
		t.Fatal("late apply completed a newer proposal")
	default:
	}
	w.didApply("slot", 2)
	<-next.waitC
	w.put(next)
}

func TestApplyWait_didApply(t *testing.T) {
	aw := newWait()

	progress := aw.waitApply("key", 10)
	if progress == nil {
		t.Fatalf("expected non-nil progress")
	}

	aw.didApply("key", 5)
	select {
	case <-progress.waitC:
		t.Fatalf("expected progress to not be done")
	default:

	}

	aw.didApply("key", 10)
	select {
	case <-progress.waitC:
		// expected
	default:
		t.Fatalf("expected progress to be done")
	}
}

// Keys that hash to one bucket share its slice, lock and pool, which is where
// a recycled waiter could still be referenced or woken for the wrong proposal.
func sameBucketKeys(t *testing.T, w *wait, n int) []string {
	t.Helper()
	keys := []string{"slot-0"}
	target := w.bucketIndex(keys[0])
	for i := 1; len(keys) < n && i < 100000; i++ {
		key := fmt.Sprintf("slot-%d", i)
		if w.bucketIndex(key) == target {
			keys = append(keys, key)
		}
	}
	if len(keys) < n {
		t.Fatalf("found %d keys for bucket %d, want %d", len(keys), target, n)
	}
	return keys
}

func TestApplyWaitRegisterReleaseReuseStress(t *testing.T) {
	const iterations = 1000
	w := newWait()
	keys := sameBucketKeys(t, w, 4)
	b := w.buckets[w.bucketIndex(keys[0])]
	live := make(map[*progress]string, len(keys))
	for i := 0; i < iterations; i++ {
		index := uint64(i + 1)
		waiters := make([]*progress, len(keys))
		for k, key := range keys {
			p := w.waitApply(key, index)
			if owner, ok := live[p]; ok {
				t.Fatalf("iteration %d: %s got the live waiter owned by %s", i, key, owner)
			}
			live[p] = key
			waiters[k] = p
		}

		// An apply below every waiter's index must not wake any of them.
		for _, key := range keys {
			w.didApply(key, index-1)
		}
		for k, p := range waiters {
			select {
			case <-p.waitC:
				t.Fatalf("iteration %d: %s woken by an earlier index", i, keys[k])
			default:
			}
		}

		// Even keys are applied while odd keys are released as timed out, so
		// notification and detach contend on the same bucket.
		var wg sync.WaitGroup
		for k := 0; k < len(keys); k += 2 {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				w.didApply(key, index)
			}(keys[k])
		}
		for k := 1; k < len(keys); k += 2 {
			p := waiters[k]
			select {
			case <-p.waitC:
				t.Fatalf("iteration %d: %s woken by another key's apply", i, keys[k])
			default:
			}
			if w.put(p) {
				t.Fatalf("iteration %d: unapplied %s reported as completed", i, keys[k])
			}
			delete(live, p)
		}
		wg.Wait()
		for k := 0; k < len(keys); k += 2 {
			p := waiters[k]
			select {
			case <-p.waitC:
			case <-time.After(time.Second):
				t.Fatalf("iteration %d: applied %s was not woken", i, keys[k])
			}
			if !w.put(p) {
				t.Fatalf("iteration %d: applied %s reported as not completed", i, keys[k])
			}
			delete(live, p)
		}

		b.mu.Lock()
		remaining := len(b.progresses)
		b.mu.Unlock()
		if remaining != 0 {
			t.Fatalf("iteration %d: bucket retained %d released waiters", i, remaining)
		}
	}
	if len(live) != 0 {
		t.Fatalf("%d waiters were never released", len(live))
	}
}
