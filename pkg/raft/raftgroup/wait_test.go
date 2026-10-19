package raftgroup

import (
	"sync"
	"testing"
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
