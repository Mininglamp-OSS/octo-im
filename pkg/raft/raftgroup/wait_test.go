package raftgroup

import (
	"sync"
	"testing"
)

func TestApplyWaitConcurrentNotificationAndRelease(t *testing.T) {
	w := newWait()
	for i := uint64(1); i <= 100; i++ {
		p := w.waitApply("key", i)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); w.didApply("key", i) }()
		<-p.waitC
		if !w.put(p) {
			t.Fatal("notified waiter must be complete")
		}
		wg.Wait()
	}
}

func TestApplyWaitFailedAdmissionCannotNotifyReusedWaiter(t *testing.T) {
	w := newWait()
	p := w.waitApply("key", 1)
	if w.put(p) { // admission failed before an apply notification
		t.Fatal("detached waiter must not be complete")
	}
	next := w.waitApply("key", 2)
	w.didApply("key", 1)
	select {
	case <-next.waitC:
		t.Fatal("old notification completed a newer waiter")
	default:
	}
	w.didApply("key", 2)
	<-next.waitC
	if !w.put(next) {
		t.Fatal("notified waiter must be complete")
	}
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
