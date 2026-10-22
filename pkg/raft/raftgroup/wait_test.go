package raftgroup

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func testReceipt(key string, seq uint64, term uint32, index uint64, id uint64) receipt {
	return receipt{raftKey: key, seq: seq, term: term, first: index, last: index, logs: []types.Log{{Index: index, Id: id}}}
}

func applied(index uint64, term uint32, id uint64) []types.Log {
	return []types.Log{{Index: index, Term: term, Id: id}}
}

// result returns the published result of w, or fails if none arrives.
func result(t *testing.T, w *receiptWaiter) error {
	t.Helper()
	select {
	case err := <-w.doneC:
		return err
	case <-time.After(time.Second):
		t.Fatalf("receipt %s was not completed", w.receipt)
		return nil
	}
}

func assertNoResult(t *testing.T, w *receiptWaiter) {
	t.Helper()
	select {
	case err := <-w.doneC:
		t.Fatalf("receipt %s unexpectedly completed: %v", w.receipt, err)
	default:
	}
}

func TestReceiptDetachRemovesPendingWaiter(t *testing.T) {
	w := newWait(nil)
	r, _ := w.register(testReceipt("slot", 1, 1, 1, 100))
	if _, completed := w.detach(r); completed {
		t.Fatal("pending receipt reported as completed")
	}
	if n := w.pending(); n != 0 {
		t.Fatalf("detached receipt retained: %d", n)
	}
	// A late apply must not reach the detached receipt.
	w.didApply("slot", 1, applied(1, 1, 100))
	assertNoResult(t, r)
}

func TestReceiptConcurrentApplyAndDetach(t *testing.T) {
	w := newWait(nil)
	for index := uint64(1); index <= 200; index++ {
		r, _ := w.register(testReceipt("slot", index, 1, index, index))
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.didApply("slot", index, applied(index, 1, index))
		}()
		res, completed := w.detach(r)
		wg.Wait()
		if completed {
			if res != nil {
				t.Fatalf("index %d: applied receipt completed with %v", index, res)
			}
			if err := result(t, r); err != nil {
				t.Fatalf("index %d: published %v", index, err)
			}
		} else {
			// Detach won: apply must not publish a second, late result.
			assertNoResult(t, r)
		}
		if n := w.pending(); n != 0 {
			t.Fatalf("index %d: %d receipts retained", index, n)
		}
	}
}

func TestReceiptLateApplyDoesNotCompleteNewerWaiter(t *testing.T) {
	w := newWait(nil)
	old, _ := w.register(testReceipt("slot", 1, 1, 1, 100))
	w.detach(old)
	next, _ := w.register(testReceipt("slot", 2, 1, 2, 200))
	w.didApply("slot", 1, applied(1, 1, 100))
	assertNoResult(t, next)
	w.didApply("slot", 2, applied(2, 1, 200))
	if err := result(t, next); err != nil {
		t.Fatalf("own entry applied, got %v", err)
	}
}

// A different entry at the receipt's last index never completes it with
// success: neither another Id nor the same Id in another term.
func TestReceiptIdentityMismatchIsSuperseded(t *testing.T) {
	for name, log := range map[string]types.Log{
		"other id":   {Index: 1, Term: 1, Id: 200},
		"other term": {Index: 1, Term: 2, Id: 100},
		"both":       {Index: 1, Term: 2, Id: 200},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWait(nil)
			r, _ := w.register(testReceipt("slot", 1, 1, 1, 100))
			w.didApply("slot", 1, []types.Log{log})
			if err := result(t, r); !errors.Is(err, types.ErrProposalSuperseded) {
				t.Fatalf("expected superseded, got %v", err)
			}
		})
	}
	// Forwarded receipts do not know the leader's term; Id decides.
	w := newWait(nil)
	fr := testReceipt("slot", 1, 0, 1, 100)
	fr.forwarded = true
	r, _ := w.register(fr)
	w.didApply("slot", 1, applied(1, 7, 100))
	if err := result(t, r); err != nil {
		t.Fatalf("forwarded receipt with matching id: %v", err)
	}
}

// A batch whose range is split across apply batches is verified from storage,
// entry by entry; the applied batch holding only its last entry is not enough.
func TestReceiptBatchCompletesFromRange(t *testing.T) {
	stored := map[uint64]types.Log{4: {Index: 4, Term: 3, Id: 400}, 5: {Index: 5, Term: 3, Id: 500}, 6: {Index: 6, Term: 3, Id: 600}}
	w := newWait(func(_ string, index uint64) (types.Log, bool, error) {
		log, ok := stored[index]
		return log, ok, nil
	})
	r, _ := w.register(receipt{raftKey: "slot", seq: 1, term: 3, first: 4, last: 6, logs: []types.Log{{Index: 4, Id: 400}, {Index: 5, Id: 500}, {Index: 6, Id: 600}}})
	w.didApply("slot", 5, []types.Log{{Index: 4, Term: 3, Id: 400}, {Index: 5, Term: 3, Id: 500}})
	assertNoResult(t, r)
	w.didApply("slot", 7, []types.Log{{Index: 6, Term: 3, Id: 600}, {Index: 7, Term: 3, Id: 700}})
	if err := result(t, r); err != nil {
		t.Fatalf("batch: %v", err)
	}
	// The same split with a different earlier entry in storage is superseded.
	stored[4] = types.Log{Index: 4, Term: 3, Id: 999}
	r, _ = w.register(receipt{raftKey: "slot", seq: 2, term: 3, first: 4, last: 6, logs: []types.Log{{Index: 4, Id: 400}, {Index: 5, Id: 500}, {Index: 6, Id: 600}}})
	w.verifyStored(r)
	if err := result(t, r); !errors.Is(err, types.ErrProposalSuperseded) {
		t.Fatalf("expected superseded, got %v", err)
	}
}

// Every entry of a batch is checked, Id and Data, for forwarded and
// leader-local receipts alike: a matching last Id does not vouch for the rest.
func TestReceiptBatchVerifiesEveryEntry(t *testing.T) {
	want := []types.Log{{Index: 4, Id: 400, Data: []byte("a")}, {Index: 5, Id: 500, Data: []byte("b")}}
	for name, appliedLogs := range map[string][]types.Log{
		"earlier id":   {{Index: 4, Term: 3, Id: 999, Data: []byte("a")}, {Index: 5, Term: 3, Id: 500, Data: []byte("b")}},
		"earlier data": {{Index: 4, Term: 3, Id: 400, Data: []byte("x")}, {Index: 5, Term: 3, Id: 500, Data: []byte("b")}},
		"last data":    {{Index: 4, Term: 3, Id: 400, Data: []byte("a")}, {Index: 5, Term: 3, Id: 500, Data: []byte("x")}},
		"earlier term": {{Index: 4, Term: 2, Id: 400, Data: []byte("a")}, {Index: 5, Term: 3, Id: 500, Data: []byte("b")}},
	} {
		for _, forwarded := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/forwarded=%v", name, forwarded), func(t *testing.T) {
				for _, path := range []string{"applied", "stored"} {
					stored := map[uint64]types.Log{}
					for _, l := range appliedLogs {
						stored[l.Index] = l
					}
					w := newWait(func(_ string, index uint64) (types.Log, bool, error) {
						log, ok := stored[index]
						return log, ok, nil
					})
					r, _ := w.register(receipt{raftKey: "slot", seq: 1, term: 3, first: 4, last: 5, logs: want, forwarded: forwarded})
					if path == "applied" {
						w.didApply("slot", 5, appliedLogs)
					} else {
						w.verifyStored(r)
					}
					err := result(t, r)
					if name == "earlier term" && forwarded {
						// Forwarded receipts do not know the leader's term.
						if err != nil {
							t.Fatalf("%s: forwarded receipt with matching entries: %v", path, err)
						}
						continue
					}
					if !errors.Is(err, types.ErrProposalSuperseded) {
						t.Fatalf("%s: expected superseded, got %v", path, err)
					}
				}
			})
		}
	}
	// An exact match succeeds through both paths.
	w := newWait(nil)
	r, _ := w.register(receipt{raftKey: "slot", seq: 1, term: 3, first: 4, last: 5, logs: want, forwarded: true})
	w.didApply("slot", 5, []types.Log{{Index: 4, Term: 9, Id: 400, Data: []byte("a")}, {Index: 5, Term: 9, Id: 500, Data: []byte("b")}})
	if err := result(t, r); err != nil {
		t.Fatalf("exact match: %v", err)
	}
	// A receipt missing its submitted entries is never a success.
	r, _ = w.register(receipt{raftKey: "slot", seq: 2, term: 3, first: 6, last: 7, logs: want[:1], forwarded: true})
	w.didApply("slot", 7, []types.Log{{Index: 6, Id: 400, Data: []byte("a")}, {Index: 7, Id: 500, Data: []byte("b")}})
	if err := result(t, r); !errors.Is(err, types.ErrProposalOutcomeUnknown) {
		t.Fatalf("incomplete receipt: %v", err)
	}
}

func TestReceiptWithoutAppliedLogs(t *testing.T) {
	stored := map[uint64]types.Log{1: {Index: 1, Term: 1, Id: 100}, 2: {Index: 2, Term: 1, Id: 999}}
	readErr := errors.New("read failed")
	var failRead bool
	w := newWait(func(key string, index uint64) (types.Log, bool, error) {
		if failRead {
			return types.Log{}, false, readErr
		}
		log, ok := stored[index]
		return log, ok, nil
	})

	// Leader-local receipts complete from uniqueness and the fence.
	local, _ := w.register(testReceipt("slot", 1, 1, 1, 100))
	w.didApply("slot", 1, nil)
	if err := result(t, local); err != nil {
		t.Fatalf("local NotNeedApplied receipt: %v", err)
	}

	forwarded := func(seq, index, id uint64) *receiptWaiter {
		r := testReceipt("slot", seq, 0, index, id)
		r.forwarded = true
		waiter, _ := w.register(r)
		return waiter
	}
	ok := forwarded(2, 1, 100)
	w.didApply("slot", 1, nil)
	if err := result(t, ok); err != nil {
		t.Fatalf("forwarded receipt, stored id matches: %v", err)
	}
	other := forwarded(3, 2, 200)
	w.didApply("slot", 2, nil)
	if err := result(t, other); !errors.Is(err, types.ErrProposalSuperseded) {
		t.Fatalf("forwarded receipt, stored id differs: %v", err)
	}
	missing := forwarded(4, 3, 300)
	w.didApply("slot", 3, nil)
	if err := result(t, missing); !errors.Is(err, types.ErrProposalOutcomeUnknown) {
		t.Fatalf("forwarded receipt, entry missing: %v", err)
	}
	failRead = true
	unreadable := forwarded(5, 1, 100)
	w.didApply("slot", 1, nil)
	if err := result(t, unreadable); !errors.Is(err, types.ErrProposalOutcomeUnknown) || !errors.Is(err, readErr) {
		t.Fatalf("forwarded receipt, read failed: %v", err)
	}
	if n := w.pending(); n != 0 {
		t.Fatalf("%d receipts retained", n)
	}
}

// A forwarded receipt registered after its range was applied is reported
// so the caller verifies it; registration and apply share the bucket lock.
func TestReceiptAppliedBeforeRegistration(t *testing.T) {
	w := newWait(nil)
	w.didApply("slot", 5, nil)
	fr := testReceipt("slot", 1, 0, 3, 300)
	fr.forwarded = true
	r, appliedBefore := w.register(fr)
	if !appliedBefore {
		t.Fatal("receipt for an applied index not reported")
	}
	assertNoResult(t, r)
	later := fr
	later.seq, later.first, later.last = 2, 6, 6
	if _, appliedBefore = w.register(later); appliedBefore {
		t.Fatal("receipt for an unapplied index reported as applied")
	}
	w.removeKey("slot", errors.New("removed"))
	if _, appliedBefore = w.register(fr); appliedBefore {
		t.Fatal("applied index kept after the key was removed")
	}
}

func TestReceiptFenceCompletesOnlyEndedTerms(t *testing.T) {
	w := newWait(nil)
	term1, _ := w.register(testReceipt("slot", 1, 1, 1, 100))
	term2, _ := w.register(testReceipt("slot", 2, 2, 2, 200))
	other, _ := w.register(testReceipt("other", 3, 1, 1, 300))
	fr := testReceipt("slot", 4, 0, 3, 400)
	fr.forwarded = true
	forwarded, _ := w.register(fr)

	w.fence("slot", 1)
	if err := result(t, term1); !errors.Is(err, types.ErrProposalOutcomeUnknown) {
		t.Fatalf("fenced receipt: %v", err)
	}
	assertNoResult(t, term2)
	assertNoResult(t, other)
	assertNoResult(t, forwarded)

	// A replacement entry applied after the fence cannot complete term1.
	w.didApply("slot", 1, applied(1, 2, 999))
	assertNoResult(t, term1)
	if res, completed := w.detach(term1); !completed || !errors.Is(res, types.ErrProposalOutcomeUnknown) {
		t.Fatalf("detach after fence: %v %v", res, completed)
	}
	if n := w.pending(); n != 3 {
		t.Fatalf("pending receipts: %d, want 3", n)
	}
}

func TestReceiptRemoveKeyAndCompleteAll(t *testing.T) {
	w := newWait(nil)
	a, _ := w.register(testReceipt("a", 1, 1, 1, 1))
	b, _ := w.register(testReceipt("b", 2, 1, 1, 2))
	gone := errors.New("gone")
	w.removeKey("a", gone)
	if err := result(t, a); !errors.Is(err, gone) {
		t.Fatalf("removeKey: %v", err)
	}
	assertNoResult(t, b)
	w.completeAll(gone)
	if err := result(t, b); !errors.Is(err, gone) {
		t.Fatalf("completeAll: %v", err)
	}
	if n := w.pending(); n != 0 {
		t.Fatalf("%d receipts retained", n)
	}
}

// Keys that hash to one bucket share its map and lock, which is where a
// receipt could be completed for the wrong proposal.
func sameBucketKeys(t *testing.T, w *wait, n int) []string {
	t.Helper()
	keys := []string{"slot-0"}
	target := w.bucket(keys[0])
	for i := 1; len(keys) < n && i < 100000; i++ {
		key := fmt.Sprintf("slot-%d", i)
		if w.bucket(key) == target {
			keys = append(keys, key)
		}
	}
	if len(keys) < n {
		t.Fatalf("found %d keys for one bucket, want %d", len(keys), n)
	}
	return keys
}

func TestReceiptRegisterApplyDetachStress(t *testing.T) {
	const iterations = 1000
	w := newWait(nil)
	keys := sameBucketKeys(t, w, 4)
	seq := uint64(0)
	for i := 0; i < iterations; i++ {
		index := uint64(i + 1)
		waiters := make([]*receiptWaiter, len(keys))
		for k, key := range keys {
			seq++
			waiters[k], _ = w.register(testReceipt(key, seq, 1, index, seq))
		}

		// An apply below every receipt's index must not complete any of them.
		for k, key := range keys {
			w.didApply(key, index-1, nil)
			assertNoResult(t, waiters[k])
		}

		// Even keys are applied while odd keys detach as timed out, so
		// completion and detach contend on the same bucket.
		var wg sync.WaitGroup
		for k := 0; k < len(keys); k += 2 {
			wg.Add(1)
			go func(key string, r *receiptWaiter) {
				defer wg.Done()
				w.didApply(key, index, applied(index, 1, r.lastLogId()))
			}(keys[k], waiters[k])
		}
		for k := 1; k < len(keys); k += 2 {
			assertNoResult(t, waiters[k])
			if _, completed := w.detach(waiters[k]); completed {
				t.Fatalf("iteration %d: unapplied %s reported as completed", i, keys[k])
			}
		}
		wg.Wait()
		for k := 0; k < len(keys); k += 2 {
			if err := result(t, waiters[k]); err != nil {
				t.Fatalf("iteration %d: applied %s: %v", i, keys[k], err)
			}
			if res, completed := w.detach(waiters[k]); !completed || res != nil {
				t.Fatalf("iteration %d: applied %s detached as %v %v", i, keys[k], res, completed)
			}
		}
		for k := 1; k < len(keys); k += 2 {
			assertNoResult(t, waiters[k])
		}
		if n := w.pending(); n != 0 {
			t.Fatalf("iteration %d: %d receipts retained", i, n)
		}
	}
}
