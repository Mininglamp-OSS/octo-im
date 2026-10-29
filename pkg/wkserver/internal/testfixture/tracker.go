package testfixture

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const maxReported = 20

// Tracker accounts message IDs between a sender and a receive callback.
//
// Lifecycle per ID: Expect before Send (delivery may race the return of
// Send), then either Cancel when Send is rejected or Received from the
// receive callback. Only expected IDs that have not been received are
// pending, so a rejected Send never leaves an obligation behind.
type Tracker struct {
	mu       sync.Mutex
	expected map[uint64]struct{}
	received map[uint64]int
	misuse   []string
	pending  int
	notify   chan struct{}
}

func NewTracker() *Tracker {
	return &Tracker{
		expected: make(map[uint64]struct{}),
		received: make(map[uint64]int),
		notify:   make(chan struct{}, 1),
	}
}

// Expect registers a completion obligation for id.
func (t *Tracker) Expect(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.expected[id]; ok {
		t.misuse = append(t.misuse, fmt.Sprintf("id %d expected twice", id))
		return
	}
	t.expected[id] = struct{}{}
	if t.received[id] == 0 {
		t.pending++
	}
}

// Cancel removes the obligation for id after its Send was rejected. If the
// id was nevertheless received it is reported as unexpected.
func (t *Tracker) Cancel(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.expected[id]; !ok {
		t.misuse = append(t.misuse, fmt.Sprintf("id %d cancelled without expectation", id))
		return
	}
	delete(t.expected, id)
	if t.received[id] == 0 {
		t.pending--
		t.signal()
	}
}

// Received records a delivery of id. Safe for concurrent use.
func (t *Tracker) Received(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.received[id]++
	if _, ok := t.expected[id]; ok && t.received[id] == 1 {
		t.pending--
		t.signal()
	}
}

func (t *Tracker) signal() {
	select {
	case t.notify <- struct{}{}:
	default:
	}
}

// Pending returns the number of expected IDs not yet received.
func (t *Tracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pending
}

// Wait blocks until every expected ID was received or timeout elapses. On
// completion it returns Verify(); on timeout it returns the full report.
func (t *Tracker) Wait(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		if t.Pending() == 0 {
			return t.Verify()
		}
		select {
		case <-t.notify:
		case <-timer.C:
			return fmt.Errorf("delivery incomplete after %s: %s", timeout, t.report())
		}
	}
}

// Verify checks exactly-once delivery: no missing, duplicate or unexpected
// IDs and no tracker misuse.
func (t *Tracker) Verify() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending == 0 && len(t.misuse) == 0 && t.cleanLocked() {
		return nil
	}
	return fmt.Errorf("delivery accounting failed: %s", t.reportLocked())
}

func (t *Tracker) cleanLocked() bool {
	for id, n := range t.received {
		if _, ok := t.expected[id]; !ok || n != 1 {
			return false
		}
	}
	return true
}

func (t *Tracker) report() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reportLocked()
}

func (t *Tracker) reportLocked() string {
	var missing, unexpected []uint64
	var duplicates []string
	for id := range t.expected {
		if t.received[id] == 0 {
			missing = append(missing, id)
		}
	}
	for id, n := range t.received {
		if _, ok := t.expected[id]; !ok {
			unexpected = append(unexpected, id)
		}
		if n > 1 {
			duplicates = append(duplicates, fmt.Sprintf("%d(x%d)", id, n))
		}
	}
	sort.Slice(unexpected, func(i, j int) bool { return unexpected[i] < unexpected[j] })
	sort.Strings(duplicates)

	var b strings.Builder
	fmt.Fprintf(&b, "expected=%d received_distinct=%d missing=%d %s duplicates=%d %s unexpected=%d %s",
		len(t.expected), len(t.received),
		len(missing), Ranges(missing),
		len(duplicates), limitStrings(duplicates),
		len(unexpected), limitIDs(unexpected))
	if len(t.misuse) > 0 {
		fmt.Fprintf(&b, " misuse=%s", limitStrings(t.misuse))
	}
	return b.String()
}

// Ranges renders ids as sorted inclusive ranges, e.g. "[0-9 12]".
func Ranges(ids []uint64) string {
	if len(ids) == 0 {
		return "[]"
	}
	sorted := append([]uint64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var parts []string
	start, prev := sorted[0], sorted[0]
	flush := func() {
		if start == prev {
			parts = append(parts, fmt.Sprintf("%d", start))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", start, prev))
		}
	}
	for _, id := range sorted[1:] {
		if id == prev+1 {
			prev = id
			continue
		}
		flush()
		start, prev = id, id
	}
	flush()
	return limitStrings(parts)
}

func limitIDs(ids []uint64) string {
	s := make([]string, len(ids))
	for i, id := range ids {
		s[i] = fmt.Sprintf("%d", id)
	}
	return limitStrings(s)
}

func limitStrings(s []string) string {
	if len(s) > maxReported {
		return fmt.Sprintf("[%s ... +%d more]", strings.Join(s[:maxReported], " "), len(s)-maxReported)
	}
	return "[" + strings.Join(s, " ") + "]"
}
