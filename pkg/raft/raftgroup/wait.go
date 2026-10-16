package raftgroup

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

// receipt identifies one admitted proposal. seq is unique within a RaftGroup
// and is the waiter identity; term and the index range were assigned by the
// event loop (or by the leader, for forwarded receipts). logs holds the
// submitted Id and Data for every index in [first, last]; each applied entry
// in the range is checked against it.
type receipt struct {
	raftKey string
	seq     uint64
	term    uint32
	first   uint64
	last    uint64
	logs    []types.Log
	// forwarded receipts come from another node's admission. Their term is not
	// known here, so they are verified by Id and Data only and are not fenced.
	forwarded bool
}

// newReceiptLogs pairs the submitted requests with the admitted indexes.
func newReceiptLogs(reqs []types.ProposeReq, first uint64) []types.Log {
	logs := make([]types.Log, len(reqs))
	for i, req := range reqs {
		logs[i] = types.Log{Id: req.Id, Index: first + uint64(i), Data: req.Data}
	}
	return logs
}

func (r receipt) lastLogId() uint64 {
	if len(r.logs) == 0 {
		return 0
	}
	return r.logs[len(r.logs)-1].Id
}

func (r receipt) String() string {
	return fmt.Sprintf("key:%s seq:%d term:%d range:[%d,%d] forwarded:%v", r.raftKey, r.seq, r.term, r.first, r.last, r.forwarded)
}

// receiptWaiter is completed exactly once, under its bucket lock, either by
// apply, fence, Stop/RemoveRaft or by the caller detaching it. doneC has
// capacity 1 and receives the single result; it is never closed.
type receiptWaiter struct {
	receipt
	doneC     chan error
	done      bool
	verifying bool
	err       error
}

// readLogFunc reads the stored log at index for raftKey; ok is false when the
// log does not exist.
type readLogFunc func(raftKey string, index uint64) (log types.Log, ok bool, err error)

type wait struct {
	buckets              []*waitBucket
	readLog              readLogFunc
	dispatchVerification func(*receiptWaiter)
}

func newWait(readLog readLogFunc) *wait {
	count := 100
	w := &wait{
		buckets: make([]*waitBucket, count),
		readLog: readLog,
	}
	for i := 0; i < count; i++ {
		w.buckets[i] = newWaitBucket(i)
	}
	return w
}

func (m *wait) bucket(key string) *waitBucket {
	return m.buckets[int(fnv32(key)%uint32(len(m.buckets)))]
}

// register adds a pending waiter for r. appliedBefore reports that apply had
// already reached r.last when it was registered; didApply will not visit the
// waiter for that range, so the caller must verify it with verifyStored. The
// event loop registers leader-local receipts before their range can be
// applied, so this only happens to forwarded receipts.
func (m *wait) register(r receipt) (w *receiptWaiter, appliedBefore bool) {
	w = &receiptWaiter{receipt: r, doneC: make(chan error, 1)}
	b := m.bucket(r.raftKey)
	b.mu.Lock()
	defer b.mu.Unlock()
	keyWaiters := b.waiters[r.raftKey]
	if keyWaiters == nil {
		keyWaiters = make(map[uint64]*receiptWaiter)
		b.waiters[r.raftKey] = keyWaiters
	}
	keyWaiters[r.seq] = w
	if b.observe != nil {
		b.observe(r, "register", nil)
	}
	return w, b.applied[r.raftKey] >= r.last
}

// didApply completes the waiters of key whose last index is covered by
// appliedIndex. logs are the entries just applied, or nil when the group does
// not read logs at apply time (NotNeedApplied).
func (m *wait) didApply(key string, appliedIndex uint64, logs []types.Log) {
	b := m.bucket(key)
	var unverified []*receiptWaiter
	b.mu.Lock()
	if appliedIndex > b.applied[key] {
		b.applied[key] = appliedIndex
	}
	for _, w := range b.waiters[key] {
		if w.last > appliedIndex {
			continue
		}
		if covers(logs, w.first, w.last) {
			b.completeLocked(w, verifyReceipt(w.receipt, logs))
			continue
		}
		if logs == nil && !w.forwarded {
			// Leader-local receipt without logs: only this proposal was
			// admitted at (term, last), and a fence completes the receipt
			// before any other entry can replace it.
			b.completeLocked(w, nil)
			continue
		}
		unverified = append(unverified, w)
	}
	b.mu.Unlock()
	// Dispatch identity reads outside the bucket lock and apply path. The
	// bucket serializes dispatch, completion and terminal detachment.
	for _, w := range unverified {
		m.verifyStored(w)
	}
}

// verifyStored completes w from the stored entries of its range. It is used
// when the applied batch does not cover the range, e.g. a forwarded receipt
// registered after its range was applied.
func (m *wait) verifyStored(w *receiptWaiter) {
	b := m.bucket(w.raftKey)
	b.mu.Lock()
	if w.done || w.verifying {
		b.mu.Unlock()
		return
	}
	w.verifying = true
	b.mu.Unlock()
	if m.dispatchVerification != nil {
		m.dispatchVerification(w)
		return
	}
	m.readReceipt(w)
}

func (m *wait) readReceipt(w *receiptWaiter) {
	result := m.readRange(w.receipt)
	b := m.bucket(w.raftKey)
	b.mu.Lock()
	b.completeLocked(w, result)
	b.mu.Unlock()
}

// readRange point-reads every index of r's range and verifies it. Any missing
// or unreadable entry leaves the outcome unknown.
func (m *wait) readRange(r receipt) error {
	if m.readLog == nil {
		return proposalOutcomeUnknown(fmt.Errorf("applied log %d is not readable", r.last))
	}
	logs := make([]types.Log, 0, r.last-r.first+1)
	for index := r.first; index <= r.last; index++ {
		log, ok, err := m.readLog(r.raftKey, index)
		if err != nil {
			return proposalOutcomeUnknown(err)
		}
		if !ok {
			return proposalOutcomeUnknown(fmt.Errorf("applied log %d is not readable", index))
		}
		logs = append(logs, log)
	}
	return verifyReceipt(r, logs)
}

// detach removes w on behalf of a caller that stops waiting. If w was already
// completed, its result is returned with completed=true.
func (m *wait) detach(w *receiptWaiter) (result error, completed bool) {
	b := m.bucket(w.raftKey)
	b.mu.Lock()
	defer b.mu.Unlock()
	if w.done {
		return w.err, true
	}
	b.removeLocked(w)
	w.done = true
	if b.observe != nil {
		b.observe(w.receipt, "detach", nil)
	}
	return nil, false
}

// fence completes every pending leader-local receipt of key admitted in term
// or earlier as ErrProposalOutcomeUnknown: the node no longer leads that term,
// so a later entry at the same index must not complete it.
func (m *wait) fence(key string, term uint32) {
	b := m.bucket(key)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, w := range b.waiters[key] {
		if !w.forwarded && w.term <= term {
			b.completeLocked(w, proposalOutcomeUnknown(fmt.Errorf("leadership of term %d ended", term)))
		}
	}
}

// complete completes w with err unless it already completed or detached.
func (m *wait) complete(w *receiptWaiter, err error) {
	b := m.bucket(w.raftKey)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.completeLocked(w, err)
}

// removeKey completes every pending receipt of key with err and forgets the
// key's applied index.
func (m *wait) removeKey(key string, err error) {
	b := m.bucket(key)
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, w := range b.waiters[key] {
		b.completeLocked(w, err)
	}
	delete(b.applied, key)
}

// completeAll completes every pending receipt with err.
func (m *wait) completeAll(err error) {
	for _, b := range m.buckets {
		b.mu.Lock()
		for _, keyWaiters := range b.waiters {
			for _, w := range keyWaiters {
				b.completeLocked(w, err)
			}
		}
		b.mu.Unlock()
	}
}

// pending returns the number of registered, not yet completed receipts.
func (m *wait) pending() int {
	n := 0
	for _, b := range m.buckets {
		b.mu.Lock()
		for _, keyWaiters := range b.waiters {
			n += len(keyWaiters)
		}
		b.mu.Unlock()
	}
	return n
}

// covers reports whether logs, an ascending contiguous applied batch, holds
// every index in [first, last].
func covers(logs []types.Log, first, last uint64) bool {
	return len(logs) > 0 && logs[0].Index <= first && last <= logs[len(logs)-1].Index
}

// verifyReceipt checks every entry of r's range against logs, an ascending
// range holding [r.first, r.last]. A different Id or Data at any index, or a
// different term for a leader-local receipt, is superseded; an index logs
// does not hold, or a receipt without its submitted entries, is unknown.
func verifyReceipt(r receipt, logs []types.Log) error {
	if len(r.logs) == 0 || uint64(len(r.logs)) != r.last-r.first+1 {
		return proposalOutcomeUnknown(fmt.Errorf("receipt %s does not hold its submitted entries", r))
	}
	j := sort.Search(len(logs), func(i int) bool { return logs[i].Index >= r.first })
	for _, want := range r.logs {
		if j == len(logs) || logs[j].Index != want.Index {
			return proposalOutcomeUnknown(fmt.Errorf("applied log %d unavailable for receipt %s", want.Index, r))
		}
		got := logs[j]
		if got.Id != want.Id || !bytes.Equal(got.Data, want.Data) || (!r.forwarded && got.Term != r.term) {
			return fmt.Errorf("%w: index %d holds id %d term %d, receipt %s, id %d",
				types.ErrProposalSuperseded, got.Index, got.Id, got.Term, r, want.Id)
		}
		j++
	}
	return nil
}

func fnv32(key string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	hash := offset32
	for i := 0; i < len(key); i++ {
		hash ^= int(key[i])
		hash *= prime32
	}
	return uint32(hash)
}

type waitBucket struct {
	// observe is optional and must be installed before the group starts.
	// It runs under mu and must not reenter wait.
	observe func(receipt, string, error)
	mu      sync.Mutex
	wklog.Log

	waiters map[string]map[uint64]*receiptWaiter
	// applied is the highest index didApply reported per key, read and
	// written under mu so registration and apply cannot miss each other.
	applied map[string]uint64
}

func newWaitBucket(i int) *waitBucket {
	return &waitBucket{
		Log:     wklog.NewWKLog(fmt.Sprintf("applyWait[%d]", i)),
		waiters: make(map[string]map[uint64]*receiptWaiter),
		applied: make(map[string]uint64),
	}
}

// completeLocked publishes err as w's only result and unregisters it. doneC
// has capacity 1 and is written once, so this never blocks.
func (b *waitBucket) completeLocked(w *receiptWaiter, err error) {
	if w.done {
		return
	}
	w.done = true
	w.err = err
	if b.observe != nil {
		b.observe(w.receipt, "publish", err)
	}
	b.removeLocked(w)
	w.doneC <- err
}

func (b *waitBucket) removeLocked(w *receiptWaiter) {
	keyWaiters := b.waiters[w.raftKey]
	if keyWaiters[w.seq] != w {
		return
	}
	delete(keyWaiters, w.seq)
	if len(keyWaiters) == 0 {
		delete(b.waiters, w.raftKey)
	}
}
