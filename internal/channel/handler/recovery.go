package handler

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

const (
	MaxActiveRecoveryAttempts = 6
	recoveryBaseBackoff       = 50 * time.Millisecond
	recoveryMaxActiveBackoff  = 1600 * time.Millisecond
	recoveryCooldownBackoff   = 5 * time.Second
	maxRecoveryWorkers        = 4
)

// RecoveryStage identifies which post-commit side-effect stage is owed recovery.
type RecoveryStage uint8

const (
	RecoveryStageChannelForward RecoveryStage = 1 // Forward persisted EventChannelOnSend/Distribute to channel slot leader
	RecoveryStagePluginAfter    RecoveryStage = 2 // Stage 0: PluginPersistAfter
	RecoveryStageWebhook        RecoveryStage = 3 // Stage 1: WebhookAppendNotifyQueue
	RecoveryStageDistribute     RecoveryStage = 4 // Stage 2/3: Remote/local per-user-slot distribution
)

type RecoveryItem struct {
	Seq               uint64
	Stage             RecoveryStage
	PendingSlots      eventbus.SlotBitset
	AllRecipientSlots eventbus.SlotBitset
	Event             *eventbus.Event
	Ticket            *eventbus.PostCommitTicket
	Attempts          uint8
	TotalAttempts     uint32
	NextRetryAt       time.Time
}

type channelRecoveryEntry struct {
	key         ChannelWatermarkKey
	channelID   string
	channelType uint8
	items       []RecoveryItem // Kept sorted by (Seq, Stage)
}

type leasedRecoveryBatch struct {
	key         ChannelWatermarkKey
	channelID   string
	channelType uint8
	items       []RecoveryItem
}

type RecoveryExecutor func(ctx context.Context, channelID string, channelType uint8, item RecoveryItem) (remainingSlots eventbus.SlotBitset, allSlots eventbus.SlotBitset, updatedEv *eventbus.Event, done bool)

type RecoveryManager struct {
	mu                  sync.Mutex
	entries             map[ChannelWatermarkKey]*channelRecoveryEntry
	totalItems          int
	inFlightItems       int
	inFlightChannels    int
	stopped             bool
	timer               *time.Timer
	wakeCh              chan struct{}
	stopCh              chan struct{}
	workerCtx           context.Context
	workerCancel        context.CancelFunc
	wg                  sync.WaitGroup
	startedOnce         sync.Once
	exec                RecoveryExecutor
	cooldownProbesTotal atomic.Uint64
}

func NewRecoveryManager(exec RecoveryExecutor) *RecoveryManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RecoveryManager{
		entries:      make(map[ChannelWatermarkKey]*channelRecoveryEntry, 64),
		wakeCh:       make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
		workerCtx:    ctx,
		workerCancel: cancel,
		exec:         exec,
	}
}

func (m *RecoveryManager) ensureStarted() {
	if m == nil {
		return
	}
	m.startedOnce.Do(func() {
		m.wg.Add(1)
		go m.loop()
	})
}

func backoffForRecoveryAttempt(attempts uint8) time.Duration {
	if attempts >= MaxActiveRecoveryAttempts {
		return recoveryCooldownBackoff
	}
	shift := attempts
	if shift > 5 {
		shift = 5
	}
	d := recoveryBaseBackoff << shift
	if d > recoveryMaxActiveBackoff {
		return recoveryMaxActiveBackoff
	}
	return d
}

func findRecoveryItemIndex(items []RecoveryItem, seq uint64, stage RecoveryStage) (idx int, found bool) {
	idx = sort.Search(len(items), func(i int) bool {
		if items[i].Seq != seq {
			return items[i].Seq > seq
		}
		return items[i].Stage >= stage
	})
	if idx > 0 && items[idx-1].Seq == seq && items[idx-1].Stage == stage {
		return idx - 1, true
	}
	if idx < len(items) && items[idx].Seq == seq && items[idx].Stage == stage {
		return idx, true
	}
	return idx, false
}

func insertRecoveryItemAt(items []RecoveryItem, idx int, item RecoveryItem) []RecoveryItem {
	items = append(items, RecoveryItem{})
	copy(items[idx+1:], items[idx:])
	items[idx] = item
	return items
}

func resolveMergedTickets(preferred, secondary *eventbus.PostCommitTicket) (keep, release *eventbus.PostCommitTicket) {
	if preferred != nil {
		keep = preferred
		if secondary != nil && (secondary != preferred || secondary.Refs() > 1) {
			release = secondary
		}
		return keep, release
	}
	return secondary, nil
}

// Enqueue transfers ownership of ev.Ticket into RecoveryManager (clearing ev.Ticket on the caller's Event if still set)
// or releases any redundant ticket safely outside m.mu if merged with an existing item or rejected after shutdown.
func (m *RecoveryManager) Enqueue(
	channelID string,
	channelType uint8,
	stage RecoveryStage,
	pendingSlots eventbus.SlotBitset,
	allRecipientSlots eventbus.SlotBitset,
	ev *eventbus.Event,
) bool {
	if m == nil || ev == nil || ev.MessageSeq == 0 {
		if ev != nil && ev.Ticket != nil {
			ev.Ticket.Release()
			ev.Ticket = nil
		}
		return false
	}
	incomingTicket := ev.Ticket
	ev.Ticket = nil

	k, ok := MakeChannelWatermarkKey(channelID, channelType)
	if !ok {
		if incomingTicket != nil {
			incomingTicket.Release()
		}
		return false
	}

	compacted := eventbus.CompactSendPayloadClone(ev)
	compacted.Ticket = nil
	if compacted.Conn == nil {
		compacted.Conn = &eventbus.Conn{}
	}
	if sp, ok := compacted.Frame.(*wkproto.SendPacket); !ok || sp == nil {
		if incomingTicket != nil {
			incomingTicket.Release()
		}
		return false
	}

	m.ensureStarted()

	now := time.Now()
	nextDue := now.Add(recoveryBaseBackoff)
	var releaseTicket *eventbus.PostCommitTicket

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		if incomingTicket != nil {
			incomingTicket.Release()
		}
		return false
	}
	if m.entries == nil {
		m.entries = make(map[ChannelWatermarkKey]*channelRecoveryEntry, 64)
	}
	entry := m.entries[k]
	if entry == nil {
		entry = &channelRecoveryEntry{
			key:         k,
			channelID:   channelID,
			channelType: channelType,
		}
		m.entries[k] = entry
	}
	idx, found := findRecoveryItemIndex(entry.items, compacted.MessageSeq, stage)
	if found {
		existing := &entry.items[idx]
		existing.PendingSlots = existing.PendingSlots.Or(pendingSlots)
		existing.AllRecipientSlots = existing.AllRecipientSlots.Or(allRecipientSlots)
		if existing.Event == nil {
			existing.Event = compacted
		} else {
			existing.Event.MergeRecoveryFlagsFrom(compacted)
		}
		keep, rel := resolveMergedTickets(existing.Ticket, incomingTicket)
		existing.Ticket = keep
		releaseTicket = rel
	} else {
		item := RecoveryItem{
			Seq:               compacted.MessageSeq,
			Stage:             stage,
			PendingSlots:      pendingSlots.Clone(),
			AllRecipientSlots: allRecipientSlots.Clone(),
			Event:             compacted,
			Ticket:            incomingTicket,
			Attempts:          0,
			TotalAttempts:     0,
			NextRetryAt:       nextDue,
		}
		entry.items = insertRecoveryItemAt(entry.items, idx, item)
		m.totalItems++
	}
	m.armTimerLocked(now)
	m.mu.Unlock()

	if releaseTicket != nil {
		releaseTicket.Release()
	}
	return true
}

func (m *RecoveryManager) armTimerLocked(now time.Time) {
	if m.stopped {
		return
	}
	var earliest time.Time
	hasAny := false
	for _, entry := range m.entries {
		for i := range entry.items {
			due := entry.items[i].NextRetryAt
			if !hasAny || due.Before(earliest) {
				earliest = due
				hasAny = true
			}
		}
	}
	if !hasAny {
		if m.timer != nil {
			m.timer.Stop()
			m.timer = nil
		}
		return
	}
	delay := earliest.Sub(now)
	if delay < 0 {
		delay = 0
	}
	if m.timer != nil {
		m.timer.Stop()
	}
	m.timer = time.AfterFunc(delay, func() {
		select {
		case m.wakeCh <- struct{}{}:
		default:
		}
	})
}

// leaseReadyBatchesLocked detaches ONLY due items (!now.Before(it.NextRetryAt)) per V12-1,
// preserving not-yet-due sibling items and their backoff state untouched.
func (m *RecoveryManager) leaseReadyBatchesLocked(now time.Time) []leasedRecoveryBatch {
	if m.stopped || len(m.entries) == 0 {
		return nil
	}
	var batches []leasedRecoveryBatch
	for k, entry := range m.entries {
		var due []RecoveryItem
		writeIdx := 0
		for i := range entry.items {
			it := entry.items[i]
			if !now.Before(it.NextRetryAt) {
				due = append(due, it)
			} else {
				entry.items[writeIdx] = it
				writeIdx++
			}
		}
		if len(due) == 0 {
			continue
		}
		for i := writeIdx; i < len(entry.items); i++ {
			entry.items[i] = RecoveryItem{}
		}
		entry.items = entry.items[:writeIdx]
		if len(entry.items) == 0 {
			delete(m.entries, k)
		}
		m.totalItems -= len(due)
		m.inFlightItems += len(due)
		m.inFlightChannels++
		batches = append(batches, leasedRecoveryBatch{
			key:         k,
			channelID:   entry.channelID,
			channelType: entry.channelType,
			items:       due,
		})
	}
	m.armTimerLocked(now)
	return batches
}

// completeRecoveryLease re-inserts or merges failed leased items and releases completed/superseded tickets outside m.mu (closing V13-1).
func (m *RecoveryManager) completeRecoveryLease(
	b leasedRecoveryBatch,
	failedItems []RecoveryItem,
	succeededTickets []*eventbus.PostCommitTicket,
) {
	ticketsToRelease := make([]*eventbus.PostCommitTicket, 0, len(succeededTickets)+len(failedItems))
	for _, tk := range succeededTickets {
		if tk != nil {
			ticketsToRelease = append(ticketsToRelease, tk)
		}
	}

	now := time.Now()
	m.mu.Lock()
	m.inFlightItems -= len(b.items)
	m.inFlightChannels--

	if m.stopped {
		for i := range failedItems {
			if failedItems[i].Ticket != nil {
				ticketsToRelease = append(ticketsToRelease, failedItems[i].Ticket)
				failedItems[i].Ticket = nil
			}
		}
		m.mu.Unlock()
		for _, tk := range ticketsToRelease {
			tk.Release()
		}
		return
	}

	if len(failedItems) > 0 {
		entry := m.entries[b.key]
		if entry == nil {
			entry = &channelRecoveryEntry{
				key:         b.key,
				channelID:   b.channelID,
				channelType: b.channelType,
			}
			m.entries[b.key] = entry
		}
		for i := range failedItems {
			fi := failedItems[i]
			fi.TotalAttempts++
			if fi.Attempts < MaxActiveRecoveryAttempts {
				fi.Attempts++
			}
			if fi.Attempts >= MaxActiveRecoveryAttempts {
				m.cooldownProbesTotal.Add(1)
			}
			fi.NextRetryAt = now.Add(backoffForRecoveryAttempt(fi.Attempts))

			idx, found := findRecoveryItemIndex(entry.items, fi.Seq, fi.Stage)
			if !found {
				entry.items = insertRecoveryItemAt(entry.items, idx, fi)
				m.totalItems++
				continue
			}

			// V13-1 fix: concurrent duplicate item arrived while fi was leased.
			// Keep exactly one live ticket on entry.items[idx] and release the superseded ticket outside m.mu.
			concurrent := entry.items[idx]
			keepTicket, relTicket := resolveMergedTickets(fi.Ticket, concurrent.Ticket)
			if relTicket != nil {
				ticketsToRelease = append(ticketsToRelease, relTicket)
			}
			mergedEv := fi.Event
			if mergedEv == nil {
				mergedEv = concurrent.Event
			} else if concurrent.Event != nil {
				mergedEv.MergeRecoveryFlagsFrom(concurrent.Event)
			}
			fi.Event = mergedEv
			fi.PendingSlots = concurrent.PendingSlots.Or(fi.PendingSlots)
			fi.AllRecipientSlots = concurrent.AllRecipientSlots.Or(fi.AllRecipientSlots)
			fi.Ticket = keepTicket
			entry.items[idx] = fi
		}
	}
	m.armTimerLocked(now)
	m.mu.Unlock()

	for _, tk := range ticketsToRelease {
		tk.Release()
	}
}

func (m *RecoveryManager) executeBatch(ctx context.Context, b leasedRecoveryBatch) {
	var failed []RecoveryItem
	var succeededTickets []*eventbus.PostCommitTicket
	for i := range b.items {
		it := b.items[i]
		if ctx.Err() != nil || m.exec == nil {
			failed = append(failed, it)
			continue
		}
		remSlots, allSlots, updatedEv, done := m.exec(ctx, b.channelID, b.channelType, it)
		if done {
			if it.Ticket != nil {
				succeededTickets = append(succeededTickets, it.Ticket)
				it.Ticket = nil
			}
			continue
		}
		it.PendingSlots = remSlots
		if !allSlots.IsZero() {
			it.AllRecipientSlots = it.AllRecipientSlots.Or(allSlots)
		}
		if updatedEv != nil {
			updatedEv.Ticket = nil
			it.Event = updatedEv
		}
		failed = append(failed, it)
	}
	m.completeRecoveryLease(b, failed, succeededTickets)
}

func (m *RecoveryManager) runDueCycle(ctx context.Context) {
	m.mu.Lock()
	batches := m.leaseReadyBatchesLocked(time.Now())
	m.mu.Unlock()
	if len(batches) == 0 {
		return
	}
	if len(batches) == 1 {
		m.executeBatch(ctx, batches[0])
		return
	}
	sem := make(chan struct{}, maxRecoveryWorkers)
	var batchWG sync.WaitGroup
	for _, b := range batches {
		batch := b
		sem <- struct{}{}
		batchWG.Add(1)
		go func() {
			defer func() {
				<-sem
				batchWG.Done()
			}()
			m.executeBatch(ctx, batch)
		}()
	}
	batchWG.Wait()
}

func (m *RecoveryManager) loop() {
	defer m.wg.Done()
	for {
		m.runDueCycle(m.workerCtx)
		select {
		case <-m.wakeCh:
		case <-m.stopCh:
			return
		}
	}
}

// NotifyExternalWakeup immediately wakes items in CooldownProbe or pending retry upon cluster topology or slot-leader change.
func (m *RecoveryManager) NotifyExternalWakeup() {
	if m == nil {
		return
	}
	now := time.Now()
	hasItems := false
	m.mu.Lock()
	if !m.stopped && len(m.entries) > 0 {
		for _, entry := range m.entries {
			for i := range entry.items {
				if entry.items[i].Attempts >= MaxActiveRecoveryAttempts {
					entry.items[i].Attempts = 0
				}
				if entry.items[i].NextRetryAt.After(now) {
					entry.items[i].NextRetryAt = now
				}
				hasItems = true
			}
		}
		if hasItems && m.timer != nil {
			m.timer.Stop()
			m.timer = nil
		}
	}
	m.mu.Unlock()
	if hasItems {
		m.ensureStarted()
		select {
		case m.wakeCh <- struct{}{}:
		default:
		}
	}
}

func (m *RecoveryManager) Inspect() (channels int, activeItems int, cooldownProbes uint64) {
	if m == nil {
		return 0, 0, 0
	}
	m.mu.Lock()
	channels = len(m.entries) + m.inFlightChannels
	activeItems = m.totalItems + m.inFlightItems
	m.mu.Unlock()
	return channels, activeItems, m.cooldownProbesTotal.Load()
}

func (m *RecoveryManager) DrainAndStop(timeout time.Duration) {
	if m == nil {
		return
	}
	if timeout > 0 {
		now := time.Now()
		m.mu.Lock()
		hasPending := !m.stopped && len(m.entries) > 0
		if hasPending {
			for _, entry := range m.entries {
				for i := range entry.items {
					entry.items[i].NextRetryAt = now
				}
			}
		}
		m.mu.Unlock()
		if hasPending {
			drainCtx, cancel := context.WithTimeout(context.Background(), timeout)
			m.runDueCycle(drainCtx)
			cancel()
		}
	}

	var ticketsToRelease []*eventbus.PostCommitTicket
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
		if m.timer != nil {
			m.timer.Stop()
			m.timer = nil
		}
		close(m.stopCh)
		m.workerCancel()
	}
	for k, entry := range m.entries {
		for i := range entry.items {
			if entry.items[i].Ticket != nil {
				ticketsToRelease = append(ticketsToRelease, entry.items[i].Ticket)
				entry.items[i].Ticket = nil
			}
		}
		delete(m.entries, k)
	}
	m.totalItems = 0
	m.mu.Unlock()

	for _, tk := range ticketsToRelease {
		tk.Release()
	}

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	waitBudget := timeout
	if waitBudget <= 0 || waitBudget > 2*time.Second {
		waitBudget = 2 * time.Second
	}
	select {
	case <-done:
	case <-time.After(waitBudget):
	}
}
