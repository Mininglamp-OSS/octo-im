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
	MaxActiveRecoveryAttempts    = 6
	recoveryBaseBackoff          = 50 * time.Millisecond
	recoveryMaxActiveBackoff     = 1600 * time.Millisecond
	recoveryCooldownBackoff      = 5 * time.Second
	maxRecoveryWorkers           = 4
	DefaultMaxRecoveryItems      = int(eventbus.MaxPostCommitItems + eventbus.MaxLegacyInboundItems)
	DefaultMaxRecoveryMetaBytes  = int64(8 * 1024 * 1024)
	perRecoveryItemBaseMetaBytes = int64(256)
)

// RecoveryStage identifies which post-commit side-effect stage is owed recovery.
type RecoveryStage uint8

const (
	RecoveryStageChannelForward RecoveryStage = 1 // Forward persisted EventChannelOnSend/Distribute to channel slot leader
	RecoveryStagePluginAfter    RecoveryStage = 2 // Stage 0: PluginPersistAfter
	RecoveryStageWebhook        RecoveryStage = 3 // Stage 1: WebhookAppendNotifyQueue
	RecoveryStageDistribute     RecoveryStage = 4 // Stage 2/3: Remote/local per-user-slot distribution
)

// EnqueueResult explicitly reports the outcome and ticket ownership disposition of RecoveryManager.EnqueueWithResult (B7).
// Ticket ownership moves to RecoveryManager ONLY on EnqueueInserted or EnqueueMerged; on any failure result
// (EnqueueBackpressure, EnqueueStopped, EnqueueInvalid), ev.Ticket remains untouched on the caller's Event.
type EnqueueResult uint8

const (
	EnqueueInserted EnqueueResult = iota + 1
	EnqueueMerged
	EnqueueBackpressure
	EnqueueStopped
	EnqueueInvalid
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
	EnqueuedAt        time.Time
	NextRetryAt       time.Time
	MetadataBytes     int64
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

type RecoveryDetailedMetrics struct {
	ActiveChannels        int
	QueuedItems           int
	InFlightItems         int
	QueuedMetadataBytes   int64
	InFlightMetadataBytes int64
	EnqueueRejectedTotal  uint64
	CooldownProbesTotal   uint64
	OldestPendingAge      time.Duration
}

type RecoveryManager struct {
	mu                    sync.Mutex
	cycleMu               sync.Mutex
	entries               map[ChannelWatermarkKey]*channelRecoveryEntry
	totalItems            int
	inFlightItems         int
	inFlightChannels      int
	queuedMetadataBytes   int64
	inFlightMetadataBytes int64
	maxItems              int
	maxMetadataBytes      int64
	stopped               bool
	timer                 *time.Timer
	wakeCh                chan struct{}
	stopCh                chan struct{}
	workerCtx             context.Context
	workerCancel          context.CancelFunc
	wg                    sync.WaitGroup
	startedOnce           sync.Once
	exec                  RecoveryExecutor
	cooldownProbesTotal   atomic.Uint64
	enqueueRejectedTotal  atomic.Uint64
}

func NewRecoveryManager(exec RecoveryExecutor) *RecoveryManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &RecoveryManager{
		entries:          make(map[ChannelWatermarkKey]*channelRecoveryEntry, 64),
		maxItems:         DefaultMaxRecoveryItems,
		maxMetadataBytes: DefaultMaxRecoveryMetaBytes,
		wakeCh:           make(chan struct{}, 1),
		stopCh:           make(chan struct{}),
		workerCtx:        ctx,
		workerCancel:     cancel,
		exec:             exec,
	}
}

func (m *RecoveryManager) SetCapacityLimits(maxItems int, maxMetadataBytes int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.maxItems = maxItems
	m.maxMetadataBytes = maxMetadataBytes
	m.mu.Unlock()
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

// resolveMergedTickets merges two TicketHandles (A4):
//   - If preferred and secondary are the SAME handle (SameHandle == true), keep preferred and do NOT release secondary
//     (never inspect shared Entry.Refs() to guess same-stage duplicate retention).
//   - If preferred and secondary are DISTINCT handles (either distinct entries or distinct handles on a shared entry),
//     keep preferred and release secondary once.
func resolveMergedTickets(preferred, secondary *eventbus.PostCommitTicket) (keep, release *eventbus.PostCommitTicket) {
	if preferred != nil {
		keep = preferred
		if secondary != nil && !preferred.SameHandle(secondary) {
			release = secondary
		}
		return keep, release
	}
	return secondary, nil
}

// EnqueueWithResult enforces RecoveryManager as a mandatory budget boundary (B7):
// 1. Validates ev.Ticket is non-nil and live BEFORE cloning ev or mutating ev.Ticket.
// 2. Merges duplicate (channel, seq, stage) items in place without re-cloning SendPacket/Payload.
// 3. Accounts queued + inFlight item count and metadata bytes (including sparse slot-mask capacity).
// 4. Moves ev.Ticket ownership ONLY on EnqueueInserted or EnqueueMerged; on failure, ev.Ticket remains with the caller.
func (m *RecoveryManager) EnqueueWithResult(
	channelID string,
	channelType uint8,
	stage RecoveryStage,
	pendingSlots eventbus.SlotBitset,
	allRecipientSlots eventbus.SlotBitset,
	ev *eventbus.Event,
) EnqueueResult {
	if m == nil || ev == nil || ev.MessageSeq == 0 {
		if m != nil {
			m.enqueueRejectedTotal.Add(1)
		}
		return EnqueueInvalid
	}
	if ev.Ticket == nil || ev.Ticket.IsReleased() {
		m.enqueueRejectedTotal.Add(1)
		return EnqueueInvalid
	}
	k, ok := MakeChannelWatermarkKey(channelID, channelType)
	if !ok {
		m.enqueueRejectedTotal.Add(1)
		return EnqueueInvalid
	}
	if sp, ok := ev.Frame.(*wkproto.SendPacket); !ok || sp == nil {
		m.enqueueRejectedTotal.Add(1)
		return EnqueueInvalid
	}

	m.ensureStarted()

	now := time.Now()
	nextDue := now.Add(recoveryBaseBackoff)
	var releaseTicket *eventbus.PostCommitTicket

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		m.enqueueRejectedTotal.Add(1)
		return EnqueueStopped
	}
	if m.entries == nil {
		m.entries = make(map[ChannelWatermarkKey]*channelRecoveryEntry, 64)
	}
	entry := m.entries[k]
	var (
		idx   int
		found bool
	)
	if entry != nil {
		idx, found = findRecoveryItemIndex(entry.items, ev.MessageSeq, stage)
	}

	if found {
		existing := &entry.items[idx]
		mergedPending := existing.PendingSlots.Or(pendingSlots)
		mergedAll := existing.AllRecipientSlots.Or(allRecipientSlots)
		oldMaskBytes := existing.PendingSlots.ExtraHeapBytes() + existing.AllRecipientSlots.ExtraHeapBytes()
		newMaskBytes := mergedPending.ExtraHeapBytes() + mergedAll.ExtraHeapBytes()
		deltaBytes := newMaskBytes - oldMaskBytes
		if deltaBytes > 0 {
			if m.maxMetadataBytes > 0 && m.queuedMetadataBytes+m.inFlightMetadataBytes+deltaBytes > m.maxMetadataBytes {
				m.mu.Unlock()
				m.enqueueRejectedTotal.Add(1)
				eventbus.RecoveryBackpressureTotal.Add(1)
				return EnqueueBackpressure
			}
			chargeTarget := existing.Ticket
			if chargeTarget == nil || chargeTarget.IsReleased() {
				chargeTarget = ev.Ticket
			}
			if !chargeTarget.TryAddExtraBytes(deltaBytes) {
				m.mu.Unlock()
				m.enqueueRejectedTotal.Add(1)
				eventbus.RecoveryBackpressureTotal.Add(1)
				return EnqueueBackpressure
			}
		} else if deltaBytes < 0 && existing.Ticket != nil {
			_ = existing.Ticket.TryAddExtraBytes(deltaBytes)
		}

		incomingTicket := ev.MoveTicket()
		existing.PendingSlots = mergedPending
		existing.AllRecipientSlots = mergedAll
		existing.MetadataBytes += deltaBytes
		m.queuedMetadataBytes += deltaBytes
		if existing.Event == nil {
			compacted := eventbus.CompactSendPayloadClone(ev)
			compacted.Ticket = nil
			if compacted.Conn == nil {
				compacted.Conn = &eventbus.Conn{}
			}
			existing.Event = compacted
		} else {
			existing.Event.MergeRecoveryFlagsFrom(ev)
		}
		keep, rel := resolveMergedTickets(existing.Ticket, incomingTicket)
		existing.Ticket = keep
		releaseTicket = rel
		m.armTimerLocked(now)
		m.mu.Unlock()

		if releaseTicket != nil {
			releaseTicket.Release()
		}
		return EnqueueMerged
	}

	if m.maxItems > 0 && m.totalItems+m.inFlightItems >= m.maxItems {
		m.mu.Unlock()
		m.enqueueRejectedTotal.Add(1)
		eventbus.RecoveryBackpressureTotal.Add(1)
		return EnqueueBackpressure
	}
	clonedPending := pendingSlots.Clone()
	clonedAll := allRecipientSlots.Clone()
	maskExtraBytes := clonedPending.ExtraHeapBytes() + clonedAll.ExtraHeapBytes()
	itemMetaBytes := perRecoveryItemBaseMetaBytes + maskExtraBytes
	if m.maxMetadataBytes > 0 && m.queuedMetadataBytes+m.inFlightMetadataBytes+itemMetaBytes > m.maxMetadataBytes {
		m.mu.Unlock()
		m.enqueueRejectedTotal.Add(1)
		eventbus.RecoveryBackpressureTotal.Add(1)
		return EnqueueBackpressure
	}
	// Charge incremental mask bytes beyond the initial event.TargetSlotMask already charged in EstimatePostCommitEventBytes.
	initialEvMaskBytes := ev.TargetSlotMask.ExtraHeapBytes()
	if extraMaskDelta := maskExtraBytes - initialEvMaskBytes; extraMaskDelta > 0 {
		if !ev.Ticket.TryAddExtraBytes(extraMaskDelta) {
			m.mu.Unlock()
			m.enqueueRejectedTotal.Add(1)
			eventbus.RecoveryBackpressureTotal.Add(1)
			return EnqueueBackpressure
		}
	}

	incomingTicket := ev.MoveTicket()
	compacted := eventbus.CompactSendPayloadClone(ev)
	compacted.Ticket = nil
	if compacted.Conn == nil {
		compacted.Conn = &eventbus.Conn{}
	}

	if entry == nil {
		entry = &channelRecoveryEntry{
			key:         k,
			channelID:   channelID,
			channelType: channelType,
		}
		m.entries[k] = entry
	}
	item := RecoveryItem{
		Seq:               compacted.MessageSeq,
		Stage:             stage,
		PendingSlots:      clonedPending,
		AllRecipientSlots: clonedAll,
		Event:             compacted,
		Ticket:            incomingTicket,
		Attempts:          0,
		TotalAttempts:     0,
		EnqueuedAt:        now,
		NextRetryAt:       nextDue,
		MetadataBytes:     itemMetaBytes,
	}
	entry.items = insertRecoveryItemAt(entry.items, idx, item)
	m.totalItems++
	m.queuedMetadataBytes += itemMetaBytes
	m.armTimerLocked(now)
	m.mu.Unlock()

	return EnqueueInserted
}

func (m *RecoveryManager) Enqueue(
	channelID string,
	channelType uint8,
	stage RecoveryStage,
	pendingSlots eventbus.SlotBitset,
	allRecipientSlots eventbus.SlotBitset,
	ev *eventbus.Event,
) bool {
	res := m.EnqueueWithResult(channelID, channelType, stage, pendingSlots, allRecipientSlots, ev)
	return res == EnqueueInserted || res == EnqueueMerged
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
		var dueMetaBytes int64
		writeIdx := 0
		for i := range entry.items {
			it := entry.items[i]
			if !now.Before(it.NextRetryAt) {
				due = append(due, it)
				dueMetaBytes += it.MetadataBytes
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
		m.queuedMetadataBytes -= dueMetaBytes
		m.inFlightMetadataBytes += dueMetaBytes
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

// completeRecoveryLease re-inserts or merges failed leased items and releases completed/superseded tickets outside m.mu (closing V13-1 & A4).
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

	var batchMetaBytes int64
	for i := range b.items {
		batchMetaBytes += b.items[i].MetadataBytes
	}

	now := time.Now()
	m.mu.Lock()
	m.inFlightItems -= len(b.items)
	m.inFlightMetadataBytes -= batchMetaBytes
	if m.inFlightMetadataBytes < 0 {
		m.inFlightMetadataBytes = 0
	}
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
			fi.MetadataBytes = perRecoveryItemBaseMetaBytes + fi.PendingSlots.ExtraHeapBytes() + fi.AllRecipientSlots.ExtraHeapBytes()

			idx, found := findRecoveryItemIndex(entry.items, fi.Seq, fi.Stage)
			if !found {
				entry.items = insertRecoveryItemAt(entry.items, idx, fi)
				m.totalItems++
				m.queuedMetadataBytes += fi.MetadataBytes
				continue
			}

			concurrent := entry.items[idx]
			m.queuedMetadataBytes -= concurrent.MetadataBytes
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
			if !concurrent.EnqueuedAt.IsZero() && (fi.EnqueuedAt.IsZero() || concurrent.EnqueuedAt.Before(fi.EnqueuedAt)) {
				fi.EnqueuedAt = concurrent.EnqueuedAt
			}
			fi.MetadataBytes = perRecoveryItemBaseMetaBytes + fi.PendingSlots.ExtraHeapBytes() + fi.AllRecipientSlots.ExtraHeapBytes()
			m.queuedMetadataBytes += fi.MetadataBytes
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
	if m == nil {
		return
	}
	m.cycleMu.Lock()
	defer m.cycleMu.Unlock()

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
		select {
		case <-m.wakeCh:
		case <-m.stopCh:
			return
		}
		m.runDueCycle(m.workerCtx)
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

func (m *RecoveryManager) InspectDetailed() RecoveryDetailedMetrics {
	if m == nil {
		return RecoveryDetailedMetrics{}
	}
	now := time.Now()
	var oldest time.Time
	m.mu.Lock()
	for _, entry := range m.entries {
		for i := range entry.items {
			ts := entry.items[i].EnqueuedAt
			if !ts.IsZero() && (oldest.IsZero() || ts.Before(oldest)) {
				oldest = ts
			}
		}
	}
	var oldestAge time.Duration
	if !oldest.IsZero() && now.After(oldest) {
		oldestAge = now.Sub(oldest)
	}
	out := RecoveryDetailedMetrics{
		ActiveChannels:        len(m.entries) + m.inFlightChannels,
		QueuedItems:           m.totalItems,
		InFlightItems:         m.inFlightItems,
		QueuedMetadataBytes:   m.queuedMetadataBytes,
		InFlightMetadataBytes: m.inFlightMetadataBytes,
		EnqueueRejectedTotal:  m.enqueueRejectedTotal.Load(),
		CooldownProbesTotal:   m.cooldownProbesTotal.Load(),
		OldestPendingAge:      oldestAge,
	}
	m.mu.Unlock()
	return out
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
	m.queuedMetadataBytes = 0
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
