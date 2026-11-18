package handler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

const (
	DefaultMaxPendingPersistPerChannel = 2048
	DefaultMaxPendingPersistTotal      = 8192
	pendingPersistDrainTick            = 25 * time.Millisecond
)

var (
	ErrUpgradeIngressNotDrained = errors.New("rolling-upgrade precheck failed: write ingress and post-commit queues are not drained")
)

type EventDurabilityClass uint8

const (
	DurabilityTransient EventDurabilityClass = iota
	DurabilityUnpersistedDurable
	DurabilityPersisted
)

// ClassifyEventDurability explicitly separates seq=0 transient events, seq=0 unpersisted durable sends,
// and seq>0 already-persisted events so seq=0 events are never misrouted into persisted RecoveryManager (A3).
func ClassifyEventDurability(e *eventbus.Event) EventDurabilityClass {
	if e == nil {
		return DurabilityTransient
	}
	if e.MessageSeq > 0 {
		return DurabilityPersisted
	}
	if e.Type == eventbus.EventChannelOnSend {
		if sp, ok := e.Frame.(*wkproto.SendPacket); ok && sp != nil && !sp.NoPersist {
			return DurabilityUnpersistedDurable
		}
	}
	return DurabilityTransient
}

type pendingPersistItem struct {
	Event  *eventbus.Event
	Ticket *eventbus.PostCommitTicket
}

type pendingPersistChannelQueue struct {
	key         ChannelWatermarkKey
	channelID   string
	channelType uint8
	items       []pendingPersistItem
}

type PendingPersistDrainFunc func(channelID string, channelType uint8, ev *eventbus.Event) bool

type PendingPersistManager struct {
	mu            sync.Mutex
	drainMu       sync.Mutex
	queues        map[ChannelWatermarkKey]*pendingPersistChannelQueue
	totalItems    int
	maxPerChannel int
	maxTotal      int
	stopped       bool
	wakeCh        chan struct{}
	stopCh        chan struct{}
	wg            sync.WaitGroup
	startedOnce   sync.Once
	drainFn       PendingPersistDrainFunc
}

func NewPendingPersistManager(drainFn PendingPersistDrainFunc) *PendingPersistManager {
	return &PendingPersistManager{
		queues:        make(map[ChannelWatermarkKey]*pendingPersistChannelQueue, 32),
		maxPerChannel: DefaultMaxPendingPersistPerChannel,
		maxTotal:      DefaultMaxPendingPersistTotal,
		wakeCh:        make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		drainFn:       drainFn,
	}
}

func (m *PendingPersistManager) SetLimits(maxPerChannel, maxTotal int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.maxPerChannel = maxPerChannel
	m.maxTotal = maxTotal
	m.mu.Unlock()
}

func (m *PendingPersistManager) ensureStarted() {
	if m == nil {
		return
	}
	m.startedOnce.Do(func() {
		m.wg.Add(1)
		go m.loop()
	})
}

// Enqueue transfers the already-reserved TicketHandle from ev into the per-channel PendingPersist FIFO
// without re-charging or prematurely releasing the ticket (A3).
// On failure (EnqueueInvalid, EnqueueStopped, EnqueueBackpressure), ev.Ticket remains untouched on ev.
func (m *PendingPersistManager) Enqueue(channelID string, channelType uint8, ev *eventbus.Event) EnqueueResult {
	if m == nil || ev == nil || ClassifyEventDurability(ev) != DurabilityUnpersistedDurable {
		return EnqueueInvalid
	}
	if ev.Ticket == nil || ev.Ticket.IsReleased() {
		return EnqueueInvalid
	}
	k, ok := MakeChannelWatermarkKey(channelID, channelType)
	if !ok {
		return EnqueueInvalid
	}

	m.ensureStarted()

	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return EnqueueStopped
	}
	if m.maxTotal > 0 && m.totalItems >= m.maxTotal {
		m.mu.Unlock()
		eventbus.PendingPersistFullTotal.Add(1)
		return EnqueueBackpressure
	}
	q := m.queues[k]
	if q != nil && m.maxPerChannel > 0 && len(q.items) >= m.maxPerChannel {
		m.mu.Unlock()
		eventbus.PendingPersistFullTotal.Add(1)
		return EnqueueBackpressure
	}
	if q == nil {
		q = &pendingPersistChannelQueue{
			key:         k,
			channelID:   channelID,
			channelType: channelType,
		}
		m.queues[k] = q
	}
	tk := ev.MoveTicket()
	compacted := eventbus.CompactSendPayloadClone(ev)
	compacted.Ticket = nil
	if compacted.Conn == nil {
		compacted.Conn = &eventbus.Conn{}
	}
	q.items = append(q.items, pendingPersistItem{
		Event:  compacted,
		Ticket: tk,
	})
	m.totalItems++
	m.mu.Unlock()

	select {
	case m.wakeCh <- struct{}{}:
	default:
	}
	return EnqueueInserted
}

func (m *PendingPersistManager) Inspect() (channels int, activeItems int) {
	if m == nil {
		return 0, 0
	}
	m.mu.Lock()
	channels = len(m.queues)
	activeItems = m.totalItems
	m.mu.Unlock()
	return channels, activeItems
}

func (m *PendingPersistManager) RunDrainCycle() int {
	if m == nil || m.drainFn == nil {
		return 0
	}
	m.drainMu.Lock()
	defer m.drainMu.Unlock()
	m.mu.Lock()
	if m.stopped || m.totalItems == 0 {
		m.mu.Unlock()
		return 0
	}
	keys := make([]ChannelWatermarkKey, 0, len(m.queues))
	for k := range m.queues {
		keys = append(keys, k)
	}
	m.mu.Unlock()

	drained := 0
	for _, k := range keys {
		for {
			m.mu.Lock()
			if m.stopped {
				m.mu.Unlock()
				return drained
			}
			q := m.queues[k]
			if q == nil || len(q.items) == 0 {
				if q != nil {
					delete(m.queues, k)
				}
				m.mu.Unlock()
				break
			}
			head := q.items[0]
			chID := q.channelID
			chType := q.channelType
			m.mu.Unlock()

			ev := head.Event.Clone()
			ev.Ticket = head.Ticket
			if !m.drainFn(chID, chType, ev) {
				// Could not drain yet; keep head item and its TicketHandle intact at front of queue.
				break
			}

			m.mu.Lock()
			q = m.queues[k]
			if q != nil && len(q.items) > 0 && q.items[0].Ticket.SameHandle(head.Ticket) {
				q.items[0] = pendingPersistItem{}
				q.items = q.items[1:]
				m.totalItems--
				drained++
				if len(q.items) == 0 {
					delete(m.queues, k)
				}
			}
			m.mu.Unlock()
		}
	}
	return drained
}

func (m *PendingPersistManager) loop() {
	defer m.wg.Done()
	ticker := time.NewTicker(pendingPersistDrainTick)
	defer ticker.Stop()
	for {
		_ = m.RunDrainCycle()
		select {
		case <-m.wakeCh:
		case <-ticker.C:
		case <-m.stopCh:
			return
		}
	}
}

func (m *PendingPersistManager) DrainAndStop(timeout time.Duration) {
	if m == nil {
		return
	}
	if timeout > 0 {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			_, items := m.Inspect()
			if items == 0 {
				break
			}
			if m.RunDrainCycle() == 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}
	}

	var toRelease []*eventbus.PostCommitTicket
	m.mu.Lock()
	if !m.stopped {
		m.stopped = true
		close(m.stopCh)
	}
	for k, q := range m.queues {
		for i := range q.items {
			if q.items[i].Ticket != nil {
				toRelease = append(toRelease, q.items[i].Ticket)
				q.items[i].Ticket = nil
			}
		}
		delete(m.queues, k)
	}
	m.totalItems = 0
	m.mu.Unlock()

	for _, tk := range toRelease {
		tk.Release()
	}

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
	}
}

type UpgradeDrainStatus struct {
	PendingPersistItems        int
	PostCommitInflightItems    int64
	LegacyInboundInflightItems int64
	TransientInflightItems     int64
	RecoveryActiveItems        int
}

func (s UpgradeDrainStatus) IsDrained() bool {
	return s.PendingPersistItems == 0 &&
		s.PostCommitInflightItems == 0 &&
		s.LegacyInboundInflightItems == 0 &&
		s.TransientInflightItems == 0 &&
		s.RecoveryActiveItems == 0
}

func (h *Handler) InspectUpgradeDrainStatus() UpgradeDrainStatus {
	if h == nil {
		return UpgradeDrainStatus{}
	}
	_, ppItems := h.pendingPersistMgr.Inspect()
	_, recItems, _ := h.recoveryMgr.Inspect()
	_, pcI, _ := h.postCommitBudget().Stats()
	_, lgI, _ := h.legacyInboundBudget().Stats()
	_, trI, _ := h.transientBudget().Stats()
	return UpgradeDrainStatus{
		PendingPersistItems:        ppItems,
		PostCommitInflightItems:    pcI,
		LegacyInboundInflightItems: lgI,
		TransientInflightItems:     trI,
		RecoveryActiveItems:        recItems,
	}
}

// CheckUpgradeDrainReadiness verifies that all write-ingress, PendingPersist, and post-commit recovery
// obligations have drained to zero before a controlled rolling upgrade step proceeds (A3).
func (h *Handler) CheckUpgradeDrainReadiness(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	st := h.InspectUpgradeDrainStatus()
	if !st.IsDrained() {
		return fmt.Errorf(
			"%w: pendingPersist=%d postCommitInflight=%d legacyInflight=%d transientInflight=%d recoveryActive=%d",
			ErrUpgradeIngressNotDrained,
			st.PendingPersistItems,
			st.PostCommitInflightItems,
			st.LegacyInboundInflightItems,
			st.TransientInflightItems,
			st.RecoveryActiveItems,
		)
	}
	return nil
}
