package cluster

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"go.uber.org/zap"
)

type channelConfigKey struct {
	id  string
	typ uint8
}

type configTask struct {
	revision   uint64
	busy       bool
	due, since time.Time
	failures   int
}

// The database is the recovery journal. This queue only accelerates convergence;
// overflow, lost notifications and restarts are repaired by bounded page scans.
type channelConfigReconciler struct {
	ctx                          context.Context
	cancel                       context.CancelFunc
	wg                           sync.WaitGroup
	mu                           sync.Mutex
	pending                      map[channelConfigKey]*configTask
	capacity                     int
	notify                       chan struct{}
	scanNow                      chan struct{}
	process                      func(context.Context, channelConfigKey) error
	page                         func(uint64, int) ([]wkdb.ChannelClusterConfig, error)
	owned                        func(channelConfigKey) bool
	report                       func(int, time.Duration, uint64, uint64, uint64)
	warn                         func(channelConfigKey, error)
	completed, retries, overflow uint64
}

func newChannelConfigReconciler(parent context.Context) *channelConfigReconciler {
	ctx, cancel := context.WithCancel(parent)
	return &channelConfigReconciler{ctx: ctx, cancel: cancel, pending: make(map[channelConfigKey]*configTask),
		capacity: 4096, notify: make(chan struct{}, 1), scanNow: make(chan struct{}, 1)}
}

func (r *channelConfigReconciler) add(key channelConfigKey, changed bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx.Err() != nil {
		return false
	}
	if task := r.pending[key]; task != nil {
		if changed {
			task.revision++
		}
		return true
	}
	if len(r.pending) >= r.capacity {
		r.overflow++
		r.rescan()
		return false
	}
	now := time.Now()
	r.pending[key] = &configTask{revision: 1, due: now, since: now}
	select {
	case r.notify <- struct{}{}:
	default:
	}
	return true
}

func (r *channelConfigReconciler) take() (channelConfigKey, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for key, task := range r.pending {
		if !task.busy && !now.Before(task.due) {
			task.busy = true
			return key, task.revision, true
		}
	}
	return channelConfigKey{}, 0, false
}

func (r *channelConfigReconciler) finish(key channelConfigKey, revision uint64, err error) {
	r.mu.Lock()
	task := r.pending[key]
	task.busy = false
	if err == nil && task.revision == revision {
		delete(r.pending, key)
		r.completed++
	} else {
		task.due = time.Now()
		if err != nil {
			r.retries++
			task.failures++
			delay := min(100*time.Millisecond<<min(task.failures-1, 5), 2*time.Second)
			task.due = task.due.Add(delay + time.Duration(rand.Int64N(int64(delay/4))))
			// A full queue of unavailable channels must not prevent the scanner
			// reaching healthy channels further on. Yield this slot under pressure;
			// the durable metadata will rediscover it on the next complete pass.
			if len(r.pending) >= r.capacity {
				delete(r.pending, key)
			}
		}
	}
	// Rate limit per key; transient election/application lag is expected.
	warn := err != nil && task.failures%16 == 0
	r.mu.Unlock()
	if warn && r.warn != nil {
		r.warn(key, err)
	}
}

func (r *channelConfigReconciler) start() {
	for i := 0; i < 4; i++ {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for r.ctx.Err() == nil {
				key, revision, ok := r.take()
				if !ok {
					select {
					case <-r.ctx.Done():
						return
					case <-r.notify:
					case <-ticker.C:
					}
					continue
				}
				ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
				err := r.process(ctx, key)
				cancel()
				r.finish(key, revision, err)
			}
		}()
	}
	r.wg.Add(1)
	go r.scanLoop()
}

func (r *channelConfigReconciler) stop() { r.cancel(); r.wg.Wait() }

func (r *channelConfigReconciler) rescan() {
	select {
	case r.scanNow <- struct{}{}:
	default:
	}
}

// Each tick consumes at most one page, including when the queue is full. Do not
// advance past an unqueued row. A new full pass catches edits behind the cursor.
func (r *channelConfigReconciler) scanPage(offset uint64) (uint64, bool) {
	cfgs, err := r.page(offset, 128)
	if err != nil {
		return offset, false
	}
	for _, cfg := range cfgs {
		key := channelConfigKey{cfg.ChannelId, cfg.ChannelType}
		if r.owned(key) && !r.add(key, false) {
			return offset, false
		}
		offset = cfg.Id
	}
	return offset, len(cfgs) < 128
}

func (r *channelConfigReconciler) scanLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	stats := time.NewTicker(30 * time.Second)
	defer stats.Stop()
	var offset uint64
	var next time.Time
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-r.scanNow:
			next = time.Time{}
		case <-stats.C:
			r.mu.Lock()
			oldest := time.Duration(0)
			for _, t := range r.pending {
				oldest = max(oldest, time.Since(t.since))
			}
			pending, completed, retries, overflow := len(r.pending), r.completed, r.retries, r.overflow
			r.mu.Unlock()
			if r.report != nil {
				r.report(pending, oldest, completed, retries, overflow)
			}
		case <-ticker.C:
			if time.Now().Before(next) {
				continue
			}
			var done bool
			offset, done = r.scanPage(offset)
			if done {
				offset = 0
				next = time.Now().Add(30 * time.Second)
			}
		}
	}
}

func (s *Server) initChannelConfigReconciler() {
	r := newChannelConfigReconciler(s.cancelCtx)
	r.process = s.reconcileChannelConfig
	r.page = s.db.GetChannelClusterConfigs
	r.owned = func(key channelConfigKey) bool {
		return s.cfgServer.SlotLeaderId(s.getSlotId(key.id)) == s.opts.ConfigOptions.NodeId
	}
	r.warn = func(key channelConfigKey, err error) {
		s.Warn("channel config reconciliation retry", zap.String("channelId", key.id), zap.Uint8("channelType", key.typ), zap.Error(err))
	}
	r.report = func(pending int, oldest time.Duration, completed, retries, overflow uint64) {
		s.Info("channel config reconciliation", zap.Int("pending", pending), zap.Duration("oldest", oldest),
			zap.Uint64("completed", completed), zap.Uint64("retries", retries), zap.Uint64("overflow", overflow))
	}
	s.configReconciler = r
}

func (s *Server) hintChannelConfig(id string, typ uint8) {
	if s.configReconciler != nil {
		// A repeated read observes the same durable work; it is not a new write.
		s.configReconciler.add(channelConfigKey{id, typ}, false)
	}
}

func (s *Server) onChannelConfigSaved(id string, typ uint8) {
	// Store apply runs on replicas too. Only the slot owner distributes updates.
	if s.cfgServer.SlotLeaderId(s.getSlotId(id)) == s.opts.ConfigOptions.NodeId {
		s.configReconciler.add(channelConfigKey{id, typ}, true)
	}
}

func (s *Server) reconcileChannelConfig(ctx context.Context, key channelConfigKey) error {
	cfg, err := s.loadConversationConfig(ctx, key.id, key.typ)
	if errors.Is(err, wkdb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	owner, err := s.SlotLeaderIdOfChannel(key.id, key.typ)
	if err != nil {
		return err
	}
	// A local read hint on the message leader remains useful even when its slot
	// lives elsewhere. Other replicas leave distribution to the current owner.
	if owner != s.opts.ConfigOptions.NodeId && cfg.LeaderId != s.opts.ConfigOptions.NodeId {
		return nil
	}
	if owner == s.opts.ConfigOptions.NodeId {
		// Foreground GetOrCreate may race this maintenance without sharing its
		// channelKeyLock. saveChannelConfig fences both decisions by ConfVersion
		// and slot read state; a losing decision is reloaded on the next attempt.
		cfg, err = s.maintainChannelConfig(ctx, cfg)
		if err != nil {
			return err
		}
	}
	if !validConversationConfig(cfg, key.id, key.typ) {
		return ErrConversationReadRetry
	}
	if cfg.LeaderId == s.opts.ConfigOptions.NodeId {
		_, err = s.reconcileLocalChannelConfig(ctx, cfg)
		return err
	}
	return s.requestChannelConfigReconcile(ctx, cfg)
}
