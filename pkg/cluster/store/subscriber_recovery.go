package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"golang.org/x/sync/errgroup"
)

var ErrInvalidSubscriberOperation = errors.New("invalid subscriber operation")

var ErrSubscriberConflict = errors.New("operation_id already used for different subscriber input")

// Keep foreground fan-out bounded across both a single operation and the node.
// Recovery is maintenance traffic and must not saturate slot apply behind HTTP
// requests when several large membership changes arrive together.
const (
	subscriberRecoveryInlinePageSize        = 1024
	subscriberRecoveryInlineParallelism     = 8
	subscriberRecoveryTargetProposalLimit   = 16
	subscriberRecoveryForegroundLimit       = 14
	subscriberRecoveryMinimumProposalBudget = 2 * time.Second
)

type SubscriberRecoveryConfig struct {
	Workers    int
	MaxPending uint64
	Interval   time.Duration
	Timeout    time.Duration
	// Paused stops background recovery, not normal versioned member writes.
	Paused bool
}

type SubscriberRecoveryStats struct {
	Workers                  int    `json:"workers"`
	Pages                    uint64 `json:"pages"`
	Failures                 uint64 `json:"failures"`
	LastProgress             int64  `json:"last_progress,omitempty"`
	LastError                string `json:"last_error,omitempty"`
	TargetCapacity           int    `json:"target_capacity"`
	TargetForegroundCapacity int    `json:"target_foreground_capacity"`
	TargetInflight           int    `json:"target_inflight"`
	TargetWaiting            int    `json:"target_waiting"`
	TargetPeak               int    `json:"target_peak"`
}

type subscriberRecovery struct {
	config                 SubscriberRecoveryConfig
	finalize               func(context.Context, wkdb.SubscriberWork) error
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	workMu                 sync.Mutex
	workLocks              map[subscriberWorkKey]*subscriberWorkGate
	targetTokens           chan struct{}
	foregroundTargetTokens chan struct{}
	mu                     sync.Mutex
	stats                  SubscriberRecoveryStats
}

// StartSubscriberRecovery must be called once, after the API finalizer is wired
// and before serving requests. Workers use current source-slot leadership; no
// in-memory task is required for recovery after restart or leadership transfer.
func (s *Store) StartSubscriberRecovery(cfg SubscriberRecoveryConfig, finalize func(context.Context, wkdb.SubscriberWork) error) error {
	if s.recovery != nil {
		return errors.New("subscriber recovery already started")
	}
	if cfg.Workers < 1 || cfg.Workers > 2 {
		return errors.New("subscriberRecovery.workers must be 1 or 2")
	}
	if cfg.MaxPending < 1 {
		return errors.New("subscriberRecovery.maxPending must be positive")
	}
	if cfg.Interval < 10*time.Millisecond {
		return errors.New("subscriberRecovery.interval must be at least 10ms")
	}
	if cfg.Timeout <= 0 {
		return errors.New("subscriberRecovery.timeout must be positive")
	}
	if finalize == nil {
		return errors.New("subscriber recovery finalizer is required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &subscriberRecovery{
		config:                 cfg,
		finalize:               finalize,
		cancel:                 cancel,
		targetTokens:           make(chan struct{}, subscriberRecoveryTargetProposalLimit),
		foregroundTargetTokens: make(chan struct{}, subscriberRecoveryForegroundLimit),
		stats: SubscriberRecoveryStats{
			Workers:                  cfg.Workers,
			TargetCapacity:           subscriberRecoveryTargetProposalLimit,
			TargetForegroundCapacity: subscriberRecoveryForegroundLimit,
		},
	}
	r.workLocks = make(map[subscriberWorkKey]*subscriberWorkGate)
	s.recovery = r
	s.recoveryEnabled.Store(!cfg.Paused || s.wdb.SubscriberRecoveryActive())
	if cfg.Paused {
		r.stats.Workers = 0
		return nil
	}
	r.stats.Workers = min(cfg.Workers, s.wdb.GetShardNum())
	for i := 0; i < r.stats.Workers; i++ {
		r.wg.Add(1)
		go func(worker int) { defer r.wg.Done(); s.runSubscriberRecovery(ctx, worker) }(i)
	}
	return nil
}

func (s *Store) SubscriberBacklog() ([]wkdb.SubscriberBacklogPartition, error) {
	parts, err := s.wdb.SubscriberBacklog()
	if err != nil {
		return nil, err
	}
	owned := parts[:0]
	for _, p := range parts {
		if s.opts.Slot.SlotLeaderId(p.SlotID) == s.opts.NodeId {
			owned = append(owned, p)
		}
	}
	return owned, nil
}

func (s *Store) SubscriberRecoveryStats() SubscriberRecoveryStats {
	if s.recovery == nil {
		return SubscriberRecoveryStats{}
	}
	s.recovery.mu.Lock()
	defer s.recovery.mu.Unlock()
	return s.recovery.stats
}

func canonicalSubscriberUIDs(uids []string) []string {
	uids = append([]string(nil), uids...)
	sort.Strings(uids)
	unique := uids[:0]
	for _, uid := range uids {
		if len(unique) == 0 || unique[len(unique)-1] != uid {
			unique = append(unique, uid)
		}
	}
	return unique
}

// ExistingSubscriberOperation is checked before any admission-only dependency
// (message-leader read floor, restore-set reads). A receipt already owns those
// immutable inputs, so a lost-response retry can resume or return completion.
func (s *Store) ExistingSubscriberOperation(o wkdb.SubscriberOperation) (wkdb.SubscriberReceipt, bool, error) {
	if o.OperationID == "" {
		return wkdb.SubscriberReceipt{}, false, nil
	}
	if s.opts.Slot.SlotLeaderId(s.opts.Slot.GetSlotId(o.ChannelID)) != s.opts.NodeId {
		return wkdb.SubscriberReceipt{}, false, errors.New("subscriber source leader changed")
	}
	r, found, err := s.wdb.GetSubscriberReceipt(o.ChannelID, o.ChannelType, o.OperationID)
	if err != nil || !found {
		return r, found, err
	}
	o.UIDs = canonicalSubscriberUIDs(o.UIDs)
	if r.Digest != o.Digest() {
		return r, false, ErrSubscriberConflict
	}
	return r, r.State != "rejected" || (r.Error != "backlog_full" && r.Error != "restore_set_changed"), nil
}

func (s *Store) SubmitSubscriberOperation(ctx context.Context, o wkdb.SubscriberOperation) (wkdb.SubscriberReceipt, error) {
	if s.recovery == nil {
		return wkdb.SubscriberReceipt{}, errors.New("subscriber recovery is disabled")
	}
	if len(o.UIDs) > wkdb.MaxSubscriberOperationMembers {
		return wkdb.SubscriberReceipt{}, fmt.Errorf("%w: too many subscribers", ErrInvalidSubscriberOperation)
	}
	o.UIDs = canonicalSubscriberUIDs(o.UIDs)
	if o.OperationID == "" {
		o.OperationID = wkutil.GenUUID()
	}
	o.CreatedAt = time.Now().UnixNano()
	o.MaxPending = s.recovery.config.MaxPending
	o.ConversationIDs = make([]uint64, len(o.UIDs))
	for i := range o.UIDs {
		o.ConversationIDs[i] = s.NextPrimaryKey()
	}
	if o.Mode == "deny_set" || o.Mode == "deny_remove_all" {
		denied, err := s.wdb.GetDenylist(o.ChannelID, o.ChannelType)
		if err != nil {
			return wkdb.SubscriberReceipt{}, err
		}
		newDeny := make(map[string]bool, len(o.UIDs))
		if o.Mode == "deny_set" {
			for _, uid := range o.UIDs {
				newDeny[uid] = true
			}
		}
		restoreCount := 0
		for _, member := range denied {
			if newDeny[member.Uid] {
				continue
			}
			subscribed, err := s.wdb.ExistSubscriber(o.ChannelID, o.ChannelType, member.Uid)
			if err != nil {
				return wkdb.SubscriberReceipt{}, err
			}
			if subscribed {
				restoreCount++
			}
		}
		o.RestoreConversationIDs = make([]uint64, restoreCount)
		for i := range o.RestoreConversationIDs {
			o.RestoreConversationIDs[i] = s.NextPrimaryKey()
		}
	}
	if err := o.Validate(); err != nil {
		return wkdb.SubscriberReceipt{}, fmt.Errorf("%w: %v", ErrInvalidSubscriberOperation, err)
	}
	slot := s.opts.Slot.GetSlotId(o.ChannelID)
	if s.opts.Slot.SlotLeaderId(slot) != s.opts.NodeId {
		return wkdb.SubscriberReceipt{}, errors.New("subscriber source leader changed")
	}
	// Read only on the source leader, then verify again after replicated apply.
	old, ok, err := s.wdb.GetSubscriberReceipt(o.ChannelID, o.ChannelType, o.OperationID)
	if err != nil {
		return old, err
	}
	if ok {
		if old.Digest != o.Digest() {
			return old, ErrSubscriberConflict
		}
		if old.State != "rejected" || (old.Error != "backlog_full" && old.Error != "restore_set_changed") {
			return old, nil
		}
	}
	if err := s.proposeRecovery(ctx, slot, CMDSubscriberOperation, o); err != nil {
		return wkdb.SubscriberReceipt{OperationID: o.OperationID, ChannelID: o.ChannelID, ChannelType: o.ChannelType, State: "unknown"}, err
	}
	r, found, err := s.wdb.GetSubscriberReceipt(o.ChannelID, o.ChannelType, o.OperationID)
	if err != nil {
		return r, err
	}
	if !found {
		return r, errors.New("subscriber receipt not visible; retry on current source leader")
	}
	if r.Digest != o.Digest() {
		return r, ErrSubscriberConflict
	}
	return r, nil
}

func (s *Store) GetSubscriberReceipt(ch string, tp uint8, id string) (wkdb.SubscriberReceipt, bool, error) {
	return s.wdb.GetSubscriberReceipt(ch, tp, id)
}

// CompleteSubscriberOperation runs the durable work on the request path. The
// record is committed before this method is called, so a timeout or process
// failure leaves only the unfinished tail for background recovery.
func (s *Store) CompleteSubscriberOperation(ctx context.Context, receipt wkdb.SubscriberReceipt) (wkdb.SubscriberReceipt, error) {
	if s.recovery == nil {
		return receipt, errors.New("subscriber recovery is disabled")
	}
	if receipt.State != "pending" {
		return receipt, nil
	}
	unlock, _, err := s.recovery.lockWork(ctx, subscriberWorkKey{receipt.ChannelID, receipt.ChannelType, receipt.Version}, true)
	if err != nil {
		return receipt, err
	}
	defer unlock()

	slot := s.opts.Slot.GetSlotId(receipt.ChannelID)
	for {
		if err := ctx.Err(); err != nil {
			return receipt, err
		}
		work, ok, err := s.wdb.GetSubscriberWork(receipt.ChannelID, receipt.ChannelType, slot, receipt.Version)
		if err != nil {
			return receipt, err
		}
		if !ok {
			latest, found, err := s.wdb.GetSubscriberReceipt(receipt.ChannelID, receipt.ChannelType, receipt.OperationID)
			if err != nil {
				return receipt, err
			}
			if found {
				return latest, nil
			}
			return receipt, errors.New("subscriber recovery work disappeared")
		}
		if err := s.processSubscriberWorkInline(ctx, work); err != nil {
			return receipt, err
		}
		latest, found, err := s.wdb.GetSubscriberReceipt(receipt.ChannelID, receipt.ChannelType, receipt.OperationID)
		if err != nil {
			return receipt, err
		}
		if !found {
			return receipt, errors.New("subscriber recovery receipt disappeared")
		}
		receipt = latest
		if receipt.State != "pending" {
			return receipt, nil
		}
	}
}

type subscriberWorkKey struct {
	channel string
	typeID  uint8
	version uint64
}
type subscriberWorkGate struct {
	token chan struct{}
	refs  int
}

// Only competing attempts of the SAME operation wait for one another. Ref
// counting includes waiters, so locks are reclaimed without a split-lock race.
func (r *subscriberRecovery) lockWork(ctx context.Context, key subscriberWorkKey, wait bool) (func(), bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	r.workMu.Lock()
	gate := r.workLocks[key]
	if gate == nil {
		gate = &subscriberWorkGate{token: make(chan struct{}, 1)}
		r.workLocks[key] = gate
	}
	gate.refs++
	r.workMu.Unlock()
	drop := func() {
		r.workMu.Lock()
		gate.refs--
		if gate.refs == 0 {
			delete(r.workLocks, key)
		}
		r.workMu.Unlock()
	}
	if wait {
		select {
		case gate.token <- struct{}{}:
		case <-ctx.Done():
			drop()
			return nil, false, ctx.Err()
		}
	} else {
		select {
		case gate.token <- struct{}{}:
		default:
			drop()
			return nil, false, nil
		}
	}
	return func() { <-gate.token; drop() }, true, nil
}

func (s *Store) proposeRecovery(ctx context.Context, slot uint32, tp CMDType, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(data) > wkdb.MaxSubscriberOperationBytes*8 {
		return errors.New("recovery command too large")
	}
	cmd, err := NewCMD(tp, data).Marshal()
	if err != nil {
		return err
	}
	_, err = s.opts.Slot.ProposeUntilAppliedTimeout(ctx, slot, cmd)
	return err
}

// proposeConversationEffects limits only recovery fan-out. Source operation
// and checkpoint commands remain unthrottled so progress can always be
// recorded. A small deadline reserve prevents a request which spent its budget
// waiting for admission from appending work that it can no longer await.
func (s *Store) proposeConversationEffects(ctx context.Context, slot uint32, effects []wkdb.ConversationEffect, foreground bool) error {
	r := s.recovery
	r.mu.Lock()
	r.stats.TargetWaiting++
	r.mu.Unlock()
	waiting := true
	finishWaiting := func() {
		if !waiting {
			return
		}
		r.mu.Lock()
		r.stats.TargetWaiting--
		r.mu.Unlock()
		waiting = false
	}
	if foreground {
		select {
		case r.foregroundTargetTokens <- struct{}{}:
			defer func() { <-r.foregroundTargetTokens }()
		case <-ctx.Done():
			finishWaiting()
			return ctx.Err()
		}
	}

	select {
	case r.targetTokens <- struct{}{}:
	case <-ctx.Done():
		finishWaiting()
		return ctx.Err()
	}
	defer func() { <-r.targetTokens }()
	finishWaiting()
	if foreground {
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < subscriberRecoveryMinimumProposalBudget {
			return context.DeadlineExceeded
		}
	}
	r.mu.Lock()
	r.stats.TargetInflight++
	if r.stats.TargetInflight > r.stats.TargetPeak {
		r.stats.TargetPeak = r.stats.TargetInflight
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.stats.TargetInflight--
		r.mu.Unlock()
	}()
	return s.proposeRecovery(ctx, slot, CMDConversationEffects, effects)
}

func (s *Store) applySubscriberRecovery(slot uint32, cmd *CMD, index uint64) error {
	if len(cmd.Data) > wkdb.MaxSubscriberOperationBytes*8 {
		return &PermanentApplyError{Err: errors.New("recovery command too large")}
	}
	switch cmd.CmdType {
	case CMDSubscriberOperation:
		return s.applySubscriberRecoveryCommands(slot, []*CMD{cmd}, []uint64{index})
	case CMDConversationEffects:
		var effects []wkdb.ConversationEffect
		if err := json.Unmarshal(cmd.Data, &effects); err != nil {
			return &PermanentApplyError{Err: err}
		}
		if len(effects) == 0 || len(effects) > wkdb.MaxConversationEffects {
			return &PermanentApplyError{Err: errors.New("invalid conversation effect page")}
		}
		for _, e := range effects {
			if e.UID == "" || e.ChannelID == "" || e.ChannelType == 0 || e.Version == 0 || (!e.Deleted && e.ConversationID == 0) || e.CreatedAt <= 0 {
				return &PermanentApplyError{Err: errors.New("invalid conversation effect")}
			}
			if s.opts.Slot.GetSlotId(e.UID) != slot {
				return &PermanentApplyError{Err: errors.New("conversation effect routed to wrong slot")}
			}
		}
		return s.wdb.ApplyConversationEffects(effects)
	case CMDSubscriberCheckpoint:
		return s.applySubscriberRecoveryCommands(slot, []*CMD{cmd}, []uint64{index})
	}
	return &PermanentApplyError{Err: errors.New("unknown recovery command")}
}

func (s *Store) applySubscriberRecoveryCommands(slot uint32, cmds []*CMD, versions []uint64) error {
	if len(cmds) != len(versions) || len(cmds) == 0 {
		return &PermanentApplyError{Err: errors.New("invalid recovery command batch")}
	}
	commands := make([]wkdb.SubscriberRecoveryCommand, 0, len(cmds))
	for i, cmd := range cmds {
		if len(cmd.Data) > wkdb.MaxSubscriberOperationBytes*8 {
			return &PermanentApplyError{Err: errors.New("recovery command too large")}
		}
		switch cmd.CmdType {
		case CMDSubscriberOperation:
			var operation wkdb.SubscriberOperation
			if err := json.Unmarshal(cmd.Data, &operation); err != nil {
				return &PermanentApplyError{Err: err}
			}
			if s.opts.Slot.GetSlotId(operation.ChannelID) != slot {
				return &PermanentApplyError{Err: errors.New("subscriber operation routed to wrong slot")}
			}
			if err := operation.Validate(); err != nil {
				return &PermanentApplyError{Err: err}
			}
			commands = append(commands, wkdb.SubscriberRecoveryCommand{SlotID: slot, Version: versions[i], Operation: &operation})
		case CMDSubscriberCheckpoint:
			var checkpoint wkdb.SubscriberCheckpoint
			if err := json.Unmarshal(cmd.Data, &checkpoint); err != nil {
				return &PermanentApplyError{Err: err}
			}
			if checkpoint.SlotID != slot || s.opts.Slot.GetSlotId(checkpoint.ChannelID) != slot {
				return &PermanentApplyError{Err: errors.New("subscriber checkpoint routed to wrong slot")}
			}
			commands = append(commands, wkdb.SubscriberRecoveryCommand{SlotID: slot, Version: versions[i], Checkpoint: &checkpoint})
		default:
			return &PermanentApplyError{Err: errors.New("invalid recovery command batch")}
		}
	}
	return s.wdb.ApplySubscriberRecoveryCommands(commands)
}

func (s *Store) runSubscriberRecovery(ctx context.Context, worker int) {
	r := s.recovery
	// DB ownership partitions scanning, enforcing the node-wide worker limit.
	cursors := make(map[int][]byte)
	shard := worker
	failures := 0
	for {
		delay := r.config.Interval
		if failures > 0 {
			delay *= time.Duration(1 << min(failures, 6))
			if delay > 5*time.Second {
				delay = 5 * time.Second
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if shard >= s.wdb.GetShardNum() {
			shard = worker
		}
		if shard >= s.wdb.GetShardNum() {
			continue
		}
		work, next, done, err := s.wdb.ListSubscriberWork(shard, cursors[shard], 1)
		if err == nil {
			cursors[shard] = next
			if done {
				cursors[shard] = nil
			}
		}
		shard += r.config.Workers
		if err == nil && len(work) > 0 && work[0].NextAttempt <= time.Now().UnixNano() && s.opts.Slot.SlotLeaderId(work[0].SlotID) == s.opts.NodeId {
			// Never park a bounded worker behind a request or another work item.
			unlock, acquired, _ := r.lockWork(ctx, subscriberWorkKey{work[0].Operation.ChannelID, work[0].Operation.ChannelType, work[0].Version}, false)
			if !acquired {
				continue
			}
			current, found, getErr := s.wdb.GetSubscriberWork(work[0].Operation.ChannelID, work[0].Operation.ChannelType, work[0].SlotID, work[0].Version)
			if getErr != nil {
				err = getErr
			} else if !found {
				err = nil
			} else {
				opCtx, cancel := context.WithTimeout(ctx, r.config.Timeout)
				err = s.processSubscriberWork(opCtx, current)
				cancel()
			}
			unlock()
			if err != nil && ctx.Err() == nil {
				w := work[0]
				// Persist retry scheduling so failover/restart cannot create a retry storm.
				wait := 250 * time.Millisecond * time.Duration(1<<min(w.Attempts, 7))
				retryCtx, retryCancel := context.WithTimeout(ctx, r.config.Timeout)
				message := err.Error()
				if len(message) > 512 {
					message = message[:512]
				}
				checkpointErr := s.proposeRecovery(retryCtx, w.SlotID, CMDSubscriberCheckpoint, wkdb.SubscriberCheckpoint{SlotID: w.SlotID, ChannelID: w.Operation.ChannelID, ChannelType: w.Operation.ChannelType, OperationID: w.Operation.OperationID, Version: w.Version, Previous: w.Next, Next: w.Next, RetryAt: time.Now().Add(wait).UnixNano(), Error: message})
				retryCancel()
				if checkpointErr != nil {
					err = errors.Join(err, fmt.Errorf("persist subscriber retry: %w", checkpointErr))
				}
			}
			if err == nil {
				r.mu.Lock()
				r.stats.Pages++
				r.stats.LastProgress = time.Now().UnixNano()
				r.mu.Unlock()
			}
		}
		if err != nil {
			failures++
			r.mu.Lock()
			r.stats.Failures++
			r.stats.LastError = err.Error()
			r.mu.Unlock()
		} else {
			failures = 0
		}
	}
}

func (s *Store) processSubscriberWork(ctx context.Context, w wkdb.SubscriberWork) error {
	return s.processSubscriberWorkPage(ctx, w, 16, 1, false)
}

func (s *Store) processSubscriberWorkInline(ctx context.Context, w wkdb.SubscriberWork) error {
	return s.processSubscriberWorkPage(ctx, w, subscriberRecoveryInlinePageSize, subscriberRecoveryInlineParallelism, true)
}

func (s *Store) processSubscriberWorkPage(ctx context.Context, w wkdb.SubscriberWork, pageSize int, parallelism int, foreground bool) error {
	if s.opts.Slot.SlotLeaderId(w.SlotID) != s.opts.NodeId {
		return errors.New("subscriber source leader changed")
	}
	page, err := s.wdb.GetSubscriberWorkPage(w, pageSize)
	if err != nil {
		return err
	}
	next := w.Next + len(page)
	bySlot := make(map[uint32][]wkdb.ConversationEffect)
	for _, e := range page {
		latest, found, err := s.wdb.GetSubscriberIntent(e.ChannelID, e.ChannelType, e.UID)
		if err != nil {
			return err
		}
		// A later membership mutation supersedes this effect. Advancing the
		// old checkpoint without replaying it makes churn converge directly to
		// the latest desired state instead of executing every historical delta.
		if !found || latest.Version != e.Version {
			continue
		}
		slot := s.opts.Slot.GetSlotId(e.UID)
		bySlot[slot] = append(bySlot[slot], e)
	}
	var g errgroup.Group
	g.SetLimit(parallelism)
	for slot, effects := range bySlot {
		slot, effects := slot, effects
		g.Go(func() error {
			for start := 0; start < len(effects); start += wkdb.MaxConversationEffects {
				page := effects[start:min(start+wkdb.MaxConversationEffects, len(effects))]
				// Each target command is idempotent. Let already-admitted siblings
				// finish even if another target fails admission; canceling the group
				// here turns a local deadline guard into Raft apply timeouts.
				if err := s.proposeConversationEffects(ctx, slot, page, foreground); err != nil {
					return fmt.Errorf("subscriber target slot %d: %w", slot, err)
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	done := next == w.Total
	if done {
		if err := s.recovery.finalize(ctx, w); err != nil {
			return fmt.Errorf("subscriber finalization: %w", err)
		}
	}
	if s.opts.Slot.SlotLeaderId(w.SlotID) != s.opts.NodeId {
		return errors.New("subscriber source leader changed before checkpoint")
	}
	return s.proposeRecovery(ctx, w.SlotID, CMDSubscriberCheckpoint, wkdb.SubscriberCheckpoint{SlotID: w.SlotID, ChannelID: w.Operation.ChannelID, ChannelType: w.Operation.ChannelType, OperationID: w.Operation.OperationID, Version: w.Version, Previous: w.Next, Next: next, Done: done, At: time.Now().UnixNano()})
}
