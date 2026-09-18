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
)

var ErrInvalidSubscriberOperation = errors.New("invalid subscriber operation")

var ErrSubscriberConflict = errors.New("operation_id already used for different subscriber input")

type SubscriberRecoveryConfig struct {
	Workers    int
	MaxPending uint64
	Interval   time.Duration
	Timeout    time.Duration
}

type SubscriberRecoveryStats struct {
	Workers      int    `json:"workers"`
	Pages        uint64 `json:"pages"`
	Failures     uint64 `json:"failures"`
	LastProgress int64  `json:"last_progress,omitempty"`
	LastError    string `json:"last_error,omitempty"`
}

type subscriberRecovery struct {
	config   SubscriberRecoveryConfig
	finalize func(context.Context, wkdb.SubscriberWork) error
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex
	stats    SubscriberRecoveryStats
}

// StartSubscriberRecovery must be called once, after the API finalizer is wired
// and before serving requests. Workers use current source-slot leadership; no
// in-memory task is required for recovery after restart or leadership transfer.
func (s *Store) StartSubscriberRecovery(cfg SubscriberRecoveryConfig, finalize func(context.Context, wkdb.SubscriberWork) error) error {
	if s.recovery != nil {
		return errors.New("subscriber recovery already started")
	}
	if cfg.Workers < 1 || cfg.Workers > 2 || cfg.MaxPending < 1 || cfg.Interval < 10*time.Millisecond || cfg.Timeout <= 0 || finalize == nil {
		return errors.New("invalid subscriber recovery configuration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &subscriberRecovery{config: cfg, finalize: finalize, cancel: cancel, stats: SubscriberRecoveryStats{Workers: cfg.Workers}}
	s.recovery = r
	for i := 0; i < cfg.Workers; i++ {
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

func (s *Store) SubmitSubscriberOperation(ctx context.Context, o wkdb.SubscriberOperation) (wkdb.SubscriberReceipt, error) {
	if s.recovery == nil {
		return wkdb.SubscriberReceipt{}, errors.New("subscriber recovery is disabled")
	}
	if len(o.UIDs) > wkdb.MaxSubscriberOperationMembers {
		return wkdb.SubscriberReceipt{}, fmt.Errorf("%w: too many subscribers", ErrInvalidSubscriberOperation)
	}
	o.UIDs = append([]string(nil), o.UIDs...)
	sort.Strings(o.UIDs)
	unique := o.UIDs[:0]
	for _, uid := range o.UIDs {
		if len(unique) == 0 || unique[len(unique)-1] != uid {
			unique = append(unique, uid)
		}
	}
	o.UIDs = unique
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
		o.RestoreConversationIDs = make([]uint64, min(o.MaxPending, uint64(wkdb.MaxSubscriberOperationMembers)))
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
		if old.State != "rejected" || old.Error != "backlog_full" {
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

func (s *Store) applySubscriberRecovery(slot uint32, cmd *CMD, index uint64) error {
	if len(cmd.Data) > wkdb.MaxSubscriberOperationBytes*8 {
		return errors.New("recovery command too large")
	}
	switch cmd.CmdType {
	case CMDSubscriberOperation:
		var o wkdb.SubscriberOperation
		if err := json.Unmarshal(cmd.Data, &o); err != nil {
			return err
		}
		if s.opts.Slot.GetSlotId(o.ChannelID) != slot {
			return errors.New("subscriber operation routed to wrong slot")
		}
		return s.wdb.ApplySubscriberOperation(slot, index, o)
	case CMDConversationEffects:
		var effects []wkdb.ConversationEffect
		if err := json.Unmarshal(cmd.Data, &effects); err != nil {
			return err
		}
		for _, e := range effects {
			if s.opts.Slot.GetSlotId(e.UID) != slot {
				return errors.New("conversation effect routed to wrong slot")
			}
		}
		return s.wdb.ApplyConversationEffects(effects)
	case CMDSubscriberCheckpoint:
		var c wkdb.SubscriberCheckpoint
		if err := json.Unmarshal(cmd.Data, &c); err != nil {
			return err
		}
		if c.SlotID != slot || s.opts.Slot.GetSlotId(c.ChannelID) != slot {
			return errors.New("subscriber checkpoint routed to wrong slot")
		}
		return s.wdb.CheckpointSubscriberWork(c)
	}
	return errors.New("unknown recovery command")
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
			opCtx, cancel := context.WithTimeout(ctx, r.config.Timeout)
			err = s.processSubscriberWork(opCtx, work[0])
			cancel()
			if err != nil && ctx.Err() == nil {
				w := work[0]
				// Persist retry scheduling so failover/restart cannot create a retry storm.
				wait := 250 * time.Millisecond * time.Duration(1<<min(w.Attempts, 7))
				retryCtx, retryCancel := context.WithTimeout(ctx, r.config.Timeout)
				message := err.Error()
				if len(message) > 512 {
					message = message[:512]
				}
				_ = s.proposeRecovery(retryCtx, w.SlotID, CMDSubscriberCheckpoint, wkdb.SubscriberCheckpoint{SlotID: w.SlotID, ChannelID: w.Operation.ChannelID, ChannelType: w.Operation.ChannelType, OperationID: w.Operation.OperationID, Version: w.Version, Previous: w.Next, Next: w.Next, RetryAt: time.Now().Add(wait).UnixNano(), Error: message})
				retryCancel()
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
	if s.opts.Slot.SlotLeaderId(w.SlotID) != s.opts.NodeId {
		return errors.New("subscriber source leader changed")
	}
	next := min(w.Next+16, len(w.Effects))
	bySlot := make(map[uint32][]wkdb.ConversationEffect)
	for _, e := range w.Effects[w.Next:next] {
		slot := s.opts.Slot.GetSlotId(e.UID)
		bySlot[slot] = append(bySlot[slot], e)
	}
	for slot, effects := range bySlot {
		if err := s.proposeRecovery(ctx, slot, CMDConversationEffects, effects); err != nil {
			return fmt.Errorf("subscriber target slot %d: %w", slot, err)
		}
	}
	done := next == len(w.Effects)
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
