package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// A deterministic slot transport exercises real Store apply and Pebble. It can
// lose a reply after apply, pause target slots, and change source leadership.
type recoverySlots struct {
	icluster.Slot
	store       *Store
	locks       [8]sync.Mutex
	indexes     [8]uint64
	logs        [8][]types.Log
	leader      atomic.Uint64
	failTargets atomic.Bool
	loseReply   atomic.Bool
	active      atomic.Int32
	peak        atomic.Int32
	targetBlock chan struct{}
}

func (s *recoverySlots) GetSlotId(v string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(v))
	return h.Sum32() % 8
}
func (s *recoverySlots) SlotLeaderId(uint32) uint64 { return s.leader.Load() }
func (s *recoverySlots) ProposeUntilApplied(slot uint32, data []byte) (*types.ProposeResp, error) {
	return s.ProposeUntilAppliedTimeout(context.Background(), slot, data)
}
func (s *recoverySlots) ProposeUntilAppliedTimeout(ctx context.Context, slot uint32, data []byte) (*types.ProposeResp, error) {
	cmd := &CMD{}
	if err := cmd.Unmarshal(data); err != nil {
		return nil, err
	}
	if cmd.CmdType == CMDConversationEffects {
		a := s.active.Add(1)
		defer s.active.Add(-1)
		for {
			p := s.peak.Load()
			if a <= p || s.peak.CompareAndSwap(p, a) {
				break
			}
		}
		if s.failTargets.Load() {
			return nil, context.DeadlineExceeded
		}
		if s.targetBlock != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-s.targetBlock:
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	s.locks[slot].Lock()
	defer s.locks[slot].Unlock()
	s.indexes[slot]++
	log := types.Log{Index: s.indexes[slot], Data: data}
	s.logs[slot] = append(s.logs[slot], log)
	err := s.store.ApplySlotLogs(slot, []types.Log{log})
	if err == nil && cmd.CmdType == CMDConversationEffects && s.loseReply.CompareAndSwap(true, false) {
		return nil, context.DeadlineExceeded
	}
	return &types.ProposeResp{}, err
}

func recoveryStore(t *testing.T) (*Store, *recoverySlots) {
	t.Helper()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithNodeId(1), wkdb.WithShardNum(2), wkdb.WithMemTableSize(1<<20)))
	require.NoError(t, db.Open())
	slots := &recoverySlots{}
	slots.leader.Store(1)
	s := New(NewOptions(WithNodeId(1), WithDB(db), WithSlot(slots)))
	slots.store = s
	t.Cleanup(func() { s.Stop(); require.NoError(t, db.Close()) })
	return s, slots
}

func recoveryConfig() SubscriberRecoveryConfig {
	return SubscriberRecoveryConfig{Workers: 2, MaxPending: 128, Interval: 10 * time.Millisecond, Timeout: time.Second}
}

func TestCompleteSubscriberOperationDrainsOnRequestPath(t *testing.T) {
	s, _ := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Interval = time.Hour
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))

	receipt, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{
		OperationID: "inline", ChannelID: "inline-group", ChannelType: 2,
		Mode: "add", UIDs: []string{"a", "b", "c"},
	})
	require.NoError(t, err)
	require.Equal(t, "pending", receipt.State)

	receipt, err = s.CompleteSubscriberOperation(context.Background(), receipt)
	require.NoError(t, err)
	require.Equal(t, "complete", receipt.State)
	require.Equal(t, receipt.Total, receipt.Completed)
	parts, err := s.SubscriberBacklog()
	require.NoError(t, err)
	require.Empty(t, parts)
}

func TestRecoveryDisbandPreservesMembersAndConversations(t *testing.T) {
	s, _ := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Interval = time.Hour
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	apply := func(o wkdb.SubscriberOperation) wkdb.SubscriberReceipt {
		r, err := s.SubmitSubscriberOperation(context.Background(), o)
		require.NoError(t, err)
		r, err = s.CompleteSubscriberOperation(context.Background(), r)
		require.NoError(t, err)
		require.Equal(t, "complete", r.State)
		return r
	}
	apply(wkdb.SubscriberOperation{OperationID: "join", ChannelID: "disband", ChannelType: 2, Mode: "add", UIDs: []string{"u"}, ReadToMsgSeq: 17})
	before, err := s.DB().GetConversation("u", "disband", 2)
	require.NoError(t, err)
	for _, id := range []string{"disband-1", "disband-2"} {
		r := apply(wkdb.SubscriberOperation{OperationID: id, ChannelID: "disband", ChannelType: 2, Mode: "disband"})
		require.Zero(t, r.Total, "disband must only update the channel flag and tag")
		info, err := s.DB().GetChannel("disband", 2)
		require.NoError(t, err)
		require.True(t, info.Disband)
		subscribed, err := s.DB().ExistSubscriber("disband", 2, "u")
		require.NoError(t, err)
		require.True(t, subscribed)
		after, err := s.DB().GetConversation("u", "disband", 2)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
}

func TestRecoveryMissingChannelReceiptDoesNotMutateLaterCreation(t *testing.T) {
	s, _ := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Interval = time.Hour
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	o := wkdb.SubscriberOperation{OperationID: "missing-remove", ChannelID: "later", ChannelType: 2, Mode: "remove", UIDs: []string{"u"}}
	r, err := s.SubmitSubscriberOperation(context.Background(), o)
	require.NoError(t, err)
	require.Equal(t, "complete", r.State)
	joined, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{OperationID: "join-later", ChannelID: "later", ChannelType: 2, Mode: "add", UIDs: []string{"u"}})
	require.NoError(t, err)
	_, err = s.CompleteSubscriberOperation(context.Background(), joined)
	require.NoError(t, err)
	retry, err := s.SubmitSubscriberOperation(context.Background(), o)
	require.NoError(t, err)
	require.Equal(t, r.Version, retry.Version)
	subscribed, err := s.DB().ExistSubscriber("later", 2, "u")
	require.NoError(t, err)
	require.True(t, subscribed)
}

func TestLegacySubscriberMutationsRejectedWhenRecoveryEnabled(t *testing.T) {
	s := New(NewOptions(WithSubscriberRecoveryEnabled(true)))
	members := []wkdb.Member{{Uid: "a"}}
	for _, err := range []error{
		s.AddSubscribers("g", 2, members),
		s.RemoveSubscribers("g", 2, []string{"a"}),
		s.RemoveAllSubscriber("g", 2),
		s.AddDenylist("g", 2, members),
		s.RemoveDenylist("g", 2, []string{"a"}),
		s.RemoveAllDenylist("g", 2),
	} {
		require.ErrorIs(t, err, ErrLegacySubscriberMutationDisabled)
	}
	require.NoError(t, s.rejectLegacySubscriberMutation(wkproto.ChannelTypePerson))
}

func TestLegacySubscriberMutationsRejectedAfterRecoveryActivation(t *testing.T) {
	db := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(t.TempDir()),
		wkdb.WithNodeId(1),
		wkdb.WithShardNum(1),
		wkdb.WithMemTableSize(1<<20),
	))
	require.NoError(t, db.Open())
	defer db.Close()
	effect := wkdb.ConversationEffect{
		UID: "a", ChannelID: "g", ChannelType: wkproto.ChannelTypeGroup,
		Version: 1, ConversationID: 7, CreatedAt: time.Now().UnixNano(),
	}
	require.NoError(t, db.(wkdb.SubscriberRecoveryDB).ApplyConversationEffects([]wkdb.ConversationEffect{effect}))
	require.True(t, db.SubscriberRecoveryActive())
	s := New(NewOptions(WithDB(db)))
	require.ErrorIs(t, s.AddSubscribers("g", wkproto.ChannelTypeGroup, []wkdb.Member{{Uid: "a"}}), ErrLegacySubscriberMutationDisabled)
}

func TestSubscriberRecoveryWorkerDrainAfterTimeout(t *testing.T) {
	s, slots := recoveryStore(t)
	slots.failTargets.Store(true)
	var failFinalize atomic.Bool
	failFinalize.Store(true)
	require.NoError(t, s.StartSubscriberRecovery(recoveryConfig(), func(context.Context, wkdb.SubscriberWork) error {
		if failFinalize.CompareAndSwap(true, false) {
			return errors.New("tag unavailable")
		}
		return nil
	}))
	receipts := make([]wkdb.SubscriberReceipt, 0, 8)
	for i := 0; i < 8; i++ {
		r, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{OperationID: fmt.Sprint(i), ChannelID: fmt.Sprintf("group-%d", i), ChannelType: 2, Mode: "add", UIDs: []string{"a", "b", "c"}})
		require.NoError(t, err)
		require.Equal(t, "pending", r.State)
		receipts = append(receipts, r)
	}
	require.Eventually(t, func() bool { return s.SubscriberRecoveryStats().Failures > 1 }, 3*time.Second, 10*time.Millisecond)
	parts, err := s.SubscriberBacklog()
	require.NoError(t, err)
	require.NotEmpty(t, parts)
	slots.failTargets.Store(false)
	slots.loseReply.Store(true)
	require.Eventually(t, func() bool {
		for _, r := range receipts {
			got, ok, err := s.GetSubscriberReceipt(r.ChannelID, 2, r.OperationID)
			if err != nil || !ok || got.State != "complete" {
				return false
			}
		}
		return true
	}, 8*time.Second, 20*time.Millisecond)
	require.LessOrEqual(t, slots.peak.Load(), int32(2))
	require.Equal(t, int32(2), slots.peak.Load(), "two bounded workers should make concurrent progress")
	parts, err = s.SubscriberBacklog()
	require.NoError(t, err)
	require.Empty(t, parts)
	for _, r := range receipts {
		for _, uid := range []string{"a", "b", "c"} {
			_, err := s.DB().GetConversation(uid, r.ChannelID, 2)
			require.NoError(t, err)
		}
	}
	require.GreaterOrEqual(t, s.SubscriberRecoveryStats().Failures, uint64(4))
}

func TestSubscriberRecoveryLeaderLossAndStableRequest(t *testing.T) {
	s, slots := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Interval = time.Hour // deterministic manual worker steps
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	input := wkdb.SubscriberOperation{OperationID: "stable", ChannelID: "g", ChannelType: 2, Mode: "add", UIDs: []string{"b", "a", "a"}}
	r, err := s.SubmitSubscriberOperation(context.Background(), input)
	require.NoError(t, err)
	retry, err := s.SubmitSubscriberOperation(context.Background(), input)
	require.NoError(t, err)
	require.Equal(t, r.Version, retry.Version)
	input.Mode = "remove"
	_, err = s.SubmitSubscriberOperation(context.Background(), input)
	require.ErrorIs(t, err, ErrSubscriberConflict)
	ws, _, _, err := s.DB().ListSubscriberWork(int(s.DB().GetChannelShardIndex("g", 2)), nil, 1)
	require.NoError(t, err)
	require.Len(t, ws, 1)
	slots.leader.Store(2)
	require.Error(t, s.processSubscriberWork(context.Background(), ws[0]))
	require.Zero(t, slots.peak.Load())
	slots.leader.Store(1)
	require.NoError(t, s.processSubscriberWork(context.Background(), ws[0]))
	done, _, err := s.GetSubscriberReceipt("g", 2, "stable")
	require.NoError(t, err)
	require.Equal(t, "complete", done.State)
}

func TestInlineRecoverySkipsSupersededEffects(t *testing.T) {
	s, _ := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Interval = time.Hour
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))

	add, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{
		OperationID: "add", ChannelID: "superseded", ChannelType: 2,
		Mode: "add", UIDs: []string{"a"},
	})
	require.NoError(t, err)
	remove, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{
		OperationID: "remove", ChannelID: "superseded", ChannelType: 2,
		Mode: "remove", UIDs: []string{"a"},
	})
	require.NoError(t, err)

	add, err = s.CompleteSubscriberOperation(context.Background(), add)
	require.NoError(t, err)
	require.Equal(t, "complete", add.State)
	_, err = s.DB().GetConversation("a", "superseded", 2)
	require.ErrorIs(t, err, wkdb.ErrNotFound)

	remove, err = s.CompleteSubscriberOperation(context.Background(), remove)
	require.NoError(t, err)
	require.Equal(t, "complete", remove.State)
}

func TestSubscriberRecoveryPartialPageAndRestartedWorker(t *testing.T) {
	s, slots := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Interval = time.Hour
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	uids := make([]string, 20)
	for i := range uids {
		uids[i] = fmt.Sprintf("u%02d", i)
	}
	_, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{OperationID: "page", ChannelID: "g", ChannelType: 2, Mode: "add", UIDs: uids})
	require.NoError(t, err)
	ws, _, _, err := s.DB().ListSubscriberWork(int(s.DB().GetChannelShardIndex("g", 2)), nil, 1)
	require.NoError(t, err)
	slots.loseReply.Store(true)
	require.Error(t, s.processSubscriberWork(context.Background(), ws[0]))
	got, _, err := s.GetSubscriberReceipt("g", 2, "page")
	require.NoError(t, err)
	require.Zero(t, got.Completed)
	require.NoError(t, s.processSubscriberWork(context.Background(), ws[0]))
	got, _, err = s.GetSubscriberReceipt("g", 2, "page")
	require.NoError(t, err)
	require.Equal(t, 16, got.Completed)
	// Replace all runtime scheduling state while preserving only the database.
	s.recovery.cancel()
	s.recovery.wg.Wait()
	s.recovery = nil
	require.NoError(t, s.StartSubscriberRecovery(recoveryConfig(), func(context.Context, wkdb.SubscriberWork) error { return nil }))
	require.Eventually(t, func() bool {
		r, _, err := s.GetSubscriberReceipt("g", 2, "page")
		return err == nil && r.State == "complete"
	}, 3*time.Second, 10*time.Millisecond)
}

func TestSubscriberRecoveryApplyLogOrderAndRouting(t *testing.T) {
	s, slots := recoveryStore(t)
	uid := "a"
	slot := slots.GetSlotId(uid)
	at := time.Now()
	logs := make([]types.Log, 0, 12)
	for i := 0; i < 6; i++ {
		c := wkdb.Conversation{Uid: uid, ChannelId: "g", ChannelType: 2, Id: 1, Type: wkdb.ConversationTypeChat, CreatedAt: &at, UpdatedAt: &at}
		body, err := EncodeCMDAddOrUpdateUserConversations(uid, []wkdb.Conversation{c})
		require.NoError(t, err)
		data, err := NewCMD(CMDAddOrUpdateUserConversations, body).Marshal()
		require.NoError(t, err)
		logs = append(logs, types.Log{Index: uint64(i*2 + 1), Data: data})
		body = EncodeCMDDeleteConversation(uid, "g", 2)
		data, err = NewCMD(CMDDeleteConversation, body).Marshal()
		require.NoError(t, err)
		logs = append(logs, types.Log{Index: uint64(i*2 + 2), Data: data})
	}
	require.NoError(t, s.ApplySlotLogs(slot, logs))
	_, err := s.DB().GetConversation(uid, "g", 2)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	effect := wkdb.ConversationEffect{UID: uid, ChannelID: "g", ChannelType: 2, Version: 1, ConversationID: 9, CreatedAt: at.UnixNano()}
	b, err := json.Marshal([]wkdb.ConversationEffect{effect})
	require.NoError(t, err)
	require.Error(t, s.applySubscriberRecovery((slot+1)%8, NewCMD(CMDConversationEffects, b), 13))
}

func TestRecoveryInlineChunksSkewedSlot(t *testing.T) {
	s, slots := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.MaxPending = 1024
	cfg.Interval = time.Hour
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	var uids []string
	for i := 0; len(uids) < 129; i++ {
		u := fmt.Sprintf("u%06d", i)
		if slots.GetSlotId(u) == 0 {
			uids = append(uids, u)
		}
	}
	r, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{OperationID: "skew", ChannelID: "g", ChannelType: 2, Mode: "add", UIDs: uids})
	require.NoError(t, err)
	r, err = s.CompleteSubscriberOperation(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, "complete", r.State)
	require.Equal(t, 129, r.Completed)
}

func TestRecoveryPausedStillCompletesNewMembership(t *testing.T) {
	s, _ := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Paused = true
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	for _, mode := range []string{"add", "remove", "add"} {
		r, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{ChannelID: "g", ChannelType: 2, Mode: mode, UIDs: []string{"a"}})
		require.NoError(t, err)
		r, err = s.CompleteSubscriberOperation(context.Background(), r)
		require.NoError(t, err)
		require.Equal(t, "complete", r.State)
	}
	_, err := s.DB().GetConversation("a", "g", 2)
	require.NoError(t, err)
	require.Zero(t, s.SubscriberRecoveryStats().Workers)
	require.Zero(t, s.SubscriberRecoveryStats().Pages)
}

func TestRecoveryRequestLockHonorsCancellation(t *testing.T) {
	s, _ := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Paused = true
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	r, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{ChannelID: "g", ChannelType: 2, Mode: "add", UIDs: []string{"a"}})
	require.NoError(t, err)
	unlock, acquired, err := s.recovery.lockWork(context.Background(), subscriberWorkKey{r.ChannelID, r.ChannelType, r.Version}, true)
	require.NoError(t, err)
	require.True(t, acquired)
	defer unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.CompleteSubscriberOperation(ctx, r)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRecoveryLocksOnlyBlockSameOperationAndAreReclaimed(t *testing.T) {
	r := &subscriberRecovery{workLocks: make(map[subscriberWorkKey]*subscriberWorkGate)}
	ctx := context.Background()
	key := subscriberWorkKey{"g", 2, 1}
	first, ok, err := r.lockWork(ctx, key, true)
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = r.lockWork(ctx, key, false)
	require.NoError(t, err)
	require.False(t, ok)
	other, ok, err := r.lockWork(ctx, subscriberWorkKey{"g", 2, 2}, false)
	require.NoError(t, err)
	require.True(t, ok)
	other()
	first()
	require.Empty(t, r.workLocks)
}

func TestRecoveryTargetProposalAdmission(t *testing.T) {
	t.Run("node capacity", func(t *testing.T) {
		s, slots := recoveryStore(t)
		cfg := recoveryConfig()
		cfg.Paused = true
		require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
		slots.targetBlock = make(chan struct{})
		receipts := make([]wkdb.SubscriberReceipt, 0, subscriberRecoveryForegroundLimit)
		for i := 0; i < subscriberRecoveryForegroundLimit; i++ {
			uid := uidForRecoverySlot(slots, uint32(i%8))
			r, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{
				OperationID: fmt.Sprintf("capacity-%d", i), ChannelID: fmt.Sprintf("capacity-group-%d", i),
				ChannelType: 2, Mode: "add", UIDs: []string{uid},
			})
			require.NoError(t, err)
			receipts = append(receipts, r)
		}
		errs := completeRecoveryConcurrently(s, receipts)
		require.Eventually(t, func() bool {
			stats := s.SubscriberRecoveryStats()
			return stats.TargetInflight == subscriberRecoveryForegroundLimit
		}, time.Second, time.Millisecond)
		close(slots.targetBlock)
		for range receipts {
			require.NoError(t, <-errs)
		}
		stats := s.SubscriberRecoveryStats()
		require.Equal(t, subscriberRecoveryTargetProposalLimit, stats.TargetCapacity)
		require.Equal(t, subscriberRecoveryForegroundLimit, stats.TargetForegroundCapacity)
		require.Equal(t, subscriberRecoveryForegroundLimit, stats.TargetPeak)
		require.Zero(t, stats.TargetInflight)
		require.Zero(t, stats.TargetWaiting)
	})
}

func uidForRecoverySlot(slots *recoverySlots, target uint32) string {
	for i := 0; ; i++ {
		uid := fmt.Sprintf("target-%d-%d", target, i)
		if slots.GetSlotId(uid) == target {
			return uid
		}
	}
}

func completeRecoveryConcurrently(s *Store, receipts []wkdb.SubscriberReceipt) <-chan error {
	start := make(chan struct{})
	errs := make(chan error, len(receipts))
	for _, receipt := range receipts {
		receipt := receipt
		go func() {
			<-start
			_, err := s.CompleteSubscriberOperation(context.Background(), receipt)
			errs <- err
		}()
	}
	close(start)
	return errs
}

func TestRecoveryPausedAfterRestartFencesOldPendingWork(t *testing.T) {
	dir := t.TempDir()
	// This transport has no Raft log storage. Retain its committed log indexes
	// separately, as real Raft storage does, instead of treating the business
	// DB's legacy replay boundary as the complete Raft applied index.
	var logIndexes [8]uint64
	open := func() (*Store, *recoverySlots) {
		db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithNodeId(1), wkdb.WithShardNum(2), wkdb.WithMemTableSize(1<<20)))
		require.NoError(t, db.Open())
		slots := &recoverySlots{}
		slots.leader.Store(1)
		slots.indexes = logIndexes
		s := New(NewOptions(WithNodeId(1), WithDB(db), WithSlot(slots)))
		slots.store = s
		return s, slots
	}
	s, slots := open()
	cfg := recoveryConfig()
	cfg.Interval = time.Hour
	finalize := func(context.Context, wkdb.SubscriberWork) error { return nil }
	require.NoError(t, s.StartSubscriberRecovery(cfg, finalize))
	old, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{OperationID: "old", ChannelID: "g", ChannelType: 2, Mode: "add", UIDs: []string{"a"}})
	require.NoError(t, err)
	work, found, err := s.DB().GetSubscriberWork("g", 2, slots.GetSlotId("g"), old.Version)
	effects, pageErr := s.DB().GetSubscriberWorkPage(work, wkdb.MaxSubscriberWorkPage)
	require.NoError(t, pageErr)
	require.NoError(t, err)
	require.True(t, found)
	logIndexes = slots.indexes
	s.Stop()
	require.NoError(t, s.DB().Close())
	s, _ = open()
	defer func() { s.Stop(); require.NoError(t, s.DB().Close()) }()
	cfg.Paused = true
	require.NoError(t, s.StartSubscriberRecovery(cfg, finalize))
	require.True(t, s.DB().SubscriberRecoveryActive())
	removed, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{OperationID: "remove", ChannelID: "g", ChannelType: 2, Mode: "remove", UIDs: []string{"a"}})
	require.NoError(t, err)
	removed, err = s.CompleteSubscriberOperation(context.Background(), removed)
	require.NoError(t, err)
	require.Equal(t, "complete", removed.State)
	require.NoError(t, s.DB().ApplyConversationEffects(effects), "delayed old add must be harmless")
	_, err = s.DB().GetConversation("a", "g", 2)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	require.Zero(t, s.SubscriberRecoveryStats().Workers)
}
