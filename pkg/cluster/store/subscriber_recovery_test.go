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
	"github.com/stretchr/testify/require"
)

// A deterministic slot transport exercises real Store apply and Pebble. It can
// lose a reply after apply, pause target slots, and change source leadership.
type recoverySlots struct {
	icluster.Slot
	store       *Store
	locks       [8]sync.Mutex
	indexes     [8]uint64
	leader      atomic.Uint64
	failTargets atomic.Bool
	loseReply   atomic.Bool
	active      atomic.Int32
	peak        atomic.Int32
}

func (s *recoverySlots) GetSlotId(v string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(v))
	return h.Sum32() % 8
}
func (s *recoverySlots) SlotLeaderId(uint32) uint64 { return s.leader.Load() }
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
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	s.locks[slot].Lock()
	defer s.locks[slot].Unlock()
	s.indexes[slot]++
	err := s.store.ApplySlotLogs(slot, []types.Log{{Index: s.indexes[slot], Data: data}})
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
