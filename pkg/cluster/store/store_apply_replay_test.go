package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type recoveryOnlyProgressDB struct{ wkdb.DB }

func (d *recoveryOnlyProgressDB) SetSlotAppliedIndex(uint32, uint64) error {
	return errors.New("recovery must use its atomic fence, not the legacy checkpoint")
}

func TestRecoveryReplaysAfterReopenWithoutLegacyCheckpoint(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Store, *recoverySlots) {
		db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(2), wkdb.WithMemTableSize(1<<20)))
		require.NoError(t, db.Open())
		slots := &recoverySlots{}
		slots.leader.Store(1)
		s := New(NewOptions(WithNodeId(1), WithDB(&recoveryOnlyProgressDB{db}), WithSlot(slots)))
		slots.store = s
		return s, slots
	}
	s, slots := open()
	cfg := recoveryConfig()
	cfg.Paused = true
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	var receipts []wkdb.SubscriberReceipt
	for i, mode := range []string{"add", "remove", "add", "remove"} {
		r, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{
			OperationID: fmt.Sprintf("op-%d", i), ChannelID: "replay-group", ChannelType: 2,
			Mode: mode, UIDs: []string{"a", "b"},
		})
		require.NoError(t, err)
		r, err = s.CompleteSubscriberOperation(context.Background(), r)
		require.NoError(t, err)
		require.Equal(t, "complete", r.State)
		receipts = append(receipts, r)
	}
	s.Stop()
	require.NoError(t, s.DB().Close())
	restarted, _ := open()
	defer func() { restarted.Stop(); require.NoError(t, restarted.DB().Close()) }()
	for slot, logs := range slots.logs {
		index, err := restarted.DB().SlotAppliedIndex(uint32(slot))
		require.NoError(t, err)
		require.Zero(t, index)
		// Simulate losing the separate Raft applied watermark. Replay every
		// source operation, target effect and completion checkpoint from disk.
		require.NoError(t, restarted.ApplySlotLogs(uint32(slot), logs))
	}
	members, err := restarted.DB().GetSubscribers("replay-group", 2)
	require.NoError(t, err)
	require.Empty(t, members)
	for _, uid := range []string{"a", "b"} {
		_, err := restarted.DB().GetConversation(uid, "replay-group", 2)
		require.ErrorIs(t, err, wkdb.ErrNotFound)
	}
	backlog, err := restarted.DB().SubscriberBacklog()
	require.NoError(t, err)
	require.Empty(t, backlog)
	for _, want := range receipts {
		got, found, err := restarted.DB().GetSubscriberReceipt(want.ChannelID, want.ChannelType, want.OperationID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, want, got, "replay must not reopen or double-count completed work")
	}
}

type progressFailureDB struct {
	wkdb.DB
	failIndex uint64
}

func (d *progressFailureDB) SetSlotAppliedIndex(slot uint32, index uint64) error {
	if index == d.failIndex {
		return errors.New("injected progress fsync failure")
	}
	return d.DB.SetSlotAppliedIndex(slot, index)
}

func applyTestLog(t *testing.T, index uint64, tp CMDType, body []byte) types.Log {
	t.Helper()
	data, err := NewCMD(tp, body).Marshal()
	require.NoError(t, err)
	return types.Log{Index: index, Data: data}
}

func TestSlotApplyRetriesCommittedPrefixAndCrashGap(t *testing.T) {
	s, _ := recoveryStore(t)
	db := s.DB()
	wrapped := &progressFailureDB{DB: db, failIndex: 2}
	s.wdb = wrapped
	logs := []types.Log{
		applyTestLog(t, 1, CMDAddDenylist, EncodeMembers("g", 2, []wkdb.Member{{Uid: "a"}})),
		applyTestLog(t, 2, CMDRemoveDenylist, EncodeChannelUids("g", 2, []string{"a"})),
		applyTestLog(t, 3, CMDAddDenylist, EncodeMembers("g", 2, []wkdb.Member{{Uid: "b"}})),
	}
	require.Error(t, s.ApplySlotLogs(0, logs))
	index, err := db.SlotAppliedIndex(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), index)
	// A new Store has no process-local replay memory. The durable prefix and
	// the idempotent current entry must be sufficient to finish this batch.
	restarted := New(NewOptions(WithDB(db)))
	require.NoError(t, restarted.ApplySlotLogs(0, logs))
	require.NoError(t, restarted.ApplySlotLogs(0, logs))
	members, err := db.GetDenylist("g", 2)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "b", members[0].Uid)
	channel, err := db.GetChannel("g", 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, channel.DenylistCount)
}

func TestSlotApplyEventMarkerSurvivesProgressFailure(t *testing.T) {
	s, _ := recoveryStore(t)
	db := s.DB()
	s.wdb = &progressFailureDB{DB: db, failIndex: 2}
	event := func(id, payload string) *wkdb.MessageEvent {
		return &wkdb.MessageEvent{ChannelId: "g", ChannelType: 2, ClientMsgNo: "m", EventID: id, EventType: wkdb.EventTypeStreamDelta, Payload: []byte(payload)}
	}
	logs := []types.Log{
		applyTestLog(t, 1, CMDAppendMessageEvent, EncodeCMDMessageEvent(event("a", "A"), 1)),
		applyTestLog(t, 2, CMDAppendMessageEvent, EncodeCMDMessageEvent(event("b", "B"), 1)),
		applyTestLog(t, 3, CMDAppendMessageEvent, EncodeCMDMessageEvent(event("c", "C"), 1)),
	}
	require.Error(t, s.ApplySlotLogs(0, logs))
	// Change LastEventID through another writer to prove last-event-only
	// deduplication does not protect the interrupted log's commit gap.
	_, _, err := db.AppendMessageEventWithState(event("other", "X"))
	require.NoError(t, err)
	s.wdb = db
	require.NoError(t, s.ApplySlotLogs(0, logs))
	state, err := db.GetMessageEventState("g", 2, "m", "main")
	require.NoError(t, err)
	require.Equal(t, "ABXC", string(state.SnapshotPayload))
	require.EqualValues(t, 4, state.LastMsgEventSeq)
}

func TestSlotApplyUnknownCommandNeverAdvances(t *testing.T) {
	s, _ := recoveryStore(t)
	err := s.ApplySlotLogs(0, []types.Log{applyTestLog(t, 1, CMDType(65535), nil)})
	var permanent *PermanentApplyError
	require.ErrorAs(t, err, &permanent)
	index, err := s.DB().SlotAppliedIndex(0)
	require.NoError(t, err)
	require.Zero(t, index)
}

func TestSlotApplyRetiredCommandsRemainReplayCompatible(t *testing.T) {
	s, _ := recoveryStore(t)
	retired := []CMDType{
		_cmdSaveStreamMetaRemoved, _cmdStreamEndRemoved, _cmdAppendStreamItemRemoved,
		_cmdAddStreamMetaRemoved, _cmdAddStreamsRemoved, _cmdSaveStreamV2Removed,
		CMDAppendMessagesOfNotifyQueue, CMDRemoveMessagesOfNotifyQueue,
		CMDDeleteChannelAndClearMessages, CMDChannelClusterConfigDelete,
		CMDAddOrUpdatePlugin, CMDUpdatePluginConfig,
	}
	var logs []types.Log
	for i, cmd := range retired {
		logs = append(logs, applyTestLog(t, uint64(i+1), cmd, []byte("historic payload")))
	}
	// A normal command following the retired entries must still run.
	logs = append(logs, applyTestLog(t, uint64(len(logs)+1), CMDAddDenylist, EncodeMembers("g", 2, []wkdb.Member{{Uid: "a"}})))
	require.NoError(t, s.ApplySlotLogs(0, logs))
	require.NoError(t, s.ApplySlotLogs(0, logs))
	index, err := s.DB().SlotAppliedIndex(0)
	require.NoError(t, err)
	require.EqualValues(t, len(logs), index)
	members, err := s.DB().GetDenylist("g", 2)
	require.NoError(t, err)
	require.Len(t, members, 1)
}

func TestSlotApplyRecoveryPrefixCannotUndoInterruptedLegacyDelete(t *testing.T) {
	s, slots := recoveryStore(t)
	db := s.DB()
	effect := wkdb.ConversationEffect{UID: "a", ChannelID: "g", ChannelType: 2, Version: 1, ConversationID: 7, CreatedAt: time.Now().UnixNano()}
	body, err := json.Marshal([]wkdb.ConversationEffect{effect})
	require.NoError(t, err)
	logs := []types.Log{
		applyTestLog(t, 1, CMDConversationEffects, body),
		applyTestLog(t, 2, CMDDeleteConversation, EncodeCMDDeleteConversation("a", "g", 2)),
	}
	s.wdb = &progressFailureDB{DB: db, failIndex: 2}
	slot := slots.GetSlotId("a")
	require.Error(t, s.ApplySlotLogs(slot, logs))
	index, err := db.SlotAppliedIndex(slot)
	require.NoError(t, err)
	require.Zero(t, index, "the recovery prefix shares its next durable checkpoint")
	s.wdb = db
	require.NoError(t, s.ApplySlotLogs(slot, logs))
	_, err = db.GetConversation("a", "g", 2)
	require.ErrorIs(t, err, wkdb.ErrNotFound, "replayed recovery cannot resurrect the legacy deletion")
}

func TestSlotApplyBatchesAdjacentConversationEffectsWithoutReordering(t *testing.T) {
	s, slots := recoveryStore(t)
	db := s.DB()
	uidA := uidForRecoverySlot(slots, 3)
	uidB := ""
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("batched-target-3-%d", i)
		if candidate != uidA && slots.GetSlotId(candidate) == 3 {
			uidB = candidate
			break
		}
	}
	now := time.Now().UnixNano()
	effect := func(uid, channel string, version, conversationID uint64, deleted bool) []byte {
		body, err := json.Marshal([]wkdb.ConversationEffect{{
			UID: uid, ChannelID: channel, ChannelType: 2, Version: version,
			ConversationID: conversationID, Deleted: deleted, CreatedAt: now,
		}})
		require.NoError(t, err)
		return body
	}
	logs := []types.Log{
		applyTestLog(t, 1, CMDConversationEffects, effect(uidA, "g-a", 1, 11, false)),
		applyTestLog(t, 2, CMDConversationEffects, effect(uidB, "g-b", 1, 12, false)),
		// The repeated key must be applied after the first batch, not collapsed
		// ahead of it or reordered around it.
		applyTestLog(t, 3, CMDConversationEffects, effect(uidA, "g-a", 2, 0, true)),
	}
	require.NoError(t, s.ApplySlotLogs(3, logs))
	_, err := db.GetConversation(uidA, "g-a", 2)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	conversation, err := db.GetConversation(uidB, "g-b", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(12), conversation.Id)
	require.NoError(t, s.ApplySlotLogs(3, logs), "replaying a batched prefix must remain idempotent")
}
