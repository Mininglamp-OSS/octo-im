package wkdb

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func recoveryTestDB(t *testing.T, dir string) *wukongDB {
	t.Helper()
	db := NewWukongDB(NewOptions(WithDir(dir), WithNodeId(1), WithShardNum(2), WithMemTableSize(1<<20))).(*wukongDB)
	require.NoError(t, db.Open())
	return db
}

func reopenRecoveryTestDB(t *testing.T, dir string) *wukongDB {
	t.Helper()
	db := NewWukongDB(NewOptions(WithDir(dir), WithNodeId(1), WithShardNum(2), WithMemTableSize(1<<20), WithSubscriberRecoveryEnabled(true))).(*wukongDB)
	require.NoError(t, db.Open())
	return db
}

func TestSubscriberRecoveryInactiveSkipsLifecycleReads(t *testing.T) {
	db := NewWukongDB(NewOptions(WithShardNum(1))).(*wukongDB)
	input := []Conversation{{Uid: "a", ChannelId: "g", ChannelType: 2, Id: 1}}
	filtered, err := db.filterRecoveryConversations(input)
	require.NoError(t, err)
	require.Equal(t, input, filtered)
}

func TestSubscriberRecoveryActivationSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	db := recoveryTestDB(t, dir)
	require.False(t, db.SubscriberRecoveryActive())
	effect := ConversationEffect{UID: "a", ChannelID: "g", ChannelType: 2, Version: 1, ConversationID: 7, CreatedAt: time.Now().UnixNano()}
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{effect}))
	require.True(t, db.SubscriberRecoveryActive())
	require.NoError(t, db.Close())

	disabled := NewWukongDB(NewOptions(WithDir(dir), WithNodeId(1), WithShardNum(2), WithMemTableSize(1<<20))).(*wukongDB)
	require.NoError(t, disabled.Open())
	require.True(t, disabled.SubscriberRecoveryActive())
	require.NoError(t, disabled.Close())

	db = reopenRecoveryTestDB(t, dir)
	defer db.Close()
	require.True(t, db.SubscriberRecoveryActive())
	filtered, err := db.filterRecoveryConversations([]Conversation{{Uid: "a", ChannelId: "g", ChannelType: 2, Id: 8}})
	require.NoError(t, err)
	require.Empty(t, filtered)
}

func recoveryOperation(id, mode string, uids ...string) SubscriberOperation {
	ids := make([]uint64, len(uids))
	for i := range ids {
		ids[i] = 100 + uint64(i)
	}
	reserve := make([]uint64, 128)
	for i := range reserve {
		reserve[i] = 1000 + uint64(i)
	}
	return SubscriberOperation{RestoreConversationIDs: reserve, OperationID: id, ChannelID: "group", ChannelType: 2, Mode: mode, UIDs: uids, ConversationIDs: ids, CreatedAt: time.Now().UnixNano(), MaxPending: 128, ReadToMsgSeq: 10}
}

func recoveryWork(t *testing.T, db *wukongDB, o SubscriberOperation) SubscriberWork {
	t.Helper()
	ws, _, _, err := db.ListSubscriberWork(int(db.GetChannelShardIndex(o.ChannelID, o.ChannelType)), nil, 64)
	require.NoError(t, err)
	for _, w := range ws {
		if w.Operation.OperationID == o.OperationID {
			for at := 0; at < w.Total; {
				cursor := w
				cursor.Next = at
				page, err := db.GetSubscriberWorkPage(cursor, MaxSubscriberWorkPage)
				require.NoError(t, err)
				require.NotEmpty(t, page)
				w.Effects = append(w.Effects, page...)
				at += len(page)
			}
			return w
		}
	}
	t.Fatal("work missing")
	return SubscriberWork{}
}

func completeRecoveryWork(t *testing.T, db *wukongDB, w SubscriberWork) {
	t.Helper()
	require.NoError(t, db.CheckpointSubscriberWork(SubscriberCheckpoint{SlotID: w.SlotID, ChannelID: w.Operation.ChannelID, ChannelType: w.Operation.ChannelType, OperationID: w.Operation.OperationID, Version: w.Version, Previous: w.Next, Next: w.Total, Done: true, At: time.Now().UnixNano()}))
}

func TestSubscriberRecoveryAtomicAdmissionAndReplay(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	seed := recoveryOperation("seed", "add")
	seed.ChannelID = "seed-channel"
	for db.GetChannelShardIndex(seed.ChannelID, 2) != db.GetChannelShardIndex("group", 2) {
		seed.ChannelID += "x"
	}
	require.NoError(t, db.ApplySubscriberOperation(1, 1, seed))
	o := recoveryOperation("join", "add", "a", "b")
	o.MaxPending = 2
	require.NoError(t, db.ApplySubscriberOperation(1, 2, o))
	r, ok, err := db.GetSubscriberReceipt("group", 2, "join")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "backlog_full", r.Error)
	members, err := db.GetSubscribers("group", 2)
	require.NoError(t, err)
	require.Empty(t, members)
	ch, err := db.GetChannel("group", 2)
	require.NoError(t, err)
	require.True(t, IsEmptyChannelInfo(ch))
	completeRecoveryWork(t, db, recoveryWork(t, db, seed))
	o.MaxPending = 10
	require.NoError(t, db.ApplySubscriberOperation(1, 3, o))
	members, err = db.GetSubscribers("group", 2)
	require.NoError(t, err)
	require.Len(t, members, 2)
	w := recoveryWork(t, db, o)
	require.Len(t, w.Effects, 2)
	completeRecoveryWork(t, db, w)
	// Both an HTTP retry with a new log index and a replay of the original log
	// retain the completed receipt instead of regenerating work.
	require.NoError(t, db.ApplySubscriberOperation(1, 4, o))
	require.NoError(t, db.ApplySubscriberOperation(1, 3, o))
	ws, _, _, err := db.ListSubscriberWork(int(db.GetChannelShardIndex("group", 2)), nil, 64)
	require.NoError(t, err)
	require.Empty(t, ws)
	r, _, err = db.GetSubscriberReceipt("group", 2, "join")
	require.NoError(t, err)
	require.Equal(t, "complete", r.State)
	conflict := o
	conflict.Mode = "remove"
	require.NoError(t, db.ApplySubscriberOperation(1, 5, conflict))
	members, err = db.GetSubscribers("group", 2)
	require.NoError(t, err)
	require.Len(t, members, 2)
	require.NotEqual(t, conflict.Digest(), r.Digest)
	parts, err := db.SubscriberBacklog()
	require.NoError(t, err)
	require.Empty(t, parts)
}

func TestSubscriberRecoveryRestartCheckpointAndReset(t *testing.T) {
	dir := t.TempDir()
	db := recoveryTestDB(t, dir)
	join := recoveryOperation("join", "add", "a", "b")
	require.NoError(t, db.ApplySubscriberOperation(1, 10, join))
	reset := recoveryOperation("reset", "reset", "b", "c")
	require.NoError(t, db.ApplySubscriberOperation(1, 11, reset))
	w := recoveryWork(t, db, reset)
	require.Len(t, w.Effects, 3)
	require.Equal(t, uint64(10), w.Effects[1].Version, "retained membership keeps its generation")
	require.True(t, w.Effects[0].Deleted)
	require.False(t, w.Effects[2].Deleted)
	cp := SubscriberCheckpoint{SlotID: 1, ChannelID: "group", ChannelType: 2, OperationID: "reset", Version: 11, Previous: 0, Next: 1}
	require.NoError(t, db.CheckpointSubscriberWork(cp))
	require.NoError(t, db.CheckpointSubscriberWork(cp))
	cp.Previous = 1
	cp.Next = 1
	cp.RetryAt = time.Now().Add(time.Minute).UnixNano()
	cp.Error = "target unavailable"
	require.NoError(t, db.CheckpointSubscriberWork(cp))
	require.NoError(t, db.CheckpointSubscriberWork(cp))
	require.NoError(t, db.Close())
	db = reopenRecoveryTestDB(t, dir)
	defer db.Close()
	w = recoveryWork(t, db, reset)
	require.Equal(t, 1, w.Next)
	require.Equal(t, uint32(1), w.Attempts)
	require.Equal(t, cp.RetryAt, w.NextAttempt)
	r, _, err := db.GetSubscriberReceipt("group", 2, "reset")
	require.NoError(t, err)
	require.Equal(t, "target unavailable", r.LastError)
	stale := cp
	stale.Previous = 0
	stale.Next = 3
	stale.Done = true
	stale.RetryAt = 0
	require.NoError(t, db.CheckpointSubscriberWork(stale))
	require.Equal(t, 1, recoveryWork(t, db, reset).Next)
	completeRecoveryWork(t, db, w)
	members, err := db.GetSubscribers("group", 2)
	require.NoError(t, err)
	uids := []string{}
	for _, m := range members {
		uids = append(uids, m.Uid)
	}
	require.ElementsMatch(t, []string{"b", "c"}, uids)
}

func TestConversationLifecycleOutOfOrderAndCacheFencing(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	e := ConversationEffect{UID: "a", ChannelID: "group", ChannelType: 2, Version: 10, ConversationID: 100, CreatedAt: time.Now().UnixNano(), ReadToMsgSeq: 10}
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	initial, err := db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	leave := e
	leave.Version = 11
	leave.Deleted = true
	leave.ConversationID = 0
	rejoin := e
	rejoin.Version = 12
	rejoin.ConversationID = 200
	rejoin.ReadToMsgSeq = 30
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{leave, rejoin, leave, e, rejoin}))
	initial.ReadToMsgSeq = 999
	require.NoError(t, db.AddOrUpdateConversations([]Conversation{initial}))
	require.NoError(t, db.AddOrUpdateConversationsWithUser("a", []Conversation{initial}))
	require.NoError(t, db.AddOrUpdateConversationsBatchIfNotExist([]Conversation{initial}))
	got, err := db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(200), got.Id)
	require.Equal(t, uint64(30), got.ReadToMsgSeq)
	got.ReadToMsgSeq = 42
	require.NoError(t, db.AddOrUpdateConversationsWithUser("a", []Conversation{got}))
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{rejoin}))
	got, err = db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(42), got.ReadToMsgSeq, "same-generation retry must not reset reads")
	leave.Version = 13
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{leave}))
	require.NoError(t, db.AddOrUpdateConversations([]Conversation{got}))
	require.NoError(t, db.AddOrUpdateConversationsBatchIfNotExist([]Conversation{got}))
	_, err = db.GetConversation("a", "group", 2)
	require.ErrorIs(t, err, ErrNotFound)
	users, err := db.GetChannelConversationLocalUsers("group", 2)
	require.NoError(t, err)
	require.Empty(t, users)
}

func TestConversationLifecycleRepairsInterruptedRelation(t *testing.T) {
	dir := t.TempDir()
	db := recoveryTestDB(t, dir)
	e := ConversationEffect{UID: "a", ChannelID: "group", ChannelType: 2, Version: 1, ConversationID: 7, CreatedAt: time.Now().UnixNano()}
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	// Model the durable state at the crash point between the user commit and
	// reverse relation commit, then reopen both physical databases.
	b := db.shardDB("a").NewBatch()
	e.RelationDone = false
	require.NoError(t, recoverySet(b, recoveryKey(recoveryLifecycle, "a", "group", string([]byte{2})), e))
	require.NoError(t, b.Commit(db.sync))
	require.NoError(t, b.Close())
	require.NoError(t, db.deleteConversationLocalUserRelation("group", 2, "a"))
	require.NoError(t, db.Close())
	db = reopenRecoveryTestDB(t, dir)
	defer db.Close()
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	users, err := db.GetChannelConversationLocalUsers("group", 2)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, users)
	stored, ok, err := db.ConversationLifecycle("a", "group", 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, stored.RelationDone)
}

func TestSubscriberRecoveryDenylistIdempotencyAndLiveChannel(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	join := recoveryOperation("join", "add", "a")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, join))
	for i, mode := range []string{"deny_add", "deny_add", "deny_remove", "deny_set", "deny_remove_all"} {
		o := recoveryOperation(fmt.Sprint(i), mode, "a")
		require.NoError(t, db.ApplySubscriberOperation(1, uint64(i+2), o))
		got, err := db.GetDenylist("group", 2)
		require.NoError(t, err)
		n := 1
		if mode == "deny_remove" || mode == "deny_remove_all" {
			n = 0
		}
		require.Len(t, got, n)
		ch, err := db.GetChannel("group", 2)
		require.NoError(t, err)
		require.Equal(t, n, ch.DenylistCount)
	}
	live := recoveryOperation("live", "add", "a")
	live.ChannelType = 9
	require.NoError(t, db.ApplySubscriberOperation(1, 20, live))
	require.Empty(t, recoveryWork(t, db, live).Effects)
}

func TestConversationLifecycleDeletesLegacyDuplicateRows(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	b := db.sharedBatchDB("a").NewBatch()
	at := time.Now()
	for _, id := range []uint64{1, 2} {
		require.NoError(t, db.writeConversation(Conversation{Id: id, Uid: "a", ChannelId: "group", ChannelType: 2, Type: ConversationTypeChat, CreatedAt: &at, UpdatedAt: &at}, b))
	}
	require.NoError(t, b.CommitWait())
	e := ConversationEffect{UID: "a", ChannelID: "group", ChannelType: 2, Version: 1, Deleted: true, CreatedAt: at.UnixNano()}
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	cs, err := db.GetConversations("a")
	require.NoError(t, err)
	require.Empty(t, cs)

}

func TestSubscriberRecoveryAdoptsRetainedLegacyConversation(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	at := time.Now()
	require.NoError(t, db.AddSubscribers("group", 2, []Member{{Uid: "a", CreatedAt: &at, UpdatedAt: &at}}))
	c := Conversation{Id: 44, Uid: "a", ChannelId: "group", ChannelType: 2, Type: ConversationTypeChat, ReadToMsgSeq: 3, UnreadCount: 5, CreatedAt: &at, UpdatedAt: &at}
	require.NoError(t, db.AddOrUpdateConversations([]Conversation{c}))
	o := recoveryOperation("adopt", "add", "a")
	o.ReadToMsgSeq = 50
	require.NoError(t, db.ApplySubscriberOperation(1, 1, o))
	w := recoveryWork(t, db, o)
	require.True(t, w.Effects[0].PreserveExisting)
	require.NoError(t, db.ApplyConversationEffects(w.Effects))
	got, err := db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(3), got.ReadToMsgSeq)
	require.Equal(t, uint32(5), got.UnreadCount)
	require.NotEqual(t, c.Id, got.Id)
	require.NoError(t, db.ApplyConversationEffects(w.Effects))
	got.ReadToMsgSeq = 4
	require.NoError(t, db.AddOrUpdateConversationsWithUser("a", []Conversation{got}))
	got, err = db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(4), got.ReadToMsgSeq)
}

func TestSubscriberRecoveryResetRejectionKeepsOriginalMembers(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	o := recoveryOperation("initial", "add", "a", "b")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, o))
	reset := recoveryOperation("too-small", "reset", "c")
	reset.MaxPending = 2
	require.NoError(t, db.ApplySubscriberOperation(1, 2, reset))
	r, _, err := db.GetSubscriberReceipt("group", 2, "too-small")
	require.NoError(t, err)
	require.Equal(t, "rejected", r.State)
	members, err := db.GetSubscribers("group", 2)
	require.NoError(t, err)
	uids := []string{}
	for _, m := range members {
		uids = append(uids, m.Uid)
	}
	require.ElementsMatch(t, []string{"a", "b"}, uids)
}

func TestConversationLifecycleConcurrentStaleWritersAndCacheReads(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	e := ConversationEffect{UID: "a", ChannelID: "group", ChannelType: 2, Version: 1, ConversationID: 1, CreatedAt: time.Now().UnixNano()}
	require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	stale, err := db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	var wg sync.WaitGroup
	failures := make(chan error, 100)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if err := db.AddOrUpdateConversations([]Conversation{stale}); err != nil {
				failures <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if _, err := db.GetLastConversations("a", ConversationTypeChat, 0, nil, 10); err != nil {
				failures <- err
			}
		}
	}()
	for i := uint64(2); i <= 20; i++ {
		e.Version = i
		e.ConversationID = i
		e.Deleted = i%2 == 0
		require.NoError(t, db.ApplyConversationEffects([]ConversationEffect{e}))
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	cs, err := db.GetLastConversations("a", ConversationTypeChat, 0, nil, 10)
	require.NoError(t, err)
	require.Empty(t, cs)
}

func TestSubscriberRecoveryBlacklistUnblockDoesNotRejoinDeparted(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	version := uint64(0)
	apply := func(mode string, uids ...string) {
		version++
		o := recoveryOperation(fmt.Sprint(version), mode, uids...)
		for i := range o.ConversationIDs {
			o.ConversationIDs[i] += version * 10000
		}
		for i := range o.RestoreConversationIDs {
			o.RestoreConversationIDs[i] += version * 10000
		}
		require.NoError(t, db.ApplySubscriberOperation(1, version, o))
		w := recoveryWork(t, db, o)
		if len(w.Effects) > 0 {
			require.NoError(t, db.ApplyConversationEffects(w.Effects))
		}
		completeRecoveryWork(t, db, w)
	}
	apply("add", "a", "b")
	apply("deny_set", "a", "b")
	_, err := db.GetConversation("a", "group", 2)
	require.ErrorIs(t, err, ErrNotFound)
	apply("remove", "b")
	apply("deny_set", "b") // replacement unblocks a, while b has already left
	_, err = db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	apply("deny_remove_all")
	_, err = db.GetConversation("b", "group", 2)
	require.ErrorIs(t, err, ErrNotFound)
	apply("deny_add", "a")
	apply("deny_remove", "a")
	_, err = db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	apply("deny_add", "a")
	apply("deny_remove_all")
	_, err = db.GetConversation("a", "group", 2)
	require.NoError(t, err)
	members, err := db.GetSubscribers("group", 2)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "a", members[0].Uid)
}

func TestSubscriberRecoveryRejectsUnboundedExistingMember(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	at := time.Now()
	info := NewChannelInfo("group", 2)
	_, err := db.AddChannel(info)
	require.NoError(t, err)
	require.NoError(t, db.AddSubscribers("group", 2, []Member{{Uid: strings.Repeat("x", 1025), CreatedAt: &at, UpdatedAt: &at}}))
	o := recoveryOperation("remove-all", "remove_all")
	require.NoError(t, db.ApplySubscriberOperation(1, 1, o))
	r, _, err := db.GetSubscriberReceipt("group", 2, o.OperationID)
	require.NoError(t, err)
	require.Equal(t, "invalid_existing_member", r.Error)
	ms, err := db.GetSubscribers("group", 2)
	require.NoError(t, err)
	require.Len(t, ms, 1)
	invalid := recoveryOperation("invalid", "add", " ")
	require.Error(t, invalid.Validate())
}

func TestSubscriberRecoveryCommandBatchPreservesVisibilityAndReplay(t *testing.T) {
	db := recoveryTestDB(t, t.TempDir())
	defer db.Close()
	operation := recoveryOperation("grouped", "add", "a")
	commands := []SubscriberRecoveryCommand{
		{SlotID: 1, Version: 1, Operation: &operation},
		{SlotID: 1, Version: 2, Checkpoint: &SubscriberCheckpoint{
			SlotID: 1, ChannelID: operation.ChannelID, ChannelType: operation.ChannelType,
			OperationID: operation.OperationID, Version: 1, Previous: 0, Next: 1,
			Done: true, At: time.Now().UnixNano(),
		}},
	}
	recovery := SubscriberRecoveryDB(db)
	require.NoError(t, recovery.ApplySubscriberRecoveryCommands(commands))
	require.NoError(t, recovery.ApplySubscriberRecoveryCommands(commands), "durable replay must be idempotent")

	receipt, found, err := db.GetSubscriberReceipt(operation.ChannelID, operation.ChannelType, operation.OperationID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "complete", receipt.State)
	require.Equal(t, 1, receipt.Completed)
	_, pending, err := db.GetSubscriberWork(operation.ChannelID, operation.ChannelType, 1, 1)
	require.NoError(t, err)
	require.False(t, pending)
	members, err := db.GetSubscribers(operation.ChannelID, operation.ChannelType)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "a", members[0].Uid)
}
