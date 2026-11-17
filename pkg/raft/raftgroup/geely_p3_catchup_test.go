package raftgroup

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestP3_GetLogsConcurrencyAndByteBudgetLimiter(t *testing.T) {
	rg := New(NewOptions(
		WithMaxLogSizePerBatch(1024),
		WithMaxLogCountPerBatch(50),
		WithMaxInFlightGetLogs(2),
		WithMaxInFlightGetLogsBytes(2048),
		WithGetLogsTimeout(20*time.Millisecond),
	))

	// Same key cannot acquire two concurrent GetLogs permits.
	release1, ok := rg.tryAcquireGetLogs("ch-1", 1024)
	require.True(t, ok)

	_, ok = rg.tryAcquireGetLogs("ch-1", 1024)
	require.False(t, ok, "duplicate in-flight GetLogs for same key must be rejected")

	// Second distinct key succeeds up to MaxInFlightGetLogs=2 and MaxInFlightGetLogsBytes=2048.
	release2, ok := rg.tryAcquireGetLogs("ch-2", 1024)
	require.True(t, ok)

	// Third distinct key is rejected by global concurrency/byte budget.
	_, ok = rg.tryAcquireGetLogs("ch-3", 1024)
	require.False(t, ok, "third concurrent GetLogs must be rejected when budget is full")

	release1()
	release3, ok := rg.tryAcquireGetLogs("ch-3", 1024)
	require.True(t, ok, "releasing permit must allow next key to proceed")

	release2()
	release3()
	require.Zero(t, rg.inFlightGetLogs)
	require.Zero(t, rg.inFlightGetLogsBytes)
}

func TestP3_EventQueueClearsReferencesOnGet(t *testing.T) {
	q := NewEventQueue(8, false, 0, 0)
	payload := make([]byte, 4096)
	q.MustAdd(Event{
		RaftKey: "ch-1",
		Event: types.Event{
			Type: types.StoreResp,
			Logs: []types.Log{{Index: 1, Term: 1, Data: payload}},
		},
	})
	require.True(t, q.Add(Event{
		RaftKey: "ch-1",
		Event: types.Event{
			Type: types.SyncResp,
			Logs: []types.Log{{Index: 2, Term: 1, Data: payload}},
		},
	}))

	got := q.Get()
	require.Len(t, got, 2)

	// Underlying backing arrays in q.nodrop and q.left/q.right must have been zeroed.
	fullNodrop := q.nodrop[:cap(q.nodrop)]
	for i := range fullNodrop {
		require.Nil(t, fullNodrop[i].Event.Logs, "nodrop backing slot %d must not retain Logs slice", i)
	}
	fullLeft := q.left[:cap(q.left)]
	for i := range fullLeft {
		require.Nil(t, fullLeft[i].Event.Logs, "left backing slot %d must not retain Logs slice", i)
	}
}

func TestP3_PriorityPartitionEvents(t *testing.T) {
	require.True(t, isRealtimeOrControlEvent(types.Ping))
	require.True(t, isRealtimeOrControlEvent(types.Propose))
	require.True(t, isRealtimeOrControlEvent(types.StoreResp))
	require.True(t, isRealtimeOrControlEvent(types.ApplyResp))
	require.True(t, isRealtimeOrControlEvent(types.TruncateReq))
	require.True(t, isRealtimeOrControlEvent(types.TruncateResp))

	require.False(t, isRealtimeOrControlEvent(types.SyncReq))
	require.False(t, isRealtimeOrControlEvent(types.GetLogsReq))
	require.False(t, isRealtimeOrControlEvent(types.GetLogsResp))
	require.False(t, isRealtimeOrControlEvent(types.SyncResp))
}

type captureTransport struct {
	mu     sync.Mutex
	events []types.Event
}

func (c *captureTransport) Send(_ string, e types.Event) {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

func (c *captureTransport) snapshot() []types.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]types.Event(nil), c.events...)
}

func TestHandleGetLogsReqSubmitFailurePreservesToAndIndex(t *testing.T) {
	tr := &captureTransport{}
	rg := New(NewOptions(WithTransport(tr)))
	t.Cleanup(rg.Stop)

	leader := raft.NewNode(1, types.RaftState{
		LastLogIndex: 5,
		LastTerm:     1,
		AppliedIndex: 5,
	}, raft.NewOptions(
		raft.WithKey("ch-submit-fail"),
		raft.WithNodeId(1),
		raft.WithReplicas([]uint64{1, 2}),
	))
	require.NoError(t, leader.Step(types.Event{
		Type: types.ConfChange,
		Config: types.Config{
			Replicas: []uint64{1, 2},
			Leader:   1,
			Term:     1,
			Version:  1,
			Role:     types.RoleLeader,
		},
	}))
	_ = leader.Ready()
	rg.AddRaft(leader)

	// Follower 2 sends SyncReq for index 2; leader sets GetingLogs=true and emits GetLogsReq.
	require.NoError(t, leader.Step(types.Event{
		Type:          types.SyncReq,
		From:          2,
		To:            1,
		Term:          1,
		Index:         2,
		StoredIndex:   2,
		AppliedIndex:  2,
		ConfigVersion: 1,
		Reason:        types.ReasonOnlySync,
	}))
	ready := leader.Ready()
	var getLogsReq *types.Event
	for i := range ready {
		if ready[i].Type == types.GetLogsReq {
			getLogsReq = &ready[i]
			break
		}
	}
	require.NotNil(t, getLogsReq, "expected leader to emit GetLogsReq")
	require.Equal(t, uint64(2), getLogsReq.From)
	require.Equal(t, uint64(2), getLogsReq.Index)

	// Simulate submitWork failure by releasing goPool (so tryAcquireGetLogs succeeds, then submitWork fails
	// and must both release the in-flight permit and preserve To/Index on the ReasonError GetLogsResp).
	rg.goPool.Release()
	rg.handleGetLogsReq(leader, *getLogsReq)
	require.Zero(t, rg.inFlightGetLogs, "submitWork error path must release the acquired GetLogs permit")

	queued := rg.mq.Get()
	require.Len(t, queued, 1)
	resp := queued[0].Event
	require.Equal(t, types.GetLogsResp, resp.Type)
	require.Equal(t, uint64(2), resp.To, "fallback GetLogsResp must preserve requesting replica To=e.From")
	require.Equal(t, uint64(2), resp.Index, "fallback GetLogsResp must preserve requested log Index=e.Index")
	require.Equal(t, types.ReasonError, resp.Reason)

	// Stepping the fallback GetLogsResp into leader must clear GetingLogs and send SyncResp(ReasonError) to node 2.
	require.NoError(t, leader.Step(resp))
	rg.handleReady(leader)

	sent := tr.snapshot()
	var foundSyncErr bool
	for _, ev := range sent {
		if ev.Type == types.SyncResp && ev.To == 2 && ev.Index == 2 && ev.Reason == types.ReasonError {
			foundSyncErr = true
			break
		}
	}
	require.True(t, foundSyncErr, "leader must send SyncResp with To=2, Index=2, Reason=ReasonError after GetLogs submit failure")
}

func TestRaftGroupProposalAdmissionDrainsBeforeLeaderHandoff(t *testing.T) {
	tr := &captureTransport{}
	rg := New(NewOptions(
		WithTransport(tr),
		WithTickInterval(time.Hour),
		WithProposeTimeout(time.Second),
	))
	t.Cleanup(rg.Stop)

	leader := raft.NewNode(1, types.RaftState{
		LastLogIndex: 10,
		LastTerm:     2,
		AppliedIndex: 10,
	}, raft.NewOptions(
		raft.WithKey("slot-drain"),
		raft.WithNodeId(1),
		raft.WithReplicas([]uint64{1, 2}),
		raft.WithAdvance(rg.Advance),
	))
	require.NoError(t, leader.Step(types.Event{
		Type: types.ConfChange,
		Config: types.Config{
			MigrateFrom: 1,
			MigrateTo:   2,
			Replicas:    []uint64{1, 2},
			Leader:      1,
			Term:        2,
			Version:     3,
			Role:        types.RoleLeader,
		},
	}))
	_ = leader.Ready()
	rg.AddRaft(leader)

	// Start a production proposal via ProposeBatchTimeout before the event loop processes mq.
	proposeDone := make(chan error, 1)
	var proposeResps types.ProposeRespSet
	go func() {
		var err error
		proposeResps, err = rg.ProposeBatchTimeout(context.Background(), "slot-drain", types.ProposeReqSet{
			{Id: 501, Data: []byte("in-flight-write")},
		})
		proposeDone <- err
	}()

	// Wait until the proposal has been admitted and queued in rg.mq (signaled via rg.Advance()).
	select {
	case <-rg.advanceC:
	case <-time.After(time.Second):
		t.Fatal("proposal did not enter event queue")
	}

	// Now replica 2 sends SyncReq caught up to index 10 (Index=11, StoredIndex=11, AppliedIndex=11)
	// BEFORE the queued Propose event has been stepped.
	require.NoError(t, leader.Step(types.Event{
		Type:          types.SyncReq,
		From:          2,
		To:            1,
		Term:          2,
		Index:         11,
		StoredIndex:   11,
		AppliedIndex:  11,
		ConfigVersion: 3,
		Reason:        types.ReasonOnlySync,
	}))
	require.True(t, leader.IsDrainingOrPaused(), "leader must enter drainingAdmissions on SyncReq")
	for _, ev := range leader.Ready() {
		require.NotEqual(t, types.FollowerToLeaderReq, ev.Type, "leader must NOT emit FollowerToLeaderReq while admitted proposal 501 is still draining")
	}

	// Process the queued events: admitted proposal 501 must succeed, advance LastLogIndex to 11, and release its admission entry.
	rg.handleReceivedEvents()
	require.NoError(t, <-proposeDone)
	require.Len(t, proposeResps, 1)
	require.Equal(t, uint64(11), proposeResps[0].Index)
	require.Equal(t, uint64(11), leader.LastLogIndex())

	// Any new proposal arriving while drainingAdmissions is active must be rejected before enqueueing.
	_, err := rg.ProposeBatchTimeout(context.Background(), "slot-drain", types.ProposeReqSet{
		{Id: 502, Data: []byte("late-write")},
	})
	require.ErrorIs(t, err, types.ErrProposalDropped)

	// Drain local StoreReq from Ready() then complete StoreResp and ApplyResp on leader.
	_ = leader.Ready()
	require.NoError(t, leader.Step(types.Event{Type: types.StoreResp, Index: 11, Reason: types.ReasonOk}))
	_ = leader.Ready()
	require.NoError(t, leader.Step(types.Event{Type: types.ApplyResp, Index: 11, Reason: types.ReasonOk}))
	require.NoError(t, leader.Step(types.Event{
		Type:          types.SyncReq,
		From:          2,
		To:            1,
		Term:          2,
		Index:         12,
		StoredIndex:   12,
		AppliedIndex:  12,
		ConfigVersion: 3,
		Reason:        types.ReasonOnlySync,
	}))
	var emittedHandoff bool
	for _, ev := range leader.Ready() {
		if ev.Type == types.FollowerToLeaderReq && ev.From == 2 {
			emittedHandoff = true
		}
	}
	require.True(t, emittedHandoff, "leader must emit FollowerToLeaderReq after admitted proposal 501 drained and replicated")

	// Also verify that a cancelled proposal releases its admission entry and cannot wedge future handoffs.
	leaderCancel := raft.NewNode(1, types.RaftState{
		LastLogIndex: 5,
		LastTerm:     2,
		AppliedIndex: 5,
	}, raft.NewOptions(
		raft.WithKey("slot-cancel-drain"),
		raft.WithNodeId(1),
		raft.WithReplicas([]uint64{1, 2}),
	))
	require.NoError(t, leaderCancel.Step(types.Event{
		Type: types.ConfChange,
		Config: types.Config{
			MigrateFrom: 1,
			MigrateTo:   2,
			Replicas:    []uint64{1, 2},
			Leader:      1,
			Term:        2,
			Version:     3,
			Role:        types.RoleLeader,
		},
	}))
	_ = leaderCancel.Ready()
	rg.AddRaft(leaderCancel)

	cancelCtx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	_, err = rg.ProposeBatchTimeout(cancelCtx, "slot-cancel-drain", types.ProposeReqSet{
		{Id: 601, Data: []byte("timed-out-write")},
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_ = rg.mq.Get() // discard unstepped timed-out event

	// Because 601's admission was released on timeout, SyncReq from caught-up replica 2 must immediately emit FollowerToLeaderReq.
	require.NoError(t, leaderCancel.Step(types.Event{
		Type:          types.SyncReq,
		From:          2,
		To:            1,
		Term:          2,
		Index:         6,
		StoredIndex:   6,
		AppliedIndex:  6,
		ConfigVersion: 3,
		Reason:        types.ReasonOnlySync,
	}))
	var emittedAfterCancel bool
	for _, ev := range leaderCancel.Ready() {
		if ev.Type == types.FollowerToLeaderReq && ev.From == 2 {
			emittedAfterCancel = true
		}
	}
	require.True(t, emittedAfterCancel, "cancelled/timed-out proposal must release its admission entry so handoff is not wedged")
}
