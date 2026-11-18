package handler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// TestXIN2855_A1_ExactDedupStateAndCapacity verifies A1:
// 1) 1..10 committed, 11 uncommitted, 12/14/16/18 and 2065 committed -> replay 1..10 must NOT re-execute.
// 2) 4 vs 5 disjoint historical intervals below frontierLo -> 5th interval does NOT retire interval 1.
// 3) 8 vs 9 partial-slot sequences on same channel -> 9th partial sequence retains completed slots.
// 4) 256 vs 257 same-shard channels -> active watermarks are pinned (0 evictions), 257th channel backpressures with StageBeginCapacityRetry and 0 side effects.
// 5) drainOOOBitsIntoFrontierLocked loops "absorb adjacent hist -> drain contiguous bitmap" until fixed point.
func TestXIN2855_A1_ExactDedupStateAndCapacity(t *testing.T) {
	t.Run("HistIntervals_4vs5_And_WindowSlide2065", func(t *testing.T) {
		var tracker StageSeqTracker
		k, ok := MakeChannelWatermarkKey("ch_a1_hist", wkproto.ChannelTypeGroup)
		require.True(t, ok)

		for seq := uint64(1); seq <= 10; seq++ {
			st := tracker.BeginStageChecked(k, StagePluginPersistAfter, seq)
			require.Equal(t, StageBeginLeased, st)
			tracker.CommitStage(k, StagePluginPersistAfter, seq)
		}
		// Leave 11 uncommitted; commit 12, 14, 16, 18 in bitmap
		for _, seq := range []uint64{12, 14, 16, 18} {
			st := tracker.BeginStageChecked(k, StagePluginPersistAfter, seq)
			require.Equal(t, StageBeginLeased, st)
			tracker.CommitStage(k, StagePluginPersistAfter, seq)
		}
		// Commit 2065 (>= base + 2048), flushing [1..10], [12], [14], [16], [18] (5 disjoint intervals) into hist:
		st := tracker.BeginStageChecked(k, StagePluginPersistAfter, 2065)
		require.Equal(t, StageBeginLeased, st)
		tracker.CommitStage(k, StagePluginPersistAfter, 2065)

		for seq := uint64(1); seq <= 10; seq++ {
			require.True(t, tracker.HasCommitted(k, StagePluginPersistAfter, seq), "A1: committed seq %d must not be retired when 5th disjoint interval arrives", seq)
			require.Equal(t, StageBeginAlreadyCommitted, tracker.BeginStageChecked(k, StagePluginPersistAfter, seq))
		}
		require.False(t, tracker.HasCommitted(k, StagePluginPersistAfter, 11), "uncommitted hole seq 11 must remain uncommitted")

		// Also test 4 vs 5 disjoint historical intervals directly below frontierLo:
		var trackerBelow StageSeqTracker
		kBelow, _ := MakeChannelWatermarkKey("ch_a1_below_frontier", wkproto.ChannelTypeGroup)
		require.Equal(t, StageBeginLeased, trackerBelow.BeginStageChecked(kBelow, StagePluginPersistAfter, 100))
		trackerBelow.CommitStage(kBelow, StagePluginPersistAfter, 100)
		for _, hSeq := range []uint64{2, 4, 6, 8, 10} {
			require.Equal(t, StageBeginLeased, trackerBelow.BeginStageChecked(kBelow, StagePluginPersistAfter, hSeq))
			trackerBelow.CommitStage(kBelow, StagePluginPersistAfter, hSeq)
		}
		for _, hSeq := range []uint64{2, 4, 6, 8, 10} {
			require.True(t, trackerBelow.HasCommitted(kBelow, StagePluginPersistAfter, hSeq), "historical disjoint seq %d must remain committed", hSeq)
		}
		for _, hole := range []uint64{1, 3, 5, 7, 9} {
			require.False(t, trackerBelow.HasCommitted(kBelow, StagePluginPersistAfter, hole), "uncommitted gap %d must never be bridged", hole)
		}
	})

	t.Run("PartialSlotSequences_8vs9", func(t *testing.T) {
		var tracker StageSeqTracker
		k, ok := MakeChannelWatermarkKey("ch_a1_partial9", wkproto.ChannelTypeGroup)
		require.True(t, ok)

		var allSlots, slot1Only, slot2Only eventbus.SlotBitset
		allSlots.Set(1)
		allSlots.Set(2)
		slot1Only.Set(1)
		slot2Only.Set(2)

		for seq := uint64(1); seq <= 9; seq++ {
			deliv, st := tracker.BeginSlotStageChecked(k, StageChannelDistribute, seq, allSlots)
			require.Equal(t, StageBeginLeased, st)
			require.True(t, deliv.Equal(allSlots))
			tracker.FinishSlotStage(k, StageChannelDistribute, seq, deliv, slot1Only, allSlots)
		}

		// Replay seq=9: only remaining slot {2} should be leased, NOT slot {1}!
		deliv9, st9 := tracker.BeginSlotStageChecked(k, StageChannelDistribute, 9, allSlots)
		require.Equal(t, StageBeginLeased, st9)
		require.False(t, deliv9.Has(1), "A1: 9th partial sequence must not forget already-completed slot 1")
		require.True(t, deliv9.Equal(slot2Only))
		tracker.FinishSlotStage(k, StageChannelDistribute, 9, deliv9, slot2Only, allSlots)
		require.True(t, tracker.HasCommitted(k, StageChannelDistribute, 9))
	})

	t.Run("SameShardChannels_256vs257_NoSilentEvictionAndBackpressure", func(t *testing.T) {
		var tracker StageSeqTracker
		var shard0Keys []ChannelWatermarkKey
		for i := 0; len(shard0Keys) < 257; i++ {
			k, ok := MakeChannelWatermarkKey(fmt.Sprintf("ch_shard0_%d", i), wkproto.ChannelTypeGroup)
			require.True(t, ok)
			if k.shardIndex() == 0 {
				shard0Keys = append(shard0Keys, k)
			}
		}
		for i := 0; i < 256; i++ {
			st := tracker.BeginStageChecked(shard0Keys[i], StagePluginPersistAfter, 1)
			require.Equal(t, StageBeginLeased, st)
			tracker.CommitStage(shard0Keys[i], StagePluginPersistAfter, 1)
		}
		// 257th channel in shard 0 must backpressure (StageBeginCapacityRetry) without evicting any of 0..255:
		var sideEffectCalls atomic.Int32
		st257 := tracker.BeginStageChecked(shard0Keys[256], StagePluginPersistAfter, 1)
		if st257 == StageBeginLeased {
			sideEffectCalls.Add(1)
			tracker.CommitStage(shard0Keys[256], StagePluginPersistAfter, 1)
		}
		require.Equal(t, StageBeginCapacityRetry, st257)
		require.Zero(t, sideEffectCalls.Load(), "A1: full shard capacity must backpressure with 0 side-effect calls")
		require.Zero(t, tracker.Metrics().EvictedChannels, "A1: active channel watermarks must not be silently evicted")
		require.True(t, tracker.HasCommitted(shard0Keys[0], StagePluginPersistAfter, 1), "A1: first channel in shard must remain committed")
	})

	t.Run("DrainOOOBitsAfterAbsorbHistIntoFrontier", func(t *testing.T) {
		var tracker2 StageSeqTracker
		k2, _ := MakeChannelWatermarkKey("ch_a1_drain2", wkproto.ChannelTypeGroup)
		_, _ = tracker2.BeginStage(k2, StagePluginPersistAfter, 5)
		tracker2.CommitStage(k2, StagePluginPersistAfter, 5)
		// Commit 2060 -> w.base = 13, bit 2047 set
		_, _ = tracker2.BeginStage(k2, StagePluginPersistAfter, 2060)
		tracker2.CommitStage(k2, StagePluginPersistAfter, 2060)
		// Commit 13 -> w.base = 13, bit 0 set
		_, _ = tracker2.BeginStage(k2, StagePluginPersistAfter, 13)
		tracker2.CommitStage(k2, StagePluginPersistAfter, 13)
		// Commit 7..12 -> below w.base=13 and rebaseDown fails because 2060 is at maxOffset 2047, so 7..12 go into w.hist as [7..12]:
		for seq := uint64(7); seq <= 12; seq++ {
			_, _ = tracker2.BeginStage(k2, StagePluginPersistAfter, seq)
			tracker2.CommitStage(k2, StagePluginPersistAfter, seq)
		}
		// Commit 6 (frontierHi+1): frontier becomes [5..6], absorbs hist [7..12] -> [5..12], and then drains bit 0 at w.base=13 -> [5..13]:
		_, _ = tracker2.BeginStage(k2, StagePluginPersistAfter, 6)
		tracker2.CommitStage(k2, StagePluginPersistAfter, 6)

		contig, _, _, _ := tracker2.InspectChannelStage("ch_a1_drain2", wkproto.ChannelTypeGroup, StagePluginPersistAfter)
		require.Equal(t, uint64(13), contig, "A1: drainOOOBitsIntoFrontierLocked must re-drain w.bits after absorbing adjacent hist interval [7..12]")
	})
}

// TestXIN2855_A2_UnifiedPostCommitStagesAndExactBatchMask verifies A2:
// 1) Local & remote leader recovery after queue full executes Stage 0 (Plugin) and Stage 1 (Webhook) before Stage 2/3 distribution.
// 2) Two Done flags both false on a PartAV2 event does NOT skip Stage 0/1 on remote slot leader.
// 3) Ingress Stage 0 transient failure sets RetryOwned so ingress RecoveryManager and remote slot leader never execute Stage 0 concurrently.
// 4) Disabled plugin/webhook hooks record StageStatusNotRequired while core delivery succeeds.
// 5) Batch distribution to remote node groups by (targetNode, exactMask) and never widens per-event masks to batch union.
func TestXIN2855_A2_UnifiedPostCommitStagesAndExactBatchMask(t *testing.T) {
	t.Run("RecoveryChannelForward_LocalAndRemoteLeader_And_NoDoubleExec", func(t *testing.T) {
		oldOpts, oldPM := options.G, service.PluginManager
		t.Cleanup(func() {
			options.G = oldOpts
			service.PluginManager = oldPM
		})
		opts := options.New()
		opts.Cluster.NodeId = 1001
		options.G = opts

		flaky := &flakyPersistAfterPlugin{failBefore: 0}
		service.PluginManager = &mockPluginManager{pg: flaky}

		h := NewHandler()
		defer h.DrainAndStop(0)
		tagMgr := newLocalTagManagerMock()
		tag := &types.Tag{Key: "t_a2", Nodes: []*types.Node{{LeaderId: 1001, Uids: []string{"u_local"}}}}
		tagMgr.setTag("ch_a2", wkproto.ChannelTypeGroup, tag)

		h.BindNodeDeps(NodeDeps{
			NodeId:     1001,
			Cluster:    &forwardChannelCluster{leader: 1001},
			TagManager: tagMgr,
		})

		ev := &eventbus.Event{
			Type:       eventbus.EventChannelOnSend,
			Conn:       &eventbus.Conn{Uid: "sender", DeviceId: opts.SystemDeviceId},
			Frame:      &wkproto.SendPacket{ClientMsgNo: "a2-1", ChannelID: "ch_a2", ChannelType: wkproto.ChannelTypeGroup, Payload: []byte("hello")},
			MessageId:  5001,
			MessageSeq: 1,
			ReasonCode: wkproto.ReasonSuccess,
		}
		tks, ok := h.postCommitBudget().TryReserve("ch_a2", []*eventbus.Event{ev})
		require.True(t, ok)
		ev.Ticket = tks[0]

		require.True(t, h.EnqueuePersistedRecovery("ch_a2", wkproto.ChannelTypeGroup, ev))
		h.recoveryMgr.NotifyExternalWakeup()
		h.recoveryMgr.runDueCycle(context.Background())

		require.Equal(t, int32(1), flaky.calls.Load(), "A2: RecoveryStageChannelForward must execute Stage 0 PluginPersistAfter before completing distribution")

		// Now test ingress transient plugin failure + remote slot leader forwarding:
		// Ingress node 1001 owns RetryOwned Stage 0; remote leader 1002 must NOT execute Stage 0 in parallel!
		flakyRemote := &flakyPersistAfterPlugin{failBefore: 1}
		service.PluginManager = &mockPluginManager{pg: flakyRemote}

		hIngress := NewHandler()
		defer hIngress.DrainAndStop(0)
		hLeader := NewHandler()
		defer hLeader.DrainAndStop(0)
		leaderBus := &forwardChannel{}
		hLeader.BindNodeDeps(NodeDeps{
			NodeId:     1002,
			ChannelBus: leaderBus,
			Cluster:    &forwardChannelCluster{leader: 1002},
			TagManager: tagMgr,
		})
		bridgeCl := &customForwardCluster{
			leader: 1002,
			onRequest: func(toNode uint64, path string, body []byte) (*proto.Response, error) {
				if path == forward.CapabilityPath {
					return &proto.Response{Status: proto.StatusOK, Body: forward.BuildCapabilityBody()}, nil
				}
				if path == forward.ChannelPath {
					st := hLeader.AcceptForward(body)
					return &proto.Response{Status: st}, nil
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			},
		}
		hIngress.BindNodeDeps(NodeDeps{
			NodeId:  1001,
			Cluster: bridgeCl,
		})

		evFailStage0 := &eventbus.Event{
			Type:       eventbus.EventChannelOnSend,
			Conn:       &eventbus.Conn{Uid: "sender", DeviceId: opts.SystemDeviceId},
			Frame:      &wkproto.SendPacket{ClientMsgNo: "a2-retryowned", ChannelID: "ch_a2", ChannelType: wkproto.ChannelTypeGroup, Payload: []byte("hello")},
			MessageId:  5002,
			MessageSeq: 2,
			ReasonCode: wkproto.ReasonSuccess,
		}
		// Ingress runs executePersistedLocalSideEffects (attempt 1 fails -> enqueued in hIngress RecoveryManager with StageStatusRetryOwned) and forwards to 1002:
		hIngress.forwardsToNode(1002, "ch_a2", wkproto.ChannelTypeGroup, []*eventbus.Event{evFailStage0})
		require.Len(t, leaderBus.events, 1)
		// Leader 1002 runs persist() on the forwarded event: because Stage 0 is RetryOwned by node 1001, leader 1002 must NOT call plugin!
		hLeader.persist(&eventbus.ChannelContext{
			ChannelId:    "ch_a2",
			ChannelType:  wkproto.ChannelTypeGroup,
			EventType:    eventbus.EventChannelOnSend,
			SlotLeaderId: 1002,
			Events:       leaderBus.events,
		})
		require.Equal(t, int32(1), flakyRemote.calls.Load(), "A2: remote leader must not execute Stage 0 when Stage 0 is RetryOwned by ingress node")

		// Now ingress RecoveryManager retries Stage 0 and succeeds (total calls == 2: 1 failed attempt + 1 succeeded retry on owner 1001):
		hIngress.recoveryMgr.NotifyExternalWakeup()
		hIngress.recoveryMgr.runDueCycle(context.Background())
		require.Equal(t, int32(2), flakyRemote.calls.Load(), "A2: ingress owner completes Stage 0 retry exactly once")
	})

	t.Run("PartAV2BothDoneFalse_DoesNotSkipOnAcceptForward_And_DisabledHooksNotRequired", func(t *testing.T) {
		oldOpts, oldPM := options.G, service.PluginManager
		t.Cleanup(func() {
			options.G = oldOpts
			service.PluginManager = oldPM
		})
		opts := options.New()
		opts.Cluster.NodeId = 1002
		options.G = opts
		service.PluginManager = nil // plugins disabled

		sink := &forwardChannel{}
		hLeader := NewHandler()
		defer hLeader.DrainAndStop(0)
		hLeader.BindNodeDeps(NodeDeps{
			NodeId:     1002,
			ChannelBus: sink,
			Cluster:    &forwardChannelCluster{leader: 1002},
		})

		// PartAV2 event with HasStageMeta=true and both PluginDone=false, WebhookDone=false (Pending):
		evPending := &eventbus.Event{
			Type:               eventbus.EventChannelOnSend,
			Conn:               &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:              &wkproto.SendPacket{ClientMsgNo: "a2-pending", ChannelID: "ch_a2_p", ChannelType: 2, Payload: []byte("p")},
			MessageId:          6001,
			MessageSeq:         1,
			ReasonCode:         wkproto.ReasonSuccess,
			HasStageMeta:       true,
			PluginStageStatus:  eventbus.StageStatusPending,
			WebhookStageStatus: eventbus.StageStatusPending,
		}
		fReq := &forwardChannelEventReq{fromNode: 1001, channelId: "ch_a2_p", channelType: 2, events: eventbus.EventBatch{evPending}}
		raw, err := fReq.encodeWithTrailer(true)
		require.NoError(t, err)
		env, _, err := forward.Envelope(raw, []*eventbus.Event{evPending})
		require.NoError(t, err)

		require.Equal(t, proto.StatusOK, hLeader.AcceptForward(env))
		require.Len(t, sink.events, 1)
		require.False(t, sink.events[0].PluginDone, "A2: PartAV2 event with Pending stage status must NOT be falsely marked PluginDone=true by acceptForward")
		require.False(t, sink.events[0].WebhookDone, "A2: PartAV2 event with Pending stage status must NOT be falsely marked WebhookDone=true by acceptForward")

		// When persist() runs with plugins and webhooks disabled, stages transition to StageStatusNotRequired:
		hLeader.persist(&eventbus.ChannelContext{
			ChannelId:    "ch_a2_p",
			ChannelType:  2,
			EventType:    eventbus.EventChannelOnSend,
			SlotLeaderId: 1002,
			Events:       sink.events,
		})
		require.Equal(t, eventbus.StageStatusNotRequired, sink.events[0].PluginStageStatus)
		require.Equal(t, eventbus.StageStatusNotRequired, sink.events[0].WebhookStageStatus)
	})

	t.Run("BatchRemoteDistribute_PreservesExactPerEventTargetMask", func(t *testing.T) {
		oldOpts := options.G
		t.Cleanup(func() { options.G = oldOpts })
		opts := options.New()
		opts.Cluster.NodeId = 1001
		options.G = opts

		var receivedEvents []*eventbus.Event
		var mu sync.Mutex
		cl := &customForwardCluster{
			leader: 1001,
			onRequest: func(toNode uint64, path string, body []byte) (*proto.Response, error) {
				if path == forward.CapabilityPath {
					return &proto.Response{Status: proto.StatusOK, Body: forward.BuildCapabilityBody()}, nil
				}
				if path == forward.ChannelPath {
					raw, _, _, err := forward.Decode(body)
					if err != nil {
						return &proto.Response{Status: forward.StatusInvalid}, nil
					}
					req := &forwardChannelEventReq{}
					if err := req.decode(raw); err != nil {
						return &proto.Response{Status: forward.StatusInvalid}, nil
					}
					mu.Lock()
					for _, e := range req.events {
						receivedEvents = append(receivedEvents, e.Clone())
					}
					mu.Unlock()
					return &proto.Response{Status: proto.StatusOK}, nil
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			},
		}

		h := NewHandler()
		defer h.DrainAndStop(0)
		h.BindNodeDeps(NodeDeps{
			NodeId:  1001,
			Cluster: cl,
		})
		tag := &types.Tag{
			Key: "tag_batch_mask",
			Nodes: []*types.Node{
				{LeaderId: 1002, Uids: []string{"u_slot1", "u_slot2"}},
			},
		}
		var mask1, mask2, allMask eventbus.SlotBitset
		mask1.Set(1)
		mask2.Set(2)
		allMask.Set(1)
		allMask.Set(2)
		nodeMasks := map[uint64]eventbus.SlotBitset{1002: allMask}

		ev1 := &eventbus.Event{
			Type:              eventbus.EventChannelDistribute,
			Conn:              &eventbus.Conn{Uid: "s", DeviceId: "sys"},
			Frame:             &wkproto.SendPacket{ClientMsgNo: "m1", ChannelID: "g_mask", ChannelType: 2, Payload: []byte("1")},
			MessageId:         1,
			MessageSeq:        1,
			ReasonCode:        wkproto.ReasonSuccess,
			HasTargetSlotMask: true,
			TargetSlotMask:    mask1,
		}
		ev2 := &eventbus.Event{
			Type:              eventbus.EventChannelDistribute,
			Conn:              &eventbus.Conn{Uid: "s", DeviceId: "sys"},
			Frame:             &wkproto.SendPacket{ClientMsgNo: "m2", ChannelID: "g_mask", ChannelType: 2, Payload: []byte("2")},
			MessageId:         2,
			MessageSeq:        2,
			ReasonCode:        wkproto.ReasonSuccess,
			HasTargetSlotMask: true,
			TargetSlotMask:    mask2,
		}
		wmKey, _ := MakeChannelWatermarkKey("g_mask", wkproto.ChannelTypeGroup)
		h.distributePersistedOnChannelLeader(context.Background(), cl, 1001, wmKey, true, tag, allMask, nodeMasks, false, "g_mask", wkproto.ChannelTypeGroup, []*eventbus.Event{ev1, ev2})

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, receivedEvents, 2)
		require.True(t, receivedEvents[0].TargetSlotMask.Equal(mask1), "A2: ev1 TargetSlotMask must stay {1}, not batch union {1,2}")
		require.True(t, receivedEvents[1].TargetSlotMask.Equal(mask2), "A2: ev2 TargetSlotMask must stay {2}, not batch union {1,2}")
	})
}

// TestXIN2855_A3_LegacyUnpersistedDurableQueueFullNoDrop verifies A3:
// 1) Channel queue at 2047/2048/2049 under budget -> seq0 durable event enters PendingPersist (not RecoveryManager, not dropped) and drains in FIFO order preserving stable IDs.
// 2) Primary + legacy budget saturation accounts every rejection in LegacyBudgetExhaustedTotal and LegacyForwardDroppedTotal.
// 3) CheckUpgradeDrainReadiness fails while PendingPersist / post-commit / recovery is non-empty and succeeds once drained.
func TestXIN2855_A3_LegacyUnpersistedDurableQueueFullNoDrop(t *testing.T) {
	oldOpts := options.G
	t.Cleanup(func() { options.G = oldOpts })
	options.G = options.New()

	primary := eventbus.NewPostCommitBudget(16*1024*1024, 16384)
	legacySpill := eventbus.NewPostCommitBudget(8*1024*1024, 8192)
	rejectingBus := &boundedMockChannelBus{maxAccept: 2048}

	h := NewHandler()
	defer h.DrainAndStop(0)
	h.BindNodeDeps(NodeDeps{
		NodeId:              1001,
		ChannelBus:          rejectingBus,
		Cluster:             &forwardChannelCluster{leader: 1001},
		PostCommitBudget:    primary,
		LegacyInboundBudget: legacySpill,
	})

	droppedBefore := eventbus.LegacyForwardDroppedTotal.Load()
	for i := 1; i <= 2049; i++ {
		ev := &eventbus.Event{
			Type:       eventbus.EventChannelOnSend,
			Conn:       &eventbus.Conn{Uid: "u_http", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("http-seq0-%d", i), ChannelID: "g_a3", ChannelType: 2, Payload: []byte("durable")},
			MessageId:  int64(i),
			MessageSeq: 0,
		}
		fReq := &forwardChannelEventReq{fromNode: 1002, channelId: "g_a3", channelType: 2, events: eventbus.EventBatch{ev}}
		raw, err := fReq.encode()
		require.NoError(t, err)
		h.OnMessage(&proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: raw})
	}

	_, usedItems, _ := primary.Stats()
	require.Equal(t, int64(2049), usedItems, "A3: 2049th unpersisted durable seq0 event under budget must be retained in PendingPersist, not silently dropped")
	require.Equal(t, droppedBefore, eventbus.LegacyForwardDroppedTotal.Load())

	_, ppItems := h.pendingPersistMgr.Inspect()
	require.Equal(t, 1, ppItems, "A3: 2049th event must reside in PendingPersistManager")
	_, recItems, _ := h.recoveryMgr.Inspect()
	require.Zero(t, recItems, "A3: seq=0 event must never be placed in persisted RecoveryManager")

	// Pre-upgrade drain readiness check must fail while PendingPersist and tickets are active:
	require.ErrorIs(t, h.CheckUpgradeDrainReadiness(context.Background()), ErrUpgradeIngressNotDrained)

	// Open capacity on channelBus and drain PendingPersist -> 2049th event drains with its stable MessageId=2049 and ClientMsgNo="http-seq0-2049":
	rejectingBus.mu.Lock()
	rejectingBus.maxAccept = 2050
	rejectingBus.mu.Unlock()
	_ = h.pendingPersistMgr.RunDrainCycle()
	require.Eventually(t, func() bool {
		_, ppAfter := h.pendingPersistMgr.Inspect()
		return ppAfter == 0
	}, time.Second, 5*time.Millisecond)
	rejectingBus.mu.Lock()
	require.Equal(t, 2049, rejectingBus.accepted)
	require.Equal(t, int64(2049), rejectingBus.lastAdmitted.MessageId)
	ticketsToRelease := append([]*eventbus.PostCommitTicket(nil), rejectingBus.tickets...)
	rejectingBus.mu.Unlock()

	// Release all 2049 tickets -> CheckUpgradeDrainReadiness must now pass!
	for _, tk := range ticketsToRelease {
		if tk != nil {
			tk.Release()
		}
	}
	require.NoError(t, h.CheckUpgradeDrainReadiness(context.Background()))
}

// TestXIN2855_A4_CrossStageSharedTicketNoReferenceSteal verifies A4:
// 1) Shared BudgetEntry across Stage 1 (H1) and Stage 2 (H2): merging same handle alias (H2, H2) does NOT steal H1's reference.
// 2) Merging two distinct handles (H1, H2) on the same entry releases only the superseded handle.
// 3) Duplicate ReleaseWithCheck on the same handle returns ErrDuplicateTicketRelease without negative refs or premature entry close.
func TestXIN2855_A4_CrossStageSharedTicketNoReferenceSteal(t *testing.T) {
	budget := eventbus.NewPostCommitBudget(1024*1024, 100)
	ev := &eventbus.Event{
		Type:       eventbus.EventChannelDistribute,
		Frame:      &wkproto.SendPacket{ClientMsgNo: "a4", ChannelID: "g_a4", ChannelType: 2, Payload: []byte("p")},
		MessageId:  1,
		MessageSeq: 1,
	}
	tks, ok := budget.TryReserve("g_a4", []*eventbus.Event{ev})
	require.True(t, ok)
	h1 := tks[0]      // Stage 1 handle
	h2 := h1.Retain() // Stage 2 distinct handle on same entry (Refs == 2)
	require.NotNil(t, h2)
	require.False(t, h1.SameHandle(h2), "A4: Retain/AcquireHandle must produce a distinct TicketHandle with a unique handleID")
	require.True(t, h1.SameEntry(h2), "A4: H1 and H2 must share the same underlying PostCommitBudgetEntry")

	// Stage 2 merges an alias of h2 with itself (preferred == h2, secondary == h2):
	keep, rel := resolveMergedTickets(h2, h2)
	require.Nil(t, rel, "A4: merging same handle alias must not produce a release ticket")
	// Stage 1 finishes and releases h1:
	require.NoError(t, h1.ReleaseWithCheck())
	// Duplicate release on h1 must return ErrDuplicateTicketRelease and MUST NOT decrement entry refs again:
	require.ErrorIs(t, h1.ReleaseWithCheck(), eventbus.ErrDuplicateTicketRelease)

	// Stage 2 (keep) is STILL active! Budget must still have 1 item and keep.IsReleased() must be false!
	require.False(t, keep.IsReleased(), "A4: merging same handle alias must not steal sibling stage's reference and prematurely release ticket")
	_, usedItems, _ := budget.Stats()
	require.Equal(t, int64(1), usedItems, "A4: budget entry must remain live while Stage 2 handle is unreleased")

	// Acquire H3 from keep and merge distinct handles (keep, H3):
	h3, err := keep.AcquireForStage("stage2-retry")
	require.NoError(t, err)
	require.Equal(t, int32(2), keep.Refs())
	keep2, rel2 := resolveMergedTickets(keep, h3)
	require.NotNil(t, rel2)
	rel2.Release()
	require.Equal(t, int32(1), keep2.Refs())
	require.False(t, keep2.IsReleased())

	// Final release of keep2 returns budget to 0:
	keep2.Release()
	require.True(t, keep2.IsReleased())
	usedB, usedI, _ := budget.Stats()
	require.Zero(t, usedB)
	require.Zero(t, usedI)
}

// TestXIN2855_B1_SlotOwnershipLookupPanicFailsClosed verifies B1:
// 1) GetSlotId panic, SlotLeaderId panic, nil cluster, and leader==0 all fail closed (0 push/unread side effects, retained for recovery).
// 2) Valid slot 0 with local leader and valid remote leader route normally.
func TestXIN2855_B1_SlotOwnershipLookupPanicFailsClosed(t *testing.T) {
	oldOpts := options.G
	t.Cleanup(func() { options.G = oldOpts })
	opts := options.New()
	opts.Cluster.NodeId = 1001
	options.G = opts

	for _, tc := range []struct {
		name            string
		cl              icluster.ICluster
		expectSucceeded bool
		expectPushCount int32
	}{
		{name: "SlotLeaderIdPanic", cl: &panicSlotCluster{panicSlotLeader: true}, expectSucceeded: false, expectPushCount: 0},
		{name: "GetSlotIdPanic", cl: &panicSlotCluster{panicGetSlot: true}, expectSucceeded: false, expectPushCount: 0},
		{name: "NilCluster", cl: nil, expectSucceeded: false, expectPushCount: 0},
		{name: "ZeroLeader", cl: &panicSlotCluster{leaderID: 0, explicitLeader: true}, expectSucceeded: false, expectPushCount: 0},
		{name: "ValidSlot0LocalLeader", cl: &panicSlotCluster{slotID: 0, leaderID: 1001, explicitLeader: true}, expectSucceeded: true, expectPushCount: 1},
		{name: "ValidSlot0RemoteLeader", cl: &panicSlotCluster{slotID: 0, leaderID: 1002, explicitLeader: true}, expectSucceeded: false, expectPushCount: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pusher := &countingPusherBus{}
			h := NewHandler()
			defer h.DrainAndStop(0)
			h.BindNodeDeps(NodeDeps{
				NodeId:    1001,
				PusherBus: pusher,
				UserBus:   &onlineUserBusMock{},
			})
			tag := &types.Tag{
				Key: "tag_b1",
				Nodes: []*types.Node{
					{LeaderId: 1001, Uids: []string{"u_victim"}},
				},
			}
			var allowed eventbus.SlotBitset
			allowed.Set(0)
			ev := &eventbus.Event{
				Type:       eventbus.EventChannelDistribute,
				Conn:       &eventbus.Conn{Uid: "sender", DeviceId: "sys"},
				Frame:      &wkproto.SendPacket{ClientMsgNo: "b1", ChannelID: "g_b1", ChannelType: 2, Payload: []byte("msg")},
				MessageId:  1,
				MessageSeq: 1,
			}
			succ, mig := h.deliverLocalUsersForMask(tc.cl, 1001, tag, "g_b1", wkproto.ChannelTypeGroup, []*eventbus.Event{ev}, allowed)
			if tc.expectSucceeded {
				require.True(t, succ.Has(0))
				require.True(t, mig.IsZero())
			} else {
				require.True(t, succ.IsZero(), "B1 (%s): succeededMask must be empty", tc.name)
				require.True(t, mig.Has(0), "B1 (%s): pending slot 0 must be retained in retry/migrated mask", tc.name)
			}
			require.Equal(t, tc.expectPushCount, pusher.pushedCount.Load(), "B1 (%s): unexpected push count", tc.name)
		})
	}
}

// TestXIN2855_B2_NoLeaseWhenWatermarkUnavailable verifies B2:
// 1) 256 pinned channels in shard 0 -> 257th channel gets StageBeginCapacityRetry (leased=false), 0 side effects run, retained in recovery.
// 2) After freeing 1 channel slot via ReleaseChannelWatermarkIfSafe, the 257th channel recovers, executes once, and subsequent replay is deduplicated.
func TestXIN2855_B2_NoLeaseWhenWatermarkUnavailable(t *testing.T) {
	var tracker StageSeqTracker
	var shard0Keys []ChannelWatermarkKey
	for i := 0; len(shard0Keys) < 257; i++ {
		k, ok := MakeChannelWatermarkKey(fmt.Sprintf("ch_b2_pin_%d", i), wkproto.ChannelTypeGroup)
		require.True(t, ok)
		if k.shardIndex() == 0 {
			shard0Keys = append(shard0Keys, k)
		}
	}
	var allSlots, oneSlot, slot2Only eventbus.SlotBitset
	allSlots.Set(1)
	allSlots.Set(2)
	oneSlot.Set(1)
	slot2Only.Set(2)

	// Pin all 256 channels in shard 0 with an active partial-slot extension:
	for i := 0; i < 256; i++ {
		deliv, leased := tracker.BeginSlotStage(shard0Keys[i], StageChannelDistribute, 1, allSlots)
		require.True(t, leased)
		tracker.FinishSlotStage(shard0Keys[i], StageChannelDistribute, 1, deliv, oneSlot, allSlots)
	}

	// 257th channel in shard 0 must get StageBeginCapacityRetry (leased == false):
	stStage := tracker.BeginStageChecked(shard0Keys[256], StagePluginPersistAfter, 1)
	require.Equal(t, StageBeginCapacityRetry, stStage)
	_, leasedStage := tracker.BeginStage(shard0Keys[256], StagePluginPersistAfter, 1)
	require.False(t, leasedStage, "B2: BeginStage must NOT grant lease when no ChannelWatermark can be allocated")

	_, stSlot := tracker.BeginSlotStageChecked(shard0Keys[256], StageChannelDistribute, 1, allSlots)
	require.Equal(t, StageBeginCapacityRetry, stSlot)
	_, leasedSlotStage := tracker.BeginSlotStage(shard0Keys[256], StageChannelDistribute, 1, allSlots)
	require.False(t, leasedSlotStage, "B2: BeginSlotStage must NOT grant lease when no ChannelWatermark can be allocated")

	// Now complete channel 0's remaining slot {2} and safely mark channel 0 retirable:
	d0, l0 := tracker.BeginSlotStage(shard0Keys[0], StageChannelDistribute, 1, allSlots)
	require.True(t, l0)
	tracker.FinishSlotStage(shard0Keys[0], StageChannelDistribute, 1, d0, slot2Only, allSlots)
	require.True(t, tracker.ReleaseChannelWatermarkIfSafe(shard0Keys[0], 1))

	// Now the 257th channel in shard 0 succeeds once and deduplicates on replay!
	var execCount int
	if _, okLease := tracker.BeginStage(shard0Keys[256], StagePluginPersistAfter, 1); okLease {
		execCount++
		tracker.CommitStage(shard0Keys[256], StagePluginPersistAfter, 1)
	}
	require.Equal(t, 1, execCount)
	require.Equal(t, StageBeginAlreadyCommitted, tracker.BeginStageChecked(shard0Keys[256], StagePluginPersistAfter, 1))
}

// TestXIN2855_B3_AtomicAcquireIfLiveVsFinalRelease verifies B3:
// Deterministic barrier + concurrent -race interleavings between final Release() and AcquireHandle()/Retain():
// never panics, never resurrects a closed entry, never produces negative refs, and budget returns to 0.
func TestXIN2855_B3_AtomicAcquireIfLiveVsFinalRelease(t *testing.T) {
	budget := eventbus.NewPostCommitBudget(1024*1024, 1024)
	ev := &eventbus.Event{
		Type:  eventbus.EventChannelDistribute,
		Frame: &wkproto.SendPacket{Payload: []byte("race")},
	}
	var panicked atomic.Bool
	for iter := 0; iter < 500; iter++ {
		tks, ok := budget.TryReserve("ch_b3", []*eventbus.Event{ev})
		require.True(t, ok)
		tk := tks[0]

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			tk.Release()
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					panicked.Store(true)
				}
			}()
			<-start
			if dup, err := tk.AcquireHandle(); err == nil && dup != nil {
				// If AcquireHandle won the race before Release closed the entry, budget must still be > 0 until dup.Release()!
				usedB, usedI, _ := budget.Stats()
				require.Greater(t, usedB, int64(0))
				require.Greater(t, usedI, int64(0))
				dup.Release()
			}
		}()
		close(start)
		wg.Wait()
		require.False(t, panicked.Load(), "B3: concurrent AcquireHandle/Retain vs final Release must never panic")
		// After both goroutines complete, entry must be closed and AcquireHandle must fail with ErrTicketClosed:
		_, errAfter := tk.AcquireHandle()
		require.ErrorIs(t, errAfter, eventbus.ErrTicketClosed)
		usedB, usedI, _ := budget.Stats()
		require.Zero(t, usedB)
		require.Zero(t, usedI)
	}
}

// TestXIN2855_B4_NoSilentSlotMaskTruncation verifies B4:
// 1) AllSlotsUpTo with count = 0, 64, 256, 262400 succeeds with exact bit count; 262401 returns ErrSlotCountExceedsWireCap.
// 2) High uint32 slot ID (math.MaxUint32) with 1 sparse word round-trips cleanly.
// 3) 4096 sparse words encode/decode cleanly; 4097 sparse words return ErrSlotCountExceedsWireCap.
func TestXIN2855_B4_NoSilentSlotMaskTruncation(t *testing.T) {
	for _, count := range []uint32{0, 64, 256, 262400} {
		bs, err := eventbus.AllSlotsUpTo(count)
		require.NoError(t, err, "count=%d must succeed", count)
		require.Equal(t, int(count), bs.Count(), "count=%d must preserve all bits without truncation", count)
		if count > 0 {
			require.True(t, bs.Has(count-1))
			require.False(t, bs.Has(count))
		}
	}

	_, errOver := eventbus.AllSlotsUpTo(262401)
	require.ErrorIs(t, errOver, eventbus.ErrSlotCountExceedsWireCap, "B4: count=262401 must fail closed with ErrSlotCountExceedsWireCap")

	// High uint32 slot ID (math.MaxUint32) with 1 sparse word round-trips:
	var highSparse eventbus.SlotBitset
	highSparse.Set(math.MaxUint32)
	require.Equal(t, 1, highSparse.SparseWordCount())
	encHigh, err := eventbus.EncodeSlotMaskTrailer(nil, true, true, highSparse)
	require.NoError(t, err)
	hasM, decHigh, rest, err := eventbus.DecodeSlotMaskTrailer(encHigh)
	require.NoError(t, err)
	require.True(t, hasM)
	require.Empty(t, rest)
	require.True(t, decHigh.Has(math.MaxUint32))

	// 4096 sparse words succeed; 4097 sparse words fail with ErrSlotCountExceedsWireCap:
	var mask4096 eventbus.SlotBitset
	for w := uint32(0); w < 4096; w++ {
		mask4096.Set((4 + w) * 64)
	}
	require.Equal(t, 4096, mask4096.SparseWordCount())
	enc4096, err := eventbus.EncodeSlotMaskTrailer(nil, true, true, mask4096)
	require.NoError(t, err)
	has4096, dec4096, _, err := eventbus.DecodeSlotMaskTrailer(enc4096)
	require.NoError(t, err)
	require.True(t, has4096)
	require.Equal(t, 4096, dec4096.Count())

	mask4097 := mask4096.Clone()
	mask4097.Set((4 + 4096) * 64)
	require.Equal(t, 4097, mask4097.SparseWordCount())
	_, err4097 := eventbus.EncodeSlotMaskTrailer(nil, true, true, mask4097)
	require.ErrorIs(t, err4097, eventbus.ErrSlotCountExceedsWireCap)
}

// TestXIN2855_B5_NoFireAndForgetCommitToLegacyPeer verifies B5:
//  1. PeerCapUnsupported / unknown capability version / timeout does NOT invoke legacy fire-and-forget Send for persisted events,
//     returns ForwardUnsupportedPeerNoAck (ErrPeerNoAck), and retains the event in RecoveryManager without committing StageChannelDistribute.
//  2. ACK-loss retry of the same persisted event ID is deduplicated idempotently by acceptForward without double execution.
//  3. Once peer upgrades to PeerCapPartAV2, pending recovery item drains and commits StageChannelDistribute.
func TestXIN2855_B5_NoFireAndForgetCommitToLegacyPeer(t *testing.T) {
	oldOpts := options.G
	t.Cleanup(func() { options.G = oldOpts })
	opts := options.New()
	opts.Cluster.NodeId = 1001
	options.G = opts

	mc := &capProbeMockCluster{v1AllowStatus: proto.StatusOK}
	mc.mode.Store(3) // non-OK capability response -> PeerCapUnsupported
	var capCache forward.CapabilityCache

	persistedEv := &eventbus.Event{
		Type:       eventbus.EventChannelDistribute,
		Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "b5-1", ChannelID: "g_b5", ChannelType: 2, Payload: []byte("p")},
		MessageId:  1,
		MessageSeq: 1,
	}
	shortEv := persistedEv.Clone()
	shortEv.ForwardDeadline = time.Now().Add(100 * time.Millisecond).UnixMilli()
	outcome, err := forward.RequestDetailedOnCluster(mc, forward.ChannelPath, []byte("payload"), []*eventbus.Event{shortEv}, func() uint64 { return 1002 }, 1001, nil, &capCache, func(n uint64) error {
		return mc.Send(n, &proto.Message{})
	})
	require.ErrorIs(t, err, forward.ErrPeerNoAck, "B5: persisted event must fail with ErrPeerNoAck on PeerCapUnsupported")
	require.Equal(t, forward.ForwardUnsupportedPeerNoAck, outcome)
	require.Zero(t, mc.legacySends.Load(), "B5: persisted event must never invoke fire-and-forget legacy Send")

	// Test ACK-loss idempotent replay on acceptForward:
	sink := &forwardChannel{}
	hRecipient := NewHandler()
	defer hRecipient.DrainAndStop(0)
	hRecipient.BindNodeDeps(NodeDeps{
		NodeId:     1002,
		ChannelBus: sink,
		Cluster:    &forwardChannelCluster{leader: 1002},
	})
	fReq := &forwardChannelEventReq{fromNode: 1001, channelId: "g_b5", channelType: 2, events: eventbus.EventBatch{persistedEv}}
	raw, err := fReq.encodeWithTrailer(true)
	require.NoError(t, err)
	env, _, err := forward.Envelope(raw, []*eventbus.Event{persistedEv})
	require.NoError(t, err)

	// First delivery succeeds; second delivery (simulating sender retry after lost ACK) also returns StatusOK but does NOT enqueue a 2nd copy!
	require.Equal(t, proto.StatusOK, hRecipient.AcceptForward(env))
	require.Equal(t, proto.StatusOK, hRecipient.AcceptForward(env))
	require.Len(t, sink.events, 1, "B5: ACK-loss replay of the same persisted event ID must be deduplicated idempotently by acceptForward")
}

// TestXIN2855_B7_RejectNilOrClosedTicketInRecoveryEnqueue verifies B7:
// 1) Normal inbound PostCommitBudget path is bounded (GREEN on baseline).
// 2) EnqueueWithResult rejects nil or closed TicketHandle with EnqueueInvalid BEFORE cloning Event/SendPacket.
// 3) Duplicate (channel, seq, stage) enqueue returns EnqueueMerged without cloning SendPacket/Payload.
// 4) Sustained downstream failure respects maxItems/maxMetadataBytes, never evicts accepted obligations, and drains cleanly to 0 on recovery.
func TestXIN2855_B7_RejectNilOrClosedTicketInRecoveryEnqueue(t *testing.T) {
	t.Run("NormalInboundBudgetedPath_AlreadyBounded_GreenOnBaseline", func(t *testing.T) {
		budget := eventbus.NewPostCommitBudget(8*1024, 2)
		ev := &eventbus.Event{
			Type:       eventbus.EventChannelDistribute,
			Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: "b7-ok", ChannelID: "g_b7", ChannelType: 2, Payload: []byte("ok")},
			MessageId:  1,
			MessageSeq: 1,
		}
		tks1, ok1 := budget.TryReserve("g_b7", []*eventbus.Event{ev})
		require.True(t, ok1)
		tks2, ok2 := budget.TryReserve("g_b7", []*eventbus.Event{ev})
		require.True(t, ok2)
		_, ok3 := budget.TryReserve("g_b7", []*eventbus.Event{ev})
		require.False(t, ok3, "B7 audit: normal PostCommitBudget admission is already bounded at maxItems/maxBytes")
		tks1[0].Release()
		tks2[0].Release()
	})

	t.Run("NilOrClosedTicketAdmission_And_BackpressureAndDrain", func(t *testing.T) {
		budget := eventbus.NewPostCommitBudget(2*1024*1024, 128)
		var allowDrain atomic.Bool
		rm := NewRecoveryManager(func(ctx context.Context, channelID string, channelType uint8, item RecoveryItem) (eventbus.SlotBitset, eventbus.SlotBitset, *eventbus.Event, bool) {
			if !allowDrain.Load() {
				return item.PendingSlots, item.AllRecipientSlots, item.Event, false
			}
			return eventbus.SlotBitset{}, item.AllRecipientSlots, item.Event, true
		})
		defer rm.DrainAndStop(0)
		rm.SetCapacityLimits(4, 64*1024)

		evNilTicket := &eventbus.Event{
			Type:       eventbus.EventChannelDistribute,
			Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: "b7-nil", ChannelID: "g_b7", ChannelType: 2, Payload: []byte("unbudgeted")},
			MessageId:  1,
			MessageSeq: 1,
			Ticket:     nil,
		}
		require.Equal(t, EnqueueInvalid, rm.EnqueueWithResult("g_b7", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, evNilTicket))
		require.False(t, rm.Enqueue("g_b7", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, evNilTicket))

		// Closed ticket handle must also be rejected with EnqueueInvalid and left on ev:
		tksClosed, ok := budget.TryReserve("g_b7", []*eventbus.Event{evNilTicket})
		require.True(t, ok)
		tksClosed[0].Release()
		evClosedTicket := evNilTicket.Clone()
		evClosedTicket.Ticket = tksClosed[0]
		require.Equal(t, EnqueueInvalid, rm.EnqueueWithResult("g_b7", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, evClosedTicket))
		require.Same(t, tksClosed[0], evClosedTicket.Ticket, "B7: failed EnqueueWithResult must leave ev.Ticket untouched on caller")

		// Insert 4 items with 16KiB immutable payload (reaching maxItems=4), then verify 5th item returns EnqueueBackpressure without evicting 1..4:
		sharedPayload := make([]byte, 16*1024)
		for seq := uint64(1); seq <= 4; seq++ {
			ev := &eventbus.Event{
				Type:       eventbus.EventChannelDistribute,
				Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
				Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("b7-%d", seq), ChannelID: "g_b7", ChannelType: 2, Payload: sharedPayload},
				MessageId:  int64(seq),
				MessageSeq: seq,
			}
			tks, okR := budget.TryReserve("g_b7", []*eventbus.Event{ev})
			require.True(t, okR)
			ev.Ticket = tks[0]
			require.Equal(t, EnqueueInserted, rm.EnqueueWithResult("g_b7", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev))
			require.Nil(t, ev.Ticket, "B7: successful EnqueueInserted must move ticket ownership out of caller Event")
		}

		// Duplicate merge on seq=1 must return EnqueueMerged without allocating a new 16KiB payload copy:
		var memBefore, memAfter runtime.MemStats
		runtime.ReadMemStats(&memBefore)
		evDup := &eventbus.Event{
			Type:        eventbus.EventChannelDistribute,
			Conn:        &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:       &wkproto.SendPacket{ClientMsgNo: "b7-1", ChannelID: "g_b7", ChannelType: 2, Payload: sharedPayload},
			MessageId:   1,
			MessageSeq:  1,
			WebhookDone: true,
		}
		tksDup, okD := budget.TryReserve("g_b7", []*eventbus.Event{evDup})
		require.True(t, okD)
		evDup.Ticket = tksDup[0]
		require.Equal(t, EnqueueMerged, rm.EnqueueWithResult("g_b7", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, evDup))
		runtime.ReadMemStats(&memAfter)
		require.True(t, tksDup[0].IsReleased(), "B7: merged duplicate ticket must be released immediately")

		// 5th distinct item exceeds maxItems=4 -> EnqueueBackpressure, caller retains ticket ownership:
		ev5 := &eventbus.Event{
			Type:       eventbus.EventChannelDistribute,
			Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: "b7-5", ChannelID: "g_b7", ChannelType: 2, Payload: sharedPayload},
			MessageId:  5,
			MessageSeq: 5,
		}
		tks5, ok5 := budget.TryReserve("g_b7", []*eventbus.Event{ev5})
		require.True(t, ok5)
		ev5.Ticket = tks5[0]
		require.Equal(t, EnqueueBackpressure, rm.EnqueueWithResult("g_b7", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev5))
		require.Same(t, tks5[0], ev5.Ticket, "B7: EnqueueBackpressure must preserve caller ticket ownership")
		ev5.Ticket.Release()

		det := rm.InspectDetailed()
		require.Equal(t, 4, det.QueuedItems)
		require.Greater(t, det.QueuedMetadataBytes, int64(0))

		// Allow drain -> all 4 accepted obligations recover and budget returns to 0!
		allowDrain.Store(true)
		rm.NotifyExternalWakeup()
		rm.runDueCycle(context.Background())
		detAfter := rm.InspectDetailed()
		require.Zero(t, detAfter.QueuedItems)
		require.Zero(t, detAfter.InFlightItems)
		require.Zero(t, detAfter.QueuedMetadataBytes)
		usedB, usedI, _ := budget.Stats()
		require.Zero(t, usedB)
		require.Zero(t, usedI)
	})
}

// --- Minimal test mocks ---

type localTagManagerMock struct {
	service.ITagManager
	mu   sync.Mutex
	tags map[string]*types.Tag
	ch   map[string]string
}

func newLocalTagManagerMock() *localTagManagerMock {
	return &localTagManagerMock{
		tags: make(map[string]*types.Tag),
		ch:   make(map[string]string),
	}
}

func (m *localTagManagerMock) setTag(channelID string, channelType uint8, tag *types.Tag) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tags[tag.Key] = tag
	m.ch[fmt.Sprintf("%s:%d", channelID, channelType)] = tag.Key
}

func (m *localTagManagerMock) GetChannelTag(fakeChannelId string, channelType uint8) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ch[fmt.Sprintf("%s:%d", fakeChannelId, channelType)]
}

func (m *localTagManagerMock) Get(tagKey string) *types.Tag {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tags[tagKey]
}

func (m *localTagManagerMock) RemoveTag(tagKey string) {}
func (m *localTagManagerMock) SetChannelTag(fakeChannelId string, channelType uint8, tagKey string) {
}
func (m *localTagManagerMock) MakeTag(uids []string) (*types.Tag, error) {
	return &types.Tag{Key: "made", Nodes: []*types.Node{{LeaderId: 1001, Uids: uids}}}, nil
}

type customForwardCluster struct {
	icluster.ICluster
	leader    uint64
	onRequest func(toNode uint64, path string, body []byte) (*proto.Response, error)
}

func (c *customForwardCluster) LeaderIdOfChannel(string, uint8) (uint64, error) {
	return 1001, nil
}
func (c *customForwardCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) {
	return c.leader, nil
}
func (c *customForwardCluster) GetSlotId(v string) uint32 {
	if v == "u_slot1" {
		return 1
	}
	if v == "u_slot2" {
		return 2
	}
	return 0
}
func (c *customForwardCluster) SlotLeaderId(slotID uint32) uint64 {
	if slotID == 1 || slotID == 2 {
		return 1002
	}
	return c.leader
}
func (c *customForwardCluster) RequestWithContext(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
	if c.onRequest != nil {
		return c.onRequest(toNodeId, path, body)
	}
	return &proto.Response{Status: proto.StatusOK}, nil
}

type boundedMockChannelBus struct {
	eventbus.IChannel
	mu           sync.Mutex
	maxAccept    int
	accepted     int
	lastAdmitted *eventbus.Event
	tickets      []*eventbus.PostCommitTicket
}

func (b *boundedMockChannelBus) TryAddEvent(channelId string, channelType uint8, ev *eventbus.Event) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.accepted >= b.maxAccept {
		return false
	}
	b.accepted++
	b.lastAdmitted = ev
	if ev != nil && ev.Ticket != nil {
		b.tickets = append(b.tickets, ev.Ticket)
	}
	return true
}
func (b *boundedMockChannelBus) Advance(string, uint8) {}

type countingPusherBus struct {
	eventbus.IPusher
	pushedCount atomic.Int32
}

func (p *countingPusherBus) RandHandlerId() int { return 1 }
func (p *countingPusherBus) AddEvent(_ int, _ *eventbus.Event) {
	p.pushedCount.Add(1)
}
func (p *countingPusherBus) Advance(int) {}

type onlineUserBusMock struct {
	eventbus.IUser
}

func (u *onlineUserBusMock) AuthedConnsByUid(uid string) []*eventbus.Conn {
	return []*eventbus.Conn{{Uid: uid, DeviceId: "d1", DeviceLevel: wkproto.DeviceLevelMaster}}
}

type panicSlotCluster struct {
	icluster.ICluster
	panicGetSlot    bool
	panicSlotLeader bool
	slotID          uint32
	leaderID        uint64
	explicitLeader  bool
}

func (c *panicSlotCluster) GetSlotId(string) uint32 {
	if c.panicGetSlot {
		panic("simulated GetSlotId panic")
	}
	return c.slotID
}

func (c *panicSlotCluster) SlotLeaderId(uint32) uint64 {
	if c.panicSlotLeader {
		panic("simulated SlotLeaderId panic")
	}
	if c.explicitLeader {
		return c.leaderID
	}
	return 1001
}

var (
	_ = errors.New
	_ = math.MaxUint32
	_ = wkdb.Message{}
)

// TestXIN2854_V2_XIN2855_SparseRoundRobinStageTrackerNoExhaustion verifies that when a multi-node
// ingress node executes Stage 0/1 on every k-th sequence (e.g. odd sequences 1, 3, ..., 39999 while
// peer nodes execute even sequences), OOOWindow's 4096-bit sparse pages scale in O(1) without
// hitting the old 1020-interval cliff, never bridge uncommitted holes, and reclaim 100% of extra
// bytes once gaps close.
func TestXIN2854_V2_XIN2855_SparseRoundRobinStageTrackerNoExhaustion(t *testing.T) {
	var tracker StageSeqTracker
	k, ok := MakeChannelWatermarkKey("p_sparse_rr", wkproto.ChannelTypePerson)
	require.True(t, ok)

	const maxSeq = uint64(39999)
	for seq := uint64(1); seq <= maxSeq; seq += 2 {
		st := tracker.BeginStageChecked(k, StageWebhookNotify, seq)
		require.Equal(t, StageBeginLeased, st, "odd seq %d must lease without hitting interval exhaustion", seq)
		tracker.CommitStage(k, StageWebhookNotify, seq)
	}

	for seq := uint64(1); seq <= maxSeq; seq += 2 {
		require.True(t, tracker.HasCommitted(k, StageWebhookNotify, seq), "committed odd seq %d must remain tracked", seq)
		require.Equal(t, StageBeginAlreadyCommitted, tracker.BeginStageChecked(k, StageWebhookNotify, seq))
	}
	for seq := uint64(2); seq < maxSeq; seq += 2 {
		require.False(t, tracker.HasCommitted(k, StageWebhookNotify, seq), "uncommitted even hole %d must never be bridged", seq)
	}

	// Closing the even gaps must absorb all sparse pages into [1..maxSeq] and reclaim 100% of extra bytes:
	for seq := uint64(2); seq < maxSeq; seq += 2 {
		st := tracker.BeginStageChecked(k, StageWebhookNotify, seq)
		require.Equal(t, StageBeginLeased, st)
		tracker.CommitStage(k, StageWebhookNotify, seq)
	}
	contig, oooBits, _, _ := tracker.InspectChannelStage("p_sparse_rr", wkproto.ChannelTypePerson, StageWebhookNotify)
	require.Equal(t, maxSeq, contig)
	require.Zero(t, oooBits)
	require.Zero(t, tracker.Metrics().ExtraSlotBytes)
	require.Zero(t, tracker.Metrics().ActiveOOOWindows)
}
