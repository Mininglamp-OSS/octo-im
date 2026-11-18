package handler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type failableChannelProposer struct {
	icluster.Channel
	calls      atomic.Int32
	failBefore atomic.Int32
	errToRet   error
}

func (f *failableChannelProposer) ProposeBatchUntilAppliedTimeout(_ context.Context, _ string, _ uint8, reqs rafttypes.ProposeReqSet) (rafttypes.ProposeRespSet, error) {
	c := f.calls.Add(1)
	if c <= f.failBefore.Load() {
		if f.errToRet != nil {
			return nil, f.errToRet
		}
		return nil, rafttypes.ErrNotLeader
	}
	resps := make(rafttypes.ProposeRespSet, len(reqs))
	for i, r := range reqs {
		resps[i] = &rafttypes.ProposeResp{Id: r.Id, CanonicalID: r.Id, Index: uint64(c*100) + uint64(i+1)}
	}
	return resps, nil
}

func TestXIN2857_YP104_PendingPersistSerializedDrainAndFailureRetention(t *testing.T) {
	oldOpts := options.G
	t.Cleanup(func() { options.G = oldOpts })
	opts := options.New()
	opts.Cluster.NodeId = 1001
	options.G = opts

	t.Run("ChannelBusPresentAndFull_NeverBypassesBusOrDropsOnFailedAppend", func(t *testing.T) {
		budget := eventbus.NewPostCommitBudget(16*1024*1024, 16384)
		rejectingBus := &boundedMockChannelBus{maxAccept: 0}
		proposer := &failableChannelProposer{errToRet: errors.New("propose timeout")}
		proposer.failBefore.Store(1000)
		st := store.New(store.NewOptions(store.WithNodeId(1001), store.WithChannel(proposer)))

		h := NewHandler()
		defer h.DrainAndStop(0)
		h.BindNodeDeps(NodeDeps{
			NodeId:           1001,
			ChannelBus:       rejectingBus,
			Cluster:          &forwardChannelCluster{leader: 1001},
			Store:            st,
			PostCommitBudget: budget,
		})

		ev := &eventbus.Event{
			Type:      eventbus.EventChannelOnSend,
			Conn:      &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
			Frame:     &wkproto.SendPacket{ClientMsgNo: "yp104-bus-1", ChannelID: "g_yp104", ChannelType: wkproto.ChannelTypeGroup, Payload: []byte("durable")},
			MessageId: 9001,
		}
		tks, ok := budget.TryReserve("g_yp104", []*eventbus.Event{ev})
		require.True(t, ok)
		ev.Ticket = tks[0]

		require.True(t, h.EnqueuePendingPersist("g_yp104", wkproto.ChannelTypeGroup, ev))

		// While ChannelBus is full, RunDrainCycle must NOT bypass ChannelBus to call Store.AppendMessages directly,
		// and must NOT drop the pending event or release its ticket!
		drained := h.pendingPersistMgr.RunDrainCycle()
		require.Equal(t, 0, drained, "Y-P1-04: must not treat saturated ChannelBus or failed direct store as drained")
		require.Zero(t, proposer.calls.Load(), "Y-P1-04: must not bypass serialized ChannelBus when chBus != nil")
		_, ppItems := h.pendingPersistMgr.Inspect()
		require.Equal(t, 1, ppItems, "Y-P1-04: pending durable event must remain in PendingPersistManager")
		_, usedItems, _ := budget.Stats()
		require.Equal(t, int64(1), usedItems, "Y-P1-04: ticket ownership must remain valid in PendingPersistManager")

		// Once ChannelBus has space, RunDrainCycle (or background drain loop) hands the event to ChannelBus in order:
		rejectingBus.mu.Lock()
		rejectingBus.maxAccept = 1
		rejectingBus.mu.Unlock()
		_ = h.pendingPersistMgr.RunDrainCycle()
		_, ppAfter := h.pendingPersistMgr.Inspect()
		require.Zero(t, ppAfter)
		rejectingBus.mu.Lock()
		require.Equal(t, 1, rejectingBus.accepted)
		for _, tk := range rejectingBus.tickets {
			if tk != nil {
				tk.Release()
			}
		}
		rejectingBus.mu.Unlock()
		_, usedAfter, _ := budget.Stats()
		require.Zero(t, usedAfter)
	})

	t.Run("NoChannelBusFallback_RetainsFailedAppendAndTicketUntilSuccess", func(t *testing.T) {
		budget := eventbus.NewPostCommitBudget(16*1024*1024, 16384)
		proposer := &failableChannelProposer{errToRet: rafttypes.ErrNotLeader}
		proposer.failBefore.Store(1000)
		st := store.New(store.NewOptions(store.WithNodeId(1001), store.WithChannel(proposer)))

		h := NewHandler()
		defer h.DrainAndStop(0)
		h.BindNodeDeps(NodeDeps{
			NodeId:           1001,
			ChannelBus:       nil,
			Cluster:          &forwardChannelCluster{leader: 1001},
			Store:            st,
			PostCommitBudget: budget,
		})

		ev := &eventbus.Event{
			Type:      eventbus.EventChannelOnSend,
			Conn:      &eventbus.Conn{Uid: "u_sender", DeviceId: opts.SystemDeviceId},
			Frame:     &wkproto.SendPacket{ClientMsgNo: "yp104-nobus-1", ChannelID: "g_yp104_2", ChannelType: wkproto.ChannelTypeGroup, Payload: []byte("durable")},
			MessageId: 9002,
		}
		tks, ok := budget.TryReserve("g_yp104_2", []*eventbus.Event{ev})
		require.True(t, ok)
		ev.Ticket = tks[0]

		require.True(t, h.EnqueuePendingPersist("g_yp104_2", wkproto.ChannelTypeGroup, ev))

		// While AppendMessages fails -> must NOT drain or lose ticket!
		require.Equal(t, 0, h.pendingPersistMgr.RunDrainCycle(), "Y-P1-04: failed AppendMessages must not be treated as a successful drain")
		_, ppItems := h.pendingPersistMgr.Inspect()
		require.Equal(t, 1, ppItems, "Y-P1-04: failed entry must remain at head of PendingPersistManager")
		_, usedItems, _ := budget.Stats()
		require.Equal(t, int64(1), usedItems, "Y-P1-04: head ticket must remain live after failed AppendMessages attempt")

		// Now allow AppendMessages to succeed -> drains from PendingPersistManager!
		proposer.failBefore.Store(0)
		_ = h.pendingPersistMgr.RunDrainCycle()
		_, ppAfter := h.pendingPersistMgr.Inspect()
		require.Zero(t, ppAfter, "Y-P1-04: once AppendMessages succeeds, item must drain")
	})
}

func TestXIN2857_YP105_PublishRetryOwnedOnlyAfterRecoveryAdmission(t *testing.T) {
	oldOpts, oldPM := options.G, service.PluginManager
	t.Cleanup(func() {
		options.G = oldOpts
		service.PluginManager = oldPM
	})
	opts := options.New()
	opts.Cluster.NodeId = 1001
	opts.Webhook.HTTPAddr = "http://127.0.0.1:19999"
	options.G = opts

	// Attempt 1 on ingress (node 1001) fails; attempt 2 on remote leader (node 1002) succeeds.
	flakyPlugin := &flakyPersistAfterPlugin{failBefore: 1}
	service.PluginManager = &mockPluginManager{pg: flakyPlugin}

	hIngress := NewHandler()
	defer hIngress.DrainAndStop(0)
	hLeader := NewHandler()
	defer hLeader.DrainAndStop(0)

	tagMgr := newLocalTagManagerMock()
	tag := &types.Tag{Key: "t_yp105", Nodes: []*types.Node{{LeaderId: 1002, Uids: []string{"u_local"}}}}
	tagMgr.setTag("ch_yp105", wkproto.ChannelTypeGroup, tag)

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

	// Ingress has saturated ChannelBus (maxAccept=0) and exhausted PostCommitBudget (0, 0),
	// so hIngress.recoveryMgr.Enqueue rejects both Stage 0 (Plugin) and Stage 1 (Webhook).
	hIngress.BindNodeDeps(NodeDeps{
		NodeId:           1001,
		ChannelBus:       &boundedMockChannelBus{maxAccept: 0},
		Cluster:          bridgeCl,
		PostCommitBudget: eventbus.NewPostCommitBudget(0, 0),
	})

	ev := &eventbus.Event{
		Type:       eventbus.EventChannelOnSend,
		Conn:       &eventbus.Conn{Uid: "sender", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "yp105-msg-1", ChannelID: "ch_yp105", ChannelType: wkproto.ChannelTypeGroup, Payload: []byte("hello")},
		MessageId:  9101,
		MessageSeq: 1,
		ReasonCode: wkproto.ReasonSuccess,
	}

	hIngress.forwardsToNode(1002, "ch_yp105", wkproto.ChannelTypeGroup, []*eventbus.Event{ev})
	_, ingRecItems, _ := hIngress.recoveryMgr.Inspect()
	require.Zero(t, ingRecItems, "ingress recovery manager rejected admission due to exhausted budget")

	require.Len(t, leaderBus.events, 1, "event must be forwarded to remote slot leader 1002")
	forwardedEv := leaderBus.events[0]
	require.Equal(t, eventbus.StageStatusPending, forwardedEv.PluginStageStatus, "Y-P1-05: rejected Stage 0 recovery on ingress must remain Pending, never RetryOwned")
	require.False(t, forwardedEv.PluginDone, "Y-P1-05: rejected Stage 0 recovery on ingress must not mark PluginDone=true")
	require.Equal(t, eventbus.StageStatusPending, forwardedEv.WebhookStageStatus, "Y-P1-05: rejected Stage 1 admission on ingress must remain Pending, never RetryOwned")
	require.False(t, forwardedEv.WebhookDone, "Y-P1-05: rejected Stage 1 admission on ingress must not mark WebhookDone=true")

	// When remote leader 1002 runs persist() on the forwarded event, it must execute Stage 0 Plugin and enqueue Stage 1 Webhook!
	leaderBus.events = nil
	hLeader.persist(&eventbus.ChannelContext{
		ChannelId:    "ch_yp105",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelOnSend,
		SlotLeaderId: 1002,
		Events:       []*eventbus.Event{forwardedEv},
	})
	require.Equal(t, int32(2), flakyPlugin.calls.Load(), "Y-P1-05: remote slot leader must execute Stage 0 when ingress admission failed")
	hasWebhook := false
	for _, le := range leaderBus.events {
		if le.Type == eventbus.EventChannelWebhook {
			hasWebhook = true
		}
	}
	require.True(t, hasWebhook, "Y-P1-05: remote slot leader must enqueue Stage 1 Webhook when ingress admission failed")
}

func TestXIN2857_JP101_BaseV1RejectsProperSubsetRetryAndAllowsFullNodeMask(t *testing.T) {
	oldOpts := options.G
	t.Cleanup(func() { options.G = oldOpts })
	opts := options.New()
	opts.Cluster.NodeId = 1001
	options.G = opts

	var peer1002CapBody []byte // nil = BaseV1 (21cd8f88); WKF2\x01 = PartAV2
	var rpcSendsTo1002 atomic.Int32
	var lastReceivedOn1002 *forwardChannelEventReq

	hLeader := NewHandler()
	defer hLeader.DrainAndStop(0)
	hPeer1002 := NewHandler()
	defer hPeer1002.DrainAndStop(0)

	peer1002Pusher := &countingPusherBus{}
	tagMgr := newLocalTagManagerMock()
	// Node 1002 owns TWO user slots: slot 1 ("u_slot1") and slot 2 ("u_slot2")
	tag := &types.Tag{
		Key: "t_jp101",
		Nodes: []*types.Node{
			{LeaderId: 1002, Uids: []string{"u_slot1", "u_slot2"}},
		},
	}
	tagMgr.setTag("ch_jp101", wkproto.ChannelTypeGroup, tag)

	cl := &customForwardCluster{
		leader: 1001,
		onRequest: func(toNode uint64, path string, body []byte) (*proto.Response, error) {
			if path == forward.CapabilityPath {
				return &proto.Response{Status: proto.StatusOK, Body: peer1002CapBody}, nil
			}
			if path == forward.ChannelPath && toNode == 1002 {
				rpcSendsTo1002.Add(1)
				raw, _, _, err := forward.Decode(body)
				if err == nil {
					req := &forwardChannelEventReq{}
					if req.decode(raw) == nil {
						lastReceivedOn1002 = req
					}
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			}
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}

	hLeader.BindNodeDeps(NodeDeps{
		NodeId:     1001,
		Cluster:    cl,
		TagManager: tagMgr,
	})
	hPeer1002.BindNodeDeps(NodeDeps{
		NodeId:     1002,
		Cluster:    cl,
		TagManager: tagMgr,
		UserBus:    &onlineUserBusMock{},
		PusherBus:  peer1002Pusher,
	})

	var slot1Only, fullSlots12 eventbus.SlotBitset
	slot1Only.Set(1)
	fullSlots12.Set(1)
	fullSlots12.Set(2)

	// 1. Proper subset retry ({slot 1} out of {slot 1, 2}) to BaseV1 peer 1002 MUST be rejected (ErrPeerNoAck, 0 RPC sends)
	// and retained in RecoveryManager until peer 1002 upgrades to PartAV2:
	evPartial := &eventbus.Event{
		Type:              eventbus.EventChannelDistribute,
		Conn:              &eventbus.Conn{Uid: "sender", DeviceId: opts.SystemDeviceId},
		Frame:             &wkproto.SendPacket{ClientMsgNo: "jp101-partial", ChannelID: "ch_jp101", ChannelType: wkproto.ChannelTypeGroup, Payload: []byte("p")},
		MessageId:         9201,
		MessageSeq:        1,
		ReasonCode:        wkproto.ReasonSuccess,
		TagKey:            "t_jp101",
		HasTargetSlotMask: true,
		TargetSlotMask:    slot1Only.Clone(),
	}
	tks, ok := hLeader.postCommitBudget().TryReserve("ch_jp101", []*eventbus.Event{evPartial})
	require.True(t, ok)
	evPartial.Ticket = tks[0]

	hLeader.distribute(&eventbus.ChannelContext{
		ChannelId:    "ch_jp101",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelDistribute,
		SlotLeaderId: 1001,
		Events:       []*eventbus.Event{evPartial},
	})
	require.Zero(t, rpcSendsTo1002.Load(), "J-P1-01: proper subset {slot 1} must NOT be sent maskless to BaseV1 peer owning {slot 1, 2}")
	_, recItems, _ := hLeader.recoveryMgr.Inspect()
	require.Equal(t, 1, recItems, "J-P1-01: partial retry must be retained in RecoveryManager while peer is BaseV1")

	// Also verify executeSingleEventDistributeRecovery directly rejects proper subset while peer is BaseV1:
	hLeader.recoveryMgr.NotifyExternalWakeup()
	hLeader.recoveryMgr.runDueCycle(context.Background())
	require.Zero(t, rpcSendsTo1002.Load(), "J-P1-01: recovery cycle must also hold proper subset while peer is BaseV1")
	_, recItemsAfterBaseV1Cycle, _ := hLeader.recoveryMgr.Inspect()
	require.Equal(t, 1, recItemsAfterBaseV1Cycle)

	// 2. Full-node distribution ({slot 1, 2} == remoteMask) to BaseV1 peer 1002 MUST still be allowed:
	evFull := &eventbus.Event{
		Type:       eventbus.EventChannelDistribute,
		Conn:       &eventbus.Conn{Uid: "sender", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "jp101-full", ChannelID: "ch_jp101", ChannelType: wkproto.ChannelTypeGroup, Payload: []byte("f")},
		MessageId:  9202,
		MessageSeq: 2,
		ReasonCode: wkproto.ReasonSuccess,
		TagKey:     "t_jp101",
	}
	tks2, ok := hLeader.postCommitBudget().TryReserve("ch_jp101", []*eventbus.Event{evFull})
	require.True(t, ok)
	evFull.Ticket = tks2[0]

	hLeader.distribute(&eventbus.ChannelContext{
		ChannelId:    "ch_jp101",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelDistribute,
		SlotLeaderId: 1001,
		Events:       []*eventbus.Event{evFull},
	})
	require.Equal(t, int32(1), rpcSendsTo1002.Load(), "J-P1-01: full-node recipient mask {slot 1, 2} to BaseV1 peer must succeed")

	// 3. Once peer 1002 upgrades to PartAV2, the retained partial subset {slot 1} drains with WKST/WKSM TargetSlotMask={slot 1}
	// and delivers ONLY to u_slot1 on node 1002 (never u_slot2):
	peer1002CapBody = forward.BuildCapabilityBody()
	hLeader.forwardCapabilities = forward.CapabilityCache{}
	hLeader.recoveryMgr.NotifyExternalWakeup()
	hLeader.recoveryMgr.runDueCycle(context.Background())

	require.Equal(t, int32(2), rpcSendsTo1002.Load(), "J-P1-01: retained partial subset must be sent once peer 1002 upgrades to PartAV2")
	_, recItemsFinal, _ := hLeader.recoveryMgr.Inspect()
	require.Zero(t, recItemsFinal, "J-P1-01: RecoveryManager must drain to 0 after PartAV2 delivery")
	require.NotNil(t, lastReceivedOn1002)
	require.Len(t, lastReceivedOn1002.events, 1)
	require.True(t, lastReceivedOn1002.events[0].HasTargetSlotMask)
	require.True(t, lastReceivedOn1002.events[0].TargetSlotMask.Equal(slot1Only), "J-P1-01: PartAV2 wire payload must preserve exact subset {slot 1}")

	hPeer1002.distribute(&eventbus.ChannelContext{
		ChannelId:    "ch_jp101",
		ChannelType:  wkproto.ChannelTypeGroup,
		EventType:    eventbus.EventChannelDistribute,
		SlotLeaderId: 1001,
		Events:       lastReceivedOn1002.events,
	})
	require.Equal(t, int32(1), peer1002Pusher.pushedCount.Load(), "J-P1-01: recipient node 1002 must push ONLY to u_slot1 (1 user), not u_slot2")
}


