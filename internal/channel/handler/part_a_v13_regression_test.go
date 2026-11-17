package handler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestPartA_BF_R7_1a_NoGapCoalescingOnOOOWindow(t *testing.T) {
	var tracker StageSeqTracker
	k, ok := MakeChannelWatermarkKey("group_gap_safety", wkproto.ChannelTypeGroup)
	require.True(t, ok)

	for seq := uint64(1); seq <= 10; seq++ {
		skip, leased := tracker.BeginStage(k, StageChannelDistribute, seq)
		require.False(t, skip)
		require.True(t, leased)
		tracker.CommitStage(k, StageChannelDistribute, seq)
	}

	// Commit 120 discontinuous even sequences (exceeding R7's 64-span cap): 12, 14, ..., 250.
	for i := uint64(0); i < 120; i++ {
		evenSeq := 12 + i*2
		skip, leased := tracker.BeginStage(k, StageChannelDistribute, evenSeq)
		require.False(t, skip)
		require.True(t, leased)
		tracker.CommitStage(k, StageChannelDistribute, evenSeq)
	}

	// Every odd sequence in 11..249 is uncommitted and MUST NOT be falsely reported as committed.
	for i := uint64(0); i < 120; i++ {
		oddSeq := 11 + i*2
		require.False(t, tracker.HasCommitted(k, StageChannelDistribute, oddSeq), "uncommitted gap seq %d must never be bridged", oddSeq)
	}
	// Every even sequence in 12..250 MUST be reported as committed.
	for i := uint64(0); i < 120; i++ {
		evenSeq := 12 + i*2
		require.True(t, tracker.HasCommitted(k, StageChannelDistribute, evenSeq), "committed seq %d must stay deduplicated", evenSeq)
	}

	// Now commit all missing odd sequences 11..249: contiguous frontier must advance to 250 and free the OOOWindow.
	for i := uint64(0); i < 120; i++ {
		oddSeq := 11 + i*2
		skip, leased := tracker.BeginStage(k, StageChannelDistribute, oddSeq)
		require.False(t, skip)
		require.True(t, leased)
		tracker.CommitStage(k, StageChannelDistribute, oddSeq)
	}
	contig, oooBits, intervals, _ := tracker.InspectChannelStage("group_gap_safety", wkproto.ChannelTypeGroup, StageChannelDistribute)
	require.Equal(t, uint64(250), contig)
	require.Equal(t, 0, oooBits)
	require.Equal(t, 1, intervals)
	require.Equal(t, int32(0), tracker.Metrics().ActiveOOOWindows)
}

func TestPartA_BF_R7_1bcd_LosslessRecoveryCooldownAndNoStoreScan(t *testing.T) {
	budget := eventbus.NewPostCommitBudget(16*1024*1024, 16384)
	var allowSuccess atomic.Bool
	var delivered sync.Map
	var deliveredCount atomic.Int64

	rm := NewRecoveryManager(func(ctx context.Context, channelID string, channelType uint8, item RecoveryItem) (eventbus.SlotBitset, eventbus.SlotBitset, *eventbus.Event, bool) {
		if !allowSuccess.Load() {
			return item.PendingSlots, item.AllRecipientSlots, item.Event, false
		}
		key := fmt.Sprintf("%s#%d", channelID, item.Seq)
		if _, loaded := delivered.LoadOrStore(key, true); !loaded {
			deliveredCount.Add(1)
		}
		return eventbus.SlotBitset{}, item.AllRecipientSlots, item.Event, true
	})
	defer rm.DrainAndStop(0)

	// Enqueue 1,100 channels (> R7's 1,024 channel eviction cap) + 70 items on ch-0 (> R7's 64 per-channel compaction cap).
	const totalCh = 1100
	const extraCh0 = 70
	expectedTotal := totalCh - 1 + extraCh0

	for seq := uint64(1); seq <= extraCh0; seq++ {
		ev := &eventbus.Event{
			Type:       eventbus.EventChannelDistribute,
			Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("m-%d", seq), ChannelID: "ch-0", ChannelType: 2, Payload: make([]byte, 32, 4096)},
			MessageId:  int64(seq),
			MessageSeq: seq,
			ReasonCode: wkproto.ReasonSuccess,
		}
		tks, ok := budget.TryReserve("ch-0", []*eventbus.Event{ev})
		require.True(t, ok)
		ev.Ticket = tks[0]
		require.True(t, rm.Enqueue("ch-0", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev))
	}
	for ch := 1; ch < totalCh; ch++ {
		chID := fmt.Sprintf("ch-%d", ch)
		ev := &eventbus.Event{
			Type:       eventbus.EventChannelDistribute,
			Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: "m-1", ChannelID: chID, ChannelType: 2, Payload: []byte("ok")},
			MessageId:  int64(10000 + ch),
			MessageSeq: 1,
			ReasonCode: wkproto.ReasonSuccess,
		}
		tks, ok := budget.TryReserve(chID, []*eventbus.Event{ev})
		require.True(t, ok)
		ev.Ticket = tks[0]
		require.True(t, rm.Enqueue(chID, wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev))
	}

	chActive, itemsActive, _ := rm.Inspect()
	require.Equal(t, totalCh, chActive, "BF-R7-1b: zero silent channel eviction above 1,024 channels")
	require.Equal(t, expectedTotal, itemsActive, "BF-R7-1c: all items retained in memory with compacted payload, zero numeric range scan")

	// Drive 6 failed cycles into CooldownProbe (BF-R7-1d: never dead-end dormant).
	for cycle := 0; cycle < MaxActiveRecoveryAttempts; cycle++ {
		rm.NotifyExternalWakeup()
		rm.runDueCycle(context.Background())
	}
	_, itemsAfterFail, cooldownProbes := rm.Inspect()
	require.Equal(t, expectedTotal, itemsAfterFail)
	require.Greater(t, cooldownProbes, uint64(0), "exhausted active retries must enter CooldownProbe rather than dormant")

	// Restore peer and trigger wakeup: 100% of items must recover with zero Store dependency and return budget to 0.
	allowSuccess.Store(true)
	rm.NotifyExternalWakeup()
	require.Eventually(t, func() bool {
		return deliveredCount.Load() == int64(expectedTotal)
	}, 3*time.Second, 20*time.Millisecond)

	usedB, usedI, _ := budget.Stats()
	require.Zero(t, usedB)
	require.Zero(t, usedI)
}

func TestPartA_V12_1_DueOnlyRecoveryLeasePreservesSiblingBackoff(t *testing.T) {
	budget := eventbus.NewPostCommitBudget(1024*1024, 100)
	var execSeq1, execSeq2 atomic.Int32
	rm := NewRecoveryManager(func(ctx context.Context, channelID string, channelType uint8, item RecoveryItem) (eventbus.SlotBitset, eventbus.SlotBitset, *eventbus.Event, bool) {
		if item.Seq == 1 {
			execSeq1.Add(1)
			return item.PendingSlots, item.AllRecipientSlots, item.Event, false
		}
		execSeq2.Add(1)
		return eventbus.SlotBitset{}, item.AllRecipientSlots, item.Event, true
	})
	defer rm.DrainAndStop(0)

	makeEv := func(seq uint64) *eventbus.Event {
		ev := &eventbus.Event{
			Type:       eventbus.EventChannelDistribute,
			Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("c-%d", seq), ChannelID: "g1", ChannelType: 2, Payload: []byte("p")},
			MessageId:  int64(seq),
			MessageSeq: seq,
			ReasonCode: wkproto.ReasonSuccess,
		}
		tks, ok := budget.TryReserve("g1", []*eventbus.Event{ev})
		require.True(t, ok)
		ev.Ticket = tks[0]
		return ev
	}

	require.True(t, rm.Enqueue("g1", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, makeEv(1)))

	// Force seq=1 into CooldownProbe (NextRetryAt ~ +5s).
	for i := 0; i < MaxActiveRecoveryAttempts; i++ {
		rm.mu.Lock()
		for _, ent := range rm.entries {
			for idx := range ent.items {
				ent.items[idx].NextRetryAt = time.Now().Add(-time.Millisecond)
			}
		}
		rm.mu.Unlock()
		rm.runDueCycle(context.Background())
	}
	seq1CallsBefore := execSeq1.Load()
	require.Equal(t, int32(MaxActiveRecoveryAttempts), seq1CallsBefore)

	// Enqueue fresh sibling seq=2 on the SAME channel: after 60ms only seq=2 is due; seq=1 must NOT be detached early!
	require.True(t, rm.Enqueue("g1", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, makeEv(2)))
	time.Sleep(65 * time.Millisecond)
	rm.runDueCycle(context.Background())

	require.Equal(t, int32(1), execSeq2.Load(), "due sibling seq=2 must execute")
	require.Equal(t, seq1CallsBefore, execSeq1.Load(), "V12-1: not-yet-due seq=1 must preserve its cooldown backoff")
}

func TestPartA_V12_2_ConcurrentSlotStageDedupUnderExhaustedSlotExts(t *testing.T) {
	var tracker StageSeqTracker

	// Exhaust all 2,048 global SlotHandoffExt structures across other channels.
	var twoSlots, oneSlot eventbus.SlotBitset
	twoSlots.Set(1)
	twoSlots.Set(2)
	oneSlot.Set(1)
	for i := 0; i < MaxGlobalSlotExts; i++ {
		ck, ok := MakeChannelWatermarkKey(fmt.Sprintf("fill_ext_%d", i), wkproto.ChannelTypeGroup)
		require.True(t, ok)
		deliv, leased := tracker.BeginSlotStage(ck, StageChannelDistribute, 1, twoSlots)
		require.True(t, leased)
		tracker.FinishSlotStage(ck, StageChannelDistribute, 1, deliv, oneSlot, twoSlots)
	}
	require.Equal(t, int32(MaxGlobalSlotExts), tracker.Metrics().ActiveSlotExts)

	// Now launch 16 concurrent workers requesting overlapping slots {0..7} on target channel with 0 free SlotHandoffExts.
	targetKey, ok := MakeChannelWatermarkKey("target_exhausted_ext", wkproto.ChannelTypeGroup)
	require.True(t, ok)
	reqSlots := eventbus.AllSlotsUpTo(8)

	var startBarrier sync.WaitGroup
	startBarrier.Add(1)
	var doneWG sync.WaitGroup
	var slotDeliveries [8]atomic.Int32

	for w := 0; w < 16; w++ {
		doneWG.Add(1)
		go func() {
			defer doneWG.Done()
			startBarrier.Wait()
			deliv, leased := tracker.BeginSlotStage(targetKey, StageChannelDistribute, 42, reqSlots)
			if !leased {
				return
			}
			deliv.ForEachSetBit(func(slotID uint32) {
				slotDeliveries[slotID].Add(1)
			})
			time.Sleep(10 * time.Millisecond)
			tracker.FinishSlotStage(targetKey, StageChannelDistribute, 42, deliv, deliv, reqSlots)
		}()
	}
	startBarrier.Done()
	doneWG.Wait()

	for s := 0; s < 8; s++ {
		require.Equal(t, int32(1), slotDeliveries[s].Load(), "V12-2: slot %d must be leased once across concurrent workers even when SlotHandoffExt pool is exhausted", s)
	}
}

func TestPartA_V13_1_RecoveryLeaseMergeTicketOwnership(t *testing.T) {
	t.Run("DistinctTickets_ConcurrentDuplicateDuringFailedLease_ThenSuccess", func(t *testing.T) {
		budget := eventbus.NewPostCommitBudget(4*1024*1024, 1024)
		inLease := make(chan struct{}, 4)
		releaseLease := make(chan struct{})
		var succeedNext atomic.Bool

		rm := NewRecoveryManager(func(ctx context.Context, channelID string, channelType uint8, item RecoveryItem) (eventbus.SlotBitset, eventbus.SlotBitset, *eventbus.Event, bool) {
			if !succeedNext.Load() {
				inLease <- struct{}{}
				<-releaseLease
				return item.PendingSlots, item.AllRecipientSlots, item.Event, false
			}
			return eventbus.SlotBitset{}, item.AllRecipientSlots, item.Event, true
		})
		defer rm.DrainAndStop(0)

		var maskA, maskB, allMask eventbus.SlotBitset
		maskA.Set(1)
		maskB.Set(2)
		allMask.Set(1)
		allMask.Set(2)

		evA := &eventbus.Event{
			Type:       eventbus.EventChannelDistribute,
			Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: "v13-1", ChannelID: "g_v13", ChannelType: 2, Payload: []byte("hello")},
			MessageId:  101,
			MessageSeq: 1,
			ReasonCode: wkproto.ReasonSuccess,
		}
		tksA, ok := budget.TryReserve("g_v13", []*eventbus.Event{evA})
		require.True(t, ok)
		ticketA := tksA[0]
		evA.Ticket = ticketA
		require.True(t, rm.Enqueue("g_v13", wkproto.ChannelTypeGroup, RecoveryStageDistribute, maskA, allMask, evA))

		rm.NotifyExternalWakeup()
		<-inLease
		// While evA (owning ticketA) is leased out, enqueue concurrent duplicate evB (owning distinct ticketB).
		evB := &eventbus.Event{
			Type:        eventbus.EventChannelDistribute,
			Conn:        &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:       &wkproto.SendPacket{ClientMsgNo: "v13-1", ChannelID: "g_v13", ChannelType: 2, Payload: []byte("hello")},
			MessageId:   101,
			MessageSeq:  1,
			ReasonCode:  wkproto.ReasonSuccess,
			WebhookDone: true,
		}
		tksB, ok := budget.TryReserve("g_v13", []*eventbus.Event{evB})
		require.True(t, ok)
		ticketB := tksB[0]
		evB.Ticket = ticketB
		require.True(t, rm.Enqueue("g_v13", wkproto.ChannelTypeGroup, RecoveryStageDistribute, maskB, allMask, evB))

		usedB2, usedI2, _ := budget.Stats()
		require.Equal(t, int64(2), usedI2)
		require.Greater(t, usedB2, int64(0))

		// Let leased execution fail -> completeRecoveryLease must release ticketB once, keep ticketA live!
		close(releaseLease)
		require.Eventually(t, func() bool {
			return ticketB.IsReleased()
		}, 2*time.Second, 10*time.Millisecond, "V13-1: superseded concurrent ticketB must be released once")

		require.False(t, ticketA.IsReleased(), "V13-1: retained ticketA on merged entry must remain live")
		_, usedIAfterMerge, _ := budget.Stats()
		require.Equal(t, int64(1), usedIAfterMerge, "exactly 1 live ticket must remain accounted after failed-lease merge")
		_, activeItems, _ := rm.Inspect()
		require.Equal(t, 1, activeItems, "merged item count must remain 1")

		// Now succeed on next retry -> ticketA must release cleanly with zero panic and 0 residual bytes/items!
		succeedNext.Store(true)
		rm.NotifyExternalWakeup()
		require.Eventually(t, func() bool {
			return ticketA.IsReleased()
		}, 2*time.Second, 10*time.Millisecond)

		finalB, finalI, _ := budget.Stats()
		require.Zero(t, finalB)
		require.Zero(t, finalI)
	})

	t.Run("SharedRetainedTicket_RepeatedFailedMerges_AndDrainCancel", func(t *testing.T) {
		budget := eventbus.NewPostCommitBudget(4*1024*1024, 1024)
		inLease := make(chan struct{}, 4)
		releaseLease := make(chan struct{})

		rm := NewRecoveryManager(func(ctx context.Context, channelID string, channelType uint8, item RecoveryItem) (eventbus.SlotBitset, eventbus.SlotBitset, *eventbus.Event, bool) {
			inLease <- struct{}{}
			<-releaseLease
			return item.PendingSlots, item.AllRecipientSlots, item.Event, false
		})

		ev := &eventbus.Event{
			Type:       eventbus.EventChannelDistribute,
			Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: "v13-shared", ChannelID: "g_v13_shared", ChannelType: 2, Payload: []byte("shared")},
			MessageId:  202,
			MessageSeq: 7,
			ReasonCode: wkproto.ReasonSuccess,
		}
		tks, ok := budget.TryReserve("g_v13_shared", []*eventbus.Event{ev})
		require.True(t, ok)
		sharedTk := tks[0]
		dupRef := sharedTk.Retain()
		require.Equal(t, int32(2), sharedTk.Refs())

		ev.Ticket = sharedTk
		require.True(t, rm.Enqueue("g_v13_shared", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev))

		// Trigger immediate wakeup on the RecoveryManager loop so m.loop() leases ev deterministically.
		rm.NotifyExternalWakeup()
		<-inLease

		// Concurrent duplicate carrying Retained() reference to the SAME underlying PostCommitTicket.
		evDup := ev.Clone()
		evDup.Ticket = dupRef
		require.True(t, rm.Enqueue("g_v13_shared", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, evDup))

		close(releaseLease)
		require.Eventually(t, func() bool {
			return sharedTk.Refs() == 1
		}, 2*time.Second, 10*time.Millisecond, "V13-1: shared retained ticket must decrement from 2 -> 1 without premature release")
		require.False(t, sharedTk.IsReleased())

		// DrainAndStop(0) cancels and releases remaining ticket cleanly to 0!
		rm.DrainAndStop(0)
		require.True(t, sharedTk.IsReleased())
		finalB, finalI, _ := budget.Stats()
		require.Zero(t, finalB)
		require.Zero(t, finalI)
	})

	t.Run("ConcurrentStressRace_ZeroLeakZeroPanic", func(t *testing.T) {
		budget := eventbus.NewPostCommitBudget(8*1024*1024, 4096)
		var step atomic.Uint64
		rm := NewRecoveryManager(func(ctx context.Context, channelID string, channelType uint8, item RecoveryItem) (eventbus.SlotBitset, eventbus.SlotBitset, *eventbus.Event, bool) {
			n := step.Add(1)
			return item.PendingSlots, item.AllRecipientSlots, item.Event, (n % 3) == 0
		})

		var wg sync.WaitGroup
		for g := 0; g < 6; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for i := 0; i < 60; i++ {
					seq := uint64((i % 8) + 1)
					ev := &eventbus.Event{
						Type:       eventbus.EventChannelDistribute,
						Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
						Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("stress-%d-%d", id, i), ChannelID: "g_stress", ChannelType: 2, Payload: []byte("payload")},
						MessageId:  int64(seq),
						MessageSeq: seq,
						ReasonCode: wkproto.ReasonSuccess,
					}
					if tks, ok := budget.TryReserve("g_stress", []*eventbus.Event{ev}); ok {
						ev.Ticket = tks[0]
						rm.Enqueue("g_stress", wkproto.ChannelTypeGroup, RecoveryStageDistribute, eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev)
					}
					if i%5 == 0 {
						rm.NotifyExternalWakeup()
						rm.runDueCycle(context.Background())
					}
				}
			}(g)
		}
		wg.Wait()
		rm.DrainAndStop(100 * time.Millisecond)

		finalB, finalI, _ := budget.Stats()
		require.Zero(t, finalB)
		require.Zero(t, finalI)
	})
}

type flakyPersistAfterPlugin struct {
	failBefore int32
	calls      atomic.Int32
}

func (p *flakyPersistAfterPlugin) GetNo() string { return "flaky_persist_after" }
func (p *flakyPersistAfterPlugin) Send(ctx context.Context, f *pluginproto.SendPacket) (*pluginproto.SendPacket, error) {
	return f, nil
}
func (p *flakyPersistAfterPlugin) PersistAfter(ctx context.Context, mb *pluginproto.MessageBatch) error {
	c := p.calls.Add(1)
	if c <= p.failBefore {
		return errors.New("plugin transient error")
	}
	return nil
}
func (p *flakyPersistAfterPlugin) Receive(ctx context.Context, r *pluginproto.RecvPacket) error {
	return nil
}
func (p *flakyPersistAfterPlugin) Route(ctx context.Context, req *pluginproto.HttpRequest) (*pluginproto.HttpResponse, error) {
	return nil, nil
}
func (p *flakyPersistAfterPlugin) Status() types.PluginStatus {
	return types.PluginStatusNormal
}

type mockPluginManager struct {
	pg types.Plugin
}

func (m *mockPluginManager) Plugins(methods ...types.PluginMethod) []types.Plugin {
	for _, method := range methods {
		if method == types.PluginPersistAfter && m.pg != nil {
			return []types.Plugin{m.pg}
		}
	}
	return nil
}
func (m *mockPluginManager) Plugin(no string) types.Plugin              { return m.pg }
func (m *mockPluginManager) UserIsAI(uid string) bool                   { return false }
func (m *mockPluginManager) GetUserPluginNo(uid string) (string, error) { return "", nil }

func TestPartA_V12_4_IngressPersistAfterPropagatesPluginError(t *testing.T) {
	oldOpts, oldPM := options.G, service.PluginManager
	t.Cleanup(func() {
		options.G = oldOpts
		service.PluginManager = oldPM
	})
	logOpts := wklog.NewOptions()
	logOpts.LogDir = t.TempDir()
	wklog.Configure(logOpts)

	opts := options.New()
	opts.Channel.ProcessTimeout = time.Second
	options.G = opts

	flaky := &flakyPersistAfterPlugin{failBefore: 1}
	service.PluginManager = &mockPluginManager{pg: flaky}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := "tcp://" + ln.Addr().String()
	_ = ln.Close()

	srv := wkserver.New(addr)
	mockCl := &routeCaptureCluster{srv: srv}
	ing := ingress.New()
	ing.BindDeps(mockCl, nil, nil, nil)
	ing.SetRoutes()
	require.NoError(t, srv.Start())
	t.Cleanup(func() { srv.Stop() })

	cli := client.New(addr, client.WithUid("test-v12-4"))
	require.NoError(t, cli.Start())
	t.Cleanup(func() { cli.Stop() })
	require.Eventually(t, func() bool { return cli.IsAuthed() }, 2*time.Second, 20*time.Millisecond)

	mb := &pluginproto.MessageBatch{
		Messages: []*pluginproto.Message{{MessageId: 1, MessageSeq: 1, ChannelId: "g1", ChannelType: 2}},
	}
	mbBytes, err := mb.Marshal()
	require.NoError(t, err)
	req := &ingress.PersistAfterReq{ChannelId: "g1", ChannelType: 2, Messages: mbBytes}
	reqBytes, err := req.Encode()
	require.NoError(t, err)

	// First RPC call must propagate plugin error via non-StatusOK response (closing V12-4).
	resp1, err1 := cli.Request("/wk/ingress/persistAfter", reqBytes)
	require.NoError(t, err1)
	require.Equal(t, proto.StatusError, resp1.Status)

	// Second RPC call succeeds cleanly.
	resp2, err2 := cli.Request("/wk/ingress/persistAfter", reqBytes)
	require.NoError(t, err2)
	require.Equal(t, proto.StatusOK, resp2.Status)
}

type routeCaptureCluster struct {
	icluster.ICluster
	srv *wkserver.Server
}

func (c *routeCaptureCluster) Route(p string, h wkserver.Handler) {
	if c.srv != nil {
		c.srv.Route(p, h)
	}
}

func TestPartA_Requirement4_FullUint32SlotDomainAndLongDerivedChannelIDs(t *testing.T) {
	// 1. Full uint32 SlotBitset + WKSM trailer round-trip (including > 65,536 and math.MaxUint32).
	highSlots := []uint32{0, 1, 63, 64, 255, 256, 1023, 65535, 65536, 100000, 1<<24 + 7, math.MaxUint32 - 1, math.MaxUint32}
	bs := eventbus.SlotIDsToBitset(highSlots)
	for _, s := range highSlots {
		require.True(t, bs.Has(s), "SlotBitset must contain uint32 slot %d", s)
	}
	require.False(t, bs.Has(2))
	require.False(t, bs.Has(65537))
	require.False(t, bs.Has(math.MaxUint32-2))

	req := &forwardChannelEventReq{
		fromNode:    1001,
		channelId:   strings.Repeat("x", 1024),
		channelType: wkproto.ChannelTypeGroup,
		events: eventbus.EventBatch{
			{
				Type:        eventbus.EventChannelDistribute,
				Conn:        &eventbus.Conn{Uid: "u_sender", DeviceId: "sys"},
				Frame:       &wkproto.SendPacket{ClientMsgNo: "c-high", ChannelID: strings.Repeat("x", 1024), ChannelType: 2, Payload: []byte("ok")},
				MessageId:   9001,
				MessageSeq:  42,
				ReasonCode:  wkproto.ReasonSuccess,
				PluginDone:  true,
				WebhookDone: true,
			},
		},
		hasSlotMask: true,
		slotMask:    bs,
	}
	encoded, err := req.encodeWithTrailer(true)
	require.NoError(t, err)

	// Verify 21cd8f88 legacy decode (EventBatch.Decode ignoring WKSM trailer) succeeds cleanly!
	dec21 := wkproto.NewDecoder(encoded)
	chID21, err := dec21.String()
	require.NoError(t, err)
	require.Len(t, chID21, 1024)
	_, _ = dec21.Uint8()
	_, _ = dec21.Uint64()
	remAll, err := dec21.BinaryAll()
	require.NoError(t, err)
	var legacyBatch eventbus.EventBatch
	require.NoError(t, legacyBatch.Decode(remAll), "21cd8f88 EventBatch.Decode must ignore trailing WKSM bytes")
	require.Len(t, legacyBatch, 1)

	// Verify PartAV2 decode restores full uint32 TargetSlotMask and PluginDone/WebhookDone flags!
	decodedReq := &forwardChannelEventReq{}
	require.NoError(t, decodedReq.decode(encoded))
	require.True(t, decodedReq.hasSlotMask)
	require.True(t, decodedReq.slotMask.Equal(bs))
	require.True(t, decodedReq.events[0].HasTargetSlotMask)
	require.True(t, decodedReq.events[0].TargetSlotMask.Equal(bs))
	require.True(t, decodedReq.events[0].PluginDone)
	require.True(t, decodedReq.events[0].WebhookDone)

	// 2. StageSeqTracker partial handoff with high uint32 slot IDs (65536 and math.MaxUint32).
	var tracker StageSeqTracker
	var part1, part2, unionHigh eventbus.SlotBitset
	part1.Set(65536)
	part2.Set(math.MaxUint32)
	unionHigh = part1.Or(part2)

	// 3. Long channel IDs (>256B, 409B Person+cmd u200@u200____cmd_, Agent ID, and 32,767B wire max) succeed; 32,768B fails cleanly without panic!
	u200A := "u_" + strings.Repeat("a", 198)
	u200B := "u_" + strings.Repeat("b", 198)
	derivedPersonCmd := options.New().OrginalConvertCmdChannel(options.GetFakeChannelIDWith(u200A, u200B))
	require.Greater(t, len(derivedPersonCmd), 400)
	derivedAgent := options.GetAgentChannelIDWith(u200A, "agent_"+strings.Repeat("z", 220))
	require.Greater(t, len(derivedAgent), 420)
	maxWireCh := strings.Repeat("m", eventbus.MaxWireStringBytes) // 32,767 B

	for _, longID := range []string{
		strings.Repeat("g", 300),
		derivedPersonCmd,
		derivedAgent,
		maxWireCh,
	} {
		wmK, ok := MakeChannelWatermarkKey(longID, wkproto.ChannelTypeGroup)
		require.True(t, ok)
		d1, l1 := tracker.BeginSlotStage(wmK, StageChannelDistribute, 1, unionHigh)
		require.True(t, l1)
		require.True(t, d1.Equal(unionHigh))
		// Complete only slot 65536 first; math.MaxUint32 remains pending:
		tracker.FinishSlotStage(wmK, StageChannelDistribute, 1, d1, part1, unionHigh)
		require.False(t, tracker.HasCommitted(wmK, StageChannelDistribute, 1))

		// Retry unionHigh -> only math.MaxUint32 is leased:
		d2, l2 := tracker.BeginSlotStage(wmK, StageChannelDistribute, 1, unionHigh)
		require.True(t, l2)
		require.True(t, d2.Equal(part2))
		tracker.FinishSlotStage(wmK, StageChannelDistribute, 1, d2, part2, unionHigh)
		require.True(t, tracker.HasCommitted(wmK, StageChannelDistribute, 1))

		wireReq := &forwardChannelEventReq{
			fromNode:    1001,
			channelId:   longID,
			channelType: wkproto.ChannelTypeGroup,
			events: eventbus.EventBatch{
				{
					Type:       eventbus.EventChannelDistribute,
					Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
					Frame:      &wkproto.SendPacket{ClientMsgNo: "c1", ChannelID: "short", ChannelType: 2, Payload: []byte("p")},
					MessageId:  1,
					MessageSeq: 1,
				},
			},
		}
		b, encErr := wireReq.encodeWithTrailer(false)
		require.NoError(t, encErr)
		outReq := &forwardChannelEventReq{}
		require.NoError(t, outReq.decode(b))
		require.Equal(t, longID, outReq.channelId)
	}

	// Over-32,767B channelId must return ErrWireChannelIDTooLong cleanly without panicking wkproto.Encoder!
	overMaxID := strings.Repeat("x", eventbus.MaxWireStringBytes+1)
	overReq := &forwardChannelEventReq{channelId: overMaxID, channelType: 2}
	_, overErr := overReq.encode()
	require.ErrorIs(t, overErr, eventbus.ErrWireChannelIDTooLong)
}

func TestPartA_V11_1_ConversationPushForTargetUsersNoDeletedResurrection(t *testing.T) {
	oldOpts := options.G
	t.Cleanup(func() { options.G = oldOpts })
	opts := options.New()
	opts.DataDir = t.TempDir()
	opts.WhitelistOffOfPerson = true
	options.G = opts

	cmdCh := opts.OrginalConvertCmdChannel("cmd_room")
	cm := manager.NewConversationManager(4)
	ev := &eventbus.Event{
		Type:       eventbus.EventChannelDistribute,
		Conn:       &eventbus.Conn{Uid: "alice", DeviceId: opts.SystemDeviceId},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "c1", ChannelID: cmdCh, ChannelType: 2, Payload: []byte("hi")},
		MessageId:  1001,
		MessageSeq: 1,
	}

	// Initial delivery reaches slot user "bob" only; "charlie" is on a delayed slot.
	cm.PushForTargetUsers(cmdCh, wkproto.ChannelTypeGroup, "tag-v1", []string{"alice", "bob"}, []*eventbus.Event{ev})
	bobChs, err := cm.GetUserChannelsFromCache("bob", wkdb.ConversationTypeCMD)
	require.NoError(t, err)
	require.Len(t, bobChs, 1)

	// Bob deletes the conversation from cache before Charlie's slot recovers.
	require.NoError(t, cm.DeleteFromCache("bob", cmdCh, wkproto.ChannelTypeGroup))
	bobChsAfterDel, err := cm.GetUserChannelsFromCache("bob", wkdb.ConversationTypeCMD)
	require.NoError(t, err)
	require.Empty(t, bobChsAfterDel)

	// Partial-slot recovery delivers seq=1 (same LastMsgSeq=1 and same TagKey="tag-v1"!) strictly for targetUIDs=["charlie"].
	cm.PushForTargetUsers(cmdCh, wkproto.ChannelTypeGroup, "tag-v1", []string{"charlie"}, []*eventbus.Event{ev})

	// Charlie MUST receive the conversation even though LastMsgSeq==1 && TagKey=="tag-v1", AND Bob's deleted conversation MUST NOT resurrect!
	charlieChs, err := cm.GetUserChannelsFromCache("charlie", wkdb.ConversationTypeCMD)
	require.NoError(t, err)
	require.Len(t, charlieChs, 1)

	bobChsFinal, err := cm.GetUserChannelsFromCache("bob", wkdb.ConversationTypeCMD)
	require.NoError(t, err)
	require.Empty(t, bobChsFinal, "V11-1: partial-slot replay must not resurrect Bob's deleted conversation")
}

type capProbeMockCluster struct {
	icluster.ICluster
	mode          atomic.Int32 // 0: timeout, 1: BaseV1, 2: PartAV2
	legacySends   atomic.Int32
	v1Calls       atomic.Int32
	v1AllowStatus proto.Status
}

func (c *capProbeMockCluster) RequestWithContext(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
	if path == forward.CapabilityPath {
		switch c.mode.Load() {
		case 0:
			return nil, context.DeadlineExceeded
		case 1:
			return &proto.Response{Status: proto.StatusOK}, nil
		case 2:
			return &proto.Response{Status: proto.StatusOK, Body: forward.BuildCapabilityBody()}, nil
		}
	}
	if path == forward.ChannelPath {
		c.v1Calls.Add(1)
		return &proto.Response{Status: c.v1AllowStatus}, nil
	}
	return &proto.Response{Status: proto.StatusOK}, nil
}

func (c *capProbeMockCluster) Send(uint64, *proto.Message) error {
	c.legacySends.Add(1)
	return nil
}

func TestPartA_OD1_V12_3_CapabilityProbeAnd24MiBLegacySpillover(t *testing.T) {
	oldOpts := options.G
	t.Cleanup(func() { options.G = oldOpts })
	options.G = options.New()

	// 1. V12-3: On PeerCapUnknown (probe timeout), persisted (MessageSeq > 0) events MUST NOT downgrade to fire-and-forget legacy Send!
	mc := &capProbeMockCluster{v1AllowStatus: proto.StatusOK}
	mc.mode.Store(0) // timeout -> PeerCapUnknown
	var capCache forward.CapabilityCache
	require.Equal(t, forward.PeerCapUnknown, capCache.ProbeOn(context.Background(), mc, 1002))

	persistedEv := &eventbus.Event{
		Type:       eventbus.EventChannelDistribute,
		Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
		Frame:      &wkproto.SendPacket{ClientMsgNo: "c1", ChannelID: "g1", ChannelType: 2, Payload: []byte("p")},
		MessageId:  1,
		MessageSeq: 1,
	}
	shortEv := persistedEv.Clone()
	shortEv.ForwardDeadline = time.Now().Add(120 * time.Millisecond).UnixMilli()
	err := forward.RequestOnCluster(mc, forward.ChannelPath, []byte("payload"), []*eventbus.Event{shortEv}, func() uint64 { return 1002 }, 1001, nil, &capCache, func(n uint64) error {
		return mc.Send(n, &proto.Message{})
	})
	require.Error(t, err)
	require.Zero(t, mc.legacySends.Load(), "V12-3: persisted events must NEVER fall back to fire-and-forget legacy Send when capability probe times out")

	// Once peer responds to probe as BaseV1 (21cd8f88) and then upgrades to PartAV2, ProbeOn transitions cleanly without stale cache poisoning:
	mc.mode.Store(1)
	require.Equal(t, forward.PeerCapBaseV1, capCache.ProbeOn(context.Background(), mc, 1002))
	capCache.InvalidateAll()
	mc.mode.Store(2)
	require.Equal(t, forward.PeerCapPartAV2, capCache.ProbeOn(context.Background(), mc, 1002))

	// 2. OD-1: Inbound legacy msgForwardChannelEvent (3002) uses Primary PostCommitBudget + LegacyInboundBudget spillover,
	//    and increments LegacyForwardDroppedTotal strictly when BOTH budgets are saturated!
	primary := eventbus.NewPostCommitBudget(6*1024, 10)
	legacySpill := eventbus.NewPostCommitBudget(6*1024, 10)
	transient := eventbus.NewPostCommitBudget(4*1024, 10)

	var admitted []*eventbus.Event
	capture := &forwardChannel{}
	h := NewHandler()
	defer h.DrainAndStop(0)
	h.BindNodeDeps(NodeDeps{
		NodeId:              1001,
		ChannelBus:          capture,
		Cluster:             &forwardChannelCluster{leader: 1001},
		PostCommitBudget:    primary,
		LegacyInboundBudget: legacySpill,
		TransientBudget:     transient,
	})

	droppedBefore := eventbus.LegacyForwardDroppedTotal.Load()
	for i := 1; i <= 6; i++ {
		ev := &eventbus.Event{
			Type:       eventbus.EventChannelDistribute,
			Conn:       &eventbus.Conn{Uid: "u1", DeviceId: "sys"},
			Frame:      &wkproto.SendPacket{ClientMsgNo: fmt.Sprintf("leg-%d", i), ChannelID: "g_leg", ChannelType: 2, Payload: make([]byte, 512)},
			MessageId:  int64(i),
			MessageSeq: uint64(i),
		}
		fReq := &forwardChannelEventReq{fromNode: 1002, channelId: "g_leg", channelType: 2, events: eventbus.EventBatch{ev}}
		raw, encErr := fReq.encode()
		require.NoError(t, encErr)
		h.OnMessage(&proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: raw})
	}
	admitted = capture.events
	require.Equal(t, 4, len(admitted), "2 events fit in 6KiB primary + 2 events spill into 6KiB legacy budget; events 5..6 exceed combined cap")
	require.Equal(t, droppedBefore+2, eventbus.LegacyForwardDroppedTotal.Load(), "overflow above combined primary+legacy budget must increment wk_legacy_forward_dropped_total")

	// Release tickets on admitted events -> both primary and legacy spillover budgets return to 0!
	for _, ev := range admitted {
		if ev.Ticket != nil {
			ev.Ticket.Release()
		}
	}
	pB, pI, _ := primary.Stats()
	lB, lI, _ := legacySpill.Stats()
	require.Zero(t, pB)
	require.Zero(t, pI)
	require.Zero(t, lB)
	require.Zero(t, lI)
}
