package handler

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type reviewDistributionCluster struct {
	versionTestCluster
	request func(context.Context, uint64, string, []byte) (*proto.Response, error)
	sends   int
}

func (c *reviewDistributionCluster) RequestWithContext(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	return c.request(ctx, node, path, body)
}
func (c *reviewDistributionCluster) Send(uint64, *proto.Message) error { c.sends++; return nil }

func TestDistributionReviewCanonicalRetryDoesNotDiscardFreshMessage(t *testing.T) {
	for _, key := range []string{"A", "B"} {
		t.Run(key, func(t *testing.T) {
			m, _ := versionTestFixture(t)
			previous := eventbus.Channel
			t.Cleanup(func() { eventbus.Channel = previous })
			sink := &forwardChannel{}
			eventbus.RegisterChannel(sink)
			receiver := NewHandler()
			require.Equal(t, proto.StatusOK, receiver.acceptDistribution(distributionTestBody(t, 2, "A", 101)))
			_, err := m.MakeTagWithTagKey("B", []string{"fixed", "new"})
			require.NoError(t, err)
			m.SetChannelTag("channel", wkproto.ChannelTypeData, key)
			service.Cluster = &reviewDistributionCluster{request: func(_ context.Context, _ uint64, path string, body []byte) (*proto.Response, error) {
				if path != distributionPath {
					return &proto.Response{Status: proto.StatusOK}, nil
				}
				options.G.Cluster.NodeId = 2
				status := receiver.acceptDistribution(body)
				options.G.Cluster.NodeId = 1
				return &proto.Response{Status: status}, nil
			}}
			options.G.Cluster.NodeId = 1
			retry := &eventbus.Event{Type: eventbus.EventChannelDistribute, MessageId: 901, ReasonCode: wkproto.ReasonSuccess,
				Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{ClientMsgNo: "same"}}
			require.NoError(t, applyPersistResults([]*eventbus.Event{retry}, []wkdb.Message{{RecvPacket: wkproto.RecvPacket{MessageID: 901}}},
				rafttypes.ProposeRespSet{{Id: 901, CanonicalID: 101, Index: 1, Duplicate: true}}))
			require.True(t, shouldDistributePersistResult(retry))
			fresh := &eventbus.Event{Type: eventbus.EventChannelDistribute, MessageId: 102, Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{}}
			ctx := &eventbus.ChannelContext{ChannelId: "channel", ChannelType: wkproto.ChannelTypeData, SlotLeaderId: 1, Events: []*eventbus.Event{retry, fresh}}
			NewHandler().distributeCommon(ctx)
			require.Empty(t, ctx.RetryEvents)
			require.Len(t, sink.events, 2, "an already-admitted retry must not discard fresh 102")
			require.Equal(t, int64(102), sink.events[1].MessageId)
			require.Equal(t, key, sink.events[1].TagKey)
			require.Equal(t, "A", sink.events[0].TagKey, "the original admission keeps its recipients")
		})
	}
}

func TestDistributionReviewRecipientSnapshotUpdatesConversation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		channelType uint8
		cmd         bool
	}{
		{"person", wkproto.ChannelTypePerson, false}, {"customer", wkproto.ChannelTypeCustomerService, false}, {"cmd", wkproto.ChannelTypeGroup, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _ := versionTestFixture(t)
			channelType, channelID, conversationType := tc.channelType, "room", wkdb.ConversationTypeChat
			if tc.cmd {
				channelID = options.G.OrginalConvertCmdChannel(channelID)
				conversationType = wkdb.ConversationTypeCMD
			}
			previousChannel, previousConversations := eventbus.Channel, service.ConversationManager
			t.Cleanup(func() { eventbus.Channel, service.ConversationManager = previousChannel, previousConversations })
			sink := &forwardChannel{}
			eventbus.RegisterChannel(sink)
			conversations := manager.NewConversationManager(1)
			service.ConversationManager = conversations
			m.RemoveTag("A")
			request := &distributionRequest{source: 1, target: 2, channel: channelID, channelType: channelType, key: "A", uids: []string{"fixed"},
				events: eventbus.EventBatch{{Type: eventbus.EventChannelDistribute, MessageId: 101, MessageSeq: 1, TagKey: "A", Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{}}}}
			body, err := request.encode()
			require.NoError(t, err)
			h := NewHandler()
			require.Equal(t, proto.StatusOK, h.acceptDistribution(body))
			h.distributeCommon(&eventbus.ChannelContext{ChannelId: channelID, ChannelType: channelType, SlotLeaderId: 1, Events: sink.events})
			channels, err := conversations.GetUserChannelsFromCache("fixed", conversationType)
			require.NoError(t, err)
			require.Equal(t, []wkproto.Channel{{ChannelID: channelID, ChannelType: channelType}}, channels)
			require.Nil(t, m.Get("A"), "conversation updates must not publish unbounded receiver cache entries")
		})
	}
}

func TestDistributionReviewAmbiguousAdmissionNeverDowngrades(t *testing.T) {
	versionTestFixture(t)
	options.G.Cluster.NodeId = 1
	probes, admissions := 0, 0
	cluster := &reviewDistributionCluster{request: func(_ context.Context, _ uint64, path string, _ []byte) (*proto.Response, error) {
		if path == distributionCapabilityPath {
			probes++
			if probes > 1 {
				return nil, context.DeadlineExceeded
			}
			return &proto.Response{Status: proto.StatusOK}, nil
		}
		admissions++
		if admissions == 1 {
			return nil, context.DeadlineExceeded
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}}
	service.Cluster = cluster
	h := NewHandler()
	ctx := progressContext(&types.Tag{Key: "A", Nodes: []*types.Node{{LeaderId: 2, Uids: []string{"fixed"}}}})
	h.distributeCommon(ctx)
	require.Len(t, ctx.RetryEvents, 1)
	h.forwardCapabilities = forward.CapabilityCache{} // Simulate expiry before the next retry.
	ctx.Events, ctx.RetryEvents = ctx.RetryEvents, nil
	h.distributeCommon(ctx)
	require.Empty(t, ctx.RetryEvents)
	require.Equal(t, 1, probes)
	require.Equal(t, 2, admissions)
	require.Zero(t, cluster.sends, "an ambiguous snapshot admission must never switch to legacy")
}

func TestDistributionReviewCapabilityCacheIsRouteSpecific(t *testing.T) {
	versionTestFixture(t)
	options.G.Cluster.NodeId = 1
	cluster := &reviewDistributionCluster{request: func(_ context.Context, _ uint64, path string, _ []byte) (*proto.Response, error) {
		if path == forward.CapabilityPath {
			return &proto.Response{Status: proto.StatusOK}, nil
		}
		return nil, context.DeadlineExceeded
	}}
	service.Cluster = cluster
	h := NewHandler()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.True(t, h.forwardCapabilities.Supports(ctx, 2))
	distribution := progressContext(&types.Tag{Key: "A", Nodes: []*types.Node{{LeaderId: 2, Uids: []string{"fixed"}}}})
	h.distributeCommon(distribution)
	require.Empty(t, distribution.RetryEvents)
	require.Equal(t, 1, cluster.sends, "a generic forwarding capability does not imply snapshot distribution support")
}

func TestDistributionReviewEmptyOnlineVersionDoesNotBlockTail(t *testing.T) {
	versionTestFixture(t)
	ctx := &eventbus.ChannelContext{ChannelId: "channel", ChannelType: wkproto.ChannelTypeData, SlotLeaderId: 2,
		Events: []*eventbus.Event{{Type: eventbus.EventChannelDistribute, MessageId: 101, Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{}}}}
	NewHandler().distributeOnlineCmd(ctx)
	require.Empty(t, ctx.RetryEvents, "an event with no version can never recover by retrying")
}

func TestDistributionReviewLegacyRecipientDoesNotBounceToSource(t *testing.T) {
	versionTestFixture(t)
	options.G.Cluster.NodeId = 1
	cluster := &reviewDistributionCluster{request: func(context.Context, uint64, string, []byte) (*proto.Response, error) {
		return &proto.Response{Status: proto.StatusOK}, nil
	}}
	service.Cluster = cluster
	tag := &types.Tag{Key: "A", Nodes: []*types.Node{{LeaderId: 1, Uids: []string{"local"}}, {LeaderId: 2, Uids: []string{"source"}}}}
	ctx := progressContext(tag)
	ctx.Events[0].SourceNodeId = 2
	h := NewHandler()
	// A returning legacy event has no captured snapshot, only an exact tag key.
	_, err := service.TagManager.MakeTagWithTagKey("legacy", []string{"source"})
	require.NoError(t, err)
	ctx.Events[0].Distribution = nil
	ctx.Events[0].TagKey = "legacy"
	h.distributeCommon(ctx)
	require.Zero(t, cluster.sends)
	// The request count is visible through the callback for both transports.
	calls := 0
	cluster.request = func(context.Context, uint64, string, []byte) (*proto.Response, error) {
		calls++
		return &proto.Response{Status: forward.StatusRetry}, nil
	}
	ctx.Events[0].Distribution = nil
	h.distributeCommon(ctx)
	require.Zero(t, calls, "legacy fan-out must not return to its source")
}

func TestDistributionReviewMalformedCountDoesNotAmplifyAllocation(t *testing.T) {
	e := wkproto.NewEncoder()
	e.WriteUint8(1)
	e.WriteUint64(1)
	e.WriteUint64(2)
	e.WriteString("channel")
	e.WriteUint8(wkproto.ChannelTypeData)
	e.WriteString("A")
	e.WriteUint32(128 * 1024)
	body := append(append([]byte(nil), e.Bytes()...), make([]byte, 512*1024)...)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := decodeDistribution(body)
	runtime.ReadMemStats(&after)
	require.Error(t, err)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(512*1024), "invalid count must not allocate megabytes before its first uid is validated")
}

func TestDistributionReviewMissingOnlineVersionIsTerminal(t *testing.T) {
	versionTestFixture(t)
	ctx := &eventbus.ChannelContext{ChannelId: "channel", ChannelType: wkproto.ChannelTypeData, SlotLeaderId: 2,
		Events: []*eventbus.Event{{Type: eventbus.EventChannelDistribute, MessageId: 101, TagKey: "evicted", Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{}}}}
	NewHandler().distributeOnlineCmd(ctx)
	require.Empty(t, ctx.RetryEvents, "a missing local online-CMD version must not block the global queue")
}

func TestDistributionReviewLegacyPersonReusesStableTag(t *testing.T) {
	m, _ := versionTestFixture(t)
	h := NewHandler()
	ctx := &eventbus.ChannelContext{ChannelId: "sender@recipient", ChannelType: wkproto.ChannelTypePerson, SlotLeaderId: 1,
		Events: []*eventbus.Event{{TagKey: "foreign-version"}}}
	first, err := h.getCommonTag(ctx)
	require.NoError(t, err)
	for i := 0; i < 64; i++ {
		next, err := h.getCommonTag(ctx)
		require.NoError(t, err)
		require.Equal(t, first.Key, next.Key, "a stable Person channel must not mint a local tag per legacy event")
	}
	require.Len(t, m.GetAllTags(), 2) // The fixture's A and the stable person tag.
}
