package handler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type progressCluster struct {
	icluster.ICluster
	calls  map[uint64]int
	legacy int
	status proto.Status
}

func (c *progressCluster) RequestWithContext(_ context.Context, node uint64, _ string, _ []byte) (*proto.Response, error) {
	c.calls[node]++
	if c.status != 0 {
		return &proto.Response{Status: c.status}, nil
	}
	if node == 3 && c.calls[node] == 1 {
		return nil, errors.New("lost acknowledgement")
	}
	return &proto.Response{Status: proto.StatusOK}, nil
}
func (c *progressCluster) Send(uint64, *proto.Message) error { c.legacy++; return nil }

func progressContext(tag *types.Tag) *eventbus.ChannelContext {
	return &eventbus.ChannelContext{SlotLeaderId: 1, ChannelId: "channel", ChannelType: wkproto.ChannelTypeData,
		Events: []*eventbus.Event{{Type: eventbus.EventChannelDistribute, MessageId: 101, TagKey: tag.Key,
			Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{}, Distribution: &eventbus.DistributionState{Snapshot: tag}}}}
}

func TestDistributionRetrySkipsCompletedDestinations(t *testing.T) {
	_, pusher := versionTestFixture(t)
	options.G.Cluster.NodeId = 1
	c := &progressCluster{calls: make(map[uint64]int)}
	service.Cluster = c
	tag := &types.Tag{Key: "A", Nodes: []*types.Node{
		{LeaderId: 1, Uids: []string{"local"}}, {LeaderId: 2, Uids: []string{"remote-2"}}, {LeaderId: 3, Uids: []string{"remote-3"}},
	}}
	h := NewHandler()
	ctx := progressContext(tag)
	h.distributeCommon(ctx)
	require.Len(t, ctx.RetryEvents, 1)
	require.Len(t, pusher.events, 1)
	ctx = &eventbus.ChannelContext{SlotLeaderId: 1, ChannelId: "channel", ChannelType: wkproto.ChannelTypeData, Events: ctx.RetryEvents}
	h.distributeCommon(ctx)
	require.Empty(t, ctx.RetryEvents)
	require.Equal(t, map[uint64]int{2: 1, 3: 2}, c.calls)
	require.Zero(t, c.legacy, "ambiguous new-protocol responses never fall back to a second transport")
	require.Len(t, pusher.events, 1, "local recipients must not be replayed")
}

func TestDistributionLegacyFallbackRequiresMissingRoute(t *testing.T) {
	for _, status := range []proto.Status{proto.StatusNotFound, 99} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			versionTestFixture(t)
			options.G.Cluster.NodeId = 1
			c := &progressCluster{calls: make(map[uint64]int), status: status}
			service.Cluster = c
			tag := &types.Tag{Key: "A", Nodes: []*types.Node{{LeaderId: 2, Uids: []string{"remote"}}}}
			ctx := progressContext(tag)
			NewHandler().distributeCommon(ctx)
			if status == proto.StatusNotFound {
				require.Equal(t, 1, c.legacy)
				require.Empty(t, ctx.RetryEvents)
			} else {
				require.Zero(t, c.legacy)
				require.Len(t, ctx.RetryEvents, 1)
			}
		})
	}
}

func TestDistributionRetriesHaveAttemptAndTimeBounds(t *testing.T) {
	versionTestFixture(t)
	options.G.Cluster.NodeId = 1
	c := &progressCluster{calls: make(map[uint64]int), status: 99}
	service.Cluster = c
	tag := &types.Tag{Key: "A", Nodes: []*types.Node{{LeaderId: 2, Uids: []string{"remote"}}}}
	h := NewHandler()
	now := time.Now()
	h.distributionNow = func() time.Time { return now }
	ctx := progressContext(tag)
	for i := 0; i < distributionMaxAttempts; i++ {
		h.distributeCommon(ctx)
		if i == distributionMaxAttempts-1 {
			require.Empty(t, ctx.RetryEvents)
			break
		}
		require.Len(t, ctx.RetryEvents, 1)
		ctx = &eventbus.ChannelContext{SlotLeaderId: 1, ChannelId: "channel", ChannelType: wkproto.ChannelTypeData, Events: ctx.RetryEvents}
	}
	require.Equal(t, distributionMaxAttempts, c.calls[2])
	ctx = progressContext(tag)
	ctx.Events[0].Distribution.Deadline = now.Add(-time.Second)
	h.distributeCommon(ctx)
	require.Empty(t, ctx.RetryEvents)
}
