package handler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/common"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/manager"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type subscriberDistributionCluster struct {
	icluster.ICluster
	request func(uint64, []byte) (*proto.Response, error)
	owner   uint64
}

func (c *subscriberDistributionCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) {
	return c.owner, nil
}
func (*subscriberDistributionCluster) GetSlotId(string) uint32    { return 1 }
func (*subscriberDistributionCluster) SlotLeaderId(uint32) uint64 { return 1002 }
func (c *subscriberDistributionCluster) RequestWithContext(_ context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	if path != "/wk/ingress/subscriberSnapshot" {
		return nil, errors.New("unexpected route")
	}
	return c.request(node, body)
}

func TestDistributionValidatesWarmSubscriberTagBeforeUse(t *testing.T) {
	oldOpts, oldCluster, oldTags := options.G, service.Cluster, service.TagManager
	t.Cleanup(func() { options.G, service.Cluster, service.TagManager = oldOpts, oldCluster, oldTags })
	options.G = options.New()
	options.G.Cluster.NodeId = 1002
	c := &subscriberDistributionCluster{owner: 1001}
	service.Cluster = c
	tags := manager.NewTagManager(2, func() uint64 { return 1 })
	service.TagManager = tags
	h := &Handler{commonService: common.NewService()}
	for _, id := range []string{"room", "room____cmd"} {
		t.Run(id, func(t *testing.T) {
			old, err := tags.MakeSubscriberTag([]string{"old"}, "v-old")
			require.NoError(t, err)
			tags.SetChannelTag(id, 2, old.Key)
			ctx := &eventbus.ChannelContext{ChannelId: id, ChannelType: 2, SlotLeaderId: 1002}
			c.request = func(node uint64, body []byte) (*proto.Response, error) {
				require.Equal(t, uint64(1001), node)
				var req ingress.SubscriberSnapshotReq
				require.NoError(t, json.Unmarshal(body, &req))
				require.Equal(t, "room", req.ChannelId)
				require.Equal(t, "v-old", req.Revision)
				return &proto.Response{Status: proto.StatusOK, Body: []byte(`{"revision":"v-new","changed":true,"subscribers":["new"]}`)}, nil
			}
			got, err := h.getCommonTag(ctx)
			require.NoError(t, err)
			require.Equal(t, []string{"new"}, got.GetUsers())
			require.NotEqual(t, old.Key, got.Key)
			// A delayed incremental mutation must not corrupt the certified
			// snapshot already handed to distribution or keep its revision.
			require.NoError(t, tags.AddUsers(got.Key, []string{"stale"}))
			require.Equal(t, []string{"new"}, got.GetUsers())
			require.Empty(t, tags.Get(got.Key).SubscriberRevision.Load())
			c.request = func(uint64, []byte) (*proto.Response, error) { return nil, errors.New("partition") }
			got, err = h.getCommonTag(ctx)
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
	// Explicit temporary recipients must not be replaced by an empty DB list.
	temp, err := tags.MakeTag([]string{"temporary"})
	require.NoError(t, err)
	tags.SetChannelTag("temporary-room", wkproto.ChannelTypeTemp, temp.Key)
	got, err := h.getOrMakeTagForLeader("temporary-room", wkproto.ChannelTypeTemp)
	require.NoError(t, err)
	require.Same(t, temp, got)
}
