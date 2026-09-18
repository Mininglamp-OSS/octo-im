package api

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

type recoveryReadCluster struct {
	icluster.ICluster
	cfg wkdb.ChannelClusterConfig
	err error
}

type recoveryRemoteCluster struct {
	icluster.ICluster
	requests []string
	failure  error
}

func (c *recoveryRemoteCluster) LoadOnlyChannelClusterConfig(string, uint8) (wkdb.ChannelClusterConfig, error) {
	return wkdb.ChannelClusterConfig{LeaderId: 3}, nil
}
func (c *recoveryRemoteCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) { return 2, nil }
func (c *recoveryRemoteCluster) RequestWithContext(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	if c.failure != nil {
		return nil, c.failure
	}
	var req struct {
		ChannelID string `json:"channel_id"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	switch path {
	case "/wk/ingress/subscriberReadFloor":
		if node != 3 {
			return nil, errors.New("message tail must use message leader")
		}
		return &proto.Response{Status: proto.StatusOK, Body: []byte("37")}, nil
	case "/wk/ingress/subscriberTagInvalidate":
		if node != 2 {
			return nil, errors.New("tag invalidation must use slot leader")
		}
		c.requests = append(c.requests, req.ChannelID)
		return &proto.Response{Status: proto.StatusOK}, nil
	default:
		return nil, errors.New("unexpected request")
	}
}

func TestSubscriberRecoveryUsesDistinctMessageAndTagAuthorities(t *testing.T) {
	oldCluster, oldOptions := service.Cluster, options.G
	defer func() { service.Cluster = oldCluster; options.G = oldOptions }()
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	remote := &recoveryRemoteCluster{}
	service.Cluster = remote
	s := &Server{client: ingress.NewClient()}
	seq, err := s.subscriberReadFloor(context.Background(), "g", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(37), seq)
	w := wkdb.SubscriberWork{Operation: wkdb.SubscriberOperation{ChannelID: "g", ChannelType: 2}}
	require.NoError(t, s.finalizeSubscriberWork(context.Background(), w))
	require.Equal(t, []string{"g", options.G.OrginalConvertCmdChannel("g")}, remote.requests)
	remote.failure = errors.New("remote tag unavailable")
	require.ErrorIs(t, s.finalizeSubscriberWork(context.Background(), w), remote.failure)
}

func (c recoveryReadCluster) LoadOnlyChannelClusterConfig(string, uint8) (wkdb.ChannelClusterConfig, error) {
	return c.cfg, c.err
}

func TestSubscriberRecoveryReadFloorMissingAndUnavailable(t *testing.T) {
	old := service.Cluster
	defer func() { service.Cluster = old }()
	s := &Server{}
	service.Cluster = recoveryReadCluster{err: wkdb.ErrNotFound}
	seq, err := s.subscriberReadFloor(context.Background(), "new-group", 2)
	require.NoError(t, err)
	require.Zero(t, seq)
	service.Cluster = recoveryReadCluster{}
	_, err = s.subscriberReadFloor(context.Background(), "existing-group", 2)
	require.ErrorContains(t, err, "leader unavailable")
	unavailable := errors.New("source unavailable")
	service.Cluster = recoveryReadCluster{err: unavailable}
	_, err = s.subscriberReadFloor(context.Background(), "group", 2)
	require.ErrorIs(t, err, unavailable)
}
