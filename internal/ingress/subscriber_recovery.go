package ingress

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
)

type subscriberChannel struct {
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
}

func (c *Client) InvalidateSubscriberTag(ctx context.Context, node uint64, ch string, tp uint8) error {
	b, err := json.Marshal(subscriberChannel{ch, tp})
	if err != nil {
		return err
	}
	resp, err := service.Cluster.RequestWithContext(ctx, node, "/wk/ingress/subscriberTagInvalidate", b)
	if err != nil {
		return err
	}
	if resp.Status != proto.StatusOK {
		return errors.New("subscriber tag invalidation failed")
	}
	return nil
}

func (c *Client) SubscriberReadFloor(ctx context.Context, node uint64, ch string, tp uint8) (uint64, error) {
	b, err := json.Marshal(subscriberChannel{ch, tp})
	if err != nil {
		return 0, err
	}
	resp, err := service.Cluster.RequestWithContext(ctx, node, "/wk/ingress/subscriberReadFloor", b)
	if err != nil {
		return 0, err
	}
	if resp.Status != proto.StatusOK {
		return 0, errors.New("subscriber read floor failed")
	}
	var seq uint64
	err = json.Unmarshal(resp.Body, &seq)
	return seq, err
}

func (i *Ingress) handleSubscriberTagInvalidate(c *wkserver.Context) {
	var req subscriberChannel
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(err)
		return
	}
	if req.ChannelID == "" || req.ChannelType == 0 {
		c.WriteErr(errors.New("invalid subscriber tag"))
		return
	}
	leader, err := service.Cluster.SlotLeaderIdOfChannel(req.ChannelID, req.ChannelType)
	if err != nil {
		c.WriteErr(err)
		return
	}
	if leader != options.G.Cluster.NodeId {
		c.WriteErr(errors.New("subscriber tag leader changed"))
		return
	}
	service.InvalidateSubscriberTag(req.ChannelID, req.ChannelType)
	c.WriteOk()
}

func (i *Ingress) handleSubscriberReadFloor(c *wkserver.Context) {
	var req subscriberChannel
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(err)
		return
	}
	if req.ChannelID == "" || req.ChannelType == 0 {
		c.WriteErr(errors.New("invalid subscriber channel"))
		return
	}
	leader, err := service.Cluster.LeaderOfChannelForRead(req.ChannelID, req.ChannelType)
	if err != nil {
		c.WriteErr(err)
		return
	}
	if leader.Id != options.G.Cluster.NodeId {
		c.WriteErr(errors.New("subscriber message leader changed"))
		return
	}
	seq, err := service.Store.GetLastMsgSeq(req.ChannelID, req.ChannelType)
	if err != nil {
		c.WriteErr(err)
		return
	}
	b, err := json.Marshal(seq)
	if err != nil {
		c.WriteErr(err)
		return
	}
	c.Write(b)
}
