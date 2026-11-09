package ingress

import (
	"encoding/json"
	"errors"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
)

type SubscriberSnapshotReq struct {
	ChannelId   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	Revision    string `json:"revision"`
}
type SubscriberSnapshotResp struct {
	Revision    string   `json:"revision"`
	Changed     bool     `json:"changed"`
	Subscribers []string `json:"subscribers,omitempty"`
}

func (c *Client) RequestSubscriberSnapshot(node uint64, req *SubscriberSnapshotReq) (*SubscriberSnapshotResp, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.request(node, "/wk/ingress/subscriberSnapshot", body)
	if err != nil {
		return nil, err
	}
	if err = c.handleRespError(resp); err != nil {
		return nil, err
	}
	var snapshot SubscriberSnapshotResp
	if err = json.Unmarshal(resp.Body, &snapshot); err != nil {
		return nil, err
	}
	if snapshot.Revision == "" || (!snapshot.Changed && snapshot.Revision != req.Revision) {
		return nil, errors.New("invalid subscriber snapshot revision")
	}
	return &snapshot, nil
}

func (i *Ingress) handleSubscriberSnapshot(c *wkserver.Context) {
	var req SubscriberSnapshotReq
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		c.WriteErr(err)
		return
	}
	if req.ChannelId == "" || req.ChannelType == 0 {
		c.WriteErr(errors.New("channel id and type are required"))
		return
	}
	owner, err := service.Cluster.SlotLeaderIdOfChannel(req.ChannelId, req.ChannelType)
	if err != nil {
		c.WriteErr(err)
		return
	}
	if owner == 0 || !options.G.IsLocalNode(owner) {
		c.WriteErr(errors.New("subscriber snapshot must reach its slot leader"))
		return
	}
	snapshot, err := service.Store.GetSubscriberSnapshot(req.ChannelId, req.ChannelType, req.Revision)
	if err != nil {
		c.WriteErr(err)
		return
	}
	resp := SubscriberSnapshotResp{Revision: snapshot.Revision, Changed: snapshot.Changed}
	for _, m := range snapshot.Members {
		resp.Subscribers = append(resp.Subscribers, m.Uid)
	}
	body, err := json.Marshal(resp)
	if err != nil {
		c.WriteErr(err)
		return
	}
	c.Write(body)
}
