package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
)

// A business-owned channel opts into snapshot authority with its first
// successful revision. Legacy member/denylist deltas are then rejected; they
// cannot race a delayed snapshot and undo a later business transaction.
func (ch *channel) reconcileSubscribers(c *wkhttp.Context) {
	var req struct {
		ChannelID   string   `json:"channel_id"`
		ChannelType uint8    `json:"channel_type"`
		OperationID string   `json:"operation_id"`
		Revision    uint64   `json:"revision"`
		SnapshotID  string   `json:"snapshot_id"`
		PageIndex   uint32   `json:"page_index"`
		PageCount   uint32   `json:"page_count"`
		RangeStart  string   `json:"range_start"`
		RangeEnd    string   `json:"range_end"`
		Subscribers []string `json:"subscribers"`
		Denylist    []string `json:"denylist"`
		Ban         int      `json:"ban"`
		Large       int      `json:"large"`
		Disband     int      `json:"disband"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, wkdb.MaxSubscriberOperationBytes)
	body, err := BindJSON(&req, c)
	if err != nil {
		c.ResponseError(err)
		return
	}
	if req.ChannelID == "" || len(req.ChannelID) > 1024 || (req.ChannelType != 2 && req.ChannelType != 5) || req.Revision == 0 || req.OperationID == "" || len(req.OperationID) > 128 {
		c.ResponseError(errors.New("group or child identity, revision and operation_id are required"))
		return
	}
	if req.Ban < 0 || req.Ban > 1 || req.Large < 0 || req.Large > 1 || req.Disband < 0 || req.Disband > 1 {
		c.ResponseError(errors.New("channel flags must be zero or one"))
		return
	}
	leader, err := service.Cluster.SlotLeaderOfChannel(req.ChannelID, req.ChannelType)
	if err != nil {
		c.ResponseError(err)
		return
	}
	if leader.Id != options.G.Cluster.NodeId {
		c.ForwardWithBody(fmt.Sprintf("%s%s", leader.ApiServerAddr, c.Request.URL.Path), body)
		return
	}
	ch.submitSubscriberRecovery(c, wkdb.SubscriberOperation{
		ChannelID: req.ChannelID, ChannelType: req.ChannelType, OperationID: req.OperationID,
		Mode: "reconcile", BusinessRevision: req.Revision, UIDs: req.Subscribers, DenyUIDs: req.Denylist,
		SnapshotID: req.SnapshotID, PageIndex: req.PageIndex, PageCount: req.PageCount, RangeStart: req.RangeStart, RangeEnd: req.RangeEnd,
		Channel: &wkdb.ChannelInfo{ChannelId: req.ChannelID, ChannelType: req.ChannelType,
			Ban: req.Ban != 0, Large: req.Large != 0, Disband: req.Disband != 0},
	})
}
