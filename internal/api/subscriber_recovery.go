package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func recoveryAddMode(reset int) string {
	if reset == 1 {
		return "reset"
	}
	return "add"
}

func (ch *channel) limitSubscriberRequests(next func(*wkhttp.Context)) func(*wkhttp.Context) {
	return func(c *wkhttp.Context) {
		if options.G.SubscriberRecovery.Enabled || service.Store.DB().SubscriberRecoveryActive() {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, wkdb.MaxSubscriberOperationBytes)
		}
		next(c)
	}
}

func (ch *channel) submitSubscriberRecovery(c *wkhttp.Context, o wkdb.SubscriberOperation) {
	// Stable client IDs also cover lost HTTP responses. Generated IDs are useful
	// for polling, but cannot identify a retry if the response itself was lost.
	headerID := c.GetHeader("Idempotency-Key")
	if headerID != "" && o.OperationID != "" && headerID != o.OperationID {
		c.ResponseError(errors.New("operation_id and Idempotency-Key must match"))
		return
	}
	if o.OperationID == "" {
		o.OperationID = headerID
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), options.G.SubscriberRecovery.Timeout)
	defer cancel()
	r, existing, err := service.Store.ExistingSubscriberOperation(o)
	if err == nil && !existing && ((o.Mode == "add" || o.Mode == "reset") && len(o.UIDs) > 0 || o.Mode == "deny_set" || o.Mode == "deny_remove" || o.Mode == "deny_remove_all") && o.ChannelType != wkproto.ChannelTypeLive {
		seq, err := ch.s.subscriberReadFloor(ctx, o.ChannelID, o.ChannelType)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, map[string]any{"status": 503, "msg": err.Error(), "operation_id": o.OperationID})
			return
		}
		o.ReadToMsgSeq = seq
	}
	if err == nil && !existing {
		r, err = service.Store.SubmitSubscriberOperation(ctx, o)
	}
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, store.ErrInvalidSubscriberOperation) {
			status = http.StatusUnprocessableEntity
		}
		if errors.Is(err, store.ErrSubscriberConflict) {
			status = http.StatusConflict
		}
		c.JSON(status, map[string]any{"status": status, "msg": err.Error(), "data": r})
		return
	}
	if r.State == "rejected" {
		status := http.StatusUnprocessableEntity
		if r.Error == "backlog_full" || r.Error == "restore_set_changed" {
			status = http.StatusTooManyRequests
			c.Header("Retry-After", "1")
		}
		c.JSON(status, map[string]any{"status": status, "msg": r.Error, "data": r})
		return
	}
	completed, err := service.Store.CompleteSubscriberOperation(ctx, r)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, map[string]any{
			"status":       http.StatusServiceUnavailable,
			"msg":          err.Error(),
			"operation_id": r.OperationID,
			"data":         completed,
		})
		return
	}
	if completed.State != "complete" {
		c.JSON(http.StatusServiceUnavailable, map[string]any{
			"status":       http.StatusServiceUnavailable,
			"msg":          "subscriber recovery did not complete",
			"operation_id": completed.OperationID,
			"data":         completed,
		})
		return
	}
	c.ResponseOK()
}

func (ch *channel) subscriberOperationStatus(c *wkhttp.Context) {
	var req channelReq
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	body, err := BindJSON(&req, c)
	if err != nil {
		c.ResponseError(err)
		return
	}
	if err = req.Check(); err != nil {
		c.ResponseError(err)
		return
	}
	if req.OperationID == "" || len(req.OperationID) > 128 {
		c.ResponseError(errors.New("operation_id is required"))
		return
	}
	leader, err := service.Cluster.SlotLeaderOfChannel(req.ChannelId, req.ChannelType)
	if err != nil {
		c.ResponseError(err)
		return
	}
	if leader.Id != options.G.Cluster.NodeId {
		c.ForwardWithBody(fmt.Sprintf("%s%s", leader.ApiServerAddr, c.Request.URL.Path), body)
		return
	}
	receipt, ok, err := service.Store.GetSubscriberReceipt(req.ChannelId, req.ChannelType, req.OperationID)
	if err != nil {
		c.ResponseError(err)
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, map[string]any{"status": 404, "msg": "operation not found; a timed-out submission may still apply"})
		return
	}
	c.ResponseOKWithData(receipt)
}

func (ch *channel) subscriberRecoveryStatus(c *wkhttp.Context) {
	parts, err := service.Store.SubscriberBacklog()
	if err != nil {
		c.ResponseError(err)
		return
	}
	var total uint64
	for _, p := range parts {
		total += p.PendingUnits
	}
	c.ResponseOKWithData(map[string]any{"runtime": service.Store.SubscriberRecoveryStats(), "pending_units": total, "partitions": parts})
}

func (s *Server) subscriberReadFloor(ctx context.Context, ch string, tp uint8) (uint64, error) {
	cfg, err := service.Cluster.LoadOnlyChannelClusterConfig(ch, tp)
	if errors.Is(err, wkdb.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if cfg.LeaderId == 0 {
		return 0, errors.New("message channel leader unavailable")
	}
	if cfg.LeaderId == options.G.Cluster.NodeId {
		return service.Store.GetLastMsgSeq(ch, tp)
	}
	return s.client.SubscriberReadFloor(ctx, cfg.LeaderId, ch, tp)
}

func (s *Server) finalizeSubscriberWork(ctx context.Context, w wkdb.SubscriberWork) error {
	// Tag authority is the SLOT leader (not the message-channel Raft leader).
	// Invalidate from current membership; replaying old add/remove deltas is unsafe.
	for _, ch := range []string{w.Operation.ChannelID, options.G.OrginalConvertCmdChannel(w.Operation.ChannelID)} {
		leader, err := service.Cluster.SlotLeaderIdOfChannel(ch, w.Operation.ChannelType)
		if err != nil {
			return err
		}
		if leader == 0 {
			return errors.New("tag source leader unavailable")
		}
		if leader == options.G.Cluster.NodeId {
			service.InvalidateSubscriberTag(ch, w.Operation.ChannelType)
		} else {
			if err := s.client.InvalidateSubscriberTag(ctx, leader, ch, w.Operation.ChannelType); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
