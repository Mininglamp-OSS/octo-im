package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"go.uber.org/zap"
)

const channelConfigReconcilePath = "/rpc/channel/reconcileConfig/v1"

type configReconcileRequest struct {
	ChannelID     string        `json:"channel_id"`
	ChannelType   uint8         `json:"channel_type"`
	ConfigVersion uint64        `json:"config_version"`
	Budget        time.Duration `json:"budget"`
}

type configReconcileResponse struct {
	Version       int    `json:"version"`
	ChannelID     string `json:"channel_id"`
	ChannelType   uint8  `json:"channel_type"`
	ConfigVersion uint64 `json:"config_version"`
	Outcome       string `json:"outcome"` // applied, dormant, superseded, retry
}

func (s *Server) requestChannelConfigReconcile(ctx context.Context, cfg wkdb.ChannelClusterConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	budget := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline))
	}
	if budget <= 0 {
		return context.DeadlineExceeded
	}
	data, err := json.Marshal(configReconcileRequest{cfg.ChannelId, cfg.ChannelType, cfg.ConfVersion, budget})
	if err != nil {
		return err
	}
	resp, err := s.RequestWithContext(ctx, cfg.LeaderId, channelConfigReconcilePath, data)
	if err != nil {
		return err
	}
	if resp == nil || resp.Status != proto.StatusOK {
		return fmt.Errorf("config reconcile peer %d unavailable", cfg.LeaderId)
	}
	var result configReconcileResponse
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return err
	}
	if result.Version != 1 || result.ChannelID != cfg.ChannelId || result.ChannelType != cfg.ChannelType ||
		result.ConfigVersion != cfg.ConfVersion || (result.Outcome != "applied" && result.Outcome != "dormant") {
		return ErrConversationReadRetry
	}
	return ctx.Err()
}

func (r *rpcServer) handleChannelConfigReconcile(c *wkserver.Context) {
	var req configReconcileRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil || req.ChannelID == "" || req.ChannelType == 0 || req.Budget <= 0 {
		c.WriteErr(ErrConversationReadRetry)
		return
	}
	ctx, cancel := context.WithTimeout(r.s.cancelCtx, min(req.Budget, 2*time.Second))
	defer cancel()
	result := configReconcileResponse{Version: 1, ChannelID: req.ChannelID, ChannelType: req.ChannelType, Outcome: "retry"}
	// Treat the request as a hint, never as an authoritative config. This I/O
	// precedes channel locks and owner operations, avoiding apply/RPC deadlocks.
	cfg, err := r.s.loadConversationConfig(ctx, req.ChannelID, req.ChannelType)
	if err == nil {
		result.ConfigVersion = cfg.ConfVersion
		if cfg.ConfVersion != req.ConfigVersion || cfg.LeaderId != r.s.opts.ConfigOptions.NodeId {
			result.Outcome = "superseded"
		} else if dormant, applyErr := r.s.reconcileLocalChannelConfig(ctx, cfg); applyErr == nil {
			result.Outcome = "applied"
			if dormant {
				result.Outcome = "dormant"
			}
		} else {
			r.s.Debug("channel config reconciliation rejected", zap.String("channelId", req.ChannelID),
				zap.Uint8("channelType", req.ChannelType), zap.Uint64("version", cfg.ConfVersion), zap.Error(applyErr))
		}
	}
	data, err := json.Marshal(result)
	if err != nil {
		c.WriteErr(err)
		return
	}
	c.Write(data)
}
