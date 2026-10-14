package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
)

const channelConfigTransitionPath = "/rpc/channel/configTransition/v1"

type channelConfigTransition struct {
	ChannelID   string          `json:"channel_id"`
	ChannelType uint8           `json:"channel_type"`
	Config      rafttype.Config `json:"config"`
	Budget      time.Duration   `json:"budget"`
}

// Role-change callbacks carry the version from which they were computed. Never
// graft a delayed snapshot onto a fresh version returned by GetOrCreate.
func (s *Server) saveChannelConfigTransition(ctx context.Context, id string, typ uint8, next rafttype.Config) (wkdb.ChannelClusterConfig, error) {
	owner, err := s.SlotLeaderIdOfChannel(id, typ)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	if owner != s.opts.ConfigOptions.NodeId {
		budget := 2 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			budget = min(budget, time.Until(deadline))
		}
		if err := ctx.Err(); err != nil {
			return wkdb.EmptyChannelClusterConfig, err
		}
		if budget <= 0 {
			return wkdb.EmptyChannelClusterConfig, context.DeadlineExceeded
		}
		data, err := json.Marshal(channelConfigTransition{id, typ, next, budget})
		if err != nil {
			return wkdb.EmptyChannelClusterConfig, err
		}
		resp, err := s.RequestWithContext(ctx, owner, channelConfigTransitionPath, data)
		if err != nil {
			return wkdb.EmptyChannelClusterConfig, err
		}
		if resp == nil || resp.Status != proto.StatusOK {
			return wkdb.EmptyChannelClusterConfig, ErrConversationReadRetry
		}
		var result wkdb.ChannelClusterConfig
		if err = json.Unmarshal(resp.Body, &result); err != nil {
			return result, err
		}
		if result.ChannelId != id || result.ChannelType != typ || result.ConfVersion <= next.Version {
			return result, ErrConversationReadRetry
		}
		return result, nil
	}
	return s.saveChannelConfigTransitionLocal(ctx, id, typ, next)
}

func (s *Server) saveChannelConfigTransitionLocal(ctx context.Context, id string, typ uint8, next rafttype.Config) (wkdb.ChannelClusterConfig, error) {
	current, err := s.loadConversationConfigLocal(ctx, id, typ)
	if err != nil {
		return current, err
	}
	if next.Version != current.ConfVersion || next.Term < current.Term {
		return current, raft.ErrConfigVersionStale
	}
	s.updateChannelCfgByConfig(&current, next)
	if !validConversationConfig(current, id, typ) {
		return current, ErrConversationReadRetry
	}
	version, err := s.saveChannelConfig(ctx, current)
	if err != nil {
		return current, err
	}
	current.ConfVersion = version
	return current, nil
}

func (r *rpcServer) handleChannelConfigTransition(c *wkserver.Context) {
	var req channelConfigTransition
	if err := json.Unmarshal(c.Body(), &req); err != nil || req.ChannelID == "" || req.ChannelType == 0 || req.Budget <= 0 {
		c.WriteErr(ErrConversationReadRetry)
		return
	}
	ctx, cancel := context.WithTimeout(r.s.cancelCtx, min(req.Budget, 2*time.Second))
	defer cancel()
	cfg, err := r.s.saveChannelConfigTransitionLocal(ctx, req.ChannelID, req.ChannelType, req.Config)
	if err != nil {
		c.WriteErr(err)
		return
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		c.WriteErr(err)
		return
	}
	c.Write(data)
}

// Keep the canonical leader/membership, but fence a recovered term (and any
// vote in it) with a new authority-issued term. Reads keep their exact fence.
// A racing metadata transition wins at admission and is reloaded on retry.
func (s *Server) reconcileLocalChannelConfig(ctx context.Context, cfg wkdb.ChannelClusterConfig) (bool, error) {
	dormant, applyErr := s.channelServer.ReconcileConfig(ctx, cfg)
	state, err := s.channelServer.ReadLeaderState(ctx, cfg.ChannelId, cfg.ChannelType)
	if err != nil {
		return dormant, errors.Join(applyErr, err)
	}
	if state.Exists && state.ConfigVersion <= cfg.ConfVersion &&
		(state.Term > cfg.Term || state.ConfigVersion == cfg.ConfVersion && state.LeaderID != cfg.LeaderId) {
		term := max(state.Term, cfg.Term)
		if term == math.MaxUint32 {
			return false, ErrConversationReadRetry
		}
		_, err := s.saveChannelConfigTransition(ctx, cfg.ChannelId, cfg.ChannelType, rafttype.Config{
			Version: cfg.ConfVersion, Term: term + 1, Leader: cfg.LeaderId,
			Replicas: cfg.Replicas, Learners: cfg.Learners, MigrateFrom: cfg.MigrateFrom, MigrateTo: cfg.MigrateTo,
		})
		if err != nil {
			return false, err
		}
		// The authoritative post-write hint (or next scan) applies the new version.
		return false, ErrConversationReadRetry
	}
	return dormant, applyErr
}
