package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

// saveChannelConfig keeps the existing slot log format, but fences the decision
// at admission. cfg.ConfVersion is the version being replaced (zero for create).
// All production metadata writers use this method on the current slot leader.
func (s *Server) saveChannelConfig(ctx context.Context, cfg wkdb.ChannelClusterConfig) (uint64, error) {
	slotID := s.getSlotId(cfg.ChannelId)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		before, err := s.slotServer.ReadLeaderState(ctx, slotID)
		if err != nil {
			return 0, err
		}
		if before.LeaderID != s.opts.ConfigOptions.NodeId || s.cfgServer.SlotLeaderId(slotID) != s.opts.ConfigOptions.NodeId {
			return 0, ErrConversationReadRetry
		}
		if before.Ready {
			validate := func() error {
				if s.cfgServer.SlotLeaderId(slotID) != s.opts.ConfigOptions.NodeId {
					return ErrConversationReadRetry
				}
				current, err := s.db.GetChannelClusterConfig(cfg.ChannelId, cfg.ChannelType)
				if err != nil && !errors.Is(err, wkdb.ErrNotFound) {
					return err
				}
				if current.ConfVersion != cfg.ConfVersion || cfg.Term < current.Term {
					return raft.ErrConfigVersionStale
				}
				return nil
			}
			// Reject already-stale decisions cheaply. Recheck after the fixed
			// admitted prefix applies, while holding proposal admission.
			if err := validate(); err != nil {
				return 0, err
			}
			data, err := cfg.Marshal()
			if err != nil {
				return 0, err
			}
			data, err = store.EncodeCMDChannelClusterConfigSave(cfg.ChannelId, cfg.ChannelType, data)
			if err != nil {
				return 0, err
			}
			cmd := store.NewCMD(store.CMDChannelClusterConfigSave, data)
			data, err = cmd.Marshal()
			if err != nil {
				return 0, err
			}
			version, err := s.slotServer.ProposeAfterRead(ctx, slotID, before, data, validate)
			if !errors.Is(err, raftgroup.ErrReadStateChanged) {
				return version, err
			}
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Server) saveChannelConfigTimeout(cfg wkdb.ChannelClusterConfig) (uint64, error) {
	ctx, cancel := context.WithTimeout(s.cancelCtx, 5*time.Second)
	defer cancel()
	return s.saveChannelConfig(ctx, cfg)
}
