package channel

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// ReconcileConfig applies metadata already read through the slot's applied
// barrier. Metadata/network I/O must happen before entering this method.
// A normal dormant channel stays dormant; unfinished membership work wakes its
// designated leader so learners can catch up even without another send.
func (s *Server) ReconcileConfig(ctx context.Context, cfg wkdb.ChannelClusterConfig) (dormant bool, err error) {
	if !icluster.ValidChannelReadConfig(cfg, cfg.ChannelId, cfg.ChannelType) || cfg.LeaderId != s.opts.NodeId {
		return false, ErrConfigConflict
	}
	if err := s.wakeLeaderLock.LockContext(ctx, cfg.ChannelId); err != nil {
		return false, err
	}
	defer s.wakeLeaderLock.Unlock(cfg.ChannelId)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	rg := s.getRaftGroup(key)
	r := rg.GetRaft(key)
	if r == nil {
		if len(cfg.Learners) == 0 && cfg.MigrateFrom == 0 && cfg.MigrateTo == 0 {
			return true, nil
		}
		return false, s.createAndApplyConfig(ctx, cfg)
	}
	return false, r.(*Channel).applyConfig(ctx, channelConfigToRaftConfig(s.opts.NodeId, cfg))
}

// Load storage outside the owner, then configure and publish atomically with
// respect to owner operations. A cancelled/failed initialization publishes
// nothing; an existing instance is reused, never replaced or blindly removed.
func (s *Server) createAndApplyConfig(ctx context.Context, cfg wkdb.ChannelClusterConfig) error {
	key := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	rg := s.getRaftGroup(key)
	ch, err := createChannel(cfg, s, rg)
	if err != nil {
		return err
	}
	raftCfg := channelConfigToRaftConfig(s.opts.NodeId, cfg).Clone()
	err = rg.Do(ctx, key, func(existing raftgroup.IRaft) error {
		if existing != nil {
			ch = existing.(*Channel)
		}
		if err := ch.applyConfigOnOwner(raftCfg); err != nil {
			return err
		}
		if existing == nil {
			rg.AddRaft(ch)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return ch.resumeConfig(ctx)
}
