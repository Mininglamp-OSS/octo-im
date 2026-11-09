package channel

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// ReconcileConfig applies metadata already read through the slot's applied
// barrier. Recovery I/O finishes before taking channel locks or entering owners.
// A normal dormant channel stays dormant; unfinished membership work wakes its
// designated leader so learners can catch up even without another send.
func (s *Server) ReconcileConfig(ctx context.Context, cfg wkdb.ChannelClusterConfig) (dormant bool, err error) {
	if !icluster.ValidChannelReadConfig(cfg, cfg.ChannelId, cfg.ChannelType) || cfg.LeaderId != s.opts.NodeId {
		return false, ErrConfigConflict
	}
	wakeForRecovery, err := s.prepareLeaderStart(ctx, cfg)
	if err != nil {
		return false, err
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
		if !wakeForRecovery && len(cfg.Learners) == 0 && cfg.MigrateFrom == 0 && cfg.MigrateTo == 0 {
			return true, nil
		}
		return false, s.createAndApplyConfig(ctx, cfg)
	}
	return false, r.(*Channel).applyConfig(ctx, channelConfigToRaftConfig(s.opts.NodeId, cfg))
}

// Callers hold wakeLeaderLock to serialize creation. Reuse an active instance
// on the owner; otherwise drain retired local I/O before reading its durable
// state outside the owner and publishing a replacement atomically.
func (s *Server) createAndApplyConfig(ctx context.Context, cfg wkdb.ChannelClusterConfig) error {
	key := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	rg := s.getRaftGroup(key)
	raftCfg := channelConfigToRaftConfig(s.opts.NodeId, cfg).Clone()
	var ch *Channel
	err := rg.Do(ctx, key, func(existing raftgroup.IRaft) error {
		if existing == nil {
			return nil
		}
		ch = existing.(*Channel)
		return ch.applyConfigOnOwner(raftCfg)
	})
	if err != nil {
		return err
	}
	if ch != nil {
		return ch.resumeConfig(ctx)
	}
	// The retired instance cannot submit more local I/O, and the wake lock
	// keeps another creator from publishing while this drain is in progress.
	if err := rg.WaitStorage(ctx, key); err != nil {
		return err
	}
	ch, err = createChannel(cfg, s, rg)
	if err != nil {
		return err
	}
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
