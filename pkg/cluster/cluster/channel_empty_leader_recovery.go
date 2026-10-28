package cluster

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// A rejoined ID can still be the metadata leader after losing its local disk.
// Before activating an empty leader, ask the existing voters for their durable
// logs and move leadership to a surviving log through the slot owner's CAS.
// Nonempty and already active leaders retain their existing startup path.
func (s *Server) recoverEmptyChannelLeader(ctx context.Context, cfg wkdb.ChannelClusterConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cfg.LeaderId != s.opts.ConfigOptions.NodeId || len(cfg.Replicas) <= 1 {
		return nil
	}
	lastIndex, _, err := s.channelServer.LastLogIndexAndTerm(cfg.ChannelId, cfg.ChannelType)
	if err != nil || lastIndex != 0 {
		return err
	}
	voters := make([]uint64, 0, len(cfg.Replicas))
	for _, id := range cfg.Replicas {
		if id != 0 && !wkutil.ArrayContainsUint64(cfg.Learners, id) && !wkutil.ArrayContainsUint64(voters, id) {
			voters = append(voters, id)
		}
	}
	infos, err := s.requestChannelLastLogInfos(ctx, voters, cfg.ChannelId, cfg.ChannelType)
	if err != nil {
		return err
	}
	// A majority can have lost its disks together. Its empty logs cannot prove
	// that the absent voter does not hold the only surviving history.
	for _, id := range voters {
		if infos[id] == nil {
			return ErrConversationReadRetry
		}
	}
	next := cfg.Clone()
	if _, err := electChannelLeader(&next, infos); err != nil {
		return err
	}
	if infos[next.LeaderId].LogIndex == 0 {
		// Every voter confirms an actually unused channel. Keep its chosen leader
		// and leave it dormant until a real write needs it.
		return nil
	}
	_, err = s.saveChannelConfigTransition(ctx, cfg.ChannelId, cfg.ChannelType, rafttype.Config{
		Version: cfg.ConfVersion, Term: next.Term, Leader: next.LeaderId,
		Replicas: cfg.Replicas, Learners: cfg.Learners, MigrateFrom: cfg.MigrateFrom, MigrateTo: cfg.MigrateTo,
	})
	if err != nil {
		return err
	}
	// The caller reloads the new authority; the empty old leader must not start
	// accepting writes against the configuration it just replaced.
	return raft.ErrConfigVersionStale
}
