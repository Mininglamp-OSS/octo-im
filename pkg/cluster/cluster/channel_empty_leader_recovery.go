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
// A surviving dormant leader also needs to wake when rebuilt replicas report
// shorter logs. Healthy dormant channels and already active leaders stay idle.
func (s *Server) recoverEmptyChannelLeader(ctx context.Context, cfg wkdb.ChannelClusterConfig) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if cfg.LeaderId != s.opts.ConfigOptions.NodeId || len(cfg.Replicas) <= 1 {
		return false, nil
	}
	_, lastLogIndex, err := s.channelServer.LastLogIndexAndTerm(cfg.ChannelId, cfg.ChannelType)
	if err != nil {
		return false, err
	}
	voters := make([]uint64, 0, len(cfg.Replicas))
	for _, id := range cfg.Replicas {
		if id != 0 && !wkutil.ArrayContainsUint64(cfg.Learners, id) && !wkutil.ArrayContainsUint64(voters, id) {
			voters = append(voters, id)
		}
	}
	infos, err := s.requestChannelLastLogInfos(ctx, voters, cfg.ChannelId, cfg.ChannelType)
	if err != nil {
		return false, err
	}
	if lastLogIndex > 0 {
		// Existing data-bearing leadership is authoritative. A missing peer
		// must not block its normal activation; a known lagging peer needs a
		// live Raft leader so ordinary replication can restore its history.
		for _, id := range voters {
			if info := infos[id]; info != nil && info.LogIndex < lastLogIndex {
				return true, nil
			}
		}
		return false, nil
	}
	// A majority can have lost its disks together. Its empty logs cannot prove
	// that the absent voter does not hold the only surviving history.
	for _, id := range voters {
		if infos[id] == nil {
			return false, ErrConversationReadRetry
		}
	}
	next := cfg.Clone()
	if _, err := electChannelLeader(&next, infos); err != nil {
		return false, err
	}
	if infos[next.LeaderId].LogIndex == 0 {
		// Every voter confirms an actually unused channel. Keep its chosen leader
		// and leave it dormant until a real write needs it.
		return false, nil
	}
	_, err = s.saveChannelConfigTransition(ctx, cfg.ChannelId, cfg.ChannelType, rafttype.Config{
		Version: cfg.ConfVersion, Term: next.Term, Leader: next.LeaderId,
		Replicas: cfg.Replicas, Learners: cfg.Learners, MigrateFrom: cfg.MigrateFrom, MigrateTo: cfg.MigrateTo,
	})
	if err != nil {
		return false, err
	}
	// The caller reloads the new authority; the empty old leader must not start
	// accepting writes against the configuration it just replaced.
	return false, raft.ErrConfigVersionStale
}
