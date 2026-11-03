package cluster

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
)

func (s *Server) AdmitLocalChannelMessage(ctx context.Context, id string, typ uint8, messageID uint64, size int64, enqueue func(done func())) error {
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader(id, typ)
	if err != nil {
		return err
	}
	if cfg.LeaderId != s.opts.ConfigOptions.NodeId {
		return channel.ErrSendUnavailable
	}
	if err := s.channelServer.WakeLeaderIfNeed(cfg); err != nil {
		return err
	}
	return s.channelServer.AdmitMessage(ctx, id, typ, messageID, size, enqueue)
}
