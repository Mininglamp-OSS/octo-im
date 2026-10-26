package cluster

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"go.opentelemetry.io/otel/metric"
)

// slotLogInfoExtInvalid counts slot election responses whose divergence
// extension was present but malformed.
var slotLogInfoExtInvalid atomic.Int64

var registerElectionMetricsOnce sync.Once

func registerElectionMetrics() {
	registerElectionMetricsOnce.Do(func() {
		invalid := trace.NewInt64ObservableCounter("slot_loginfo_ext_invalid_total")
		trace.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
			obs.ObserveInt64(invalid, slotLogInfoExtInvalid.Load())
			return nil
		}, invalid)
	})
}

// fencedChannelClusterConfig is the only way this package reads a channel
// cluster config from its local database. The row is owned by the slot of
// the channel id; while the local replica of that slot is quarantined the
// read fails with ErrSlotDiverged and never yields a row or not-found.
func (s *Server) fencedChannelClusterConfig(channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	return icluster.FencedRead(s.slotServer, s.getSlotId(channelId), func() (wkdb.ChannelClusterConfig, error) {
		return s.db.GetChannelClusterConfig(channelId, channelType)
	})
}

// checkChannelSlotReadable fences a raw read of rows owned by the slot of
// channelId. Use fencedRead when a value is returned.
func (s *Server) checkChannelSlotReadable(channelId string) error {
	return s.slotServer.CheckSlotReadable(s.getSlotId(channelId))
}
