package icluster

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// ValidChannelReadConfig is shared by read routing and serving-side fencing.
func ValidChannelReadConfig(cfg wkdb.ChannelClusterConfig, id string, typ uint8) bool {
	return id != "" && typ != 0 && cfg.ChannelId == id && cfg.ChannelType == typ && cfg.LeaderId != 0 && cfg.Term != 0 &&
		cfg.Status == wkdb.ChannelClusterStatusNormal && wkutil.ArrayContainsUint64(cfg.Replicas, cfg.LeaderId) && !wkutil.ArrayContainsUint64(cfg.Learners, cfg.LeaderId)
}
