// Package channeltest exposes the channel raft storage to tests outside the
// channel package, so its production AppendLogs, GetLogs and TruncateLogTo run
// unchanged. It holds no logic of its own.
package channeltest

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

// NewStorage returns the production channel raft storage over db.
func NewStorage(db wkdb.DB) raftgroup.IStorage {
	return channel.NewStorageForTest(db)
}
