package channel

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

// NewStorageForTest returns the channel raft storage over db, without a server.
// Tests reach it through channeltest.NewStorage; SaveConfig is not usable on it.
func NewStorageForTest(db wkdb.DB) raftgroup.IStorage {
	return newStorage(db, nil)
}
