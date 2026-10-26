package store

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/lni/goutils/syncutil"
)

type Store struct {
	opts *Options
	wklog.Log

	wdb wkdb.DB

	channelCfgCh chan *channelCfgReq
	stopper      *syncutil.Stopper
}

func New(opts *Options) *Store {
	if opts.ReadFence == nil {
		if fence, ok := opts.Slot.(icluster.SlotReadFence); ok {
			opts.ReadFence = fence
		}
	}
	s := &Store{
		opts:         opts,
		Log:          wklog.NewWKLog("store"),
		wdb:          opts.DB,
		channelCfgCh: make(chan *channelCfgReq, 2048),
		stopper:      syncutil.NewStopper(),
	}

	return s
}

func (s *Store) NextPrimaryKey() uint64 {
	return s.wdb.NextPrimaryKey()
}

// DB exposes the raw business database. Reads of slot-owned tables must go
// through the fenced Store methods instead; only node-local tables (the plugin
// table) and channel-raft-owned data may be read through it. The allow-list is
// enforced by TestRawDBReadsAreFenced.
func (s *Store) DB() wkdb.DB {
	return s.wdb
}

// GetShardNum 获取数据库分片数量
func (s *Store) GetShardNum() int {
	return s.wdb.GetShardNum()
}

// GetChannelShardIndex 获取频道所在的分片索引
func (s *Store) GetChannelShardIndex(channelId string, channelType uint8) uint32 {
	return s.wdb.GetChannelShardIndex(channelId, channelType)
}

func (s *Store) Start() error {
	for i := 0; i < 50; i++ {
		go s.loopSaveChannelClusterConfig()
	}
	// s.stopper.RunWorker(s.loopSaveChannelClusterConfig)
	return nil
}

func (s *Store) Stop() {
	s.stopper.Stop()
}

type channelCfgReq struct {
	cfg   wkdb.ChannelClusterConfig
	errCh chan error
}
