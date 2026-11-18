package store

import (
	"hash/fnv"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/lni/goutils/syncutil"
)

const (
	channelCfgWorkerShards = 32
	channelCfgShardBufSize = 256
)

type Store struct {
	opts *Options
	wklog.Log

	wdb wkdb.DB

	channelCfgChs [channelCfgWorkerShards]chan *channelCfgReq
	stopper       *syncutil.Stopper
}

func New(opts *Options) *Store {
	s := &Store{
		opts:    opts,
		Log:     wklog.NewWKLog("store"),
		wdb:     opts.DB,
		stopper: syncutil.NewStopper(),
	}
	for i := 0; i < channelCfgWorkerShards; i++ {
		s.channelCfgChs[i] = make(chan *channelCfgReq, channelCfgShardBufSize)
	}

	return s
}

func channelCfgShardIndex(channelId string, channelType uint8) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(channelId))
	_, _ = h.Write([]byte{':', channelType})
	return int(h.Sum64() % channelCfgWorkerShards)
}

func (s *Store) channelCfgChanFor(channelId string, channelType uint8) chan *channelCfgReq {
	return s.channelCfgChs[channelCfgShardIndex(channelId, channelType)]
}

func (s *Store) NextPrimaryKey() uint64 {
	return s.wdb.NextPrimaryKey()
}

func (s *Store) DB() wkdb.DB {
	return s.wdb
}

func (s *Store) HasChannel() bool {
	return s != nil && s.opts != nil && s.opts.Channel != nil
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
	for i := 0; i < channelCfgWorkerShards; i++ {
		ch := s.channelCfgChs[i]
		s.stopper.RunWorker(func() {
			s.loopSaveChannelClusterConfig(ch)
		})
	}
	return nil
}

func (s *Store) Stop() {
	s.stopper.Stop()
}

type channelCfgReq struct {
	cfg   wkdb.ChannelClusterConfig
	errCh chan error
}
