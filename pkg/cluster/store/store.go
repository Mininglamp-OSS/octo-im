package store

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/lni/goutils/syncutil"
)

var ErrLegacySubscriberMutationDisabled = errors.New("legacy subscriber mutation is disabled while subscriber recovery is active")

type Store struct {
	applyLocks      sync.Map // slot ID -> *sync.Mutex; independent slots do not block
	opts            *Options
	recovery        *subscriberRecovery
	recoveryEnabled atomic.Bool
	wklog.Log

	wdb wkdb.DB

	channelCfgCh chan *channelCfgReq
	stopper      *syncutil.Stopper
}

func New(opts *Options) *Store {
	s := &Store{
		opts:         opts,
		Log:          wklog.NewWKLog("store"),
		wdb:          opts.DB,
		channelCfgCh: make(chan *channelCfgReq, 2048),
		stopper:      syncutil.NewStopper(),
	}
	s.recoveryEnabled.Store(opts.SubscriberRecoveryEnabled)

	return s
}

func (s *Store) rejectLegacySubscriberMutation(channelType uint8) error {
	// Subscriber recovery intentionally excludes person channels. Preserve
	// their existing denylist path while fencing every managed channel.
	if channelType == wkproto.ChannelTypePerson {
		return nil
	}
	if s.recoveryEnabled.Load() || (s.wdb != nil && s.wdb.SubscriberRecoveryActive()) {
		return ErrLegacySubscriberMutationDisabled
	}
	return nil
}

func (s *Store) NextPrimaryKey() uint64 {
	return s.wdb.NextPrimaryKey()
}

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
	if s.recovery != nil {
		s.recovery.cancel()
		s.recovery.wg.Wait()
	}
	s.stopper.Stop()
}

type channelCfgReq struct {
	cfg   wkdb.ChannelClusterConfig
	errCh chan error
}
