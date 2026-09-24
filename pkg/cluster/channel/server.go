package channel

import (
	"context"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/fasthash"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/ringlock"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

// ReadLeaderState observes an existing channel without waking or creating it.
func (s *Server) ReadLeaderState(ctx context.Context, channelID string, channelType uint8) (raftgroup.ReadState, error) {
	key := wkutil.ChannelToKey(channelID, channelType)
	return s.getRaftGroup(key).ReadLeaderState(ctx, key)
}

type Server struct {
	raftGroups []*raftgroup.RaftGroup
	opts       *Options
	storage    *storage
	wklog.Log

	// 正在唤醒的频道
	wake struct {
		sync.RWMutex
		channels map[string]bool
	}

	wakeLeaderLock *ringlock.RingLock
}

func NewServer(opts *Options) *Server {
	s := &Server{
		opts:           opts,
		Log:            wklog.NewWKLog("channel.Server"),
		wakeLeaderLock: ringlock.NewRingLock(1024),
	}
	s.storage = newStorage(opts.DB, s)
	for i := 0; i < opts.GroupCount; i++ {
		rg := raftgroup.New(
			raftgroup.NewOptions(
				raftgroup.WithLogPrefix("channel"),
				raftgroup.WithNotNeedApplied(false),
				raftgroup.WithTransport(opts.Transport),
				raftgroup.WithStorage(s.storage),
				raftgroup.WithEvent(s)),
		)
		s.raftGroups = append(s.raftGroups, rg)
	}
	s.wake.channels = make(map[string]bool)

	return s
}

func (s *Server) Start() error {

	for _, rg := range s.raftGroups {
		err := rg.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Stop() {
	for _, rg := range s.raftGroups {
		rg.Stop()
	}
}

// 唤醒频道领导
func (s *Server) WakeLeaderIfNeed(clusterConfig wkdb.ChannelClusterConfig) error {

	s.wakeLeaderLock.Lock(clusterConfig.ChannelId)
	defer s.wakeLeaderLock.Unlock(clusterConfig.ChannelId)

	channelKey := wkutil.ChannelToKey(clusterConfig.ChannelId, clusterConfig.ChannelType)
	rg := s.getRaftGroup(channelKey)

	raft := rg.GetRaft(channelKey)
	if raft != nil {
		ch := raft.(*Channel)
		return ch.switchConfig(channelConfigToRaftConfig(s.opts.NodeId, clusterConfig))
	}

	if clusterConfig.LeaderId != s.opts.NodeId {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.createAndApplyConfig(ctx, clusterConfig)
}

func (s *Server) WakeFollowerfNeed(channelId string, channelType uint8) error {
	clusterConfig, err := s.opts.Cluster.GetOrCreateChannelClusterConfigFromSlotLeader(channelId, channelType)
	if err != nil {
		return err
	}
	s.wakeLeaderLock.Lock(channelId)
	defer s.wakeLeaderLock.Unlock(channelId)
	isReplica := false
	for _, nodeId := range clusterConfig.Replicas {
		if nodeId == s.opts.NodeId {
			isReplica = true
			break
		}
	}
	if !isReplica {
		for _, nodeId := range clusterConfig.Learners {
			if nodeId == s.opts.NodeId {
				isReplica = true
				break
			}
		}
	}

	channelKey := wkutil.ChannelToKey(clusterConfig.ChannelId, clusterConfig.ChannelType)
	rg := s.getRaftGroup(channelKey)
	if isReplica {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = s.createAndApplyConfig(ctx, clusterConfig)
		if err != nil {
			return err
		}

		// 立马同步
		rg.AddEvent(channelKey, rafttype.Event{
			Type: rafttype.NotifySync,
		})
		rg.Advance()
	}
	return nil
}

// 异步唤醒频道副本
func (s *Server) wakeFollowerIfNeedAsync(channelId string, channelType uint8) {

	s.wake.Lock()
	channelKey := wkutil.ChannelToKey(channelId, channelType)
	if ok := s.wake.channels[channelKey]; ok {
		s.wake.Unlock()
		return
	}
	s.wake.channels[channelKey] = true
	s.wake.Unlock()

	go func() {
		err := s.WakeFollowerfNeed(channelId, channelType)
		if err != nil {
			s.Error("wake channel failed", zap.Error(err))
		}
		s.wake.Lock()
		delete(s.wake.channels, channelKey)
		s.wake.Unlock()
	}()
}

func (s *Server) AddEvent(channelKey string, e rafttype.Event) {
	// Legacy generic proposals bypass message idempotency and cannot carry the
	// canonical result. Require the versioned channel RPC for forwarded writes.
	if e.Type == rafttype.SendPropose || e.Type == rafttype.Propose {
		return
	}

	// 添加事件到对应的频道
	channelId, channelType := wkutil.ChannelFromlKey(channelKey)
	rg := s.getRaftGroup(channelKey)

	raft := rg.GetRaft(channelKey)

	// 如果领导发过来的ping消息，则需要判断是否需要唤醒副本频道
	if raft == nil && (e.Type == rafttype.Ping || e.Type == rafttype.NotifySync) {
		s.wakeFollowerIfNeedAsync(channelId, channelType)
	} else {
		rg.AddEvent(channelKey, e)
		rg.Advance()
	}

}

func (s *Server) getRaftGroup(channelKey string) *raftgroup.RaftGroup {
	index := int(fasthash.Hash(channelKey) % uint32(s.opts.GroupCount))
	return s.raftGroups[index]
}

func (s *Server) ChannelCount() int {
	var count int

	for _, rg := range s.raftGroups {
		count += rg.GetRaftCount()
	}
	return count
}

func (s *Server) ExistChannel(channelId string, channelType uint8) bool {
	channelKey := wkutil.ChannelToKey(channelId, channelType)
	rg := s.getRaftGroup(channelKey)
	return rg.GetRaft(channelKey) != nil
}

func (s *Server) Channel(channelId string, channelType uint8) *Channel {
	channelKey := wkutil.ChannelToKey(channelId, channelType)
	rg := s.getRaftGroup(channelKey)
	raft := rg.GetRaft(channelKey)
	if raft != nil {
		return raft.(*Channel)
	}
	return nil
}

func (s *Server) RemoveChannel(channelId string, channelType uint8) {
	channelKey := wkutil.ChannelToKey(channelId, channelType)
	rg := s.getRaftGroup(channelKey)
	raft := rg.GetRaft(channelKey)
	if raft != nil {
		rg.RemoveRaft(raft)
	}

}

func (s *Server) LastIndexAndAppendTime(channelId string, channelType uint8) (uint64, uint64, error) {
	channelKey := wkutil.ChannelToKey(channelId, channelType)
	return s.storage.LastIndexAndAppendTime(channelKey)
}

func (s *Server) LastLogIndexAndTerm(channelId string, channelType uint8) (uint32, uint64, error) {
	msg, err := s.storage.getLastMessage(channelId, channelType)
	if err != nil && err != wkdb.ErrNotFound {
		return 0, 0, err
	}

	return uint32(msg.Term), uint64(msg.MessageSeq), nil
}
