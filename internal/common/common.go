package common

import (
	"fmt"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/WuKongIM/WuKongIM/internal/errors"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

// CommonService 通用服务
type Service struct {
	client *ingress.Client
	wklog.Log
	timingWheel     *timingwheel.TimingWheel
	boundNodeId     uint64
	boundCluster    icluster.ICluster
	boundTagManager service.ITagManager
}

func NewService() *Service {

	return &Service{
		client:      ingress.NewClient(),
		Log:         wklog.NewWKLog("common.Service"),
		timingWheel: timingwheel.NewTimingWheel(options.G.TimingWheelTick, options.G.TimingWheelSize),
	}
}

func (s *Service) BindDeps(nodeId uint64, cl icluster.ICluster, tm service.ITagManager, cli *ingress.Client) {
	if s == nil {
		return
	}
	s.boundNodeId = nodeId
	s.boundCluster = cl
	s.boundTagManager = tm
	if cli != nil {
		s.client = cli
	}
}

func (s *Service) localNodeId() uint64 {
	if s != nil && s.boundNodeId != 0 {
		return s.boundNodeId
	}
	if options.G != nil {
		return options.G.Cluster.NodeId
	}
	return 0
}

func (s *Service) isLocalNode(nodeId uint64) bool {
	if s != nil && s.boundNodeId != 0 {
		return s.boundNodeId == nodeId
	}
	return options.G != nil && options.G.IsLocalNode(nodeId)
}

func (s *Service) cluster() icluster.ICluster {
	if s != nil && s.boundCluster != nil {
		return s.boundCluster
	}
	return service.Cluster
}

func (s *Service) tagManager() service.ITagManager {
	if s != nil && s.boundTagManager != nil {
		return s.boundTagManager
	}
	return service.TagManager
}

func (s *Service) Start() error {
	s.timingWheel.Start()
	return nil
}

func (s *Service) Stop() {
	s.timingWheel.Stop()
}

// Schedule 延迟任务
func (s *Service) Schedule(interval time.Duration, f func()) *timingwheel.Timer {
	return s.timingWheel.ScheduleFunc(&everyScheduler{
		Interval: interval,
	}, f)
}

func (s *Service) AfterFunc(d time.Duration, f func()) {
	s.timingWheel.AfterFunc(d, f)
}

type everyScheduler struct {
	Interval time.Duration
}

func (s *everyScheduler) Next(prev time.Time) time.Time {
	return prev.Add(s.Interval)
}

// 获取或请求tag
// 先根据tagKey在本地取，如果没有，则去频道领导者节点取
// 取到后设置在本地缓存中
// targetNodeId 目标节点的uids，如果为0，表示获取所有节点的uids
func (s *Service) GetOrRequestAndMakeTag(fakeChannelId string, channelType uint8, tagKey string, targetNodeId uint64) (*types.Tag, error) {

	// realFakeChannelId := fakeChannelId
	// if options.G.IsCmdChannel(fakeChannelId) {
	// 	realFakeChannelId = options.G.CmdChannelConvertOrginalChannel(fakeChannelId)
	// }

	tm := s.tagManager()
	var tag *types.Tag
	if tagKey == "" {
		tagKey = tm.GetChannelTag(fakeChannelId, channelType)
	}
	if tagKey != "" {
		tag = tm.Get(tagKey)
	}
	if tag != nil {
		return tag, nil
	}
	if channelType == wkproto.ChannelTypePerson {
		return s.getOrMakePersonTag(fakeChannelId, tagKey)
	}

	slotLeaderId, err := s.cluster().SlotLeaderIdOfChannel(fakeChannelId, channelType)
	if err != nil {
		wklog.Error("GetOrRequestTag: getLeaderOfChannel failed", zap.Error(err), zap.String("channelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	if slotLeaderId == 0 {
		wklog.Warn("GetOrRequestTag: slotLeader is 0", zap.String("channelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, errors.ChannelNotExist(fakeChannelId)
	}

	// tagKey在频道的领导节点是一定存在的，
	// 如果不存在可能就是失效了，这里直接忽略,只能等下条消息触发重构tag
	if s.isLocalNode(slotLeaderId) {
		wklog.Warn("tag not exist in leader node", zap.String("tagKey", tagKey), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, errors.TagNotExist(tagKey)
	}

	// 去领导节点请求
	tagResp, err := s.client.RequestTag(slotLeaderId, &ingress.TagReq{
		TagKey:      tagKey,
		ChannelId:   fakeChannelId,
		ChannelType: channelType,
		NodeId:      targetNodeId,
	})
	if err != nil {
		s.Error("GetOrRequestTag: get tag failed", zap.Error(err), zap.Uint64("slotLeaderId", slotLeaderId), zap.String("channelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, err
	}

	tag, err = tm.MakeTagWithTagKey(tagKey, tagResp.Uids)
	if err != nil {
		s.Error("GetOrRequestTag: make tag failed", zap.Error(err))
		return nil, err
	}
	tm.SetChannelTag(fakeChannelId, channelType, tagKey)
	return tag, nil
}

func (s *Service) GetOrRequestAndMakeTagWithLocal(fakeChannelId string, channelType uint8, tagKey string) (*types.Tag, error) {
	return s.GetOrRequestAndMakeTag(fakeChannelId, channelType, tagKey, s.localNodeId())
}

// 获取个人频道的投递tag
func (s *Service) getOrMakePersonTag(fakeChannelId string, tagKey string) (*types.Tag, error) {

	realFakeChannelId := fakeChannelId
	if options.G != nil && options.G.IsCmdChannel(fakeChannelId) {
		realFakeChannelId = options.G.CmdChannelConvertOrginalChannel(fakeChannelId)
	}
	// 处理普通假个人频道
	u1, u2 := options.GetFromUIDAndToUIDWith(realFakeChannelId)
	subscribers := []string{u1, u2}
	tm := s.tagManager()
	var (
		tag *types.Tag
		err error
	)
	if tagKey != "" {
		tag, err = tm.MakeTagWithTagKey(tagKey, subscribers)
	} else {
		tag, err = tm.MakeTag(subscribers)
	}
	if err != nil {
		return nil, err
	}
	tm.SetChannelTag(fakeChannelId, wkproto.ChannelTypePerson, tag.Key)
	return tag, nil
}

// 检查连接的真实性 并获取真实连接
func CheckConnValidAndGetRealConn(conn *eventbus.Conn) (wknet.Conn, error) {
	// 获取到真实连接
	realConn := service.ConnManager.GetConn(conn.ConnId)
	if realConn == nil {
		return nil, nil
	}

	// 验证连接的真实性, 双向验证，验证连接的fd和connId是否一致
	// 1. 通过fd获取连接
	connfd := realConn.Fd().Fd()
	realConnByFd := service.ConnManager.GetConnByFd(connfd)
	if realConnByFd == nil {
		// connId与fd不一致，说明连接已经关闭
		return nil, fmt.Errorf("connId not match, connId: %d, fd: %d", conn.ConnId, connfd)
	}

	ctx, ctxByFd := realConn.Context(), realConnByFd.Context()

	if ctx != ctxByFd {
		// connId与fd不一致，说明连接已经关闭
		return nil, fmt.Errorf("context not match, connId: %d, fd: %d", conn.ConnId, connfd)
	}

	// 2. 验证连接的fd和connId是否一致
	if ctxByFd != nil {
		eventConn, ok := ctxByFd.(*eventbus.Conn)
		if ok && eventConn != nil {
			matches := eventConn.SameSession(conn)
			if conn.IsLegacySession() {
				matches = eventConn.LegacyMatches(conn)
			}
			if !matches {
				return nil, fmt.Errorf("session identity does not match, uid: %s, connId: %d", conn.Uid, conn.ConnId)
			}
		}
	}
	return realConn, nil
}
