package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// GetOrCreateChannelClusterConfig 获取或创建频道分布式配置
func (s *Server) GetOrCreateChannelClusterConfigFromSlotLeader(channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {

	s.channelKeyLock.Lock(channelId)
	defer s.channelKeyLock.Unlock(channelId)

	// 获取频道槽领导
	slotLeaderId, err := s.SlotLeaderIdOfChannel(channelId, channelType)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}

	if slotLeaderId == 0 {
		return wkdb.EmptyChannelClusterConfig, fmt.Errorf("slot[%d] leader not found", s.getSlotId(channelId))
	}

	// 如果当前节点是频道槽领导，则直接返回频道分布式配置
	if s.opts.ConfigOptions.NodeId == slotLeaderId {
		return s.getOrCreateChannelClusterConfigFromLocal(channelId, channelType)
	}

	// 向频道槽领导请求频道分布式配置
	return s.rpcClient.RequestGetOrCreateChannelClusterConfig(slotLeaderId, channelId, channelType)
}

func (s *Server) SlotLeaderOfChannel(channelId string, channelType uint8) (*types.Node, error) {
	slotId := s.getSlotId(channelId)
	slotLeaderId := s.cfgServer.SlotLeaderId(slotId)
	if slotLeaderId == 0 {
		return nil, fmt.Errorf("slot[%d] leader not found", slotId)
	}
	node := s.cfgServer.Node(slotLeaderId)
	if node == nil {
		return nil, errors.New("slot leader node not found")
	}
	return node, nil
}

func (s *Server) SlotLeaderIdOfChannel(channelId string, channelType uint8) (nodeID uint64, err error) {
	slotId := s.getSlotId(channelId)
	slotLeaderId := s.cfgServer.SlotLeaderId(slotId)
	if slotLeaderId == 0 {
		return 0, fmt.Errorf("slot[%d] leader not found", slotId)
	}
	return slotLeaderId, nil
}

func (s *Server) LoadOnlyChannelClusterConfig(channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	slotId := s.getSlotId(channelId)
	slotLeaderId := s.cfgServer.SlotLeaderId(slotId)
	if slotLeaderId == 0 {
		return wkdb.EmptyChannelClusterConfig, fmt.Errorf("slot[%d] leader not found", slotId)
	}
	if s.opts.ConfigOptions.NodeId == slotLeaderId {
		if err := s.waitLocalSlotApplied(s.cancelCtx, slotId); err != nil {
			return wkdb.EmptyChannelClusterConfig, err
		}
		return s.db.GetChannelClusterConfig(channelId, channelType)
	}

	return s.rpcClient.RequestGetChannelClusterConfig(slotLeaderId, channelId, channelType)
}

func (s *Server) maxSlotApplyWait() time.Duration {
	if s.opts != nil && s.opts.MaxSlotApplyWait > 0 {
		return s.opts.MaxSlotApplyWait
	}
	return 500 * time.Millisecond
}

func (s *Server) waitLocalSlotApplied(ctx context.Context, slotId uint32) error {
	if s.slotServer == nil {
		return nil
	}
	st := s.slotServer.GetSlotRaft(slotId)
	if st == nil {
		return fmt.Errorf("slot[%d] local raft not found", slotId)
	}
	target := st.CommittedIndex()
	if st.AppliedIndex() >= target {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, s.maxSlotApplyWait())
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for st.AppliedIndex() < target {
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("slot[%d] local apply lagging: applied=%d committed=%d: %w", slotId, st.AppliedIndex(), target, waitCtx.Err())
		case <-ticker.C:
		}
		if s.cfgServer.SlotLeaderId(slotId) != s.opts.ConfigOptions.NodeId {
			return fmt.Errorf("slot[%d] leadership changed while waiting for apply", slotId)
		}
	}
	return nil
}

func (s *Server) LeaderOfChannel(channelId string, channelType uint8) (*types.Node, error) {
	clusterConfig, err := s.GetOrCreateChannelClusterConfigFromSlotLeader(channelId, channelType)
	if err != nil {
		return nil, err
	}
	node := s.cfgServer.Node(clusterConfig.LeaderId)
	if node == nil {
		return nil, ErrChannelClusterConfigNotFound
	}
	return node, nil
}

func (s *Server) LeaderIdOfChannel(channelId string, channelType uint8) (uint64, error) {
	clusterConfig, err := s.GetOrCreateChannelClusterConfigFromSlotLeader(channelId, channelType)
	if err != nil {
		return 0, err
	}
	return clusterConfig.LeaderId, nil
}

func (s *Server) LeaderOfChannelForRead(channelId string, channelType uint8) (*types.Node, error) {
	clusterConfig, err := s.LoadOnlyChannelClusterConfig(channelId, channelType)
	if err != nil {
		return nil, err
	}
	node := s.cfgServer.Node(clusterConfig.LeaderId)
	if node == nil {
		return nil, ErrChannelClusterConfigNotFound
	}
	return node, nil
}

func (s *Server) NodeInfoById(nodeId uint64) *types.Node {
	return s.cfgServer.Node(nodeId)
}

func (s *Server) GetSlotId(v string) uint32 {
	return s.getSlotId(v)
}

func (s *Server) SlotLeaderId(slotId uint32) uint64 {
	return s.cfgServer.SlotLeaderId(slotId)
}

func (s *Server) NodeIsOnline(nodeId uint64) bool {
	return s.cfgServer.NodeIsOnline(nodeId)
}

func (s *Server) SlotLeaderNodeInfo(slotId uint32) *types.Node {
	slotLeaderId := s.slotServer.SlotLeaderId(slotId)
	return s.cfgServer.Node(slotLeaderId)
}

func (s *Server) NodeVersion() uint64 {
	return s.cfgServer.GetClusterConfig().Version
}

func (s *Server) Nodes() []*types.Node {
	return s.cfgServer.Nodes()
}

func (s *Server) Send(nodeId uint64, msg *proto.Message) error {
	node := s.nodeManager.node(nodeId)
	if node == nil {
		return ErrNodeNotFound
	}
	return node.Send(msg)
}

func (s *Server) RequestWithContext(ctx context.Context, toNodeId uint64, path string, body []byte) (*proto.Response, error) {
	node := s.nodeManager.node(toNodeId)
	if node == nil {
		return nil, ErrNodeNotFound
	}
	return node.RequestWithContext(ctx, path, body)
}

func (s *Server) Route(path string, handler wkserver.Handler) {
	s.netServer.Route(path, handler)
}

func (s *Server) LeaderId() uint64 {
	return s.cfgServer.LeaderId()
}

// OnMessage 注册消息处理函数
func (s *Server) OnMessage(f func(fromNodeId uint64, msg *proto.Message)) {
	s.onMessageFnc = f
}

func (s *Server) MustWaitAllSlotsReady(timeout time.Duration) {
	s.slotServer.MustWaitAllSlotsReady(timeout)
	_ = s.MustWaitClusterReady(timeout)
}

// CheckLocalProcessHealth 检查本节点进程是否存活（用于 /healthz liveness 探针）
func (s *Server) CheckLocalProcessHealth() error {
	if s == nil || s.cancelCtx == nil {
		return errors.New("cluster server not initialized")
	}
	if err := s.cancelCtx.Err(); err != nil {
		return fmt.Errorf("cluster server stopped: %w", err)
	}
	return nil
}

func (s *Server) checkLocalStorageHealth() error {
	if s.opts != nil && s.opts.DataDir != "" {
		wkShardNum := 1
		slotShardNum := 1
		if s.opts.DB.WKDbShardNum > 0 {
			wkShardNum = int(s.opts.DB.WKDbShardNum)
		}
		if s.opts.DB.SlotShardNum > 0 {
			slotShardNum = int(s.opts.DB.SlotShardNum)
		}
		dirs := make([]string, 0, 8+wkShardNum+slotShardNum)
		dirs = append(dirs,
			s.opts.DataDir,
			filepath.Join(s.opts.DataDir, "db"),
			filepath.Join(s.opts.DataDir, "db", "wukongimdb"),
			filepath.Join(s.opts.DataDir, "cluster"),
			filepath.Join(s.opts.DataDir, "cluster", "logdb"),
		)
		for i := 0; i < wkShardNum; i++ {
			dirs = append(dirs, filepath.Join(s.opts.DataDir, "db", "wukongimdb", fmt.Sprintf("shard%03d", i)))
		}
		for i := 0; i < slotShardNum; i++ {
			dirs = append(dirs, filepath.Join(s.opts.DataDir, "cluster", "logdb", fmt.Sprintf("shard%03d", i)))
		}
		probeDirs := []string{
			s.opts.DataDir,
			filepath.Join(s.opts.DataDir, "db", "wukongimdb"),
			filepath.Join(s.opts.DataDir, "cluster", "logdb"),
		}
		if s.opts.ConfigOptions != nil && strings.TrimSpace(s.opts.ConfigOptions.ConfigPath) != "" {
			cfgDir := filepath.Dir(s.opts.ConfigOptions.ConfigPath)
			cfgLogDir := filepath.Join(cfgDir, "cfglogdb")
			dirs = append(dirs, cfgDir, cfgLogDir)
			probeDirs = append(probeDirs, cfgLogDir)
		}
		for _, dir := range dirs {
			info, err := os.Stat(dir)
			if err != nil {
				return fmt.Errorf("local storage stat failed on %s: %w", dir, err)
			}
			if !info.IsDir() {
				return fmt.Errorf("local storage path %s is not a directory", dir)
			}
			if info.Mode().Perm()&0200 == 0 {
				return fmt.Errorf("local storage directory %s is read-only (mode=%v)", dir, info.Mode().Perm())
			}
		}
		optionalClusterConfigDir := filepath.Join(s.opts.DataDir, "cluster", "clusterconfig")
		if info, err := os.Stat(optionalClusterConfigDir); err == nil && info.IsDir() && info.Mode().Perm()&0200 == 0 {
			return fmt.Errorf("local storage directory %s is read-only (mode=%v)", optionalClusterConfigDir, info.Mode().Perm())
		}
		s.storageHealthMu.Lock()
		needProbe := !s.bootstrapReady.Load() || time.Since(s.lastStorageProbeAt) >= 250*time.Millisecond
		if needProbe {
			for _, probeDir := range probeDirs {
				f, probeErr := os.CreateTemp(probeDir, ".readyz_probe_*")
				if probeErr != nil {
					s.lastStorageProbeAt = time.Time{}
					s.storageHealthMu.Unlock()
					return fmt.Errorf("local storage write probe failed on %s: %w", probeDir, probeErr)
				}
				probeName := f.Name()
				_ = f.Close()
				_ = os.Remove(probeName)
			}
			s.lastStorageProbeAt = time.Now()
		}
		s.storageHealthMu.Unlock()
	}
	if s.db != nil {
		if checker, ok := s.db.(interface{ CheckWriteHealth() error }); ok {
			if err := checker.CheckWriteHealth(); err != nil {
				return err
			}
		}
	}
	if s.slotServer != nil {
		if err := s.slotServer.CheckWriteHealth(); err != nil {
			return err
		}
	}
	if s.cfgServer != nil {
		if err := s.cfgServer.CheckWriteHealth(); err != nil {
			return err
		}
	}
	return nil
}

// CheckLocalServiceReady 检查本节点是否已完成启动追平并具备持续本地服务能力（用于 /readyz readiness 探针及入口连接守卫）
func (s *Server) CheckLocalServiceReady() (retErr error) {
	defer func() {
		if retErr != nil && s != nil {
			s.bootstrapReady.Store(false)
		}
	}()
	if err := s.CheckLocalProcessHealth(); err != nil {
		return err
	}
	if err := s.checkLocalStorageHealth(); err != nil {
		return err
	}
	localNodeId := s.opts.ConfigOptions.NodeId
	localNode := s.cfgServer.Node(localNodeId)
	if localNode == nil {
		return errors.New("local node not found in cluster config")
	}
	if !localNode.Online {
		return errors.New("local node is offline")
	}
	if s.opts.Role != types.NodeRole_NodeRoleProxy && localNode.Status != types.NodeStatus_NodeStatusJoined {
		return fmt.Errorf("local node status %v not joined", localNode.Status)
	}

	slots := s.Slots()
	if len(slots) == 0 {
		return errors.New("no slots")
	}
	if uint32(len(slots)) != s.opts.ConfigOptions.SlotCount {
		return errors.New("slot count not match")
	}

	maxReadyLag := s.opts.MaxReadyApplyLag
	if maxReadyLag == 0 {
		maxReadyLag = 64
	}
	maxStallTicks := s.opts.MaxApplyStallTicks
	if maxStallTicks <= 0 {
		maxStallTicks = 50
	}
	bootstrapped := s.bootstrapReady.Load()

	for _, slot := range slots {
		if slot.Leader == 0 {
			return fmt.Errorf("slot[%d] leader not found", slot.Id)
		}
		isLocalReplica := wkutil.ArrayContainsUint64(slot.Replicas, localNodeId)
		isLocalLearner := wkutil.ArrayContainsUint64(slot.Learners, localNodeId)
		if !isLocalReplica && !isLocalLearner {
			continue
		}
		if isLocalLearner {
			return fmt.Errorf("local node is still learner on slot[%d]", slot.Id)
		}
		if slot.Status != types.SlotStatus_SlotStatusNormal {
			return fmt.Errorf("local slot[%d] not ready (status=%v)", slot.Id, slot.Status)
		}
		if s.slotServer == nil {
			return errors.New("slot server not initialized")
		}
		slotRaft := s.slotServer.GetSlotRaft(slot.Id)
		if slotRaft == nil {
			return fmt.Errorf("local slot[%d] raft not initialized", slot.Id)
		}
		if slot.Leader == localNodeId && !slotRaft.IsLeader() {
			return fmt.Errorf("local slot[%d] leader not ready", slot.Id)
		}
		if slotRaft.ApplyFailureCount() > 0 {
			return fmt.Errorf("local slot[%d] has %d apply failures", slot.Id, slotRaft.ApplyFailureCount())
		}
		if slotRaft.StoreFailureCount() > 0 {
			return fmt.Errorf("local slot[%d] has %d store failures", slot.Id, slotRaft.StoreFailureCount())
		}
		if slotRaft.ApplyingTicks() > maxStallTicks {
			return fmt.Errorf("local slot[%d] apply stalled for %d ticks", slot.Id, slotRaft.ApplyingTicks())
		}
		appliedIdx := slotRaft.AppliedIndex()
		committedIdx := slotRaft.CommittedIndex()
		bootstrapTarget := slotRaft.BootstrapCommittedIndex()
		if appliedIdx < bootstrapTarget {
			return fmt.Errorf("local slot[%d] bootstrap apply lagging: applied=%d target=%d", slot.Id, appliedIdx, bootstrapTarget)
		}
		if !bootstrapped && appliedIdx < committedIdx {
			return fmt.Errorf("local slot[%d] initial apply lagging: applied=%d committed=%d", slot.Id, appliedIdx, committedIdx)
		}
		if committedIdx > appliedIdx && committedIdx-appliedIdx > maxReadyLag {
			return fmt.Errorf("local slot[%d] apply lag %d exceeds max ready lag %d", slot.Id, committedIdx-appliedIdx, maxReadyLag)
		}
	}

	if s.channelServer != nil {
		if err := s.channelServer.CheckLocalHealth(maxStallTicks, maxReadyLag); err != nil {
			return err
		}
	}

	s.bootstrapReady.Store(true)
	return nil
}

// CheckClusterStatus 检查集群状态
func (s *Server) CheckClusterStatus() error {
	if err := s.CheckLocalServiceReady(); err != nil {
		return err
	}
	slots := s.Slots()
	if len(slots) == 0 {
		return errors.New("no slots")
	}

	if uint32(len(slots)) != s.opts.ConfigOptions.SlotCount {
		return errors.New("slot count not match")
	}

	for _, slot := range slots {
		if slot.Leader == 0 {
			return errors.New("slot leader not found")
		}
		if slot.Status != types.SlotStatus_SlotStatusNormal {
			return errors.New("slot not ready")
		}
	}
	return nil
}

func (s *Server) MustWaitClusterReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := s.CheckLocalServiceReady()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *Server) Slots() []*types.Slot {
	return s.cfgServer.Slots()
}
