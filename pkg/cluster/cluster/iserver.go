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

func isTransientClusterRPCError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connect not authed") ||
		strings.Contains(msg, "conn is nil") ||
		strings.Contains(msg, "connect is not connected") ||
		strings.Contains(msg, "leader not found") ||
		strings.Contains(msg, "not slot leader") ||
		strings.Contains(msg, "retry required") ||
		strings.Contains(msg, "not a ready local leader") ||
		strings.Contains(msg, "receive propose resp error") ||
		strings.Contains(msg, "foward propose failed") ||
		strings.Contains(msg, "not leader") ||
		strings.Contains(msg, "proposal dropped") ||
		strings.Contains(msg, "stopped") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "eof") ||
		strings.Contains(msg, "connection refused")
}

// GetOrCreateChannelClusterConfig 获取或创建频道分布式配置
func (s *Server) GetOrCreateChannelClusterConfigFromSlotLeader(channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {

	s.channelKeyLock.Lock(channelId)
	defer s.channelKeyLock.Unlock(channelId)

	var lastErr error
	delay := 25 * time.Millisecond
	for attempt := 0; attempt < 10; attempt++ {
		// 获取频道槽领导
		slotLeaderId, err := s.SlotLeaderIdOfChannel(channelId, channelType)
		if err != nil {
			lastErr = err
		} else if slotLeaderId == 0 {
			lastErr = fmt.Errorf("slot[%d] leader not found", s.getSlotId(channelId))
		} else if s.opts.ConfigOptions.NodeId == slotLeaderId {
			// 如果当前节点是频道槽领导，则直接返回频道分布式配置
			cfg, lErr := s.getOrCreateChannelClusterConfigFromLocal(channelId, channelType)
			if lErr == nil || !isTransientClusterRPCError(lErr) {
				return cfg, lErr
			}
			lastErr = lErr
		} else {
			// 向频道槽领导请求频道分布式配置
			cfg, rErr := s.rpcClient.RequestGetOrCreateChannelClusterConfig(slotLeaderId, channelId, channelType)
			if rErr == nil || !isTransientClusterRPCError(rErr) {
				return cfg, rErr
			}
			lastErr = rErr
		}
		if attempt == 9 || !isTransientClusterRPCError(lastErr) {
			return wkdb.EmptyChannelClusterConfig, lastErr
		}
		if s.cancelCtx != nil {
			select {
			case <-s.cancelCtx.Done():
				return wkdb.EmptyChannelClusterConfig, lastErr
			case <-time.After(delay):
			}
		} else {
			time.Sleep(delay)
		}
		delay = min(delay*2, 100*time.Millisecond)
	}
	return wkdb.EmptyChannelClusterConfig, lastErr
}

func (s *Server) isPeerRPCAuthed(nodeId uint64) bool {
	if s == nil || nodeId == 0 {
		return false
	}
	if s.opts != nil && s.opts.ConfigOptions != nil && nodeId == s.opts.ConfigOptions.NodeId {
		return true
	}
	if s.nodeManager == nil {
		return true
	}
	n := s.nodeManager.node(nodeId)
	if n == nil {
		return false
	}
	if n.isTestMode() {
		return true
	}
	return n.client != nil && n.client.IsAuthed()
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

const maxChannelConfigReadAttempts = 3

func (s *Server) LoadOnlyChannelClusterConfig(channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	baseCtx := context.Background()
	if s.cancelCtx != nil {
		baseCtx = s.cancelCtx
	}
	ctx, cancel := context.WithTimeout(baseCtx, s.conversationReadTimeout())
	defer cancel()

	slotId := s.getSlotId(channelId)
	var lastErr error
	for attempt := 0; attempt < maxChannelConfigReadAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return wkdb.EmptyChannelClusterConfig, lastErr
			}
			return wkdb.EmptyChannelClusterConfig, err
		}
		slotLeaderId := s.cfgServer.SlotLeaderId(slotId)
		if slotLeaderId == 0 {
			lastErr = fmt.Errorf("slot[%d] leader not found", slotId)
		} else if s.opts.ConfigOptions.NodeId == slotLeaderId {
			cfg, err := s.loadConversationConfigLocal(ctx, channelId, channelType)
			if err == nil || errors.Is(err, wkdb.ErrNotFound) {
				return cfg, err
			}
			lastErr = err
			if !errors.Is(err, ErrConversationReadRetry) || s.slotServer == nil {
				return wkdb.EmptyChannelClusterConfig, err
			}
		} else {
			cfg, err := s.rpcClient.RequestGetChannelClusterConfig(slotLeaderId, channelId, channelType)
			if err == nil || errors.Is(err, wkdb.ErrNotFound) {
				return cfg, err
			}
			lastErr = err
		}
		if attempt+1 < maxChannelConfigReadAttempts {
			select {
			case <-ctx.Done():
				return wkdb.EmptyChannelClusterConfig, lastErr
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	return wkdb.EmptyChannelClusterConfig, lastErr
}

func (s *Server) maxSlotApplyWait() time.Duration {
	if s.opts != nil && s.opts.MaxSlotApplyWait > 0 {
		return s.opts.MaxSlotApplyWait
	}
	return 500 * time.Millisecond
}

func (s *Server) waitLocalSlotApplied(ctx context.Context, slotId uint32) error {
	if s == nil || s.slotServer == nil || s.cfgServer == nil {
		return fmt.Errorf("%w: slot[%d] local slot server not initialized", ErrConversationReadRetry, slotId)
	}
	st := s.slotServer.GetSlotRaft(slotId)
	if st == nil {
		return fmt.Errorf("%w: slot[%d] local raft not found", ErrConversationReadRetry, slotId)
	}
	before, err := s.slotServer.ReadLeaderState(ctx, slotId)
	if err != nil {
		return err
	}
	if !before.Exists || !before.Ready || before.LeaderID != s.opts.ConfigOptions.NodeId || s.cfgServer.SlotLeaderId(slotId) != s.opts.ConfigOptions.NodeId {
		return fmt.Errorf("%w: slot[%d] is not a ready local leader", ErrConversationReadRetry, slotId)
	}
	if before.AppliedIndex >= before.CommittedIndex {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, s.maxSlotApplyWait())
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	current := before
	for current.AppliedIndex < before.CommittedIndex {
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("%w: slot[%d] local apply lagging: applied=%d committed=%d: %w", ErrConversationReadRetry, slotId, current.AppliedIndex, before.CommittedIndex, waitCtx.Err())
		case <-ticker.C:
		}
		current, err = s.slotServer.ReadLeaderState(waitCtx, slotId)
		if err != nil {
			return err
		}
		if !sameConversationLeader(before, current) || s.cfgServer.SlotLeaderId(slotId) != s.opts.ConfigOptions.NodeId {
			return fmt.Errorf("%w: slot[%d] leadership changed while waiting for apply", ErrConversationReadRetry, slotId)
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
	if s.cfgServer == nil {
		return errors.New("clusterconfig server not initialized")
	}
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
	cfgLeaderId := s.cfgServer.LeaderId()
	if cfgLeaderId == 0 {
		return errors.New("clusterconfig leader not found")
	}
	cfgLeaderNode := s.cfgServer.Node(cfgLeaderId)
	if cfgLeaderNode == nil {
		return fmt.Errorf("clusterconfig leader node[%d] not found", cfgLeaderId)
	}
	if !cfgLeaderNode.Online {
		return fmt.Errorf("clusterconfig leader node[%d] is offline", cfgLeaderId)
	}
	if cfgLeaderId == localNodeId {
		if !s.cfgServer.IsLeader() {
			return errors.New("local clusterconfig leader not ready")
		}
		if s.cfgServer.GetClusterConfig() == nil {
			return errors.New("local clusterconfig not initialized")
		}
	} else if strings.TrimSpace(localNode.ApiServerAddr) != "" && strings.TrimSpace(cfgLeaderNode.ApiServerAddr) == "" {
		return fmt.Errorf("clusterconfig leader node[%d] api server addr not ready", cfgLeaderId)
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
