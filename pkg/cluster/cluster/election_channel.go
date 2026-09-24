package cluster

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (s *Server) getOrCreateChannelClusterConfigFromLocal(channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	ctx, cancel := context.WithTimeout(s.cancelCtx, 5*time.Second)
	defer cancel()
	for {
		cfg, err := s.getOrCreateChannelClusterConfigAttempt(ctx, channelId, channelType)
		if !errors.Is(err, raft.ErrConfigVersionStale) {
			return cfg, err
		}
		// Background maintenance can win after the foreground read. Reload and
		// recompute within the same budget; never replay the losing decision with
		// a newer version or return this pre-proposal race to the first send.
		select {
		case <-ctx.Done():
			return wkdb.EmptyChannelClusterConfig, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *Server) getOrCreateChannelClusterConfigAttempt(ctx context.Context, channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	if err := ctx.Err(); err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	// 获取频道槽领导
	slotLeaderId, err := s.SlotLeaderIdOfChannel(channelId, channelType)
	if err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}

	if s.opts.ConfigOptions.NodeId != slotLeaderId {
		s.Error("not slot leader", zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		return wkdb.EmptyChannelClusterConfig, errors.New("not slot leader")
	}

	cfg, err := s.db.GetChannelClusterConfig(channelId, channelType)
	if err != nil && err != wkdb.ErrNotFound {
		return wkdb.EmptyChannelClusterConfig, err
	}

	// 如果配置不存在，则创建一个新的配置
	if wkdb.IsEmptyChannelClusterConfig(cfg) {
		cfg, err = s.createChannelClusterConfig(channelId, channelType)
		if err != nil {
			return wkdb.EmptyChannelClusterConfig, err
		}

		version, err := s.saveChannelConfig(ctx, cfg)
		if err != nil {
			return wkdb.EmptyChannelClusterConfig, err
		}
		cfg.ConfVersion = version
		return cfg, nil
	}

	return s.maintainChannelConfig(ctx, cfg)
}

// The slot owner maintains existing metadata even without a new send. Publish
// only after election succeeds, then use the same version-fenced write as role
// callbacks. A competing writer or ownership change makes this attempt retry.
func (s *Server) maintainChannelConfig(ctx context.Context, current wkdb.ChannelClusterConfig) (wkdb.ChannelClusterConfig, error) {
	if err := ctx.Err(); err != nil {
		return current, err
	}
	cfg := current.Clone()
	leaderChanged, err := s.switchLeaderIfNeed(ctx, &cfg)
	if err != nil {
		return current, err
	}
	membersChanged, err := s.joinNewRepliceIfNeed(&cfg)
	if err != nil {
		return current, err
	}
	if !leaderChanged && !membersChanged {
		return current, nil
	}
	version, err := s.saveChannelConfig(ctx, cfg)
	if err != nil {
		return current, err
	}
	cfg.ConfVersion = version
	return cfg, nil
}

// 视情况是否需要加入新的副本
func (s *Server) joinNewRepliceIfNeed(cfg *wkdb.ChannelClusterConfig) (bool, error) {
	if len(cfg.Replicas) >= int(cfg.ReplicaMaxCount) {
		return false, nil
	}
	// An interrupted expansion can leave equal, nonzero migration endpoints
	// after its learner disappeared. If the target is not a replica either,
	// no member can complete that migration. Select eligible learners below and
	// replace the stale markers in the same version-fenced metadata write.
	// Preserve real learners, transfers, partial markers and existing replicas.
	orphanedExpansion := len(cfg.Learners) == 0 && cfg.MigrateFrom != 0 &&
		cfg.MigrateFrom == cfg.MigrateTo && !wkutil.ArrayContainsUint64(cfg.Replicas, cfg.MigrateTo)
	if len(cfg.Learners) > 0 || (cfg.MigrateFrom != 0 || cfg.MigrateTo != 0) && !orphanedExpansion {
		return false, nil
	}
	allowVoteNodes := s.cfgServer.AllowVoteAndJoinedOnlineNodes()
	if len(allowVoteNodes) == 0 {
		return false, errors.New("no allow vote nodes")
	}

	if len(cfg.Replicas) >= len(allowVoteNodes) { // 如果当前已集群的副本数大于等于允许投票的节点数，则不需要加入新的副本
		return false, nil
	}

	newReplicaIds := make([]uint64, 0, len(allowVoteNodes)-len(cfg.Replicas))
	for _, node := range allowVoteNodes {
		if !wkutil.ArrayContainsUint64(cfg.Replicas, node.Id) {
			newReplicaIds = append(newReplicaIds, node.Id)
		}
	}
	// 打乱顺序，防止每次都是相同的节点加入
	rand.Shuffle(len(newReplicaIds), func(i, j int) {
		newReplicaIds[i], newReplicaIds[j] = newReplicaIds[j], newReplicaIds[i]
	})

	// 将新节点加入到学习者列表
	for _, newReplicaId := range newReplicaIds {
		cfg.MigrateFrom = newReplicaId
		cfg.MigrateTo = newReplicaId
		cfg.Learners = append(cfg.Learners, newReplicaId)
		if len(cfg.Learners)+len(cfg.Replicas) >= int(cfg.ReplicaMaxCount) {
			break
		}
	}

	return true, nil

}

// 视情况是否需要变更领导节点
func (s *Server) switchLeaderIfNeed(ctx context.Context, cfg *wkdb.ChannelClusterConfig) (bool, error) {
	if cfg.LeaderId != 0 && s.cfgServer.NodeIsOnline(cfg.LeaderId) {
		return false, nil
	}

	// 获得在线的副本
	onlineRepics := make([]uint64, 0, len(cfg.Replicas))
	for _, replicaId := range cfg.Replicas {
		if s.cfgServer.NodeIsOnline(replicaId) {
			onlineRepics = append(onlineRepics, replicaId)
		}
	}

	// 获得在线的副本的最新日志信息
	replicaLastLogInfos, err := s.requestChannelLastLogInfos(ctx, onlineRepics, cfg.ChannelId, cfg.ChannelType)
	if err != nil {
		s.Error("requestChannelLastLogInfos failed", zap.Error(err))
		return false, err
	}
	return electChannelLeader(cfg, replicaLastLogInfos)
}

// Only current voting replicas count. Desired capacity and learners do not
// change the quorum of the configuration being replaced. Log freshness, not a
// peer's current term, chooses the candidate; the new term fences every report.
// This retains the existing controller-driven election protocol: probing alone
// does not persist votes or fence a falsely suspected old leader. Configuration
// propagation and Raft term checks still delimit that pre-existing window.
func electChannelLeader(cfg *wkdb.ChannelClusterConfig, infos map[uint64]*ChannelLastLogInfoResponse) (bool, error) {
	voters := make(map[uint64]struct{}, len(cfg.Replicas))
	for _, id := range cfg.Replicas {
		if id != 0 && !wkutil.ArrayContainsUint64(cfg.Learners, id) {
			voters[id] = struct{}{}
		}
	}
	var leader uint64
	var newest *ChannelLastLogInfoResponse
	term, responses := cfg.Term, 0
	for id := range voters {
		info := infos[id]
		if info == nil {
			continue
		}
		responses++
		term = max(term, info.Term, info.LogTerm)
		if newest == nil || info.LogTerm > newest.LogTerm ||
			info.LogTerm == newest.LogTerm && (info.LogIndex > newest.LogIndex || info.LogIndex == newest.LogIndex && id < leader) {
			leader, newest = id, info
		}
	}
	if responses < len(voters)/2+1 || leader == 0 || term == math.MaxUint32 {
		return false, ErrConversationReadRetry
	}
	cfg.LeaderId, cfg.Term = leader, term+1
	return true, nil
}

// 请求副本的最新日志信息
func (s *Server) requestChannelLastLogInfos(ctx context.Context, replices []uint64, channelId string, channelType uint8) (map[uint64]*ChannelLastLogInfoResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A slow peer must not consume the enclosing worker's entire budget and
	// discard an otherwise sufficient quorum. Reserve half for publication and
	// reconciliation; only the child probe may expire with usable partial data.
	budget := 4 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/2)
	}
	if budget <= 0 {
		return nil, context.DeadlineExceeded
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	var requestGroup errgroup.Group

	respMap := make(map[uint64]*ChannelLastLogInfoResponse)
	respMapLock := &sync.Mutex{}

	for _, nodeId := range replices {
		nodeId := nodeId

		if nodeId == s.opts.ConfigOptions.NodeId {
			resp, err := s.getChannelLastLogInfo(timeoutCtx, channelId, channelType)
			if err != nil {
				s.Error("getChannelLastLogInfo failed", zap.Uint64("nodeId", nodeId), zap.Error(err))
				continue
			}
			respMapLock.Lock()
			respMap[nodeId] = resp
			respMapLock.Unlock()
			continue
		}

		requestGroup.Go(func() error {
			resp, err := s.rpcClient.RequestChannelLastLogInfo(timeoutCtx, nodeId, channelId, channelType)
			if err != nil {
				s.Error("RequestChannelLastLogInfo failed", zap.Uint64("nodeId", nodeId), zap.Error(err))
				return err
			}
			respMapLock.Lock()
			respMap[nodeId] = resp
			respMapLock.Unlock()
			return nil
		})
	}

	_ = requestGroup.Wait()

	return respMap, ctx.Err()
}

// 创建一个频道的分布式配置
func (s *Server) createChannelClusterConfig(channelId string, channelType uint8) (wkdb.ChannelClusterConfig, error) {
	allowVoteNodes := s.cfgServer.AllowVoteAndJoinedOnlineNodes() // 获取允许投票的在线节点
	if len(allowVoteNodes) == 0 {
		return wkdb.EmptyChannelClusterConfig, errors.New("no allow vote nodes")
	}

	createdAt := time.Now()
	updatedAt := time.Now()
	clusterConfig := wkdb.ChannelClusterConfig{
		ChannelId:       channelId,
		ChannelType:     channelType,
		ReplicaMaxCount: uint16(s.opts.ConfigOptions.ChannelMaxReplicaCount),
		Term:            1,
		LeaderId:        s.opts.ConfigOptions.NodeId,
		CreatedAt:       &createdAt,
		UpdatedAt:       &updatedAt,
	}
	replicaIds := make([]uint64, 0, s.opts.ConfigOptions.ChannelMaxReplicaCount)
	replicaIds = append(replicaIds, s.opts.ConfigOptions.NodeId) // 默认当前节点是领导，所以加入到副本列表中

	// 随机选择副本
	newAllowVoteNodes := make([]*types.Node, 0, len(allowVoteNodes))
	newAllowVoteNodes = append(newAllowVoteNodes, allowVoteNodes...)
	rand.Shuffle(len(newAllowVoteNodes), func(i, j int) {
		newAllowVoteNodes[i], newAllowVoteNodes[j] = newAllowVoteNodes[j], newAllowVoteNodes[i]
	})

	for _, allowVoteNode := range newAllowVoteNodes {
		if allowVoteNode.Id == s.opts.ConfigOptions.NodeId {
			continue
		}
		if len(replicaIds) >= int(s.opts.ConfigOptions.ChannelMaxReplicaCount) {
			break
		}
		replicaIds = append(replicaIds, allowVoteNode.Id)

	}
	clusterConfig.Replicas = replicaIds
	return clusterConfig, nil
}
