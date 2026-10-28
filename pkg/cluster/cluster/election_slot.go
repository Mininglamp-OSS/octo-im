package cluster

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// 槽选举
func (s *Server) OnSlotElection(slots []*types.Slot) error {

	err := s.handleSlotElection(slots)
	if err != nil {
		s.Error("handleSlotElection failed", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) handleSlotElection(slots []*types.Slot) error {
	if len(slots) == 0 {
		return nil
	}
	electionSlotLeaderMap := make(map[uint64][]uint32) // 参与选举的节点Id和对应的槽Id集合
	electionSlotIds := make([]uint32, 0, len(slots))
	for _, slot := range slots {

		if slot.Status != types.SlotStatus_SlotStatusCandidate {
			continue
		}
		for _, replicaId := range slot.Replicas {
			// 不在线的副本 不能参与选举
			if !s.cfgServer.NodeIsOnline(replicaId) {
				continue
			}

			// 如果副本的分布式配置的版本小于当前领导的则也不能参与选举，
			// 这样做的目的防止旧领导slot的状态没更新到Candidate，导致slot继续在接受日志，
			// 这时候通过日志选举出来的领导肯定不可信
			if s.cfgServer.NodeConfigVersionFromLeader(replicaId) < s.cfgServer.GetClusterConfig().Version {
				s.Warn("replica config version < leader config version,stop election", zap.Uint64("replicaCfgVersion", s.cfgServer.NodeConfigVersionFromLeader(replicaId)), zap.Uint64("leaderCfgVersion", s.cfgServer.GetClusterConfig().Version))
				return nil
			}

			electionSlotLeaderMap[replicaId] = append(electionSlotLeaderMap[replicaId], slot.Id)
		}
		electionSlotIds = append(electionSlotIds, slot.Id)
	}
	if len(electionSlotLeaderMap) == 0 {
		s.Info("没有需要选举的槽！！！")
		return nil
	}

	// 获取槽在各个副本上的日志信息
	slotInfoResps, err := s.requestSlotInfos(electionSlotLeaderMap)
	if err != nil {
		s.Error("request slot infos error", zap.Error(err))
		return err
	}
	if len(slotInfoResps) == 0 {
		s.Info("没有获取到槽的日志信息！！！")
		return nil
	}

	// ================== 根据日志信息选举新的leader ==================
	// 收集各个槽的日志信息
	replicaLastLogInfoMap, err := s.collectSlotLogInfo(slotInfoResps)
	if err != nil {
		s.Error("collect slot log info error", zap.Error(err))
		return err
	}
	// 根据日志信息计算出槽的领导者
	slotLeaderMap := s.calculateSlotLeader(replicaLastLogInfoMap, electionSlotIds)
	if len(slotLeaderMap) == 0 {
		s.Warn("没有选举出任何槽领导者！！！", zap.Uint32s("slotIds", electionSlotIds))
		return nil
	}

	// ================== 提案新的槽配置 ==================
	getSlot := func(slotId uint32) *types.Slot {
		for _, slot := range slots {
			if slot.Id == slotId {
				return slot
			}
		}
		return nil
	}

	newSlots := make([]*types.Slot, 0, len(slots))
	for slotId, newLeaderId := range slotLeaderMap {
		if newLeaderId == 0 { // 没有选出领导忽略掉
			s.Info("no leader elected", zap.Uint32("slotId", slotId))
			continue
		}
		slot := getSlot(slotId)
		if slot != nil {
			if slot.Status == types.SlotStatus_SlotStatusCandidate && slot.ExpectLeader == slot.Leader {
				complete := true
				for _, id := range slot.Replicas {
					if _, ok := replicaLastLogInfoMap[id][slotId]; !ok {
						complete = false
						break
					}
				}
				if !complete {
					continue
				}
			}
			if slot.Leader != newLeaderId || slot.Status == types.SlotStatus_SlotStatusCandidate {
				newSlot, err := slotElectionConfig(slot, newLeaderId, replicaLastLogInfoMap)
				if err != nil {
					return err
				}
				s.Info("slot leader election success", zap.Uint32("slotId", slotId), zap.Uint64("newLeader", newLeaderId))
				newSlots = append(newSlots, newSlot)
			}
		}
	}

	if len(newSlots) > 0 {
		err = s.cfgServer.ProposeSlots(newSlots)
		if err != nil {
			s.Error("handle slot election failed, propose slot failed", zap.Error(err))
			return err
		}
	}
	return nil
}

func slotElectionConfig(slot *types.Slot, leader uint64, infos map[uint64]map[uint32]SlotInfo) (*types.Slot, error) {
	term := slot.Term
	for _, id := range slot.Replicas {
		if info, ok := infos[id][slot.Id]; ok {
			term = max(term, info.Term, info.LogTerm)
		}
	}
	if term == math.MaxUint32 {
		return nil, fmt.Errorf("slot %d election term exhausted", slot.Id)
	}
	next := slot.Clone()
	next.Leader, next.Term = leader, term+1
	next.ExpectLeader = 0
	next.Status = types.SlotStatus_SlotStatusNormal
	recovering := slot.Status == types.SlotStatus_SlotStatusCandidate && slot.ExpectLeader == slot.Leader
	// Recovery supersedes a previous leadership transfer. A real membership
	// migration still has a learner and must continue through normal catch-up.
	if next.MigrateTo != 0 && (next.MigrateTo == leader || recovering &&
		wkutil.ArrayContainsUint64(next.Replicas, next.MigrateTo) && !wkutil.ArrayContainsUint64(next.Learners, next.MigrateTo)) {
		next.MigrateFrom, next.MigrateTo = 0, 0
		next.Learners = wkutil.RemoveUint64(next.Learners, leader)
	}
	return next, nil
}

// 请求槽的日志高度
func (s *Server) requestSlotInfos(waitElectionSlots map[uint64][]uint32) ([]*SlotLogInfoResp, error) {
	slotInfoResps := make([]*SlotLogInfoResp, 0, len(waitElectionSlots))
	var responsesMu sync.Mutex
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*4)
	defer cancel()
	requestGroup, _ := errgroup.WithContext(timeoutCtx)
	for nodeId, slotIds := range waitElectionSlots {
		if nodeId == s.opts.ConfigOptions.NodeId { // 本节点
			slotInfos, err := s.slotInfos(slotIds)
			if err != nil {
				s.Error("get slot infos error", zap.Error(err))
				continue
			}
			responsesMu.Lock()
			slotInfoResps = append(slotInfoResps, &SlotLogInfoResp{
				NodeId: nodeId,
				Slots:  slotInfos,
			})
			responsesMu.Unlock()
			continue
		} else {
			requestGroup.Go(func(nID uint64, sids []uint32) func() error {
				return func() error {
					resp, err := s.rpcClient.RequestSlotLastLogInfo(nID, &SlotLogInfoReq{
						SlotIds: sids,
					})
					if err != nil {
						s.Warn("request slot log info error", zap.Error(err))
						return nil
					}
					responsesMu.Lock()
					slotInfoResps = append(slotInfoResps, resp)
					responsesMu.Unlock()
					return nil
				}
			}(nodeId, slotIds))
		}
	}
	_ = requestGroup.Wait()
	return slotInfoResps, nil

}

func (s *Server) slotInfos(slotIds []uint32) ([]SlotInfo, error) {
	slotInfos := make([]SlotInfo, 0, len(slotIds))
	for _, slotId := range slotIds {
		st := s.slotServer.GetSlotRaft(slotId)
		if st == nil {
			continue
		}
		lastLog, err := s.slotServer.LastLog(slotId)
		if err != nil {
			s.Error("get slot last log error", zap.Error(err))
			continue
		}
		slotInfos = append(slotInfos, SlotInfo{
			SlotId:   slotId,
			LogIndex: lastLog.Index,
			LogTerm:  lastLog.Term,
			Term:     st.LastTerm(),
		})
	}
	return slotInfos, nil
}

// 收集slot的各个副本的日志高度
func (s *Server) collectSlotLogInfo(slotInfoResps []*SlotLogInfoResp) (map[uint64]map[uint32]SlotInfo, error) {
	slotLogInfos := make(map[uint64]map[uint32]SlotInfo, len(slotInfoResps))
	// 合法的最小选举人数
	quorum := int(s.opts.ConfigOptions.SlotMaxReplicaCount/2) + 1

	slotQuorumMap := make(map[uint32]int)

	for _, resp := range slotInfoResps {
		for _, slotInfo := range resp.Slots {
			slotQuorumMap[slotInfo.SlotId]++
		}
	}

	for _, resp := range slotInfoResps {
		slotLogInfos[resp.NodeId] = make(map[uint32]SlotInfo, len(resp.Slots))
		for _, slotInfo := range resp.Slots {
			if slotQuorumMap[slotInfo.SlotId] < quorum {
				s.Foucs("slot quorum < quorum", zap.Uint32("slotId", slotInfo.SlotId), zap.Int("quorum", quorum), zap.Int("slotQuorum", slotQuorumMap[slotInfo.SlotId]))
				continue
			}
			slotLogInfos[resp.NodeId][slotInfo.SlotId] = slotInfo
		}
	}
	return slotLogInfos, nil
}

// 计算每个槽的领导节点
func (s *Server) calculateSlotLeader(slotLogInfos map[uint64]map[uint32]SlotInfo, slotIds []uint32) map[uint32]uint64 {
	slotLeaderMap := make(map[uint32]uint64, len(slotIds))
	for _, slotId := range slotIds {
		slotLeaderMap[slotId] = s.electionSlotLeaderBySlot(slotLogInfos, slotId)
	}
	return slotLeaderMap
}

// 计算槽的领导节点
// slotLogInfos 槽在各个副本上的日志信息
// slotId 计算领导节点的槽Id
func (s *Server) electionSlotLeaderBySlot(slotLogInfos map[uint64]map[uint32]SlotInfo, slotId uint32) uint64 {
	var expectLeader uint64 // 期望领导
	var lastLogIndex uint64 // 最新一条日志下标
	var lastLogTerm uint32  // 最新一条日志任期
	st := s.cfgServer.Slot(slotId)

	// 如果槽正在进行领导者转移，则优先计算转移节点的日志信息
	if st != nil && st.ExpectLeader != 0 && st.ExpectLeader != st.Leader {
		for replicaId, logIndexMap := range slotLogInfos {
			slotInfo, ok := logIndexMap[slotId]
			if !ok {
				continue
			}
			if replicaId == st.ExpectLeader {
				expectLeader = replicaId
				lastLogIndex = slotInfo.LogIndex
				lastLogTerm = slotInfo.LogTerm
				break
			}
		}
	}

	// Durable log freshness chooses the leader. A higher runtime term on an
	// empty restarted replica must not outrank the only surviving log.
	for replicaId, logIndexMap := range slotLogInfos {
		candidate, ok := logIndexMap[slotId]
		if !ok {
			continue
		}
		if expectLeader == 0 {
			expectLeader = replicaId
			lastLogIndex = candidate.LogIndex
			lastLogTerm = candidate.LogTerm
			continue
		}

		preferred := st != nil && st.ExpectLeader != 0 && st.ExpectLeader != st.Leader
		tieWins := candidate.LogTerm == lastLogTerm && candidate.LogIndex == lastLogIndex &&
			(!preferred || expectLeader != st.ExpectLeader) && replicaId < expectLeader
		if candidate.LogTerm > lastLogTerm || candidate.LogTerm == lastLogTerm && candidate.LogIndex > lastLogIndex || tieWins {
			expectLeader = replicaId
			lastLogIndex = candidate.LogIndex
			lastLogTerm = candidate.LogTerm
		}

	}
	return expectLeader
}
