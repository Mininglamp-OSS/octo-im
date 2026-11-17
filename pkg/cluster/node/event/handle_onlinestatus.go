package event

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

func (h *handler) handleOnlineStatus(nodeId uint64, online bool) {
	// 提案节点状态
	err := h.cfgServer.ProposeNodeOnlineStatus(nodeId, online)
	if err != nil {
		h.Error("propose node offline", zap.Error(err))
		return
	}

	// 处理槽迁移
	err = h.handleSlotChangeByNodeOnlineStatus(nodeId, online)
	if err != nil {
		h.Error("handleSlotChangeByNodeOnlineStatus failed", zap.Error(err))
		return
	}
}

func (h *handler) handleSlotChangeByNodeOnlineStatus(nodeId uint64, online bool) error {
	var newSlots []*types.Slot
	if online { // 节点上线

		h.Info("节点上线", zap.Uint64("nodeId", nodeId))
		slots := h.cfgServer.Slots()

		onlineNodeCount := h.cfgServer.AllowVoteAndJoinedOnlineNodeCount()
		if onlineNodeCount == 0 {
			return nil
		}

		avgSlotLeaderCount := h.cfgServer.SlotCount() / uint32(onlineNodeCount) // 平均每个节点的槽领导数量

		// 每个节点当前的槽领导数量
		nodeSlotLeaderCountMap := make(map[uint64]uint32)
		for _, slot := range slots {
			nodeSlotLeaderCountMap[slot.Leader]++
		}

		currentNodeSlotLeaderCount := nodeSlotLeaderCountMap[nodeId] // 当前节点的槽领导数量

		if currentNodeSlotLeaderCount >= avgSlotLeaderCount { // 当前节点的槽领导数量已经达到平均值，不需要迁入
			return nil
		}

		// 计算需要迁入的槽领导数量
		needSlotLeaderCount := avgSlotLeaderCount - currentNodeSlotLeaderCount

		for nId, slotLeaderCount := range nodeSlotLeaderCountMap {
			if slotLeaderCount < avgSlotLeaderCount { // 槽领导数量小于平均值的节点不参与计算
				continue
			}

			if needSlotLeaderCount <= 0 {
				break
			}

			if nId == nodeId {
				continue
			}

			exportSlotLeaderCount := slotLeaderCount - avgSlotLeaderCount // 需要迁出的槽领导数量

			for _, slot := range slots {
				if slot.Leader == nId &&
					exportSlotLeaderCount > 0 &&
					needSlotLeaderCount > 0 &&
					slot.MigrateFrom == 0 &&
					slot.MigrateTo == 0 &&
					wkutil.ArrayContainsUint64(slot.Replicas, nodeId) {

					newSlot := slot.Clone()
					newSlot.MigrateFrom = nId
					newSlot.MigrateTo = nodeId
					newSlots = append(newSlots, newSlot)
					exportSlotLeaderCount--
					needSlotLeaderCount--
				}
			}

		}

	} else { // 节点下线
		h.Info("节点下线", zap.Uint64("nodeId", nodeId))
		slots := h.cfgServer.Slots()
		onlineNodeCount := h.cfgServer.AllowVoteAndJoinedOnlineNodeCount()
		var avgSlotLeaderCount uint32
		if onlineNodeCount > 0 {
			avgSlotLeaderCount = h.cfgServer.SlotCount() / uint32(onlineNodeCount) // 平均每个节点的槽领导数量
		}

		// 每个节点当前的槽领导数量
		nodeSlotLeaderCountMap := make(map[uint64]uint32)
		for _, slot := range slots {
			nodeSlotLeaderCountMap[slot.Leader]++
		}

		currentNodeSlotLeaderCount := nodeSlotLeaderCountMap[nodeId] // 当前节点的槽领导数量

		for _, slot := range slots {
			// 若当前在线槽领导正在向已下线节点迁移领导权，取消该迁移以便原领导恢复服务
			if slot.Leader != nodeId && slot.MigrateFrom == slot.Leader && slot.MigrateTo == nodeId {
				newSlot := slot.Clone()
				newSlot.MigrateFrom = 0
				newSlot.MigrateTo = 0
				newSlot.Learners = wkutil.RemoveUint64(newSlot.Learners, nodeId)
				newSlots = append(newSlots, newSlot)
				continue
			}

			if slot.Leader != nodeId || slot.Status == types.SlotStatus_SlotStatusCandidate {
				continue
			}

			transitioned := false
			for nId, slotLeaderCount := range nodeSlotLeaderCountMap {
				if nId == nodeId || !h.cfgServer.NodeIsOnline(nId) {
					continue
				}

				if currentNodeSlotLeaderCount <= 0 {
					break
				}

				if slotLeaderCount > avgSlotLeaderCount {
					continue
				}

				newSlot := slot.Clone()
				newSlot.Status = types.SlotStatus_SlotStatusCandidate
				newSlot.ExpectLeader = nId
				if newSlot.MigrateFrom == nodeId {
					newSlot.MigrateFrom = 0
					newSlot.MigrateTo = 0
					newSlot.Learners = wkutil.RemoveUint64(newSlot.Learners, slot.MigrateTo)
				}
				newSlots = append(newSlots, newSlot)
				nodeSlotLeaderCountMap[nId]++
				currentNodeSlotLeaderCount--
				transitioned = true
				break
			}
			if !transitioned {
				newSlot := slot.Clone()
				newSlot.Status = types.SlotStatus_SlotStatusCandidate
				newSlot.ExpectLeader = 0
				for _, repId := range slot.Replicas {
					if repId != nodeId && h.cfgServer.NodeIsOnline(repId) {
						newSlot.ExpectLeader = repId
						break
					}
				}
				if newSlot.MigrateFrom == nodeId {
					newSlot.MigrateFrom = 0
					newSlot.MigrateTo = 0
					newSlot.Learners = wkutil.RemoveUint64(newSlot.Learners, slot.MigrateTo)
				}
				newSlots = append(newSlots, newSlot)
			}
		}

	}
	if len(newSlots) > 0 {
		err := h.cfgServer.ProposeSlots(newSlots)
		if err != nil {
			h.Error("handleNodeOnlineChange failed,ProposeSlots failed", zap.Error(err))
			return err
		}
	}
	return nil
}
