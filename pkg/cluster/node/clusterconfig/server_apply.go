package clusterconfig

import (
	"encoding/binary"
	"fmt"
	"slices"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

func (s *Server) applyLogs(logs []types.Log) error {
	for _, log := range logs {
		err := s.applyLog(log)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) applyLog(log types.Log) error {
	// A batch may be retried after saveConfig or membership delivery fails.
	// The full application config already includes earlier logs in that batch;
	// do not repeat side effects (e.g. OfflineCount), but retry persistence and
	// pending Raft delivery before allowing the applied marker to advance.
	if log.Index > s.config.version() {
		cmd := &CMD{}
		err := cmd.Unmarshal(log.Data)
		if err != nil {
			s.Error("unmarshal cmd err", zap.Error(err), zap.Uint64("index", log.Index), zap.ByteString("data", log.Data))
			return err
		}

		before := s.configToRaftConfig(s.config).Clone()
		err = s.handleCmd(cmd)
		if err != nil {
			s.Error("handle cmd failed", zap.Error(err))
			return err
		}
		after := s.configToRaftConfig(s.config)
		s.membershipPending = s.membershipPending || !sameRaftMembership(before, after)
		s.config.cfg.Term = log.Term
		s.config.cfg.Version = log.Index
		s.configSavePending = true
		s.configNotifyPending = true
	}

	if s.configSavePending {
		if err := s.config.saveConfig(); err != nil {
			s.Error("save config err", zap.Error(err))
			return err
		}
		s.configSavePending = false
	}
	// Membership becomes visible to Raft only after its version and saved
	// application config are updated. Learners must not promote on VoteReq.
	if s.membershipPending {
		if err := s.switchConfig(s.config); err != nil {
			s.Error("apply raft membership failed", zap.Error(err), zap.Uint64("index", log.Index))
			return err
		}
		s.membershipPending = false
	}
	if s.configNotifyPending {
		s.NotifyConfigChangeEvent()
		s.configNotifyPending = false
	}
	return nil
}

func (s *Server) handleCmd(cmd *CMD) error {
	switch cmd.CmdType {
	case CMDTypeConfigChange: // 配置改变
		return s.handleConfigChange(cmd)
	case CMDTypeConfigApiServerAddrChange: // 节点api server地址改变
		return s.handleApiServerAddrChange(cmd)
	case CMDTypeConfigClusterAddrChange: // 节点cluster通讯地址改变
		return s.handleClusterAddrChange(cmd)
	case CMDTypeNodeOnlineStatusChange: // 节点在线状态改变
		return s.handleNodeOnlineStatusChange(cmd)
	case CMDTypeSlotUpdate: // 槽更新
		return s.handleSlotUpdate(cmd)
	case CMDTypeNodeJoin: // 节点加入
		return s.handleNodeJoin(cmd)
	case CMDTypeNodeJoining: // 节点加入中
		return s.handleNodeJoining(cmd)
	case CMDTypeNodeJoined: // 节点加入完成
		return s.handleNodeJoined(cmd)
	case CMDTypeSlotMigrate: // 槽迁移
		return s.handleSlotMigrate(cmd)
	case CMDTypeSlotStatusChange: // 槽状态改变
		return s.handleSlotStatusChange(cmd)
	}
	return nil
}

func (s *Server) handleConfigChange(cmd *CMD) error {
	cfg := &pb.Config{}
	err := cfg.Unmarshal(cmd.Data)
	if err != nil {
		s.Error("unmarshal config err", zap.Error(err))
		return err
	}
	s.config.update(cfg)
	return nil
}

func (s *Server) handleApiServerAddrChange(cmd *CMD) error {
	nodeId, apiServerAddr, err := DecodeApiServerAddrChange(cmd.Data)
	if err != nil {
		s.Error("decode api server addr change err", zap.Error(err))
		return err
	}

	s.config.updateApiServerAddr(nodeId, apiServerAddr)
	return nil
}

func (s *Server) handleClusterAddrChange(cmd *CMD) error {
	nodeId, clusterAddr, err := DecodeClusterAddrChange(cmd.Data)
	if err != nil {
		s.Error("decode cluster addr change err", zap.Error(err))
		return err
	}

	s.config.updateClusterAddr(nodeId, clusterAddr)
	return nil
}

func (s *Server) handleNodeOnlineStatusChange(cmd *CMD) error {
	nodeId, online, err := DecodeNodeOnlineStatusChange(cmd.Data)
	if err != nil {
		s.Error("decode node online status change err", zap.Error(err))
		return err
	}

	s.config.updateNodeOnlineStatus(nodeId, online)
	return nil
}

func (s *Server) handleSlotUpdate(cmd *CMD) error {
	slotset := pb.SlotSet{}
	err := slotset.Unmarshal(cmd.Data)
	if err != nil {
		s.Error("unmarshal slotset err", zap.Error(err))
		return err
	}
	s.config.updateSlots(slotset)

	return nil
}

func (s *Server) handleNodeJoin(cmd *CMD) error {

	newNode := &pb.Node{}
	err := newNode.Unmarshal(cmd.Data)
	if err != nil {
		s.Error("unmarshal node err", zap.Error(err))
		return err
	}
	s.config.addOrUpdateNode(newNode)

	// 将新节点加入学习者列表
	if !wkutil.ArrayContainsUint64(s.config.cfg.Learners, newNode.Id) {
		s.config.cfg.Learners = append(s.config.cfg.Learners, newNode.Id)
		// 如果是新加入的节点，就是从自己迁移到自己
		s.config.cfg.MigrateFrom = newNode.Id
		s.config.cfg.MigrateTo = newNode.Id
	}
	return nil
}

func (s *Server) handleNodeJoining(cmd *CMD) error {
	if len(cmd.Data) != 8 {
		return fmt.Errorf("invalid node joining payload length: %d", len(cmd.Data))
	}
	nodeId := binary.BigEndian.Uint64(cmd.Data)
	s.config.updateNodeJoining(nodeId)
	return nil
}

func (s *Server) handleNodeJoined(cmd *CMD) error {
	nodeId, slots, err := DecodeNodeJoined(cmd.Data)
	if err != nil {
		s.Error("decode node joined err", zap.Error(err))
		return err
	}
	s.config.updateNodeJoined(nodeId, slots)
	return nil
}

func (s *Server) handleSlotMigrate(cmd *CMD) error {
	slotId, fromNodeId, toNodeId, err := DecodeMigrateSlot(cmd.Data)
	if err != nil {
		s.Error("decode migrate slot err", zap.Error(err))
		return err
	}
	s.config.updateSlotMigrate(slotId, fromNodeId, toNodeId)
	return nil
}

func (s *Server) handleSlotStatusChange(cmd *CMD) error {
	slotId, status, err := DecodeSlotStatusChange(cmd.Data)
	if err != nil {
		s.Error("decode slot status change err", zap.Error(err))
		return err
	}
	s.config.updateSlotStatus(slotId, status)
	return nil
}

// Ignore application-only versions/metadata; compare all Raft membership fields
// so new command types cannot silently bypass membership delivery.
func sameRaftMembership(a, b types.Config) bool {
	sameSet := func(x, y []uint64) bool {
		x, y = slices.Clone(x), slices.Clone(y)
		slices.Sort(x)
		slices.Sort(y)
		return slices.Equal(slices.Compact(x), slices.Compact(y))
	}
	return sameSet(a.Replicas, b.Replicas) && sameSet(a.Learners, b.Learners) &&
		a.MigrateFrom == b.MigrateFrom && a.MigrateTo == b.MigrateTo
}
