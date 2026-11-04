package clusterconfig

import (
	"encoding/binary"
	"slices"
	"strings"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"go.uber.org/zap"
)

// 提案配置
func (s *Server) ProposeConfig(cfg *pb.Config) error {
	for _, node := range cfg.Nodes {
		if err := pb.ValidateAdvertiseAddr(node.ClusterAddr); err != nil {
			return err
		}
	}
	data, err := cfg.Marshal()
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeConfigChange, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}
	_, err = s.ProposeUntilApplied(s.genConfigId(), cmdBytes)
	return err
}

// ProposeApiServerAddr 提案节点api server地址变更
func (s *Server) ProposeApiServerAddr(nodeId uint64, apiServerAddr string) error {

	data, err := EncodeApiServerAddrChange(nodeId, apiServerAddr)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeConfigApiServerAddrChange, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	_, err = s.ProposeUntilApplied(s.genConfigId(), cmdBytes)
	if err != nil {
		s.Error("ProposeApiServerAddr failed", zap.Error(err))
		return err
	}
	return nil
}

// ProposeClusterAddr 提案节点cluster通讯地址变更
func (s *Server) ProposeClusterAddr(nodeId uint64, clusterAddr string) error {
	clusterAddr = strings.TrimSpace(clusterAddr)
	if err := pb.ValidateAdvertiseAddr(clusterAddr); err != nil {
		return err
	}

	data, err := EncodeClusterAddrChange(nodeId, clusterAddr)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeConfigClusterAddrChange, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	_, err = s.ProposeUntilApplied(s.genConfigId(), cmdBytes)
	if err != nil {
		s.Error("ProposeClusterAddr failed", zap.Error(err))
		return err
	}
	return nil
}

// ProposeLeave 提案节点在线状态变更
func (s *Server) ProposeNodeOnlineStatus(nodeId uint64, online bool) error {
	data, err := EncodeNodeOnlineStatusChange(nodeId, online)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeNodeOnlineStatusChange, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}
	_, err = s.ProposeUntilApplied(s.genConfigId(), cmdBytes)
	if err != nil {
		s.Error("ProposeNodeOnlineStatus failed", zap.Error(err))
		return err
	}

	return nil
}

// ProposeSlots 提案槽变更
func (s *Server) ProposeSlots(slots []*pb.Slot) error {
	slotSet := pb.SlotSet(slots)
	data, err := slotSet.Marshal()
	if err != nil {
		return err
	}
	cmd := NewCMD(CMDTypeSlotUpdate, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	logId := s.genConfigId()

	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeSlots failed", zap.Error(err))
		return err
	}
	return nil
}

// ProposeJoined 提案节点加入
func (s *Server) ProposeJoined(nodeId uint64, slots []*pb.Slot) error {

	data, err := EncodeNodeJoined(nodeId, slots)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeNodeJoined, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	logId := s.genConfigId()
	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeJoined failed", zap.Error(err))
		return err
	}
	return nil
}

func (s *Server) ProposeJoining(nodeId uint64) error {
	_, err := s.proposeJoiningWithVersion(nodeId)
	return err
}

func (s *Server) proposeJoiningWithVersion(nodeId uint64) (uint64, error) {

	nodeIdBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nodeIdBytes, nodeId)

	cmd := NewCMD(CMDTypeNodeJoining, nodeIdBytes)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		s.Error("ProposeJoining cmd marshal failed", zap.Error(err))
		return 0, err
	}

	logId := s.genConfigId()
	resp, err := s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeJoining failed", zap.Error(err))
		return 0, err
	}
	return resp.Index, nil

}

// ProposeJoin 提案节点加入
func (s *Server) ProposeJoin(node *pb.Node) error {
	_, err := s.ProposeJoinWithVersion(node)
	return err
}

// ProposeJoinWithVersion returns the applied join command's exact log index.
// A recovering process uses it to distinguish its own rejoin from replay.
func (s *Server) ProposeJoinWithVersion(node *pb.Node) (uint64, error) {
	if err := pb.ValidateJoinAddr(node.ClusterAddr); err != nil {
		return 0, err
	}
	s.joinMu.Lock()
	defer s.joinMu.Unlock()
	current := s.Snapshot()
	for _, existing := range current.Nodes {
		if existing.Id != node.Id || !existing.AllowVote || existing.Role != pb.NodeRole_NodeRoleReplica || slices.Contains(current.Learners, node.Id) {
			continue
		}
		// A committed voter only needs a fresh recovery boundary. Reusing
		// NodeJoin would demote it and overwrite another admission's migration.
		if !existing.Online {
			if err := s.ProposeNodeOnlineStatus(node.Id, true); err != nil {
				return 0, err
			}
		}
		if existing.ClusterAddr != node.ClusterAddr {
			if err := s.ProposeClusterAddr(node.Id, node.ClusterAddr); err != nil {
				return 0, err
			}
		}
		// Retirement may already have committed before the online update.
		// Retry normal admission instead of acknowledging an absent member.
		if s.Node(node.Id) == nil {
			return 0, ErrMembershipBusy
		}
		return s.proposeJoiningWithVersion(node.Id)
	}
	// The Raft migration fields describe one change. A concurrent new voter
	// must retry instead of overwriting its target.
	for _, pending := range current.Nodes {
		if pending.Id != node.Id && (pending.Status == pb.NodeStatus_NodeStatusWillJoin || pending.Status == pb.NodeStatus_NodeStatusJoining) {
			return 0, ErrMembershipBusy
		}
	}
	data, err := node.Marshal()
	if err != nil {
		return 0, err
	}
	cmd := NewCMD(CMDTypeNodeJoin, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return 0, err
	}
	resp, err := s.ProposeUntilApplied(s.genConfigId(), cmdBytes)
	if err != nil {
		return 0, err
	}
	return resp.Index, nil
}

// ProposeMigrateSlot 提案槽迁移
func (s *Server) ProposeMigrateSlot(slotId uint32, fromNodeId, toNodeId uint64) error {
	if fromNodeId == toNodeId {
		return nil
	}
	data, err := EncodeMigrateSlot(slotId, fromNodeId, toNodeId)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeSlotMigrate, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	logId := s.genConfigId()
	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeMigrateSlot failed", zap.Error(err))
		return err
	}

	return nil
}

// ProposeSlotStatusChange 提案槽状态变更
func (s *Server) ProposeSlotStatusChange(slotId uint32, status pb.SlotStatus) error {
	data, err := EncodeSlotStatusChange(slotId, status)
	if err != nil {
		return err
	}

	cmd := NewCMD(CMDTypeSlotStatusChange, data)
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return err
	}

	logId := s.genConfigId()
	_, err = s.ProposeUntilApplied(logId, cmdBytes)
	if err != nil {
		s.Error("ProposeSlotStatusChange failed", zap.Error(err))
		return err
	}
	return nil
}
