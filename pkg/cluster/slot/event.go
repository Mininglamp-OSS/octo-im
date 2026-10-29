package slot

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// 配置发送改变
func (s *Server) OnConfigChange(cfg *types.Config) {
	s.slotUpdateLock.Lock()
	defer s.slotUpdateLock.Unlock()
	if s.opts.Recovering {
		if s.recoveryConfig != nil && cfg.Version < s.recoveryConfig.Version {
			return
		}
		s.recoveryConfig = cfg.Clone()
	}
	force := s.finishRecoveryLocked()
	for _, st := range cfg.Slots {
		s.addOrUpdateSlotRaft(st, cfg.Version, force)
	}
}

// SetRecoveryJoinVersion records the exact applied command returned by this
// process's join RPC. Old seeds without this proof leave the fence in place.
func (s *Server) SetRecoveryJoinVersion(version uint64) {
	s.slotUpdateLock.Lock()
	defer s.slotUpdateLock.Unlock()
	if !s.opts.Recovering || version == 0 || s.recoveryJoinVersion != 0 {
		return
	}
	s.recoveryJoinVersion = version
	if s.finishRecoveryLocked() {
		for _, st := range s.recoveryConfig.Slots {
			s.addOrUpdateSlotRaft(st, s.recoveryConfig.Version, true)
		}
	}
}

// finishRecoveryLocked accepts only a Joined snapshot after this boot's join.
// That transaction fences the returning node's current slot leadership as
// Candidate; only its following log-based election can publish normal leadership.
func (s *Server) finishRecoveryLocked() bool {
	cfg := s.recoveryConfig
	if s.recoveryBoundary.Load() != 0 || s.recoveryJoinVersion == 0 || cfg == nil || cfg.Version <= s.recoveryJoinVersion {
		return false
	}
	for _, n := range cfg.Nodes {
		if n.Id == s.opts.NodeId && n.Status == types.NodeStatus_NodeStatusJoined {
			s.recoveryBoundary.Store(cfg.Version)
			return true
		}
	}
	return false
}

// AddOrUpdateSlotRaft applies unversioned metadata without releasing recovery.
func (s *Server) AddOrUpdateSlotRaft(st *types.Slot) {
	s.slotUpdateLock.Lock()
	defer s.slotUpdateLock.Unlock()
	s.addOrUpdateSlotRaft(st, 0, false)
}

func (s *Server) addOrUpdateSlotRaft(slot *types.Slot, version uint64, force bool) {
	if slot.Leader == 0 && slot.Status != types.SlotStatus_SlotStatusCandidate {
		return
	}
	runtimeConfig := func(local *Slot) rafttype.Config {
		cfg := s.slotRuntimeConfig(slot, local)
		cfg.Version = version
		return cfg
	}

	shardNo := SlotIdToKey(slot.Id)
	rft := s.raftGroup.GetRaft(shardNo)

	// 如果当前节点不是slot的replica或者learner则不处理
	if !wkutil.ArrayContainsUint64(slot.Replicas, s.opts.NodeId) && !wkutil.ArrayContainsUint64(slot.Learners, s.opts.NodeId) {
		if rft != nil {
			s.raftGroup.RemoveRaft(rft)
		}
		return
	}

	if rft == nil { // 添加slot的raft
		slotNode := newSlot(slot, s)
		s.raftGroup.AddRaft(slotNode)

		s.raftGroup.AddEvent(shardNo, rafttype.Event{
			Type:   rafttype.ConfChange,
			Config: runtimeConfig(slotNode),
		})
	} else { // 更新slot的分布式配置
		raftSlot := rft.(*Slot)
		if force || raftSlot.needUpdate(slot) {
			s.raftGroup.AddEvent(shardNo, rafttype.Event{
				Type:   rafttype.ConfChange,
				Config: runtimeConfig(raftSlot),
			})
		}
	}
}

func (s *Server) slotRuntimeConfig(st *types.Slot, local *Slot) rafttype.Config {
	cfg := s.slotToConfig(st)
	if local.recoveryTerm != 0 {
		if st.Leader != s.opts.NodeId || st.Term > local.recoveryTerm {
			local.recoveryTerm = 0
		} else {
			cfg.Leader = 0
			cfg.Role = rafttype.RoleFollower
		}
	}
	return cfg
}

func (s *Server) slotToConfig(st *types.Slot) rafttype.Config {

	var role rafttype.Role
	if st.Status == types.SlotStatus_SlotStatusCandidate {
		role = rafttype.RoleCandidate
	} else if wkutil.ArrayContainsUint64(st.Learners, s.opts.NodeId) {
		role = rafttype.RoleLearner
	} else {
		if st.Leader == s.opts.NodeId {
			role = rafttype.RoleLeader
		} else {
			role = rafttype.RoleFollower
		}
	}

	cfg := rafttype.Config{
		MigrateFrom: st.MigrateFrom,
		MigrateTo:   st.MigrateTo,
		Replicas:    st.Replicas,
		Learners:    st.Learners,
		Term:        st.Term,
		Leader:      st.Leader,
		Role:        role,
	}
	if st.Status == types.SlotStatus_SlotStatusCandidate {
		// The controller owns this election; fence the old leader while it
		// compares durable logs rather than retaining a local leader identity.
		cfg.Leader = 0
		cfg.Role = rafttype.RoleFollower
	}

	return cfg

}
