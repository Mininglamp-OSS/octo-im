package slot

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

// 配置发送改变
func (s *Server) OnConfigChange(cfg *types.Config) {
	s.slotUpdateLock.Lock()
	defer s.slotUpdateLock.Unlock()

	for _, slot := range cfg.Slots {
		s.addOrUpdateSlotRaft(slot, cfg.Version)
	}
}

// 根据slot的配置添加或者更新raft
func (s *Server) AddOrUpdateSlotRaft(slot *types.Slot) {
	s.slotUpdateLock.Lock()
	defer s.slotUpdateLock.Unlock()
	s.addOrUpdateSlotRaft(slot, 0)
}

func (s *Server) addOrUpdateSlotRaft(slot *types.Slot, version uint64) {
	if slot.Leader == 0 && slot.Status != types.SlotStatus_SlotStatusCandidate {
		return
	}

	shardNo := SlotIdToKey(slot.Id)
	rft := s.raftGroup.GetRaft(shardNo)

	// 如果当前节点不是slot的replica或者learner则不处理
	if !wkutil.ArrayContainsUint64(slot.Replicas, s.opts.NodeId) && !wkutil.ArrayContainsUint64(slot.Learners, s.opts.NodeId) {
		if rft != nil {
			err := s.raftGroup.Do(context.Background(), shardNo, func(current raftgroup.IRaft) error {
				if current != nil {
					s.raftGroup.RemoveRaft(current)
				}
				return nil
			})
			if err != nil {
				s.Error("retire slot failed", zap.String("slot", shardNo), zap.Error(err))
			}
		}
		return
	}

	cfg := s.slotToConfig(slot)
	cfg.Version = version

	if rft == nil { // 添加slot的raft
		if err := s.raftGroup.WaitStorage(context.Background(), shardNo); err != nil {
			s.Error("drain retired slot failed", zap.String("slot", shardNo), zap.Error(err))
			return
		}
		slotNode := newSlot(slot, s)
		s.raftGroup.AddRaft(slotNode)

		s.raftGroup.AddEventFor(slotNode, rafttype.Event{
			Type:   rafttype.ConfChange,
			Config: cfg,
		})
	} else { // 更新slot的分布式配置
		raftSlot := rft.(*Slot)
		if raftSlot.needUpdate(slot) {
			s.raftGroup.AddEventFor(raftSlot, rafttype.Event{
				Type:   rafttype.ConfChange,
				Config: cfg,
			})
			raftSlot.slot = slot.Clone()
		}
	}
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
		cfg.Leader = 0
		cfg.Role = rafttype.RoleFollower
	}

	return cfg

}
