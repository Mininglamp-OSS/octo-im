package slot

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
)

type Slot struct {
	*raft.Node
	slot    *types.Slot
	shardNo string
	wklog.Log
}

func newSlot(slot *types.Slot, s *Server) *Slot {
	shardNo := SlotIdToKey(slot.Id)
	st := &Slot{
		slot:    slot.Clone(),
		shardNo: shardNo,
		Log:     wklog.NewWKLog("slot"),
	}
	state, err := s.storage.GetState(shardNo)
	if err != nil {
		st.Panic("get state failed", zap.Error(err))
	}
	lastLogIndex, err := s.storage.GetTermStartIndex(shardNo, state.LastTerm)
	if err != nil {
		st.Panic("get last term failed", zap.Error(err))
	}
	nodeOpts := []raft.Option{
		raft.WithKey(shardNo),
		raft.WithNodeId(s.opts.NodeId),
		raft.WithSaveHardState(func(state rafttypes.HardState) error { return s.storage.SaveHardState(shardNo, state) }),
	}
	if s.opts.LearnerToFollowerMaxApplyLag > 0 {
		nodeOpts = append(nodeOpts, raft.WithLearnerToFollowerMaxApplyLag(s.opts.LearnerToFollowerMaxApplyLag))
	}
	if s.opts.MaxAdmissionDrainTicks > 0 {
		nodeOpts = append(nodeOpts, raft.WithMaxAdmissionDrainTicks(s.opts.MaxAdmissionDrainTicks))
	}
	node := raft.NewNode(lastLogIndex, state, raft.NewOptions(nodeOpts...))
	st.Node = node

	return st
}

// needUpdate 判断是否需要更新
func (s *Slot) needUpdate(newSlot *types.Slot) bool {

	return !s.slot.Equal(newSlot)
}

func (s *Slot) LastLogIndexAndTerm() (uint64, uint32) {
	return s.LastLogIndex(), s.LastLogTerm()
}
