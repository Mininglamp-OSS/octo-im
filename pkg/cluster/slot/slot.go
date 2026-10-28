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
	// recoveryTerm fences inherited empty leadership until a new election.
	recoveryTerm uint32
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
	if s.opts.Recovering && slot.Leader == s.opts.NodeId && state.LastLogIndex == 0 {
		st.recoveryTerm = slot.Term
	}
	lastLogIndex, err := s.storage.GetTermStartIndex(shardNo, state.LastTerm)
	if err != nil {
		st.Panic("get last term failed", zap.Error(err))
	}
	node := raft.NewNode(lastLogIndex, state, raft.NewOptions(raft.WithKey(shardNo), raft.WithNodeId(s.opts.NodeId), raft.WithSaveHardState(func(state rafttypes.HardState) error { return s.storage.SaveHardState(shardNo, state) })))
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
