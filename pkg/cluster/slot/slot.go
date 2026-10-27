package slot

import (
	"fmt"

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
	if state.Diverged {
		// The fence is in place before the raft exists, so no read or
		// proposal of this slot is served by this replica.
		s.quarantine(slot.Id, ReplicaQuarantined, fmt.Errorf("%w: slot %d opened with applied %d last %d or a divergence marker",
			rafttypes.ErrSlotDiverged, slot.Id, state.AppliedIndex, state.LastLogIndex))
	}
	node := raft.NewNode(lastLogIndex, state, raft.NewOptions(
		raft.WithKey(shardNo),
		raft.WithNodeId(s.opts.NodeId),
		raft.WithSaveHardState(func(state rafttypes.HardState) error { return s.storage.SaveHardState(shardNo, state) }),
		// Leader hand-off by role switch keeps the term (learner) or bypasses
		// the healthy-quorum election; slot leadership changes only by election.
		raft.WithLeaderRoleSwitchDisabled(true),
		// Storage raises the read fence before it writes the marker; this
		// keeps the registry in step with every other quarantine path.
		raft.WithOnQuarantine(func(failStop bool, why string) {
			to := ReplicaQuarantined
			if failStop {
				to = ReplicaFailStopped
			}
			s.quarantine(slot.Id, to, fmt.Errorf("%w: %s", rafttypes.ErrSlotDiverged, why))
		}),
	))
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
