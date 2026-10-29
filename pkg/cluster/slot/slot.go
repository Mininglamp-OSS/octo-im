package slot

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
	"sync/atomic"
)

type Slot struct {
	*raft.Node
	slot    *types.Slot
	shardNo string
	// recoveryTerm fences inherited empty leadership until a new election.
	recoveryTerm uint32
	// recovering is owned by the Raft loop and lasts across historical configs.
	recovering       bool
	nodeID           uint64
	recoveryBoundary *atomic.Uint64
	wklog.Log
}

func newSlot(slot *types.Slot, s *Server) *Slot {
	shardNo := SlotIdToKey(slot.Id)
	st := &Slot{
		slot:             slot.Clone(),
		shardNo:          shardNo,
		recovering:       s.opts.Recovering,
		nodeID:           s.opts.NodeId,
		recoveryBoundary: &s.recoveryBoundary,
		Log:              wklog.NewWKLog("slot"),
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

// Step fences inherited leadership until this boot's join has established a
// fresh controller boundary. Following a peer remains allowed for catch-up.
func (s *Slot) Step(e rafttypes.Event) error {
	if s.recovering {
		boundary := s.recoveryBoundary.Load()
		if e.Type == rafttypes.ConfChange && boundary != 0 && e.Config.Version >= boundary {
			s.recovering = false
		} else if (e.Type == rafttypes.ConfChange || e.Type == rafttypes.ConfigResp) && e.Config.Leader == s.nodeID {
			e.Config = e.Config.Clone()
			e.Config.Role = rafttypes.RoleFollower
			e.Config.Leader = s.Node.LeaderId()
			if e.Config.Leader == s.nodeID {
				e.Config.Leader = raft.None
			}
		}
	}
	if s.recovering && e.Type == rafttypes.Propose {
		return rafttypes.ErrNotLeader
	}
	return s.Node.Step(e)
}

// LeaderReadReady is called on the owner loop; replay cannot authorize reads.
func (s *Slot) LeaderReadReady() bool {
	return !s.recovering && s.Node.LeaderReadReady()
}

// needUpdate 判断是否需要更新
func (s *Slot) needUpdate(newSlot *types.Slot) bool {

	return !s.slot.Equal(newSlot)
}

func (s *Slot) LastLogIndexAndTerm() (uint64, uint32) {
	return s.LastLogIndex(), s.LastLogTerm()
}
