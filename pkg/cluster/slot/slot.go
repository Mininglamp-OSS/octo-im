package slot

import (
	"sync"
	"sync/atomic"

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
	recoveryTerm     uint32
	nodeID           uint64
	recovering       bool
	recoveryProof    *sync.Map
	recoveryBoundary *atomic.Uint64
	wklog.Log
}

func newSlot(slot *types.Slot, s *Server) *Slot {
	shardNo := SlotIdToKey(slot.Id)
	st := &Slot{
		slot:             slot.Clone(),
		shardNo:          shardNo,
		nodeID:           s.opts.NodeId,
		recovering:       s.opts.Recovering,
		recoveryProof:    &s.recoveryProof,
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

func (s *Slot) recoveredPrefix() bool {
	index, ok := s.recoveryProof.Load(s.shardNo)
	return ok && s.Node.StoredPrefixReady(index.(uint64))
}

// A larger metadata term or briefly following another leader is not proof
// that a reused node ID recovered its slot log. Fence leadership on the owner
// until a peer's verified committed prefix is durable or this boot's rejoin
// establishes a fresh controller boundary. Proof survives Raft recreation.
func (s *Slot) Step(e rafttypes.Event) error {
	if s.recovering {
		boundary := s.recoveryBoundary.Load()
		if s.recoveredPrefix() || e.Type == rafttypes.ConfChange && boundary != 0 && e.Config.Version >= boundary {
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
	if err := s.Node.Step(e); err != nil {
		return err
	}
	if s.recovering && e.Type == rafttypes.SyncResp && e.Reason == rafttypes.ReasonOk &&
		e.From != s.nodeID && e.From == s.Node.LeaderId() && e.Term == s.Node.LastTerm() {
		index, ok := s.recoveryProof.Load(s.shardNo)
		if !ok || e.CommittedIndex > index.(uint64) {
			s.recoveryProof.Store(s.shardNo, e.CommittedIndex)
		}
	}
	if s.recovering && s.recoveredPrefix() {
		s.recovering = false
	}
	return nil
}

// LeaderReadReady is called on the Raft owner loop. Recovery fences reads and
// proposal admission even if another role-change path installs a leader role.
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
