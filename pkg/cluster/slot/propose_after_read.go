package slot

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
)

func (s *Server) ProposeAfterRead(ctx context.Context, slotID uint32, before raftgroup.ReadState, data []byte, validate func() error) (uint64, error) {
	if err := s.checkProposable(slotID); err != nil {
		return 0, err
	}
	return s.raftGroup.ProposeAfterRead(ctx, SlotIdToKey(slotID), before, s.GenLogId(), data, validate)
}
