package channel

import (
	"context"
	"fmt"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

const maxAdmissionBytes = 64 << 20

// AdmitMessage reserves a bounded in-memory queue entry on the current leader.
// enqueue runs off-owner; the reservation prevents handoff until done is called.
// The caller must enqueue locally and call done after processing, never on a
// timer. A successful return acknowledges admission, not durable persistence.
func (s *Server) AdmitMessage(ctx context.Context, id string, typ uint8, messageID uint64, size int64, enqueue func(done func())) error {
	if size <= 0 || size > maxAdmissionBytes || enqueue == nil {
		return ErrSendUnavailable
	}
	if total := s.admissionBytes.Add(size); total > maxAdmissionBytes {
		s.admissionBytes.Add(-size)
		return ErrSendUnavailable
	}
	key := wkutil.ChannelToKey(id, typ)
	rg := s.getRaftGroup(key)
	var owner *Channel
	err := rg.Do(ctx, key, func(r raftgroup.IRaft) error {
		ch, ok := r.(*Channel)
		if !ok {
			return ErrSendUnavailable
		}
		if err := ch.AdmitMessage(messageID); err != nil {
			return fmt.Errorf("%w: %w", ErrSendUnavailable, err)
		}
		owner = ch
		return nil
	})
	if err != nil {
		s.admissionBytes.Add(-size)
		return err
	}
	var once sync.Once
	done := func() {
		once.Do(func() {
			defer s.admissionBytes.Add(-size)
			// Do either executes this callback or the group has stopped. Do not
			// time out a completion and strand a live leader's drain barrier.
			_ = rg.Do(context.Background(), key, func(r raftgroup.IRaft) error {
				if r == owner {
					owner.CompleteMessage(messageID)
				}
				return nil
			})
		})
	}
	enqueue(done)
	return nil
}
