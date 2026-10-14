package raftgroup

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

var ErrReadStateChanged = errors.New("raft changed after metadata read")

// ProposeAfterRead reserves proposal admission, drains the fixed log prefix
// already admitted, and revalidates the business decision before appending.
// Ordinary writers cannot extend that prefix while it is draining. Replication,
// Apply and the Raft owner keep running; no naturally idle slot is required.
// validate runs outside the owner, may read storage, and must not propose to this
// same Raft. Leadership/config changes still reject the decision, never forward it.
func (rg *RaftGroup) ProposeAfterRead(ctx context.Context, key string, before ReadState, id uint64, data []byte, validate func() error) (uint64, error) {
	if validate == nil {
		return 0, errors.New("metadata revalidation is required")
	}
	r := rg.GetRaft(key)
	if r == nil {
		return 0, ErrReadStateChanged
	}
	// Use the same reservation as generic proposals, with bounded acquisition.
	locker, ok := r.(interface{ TryLock() bool })
	if !ok {
		return 0, ErrReadStateChanged
	}
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for !locker.TryLock() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-rg.stopper.ShouldStop():
			return 0, ErrGroupStopped
		case <-poll.C:
		}
	}
	checkOwner := func(current IRaft) error {
		if current != r || !before.Exists || !before.Ready || !r.IsLeader() ||
			r.LeaderId() != before.LeaderID || r.Config().Term != before.Term ||
			r.Config().Version != before.ConfigVersion {
			return ErrReadStateChanged
		}
		if ready, ok := r.(interface{ LeaderReadReady() bool }); !ok || !ready.LeaderReadReady() {
			return ErrReadStateChanged
		}
		return nil
	}
	index, err := func() (uint64, error) {
		defer r.Unlock()
		var target uint64
		if err := rg.Do(ctx, key, func(current IRaft) error {
			if err := checkOwner(current); err != nil {
				return err
			}
			target = r.LastLogIndex()
			return nil
		}); err != nil {
			return 0, err
		}
		for {
			state, err := rg.ReadLeaderState(ctx, key)
			if err != nil {
				return 0, err
			}
			if !state.Exists || !state.Ready || state.LeaderID != before.LeaderID ||
				state.Term != before.Term || state.ConfigVersion != before.ConfigVersion {
				return 0, ErrReadStateChanged
			}
			if state.AppliedIndex >= target {
				break
			}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-rg.stopper.ShouldStop():
				return 0, ErrGroupStopped
			case <-poll.C:
			}
		}
		if err := validate(); err != nil {
			return 0, err
		}
		var index uint64
		err := rg.Do(ctx, key, func(current IRaft) error {
			if err := checkOwner(current); err != nil {
				return err
			}
			// Fail closed if any future ingress bypasses the proposal reservation.
			if r.LastLogIndex() != target || r.AppliedIndex() < target {
				return ErrReadStateChanged
			}
			index = target + 1
			if index == 0 {
				return errors.New("raft log index exhausted")
			}
			return r.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Id: id, Index: index, Term: before.Term, Data: data}}})
		})
		return index, err
	}()
	if err != nil {
		return 0, err
	}
	// Release admission before waiting for this proposal's Apply. Once admitted,
	// errors are outcome-unknown, never ErrReadStateChanged.
	// Callers must reload metadata instead of replaying across newer changes.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := rg.ReadLeaderState(ctx, key)
		if err != nil {
			return index, err
		}
		if !state.Exists || state.LeaderID != before.LeaderID || state.Term != before.Term {
			return index, types.ErrNotLeader
		}
		if state.AppliedIndex >= index {
			return index, nil
		}
		select {
		case <-ctx.Done():
			return index, ctx.Err()
		case <-ticker.C:
		}
	}
}
