package channel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

type confirmationHardStateDB struct {
	*confirmationReadDB
	blockedKey  string
	saveEntered chan struct{}
	saveRelease chan struct{}
	saveOnce    sync.Once
}

func (d *confirmationHardStateDB) SaveRaftHardState(key string, state types.HardState) error {
	if key == d.blockedKey && state.Term == 2 {
		d.saveOnce.Do(func() {
			close(d.saveEntered)
			<-d.saveRelease
		})
	}
	return d.DB.SaveRaftHardState(key, state)
}

func TestCanceledProposalReturnsWhileRaftOwnerIsBlocked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	otherCfg := wkdb.ChannelClusterConfig{
		ChannelId: "bystander", ChannelType: 2, LeaderId: 1, Replicas: []uint64{1}, Term: 1, ConfVersion: 1,
	}
	otherKey := wkutil.ChannelToKey(otherCfg.ChannelId, otherCfg.ChannelType)
	db := &confirmationHardStateDB{
		confirmationReadDB: newConfirmationReadDB(t),
		blockedKey:         otherKey,
		saveEntered:        make(chan struct{}),
		saveRelease:        make(chan struct{}),
	}
	releaseRead := sync.OnceFunc(func() { close(db.release) })
	releaseOwner := sync.OnceFunc(func() { close(db.saveRelease) })
	s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
	group := s.getRaftGroup(otherKey)
	require.Same(t, group, s.getRaftGroup(wkutil.ChannelToKey("retry", 2)))
	other, err := createChannel(otherCfg, s, group)
	require.NoError(t, err)
	group.AddRaft(other)
	require.NoError(t, other.switchConfig(channelConfigToRaftConfig(1, otherCfg)))

	proposalCtx, cancelProposal := context.WithCancel(ctx)
	result := make(chan error, 1)
	finished := make(chan struct{})
	reqs := retryRequests(t, retryMessage(100, "alice", "deadline"))
	go func() {
		defer close(finished)
		_, err := s.ProposeBatchUntilAppliedTimeoutForLocal(proposalCtx, "retry", 2, reqs)
		result <- err
	}()
	t.Cleanup(func() {
		cancelProposal()
		releaseRead()
		releaseOwner()
		select {
		case <-finished:
		case <-time.After(2 * time.Second):
			t.Error("proposal did not exit after releasing the Raft owner")
		}
	})

	select {
	case <-db.entered:
	case <-ctx.Done():
		t.Fatal("proposal did not reach the final canonical read")
	}
	// A term change in a different channel stalls the shared Raft owner in
	// synchronous hard-state persistence while this proposal is off-owner.
	require.NoError(t, group.Do(ctx, otherKey, func(r raftgroup.IRaft) error {
		cfg := r.Config().Clone()
		cfg.Term++
		cfg.Version++
		return r.Step(types.Event{Type: types.ConfChange, Config: cfg})
	}))
	select {
	case <-db.saveEntered:
	case <-ctx.Done():
		t.Fatal("Raft owner did not reach the other channel's hard-state write")
	}

	cancelProposal()
	releaseRead()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, ErrSendOutcomeUnknown)
	case <-time.After(time.Second):
		t.Fatal("canceled proposal remained blocked by the other channel's hard-state write")
	}
}
