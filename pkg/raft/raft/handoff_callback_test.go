package raft

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/lni/goutils/syncutil"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

type handoffSaveStorage struct {
	Storage
	entered chan types.Config
	release chan struct{}
	err     error
}

func (s *handoffSaveStorage) SaveConfig(cfg types.Config) error {
	s.entered <- cfg
	<-s.release
	return s.err
}

func TestStandaloneHandoffCallbackPreservesIdentity(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, cancel := range []bool{false, true} {
			t.Run(fmt.Sprintf("failed=%t/cancel=%t", failed, cancel), func(t *testing.T) {
				n, req := pausedHandoff(t, false)
				storage := &handoffSaveStorage{entered: make(chan types.Config, 1), release: make(chan struct{})}
				if failed {
					storage.err = errors.New("unknown save outcome")
				}
				r := newTestRaftWithNode(n)
				r.opts = NewOptions(WithStorage(storage))
				r.stopper, r.stepC = syncutil.NewStopper(), make(chan stepReq, 1)
				var err error
				r.pool, err = ants.NewPool(1)
				require.NoError(t, err)
				defer r.pool.Release()
				r.handleRoleChangeReq(req)
				select {
				case cfg := <-storage.entered:
					require.Equal(t, uint64(4), cfg.Leader)
				case <-time.After(time.Second):
					t.Fatal("SaveConfig did not start")
				}
				if cancel {
					cancelledHandoff(t, n)
				}
				close(storage.release)
				var response types.Event
				select {
				case returned := <-r.stepC:
					response = returned.event
				case <-time.After(time.Second):
					t.Fatal("SaveConfig completion was lost")
				}
				require.Equal(t, req.RoleChangeID, response.RoleChangeID)
				require.Equal(t, req.Term, response.Term)
				require.Equal(t, req.ConfigVersion, response.ConfigVersion)
				require.NoError(t, n.Step(response))
				if cancel {
					commitAfterHandoff(t, n)
				} else {
					require.ErrorIs(t, n.Step(n.NewPropose([]byte("owner-not-updated"))), types.ErrProposalDropped)
					require.False(t, n.replicaSync[4].roleSwitching, "a matching completion must permit SyncReq retry")
				}
			})
		}
	}
}
