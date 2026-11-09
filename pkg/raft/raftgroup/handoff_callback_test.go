package raftgroup

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type handoffSaveStorage struct {
	IStorage
	entered chan types.Config
	release chan struct{}
	err     error
}

func (s *handoffSaveStorage) SaveConfig(_ string, cfg types.Config) error {
	s.entered <- cfg
	<-s.release
	return s.err
}

func TestGroupHandoffCallbackPreservesIdentity(t *testing.T) {
	for _, failed := range []bool{false, true} {
		for _, cancel := range []bool{false, true} {
			t.Run(fmt.Sprintf("failed=%t/cancel=%t", failed, cancel), func(t *testing.T) {
				n := raft.NewNode(1, types.RaftState{LastLogIndex: 100, LastTerm: 7, AppliedIndex: 100}, raft.NewOptions(raft.WithKey("handoff"), raft.WithNodeId(1), raft.WithReplicas([]uint64{1})))
				cfg := n.Config().Clone()
				cfg.Version, cfg.Learners, cfg.MigrateFrom, cfg.MigrateTo = 1, []uint64{4}, 1, 4
				require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
				n.Ready()
				sync := types.Event{Type: types.SyncReq, From: 4, Term: 7, Index: 101, StoredIndex: 101, ConfigVersion: 1, Reason: types.ReasonOnlySync}
				require.NoError(t, n.Step(sync))
				var req types.Event
				for _, e := range n.Ready() {
					if e.Type == types.LearnerToLeaderReq {
						req = e
					}
				}
				require.NotZero(t, req.RoleChangeID)
				storage := &handoffSaveStorage{entered: make(chan types.Config, 1), release: make(chan struct{})}
				if failed {
					storage.err = errors.New("unknown save outcome")
				}
				g := New(NewOptions(WithStorage(storage)))
				defer g.Stop()
				g.AddRaft(n)
				g.handleRoleChangeReq(n, req)
				select {
				case next := <-storage.entered:
					require.Equal(t, uint64(4), next.Leader)
				case <-time.After(time.Second):
					t.Fatal("SaveConfig did not start")
				}
				if cancel {
					cfg.Version++
					cfg.Learners, cfg.MigrateFrom, cfg.MigrateTo = nil, 0, 0
					require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
				}
				close(storage.release)
				var responses []Event
				require.Eventually(t, func() bool { responses = g.mq.Get(); return len(responses) > 0 }, time.Second, time.Millisecond)
				require.Len(t, responses, 1)
				response := responses[0]
				require.Same(t, n, response.instance)
				require.Equal(t, req.RoleChangeID, response.RoleChangeID)
				require.Equal(t, req.Term, response.Term)
				require.Equal(t, req.ConfigVersion, response.ConfigVersion)
				require.NoError(t, n.Step(response.Event))
				if cancel {
					require.NoError(t, n.Step(n.NewPropose([]byte("cancelled"))))
				} else {
					require.ErrorIs(t, n.Step(n.NewPropose([]byte("owner-not-updated"))), types.ErrProposalDropped)
					require.NoError(t, n.Step(sync))
					var retry types.Event
					for _, e := range n.Ready() {
						if e.Type == types.LearnerToLeaderReq {
							retry = e
						}
					}
					require.Greater(t, retry.RoleChangeID, req.RoleChangeID)
				}
			})
		}
	}
}
