package slot

import (
	"context"
	"path"
	"sync"
	"testing"
	"time"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type slotAppendGate struct {
	raftgroup.IStorage
	entered     chan struct{}
	release     chan struct{}
	first       sync.Once
	unblockOnce sync.Once
}

func (s *slotAppendGate) AppendLogs(key string, logs []types.Log, term *types.TermStartIndexInfo) error {
	s.first.Do(func() { close(s.entered); <-s.release })
	return s.IStorage.AppendLogs(key, logs, term)
}
func (s *slotAppendGate) unblock() { s.unblockOnce.Do(func() { close(s.release) }) }

func TestSlotRecreationDrainsOldStorageBeforeLoading(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		name := "same-instance"
		if recreate {
			name = "remove-and-rejoin"
		}
		t.Run(name, func(t *testing.T) {
			s := NewServer(NewOptions(WithNodeId(3), WithDataDir(t.TempDir()), WithSlotDbShardNum(1), WithTransport(&recoveryTestTransport{})))
			require.NoError(t, s.storage.Open())
			gate := &slotAppendGate{IStorage: s.storage, entered: make(chan struct{}), release: make(chan struct{})}
			s.raftGroup.Options().Storage = gate
			require.NoError(t, s.raftGroup.Start())
			t.Cleanup(s.Stop)
			t.Cleanup(gate.unblock)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cfg := &nodetypes.Config{Version: 10, Slots: []*nodetypes.Slot{{Id: 4, Term: 2, Leader: 1, Replicas: []uint64{1, 2, 3}}}}
			s.OnConfigChange(cfg)
			require.Eventually(t, func() bool { state, err := s.ReadLeaderState(ctx, 4); return err == nil && state.ConfigVersion == 10 }, time.Second, time.Millisecond)
			old := s.GetSlotRaft(4)
			logs := []types.Log{}
			for i := uint64(1); i <= 4; i++ {
				logs = append(logs, types.Log{Index: i, Term: 2, Data: []byte("slot-history")})
			}
			s.AddEvent("4", types.Event{Type: types.SyncResp, From: 1, Term: 2, Index: 1, Logs: logs, Reason: types.ReasonOk})
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if recreate {
				s.OnConfigChange(&nodetypes.Config{Version: 11, Slots: []*nodetypes.Slot{{Id: 4, Term: 2, Leader: 1, Replicas: []uint64{1, 2, 4}}}})
				require.Nil(t, s.GetSlotRaft(4))
				rejoined := make(chan struct{})
				go func() {
					s.OnConfigChange(&nodetypes.Config{Version: 12, Slots: []*nodetypes.Slot{{Id: 4, Term: 2, Leader: 1, Replicas: []uint64{1, 2, 4}, Learners: []uint64{3}, MigrateFrom: 1, MigrateTo: 3}}})
					close(rejoined)
				}()
				require.Eventually(t, func() bool {
					if s.slotUpdateLock.TryLock() {
						s.slotUpdateLock.Unlock()
						return false
					}
					return true
				}, time.Second, time.Millisecond)
				select {
				case <-rejoined:
					t.Fatal("replacement loaded before old append completed")
				default:
				}
				state, err := s.ReadLeaderState(ctx, 4)
				require.NoError(t, err, "Raft owner must remain responsive during drain")
				require.False(t, state.Exists)
				gate.unblock()
				select {
				case <-rejoined:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				require.NotSame(t, old, s.GetSlotRaft(4))
				require.Eventually(t, func() bool { state, err := s.ReadLeaderState(ctx, 4); return err == nil && state.ConfigVersion == 12 }, time.Second, time.Millisecond)
			} else {
				gate.unblock()
			}
			// Recreated state must include the completed old I/O, and a later
			// SyncResp must still store normally after discarding old callbacks.
			require.Eventually(t, func() bool { last, err := s.LastIndex(4); return err == nil && last == 4 }, time.Second, time.Millisecond)
			var last uint64
			require.NoError(t, s.raftGroup.Do(ctx, "4", func(r raftgroup.IRaft) error { last = r.LastLogIndex(); return nil }))
			require.Equal(t, uint64(4), last)
			s.AddEvent("4", types.Event{Type: types.SyncResp, From: 1, Term: 2, Index: 5, Logs: []types.Log{{Index: 5, Term: 2, Data: []byte("after-rejoin")}}, Reason: types.ReasonOk})
			require.Eventually(t, func() bool { last, err := s.LastIndex(4); return err == nil && last == 5 }, time.Second, time.Millisecond)
			stored, err := s.storage.GetLogs("4", 1, 6, 0)
			require.NoError(t, err)
			require.Len(t, stored, 5)
			for i, log := range stored {
				require.Equal(t, uint64(i+1), log.Index)
				if i < 4 {
					require.Equal(t, []byte("slot-history"), log.Data)
				} else {
					require.Equal(t, []byte("after-rejoin"), log.Data)
				}
			}
		})
	}
}

func TestSlotStopWaitsForAcceptedAppend(t *testing.T) {
	for _, rejoin := range []bool{false, true} {
		name := "active-slot"
		if rejoin {
			name = "replacement-waiting"
		}
		t.Run(name, func(t *testing.T) { slotStopWaitsForAcceptedAppend(t, rejoin) })
	}
}

func slotStopWaitsForAcceptedAppend(t *testing.T, rejoin bool) {
	s := NewServer(NewOptions(WithNodeId(3), WithDataDir(t.TempDir()), WithSlotDbShardNum(1), WithTransport(&recoveryTestTransport{})))
	require.NoError(t, s.storage.Open())
	gate := &slotAppendGate{IStorage: s.storage, entered: make(chan struct{}), release: make(chan struct{})}
	s.raftGroup.Options().Storage = gate
	require.NoError(t, s.raftGroup.Start())
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(s.Stop) }
	t.Cleanup(stop)
	t.Cleanup(gate.unblock)
	s.OnConfigChange(&nodetypes.Config{Version: 10, Slots: []*nodetypes.Slot{{Id: 4, Term: 2, Leader: 1, Replicas: []uint64{1, 2, 3}}}})
	s.AddEvent("4", types.Event{Type: types.SyncResp, From: 1, Term: 2, Index: 1, Logs: []types.Log{{Index: 1, Term: 2, Data: []byte("before-stop")}}, Reason: types.ReasonOk})
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("append did not start")
	}
	var rejoined chan struct{}
	if rejoin {
		s.OnConfigChange(&nodetypes.Config{Version: 11, Slots: []*nodetypes.Slot{{Id: 4, Term: 2, Leader: 1, Replicas: []uint64{1, 2, 4}}}})
		rejoined = make(chan struct{})
		go func() {
			s.OnConfigChange(&nodetypes.Config{Version: 12, Slots: []*nodetypes.Slot{{Id: 4, Term: 2, Leader: 1, Replicas: []uint64{1, 2, 3}}}})
			close(rejoined)
		}()
		require.Eventually(t, func() bool {
			if s.slotUpdateLock.TryLock() {
				s.slotUpdateLock.Unlock()
				return false
			}
			return true
		}, time.Second, time.Millisecond)
	}

	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err := s.ReadLeaderState(ctx, 4)
		return err != nil
	}, time.Second, time.Millisecond)
	select {
	case <-stopped:
		t.Fatal("storage closed while append was still pending")
	default:
	}
	if rejoin {
		select {
		case <-rejoined:
		case <-time.After(time.Second):
			t.Fatal("Stop did not cancel the replacement drain")
		}
		require.Nil(t, s.GetSlotRaft(4))
	}

	gate.unblock()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not complete")
	}
	reopened := NewPebbleShardLogStorage(s, path.Join(s.opts.DataDir, "logdb"), 1)
	require.NoError(t, reopened.Open())
	defer reopened.Close()
	logs, err := reopened.GetLogs("4", 1, 2, 0)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, []byte("before-stop"), logs[0].Data)
}
