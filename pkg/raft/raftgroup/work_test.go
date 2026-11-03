package raftgroup

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type lifecycleRaft struct {
	*waitTestRaft
	events []types.Event
}

func (r *lifecycleRaft) Step(e types.Event) error { r.events = append(r.events, e); return nil }
func (*lifecycleRaft) HasReady() bool             { return false }
func (*lifecycleRaft) Ready() []types.Event       { return nil }
func (*lifecycleRaft) Tick()                      {}

func TestRetiredResponsesCannotReachReplacement(t *testing.T) {
	for _, typ := range []types.EventType{types.StoreResp, types.GetLogsResp, types.TruncateResp, types.ApplyResp, types.LearnerToFollowerResp, types.LearnerToLeaderResp, types.FollowerToLeaderResp, types.ConfChange} {
		for _, queuedFirst := range []bool{true, false} {
			name := typ.String() + "/after-removal"
			if queuedFirst {
				name = typ.String() + "/already-queued"
			}
			t.Run(name, func(t *testing.T) {
				g := New(NewOptions())
				defer g.Stop()
				old := &lifecycleRaft{waitTestRaft: &waitTestRaft{}}
				current := &lifecycleRaft{waitTestRaft: &waitTestRaft{}}
				g.AddRaft(old)
				e := types.Event{Type: typ, Index: 4, Reason: types.ReasonOk}
				if queuedFirst {
					g.AddEventFor(old, e)
				}
				g.RemoveRaft(old)
				g.AddRaft(current)
				if !queuedFirst {
					g.AddEventFor(old, e)
				}
				g.RemoveRaft(old) // late destruction must not remove the new instance
				require.Same(t, current, g.GetRaft("slot"))
				waiter := g.wait.waitApply("slot", 4)
				defer g.wait.put(waiter)
				g.handleReceivedEvents()
				require.Empty(t, current.events)
				select {
				case <-waiter.waitC:
					t.Fatal("old Apply completed a new instance's waiter")
				default:
				}
				g.AddEventFor(current, e)
				g.handleReceivedEvents()
				require.Len(t, current.events, 1)
				if typ == types.ApplyResp {
					select {
					case <-waiter.waitC:
					default:
						t.Fatal("current Apply did not complete its waiter")
					}
				}
			})
		}
	}
}

type lifecycleStorage struct {
	IStorage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *lifecycleStorage) block()   { close(s.entered); <-s.release }
func (s *lifecycleStorage) unblock() { s.once.Do(func() { close(s.release) }) }
func (s *lifecycleStorage) AppendLogs(string, []types.Log, *types.TermStartIndexInfo) error {
	s.block()
	return nil
}
func (s *lifecycleStorage) GetLogs(string, uint64, uint64, uint64) ([]types.Log, error) {
	s.block()
	return []types.Log{{Index: 1}}, nil
}
func (s *lifecycleStorage) TruncateLogTo(string, uint64) error                           { s.block(); return nil }
func (*lifecycleStorage) DeleteLeaderTermStartIndexGreaterThanTerm(string, uint32) error { return nil }
func (*lifecycleStorage) Apply(string, []types.Log) error                                { return nil }

func TestRetiredStorageDrainAndShutdown(t *testing.T) {
	for _, typ := range []types.EventType{types.StoreReq, types.GetLogsReq, types.TruncateReq, types.ApplyReq} {
		t.Run(typ.String(), func(t *testing.T) {
			storage := &lifecycleStorage{entered: make(chan struct{}), release: make(chan struct{})}
			g := New(NewOptions(WithStorage(storage), WithTickInterval(time.Hour)))
			r := &lifecycleRaft{waitTestRaft: &waitTestRaft{}}
			g.AddRaft(r)
			require.NoError(t, g.Start())
			t.Cleanup(g.Stop)
			t.Cleanup(storage.unblock)
			e := types.Event{Type: typ, Logs: []types.Log{{Index: 1}}, Index: 1, StartIndex: 1, EndIndex: 2, StoredIndex: 1, Reason: types.ReasonOnlySync}
			switch typ {
			case types.StoreReq:
				g.handleStoreReq(r, e)
			case types.GetLogsReq:
				g.handleGetLogsReq(r, e)
			case types.TruncateReq:
				g.handleTruncateReq(r, e)
			case types.ApplyReq:
				g.handleApplyReq(r, e)
			}
			select {
			case <-storage.entered:
			case <-time.After(time.Second):
				t.Fatal("I/O did not start")
			}
			require.NoError(t, g.Do(context.Background(), "slot", func(current IRaft) error { g.RemoveRaft(current); return nil }))
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, g.WaitStorage(ctx, "slot"), context.DeadlineExceeded)
			require.NoError(t, g.Do(context.Background(), "other-slot", func(IRaft) error { return nil }), "drain must not block owner")
			require.ErrorIs(t, g.submitWork(r, true, func() { t.Error("retired task ran") }), ErrRaftNotExist)
			stopped := make(chan struct{})
			go func() { g.Stop(); close(stopped) }()
			<-g.stopper.ShouldStop()
			select {
			case <-stopped:
				t.Fatal("Stop returned while storage was still running")
			default:
			}
			require.ErrorIs(t, g.WaitStorage(context.Background(), "slot"), ErrGroupStopped)
			storage.unblock()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("Stop did not finish after I/O completed")
			}
			require.Empty(t, g.storageWork)
			require.ErrorIs(t, g.submitWork(nil, false, func() {}), ErrGroupStopped)
		})
	}
}

func TestStorageDrainRecoversAfterPoolRejection(t *testing.T) {
	g := New(NewOptions(WithGoPoolSize(1)))
	r := &lifecycleRaft{waitTestRaft: &waitTestRaft{}}
	g.AddRaft(r)
	release := make(chan struct{})
	t.Cleanup(g.Stop)
	t.Cleanup(func() { close(release) })
	require.NoError(t, g.submitWork(nil, false, func() { <-release }))
	require.Error(t, g.submitWork(r, true, func() { t.Error("rejected I/O ran") }))
	g.RemoveRaft(r)
	require.NoError(t, g.WaitStorage(context.Background(), "slot"))
	require.Empty(t, g.storageWork)
}

func TestConfigurationCallbackCanDrainItsOwnRemovedSlot(t *testing.T) {
	g := New(NewOptions())
	r := &lifecycleRaft{waitTestRaft: &waitTestRaft{}}
	g.AddRaft(r)
	require.NoError(t, g.Start())
	defer g.Stop()
	result := make(chan error, 1)
	require.NoError(t, g.submitWork(r, false, func() {
		err := g.Do(context.Background(), "slot", func(current IRaft) error { g.RemoveRaft(current); return nil })
		if err == nil {
			err = g.WaitStorage(context.Background(), "slot")
		}
		result <- err
	}))
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("configuration callback waited for itself")
	}
}
