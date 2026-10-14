package clusterconfig

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type ownerStorage struct {
	*PebbleShardLogStorage
	op                         string
	entered, release           chan struct{}
	once                       sync.Once
	active, closes, afterClose atomic.Int32
	closeFailure               error
}

func (s *ownerStorage) enter(op string) func() {
	s.active.Add(1)
	if s.closes.Load() != 0 {
		s.afterClose.Add(1)
	}
	if op == s.op {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
	return func() {
		if s.closes.Load() != 0 {
			s.afterClose.Add(1)
		}
		s.active.Add(-1)
	}
}
func (s *ownerStorage) AppendLogs(l []types.Log, term *types.TermStartIndexInfo) error {
	defer s.enter("append")()
	return s.PebbleShardLogStorage.AppendLogs(l, term)
}
func (s *ownerStorage) GetLogs(a, b, c uint64) ([]types.Log, error) {
	defer s.enter("read")()
	return s.PebbleShardLogStorage.GetLogs(a, b, c)
}
func (s *ownerStorage) Apply(l []types.Log) error {
	defer s.enter("apply")()
	return s.PebbleShardLogStorage.Apply(l)
}
func (s *ownerStorage) Close() error {
	if s.active.Load() != 0 {
		s.afterClose.Add(1)
	}
	s.closes.Add(1)
	err := s.PebbleShardLogStorage.Close()
	if err != nil {
		return err
	}
	return s.closeFailure
}

type ownerTransport struct{ events chan types.Event }

func (tr *ownerTransport) Send(e types.Event) { tr.events <- e }
func ownerRecv[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown barrier timed out")
		var zero T
		return zero
	}
}

func TestServerShutdownStorageOwner(t *testing.T) {
	for _, op := range []string{"append", "read", "apply", "verification"} {
		t.Run(op, func(t *testing.T) {
			tr := &ownerTransport{events: make(chan types.Event, 128)}
			s := New(NewOptions(WithNodeId(1), WithInitNodes(map[uint64]string{1: ""}), WithConfigPath(t.TempDir()+"/cluster.json"), WithTransport(tr)))
			st := &ownerStorage{PebbleShardLogStorage: s.storage.(*PebbleShardLogStorage), op: op, entered: make(chan struct{}), release: make(chan struct{})}
			s.storage = st
			require.NoError(t, st.Open())
			cmd := NewCMD(CMDType(65535), nil)
			data, err := cmd.Marshal()
			require.NoError(t, err)
			if op == "verification" {
				// Restore an applied log, then register a forwarded receipt after completion.
				st.op = "read"
				require.NoError(t, st.PebbleShardLogStorage.AppendLogs([]types.Log{{Id: 1700, Index: 1, Term: 1, Data: data}}, nil))
				require.NoError(t, st.setAppliedIndex(1))
			}
			require.NoError(t, s.initRaft())
			require.NoError(t, s.raft.Start())
			leader := uint64(1)
			if op == "verification" {
				leader = 2
			}
			setup, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			require.NoError(t, s.raft.StepWait(setup, types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1}, Term: 1, Leader: leader}}))
			setupCancel()
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(st.release) }) }
			t.Cleanup(func() {
				release()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, s.WaitStopped(ctx))
				_ = s.config.cfgFile.Close()
			})
			result := make(chan error, 1)
			go func() { _, err := s.ProposeUntilAppliedTimeout(context.Background(), 1700, data); result <- err }()
			if op == "verification" {
				for {
					e := ownerRecv(t, tr.events)
					if e.Type == types.SendPropose {
						response, err := types.ProposeRespSet{&types.ProposeResp{Id: 1700, Index: 1}}.Marshal()
						require.NoError(t, err)
						s.StepRaftEvent(types.Event{Type: types.SendProposeResp, From: 2, Reason: types.ReasonOk, Logs: []types.Log{{Data: response}}})
						break
					}
				}
			}
			ownerRecv(t, st.entered)
			stopped := make(chan struct{})
			go func() { s.Stop(); close(stopped) }()
			ownerRecv(t, stopped)
			require.ErrorIs(t, ownerRecv(t, result), types.ErrStopped)
			require.Zero(t, st.closes.Load(), "Storage closed before blocked worker exited")
			require.Positive(t, st.active.Load())
			_, err = s.ProposeUntilAppliedTimeout(context.Background(), 1701, data)
			require.ErrorIs(t, err, types.ErrStopped)
			// Concurrent repeated Stop calls must share the same pending close lifecycle.
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); s.Stop() }()
			}
			wg.Wait()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err = s.WaitStopped(ctx)
			require.ErrorIs(t, err, raft.ErrIncompleteQuiescence)
			require.ErrorIs(t, err, context.Canceled)
			counts := s.raft.ProposalOutcomes()
			require.Zero(t, st.closes.Load())
			require.Zero(t, st.afterClose.Load())
			release()
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, s.WaitStopped(ctx))
			require.NoError(t, s.WaitStopped(ctx))
			s.Stop()
			require.NoError(t, s.raft.WaitQuiescence(ctx))
			require.Zero(t, st.active.Load())
			require.EqualValues(t, 1, st.closes.Load())
			require.Zero(t, st.afterClose.Load())
			require.Equal(t, counts, s.raft.ProposalOutcomes(), "late completion counted twice")
			t.Log("bounded settlement; incomplete while blocked; zero workers before exactly one Storage close")
		})
	}
}

func TestServerShutdownCloseError(t *testing.T) {
	s := New(NewOptions(WithNodeId(1), WithInitNodes(map[uint64]string{1: ""}), WithConfigPath(t.TempDir()+"/cluster.json")))
	sentinel := errors.New("close failed")
	st := &ownerStorage{PebbleShardLogStorage: s.storage.(*PebbleShardLogStorage), closeFailure: sentinel}
	s.storage = st
	require.NoError(t, st.Open())
	require.NoError(t, s.initRaft())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.ErrorIs(t, s.WaitStopped(ctx), sentinel)
	require.ErrorIs(t, s.WaitStopped(ctx), sentinel)
	require.EqualValues(t, 1, st.closes.Load())
	require.NoError(t, s.config.cfgFile.Close())
}
