package raft

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type configBlockedStorage struct {
	studyStorage
	operation        string
	entered, release chan struct{}
	once             sync.Once
}

func (s *configBlockedStorage) block(op string) {
	if s.operation == op {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}
}
func (s *configBlockedStorage) AppendLogs(l []types.Log, term *types.TermStartIndexInfo) error {
	s.block("append")
	return s.studyStorage.AppendLogs(l, term)
}
func (s *configBlockedStorage) GetLogs(a, b, c uint64) ([]types.Log, error) {
	s.block("read")
	s.block("verify")
	return s.studyStorage.GetLogs(a, b, c)
}
func (s *configBlockedStorage) Apply(l []types.Log) error {
	s.block("apply")
	return s.studyStorage.Apply(l)
}

func TestConfigShutdownBlockedStorage(t *testing.T) {
	for _, op := range []string{"append", "read", "apply", "verify"} {
		t.Run(op, func(t *testing.T) {
			s := &configBlockedStorage{operation: op, entered: make(chan struct{}), release: make(chan struct{})}
			s.terms = make(map[uint32]uint64)
			tr := &studyTransport{c: make(chan types.Event, 64)}
			r := New(NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithStorage(s), WithTransport(tr)))
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(s.release) }) }
			t.Cleanup(func() {
				release()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, r.WaitQuiescence(ctx))
			})
			want := types.Log{Id: 1400, Index: 1, Term: 1, Data: []byte("request-1400")}
			var out <-chan studyResult
			if op == "verify" {
				r.BecomeFollower(1, 2)
				s.logs = []types.Log{want}
				r.wait.appliedLogs([]types.Log{want})
				out = studyCall(r, context.Background(), 1400)
				studyRecv(t, tr.c)
				data, err := types.ProposeRespSet{&types.ProposeResp{Id: 1400, Index: 1}}.Marshal()
				require.NoError(t, err)
				r.Step(types.Event{Type: types.SendProposeResp, From: 2, Reason: types.ReasonOk, Logs: []types.Log{{Data: data}}})
			} else {
				out = studyCall(r, context.Background(), 1400)
				r.handleStep(studyRecv(t, r.stepC))
				switch op {
				case "append":
					r.handleStoreReq(types.Event{Logs: []types.Log{want}})
				case "read":
					r.handleGetLogsReq(types.Event{Index: 1, StoredIndex: 1, Reason: types.ReasonOnlySync})
				case "apply":
					s.logs = []types.Log{want}
					r.handleApplyReq(types.Event{StartIndex: 1, EndIndex: 2})
				}
			}
			studyRecv(t, s.entered)
			// Include a queued local caller and an in-flight forwarding registration.
			var queued <-chan studyResult
			if op != "verify" {
				queued = studyCall(r, context.Background(), 1401)
				studyRecv(t, r.stepC)
			}
			forwardRaft := New(NewOptions(WithNodeId(3), WithReplicas([]uint64{1, 2, 3}), WithStorage(&studyStorage{terms: make(map[uint32]uint64)}), WithTransport(tr)))
			forwardRaft.BecomeFollower(1, 2)
			forwarded := studyCall(forwardRaft, context.Background(), 1402)
			studyRecv(t, tr.c)
			stopped := make(chan struct{})
			go func() { r.Stop(); forwardRaft.Stop(); close(stopped) }()
			studyRecv(t, stopped)
			require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
			if queued != nil {
				require.ErrorIs(t, studyRecv(t, queued).err, types.ErrProposalNotAdmitted)
			}
			require.ErrorIs(t, studyRecv(t, forwarded).err, types.ErrStopped)
			require.Zero(t, studyCount(r))
			require.Empty(t, r.forwardPending)
			require.Empty(t, forwardRaft.forwardPending)
			require.NoError(t, forwardRaft.WaitQuiescence(context.Background()))
			require.ErrorIs(t, r.submitWorker(func() { t.Error("post-stop worker ran") }), types.ErrStopped)
			_, err := r.ProposeUntilAppliedTimeout(context.Background(), 1403, nil)
			require.ErrorIs(t, err, types.ErrStopped)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			err = r.WaitQuiescence(ctx)
			cancel()
			require.ErrorIs(t, err, ErrIncompleteQuiescence)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Positive(t, r.pool.Running(), "blocked I/O must not be reported as quiescent")
			counts := r.ProposalOutcomes()
			// Raft never owns Storage.Close. The owner releases I/O before resource close.
			release()
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, r.WaitQuiescence(ctx))
			require.NoError(t, r.WaitQuiescence(ctx))
			r.Stop()
			require.Zero(t, r.pool.Running())
			require.Zero(t, studyCount(r))
			require.Empty(t, r.forwardPending)
			require.Equal(t, counts, r.ProposalOutcomes(), "late worker completion must not count twice")
			t.Logf("%s: incomplete while blocked; eventual quiescence with zero workers; outcomes=%+v", op, counts)
		})
	}
}

func TestConfigStoppedCompatibility(t *testing.T) {
	for _, leader := range []bool{false, true} {
		for _, api := range []string{"propose", "batch", "applied", "batch-applied"} {
			t.Run(fmt.Sprintf("leader=%t/%s", leader, api), func(t *testing.T) {
				c := studyNew(t, false)
				r := c.rr[0]
				if !leader {
					r.BecomeFollower(1, 0)
				}
				r.Stop()
				var err error
				switch api {
				case "propose":
					_, err = r.Propose(1500, nil)
				case "batch":
					_, err = r.ProposeBatchTimeout(context.Background(), types.ProposeReqSet{{Id: 1500}})
				case "applied":
					_, err = r.ProposeUntilAppliedTimeout(context.Background(), 1500, nil)
				case "batch-applied":
					_, err = r.ProposeBatchUntilAppliedTimeout(context.Background(), types.ProposeReqSet{{Id: 1500}})
				}
				require.ErrorIs(t, err, types.ErrStopped)
				require.ErrorIs(t, err, types.ErrProposalNotAdmitted)
				require.Equal(t, ProposalOutcomes{NotAdmitted: 1}, r.ProposalOutcomes())
			})
		}
	}
	t.Run("non-stopped-follower", func(t *testing.T) {
		c := studyNew(t, false)
		r := c.rr[0]
		r.BecomeFollower(1, 0)
		_, err := r.ProposeUntilAppliedTimeout(context.Background(), 1500, nil)
		require.ErrorIs(t, err, types.ErrNotLeader)
		require.NotErrorIs(t, err, types.ErrStopped)
		require.Equal(t, ProposalOutcomes{RejectedOrSuperseded: 1}, r.ProposalOutcomes())
	})
}

func TestConfigOutcomeCounters(t *testing.T) {
	c := studyNew(t, false)
	r := c.rr[0]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.ProposeUntilAppliedTimeout(ctx, 1600, nil)
	require.ErrorIs(t, err, types.ErrProposalNotAdmitted)
	ctx, cancel = context.WithCancel(context.Background())
	out := studyCall(r, ctx, 1601)
	c.step(0, studyRecv(t, r.stepC))
	cancel()
	require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalOutcomeUnknown)
	c.settle(1)
	out = studyCall(r, context.Background(), 1602)
	c.step(0, studyRecv(t, r.stepC))
	c.settle(2)
	require.NoError(t, studyRecv(t, out).err)
	r.node.stopPropose = true
	out = studyCall(r, context.Background(), 1603)
	c.step(0, studyRecv(t, r.stepC))
	require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalDropped)
	require.Equal(t, ProposalOutcomes{OK: 1, NotAdmitted: 1, Unknown: 1, RejectedOrSuperseded: 1}, r.ProposalOutcomes())
	r.Stop()
	require.Equal(t, ProposalOutcomes{OK: 1, NotAdmitted: 1, Unknown: 1, RejectedOrSuperseded: 1}, r.ProposalOutcomes())
}

func TestConfigShutdownSaturatedWorkers(t *testing.T) {
	c := studyNew(t, false)
	r := c.rr[0]
	r.pool.Tune(1)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	require.NoError(t, r.submitWorker(func() { close(entered); <-release }))
	studyRecv(t, entered)
	require.Error(t, r.submitWorker(func() { t.Error("saturated work executed") }))
	out := studyCall(r, context.Background(), 1700)
	// The queue contains a pending request when production Stop drains it.
	deadline := time.After(5 * time.Second)
	for len(r.stepC) == 0 {
		select {
		case <-deadline:
			t.Fatal("queue barrier not reached")
		default:
			runtime.Gosched()
		}
	}
	r.Stop()
	require.ErrorIs(t, studyRecv(t, out).err, types.ErrProposalNotAdmitted)
	require.Empty(t, r.stepC)
	require.ErrorIs(t, r.Start(), types.ErrStopped)
	once.Do(func() { close(release) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, r.WaitQuiescence(ctx))
	require.Zero(t, r.pool.Running())
	require.Zero(t, studyCount(r))
}
