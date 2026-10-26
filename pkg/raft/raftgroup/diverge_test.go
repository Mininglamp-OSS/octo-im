package raftgroup

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// refusingStorage refuses truncations or appends the way slot storage does.
type refusingStorage struct {
	*memStorage
	truncateErr  error
	appendErr    error
	markErr      error
	marks        atomic.Int32
	termDeletes  atomic.Int32
	markerCauses chan error
}

func (s *refusingStorage) TruncateLogTo(key string, index uint64) error {
	if s.truncateErr != nil {
		return s.truncateErr
	}
	return s.memStorage.TruncateLogTo(key, index)
}

func (s *refusingStorage) AppendLogs(key string, logs []types.Log, ts *types.TermStartIndexInfo) error {
	if s.appendErr != nil && len(s.memStorage.logs) > 0 {
		return s.appendErr
	}
	return s.memStorage.AppendLogs(key, logs, ts)
}

func (s *refusingStorage) DeleteLeaderTermStartIndexGreaterThanTerm(string, uint32) error {
	s.termDeletes.Add(1)
	return nil
}

func (s *refusingStorage) MarkDiverged(_ string, cause error) error {
	s.marks.Add(1)
	select {
	case s.markerCauses <- cause:
	default:
	}
	return s.markErr
}

// noMarkerStorage refuses but cannot persist a marker.
type noMarkerStorage struct{ *memStorage }

func (s *noMarkerStorage) TruncateLogTo(string, uint64) error {
	return fmt.Errorf("%w: test", types.ErrTruncateBelowApplied)
}

func startFollowerGroup(t *testing.T, storage IStorage) (*RaftGroup, *raft.Node, *recordingTransport) {
	t.Helper()
	tr := &recordingTransport{events: make(chan types.Event, 256)}
	g := New(NewOptions(WithTransport(tr), WithStorage(storage), WithTickInterval(time.Hour)))
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(admKey), raft.WithNodeId(1), raft.WithReplicas([]uint64{1, 2, 3})))
	node.BecomeFollower(2, 2)
	g.AddRaft(node)
	require.NoError(t, g.Start())
	t.Cleanup(func() {
		g.Stop()
		g.goPool.Release()
	})
	return g, node, tr
}

func driveUntil(t *testing.T, g *RaftGroup, cond func() bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		g.Advance()
		return cond()
	}, 5*time.Second, time.Millisecond)
}

// T4: a typed truncation refusal persists the marker, never deletes newer
// term start indexes, and quarantines the node through ReasonDiverged.
func TestTruncateRefusalMarksAndQuarantines(t *testing.T) {
	for _, markErr := range []error{nil, errors.New("injected marker failure")} {
		t.Run(fmt.Sprintf("markErr=%v", markErr), func(t *testing.T) {
			storage := &refusingStorage{
				memStorage:   &memStorage{},
				truncateErr:  fmt.Errorf("%w: slot test", types.ErrTruncateBelowApplied),
				markErr:      markErr,
				markerCauses: make(chan error, 1),
			}
			g, node, _ := startFollowerGroup(t, storage)
			g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 2, From: 2, To: 1, Reason: types.ReasonTruncate, Index: 3})
			driveUntil(t, g, node.Diverged)
			require.EqualValues(t, 1, storage.marks.Load())
			require.ErrorIs(t, <-storage.markerCauses, types.ErrTruncateBelowApplied)
			require.Zero(t, storage.termDeletes.Load(), "no half truncation")
			require.Equal(t, markErr != nil, node.FailStopped())
		})
	}
}

// Storage that cannot persist a marker fail-stops the replica.
func TestTruncateRefusalWithoutMarkerFailStops(t *testing.T) {
	g, node, _ := startFollowerGroup(t, &noMarkerStorage{memStorage: &memStorage{}})
	g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 2, From: 2, To: 1, Reason: types.ReasonTruncate, Index: 3})
	driveUntil(t, g, node.FailStopped)
}

// A typed append refusal quarantines instead of panicking the loop.
func TestAppendRefusalQuarantinesWithoutPanic(t *testing.T) {
	storage := &refusingStorage{
		memStorage:   &memStorage{logs: []types.Log{{Index: 1, Term: 1, Id: 1}}},
		appendErr:    fmt.Errorf("%w: slot test", types.ErrAppendBelowApplied),
		markerCauses: make(chan error, 1),
	}
	g, node, _ := startFollowerGroup(t, storage)
	g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 2, From: 2, To: 1, Reason: types.ReasonOk, Logs: []types.Log{{Index: 1, Term: 2, Id: 9, Data: []byte("x")}}})
	driveUntil(t, g, node.Diverged)
	require.ErrorIs(t, <-storage.markerCauses, types.ErrAppendBelowApplied)
}

// T26: an ordinary storage error keeps the existing ReasonError path and
// never writes a marker.
func TestOrdinaryTruncateErrorDoesNotMark(t *testing.T) {
	storage := &refusingStorage{
		memStorage:   &memStorage{},
		truncateErr:  errors.New("pebble: I/O error"),
		markerCauses: make(chan error, 1),
	}
	g, node, tr := startFollowerGroup(t, storage)
	g.AddEvent(admKey, types.Event{Type: types.SyncResp, Term: 2, From: 2, To: 1, Reason: types.ReasonTruncate, Index: 3})
	// The failed truncation is retried by the leader; meanwhile no marker.
	deadline := time.After(300 * time.Millisecond)
loop:
	for {
		g.Advance()
		select {
		case <-tr.events:
		case <-deadline:
			break loop
		case <-time.After(time.Millisecond):
		}
	}
	require.Zero(t, storage.marks.Load())
	require.False(t, node.Diverged())
}
