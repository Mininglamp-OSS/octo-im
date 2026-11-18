package raft

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type yp107Transport struct {
	mu     sync.Mutex
	events []types.Event
}

func (t *yp107Transport) Send(e types.Event) {
	t.mu.Lock()
	t.events = append(t.events, e)
	t.mu.Unlock()
}

func (t *yp107Transport) snapshot() []types.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]types.Event, len(t.events))
	copy(out, t.events)
	return out
}

type yp107Storage struct {
	applyRetryStorage
	blockStore chan struct{}
	stored     atomic.Int32
}

func (s *yp107Storage) SaveHardState(types.HardState) error { return nil }
func (s *yp107Storage) GetLogs(start, end, _ uint64) ([]types.Log, error) {
	var logs []types.Log
	for idx := start; idx < end; idx++ {
		logs = append(logs, types.Log{Index: idx, Term: 1})
	}
	return logs, nil
}
func (s *yp107Storage) AppendLogs(logs []types.Log, _ *types.TermStartIndexInfo) error {
	if s.blockStore != nil {
		<-s.blockStore
	}
	s.stored.Add(int32(len(logs)))
	return nil
}
func (s *yp107Storage) Apply([]types.Log) error { return nil }

func TestXIN2857_YP107_MalformedProposeValidationAndBoundedIndependentAdmission(t *testing.T) {
	tr := &yp107Transport{}
	st := &yp107Storage{}
	r := New(NewOptions(
		WithNodeId(1),
		WithReplicas([]uint64{1}),
		WithStorage(st),
		WithTransport(tr),
		WithGoPoolSize(2),
		WithTickInterval(5*time.Millisecond),
	))
	defer r.Stop()
	defer r.pool.Release()

	// 1. Defensive validation: empty Logs, empty Data, corrupt Data, and empty decoded slice
	// on both SendPropose and SendProposeResp must NEVER panic and must NOT emit an empty SendProposeResp.
	emptyReqs, err := (types.ProposeReqSet{}).Marshal()
	require.NoError(t, err)
	emptyResps, err := (types.ProposeRespSet{}).Marshal()
	require.NoError(t, err)

	require.NotPanics(t, func() {
		r.handleSendPropose(types.Event{From: 2, Type: types.SendPropose})
		r.handleSendPropose(types.Event{From: 2, Type: types.SendPropose, Logs: []types.Log{{Data: nil}}})
		r.handleSendPropose(types.Event{From: 2, Type: types.SendPropose, Logs: []types.Log{{Data: []byte{0xff, 0x01}}}})
		r.handleSendPropose(types.Event{From: 2, Type: types.SendPropose, Logs: []types.Log{{Data: emptyReqs}}})

		r.handleSendProposeResp(types.Event{From: 2, Type: types.SendProposeResp})
		r.handleSendProposeResp(types.Event{From: 2, Type: types.SendProposeResp, Logs: []types.Log{{Data: nil}}})
		r.handleSendProposeResp(types.Event{From: 2, Type: types.SendProposeResp, Logs: []types.Log{{Data: []byte{0xff, 0x01}}}})
		r.handleSendProposeResp(types.Event{From: 2, Type: types.SendProposeResp, Logs: []types.Log{{Data: emptyResps}}})
	})
	require.Empty(t, tr.snapshot(), "Y-P1-07: malformed or empty SendPropose must not emit an empty SendProposeResp")

	// 2. Bounded, independent in-flight SendPropose admission that never starves r.pool (GoPoolSize=1) storage progress:
	r.proposeSem = make(chan struct{}, 2)
	st.blockStore = make(chan struct{})
	r.BecomeLeader(1)
	require.NoError(t, r.Start())

	mkSendPropose := func(id uint64) types.Event {
		raw, mErr := (types.ProposeReqSet{{Id: id, Data: []byte("d")}}).Marshal()
		require.NoError(t, mErr)
		return types.Event{
			From: 2,
			To:   1,
			Type: types.SendPropose,
			Logs: []types.Log{{Data: raw}},
		}
	}

	// Admit 2 proposals (filling r.proposeSem capacity=2) while storage is paused:
	r.Step(mkSendPropose(101))
	r.Step(mkSendPropose(102))
	require.Eventually(t, func() bool {
		return len(r.proposeSem) == 2
	}, time.Second, 2*time.Millisecond)

	// 3rd proposal exceeds proposeSem capacity (2) -> immediately rejected with ReasonError without spawning a 3rd goroutine:
	r.Step(mkSendPropose(103))
	require.Eventually(t, func() bool {
		for _, ev := range tr.snapshot() {
			if ev.Type == types.SendProposeResp && ev.Reason == types.ReasonError {
				var rs types.ProposeRespSet
				if rs.Unmarshal(ev.Logs[0].Data) == nil && len(rs) == 1 && rs[0].Id == 103 {
					return true
				}
			}
		}
		return false
	}, time.Second, 2*time.Millisecond, "Y-P1-07: 3rd SendPropose exceeding proposeSem must fail fast with ReasonError")

	// Unblock storage -> even with GoPoolSize=1, r.pool is NOT starved by the 2 waiting SendPropose workers,
	// so handleStoreReq completes both admitted proposals!
	close(st.blockStore)
	require.Eventually(t, func() bool {
		okCount := 0
		for _, ev := range tr.snapshot() {
			if ev.Type == types.SendProposeResp && ev.Reason == types.ReasonOk {
				okCount++
			}
		}
		return okCount == 2
	}, 2*time.Second, 5*time.Millisecond, "Y-P1-07: admitted SendPropose requests must complete storage and apply without pool deadlock")
}

