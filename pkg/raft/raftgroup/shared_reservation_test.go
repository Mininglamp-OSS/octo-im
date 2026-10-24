package raftgroup

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type reservationTransport struct{ sent chan types.Event }

func (tr reservationTransport) Send(_ string, e types.Event) { tr.sent <- e }

func TestAdmissionSharedMetadataReservation(t *testing.T) {
	tr := reservationTransport{sent: make(chan types.Event, 10)}
	g := newAdmissionGroup(t, []uint64{1}, false, WithTransport(tr))
	require.NoError(t, g.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	before, err := g.ReadLeaderState(ctx, admKey)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	// Always release validation before fixture cleanup, even after an assertion.
	defer close(release)
	metadata := make(chan error, 1)
	go func() {
		_, err := g.ProposeAfterRead(ctx, admKey, before, 100, []byte("metadata"), func() error {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
		metadata <- err
	}()
	reservationRecv(t, entered)
	require.Equal(t, 1, len(g.admissionSem(admKey)))
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer blockedCancel()
	_, err = g.ProposeBatchTimeout(blockedCtx, admKey, types.ProposeReqSet{{Id: 101}})
	requireNotAdmitted(t, err, context.DeadlineExceeded)
	_, err = g.ProposeAfterRead(blockedCtx, admKey, before, 102, nil, func() error { t.Error("cancelled metadata validated"); return nil })
	require.ErrorIs(t, err, context.DeadlineExceeded)
	data, err := (types.ProposeReqSet{{Id: 103}}).Marshal()
	require.NoError(t, err)
	forwarded := make(chan struct{})
	go func() {
		g.handleSendPropose(admKey, types.Event{From: 2, Logs: []types.Log{{Data: data}}})
		close(forwarded)
	}()
	require.NoError(t, g.Do(ctx, admKey, func(r IRaft) error {
		require.Zero(t, r.LastLogIndex(), "local and forwarded ingress cannot extend reserved prefix")
		return nil
	}))
	// The forwarded ingress uses proposeBatchTimeout and cannot acquire a
	// second token. Resume validation and verify ordered admission and cleanup.
	release <- struct{}{}
	require.NoError(t, reservationRecv(t, metadata))
	reservationRecv(t, forwarded)
	response := reservationRecv(t, tr.sent)
	require.Equal(t, types.SendProposeResp, response.Type)
	var resps types.ProposeRespSet
	require.NoError(t, resps.Unmarshal(response.Logs[0].Data))
	require.Equal(t, uint64(2), resps[0].Index)
	require.Equal(t, uint64(103), resps[0].Id)
	require.Equal(t, []uint64{100, 103}, g.storage.waitApplied(t, 2))
	require.Zero(t, len(g.admissionSem(admKey)))
	require.Zero(t, g.wait.pending())
}

func reservationRecv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("reservation barrier timed out")
		var zero T
		return zero
	}
}
