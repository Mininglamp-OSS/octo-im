package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestApplyRetryIsTickPacedAndRecoversAfterCircuitOpens(t *testing.T) {
	n := NewNode(1, types.RaftState{LastLogIndex: 1, LastTerm: 1}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1}), WithApplyErrorRetry(true)))
	n.queue.committedIndex = 1
	attempts := 0
	for tick := 0; tick <= 640; tick++ {
		for spin := 0; spin < 100; spin++ {
			for _, e := range n.Ready() {
				if e.Type != types.ApplyReq {
					continue
				}
				attempts++
				require.Equal(t, uint64(1), e.StartIndex)
				require.NoError(t, n.Step(types.Event{Type: types.ApplyResp, Reason: types.ReasonError}))
			}
		}
		if tick == 127 {
			require.Equal(t, 8, attempts, "eight attempts maximum before opening the circuit")
			// Neither term nor role changes may bypass storage backoff.
			n.BecomeFollower(2, 2)
			n.BecomeLeader(3)
		}
		if tick == 382 {
			require.Equal(t, 8, attempts, "no retry before the 256-tick cooldown ends")
		}
		n.Tick()
	}
	require.Equal(t, 10, attempts, "persistent errors permit just one probe per cooldown")
	require.Zero(t, n.AppliedIndex(), "failed application must never advance the marker")
	// Repair storage: the next scheduled probe succeeds, and subsequent work
	// must become ready immediately (backoff is per failure streak).
	for i := 0; i < 256; i++ {
		n.Tick()
	}
	found := false
	for _, e := range n.Ready() {
		if e.Type == types.ApplyReq {
			found = true
			require.NoError(t, n.Step(types.Event{Type: types.ApplyResp, Reason: types.ReasonOk, Index: 1}))
		}
	}
	require.True(t, found)
	require.Equal(t, uint64(1), n.AppliedIndex())
	n.queue.lastLogIndex, n.queue.storedIndex, n.queue.committedIndex = 2, 2, 2
	require.True(t, n.HasReady())
	found = false
	for _, e := range n.Ready() {
		if e.Type == types.ApplyReq {
			found = true
			require.Equal(t, uint64(2), e.StartIndex)
		}
	}
	require.True(t, found)
}

func TestApplyBackoffDoesNotDelayHeartbeat(t *testing.T) {
	n := NewNode(1, types.RaftState{LastLogIndex: 1, LastTerm: 1}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1, 2}), WithElectionOn(true), WithApplyErrorRetry(true)))
	n.BecomeLeader(1)
	n.queue.committedIndex = 1
	n.Ready()
	for i := 0; i < 8; i++ {
		require.NoError(t, n.Step(types.Event{Type: types.ApplyResp, Reason: types.ReasonError}))
	}
	for i := 0; i < n.opts.HeartbeatInterval; i++ {
		n.Tick()
	}
	ping := false
	for _, e := range n.Ready() {
		require.NotEqual(t, types.ApplyReq, e.Type)
		ping = ping || e.Type == types.Ping
	}
	require.True(t, ping, "the raft loop must serve heartbeats during storage cooldown")
}

func TestApplyErrorRetryIsDisabledByDefault(t *testing.T) {
	n := NewNode(1, types.RaftState{LastLogIndex: 1, LastTerm: 1}, NewOptions(WithNodeId(1), WithReplicas([]uint64{1})))
	n.queue.committedIndex = 1
	require.Len(t, n.Ready(), 1)
	require.NoError(t, n.Step(types.Event{Type: types.ApplyResp, Reason: types.ReasonError}))
	require.Zero(t, n.applyRetryTicks)
	require.Zero(t, n.applyFailures)
	require.True(t, n.HasReady(), "default behavior retries immediately")
}
