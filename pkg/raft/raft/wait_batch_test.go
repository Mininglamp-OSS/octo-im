package raft

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

// largeBatch is far beyond any real admitted batch or receipt backlog. Linear
// work on it takes milliseconds; the former nested scan and per-completion
// compaction take on the order of n² = 4e10 steps, which no budget below
// tolerates, so this guards against restoring them.
const (
	largeBatch       = 200_000
	largeBatchBudget = 2 * time.Second
)

func batchLogs(n int, term uint32) []types.Log {
	logs := make([]types.Log, n)
	for i := range logs {
		logs[i] = types.Log{Index: uint64(i + 1), Id: uint64(i + 1), Term: term, Data: []byte{byte(i)}}
	}
	return logs
}

func TestVerifyLogsLargeBatchLinear(t *testing.T) {
	want := batchLogs(largeBatch, 3)
	got := batchLogs(largeBatch+10, 3) // a wider stored range around the batch
	started := time.Now()
	require.NoError(t, verifyLogs(want, got))
	elapsed := time.Since(started)
	require.Less(t, elapsed, largeBatchBudget)
	t.Logf("verified %d entries in %s", largeBatch, elapsed)

	// Every entry is still checked exactly: id, term and bytes.
	for _, mutate := range []func(*types.Log){
		func(l *types.Log) { l.Id++ },
		func(l *types.Log) { l.Term++ },
		func(l *types.Log) { l.Data = []byte("other") },
	} {
		changed := append([]types.Log(nil), got...)
		mutate(&changed[largeBatch-1])
		require.ErrorIs(t, verifyLogs(want, changed), types.ErrProposalSuperseded)
	}
	missing := append(append([]types.Log(nil), got[:largeBatch/2]...), got[largeBatch/2+1:]...)
	err := verifyLogs(want, missing)
	require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
	require.NotErrorIs(t, err, types.ErrProposalSuperseded)
	require.ErrorIs(t, verifyLogs(want, got[1:]), types.ErrProposalOutcomeUnknown)
	require.ErrorIs(t, verifyLogs(want, nil), types.ErrProposalOutcomeUnknown)
	// Unordered input cannot yield a false success.
	reversed := append([]types.Log(nil), want[:3]...)
	reversed[0], reversed[2] = reversed[2], reversed[0]
	require.ErrorIs(t, verifyLogs(want[:3], reversed), types.ErrProposalOutcomeUnknown)
}

// A batch of completions (fence, stop, apply) compacts the backlog once.
func TestWaitLargeBacklogCompletesLinear(t *testing.T) {
	for _, mode := range []string{"fence", "stop", "apply", "fenceForward"} {
		t.Run(mode, func(t *testing.T) {
			m := newWait("batch")
			applied := batchLogs(largeBatch, 2)
			ps := make([]*progress, largeBatch)
			for i := range ps {
				ps[i] = m.registerReceipt(applied[i:i+1], mode == "fenceForward")
			}
			// One untouched receipt above the completed range stays pending.
			keep := m.registerReceipt([]types.Log{{Index: largeBatch + 5, Id: 1, Term: 9}}, false)
			started := time.Now()
			switch mode {
			case "fence":
				m.fence(2)
			case "stop":
				m.stop()
			case "apply":
				m.appliedLogs(applied)
			case "fenceForward":
				m.fenceForward()
			}
			elapsed := time.Since(started)
			require.Less(t, elapsed, largeBatchBudget)
			t.Logf("%s completed %d receipts in %s", mode, largeBatch, elapsed)
			for _, p := range ps {
				require.True(t, p.done)
				select {
				case <-p.waitC:
				default:
					t.Fatal("completed receipt was not published")
				}
			}
			if mode == "apply" {
				for _, p := range ps {
					require.NoError(t, p.err)
				}
			} else {
				require.ErrorIs(t, ps[0].err, types.ErrProposalOutcomeUnknown)
			}
			if mode == "stop" {
				require.Empty(t, m.progresses)
				require.True(t, keep.done)
				return
			}
			require.Len(t, m.progresses, 1)
			require.Same(t, keep, m.progresses[0])
			require.False(t, keep.done)
		})
	}
}
