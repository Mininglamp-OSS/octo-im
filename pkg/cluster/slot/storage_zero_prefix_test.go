package slot

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSlotTruncatesOnlyUnappliedLogToZero(t *testing.T) {
	p := newPrefixStorage(t, func(uint32, []types.Log) error { return nil })
	logs := []types.Log{{Index: 1, Term: 1}}
	require.NoError(t, p.AppendLogs("59", logs, nil))
	require.NoError(t, p.TruncateLogTo("59", 0))
	state, err := p.GetState("59")
	require.NoError(t, err)
	require.Zero(t, state.LastLogIndex)
	require.NoError(t, p.AppendLogs("59", logs, nil))
	require.NoError(t, p.Apply("59", logs))
	require.ErrorIs(t, p.TruncateLogTo("59", 0), types.ErrCommittedLogConflict)
	state, err = p.GetState("59")
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.LastLogIndex)
}
