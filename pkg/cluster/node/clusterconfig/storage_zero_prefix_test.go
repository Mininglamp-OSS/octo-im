package clusterconfig

import (
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConfigTruncatesOnlyUnappliedLogToZero(t *testing.T) {
	p := NewPebbleShardLogStorage(t.TempDir(), nil)
	require.NoError(t, p.Open())
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	logs := []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	require.NoError(t, p.AppendLogs(logs, nil))
	require.NoError(t, p.TruncateLogTo(0))
	state, err := p.GetState()
	require.NoError(t, err)
	require.Zero(t, state.LastLogIndex)
	require.NoError(t, p.AppendLogs(logs, nil))
	require.NoError(t, p.setAppliedIndex(2))
	require.ErrorIs(t, p.TruncateLogTo(1), types.ErrCommittedLogConflict)
	require.ErrorIs(t, p.TruncateLogTo(0), types.ErrCommittedLogConflict)
	state, err = p.GetState()
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.LastLogIndex)
}
