package clusterconfig

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestConfigLogTruncateRejectsAppliedPrefix(t *testing.T) {
	s := NewPebbleShardLogStorage(t.TempDir(), nil)
	require.NoError(t, s.Open())
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	require.NoError(t, s.AppendLogs([]types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, nil))
	require.NoError(t, s.setAppliedIndex(2))
	require.Error(t, s.TruncateLogTo(1))
	last, _, err := s.LastIndexAndTerm()
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)
}
