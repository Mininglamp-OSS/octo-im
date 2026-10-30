package clusterconfig

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestMultiTermConfigAppendPersistsEveryTermBoundary(t *testing.T) {
	dir := t.TempDir()
	s := NewPebbleShardLogStorage(dir, nil)
	require.NoError(t, s.Open())
	logs := []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}, {Index: 4, Term: 3}}
	require.NoError(t, s.AppendLogs(logs, &types.TermStartIndexInfo{Term: 1, Index: 1}))
	require.NoError(t, s.Close())
	s = NewPebbleShardLogStorage(dir, nil)
	require.NoError(t, s.Open())
	defer func() { require.NoError(t, s.Close()) }()
	for term, expected := range map[uint32]uint64{1: 1, 2: 2, 3: 4} {
		got, err := s.GetTermStartIndex(term)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	}
	require.NoError(t, s.AppendLogs([]types.Log{{Index: 5, Term: 3}}, nil))
	got, err := s.GetTermStartIndex(3)
	require.NoError(t, err)
	require.Equal(t, uint64(4), got)
}
