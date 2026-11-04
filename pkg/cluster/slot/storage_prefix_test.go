package slot

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func newPrefixStorage(t *testing.T, apply func(uint32, []types.Log) error) *PebbleShardLogStorage {
	t.Helper()
	p := NewPebbleShardLogStorage(&Server{opts: &Options{OnApply: apply}}, t.TempDir(), 1)
	require.NoError(t, p.Open())
	t.Cleanup(func() { require.NoError(t, p.Close()) })
	return p
}

func TestSlotRejectsTruncationAndOverwriteOfAppliedPrefix(t *testing.T) {
	p := newPrefixStorage(t, func(uint32, []types.Log) error { return nil })
	logs := []types.Log{{Index: 1, Term: 1, Data: []byte("prefix")}, {Index: 2, Term: 1, Data: []byte("applied")}}
	require.NoError(t, p.AppendLogs("59", logs, nil))
	require.NoError(t, p.Apply("59", logs))
	require.Error(t, p.TruncateLogTo("59", 1), "refused truncation must not report success to Raft")
	require.Error(t, p.AppendLogs("59", []types.Log{{Index: 2, Term: 2, Data: []byte("conflicting")}}, nil))
	actual, err := p.GetLogs("59", 2, 3, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("applied"), actual[0].Data)
}

func TestSlotRejectsApplyReadBeforeTailReplacement(t *testing.T) {
	var applied []string
	p := newPrefixStorage(t, func(_ uint32, logs []types.Log) error {
		for _, log := range logs {
			applied = append(applied, string(log.Data))
		}
		return nil
	})
	prefix := types.Log{Index: 1, Term: 1, Data: []byte("prefix")}
	require.NoError(t, p.AppendLogs("59", []types.Log{prefix, {Index: 2, Term: 1, Data: []byte("obsolete")}}, nil))
	require.NoError(t, p.Apply("59", []types.Log{prefix}))
	old, err := p.GetLogs("59", 2, 3, 0)
	require.NoError(t, err)
	require.NoError(t, p.TruncateLogTo("59", 1))
	canonical := []types.Log{{Index: 2, Term: 2, Data: []byte("canonical")}}
	require.NoError(t, p.AppendLogs("59", canonical, nil))
	require.Error(t, p.Apply("59", old), "an old worker must not materialize replaced data")
	require.Equal(t, []string{"prefix"}, applied)
	index, err := p.AppliedIndex("59")
	require.NoError(t, err)
	require.Equal(t, uint64(1), index)
	require.NoError(t, p.Apply("59", canonical))
	require.Equal(t, []string{"prefix", "canonical"}, applied)
}

func TestSlotRejectsAppliedMarkerAheadOfLog(t *testing.T) {
	p := newPrefixStorage(t, func(uint32, []types.Log) error { return nil })
	require.NoError(t, p.AppendLogs("59", []types.Log{{Index: 1, Term: 1}}, nil))
	require.NoError(t, p.SetAppliedIndex("59", 2))
	_, err := p.GetState("59")
	require.Error(t, err, "changing an index cannot undo already materialized state")
	index, err := p.AppliedIndex("59")
	require.NoError(t, err)
	require.Equal(t, uint64(2), index)
}

func TestSlotApplyReplaysOnlyUnappliedContiguousSuffix(t *testing.T) {
	var seen []uint64
	p := newPrefixStorage(t, func(_ uint32, logs []types.Log) error {
		for _, log := range logs {
			seen = append(seen, log.Index)
		}
		return nil
	})
	logs := []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}, {Index: 3, Term: 1}}
	require.NoError(t, p.AppendLogs("59", logs, nil))
	require.ErrorIs(t, p.Apply("59", logs[1:]), types.ErrLogIndexNotContinuous)
	require.Empty(t, seen)
	require.NoError(t, p.Apply("59", logs[:1]))
	require.ErrorIs(t, p.Apply("59", []types.Log{logs[0], logs[2]}), types.ErrLogIndexNotContinuous)
	require.NoError(t, p.Apply("59", logs))
	require.NoError(t, p.Apply("59", logs))
	require.Equal(t, []uint64{1, 2, 3}, seen, "a replay must not apply older commands over newer state")
}
