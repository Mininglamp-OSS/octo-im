package slot

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestLeaderChangeVerifiesDurablePrefixBeforeApply(t *testing.T) {
	var applied []types.Log
	s := &Server{opts: &Options{OnApply: func(_ uint32, logs []types.Log) error {
		applied = append(applied, logs...)
		return nil
	}}}
	storage := NewPebbleShardLogStorage(s, t.TempDir(), 1)
	require.NoError(t, storage.Open())
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	old := []types.Log{{Index: 1, Term: 1, Data: []byte("committed")},
		{Index: 2, Term: 2, Data: []byte("old-2")}, {Index: 3, Term: 2, Data: []byte("old-3")},
		{Index: 4, Term: 2, Data: []byte("old-4")}}
	require.NoError(t, storage.AppendLogs("1", old, &types.TermStartIndexInfo{Term: 2, Index: 2}))
	require.NoError(t, storage.SetAppliedIndex("1", 1))
	state, err := storage.GetState("1")
	require.NoError(t, err)
	n := raft.NewNode(2, state, raft.NewOptions(raft.WithNodeId(2), raft.WithAdvance(func() {})))
	require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{
		Term: 3, Leader: 1, Replicas: []uint64{1, 2, 3}, Role: types.RoleFollower}}))
	n.Ready()
	require.NoError(t, n.Step(types.Event{Type: types.Ping, From: 1, Term: 3, Index: 4, CommittedIndex: 4}))
	applyReady := func() {
		for _, e := range n.Ready() {
			switch e.Type {
			case types.StoreReq:
				require.NoError(t, storage.AppendLogs("1", e.Logs, e.TermStartIndexInfo))
				require.NoError(t, n.Step(types.Event{Type: types.StoreResp, Index: e.Logs[len(e.Logs)-1].Index, Reason: types.ReasonOk}))
			case types.ApplyReq:
				logs, err := storage.GetLogs("1", e.StartIndex, e.EndIndex, 0)
				require.NoError(t, err)
				require.NotEmpty(t, logs)
				require.NoError(t, storage.Apply("1", logs))
				require.NoError(t, n.Step(types.Event{Type: types.ApplyResp, Index: logs[len(logs)-1].Index, Reason: types.ReasonOk}))
			}
		}
	}
	applyReady()
	require.Empty(t, applied, "a new leader's heartbeat must not materialize the old conflicting suffix")
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 3, Index: 1, Reason: types.ReasonTruncate}))
	n.Ready()
	require.NoError(t, storage.TruncateLogTo("1", 1))
	require.NoError(t, n.Step(types.Event{Type: types.TruncateResp, Index: 1, Reason: types.ReasonOk}))
	n.Ready()
	newLogs := []types.Log{{Index: 2, Term: 3, Data: []byte("new-2")},
		{Index: 3, Term: 3, Data: []byte("new-3")}, {Index: 4, Term: 3, Data: []byte("new-4")}}
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 3,
		Index: 2, CommittedIndex: 4, Logs: newLogs, Reason: types.ReasonOk}))
	applyReady()
	require.NoError(t, n.Step(types.Event{Type: types.SyncResp, From: 1, Term: 3,
		Index: 5, CommittedIndex: 4, Reason: types.ReasonOk}))
	applyReady()
	require.Len(t, applied, len(newLogs))
	for i := range applied {
		require.Equal(t, newLogs[i].Index, applied[i].Index)
		require.Equal(t, newLogs[i].Term, applied[i].Term)
		require.Equal(t, newLogs[i].Data, applied[i].Data)
	}
	got, err := storage.GetLogs("1", 2, 5, 0)
	require.NoError(t, err)
	for i := range got {
		require.Equal(t, newLogs[i].Term, got[i].Term)
		require.Equal(t, newLogs[i].Data, got[i].Data)
	}
	index, err := storage.AppliedIndex("1")
	require.NoError(t, err)
	require.Equal(t, uint64(4), index)
}

func TestTruncateRejectsAppliedPrefix(t *testing.T) {
	s := &Server{opts: &Options{}}
	storage := NewPebbleShardLogStorage(s, t.TempDir(), 1)
	require.NoError(t, storage.Open())
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	require.NoError(t, storage.AppendLogs("1", []types.Log{{Index: 1, Term: 1}, {Index: 2, Term: 1}}, nil))
	require.NoError(t, storage.SetAppliedIndex("1", 2))
	require.Error(t, storage.TruncateLogTo("1", 1), "a refused truncation must never be acknowledged as successful")
	last, err := storage.LastIndex("1")
	require.NoError(t, err)
	require.Equal(t, uint64(2), last)
}
