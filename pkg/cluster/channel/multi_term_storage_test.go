package channel

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func TestMultiTermChannelAppendPersistsEveryTermBoundary(t *testing.T) {
	dir := t.TempDir()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(2)))
	require.NoError(t, db.Open())
	s := newStorage(db, nil)
	key := wkutil.ChannelToKey("history", 2)
	var logs []types.Log
	for i, term := range []uint32{1, 2, 2, 3, 3} {
		msg := wkdb.Message{}
		msg.MessageID, msg.MessageSeq, msg.Term = int64(i+1), uint32(i+1), uint64(term)
		msg.ChannelID, msg.ChannelType, msg.Payload = "history", 2, []byte("history")
		data, err := msg.Marshal()
		require.NoError(t, err)
		logs = append(logs, types.Log{Index: uint64(i + 1), Term: term, Data: data})
	}
	require.NoError(t, s.AppendLogs(key, logs[:4], &types.TermStartIndexInfo{Term: 1, Index: 1}))
	require.NoError(t, db.Close())
	db = wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(2)))
	require.NoError(t, db.Open())
	defer func() { require.NoError(t, db.Close()) }()
	s = newStorage(db, nil)
	for term, expected := range map[uint32]uint64{1: 1, 2: 2, 3: 4} {
		got, err := s.GetTermStartIndex(key, term)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	}
	require.NoError(t, s.AppendLogs(key, logs[4:], nil))
	got, err := s.GetTermStartIndex(key, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(4), got)
}
