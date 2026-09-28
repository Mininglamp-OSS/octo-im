package store

import (
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type metadataProgressDB struct {
	wkdb.DB
	indexes []uint64
	fail    uint64
}

func (d *metadataProgressDB) SetSlotAppliedIndex(slot uint32, index uint64) error {
	d.indexes = append(d.indexes, index)
	if index == d.fail {
		return errors.New("injected metadata checkpoint failure")
	}
	return d.DB.SetSlotAppliedIndex(slot, index)
}
func metadataLog(t *testing.T, index uint64, info wkdb.ChannelInfo) types.Log {
	t.Helper()
	data, err := EncodeChannelInfo(info, CmdVersionChannelInfo)
	require.NoError(t, err)
	encoded, err := NewCMDWithVersion(CMDUpdateChannelInfo, data, CmdVersionChannelInfo).Marshal()
	require.NoError(t, err)
	return types.Log{Index: index, Data: encoded}
}

func TestMetadataBatchDoesNotMoveUpdatesPastChannelDeletion(t *testing.T) {
	s, _ := recoveryStore(t)
	db := &metadataProgressDB{DB: s.wdb}
	s.wdb = db
	at := time.Unix(100, 0)
	info := wkdb.ChannelInfo{ChannelId: "metadata-order", ChannelType: 2, CreatedAt: &at, UpdatedAt: &at}
	_, err := db.AddChannel(info)
	require.NoError(t, err)
	info.Ban = true
	first := metadataLog(t, 1, info)
	info.Disband = true
	second := metadataLog(t, 2, info)
	// Neither metadata update may be deferred across the channel deletion.
	logs := []types.Log{first, second, applyTestLog(t, 3, CMDDeleteChannel, EncodeChannel(info.ChannelId, 2))}
	require.NoError(t, s.ApplySlotLogs(0, logs))
	require.Equal(t, []uint64{2, 3}, db.indexes)
	got, err := db.GetChannel(info.ChannelId, 2)
	require.NoError(t, err)
	require.True(t, wkdb.IsEmptyChannelInfo(got))
	require.NoError(t, s.ApplySlotLogs(0, logs))
	got, err = db.GetChannel(info.ChannelId, 2)
	require.NoError(t, err)
	require.True(t, wkdb.IsEmptyChannelInfo(got), "replay must not resurrect the deleted channel")
}

func TestMetadataBatchCheckpointFailureReplaysFinalState(t *testing.T) {
	s, _ := recoveryStore(t)
	db := s.wdb
	wrapped := &metadataProgressDB{DB: db, fail: 2}
	s.wdb = wrapped
	info := wkdb.ChannelInfo{ChannelId: "metadata-replay", ChannelType: 2, Ban: true}
	first := metadataLog(t, 1, info)
	info.Ban, info.Large = false, true
	logs := []types.Log{first, metadataLog(t, 2, info), applyTestLog(t, 3, CMDDeleteChannel, EncodeChannel(info.ChannelId, 2))}
	require.Error(t, s.ApplySlotLogs(0, logs))
	got, err := db.GetChannel(info.ChannelId, 2)
	require.NoError(t, err)
	require.False(t, got.Ban)
	require.True(t, got.Large)
	index, err := db.SlotAppliedIndex(0)
	require.NoError(t, err)
	require.Zero(t, index, "failed progress must not skip unacknowledged effects")
	// Drop all process-local state before replaying the durable log.
	restarted := New(NewOptions(WithDB(db)))
	require.NoError(t, restarted.ApplySlotLogs(0, logs))
	got, err = db.GetChannel(info.ChannelId, 2)
	require.NoError(t, err)
	require.True(t, wkdb.IsEmptyChannelInfo(got))
}

func TestMetadataBatchBoundsOneDurabilityUnit(t *testing.T) {
	s, _ := recoveryStore(t)
	db := &metadataProgressDB{DB: s.wdb}
	s.wdb = db
	var logs []types.Log
	for i := uint64(1); i <= 300; i++ {
		logs = append(logs, metadataLog(t, i, wkdb.ChannelInfo{ChannelId: "bounded-metadata", ChannelType: 2, Ban: i%2 == 1}))
	}
	require.NoError(t, s.ApplySlotLogs(0, logs))
	require.Equal(t, []uint64{256, 300}, db.indexes)
	got, err := db.GetChannel("bounded-metadata", 2)
	require.NoError(t, err)
	require.False(t, got.Ban)
}
