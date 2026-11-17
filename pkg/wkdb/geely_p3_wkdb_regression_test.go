package wkdb

import (
	"fmt"
	"testing"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestP3_AppendMessagesSubBatchSplittingAndMonotonicSeq(t *testing.T) {
	dir := t.TempDir()
	cache := pebble.NewCache(8 * 1024 * 1024)
	defer cache.Unref()

	db := NewWukongDB(NewOptions(
		WithDir(dir),
		WithShardNum(2),
		WithMemTableSize(4*1024*1024),
		WithBlockCache(cache),
		WithMaxMessagesPerBatch(50),
		WithMaxBatchBytes(16*1024),
	))
	require.NoError(t, db.Open())
	defer db.Close()

	channelID := "ch-subbatch-test"
	channelType := uint8(2)

	// Append 180 messages in a single call; with MaxMessagesPerBatch=50 and MaxBatchBytes=16KiB,
	// AppendMessages must split them into multiple bounded Pebble batches while preserving
	// monotonic sequence numbers and full message integrity.
	msgs := make([]Message, 180)
	for i := 0; i < len(msgs); i++ {
		msgs[i] = Message{
			RecvPacket: wkproto.RecvPacket{
				MessageID:   int64(1000 + i),
				MessageSeq:  uint32(i + 1),
				ChannelID:   channelID,
				ChannelType: channelType,
				FromUID:     "u1",
				Payload:     []byte(fmt.Sprintf("payload-%04d", i+1)),
			},
			Term: 1,
		}
	}
	require.NoError(t, db.AppendMessages(channelID, channelType, msgs))

	lastSeq, _, err := db.GetChannelLastMessageSeq(channelID, channelType)
	require.NoError(t, err)
	require.Equal(t, uint64(180), lastSeq)

	// LoadNextRangeMsgsForSize with a small byte limit must cap by bytes while always returning >= 1 message.
	smallBatch, err := db.LoadNextRangeMsgsForSize(channelID, channelType, 1, 181, 512)
	require.NoError(t, err)
	require.NotEmpty(t, smallBatch)
	require.Less(t, len(smallBatch), 20)

	allLoaded, err := db.LoadNextRangeMsgsForSize(channelID, channelType, 1, 181, 0)
	require.NoError(t, err)
	require.Len(t, allLoaded, 180)
	require.Equal(t, uint32(1), allLoaded[0].MessageSeq)
	require.Equal(t, uint32(180), allLoaded[179].MessageSeq)
}

func TestP3_ValidatePebbleShardDirFailsFastOnShardMismatch(t *testing.T) {
	dir := t.TempDir()

	db4 := NewWukongDB(NewOptions(
		WithDir(dir),
		WithShardNum(4),
		WithMemTableSize(2*1024*1024),
	))
	require.NoError(t, db4.Open())
	require.NoError(t, db4.Close())

	// Decreasing shard count (4 -> 2) on a non-empty directory must fail fast.
	db2 := NewWukongDB(NewOptions(
		WithDir(dir),
		WithShardNum(2),
		WithMemTableSize(2*1024*1024),
	))
	err := db2.Open()
	require.Error(t, err)
	require.Contains(t, err.Error(), "shard count mismatch")

	// Increasing shard count (4 -> 8) on a non-empty directory must also fail fast.
	db8 := NewWukongDB(NewOptions(
		WithDir(dir),
		WithShardNum(8),
		WithMemTableSize(2*1024*1024),
	))
	err = db8.Open()
	require.Error(t, err)
	require.Contains(t, err.Error(), "shard count mismatch")
}

func TestP1_TruncateLogToZeroClearsUnappliedChannelLogs(t *testing.T) {
	dir := t.TempDir()
	db := NewWukongDB(NewOptions(
		WithDir(dir),
		WithShardNum(2),
		WithMemTableSize(2*1024*1024),
	))
	require.NoError(t, db.Open())
	defer db.Close()

	chID := "ch-truncate-zero"
	chType := uint8(2)
	require.NoError(t, db.AppendMessages(chID, chType, []Message{
		{RecvPacket: wkproto.RecvPacket{MessageID: 1, MessageSeq: 1, ChannelID: chID, ChannelType: chType, Payload: []byte("m1")}, Term: 1},
		{RecvPacket: wkproto.RecvPacket{MessageID: 2, MessageSeq: 2, ChannelID: chID, ChannelType: chType, Payload: []byte("m2")}, Term: 1},
	}))

	// Truncating to 0 must clear all unapplied messages and reset channelLastMessageSeq to 0.
	require.NoError(t, db.TruncateLogTo(chID, chType, 0))
	seq, _, err := db.GetChannelLastMessageSeq(chID, chType)
	require.NoError(t, err)
	require.Zero(t, seq)
}
