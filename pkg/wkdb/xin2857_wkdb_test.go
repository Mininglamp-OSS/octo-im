package wkdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestXIN2857_YP106_TruncateLogToSerializesWithMessageAppendLock(t *testing.T) {
	dir := t.TempDir()
	opts := NewOptions(
		WithDir(dir),
		WithNodeId(1001),
		WithShardNum(2),
		WithMaxMessagesPerBatch(2),
	)
	db := NewWukongDB(opts)
	require.NoError(t, db.Open())
	t.Cleanup(func() { _ = db.Close() })

	wdb, ok := db.(*wukongDB)
	require.True(t, ok)

	const (
		chID   = "ch_yp106"
		chType = uint8(2)
	)

	var initMsgs []Message
	for seq := uint32(1); seq <= 10; seq++ {
		initMsgs = append(initMsgs, Message{
			RecvPacket: wkproto.RecvPacket{
				MessageID:   int64(1000 + seq),
				MessageSeq:  seq,
				ClientMsgNo: fmt.Sprintf("cmn-%d", seq),
				FromUID:     "u_sender",
				ChannelID:   chID,
				ChannelType: chType,
				Payload:     []byte("payload"),
			},
		})
	}
	require.NoError(t, db.AppendMessages(chID, chType, initMsgs))
	require.NoError(t, db.UpdateChannelAppliedIndex(chID, chType, 3))

	// Part 1: Deterministic mutual exclusion check against messageAppendLock.
	wdb.dblock.messageAppendLock.lockByChannel(chID, chType)
	truncDone := make(chan error, 1)
	go func() {
		truncDone <- db.TruncateLogTo(chID, chType, 5)
	}()

	select {
	case err := <-truncDone:
		wdb.dblock.messageAppendLock.unlockByChannel(chID, chType)
		t.Fatalf("Y-P1-06: TruncateLogTo completed (err=%v) while messageAppendLock was held by AppendMessages!", err)
	case <-time.After(30 * time.Millisecond):
		// Expected: TruncateLogTo is blocked on messageAppendLock.
	}
	wdb.dblock.messageAppendLock.unlockByChannel(chID, chType)
	require.NoError(t, <-truncDone)

	lastSeq, _, err := db.GetChannelLastMessageSeq(chID, chType)
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastSeq)

	// Verify TruncateLogTo refuses to truncate below applied index (3):
	require.Error(t, db.TruncateLogTo(chID, chType, 2))

	// Part 2: Concurrent AppendMessages + TruncateLogTo stress with -race.
	var wg sync.WaitGroup
	var stop atomic.Bool
	wg.Add(2)
	go func() {
		defer wg.Done()
		for round := 0; round < 40; round++ {
			var batch []Message
			for seq := uint32(4); seq <= 12; seq++ {
				batch = append(batch, Message{
					RecvPacket: wkproto.RecvPacket{
						MessageID:   int64(round*100 + int(seq)),
						MessageSeq:  seq,
						ClientMsgNo: fmt.Sprintf("r-%d-%d", round, seq),
						FromUID:     "u_sender",
						ChannelID:   chID,
						ChannelType: chType,
						Payload:     []byte("concurrent"),
					},
				})
			}
			require.NoError(t, db.AppendMessages(chID, chType, batch))
		}
		stop.Store(true)
	}()
	go func() {
		defer wg.Done()
		for !stop.Load() {
			require.NoError(t, db.TruncateLogTo(chID, chType, 3))
		}
	}()
	wg.Wait()

	// Final deterministic truncate to applied boundary 3 and verify cache + Pebble consistency:
	require.NoError(t, db.TruncateLogTo(chID, chType, 3))
	cachedSeq, _, err := db.GetChannelLastMessageSeq(chID, chType)
	require.NoError(t, err)
	require.Equal(t, uint64(3), cachedSeq)

	wdb.channelSeqCache.invalidateChannelLastSeq(chID, chType)
	coldSeq, _, err := db.GetChannelLastMessageSeq(chID, chType)
	require.NoError(t, err)
	require.Equal(t, uint64(3), coldSeq)

	loaded, err := db.LoadLastMsgs(chID, chType, 20)
	require.NoError(t, err)
	require.Len(t, loaded, 3, "committed prefix [1..3] must remain intact and >3 must be truncated")
	for i, m := range loaded {
		require.Equal(t, uint32(i+1), m.MessageSeq)
	}
}

func TestXIN2857_YP110_LoadMsgByClientMsgNoPreservesIteratorError(t *testing.T) {
	dir := t.TempDir()
	opts := NewOptions(
		WithDir(dir),
		WithNodeId(1001),
		WithShardNum(1),
	)
	db := NewWukongDB(opts)
	require.NoError(t, db.Open())

	const (
		chID   = "ch_yp110"
		chType = uint8(2)
	)
	require.NoError(t, db.AppendMessages(chID, chType, []Message{
		{
			RecvPacket: wkproto.RecvPacket{
				MessageID:   2001,
				MessageSeq:  1,
				ClientMsgNo: "cmn-yp110-1",
				FromUID:     "u_sender_1",
				ChannelID:   chID,
				ChannelType: chType,
				Payload:     []byte("important-payload"),
			},
		},
	}))

	// Normal lookup hits the message and absent key returns ErrNotFound:
	msg, err := db.LoadMsgByClientMsgNo(chID, chType, "cmn-yp110-1")
	require.NoError(t, err)
	require.Equal(t, int64(2001), msg.MessageID)

	_, err = db.LoadMsgByClientMsgNo(chID, chType, "cmn-absent")
	require.ErrorIs(t, err, ErrNotFound)

	// Flush memtable to SSTable and close DB:
	wdb := db.(*wukongDB)
	require.NoError(t, wdb.channelDb(chID, chType).Flush())
	require.NoError(t, db.Close())

	// Corrupt the data block of the SSTable on disk (preserving the 48-byte footer so Open() succeeds,
	// while data block read in iter.First() fails checksum verification with iter.Error() != nil):
	var sstPaths []string
	err = filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr == nil && !info.IsDir() && strings.HasSuffix(info.Name(), ".sst") {
			sstPaths = append(sstPaths, path)
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, sstPaths)

	for _, p := range sstPaths {
		raw, rErr := os.ReadFile(p)
		require.NoError(t, rErr)
		if len(raw) > 128 {
			// Flip bytes at the start of the first data block (offset 0..15)
			for i := 0; i < 16; i++ {
				raw[i] ^= 0xFF
			}
			require.NoError(t, os.WriteFile(p, raw, 0644))
		}
	}

	db2 := NewWukongDB(opts)
	require.NoError(t, db2.Open())
	t.Cleanup(func() { _ = db2.Close() })

	wdb2 := db2.(*wukongDB)
	_, err = wdb2.loadMsgByClientMsgNo(context.Background(), chID, chType, "u_sender_1", "cmn-yp110-1", true)
	require.Error(t, err, "Y-P1-10: corrupted SSTable block during iter.First() must return an error")
	require.False(t, errors.Is(err, ErrNotFound), "Y-P1-10: storage/iterator corruption error (%v) must NOT be masked as ErrNotFound", err)
}

