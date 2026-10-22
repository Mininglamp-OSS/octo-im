package channeltest_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel/channeltest"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/wkdbtest"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	os.Exit(m.Run())
}

const (
	testChannel = "sn"
	testType    = uint8(2)
)

var testKey = wkutil.ChannelToKey(testChannel, testType)

func legacyData(t *testing.T, m wkdb.Message, version uint8, oldWrapper bool) []byte {
	t.Helper()
	frame, err := wkproto.New().EncodeFrame(&m.RecvPacket, version)
	require.NoError(t, err)
	enc := wkproto.NewEncoder()
	defer enc.End()
	if oldWrapper {
		enc.WriteUint8(version)
		enc.WriteBinary(frame)
	} else {
		enc.WriteUint8(version + 100)
		enc.WriteUint8(0)
		enc.WriteUint32(uint32(len(frame)))
		enc.WriteBytes(frame)
	}
	enc.WriteUint64(m.Term)
	return enc.Bytes()
}

// legacyLogs returns n term-1 logs cycling through frame v2-v5, both wrappers,
// with and without StreamNo and Topic.
func legacyLogs(t *testing.T, n int) []types.Log {
	t.Helper()
	logs := make([]types.Log, 0, n)
	for i := 1; i <= n; i++ {
		version := uint8(2 + i%4)
		m := wkdb.Message{RecvPacket: wkproto.RecvPacket{
			Framer: wkproto.FramerFromUint8(uint8(0x50 | i%16)), MessageID: int64(i), MessageSeq: uint32(i),
			ClientMsgNo: fmt.Sprintf("c-%d", i), ChannelID: testChannel, ChannelType: testType, FromUID: "u",
			Payload: []byte(fmt.Sprintf("payload-%d", i)), Timestamp: int32(i),
		}, Term: 1}
		if version >= 3 {
			m.Expire = uint32(i)
		}
		if i%3 == 0 {
			m.Topic = "topic"
			m.Setting.Set(wkproto.SettingTopic)
		}
		if i%2 == 0 {
			m.Setting.Set(wkproto.SettingStream)
			m.StreamNo = fmt.Sprintf("stream-%d", i)
			m.StreamId = uint64(i)
		}
		old := version < 5 && i%5 == 0
		logs = append(logs, types.Log{Id: uint64(i), Index: uint64(i), Term: 1, Data: legacyData(t, m, version, old)})
	}
	return logs
}

func openDisk(t *testing.T) wkdb.DB {
	t.Helper()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func requireSameRawColumns(t *testing.T, want, got wkdb.DB, last uint64, label string) {
	t.Helper()
	for seq := uint64(1); seq <= last; seq++ {
		a, err := wkdbtest.RawMessageColumns(want, testChannel, testType, seq)
		require.NoError(t, err, "%s: index %d", label, seq)
		b, err := wkdbtest.RawMessageColumns(got, testChannel, testType, seq)
		require.NoError(t, err, "%s: index %d", label, seq)
		for c := range a {
			require.Equal(t, a[c], b[c], "%s: index %d column %d", label, seq, c)
		}
	}
}

func catchUpFrom(t *testing.T, leader, follower raftgroup.IStorage, start, last, window uint64) {
	t.Helper()
	for start <= last {
		logs, err := leader.GetLogs(testKey, start, start+window, 0)
		require.NoError(t, err)
		require.NotEmpty(t, logs)
		require.NoError(t, follower.AppendLogs(testKey, logs, nil))
		start = logs[len(logs)-1].Index + 1
	}
}

func lastIndex(t *testing.T, db wkdb.DB) uint64 {
	t.Helper()
	n, _, err := db.GetChannelLastMessageSeq(testChannel, testType)
	require.NoError(t, err)
	return n
}

// SK2-R1/R2 on raw columns through the exported seam: A appended the original
// Data, C caught up from A, D caught up from C. All 14 raw columns are equal.
func TestRawColumnsEqualAfterTwoHopCatchUp(t *testing.T) {
	const n = 96
	logs := legacyLogs(t, n)
	dbA, dbC, dbD := openDisk(t), openDisk(t), openDisk(t)
	stA, stC, stD := channeltest.NewStorage(dbA), channeltest.NewStorage(dbC), channeltest.NewStorage(dbD)
	require.NoError(t, stA.AppendLogs(testKey, logs, nil))
	catchUpFrom(t, stA, stC, 1, n, 7)
	catchUpFrom(t, stC, stD, 1, n, 11)
	requireSameRawColumns(t, dbA, dbC, n, "A/C")
	requireSameRawColumns(t, dbA, dbD, n, "A/D")
	fromA, err := stA.GetLogs(testKey, 1, n+1, 0)
	require.NoError(t, err)
	fromC, err := stC.GetLogs(testKey, 1, n+1, 0)
	require.NoError(t, err)
	require.Equal(t, fromA, fromC)
}

// crashFollower is a follower DB on a strict in-memory filesystem. crash drops
// every write that was not synced and reopens the database.
type crashFollower struct {
	t   *testing.T
	fs  *vfs.MemFS
	db  wkdb.DB
	dir string
}

func newCrashFollower(t *testing.T) *crashFollower {
	f := &crashFollower{t: t, fs: vfs.NewStrictMem(), dir: "follower"}
	// The data directory of a running node already exists durably. Pebble
	// syncs only its own directory, so make the parents durable first.
	shard := f.fs.PathJoin(f.dir, "wukongimdb", "shard000")
	require.NoError(t, f.fs.MkdirAll(shard, 0755))
	for _, d := range []string{"", f.dir, f.fs.PathJoin(f.dir, "wukongimdb"), shard} {
		dir, err := f.fs.OpenDir(d)
		require.NoError(t, err)
		require.NoError(t, dir.Sync())
		require.NoError(t, dir.Close())
	}
	f.open()
	t.Cleanup(func() { require.NoError(t, f.db.Close()) })
	return f
}

func (f *crashFollower) open() {
	db, err := wkdbtest.OpenWithFS(f.dir, f.fs, wkdb.WithShardNum(1))
	require.NoError(f.t, err)
	f.db = db
}

func (f *crashFollower) storage() raftgroup.IStorage { return channeltest.NewStorage(f.db) }

func (f *crashFollower) crash() {
	f.fs.SetIgnoreSyncs(true)
	require.NoError(f.t, f.db.Close())
	f.fs.ResetToSyncedState()
	f.fs.SetIgnoreSyncs(false)
	f.open()
}

// SK2-R6: catch-up under crash points on the follower, with the production
// Sync commit path unchanged. After recovery and re-catch-up the follower's
// raw columns equal the leader's.
func TestCatchUpRecoversFromCrashPoints(t *testing.T) {
	const n = 64
	logs := legacyLogs(t, n)
	leaderDB := openDisk(t)
	leader := channeltest.NewStorage(leaderDB)
	require.NoError(t, leader.AppendLogs(testKey, logs, nil))

	t.Run("0_inside_append_before_sync", func(t *testing.T) {
		f := newCrashFollower(t)
		catchUpFrom(t, leader, f.storage(), 1, 20, 20)
		batch, err := leader.GetLogs(testKey, 21, 41, 0)
		require.NoError(t, err)
		// The sync of this commit never reaches the device.
		f.fs.SetIgnoreSyncs(true)
		require.NoError(t, f.storage().AppendLogs(testKey, batch, nil))
		f.crash()
		require.Equal(t, uint64(20), lastIndex(t, f.db))
		catchUpFrom(t, leader, f.storage(), lastIndex(t, f.db)+1, n, 9)
		requireSameRawColumns(t, leaderDB, f.db, n, "point 0")
	})
	t.Run("i_after_commit_before_response", func(t *testing.T) {
		f := newCrashFollower(t)
		catchUpFrom(t, leader, f.storage(), 1, 30, 30)
		f.crash()
		require.Equal(t, uint64(30), lastIndex(t, f.db))
		catchUpFrom(t, leader, f.storage(), lastIndex(t, f.db)+1, n, 9)
		requireSameRawColumns(t, leaderDB, f.db, n, "point i")
	})
	t.Run("ii_between_batches", func(t *testing.T) {
		f := newCrashFollower(t)
		for start := uint64(1); start <= n; start += 16 {
			catchUpFrom(t, leader, f.storage(), start, min(start+15, n), 16)
			f.crash()
			require.Equal(t, min(start+15, n), lastIndex(t, f.db))
		}
		requireSameRawColumns(t, leaderDB, f.db, n, "point ii")
	})
	t.Run("iii_after_truncate_before_reappend", func(t *testing.T) {
		f := newCrashFollower(t)
		catchUpFrom(t, leader, f.storage(), 1, n, 13)
		require.NoError(t, f.storage().TruncateLogTo(testKey, 25))
		f.crash()
		require.Equal(t, uint64(25), lastIndex(t, f.db))
		catchUpFrom(t, leader, f.storage(), lastIndex(t, f.db)+1, n, 13)
		requireSameRawColumns(t, leaderDB, f.db, n, "point iii")
	})
	t.Run("iv_leader_reopen_while_serving", func(t *testing.T) {
		// GetLogs does not write, so a leader crash while serving reduces to a
		// follower crash point plus a leader reopen: the served bytes are equal.
		lf := newCrashFollower(t)
		catchUpFrom(t, leader, lf.storage(), 1, n, n)
		before, err := lf.storage().GetLogs(testKey, 1, n+1, 0)
		require.NoError(t, err)
		lf.crash()
		after, err := lf.storage().GetLogs(testKey, 1, n+1, 0)
		require.NoError(t, err)
		require.Equal(t, before, after)
		f := newCrashFollower(t)
		catchUpFrom(t, lf.storage(), f.storage(), 1, n, 10)
		requireSameRawColumns(t, leaderDB, f.db, n, "point iv")
	})
}
