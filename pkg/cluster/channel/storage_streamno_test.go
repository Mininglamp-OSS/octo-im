package channel

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// legacyLogData frames m as a pre-v5 proposer did: frame version in the old
// (version <= 100, int16-prefixed frame) or the new (version+100) wrapper.
func legacyLogData(t *testing.T, m wkdb.Message, version uint8, oldWrapper bool) []byte {
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

type legacyEntry struct {
	msg    wkdb.Message
	stream bool
}

// legacyLogs returns term-1 logs for channel retry/2 framed at v2, v3 and v4 in
// both wrappers, each with and without a StreamNo, plus Topic and flag variants.
func legacyLogs(t *testing.T) ([]types.Log, []legacyEntry) {
	t.Helper()
	var logs []types.Log
	var entries []legacyEntry
	for _, version := range []uint8{2, 3, 4} {
		for _, old := range []bool{true, false} {
			for _, stream := range []bool{true, false} {
				idx := uint64(len(logs) + 1)
				m := retryMessage(int64(1000+idx), "legacy", fmt.Sprintf("legacy-%d", idx))
				m.Framer = wkproto.Framer{FrameType: wkproto.RECV, RedDot: idx%2 == 0, SyncOnce: idx%3 == 0}
				m.MessageSeq = uint32(idx)
				m.Term = 1
				if version >= 3 {
					m.Expire = uint32(idx)
				}
				if idx%4 == 1 {
					m.Topic = "topic"
					m.Setting.Set(wkproto.SettingTopic)
				}
				if stream {
					m.Setting.Set(wkproto.SettingStream)
					m.StreamNo = fmt.Sprintf("stream-%d", idx)
					m.StreamFlag = wkproto.StreamFlagIng
					m.StreamId = idx
				}
				logs = append(logs, types.Log{Id: uint64(m.MessageID), Index: idx, Term: 1, Data: legacyLogData(t, m, version, old)})
				entries = append(entries, legacyEntry{msg: m, stream: stream})
			}
		}
	}
	return logs, entries
}

// storedColumns is the persisted view of a message, as compared across replicas.
type storedColumns struct {
	Header      uint8
	Setting     wkproto.Setting
	Expire      uint32
	MessageID   int64
	MessageSeq  uint32
	ClientMsgNo string
	StreamNo    string
	Timestamp   int32
	ChannelID   string
	ChannelType uint8
	Topic       string
	FromUID     string
	Payload     string
	Term        uint64
}

func loadColumns(t *testing.T, db wkdb.DB, seq uint64) storedColumns {
	t.Helper()
	m, err := db.LoadMsg("retry", 2, seq)
	require.NoError(t, err, "index %d", seq)
	return storedColumns{wkproto.ToFixHeaderUint8(&m.RecvPacket), m.Setting, m.Expire, m.MessageID, m.MessageSeq, m.ClientMsgNo, m.StreamNo,
		m.Timestamp, m.ChannelID, m.ChannelType, m.Topic, m.FromUID, string(m.Payload), m.Term}
}

func requireSameColumns(t *testing.T, want, got wkdb.DB, last uint64, label string) {
	t.Helper()
	for seq := uint64(1); seq <= last; seq++ {
		require.Equal(t, loadColumns(t, want, seq), loadColumns(t, got, seq), "%s: index %d", label, seq)
	}
}

// catchUp copies [1, last] from leader to follower the way a follower sync
// does: the leader's GetLogs output is appended by the follower's AppendLogs.
func catchUp(t *testing.T, leader, follower *storage, last uint64, window uint64) []types.Log {
	t.Helper()
	key := wkutil.ChannelToKey("retry", 2)
	var served []types.Log
	for start := uint64(1); start <= last; {
		logs, err := leader.GetLogs(key, start, start+window, 0)
		require.NoError(t, err)
		require.NotEmpty(t, logs)
		require.NoError(t, follower.AppendLogs(key, logs, nil))
		served = append(served, logs...)
		start = logs[len(logs)-1].Index + 1
	}
	return served
}

// GF-1 to GF-3 on the real channel storage: a replica that catches up from
// GetLogs stores the same columns as the replica that appended the original
// Data, a second hop serves the same bytes, and reopening changes nothing.
func TestGetLogsCatchUpPreservesLegacyStreamNo(t *testing.T) {
	logs, entries := legacyLogs(t)
	last := uint64(len(logs))
	key := wkutil.ChannelToKey("retry", 2)
	optsFor := func() *wkdb.Options {
		return wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1))
	}
	optsA, optsC, optsD := optsFor(), optsFor(), optsFor()
	open := func(o *wkdb.Options) wkdb.DB {
		db := wkdb.NewWukongDB(o)
		require.NoError(t, db.Open())
		return db
	}
	dbA, dbC, dbD := open(optsA), open(optsC), open(optsD)
	stA, stC, stD := newStorage(dbA, nil), newStorage(dbC, nil), newStorage(dbD, nil)
	require.NoError(t, stA.AppendLogs(key, logs, nil))
	for i, e := range entries {
		got := loadColumns(t, dbA, uint64(i+1))
		require.Equal(t, e.msg.StreamNo, got.StreamNo, "original append, index %d", i+1)
	}

	servedA := catchUp(t, stA, stC, last, 5)
	requireSameColumns(t, dbA, dbC, last, "A->C")
	servedC := catchUp(t, stC, stD, last, 3)
	requireSameColumns(t, dbA, dbD, last, "C->D")
	for i := range servedA {
		require.Equal(t, servedA[i].Data, servedC[i].Data, "second hop bytes, index %d", i+1)
	}

	// Reopen A and C: the emitted bytes are a pure function of the columns.
	require.NoError(t, dbA.Close())
	require.NoError(t, dbC.Close())
	dbA, dbC = open(optsA), open(optsC)
	defer func() {
		require.NoError(t, dbA.Close())
		require.NoError(t, dbC.Close())
		require.NoError(t, dbD.Close())
	}()
	stA, stC = newStorage(dbA, nil), newStorage(dbC, nil)
	for _, st := range []*storage{stA, stC} {
		again, err := st.GetLogs(key, 1, last+1, 0)
		require.NoError(t, err)
		require.Len(t, again, len(servedA))
		for i := range again {
			require.Equal(t, servedA[i].Data, again[i].Data, "after reopen, index %d", i+1)
		}
	}
	requireSameColumns(t, dbA, dbC, last, "after reopen")
}

// GF-4 / E2: entries without StreamNo are emitted exactly as Marshal of the
// stored message, which is what GetLogs emitted before.
func TestGetLogsNonStreamEmissionUnchanged(t *testing.T) {
	logs, entries := legacyLogs(t)
	db := retryDB(t)
	st := newStorage(db, nil)
	key := wkutil.ChannelToKey("retry", 2)
	require.NoError(t, st.AppendLogs(key, logs, nil))
	served, err := st.GetLogs(key, 1, uint64(len(logs))+1, 0)
	require.NoError(t, err)
	for i, e := range entries {
		if e.stream {
			continue
		}
		m, err := db.LoadMsg("retry", 2, uint64(i+1))
		require.NoError(t, err)
		want, err := m.Marshal()
		require.NoError(t, err)
		require.Equal(t, want, served[i].Data, "index %d", i+1)
	}
}

// SK2-R1/R4/R7 on real channel servers: a follower catching up through the
// production raft sync path, two hops, a new stream proposal, and retries.
func TestChannelCatchUpPreservesLegacyStreamNo(t *testing.T) {
	logs, entries := legacyLogs(t)
	last := uint64(len(logs))
	key := wkutil.ChannelToKey("retry", 2)
	tr := retryNetwork()
	dbs := []wkdb.DB{retryDB(t), retryDB(t), retryDB(t), retryDB(t)}
	// Nodes 1 and 2 appended the original legacy Data; node 3 is empty.
	for _, db := range dbs[:2] {
		require.NoError(t, newStorage(db, nil).AppendLogs(key, logs, &types.TermStartIndexInfo{Term: 1, Index: 1}))
	}
	applied := func(db wkdb.DB, idx uint64) func() bool {
		return func() bool {
			n, err := db.GetChannelAppliedIndex("retry", 2)
			return err == nil && n >= idx
		}
	}
	nodes := make([]*Server, 4)
	for i := 0; i < 3; i++ {
		nodes[i] = retryServer(t, uint64(i+1), dbs[i], []uint64{1, 2, 3}, tr)
	}

	// GF-5 / SK2-R4: a new write proposed with SettingStream and a StreamNo.
	// Proposal Data is v5, so every replica stores StreamNo "".
	fresh := retryMessage(5000, "fresh", "fresh-1")
	fresh.Setting.Set(wkproto.SettingStream)
	fresh.StreamNo = "x"
	r := retryPropose(t, nodes[0], fresh)
	require.Equal(t, last+1, r[0].Index)
	require.NoError(t, ValidateMessageResults(retryRequests(t, fresh), r))
	tail := last + 1

	// Node 3 caught up from node 1 through GetLogs.
	// Wait for the applied index: a follower may still truncate an unapplied tail.
	require.Eventually(t, applied(dbs[2], tail), 5*time.Second, 10*time.Millisecond)
	requireSameColumns(t, dbs[0], dbs[2], tail, "node1->node3")
	requireSameColumns(t, dbs[0], dbs[1], tail, "node1/node2")

	proposal, err := retryRequests(t, fresh)[0].Data, error(nil)
	require.NoError(t, err)
	var decoded wkdb.Message
	require.NoError(t, decoded.Unmarshal(proposal))
	for i := 0; i < 3; i++ {
		stored, err := dbs[i].LoadMsg("retry", 2, tail)
		require.NoError(t, err)
		require.Equal(t, "", stored.StreamNo, "node %d", i+1)
		require.True(t, sameMessageContent(stored, decoded), "node %d", i+1)
		want, err := stored.Marshal()
		require.NoError(t, err)
		served, err := newStorage(dbs[i], nil).GetLogs(key, tail, tail+1, 0)
		require.NoError(t, err)
		require.Equal(t, want, served[0].Data, "node %d emits the new write as before", i+1)
	}
	again := fresh
	again.MessageID = 5001
	r = retryPropose(t, nodes[0], again)
	require.True(t, r[0].Duplicate)
	require.Equal(t, uint64(5000), r[0].CanonicalID)

	// Second hop: node 1 is cut off, node 3 leads, and an empty node 4 catches
	// up from node 3, whose entries came from node 1's emission.
	tr.block(1, true)
	nodes[3] = retryServer(t, 4, dbs[3], []uint64{1, 2, 3, 4}, tr)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 3, Replicas: []uint64{1, 2, 3, 4}, Term: 2, ConfVersion: 2}
	for _, i := range []int{2, 1, 3} {
		require.NoError(t, nodes[i].Channel("retry", 2).switchConfig(channelConfigToRaftConfig(uint64(i+1), cfg)))
	}
	require.Eventually(t, applied(dbs[3], tail), 5*time.Second, 10*time.Millisecond)
	requireSameColumns(t, dbs[0], dbs[3], tail, "node3->node4")

	// SK2-R7 on the caught-up leader (class C): the outcome equals the one an
	// intact replica returns. A v5 retry of a legacy stream message conflicts,
	// a retry of a legacy message without StreamNo is a duplicate.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i, e := range entries {
		retry := e.msg
		retry.MessageID = int64(9000 + i)
		retry.MessageSeq = 0
		retry.Term = 0
		data, err := retry.Marshal()
		require.NoError(t, err)
		rs, err := nodes[2].proposeMessages(ctx, "retry", 2, types.ProposeReqSet{{Id: uint64(retry.MessageID), Data: data}})
		if e.stream {
			require.ErrorIs(t, err, ErrMessageConflict, "R-e, index %d", i+1)
			continue
		}
		require.NoError(t, err, "R-b, index %d", i+1)
		require.True(t, rs[0].Duplicate)
		require.Equal(t, uint64(e.msg.MessageID), rs[0].CanonicalID)
	}
}

// SK2-R7 R-c, R-d and the D-view of R-g on a single voter: an intact legacy
// stream entry conflicts with a v5 retry, a replica damaged before this change
// (StreamNo stored as "") still treats it as a duplicate, and the D-view
// decoded from emitted Data carries the stored StreamNo.
func TestChannelLegacyStreamRetryOutcomes(t *testing.T) {
	logs, entries := legacyLogs(t)
	key := wkutil.ChannelToKey("retry", 2)
	for _, damaged := range []bool{false, true} {
		t.Run(fmt.Sprintf("damaged=%v", damaged), func(t *testing.T) {
			db := retryDB(t)
			st := newStorage(db, nil)
			seeded := logs
			if damaged {
				// L-2: what a replica stored after catching up from a pre-change leader.
				seeded = make([]types.Log, len(logs))
				for i, lg := range logs {
					m := entries[i].msg
					data, err := m.Marshal()
					require.NoError(t, err)
					seeded[i] = types.Log{Id: lg.Id, Index: lg.Index, Term: lg.Term, Data: data}
				}
			}
			require.NoError(t, st.AppendLogs(key, seeded, nil))
			require.NoError(t, db.UpdateChannelAppliedIndex("retry", 2, uint64(len(logs))))

			served, err := st.GetLogs(key, 1, uint64(len(logs))+1, 0)
			require.NoError(t, err)
			s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
			for i, e := range entries {
				retry := e.msg
				retry.MessageID = int64(9000 + i)
				retry.MessageSeq = 0
				retry.Term = 0
				data, err := retry.Marshal()
				require.NoError(t, err)
				var rview, dview wkdb.Message
				require.NoError(t, rview.Unmarshal(data))
				require.NoError(t, dview.Unmarshal(served[i].Data))
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				rs, err := s.proposeMessages(ctx, "retry", 2, types.ProposeReqSet{{Id: uint64(retry.MessageID), Data: data}})
				cancel()
				if e.stream && !damaged {
					require.ErrorIs(t, err, ErrMessageConflict, "R-c, index %d", i+1)
					require.Equal(t, e.msg.StreamNo, dview.StreamNo, "D-view, index %d", i+1)
					require.False(t, sameMessageContent(dview, rview), "R-g D-view, index %d", i+1)
					continue
				}
				require.NoError(t, err, "index %d", i+1)
				require.True(t, rs[0].Duplicate)
				require.True(t, sameMessageContent(dview, rview), "index %d", i+1)
			}
		})
	}
}
