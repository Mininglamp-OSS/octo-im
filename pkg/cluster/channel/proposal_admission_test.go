package channel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

// R8: channel messages through the NotNeedApplied RaftGroup and the real
// message store. A send is acknowledged as successful only when the
// proposal succeeds; the stored message at the returned MessageSeq must then
// be the sent message.

const (
	r8Channel     = "r8-channel"
	r8ChannelType = uint8(2)
)

type nopTransport struct{}

func (nopTransport) Send(string, types.Event) {}

// slowStorage delays message appends, as a slow disk does.
type slowStorage struct {
	*storage
	delay atomic.Int64
}

func (s *slowStorage) AppendLogs(key string, logs []types.Log, termStart *types.TermStartIndexInfo) error {
	time.Sleep(time.Duration(s.delay.Load()))
	return s.storage.AppendLogs(key, logs, termStart)
}

func newR8Group(t *testing.T) (*raftgroup.RaftGroup, *slowStorage, wkdb.DB) {
	t.Helper()
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions(trace.WithServiceName("test"), trace.WithServiceHostName("host"))))
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	st := &slowStorage{storage: newStorage(db, &Server{opts: NewOptions()})}
	rg := raftgroup.New(raftgroup.NewOptions(raftgroup.WithNotNeedApplied(true), raftgroup.WithStorage(st),
		raftgroup.WithTransport(nopTransport{}), raftgroup.WithTickInterval(time.Hour)))
	key := wkutil.ChannelToKey(r8Channel, r8ChannelType)
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey(key), raft.WithNodeId(1), raft.WithReplicas([]uint64{1})))
	require.True(t, node.IsLeader())
	rg.AddRaft(node)
	t.Cleanup(func() {
		rg.Stop()
		_ = db.Close()
	})
	return rg, st, db
}

// sendMessages proposes msgs like store.AppendMessages and returns whether the
// sender would be acknowledged with success.
func sendMessages(rg *raftgroup.RaftGroup, ctx context.Context, ids ...int64) (types.ProposeRespSet, bool, error) {
	reqs := types.ProposeReqSet{}
	for _, id := range ids {
		msg := wkdb.Message{}
		msg.MessageID = id
		msg.ChannelID = r8Channel
		msg.ChannelType = r8ChannelType
		msg.Payload = []byte("payload")
		data, err := msg.Marshal()
		if err != nil {
			return nil, false, err
		}
		reqs = append(reqs, types.ProposeReq{Id: uint64(id), Data: data})
	}
	resps, err := rg.ProposeBatchUntilAppliedTimeout(ctx, wkutil.ChannelToKey(r8Channel, r8ChannelType), reqs)
	return resps, err == nil, err
}

func storedMessageIDs(t *testing.T, db wkdb.DB) map[int64]uint32 {
	t.Helper()
	msgs, err := db.LoadNextRangeMsgs(r8Channel, r8ChannelType, 1, 0, 1000)
	require.NoError(t, err)
	out := map[int64]uint32{}
	for _, m := range msgs {
		out[m.MessageID] = m.MessageSeq
	}
	return out
}

func requireAckedMessagesStored(t *testing.T, db wkdb.DB, resps types.ProposeRespSet) {
	t.Helper()
	for _, resp := range resps {
		msg, err := db.LoadMsg(r8Channel, r8ChannelType, resp.Index)
		require.NoError(t, err)
		require.Equal(t, int64(resp.Id), msg.MessageID, "MessageSeq %d must hold the acknowledged message", resp.Index)
	}
}

// The XIN-2020 schedule: the event loop is stalled while A is queued and A's
// sender times out, then B is sent.
func TestChannelSendAckAfterQueuedTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b []int64
	}{
		{"duplicate", []int64{100}, []int64{200}},
		{"partial-overlap", []int64{100}, []int64{200, 201}},
		{"shorter-second", []int64{100, 101}, []int64{200}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, _, db := newR8Group(t)
			ctxA, cancelA := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancelA()
			_, ackA, errA := sendMessages(rg, ctxA, tc.a...)
			require.False(t, ackA)
			require.ErrorIs(t, errA, types.ErrProposalNotAdmitted)

			type result struct {
				resps types.ProposeRespSet
				ack   bool
				err   error
			}
			bC := make(chan result, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				resps, ack, err := sendMessages(rg, ctx, tc.b...)
				bC <- result{resps, ack, err}
			}()
			time.Sleep(50 * time.Millisecond)
			require.NoError(t, rg.Start())
			b := <-bC
			require.NoError(t, b.err)
			require.True(t, b.ack)
			requireAckedMessagesStored(t, db, b.resps)

			stored := storedMessageIDs(t, db)
			for _, id := range tc.a {
				_, ok := stored[id]
				require.False(t, ok, "message %d was reported as not sent but is stored", id)
			}
			require.Len(t, stored, len(tc.b))
		})
	}
}

// A is admitted but its sender gives up while the append is slow. A is not
// acknowledged as success; B, sent next, is acknowledged with its own seq.
func TestChannelSendAckAfterAdmittedTimeout(t *testing.T) {
	rg, st, db := newR8Group(t)
	require.NoError(t, rg.Start())
	st.delay.Store(int64(100 * time.Millisecond))
	ctxA, cancelA := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelA()
	_, ackA, errA := sendMessages(rg, ctxA, 100)
	require.False(t, ackA)
	require.ErrorIs(t, errA, types.ErrProposalOutcomeUnknown)

	st.delay.Store(0)
	ctxB, cancelB := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelB()
	resps, ackB, err := sendMessages(rg, ctxB, 200)
	require.NoError(t, err)
	require.True(t, ackB)
	require.Equal(t, uint64(2), resps[0].Index, "B follows A's admitted entry")
	requireAckedMessagesStored(t, db, resps)
	require.Equal(t, map[int64]uint32{100: 1, 200: 2}, storedMessageIDs(t, db))
}
