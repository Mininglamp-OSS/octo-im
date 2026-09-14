package channel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type messageBoundaryDB struct {
	wkdb.DB
	entered chan struct{}
	release chan struct{}
	once    atomic.Bool
	mu      sync.Mutex
	ranges  [][3]uint64
	applied []uint64
	verify  bool
}

func (d *messageBoundaryDB) LoadMsgBySenderClientMsgNo(id string, typ uint8, sender string, client string) (wkdb.Message, error) {
	if d.entered != nil && !d.verify && d.once.CompareAndSwap(false, true) {
		close(d.entered)
		<-d.release
	}
	return d.DB.LoadMsgBySenderClientMsgNo(id, typ, sender, client)
}
func (d *messageBoundaryDB) LoadNextRangeMsgsForSize(id string, typ uint8, start, end, limit uint64) ([]wkdb.Message, error) {
	d.mu.Lock()
	d.ranges = append(d.ranges, [3]uint64{start, end, limit})
	d.mu.Unlock()
	return d.DB.LoadNextRangeMsgsForSize(id, typ, start, end, limit)
}
func messageBoundarySetup(t *testing.T, d *messageBoundaryDB) *Server {
	t.Helper()
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	d.DB = wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, d.Open())
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	s := NewServer(NewOptions(WithNodeId(1), WithDB(d), WithGroupCount(1)))
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	return s
}
func messageBoundaryWake(t *testing.T, s *Server, id string) {
	t.Helper()
	require.NoError(t, s.WakeLeaderIfNeed(wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 1, Term: 1, ConfVersion: 1, Replicas: []uint64{1}}))
}
func messageBoundaryPropose(s *Server, id string, msgid int64) error {
	m := wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: id, ChannelType: 2, MessageID: msgid, ClientMsgNo: "new-send", FromUID: "sender", Payload: []byte("payload")}}
	b, err := m.Marshal()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = s.ProposeBatchUntilAppliedTimeoutForLocal(ctx, id, 2, types.ProposeReqSet{{Id: uint64(msgid), Data: b}})
	return err
}
func TestMessageRetryBoundarySlowLookupMustNotBlockOtherChannel(t *testing.T) {
	d := &messageBoundaryDB{entered: make(chan struct{}), release: make(chan struct{})}
	s := messageBoundarySetup(t, d)
	messageBoundaryWake(t, s, "slow")
	messageBoundaryWake(t, s, "other")
	result := make(chan error, 1)
	go func() { result <- messageBoundaryPropose(s, "slow", 1) }()
	consumed := false
	select {
	case <-d.entered:
		t.Log("slow storage lookup entered")
	case err := <-result:
		require.NoError(t, err)
		consumed = true
		t.Log("proposal path did not invoke synchronous lookup")
	case <-time.After(time.Second):
		close(d.release)
		t.Fatal("proposal did not reach a known state")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err := s.getRaftGroup(wkutil.ChannelToKey("other", 2)).ReadLeaderState(ctx, wkutil.ChannelToKey("other", 2))
	cancel()
	close(d.release)
	if !consumed {
		require.NoError(t, <-result)
	}
	t.Logf("unrelated channel state read while storage is delayed: %v", err)
	require.NoError(t, err, "a slow lookup in one channel must not block the shared event loop for another channel")
}
func TestMessageRetryBoundaryLegacyActivationMustBoundHistoryRead(t *testing.T) {
	d := &messageBoundaryDB{}
	s := messageBoundarySetup(t, d)
	const total = 2000
	messages := make([]wkdb.Message, total)
	for i := range messages {
		messages[i] = wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: "legacy", ChannelType: 2, MessageID: int64(i + 1), MessageSeq: uint32(i + 1), FromUID: "sender", ClientMsgNo: "old", Payload: make([]byte, 256)}, Term: 1}
	}
	require.NoError(t, d.AppendMessages("legacy", 2, messages))
	require.NoError(t, d.SetLeaderTermStartIndex(wkutil.ChannelToKey("legacy", 2), 1, 1))
	// The old implementation never wrote a channel applied marker.
	applied, err := d.GetChannelAppliedIndex("legacy", 2)
	require.NoError(t, err)
	require.Zero(t, applied)
	messageBoundaryWake(t, s, "legacy")
	require.NoError(t, messageBoundaryPropose(s, "legacy", total+1))
	d.mu.Lock()
	ranges := append([][3]uint64(nil), d.ranges...)
	d.mu.Unlock()
	t.Logf("legacy activation storage reads [start,end,size_limit]: %v", ranges)
	for _, r := range ranges {
		require.Greater(t, r[2], uint64(0))
		require.LessOrEqual(t, r[1]-r[0], uint64(1000))
	}
	d.mu.Lock()
	appliedSteps := append([]uint64(nil), d.applied...)
	d.mu.Unlock()
	require.GreaterOrEqual(t, len(appliedSteps), 3, "recovery must advance its durable marker incrementally")
	for i, index := range appliedSteps {
		if i == 0 {
			require.LessOrEqual(t, index, uint64(1000))
		} else {
			require.LessOrEqual(t, index-appliedSteps[i-1], uint64(1000))
		}
	}
}

func (d *messageBoundaryDB) LoadMsg(id string, typ uint8, seq uint64) (wkdb.Message, error) {
	if d.entered != nil && d.verify && d.once.CompareAndSwap(false, true) {
		close(d.entered)
		<-d.release
	}
	return d.DB.LoadMsg(id, typ, seq)
}
func (d *messageBoundaryDB) UpdateChannelAppliedIndex(id string, typ uint8, seq uint64) error {
	err := d.DB.UpdateChannelAppliedIndex(id, typ, seq)
	if err == nil {
		d.mu.Lock()
		d.applied = append(d.applied, seq)
		d.mu.Unlock()
	}
	return err
}

func TestMessageRetryBoundarySlowVerificationDoesNotBlockOtherChannel(t *testing.T) {
	d := &messageBoundaryDB{entered: make(chan struct{}), release: make(chan struct{}), verify: true}
	s := messageBoundarySetup(t, d)
	messageBoundaryWake(t, s, "slow")
	messageBoundaryWake(t, s, "other")
	result := make(chan error, 1)
	go func() { result <- messageBoundaryPropose(s, "slow", 1) }()
	select {
	case <-d.entered:
	case <-time.After(time.Second):
		close(d.release)
		t.Fatal("verification not reached")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	_, err := s.getRaftGroup(wkutil.ChannelToKey("other", 2)).ReadLeaderState(ctx, wkutil.ChannelToKey("other", 2))
	cancel()
	close(d.release)
	require.NoError(t, <-result)
	require.NoError(t, err)
}

func TestMessageRetryBoundaryRevalidatesConcurrentStore(t *testing.T) {
	d := &messageBoundaryDB{entered: make(chan struct{}), release: make(chan struct{})}
	s := messageBoundarySetup(t, d)
	messageBoundaryWake(t, s, "slow")
	result := make(chan error, 1)
	go func() { result <- messageBoundaryPropose(s, "slow", 1) }()
	select {
	case <-d.entered:
	case <-time.After(time.Second):
		close(d.release)
		t.Fatal("lookup not reached")
	}
	err := messageBoundaryPropose(s, "slow", 2) // commits while first lookup is stalled
	close(d.release)
	require.NoError(t, err)
	require.NoError(t, <-result)
	tail, _, err := d.GetChannelLastMessageSeq("slow", 2)
	require.NoError(t, err)
	require.Equal(t, uint64(1), tail)
	m, err := d.LoadMsg("slow", 2, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), m.MessageID)
}

func TestMessageRetryBoundaryLeaderChangeDuringDiskRead(t *testing.T) {
	for _, verify := range []bool{false, true} {
		t.Run(map[bool]string{false: "lookup", true: "verification"}[verify], func(t *testing.T) {
			d := &messageBoundaryDB{entered: make(chan struct{}), release: make(chan struct{}), verify: verify}
			s := messageBoundarySetup(t, d)
			messageBoundaryWake(t, s, "slow")
			result := make(chan error, 1)
			go func() { result <- messageBoundaryPropose(s, "slow", 1) }()
			select {
			case <-d.entered:
			case <-time.After(time.Second):
				close(d.release)
				t.Fatal("disk read not reached")
			}
			key := wkutil.ChannelToKey("slow", 2)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := s.getRaftGroup(key).Do(ctx, key, func(r raftgroup.IRaft) error {
				return r.Step(types.Event{Type: types.ConfChange, Config: types.Config{Term: 2, Version: 2, Leader: 2, Replicas: []uint64{1, 2}, Role: types.RoleFollower}})
			})
			close(d.release)
			require.NoError(t, err)
			require.ErrorIs(t, <-result, errMessageLeaderChanged)
		})
	}
}
