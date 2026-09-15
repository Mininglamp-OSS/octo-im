package channel

import (
	"context"
	"errors"
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	os.Exit(m.Run())
}

type retryTransport struct {
	mu      sync.RWMutex
	nodes   map[uint64]*Server
	blocked map[uint64]bool
}

func (tr *retryTransport) Send(key string, e types.Event) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	if tr.blocked[e.From] || tr.blocked[e.To] {
		return
	}
	if s := tr.nodes[e.To]; s != nil {
		s.AddEvent(key, e)
	}
}
func (tr *retryTransport) block(id uint64, b bool) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.blocked[id] = b
}

type retryHookDB struct {
	wkdb.DB
	appendEntered chan struct{}
	appendRelease chan struct{}
	once          sync.Once
	failApply     atomic.Bool
	beforeApply   func()
}

func (d *retryHookDB) AppendMessages(id string, typ uint8, m []wkdb.Message) error {
	if d.appendEntered != nil {
		d.once.Do(func() { close(d.appendEntered); <-d.appendRelease })
	}
	return d.DB.AppendMessages(id, typ, m)
}
func (d *retryHookDB) UpdateChannelAppliedIndex(id string, typ uint8, idx uint64) error {
	if d.beforeApply != nil {
		d.beforeApply()
	}
	if d.failApply.Load() {
		return errors.New("injected apply failure")
	}
	return d.DB.UpdateChannelAppliedIndex(id, typ, idx)
}
func retryDB(t *testing.T) wkdb.DB {
	t.Helper()
	d := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, d.Open())
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	return d
}
func retryServer(t *testing.T, id uint64, db wkdb.DB, replicas []uint64, tr *retryTransport) *Server {
	t.Helper()
	s := NewServer(NewOptions(WithNodeId(id), WithDB(db), WithGroupCount(1), WithTransport(tr)))
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)
	tr.mu.Lock()
	tr.nodes[id] = s
	tr.mu.Unlock()
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 1, Replicas: replicas, Term: 1, ConfVersion: 1}
	rg := s.getRaftGroup(wkutil.ChannelToKey("retry", 2))
	ch, err := createChannel(cfg, s, rg)
	require.NoError(t, err)
	rg.AddRaft(ch)
	require.NoError(t, ch.switchConfig(channelConfigToRaftConfig(id, cfg)))
	return s
}
func retryNetwork() *retryTransport {
	return &retryTransport{nodes: make(map[uint64]*Server), blocked: make(map[uint64]bool)}
}
func retryMessage(id int64, sender, client string) wkdb.Message {
	return wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: "retry", ChannelType: 2, MessageID: id, FromUID: sender, ClientMsgNo: client, Payload: []byte("payload"), Timestamp: 100}}
}
func retryRequests(t *testing.T, msgs ...wkdb.Message) types.ProposeReqSet {
	t.Helper()
	reqs := make(types.ProposeReqSet, len(msgs))
	for i, m := range msgs {
		b, err := m.Marshal()
		require.NoError(t, err)
		reqs[i] = types.ProposeReq{Id: uint64(m.MessageID), Data: b}
	}
	return reqs
}
func retryPropose(t *testing.T, s *Server, msgs ...wkdb.Message) types.ProposeRespSet {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := s.ProposeBatchUntilAppliedTimeoutForLocal(ctx, "retry", 2, retryRequests(t, msgs...))
	require.NoError(t, err)
	return r
}
func retryTail(t *testing.T, d wkdb.DB) uint64 {
	t.Helper()
	n, _, err := d.GetChannelLastMessageSeq("retry", 2)
	require.NoError(t, err)
	return n
}

func TestMessageRetryBatchAndConcurrent(t *testing.T) {
	db := retryDB(t)
	s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
	first := retryMessage(100, "alice", "same")
	again := first
	again.MessageID = 101
	again.Timestamp = 200
	again.ClientSeq = 999
	again.DUP = true
	r := retryPropose(t, s, first, again, retryMessage(102, "bob", "same"), retryMessage(103, "alice", "next"))
	require.Equal(t, []uint64{1, 1, 2, 3}, []uint64{r[0].Index, r[1].Index, r[2].Index, r[3].Index})
	require.Equal(t, uint64(100), r[1].CanonicalID)
	require.True(t, r[1].Duplicate)
	const count = 24
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := retryMessage(int64(200+i), "alice", "same")
			b, _ := m.Marshal()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			rs, err := s.proposeMessages(ctx, "retry", 2, types.ProposeReqSet{{Id: uint64(m.MessageID), Data: b}})
			if err == nil && (len(rs) != 1 || rs[0].CanonicalID != 100 || rs[0].Index != 1) {
				err = fmt.Errorf("wrong canonical result: %v", rs)
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, uint64(3), retryTail(t, db))
	// No client key retains append semantics.
	r = retryPropose(t, s, retryMessage(300, "alice", ""), retryMessage(301, "alice", ""))
	require.Equal(t, uint64(4), r[0].Index)
	require.Equal(t, uint64(5), r[1].Index)
}

func TestMessageRetryBufferedBeforeStore(t *testing.T) {
	raw := retryDB(t)
	db := &retryHookDB{DB: raw, appendEntered: make(chan struct{}), appendRelease: make(chan struct{})}
	s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
	type outcome struct {
		r   types.ProposeRespSet
		err error
	}
	done := make(chan outcome, 2)
	send := func(id int64) {
		m := retryMessage(id, "a", "buffered")
		b, _ := m.Marshal()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		r, e := s.proposeMessages(ctx, "retry", 2, types.ProposeReqSet{{Id: uint64(id), Data: b}})
		done <- outcome{r, e}
	}
	go send(1)
	select {
	case <-db.appendEntered:
	case <-time.After(time.Second):
		t.Fatal("append not entered")
	}
	go send(2)
	// Both attempts must remain pending, with only one index reserved.
	time.Sleep(30 * time.Millisecond)
	require.NoError(t, s.raftGroups[0].Do(context.Background(), wkutil.ChannelToKey("retry", 2), func(r raftgroup.IRaft) error { require.Equal(t, uint64(1), r.LastLogIndex()); return nil }))
	select {
	case o := <-done:
		t.Fatalf("ACK before storage: %+v", o)
	default:
	}
	close(db.appendRelease)
	for i := 0; i < 2; i++ {
		o := <-done
		require.NoError(t, o.err)
		require.Equal(t, uint64(1), o.r[0].CanonicalID)
		require.Equal(t, uint64(1), o.r[0].Index)
	}
	require.Equal(t, uint64(1), retryTail(t, db))
}

func TestMessageRetryConflictRejectsWholeBatch(t *testing.T) {
	db := retryDB(t)
	s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
	retryPropose(t, s, retryMessage(1, "a", "key"))
	for _, mutate := range []func(*wkdb.Message){func(m *wkdb.Message) { m.Payload = []byte("changed") }, func(m *wkdb.Message) { m.RedDot = true }, func(m *wkdb.Message) { m.Expire = 7 }, func(m *wkdb.Message) { m.Topic = "topic"; m.Setting.Set(wkproto.SettingTopic) }} {
		m := retryMessage(2, "a", "key")
		mutate(&m)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := s.proposeMessages(ctx, "retry", 2, retryRequests(t, retryMessage(3, "a", "new"), m))
		cancel()
		require.ErrorIs(t, err, ErrMessageConflict)
		require.Equal(t, uint64(1), retryTail(t, db))
	}
}

func TestMessageRetryRequiresQuorumAndDurableApply(t *testing.T) {
	t.Run("quorum", func(t *testing.T) {
		db := retryDB(t)
		s := retryServer(t, 1, db, []uint64{1, 2, 3}, retryNetwork())
		for i := 0; i < 2; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := s.proposeMessages(ctx, "retry", 2, retryRequests(t, retryMessage(int64(i+1), "a", "key")))
			cancel()
			require.ErrorIs(t, err, context.DeadlineExceeded)
		}
		require.Equal(t, uint64(1), retryTail(t, db))
		idx, err := db.GetChannelAppliedIndex("retry", 2)
		require.NoError(t, err)
		require.Zero(t, idx)
	})
	t.Run("durable apply", func(t *testing.T) {
		db := &retryHookDB{DB: retryDB(t)}
		db.failApply.Store(true)
		s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		_, err := s.proposeMessages(ctx, "retry", 2, retryRequests(t, retryMessage(1, "a", "key")))
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		db.failApply.Store(false)
		r := retryPropose(t, s, retryMessage(2, "a", "key"))
		require.Equal(t, uint64(1), r[0].CanonicalID)
		require.Equal(t, uint64(1), retryTail(t, db))
	})
}

func TestMessageRetryRestoredTail(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			dir := t.TempDir()
			opts := wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(1))
			db := wkdb.NewWukongDB(opts)
			require.NoError(t, db.Open())
			m := retryMessage(10, "a", "old")
			m.MessageSeq = 1
			m.Term = 1
			require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{m}))
			if committed {
				require.NoError(t, db.UpdateChannelAppliedIndex("retry", 2, 1))
			}
			// Reopen the real Pebble store, including the missing-marker upgrade case.
			require.NoError(t, db.Close())
			db = wkdb.NewWukongDB(opts)
			require.NoError(t, db.Open())
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			s := retryServer(t, 1, db, []uint64{1, 2, 3}, retryNetwork())
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			r, err := s.proposeMessages(ctx, "retry", 2, retryRequests(t, retryMessage(11, "a", "old")))
			if committed {
				require.NoError(t, err)
				require.Equal(t, uint64(10), r[0].CanonicalID)
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			require.Equal(t, uint64(1), retryTail(t, db))
		})
	}
	t.Run("legacy single voter confirms", func(t *testing.T) {
		db := retryDB(t)
		m := retryMessage(10, "a", "old")
		m.MessageSeq = 1
		m.Term = 1
		require.NoError(t, db.AppendMessages("retry", 2, []wkdb.Message{m}))
		s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
		r := retryPropose(t, s, retryMessage(11, "a", "old"))
		require.Equal(t, uint64(10), r[0].CanonicalID)
	})
	t.Run("corrupt marker", func(t *testing.T) {
		db := retryDB(t)
		require.NoError(t, db.UpdateChannelAppliedIndex("retry", 2, 5))
		_, err := newStorage(db, nil).GetState("retry", 2)
		require.Error(t, err)
	})
}

func TestMessageRetryLeaderChange(t *testing.T) {
	tr := retryNetwork()
	dbs := []wkdb.DB{retryDB(t), retryDB(t), retryDB(t)}
	nodes := make([]*Server, 3)
	for i := range nodes {
		nodes[i] = retryServer(t, uint64(i+1), dbs[i], []uint64{1, 2, 3}, tr)
	}
	original := retryPropose(t, nodes[0], retryMessage(100, "a", "key"))[0]
	require.Eventually(t, func() bool {
		idx, err := dbs[1].GetChannelAppliedIndex("retry", 2)
		return err == nil && idx >= original.Index
	}, 2*time.Second, 5*time.Millisecond)
	tr.block(1, true)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "retry", ChannelType: 2, LeaderId: 2, Replicas: []uint64{1, 2, 3}, Term: 2, ConfVersion: 2}
	for _, i := range []int{2, 1} {
		require.NoError(t, nodes[i].Channel("retry", 2).switchConfig(channelConfigToRaftConfig(uint64(i+1), cfg)))
	}
	r := retryPropose(t, nodes[1], retryMessage(200, "a", "key"))
	require.Equal(t, original.CanonicalID, r[0].CanonicalID)
	require.Equal(t, original.Index, r[0].Index)
	retryPropose(t, nodes[1], retryMessage(300, "a", "after"))
	require.Equal(t, uint64(2), retryTail(t, dbs[1]))
	tr.block(1, false)
	require.NoError(t, nodes[0].Channel("retry", 2).switchConfig(channelConfigToRaftConfig(1, cfg)))
	require.Eventually(t, func() bool { return retryTail(t, dbs[0]) == 2 }, 2*time.Second, 10*time.Millisecond)
}

func TestMessageRetryLeadershipChangeWhilePending(t *testing.T) {
	db := retryDB(t)
	s := retryServer(t, 1, db, []uint64{1, 2, 3}, retryNetwork())
	done := make(chan error, 1)
	reqs := retryRequests(t, retryMessage(1, "a", "key"))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := s.proposeMessages(ctx, "retry", 2, reqs)
		done <- err
	}()
	require.Eventually(t, func() bool { return retryTail(t, db) == 1 }, time.Second, 5*time.Millisecond)
	cfg := types.Config{Leader: 2, Term: 2, Version: 2, Role: types.RoleFollower, Replicas: []uint64{1, 2, 3}}
	require.NoError(t, s.Channel("retry", 2).switchConfig(cfg))
	require.ErrorIs(t, <-done, errMessageLeaderChanged)
}

func TestMessageRetrySameLeaderConfigDuringApply(t *testing.T) {
	db := &retryHookDB{DB: retryDB(t)}
	s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
	var once sync.Once
	db.beforeApply = func() {
		once.Do(func() {
			err := s.Channel("retry", 2).switchConfig(types.Config{Leader: 1, Role: types.RoleLeader, Term: 1, Version: 2, Replicas: []uint64{1}})
			if err != nil {
				panic(err)
			}
		})
	}
	r := retryPropose(t, s, retryMessage(1, "a", "membership"))
	require.Equal(t, uint64(1), r[0].Index)
	require.Equal(t, uint64(1), retryTail(t, db))
}

func TestMessageRetryProtocolFlags(t *testing.T) {
	db := retryDB(t)
	s := retryServer(t, 1, db, []uint64{1}, retryNetwork())
	for i, mutate := range []func(*wkdb.Message){func(m *wkdb.Message) { m.RedDot = true }, func(m *wkdb.Message) { m.SyncOnce = true }, func(m *wkdb.Message) { m.Topic = "topic"; m.Setting.Set(wkproto.SettingTopic) }} {
		m := retryMessage(int64(10+i), "a", fmt.Sprint(i))
		mutate(&m)
		first := retryPropose(t, s, m)
		m.MessageID += 100
		second := retryPropose(t, s, m)
		require.Equal(t, first[0].CanonicalID, second[0].CanonicalID)
	}
}
