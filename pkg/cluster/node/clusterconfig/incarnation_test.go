package clusterconfig

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"os/exec"
	"path"
	"syscall"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig/key"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

type fakeIncarnationStore struct {
	data     []byte
	found    bool
	readErr  error
	writeErr error
	writes   [][]byte
}

func (f *fakeIncarnationStore) senderIncarnation() ([]byte, bool, error) {
	return f.data, f.found, f.readErr
}

func (f *fakeIncarnationStore) saveSenderIncarnation(data []byte) error {
	f.writes = append(f.writes, append([]byte(nil), data...))
	return f.writeErr
}

func incarnationBytes(v uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, v)
}

func TestReserveSenderIncarnation(t *testing.T) {
	writeErr := errors.New("injected sync write failure")
	readErr := errors.New("injected read failure")
	for name, tc := range map[string]struct {
		st      fakeIncarnationStore
		want    uint64
		wantErr error
		writes  int
	}{
		"first":     {want: 1, writes: 1},
		"next":      {st: fakeIncarnationStore{data: incarnationBytes(41), found: true}, want: 42, writes: 1},
		"maxMinus1": {st: fakeIncarnationStore{data: incarnationBytes(math.MaxUint64 - 1), found: true}, want: math.MaxUint64, writes: 1},
		"wrap":      {st: fakeIncarnationStore{data: incarnationBytes(math.MaxUint64), found: true}, wantErr: ErrSenderIncarnationExhausted},
		"short":     {st: fakeIncarnationStore{data: []byte{1, 2, 3}, found: true}, wantErr: ErrSenderIncarnationCorrupt},
		"empty":     {st: fakeIncarnationStore{data: []byte{}, found: true}, wantErr: ErrSenderIncarnationCorrupt},
		"zero":      {st: fakeIncarnationStore{data: incarnationBytes(0), found: true}, wantErr: ErrSenderIncarnationCorrupt},
		"read":      {st: fakeIncarnationStore{readErr: readErr}, wantErr: readErr},
		"write":     {st: fakeIncarnationStore{writeErr: writeErr}, wantErr: writeErr, writes: 1},
	} {
		t.Run(name, func(t *testing.T) {
			st := tc.st
			got, err := reserveSenderIncarnation(&st)
			require.Len(t, st.writes, tc.writes)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Zero(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, incarnationBytes(tc.want), st.writes[0])
		})
	}
}

func storedIncarnation(t *testing.T, cfgPath string) ([]byte, bool) {
	t.Helper()
	st := NewPebbleShardLogStorage(path.Join(path.Dir(cfgPath), "cfglogdb"), nil)
	require.NoError(t, st.Open(), "the directory lock must be free")
	defer func() { require.NoError(t, st.Close()) }()
	data, found, err := st.senderIncarnation()
	require.NoError(t, err)
	return data, found
}

func seedIncarnation(t *testing.T, cfgPath string, data []byte) {
	t.Helper()
	require.NoError(t, os.MkdirAll(path.Dir(cfgPath), 0o755))
	st := NewPebbleShardLogStorage(path.Join(path.Dir(cfgPath), "cfglogdb"), nil)
	require.NoError(t, st.Open())
	require.NoError(t, st.db.Set(key.NewSenderIncarnationKey(), data, pebble.Sync))
	require.NoError(t, st.Close())
}

func stopServer(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, s.WaitStopped(ctx))
}

// forwardOnce makes s a follower of node 1, starts one forwarded proposal and
// returns its outgoing SendPropose together with the caller's result.
func forwardOnce(t *testing.T, s *Server, tr *ownerTransport, id uint64) (types.Event, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	require.NoError(t, s.raft.StepWait(ctx, types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1, 2}, Term: 1, Leader: 1}}))
	cancel()
	result := make(chan error, 1)
	go func() {
		_, err := s.ProposeUntilAppliedTimeout(context.Background(), id, []byte("forwarded"))
		result <- err
	}()
	for {
		e := ownerRecv(t, tr.events)
		if e.Type == types.SendPropose {
			return e, result
		}
	}
}

func echoResponse(t *testing.T, req types.Event, reason types.Reason, id uint64) types.Event {
	t.Helper()
	data, err := types.ProposeRespSet{&types.ProposeResp{Id: id, Index: 1}}.Marshal()
	require.NoError(t, err)
	echo := types.Log{Id: req.Logs[0].Id, Index: req.Logs[0].Index, Term: req.Logs[0].Term, Data: data}
	return types.Event{Type: types.SendProposeResp, From: 1, To: 2, Reason: reason, Logs: []types.Log{echo}}
}

// Every Start of one data directory reserves a strictly larger incarnation
// before Raft exists, and forwarded proposals carry exactly that value. A
// delayed response from incarnation N completes nothing in incarnation N+1 even
// though request ID and attempt number are identical.
func TestServerSenderIncarnationRestarts(t *testing.T) {
	cfgPath := t.TempDir() + "/cluster.json"
	const restarts = 5
	const id = 4242
	var first types.Event
	for i := uint64(1); i <= restarts; i++ {
		tr := &ownerTransport{events: make(chan types.Event, 1024)}
		s := New(NewOptions(WithNodeId(2), WithInitNodes(map[uint64]string{1: "", 2: ""}), WithConfigPath(cfgPath), WithTransport(tr)))
		require.Zero(t, s.SenderIncarnation())
		require.NoError(t, s.Start())
		require.Equal(t, i, s.SenderIncarnation(), "restart %d", i)
		req, result := forwardOnce(t, s, tr, id)
		require.EqualValues(t, 1, req.Logs[0].Term, "token version")
		require.Equal(t, i, req.Logs[0].Index, "forwarded under the reserved incarnation")
		require.EqualValues(t, 1, req.Logs[0].Id, "attempt numbers restart with the process")
		if i == 1 {
			first = req
		} else {
			// The delayed incarnation-1 response collides on ID and attempt.
			s.StepRaftEvent(echoResponse(t, first, types.ReasonOk, id))
			require.Eventually(t, func() bool { return s.ForwardIdentityStats().StaleResponses == 1 }, 5*time.Second, time.Millisecond)
			require.EqualValues(t, 1, s.ForwardIdentityStats().Pending)
			select {
			case err := <-result:
				t.Fatalf("stale incarnation settled the caller: %v", err)
			default:
			}
		}
		s.StepRaftEvent(echoResponse(t, req, types.ReasonError, id))
		err := ownerRecv(t, result)
		require.ErrorIs(t, err, types.ErrProposalOutcomeUnknown)
		require.ErrorIs(t, err, types.ErrForwardRejected)
		st := s.ForwardIdentityStats()
		require.Zero(t, st.Pending)
		require.EqualValues(t, 1, st.Registered)
		require.EqualValues(t, 1, st.Retired)
		require.Zero(t, st.UnreservedRejected)
		stopServer(t, s)
		data, found := storedIncarnation(t, cfgPath)
		require.True(t, found)
		require.Equal(t, incarnationBytes(i), data)
	}
}

// A second owner of a live data directory fails on the directory lock before it
// reserves or forwards, in this process and in another process.
func TestServerSenderIncarnationSecondOwner(t *testing.T) {
	cfgPath := t.TempDir() + "/cluster.json"
	s1 := New(NewOptions(WithNodeId(2), WithInitNodes(map[uint64]string{1: "", 2: ""}), WithConfigPath(cfgPath), WithTransport(&ownerTransport{events: make(chan types.Event, 1024)})))
	require.NoError(t, s1.Start())
	require.EqualValues(t, 1, s1.SenderIncarnation())

	tr := &ownerTransport{events: make(chan types.Event, 16)}
	s2 := New(NewOptions(WithNodeId(2), WithInitNodes(map[uint64]string{1: "", 2: ""}), WithConfigPath(cfgPath), WithTransport(tr)))
	err := s2.Start()
	require.Error(t, err)
	require.Contains(t, err.Error(), "lock")
	require.Zero(t, s2.SenderIncarnation())
	require.Nil(t, s2.raft, "no Raft, so nothing can forward")
	require.Empty(t, tr.events)
	stopServer(t, s2)

	cmd := exec.Command(os.Args[0], "-test.run=^TestSenderIncarnationHelperProcess$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "CLUSTERCONFIG_INCARNATION_HELPER="+cfgPath)
	out, err := cmd.CombinedOutput()
	t.Logf("second process output:\n%s", out)
	require.NoError(t, err)
	require.Contains(t, string(out), "HELPER start failed:")
	require.Contains(t, string(out), "HELPER lock-held=true")
	require.Contains(t, string(out), "HELPER incarnation=0")

	stopServer(t, s1)
	data, found := storedIncarnation(t, cfgPath)
	require.True(t, found)
	require.Equal(t, incarnationBytes(1), data, "the losing owners reserved nothing")
}

// TestSenderIncarnationHelperProcess is the second process of
// TestServerSenderIncarnationSecondOwner; it is a no-op otherwise.
func TestSenderIncarnationHelperProcess(t *testing.T) {
	cfgPath := os.Getenv("CLUSTERCONFIG_INCARNATION_HELPER")
	if cfgPath == "" {
		t.Skip("helper process only")
	}
	tr := &ownerTransport{events: make(chan types.Event, 16)}
	s := New(NewOptions(WithNodeId(2), WithInitNodes(map[uint64]string{1: "", 2: ""}), WithConfigPath(cfgPath), WithTransport(tr)))
	err := s.Start()
	require.Error(t, err)
	require.Empty(t, tr.events)
	require.Nil(t, s.raft)
	t.Logf("HELPER start failed: %v", err)
	// Another process holds the directory flock.
	t.Logf("HELPER lock-held=%t", errors.Is(err, syscall.EWOULDBLOCK))
	t.Logf("HELPER incarnation=%d", s.SenderIncarnation())
	stopServer(t, s)
}

type incarnationFaultStorage struct {
	*PebbleShardLogStorage
	readErr, writeErr error
}

func (s *incarnationFaultStorage) senderIncarnation() ([]byte, bool, error) {
	if s.readErr != nil {
		return nil, false, s.readErr
	}
	return s.PebbleShardLogStorage.senderIncarnation()
}

func (s *incarnationFaultStorage) saveSenderIncarnation(data []byte) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	return s.PebbleShardLogStorage.saveSenderIncarnation(data)
}

// Every reservation failure fails Start before Raft exists: nothing is sent,
// no incarnation is exposed, Storage is released and the stored value is
// unchanged.
func TestServerSenderIncarnationFailClosed(t *testing.T) {
	readErr := errors.New("injected read failure")
	writeErr := errors.New("injected sync write failure")
	for name, tc := range map[string]struct {
		seed              []byte
		readErr, writeErr error
		want              error
	}{
		"readError":  {readErr: readErr, want: readErr},
		"syncWrite":  {writeErr: writeErr, want: writeErr},
		"corrupt":    {seed: []byte{1, 2, 3}, want: ErrSenderIncarnationCorrupt},
		"zero":       {seed: incarnationBytes(0), want: ErrSenderIncarnationCorrupt},
		"wrap":       {seed: incarnationBytes(math.MaxUint64), want: ErrSenderIncarnationExhausted},
		"seededNext": {seed: incarnationBytes(7)},
	} {
		t.Run(name, func(t *testing.T) {
			cfgPath := t.TempDir() + "/cluster.json"
			if tc.seed != nil {
				seedIncarnation(t, cfgPath, tc.seed)
			}
			tr := &ownerTransport{events: make(chan types.Event, 1024)}
			s := New(NewOptions(WithNodeId(2), WithInitNodes(map[uint64]string{1: "", 2: ""}), WithConfigPath(cfgPath), WithTransport(tr)))
			s.storage = &incarnationFaultStorage{PebbleShardLogStorage: s.storage.(*PebbleShardLogStorage), readErr: tc.readErr, writeErr: tc.writeErr}
			err := s.Start()
			if tc.want == nil {
				require.NoError(t, err)
				require.EqualValues(t, 8, s.SenderIncarnation())
				stopServer(t, s)
				data, _ := storedIncarnation(t, cfgPath)
				require.Equal(t, incarnationBytes(8), data)
				return
			}
			require.ErrorIs(t, err, tc.want)
			require.Zero(t, s.SenderIncarnation())
			require.Nil(t, s.raft, "no Raft, so nothing can forward")
			require.Empty(t, tr.events)
			stopServer(t, s)
			data, found := storedIncarnation(t, cfgPath)
			require.Equal(t, tc.seed, data)
			require.Equal(t, tc.seed != nil, found)
		})
	}
}
