package client

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/internal/testfixture"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

// Regression coverage for the wkserver message fixtures: authentication
// readiness, rejected-Send accounting and bounded delivery waits. Ordering is
// driven by observed state; timeouts only guard against deadlock.

const shortWait = 300 * time.Millisecond

// dialObserver wraps the client's gnet event handler to expose a
// deterministic "first dial failed" barrier.
//
// OnTick starts a dial only when the status is disconnect, and a started dial
// holds the status at connecting until it fails (back to disconnect) or
// succeeds. Seeing disconnect at the start of two ticks therefore proves that
// the first dial was attempted and failed.
type dialObserver struct {
	*clientEvent
	disconnectTicks atomic.Int32
	failedDialOnce  sync.Once
	failedDial      chan struct{}
}

func (o *dialObserver) OnTick() (time.Duration, gnet.Action) {
	if o.c.conn().status.Load() == disconnect && o.disconnectTicks.Inc() >= 2 {
		o.failedDialOnce.Do(func() { close(o.failedDial) })
	}
	return o.clientEvent.OnTick()
}

// newClient creates a client and registers its teardown before anything else
// can fail, so the client is torn down on every path, including when it is
// never started. Teardown is idempotent: Stop closes the gnet poller and its
// descriptor first, then the ants pool is released. The returned function runs
// the same teardown explicitly.
func newClient(tb testing.TB, addr string, opts ...Option) (*Client, func()) {
	tb.Helper()
	c := New(addr, opts...)
	var once sync.Once
	stop := func() {
		once.Do(func() {
			c.Stop()
			c.pool.Release()
		})
	}
	tb.Cleanup(stop)
	return c, stop
}

// newObservedClient creates a client whose event handler is wrapped by a
// dialObserver. The client is not started.
func newObservedClient(t *testing.T, addr string) (*Client, *dialObserver, func()) {
	t.Helper()
	c, stop := newClient(t, addr, WithUid("uid"))
	obs := &dialObserver{clientEvent: c.event, failedDial: make(chan struct{})}
	unused := c.cli
	gc, err := gnet.NewClient(obs, gnet.WithTicker(true), gnet.WithMulticore(true))
	require.NoError(t, err)
	c.cli = gc
	require.NoError(t, unused.Stop()) // release the never-started poller
	return c, obs, stop
}

func startClient(tb testing.TB, c *Client) {
	tb.Helper()
	require.NoError(tb, c.Start())
}

type testServer struct {
	*wkserver.Server
	addr     string
	tracker  *testfixture.Tracker
	stopOnce sync.Once

	connMu sync.Mutex
	conn   gnet.Conn
}

func newTestServer(addr string, poolOn bool) *testServer {
	ts := &testServer{
		Server:  wkserver.New(addr, wkserver.WithMessagePoolOn(poolOn)),
		addr:    addr,
		tracker: testfixture.NewTracker(),
	}
	ts.OnMessage(func(conn gnet.Conn, m *proto.Message) {
		ts.connMu.Lock()
		ts.conn = conn
		ts.connMu.Unlock()
		ts.tracker.Received(m.Id)
	})
	return ts
}

// start starts the server and registers its teardown before waiting for the
// listener, so the server is stopped even if readiness fails.
func (ts *testServer) start(tb testing.TB) {
	tb.Helper()
	require.NoError(tb, ts.Start())
	tb.Cleanup(func() {
		ts.stop()
		assert.NoError(tb, testfixture.WaitAddrReleased(ts.addr, testfixture.DefaultReadyTimeout))
	})
	testfixture.RequireListening(tb, ts.addr)
}

// stop is idempotent so explicit shutdown and cleanup can both call it.
func (ts *testServer) stop() {
	ts.stopOnce.Do(ts.Stop)
}

func (ts *testServer) lastConn() gnet.Conn {
	ts.connMu.Lock()
	defer ts.connMu.Unlock()
	return ts.conn
}

// send registers the obligation before Send and balances it on rejection.
func send(c *Client, tr *testfixture.Tracker, id uint64) error {
	tr.Expect(id)
	err := c.Send(&proto.Message{Id: id, MsgType: 1, Content: []byte(fmt.Sprintf("m-%d", id))})
	if err != nil {
		tr.Cancel(id)
	}
	return err
}

func requireSent(tb testing.TB, c *Client, tr *testfixture.Tracker, id uint64) {
	tb.Helper()
	if err := send(c, tr, id); err != nil {
		tb.Fatalf("send %d rejected: %v (pending=%d)", id, err, tr.Pending())
	}
}

func requireRejected(t *testing.T, c *Client, tr *testfixture.Tracker, id uint64, msg string) {
	t.Helper()
	before := tr.Pending()
	err := send(c, tr, id)
	require.EqualError(t, err, msg)
	require.Equal(t, before, tr.Pending(), "rejected send must not leave an obligation")
}

func TestFixtureFailedFirstDialThenAuth(t *testing.T) {
	for _, poolOn := range []bool{true, false} {
		t.Run(fmt.Sprintf("pool_%v", poolOn), func(t *testing.T) {
			addr := testfixture.FreeAddr(t)
			c, obs, stop := newObservedClient(t, addr)
			startClient(t, c)

			// No listener exists yet, so the first dial must fail.
			_, err := testfixture.Receive(obs.failedDial, testfixture.DefaultReadyTimeout)
			require.NoError(t, err, "first dial failure not observed")
			require.False(t, c.IsAuthed())

			// The schedule that hung the legacy fixtures: Send before auth.
			ts := newTestServer(addr, poolOn)
			requireRejected(t, c, ts.tracker, 0, "connect is not connected")

			ts.start(t)
			testfixture.RequireAuthed(t, c.IsAuthed)

			requireSent(t, c, ts.tracker, 1)
			require.NoError(t, ts.tracker.Wait(testfixture.DefaultDeliveryTimeout))
			stop()
			require.NoError(t, ts.tracker.Verify())
		})
	}
}

// connGate controls the server's CONNECT handler.
type connGate struct {
	mode     atomic.String // "withhold", "reject" or "accept"
	attempts chan struct{}
}

func newConnGate(ts *testServer, mode string) *connGate {
	g := &connGate{attempts: make(chan struct{}, 64)}
	g.mode.Store(mode)
	ts.Route("/conn", func(ctx *wkserver.Context) {
		select {
		case g.attempts <- struct{}{}:
		default:
		}
		switch g.mode.Load() {
		case "withhold":
		case "reject":
			ctx.WriteConnack(&proto.Connack{Id: ctx.ConnReq().Id, Status: proto.StatusError})
		default:
			ctx.WriteConnack(&proto.Connack{Id: ctx.ConnReq().Id, Status: proto.StatusOK})
		}
	})
	return g
}

func TestFixtureAuthWithheldThenReleased(t *testing.T) {
	addr := testfixture.FreeAddr(t)
	ts := newTestServer(addr, false)
	gate := newConnGate(ts, "withhold")
	ts.start(t)

	c, stop := newClient(t, addr, WithUid("uid"))
	startClient(t, c)

	_, err := testfixture.Receive(gate.attempts, testfixture.DefaultReadyTimeout)
	require.NoError(t, err, "CONNECT never reached the server")

	// No CONNACK is ever written while withheld, so the bounded wait must
	// fail cleanly instead of hanging.
	require.EqualError(t, testfixture.WaitAuthed(c.IsAuthed, shortWait),
		fmt.Sprintf("client not authenticated within %s", shortWait))
	requireRejected(t, c, ts.tracker, 0, "connect is not connected")

	gate.mode.Store("accept")
	testfixture.RequireAuthed(t, c.IsAuthed)
	requireSent(t, c, ts.tracker, 1)
	require.NoError(t, ts.tracker.Wait(testfixture.DefaultDeliveryTimeout))
	stop()
	require.NoError(t, ts.tracker.Verify())
}

func TestFixtureAuthFailedThenAccepted(t *testing.T) {
	addr := testfixture.FreeAddr(t)
	ts := newTestServer(addr, false)
	gate := newConnGate(ts, "reject")
	ts.start(t)

	c, stop := newClient(t, addr, WithUid("uid"))
	startClient(t, c)

	// Two rejected CONNECTs prove the client stays unauthenticated and retries.
	for i := 0; i < 2; i++ {
		_, err := testfixture.Receive(gate.attempts, testfixture.DefaultReadyTimeout)
		require.NoError(t, err, "CONNECT attempt %d never reached the server", i+1)
	}
	require.Error(t, testfixture.WaitAuthed(c.IsAuthed, shortWait))
	requireRejected(t, c, ts.tracker, 0, "connect is not connected")

	gate.mode.Store("accept")
	testfixture.RequireAuthed(t, c.IsAuthed)
	requireSent(t, c, ts.tracker, 1)
	require.NoError(t, ts.tracker.Wait(testfixture.DefaultDeliveryTimeout))
	stop()
	require.NoError(t, ts.tracker.Verify())
}

func TestFixtureRejectedSendLeavesNoObligation(t *testing.T) {
	// Never started: teardown still closes the poller and releases the pool.
	c, _ := newClient(t, testfixture.FreeAddr(t))

	tr := testfixture.NewTracker()
	for id := uint64(0); id < 100; id++ {
		requireRejected(t, c, tr, id, "connect is not connected")
	}
	require.Equal(t, 0, tr.Pending())
	require.NoError(t, tr.Wait(time.Millisecond))

	// Without balancing, the same schedule is a bounded, diagnosed failure
	// rather than a hang.
	tr.Expect(100)
	err := tr.Wait(20 * time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing=1 [100]")
}

func TestFixtureSendWriteError(t *testing.T) {
	// Never started: teardown still closes the poller and releases the pool.
	c, _ := newClient(t, testfixture.FreeAddr(t))

	// Authenticated status but no transport: the write path rejects.
	c.conn().status.Store(authed)
	tr := testfixture.NewTracker()
	requireRejected(t, c, tr, 0, "conn is nil")
	require.NoError(t, tr.Wait(time.Millisecond))
}

func TestFixtureSendAfterClose(t *testing.T) {
	for _, poolOn := range []bool{true, false} {
		t.Run(fmt.Sprintf("pool_%v", poolOn), func(t *testing.T) {
			addr := testfixture.FreeAddr(t)
			ts := newTestServer(addr, poolOn)
			ts.start(t)

			c, stop := newClient(t, addr, WithUid("uid"))
			startClient(t, c)
			testfixture.RequireAuthed(t, c.IsAuthed)

			requireSent(t, c, ts.tracker, 1)
			require.NoError(t, ts.tracker.Wait(testfixture.DefaultDeliveryTimeout))

			// Client-side close.
			c.CloseConn()
			requireRejected(t, c, ts.tracker, 2, "connect is not connected")
			testfixture.RequireAuthed(t, c.IsAuthed)
			requireSent(t, c, ts.tracker, 3)
			require.NoError(t, ts.tracker.Wait(testfixture.DefaultDeliveryTimeout))

			// Server-side close of the connection that delivered id 3.
			sc := ts.lastConn()
			require.NotNil(t, sc)
			require.NoError(t, sc.Close())
			require.NoError(t, testfixture.WaitFor(func() bool { return !c.IsAuthed() }, testfixture.DefaultReadyTimeout))
			requireRejected(t, c, ts.tracker, 4, "connect is not connected")
			testfixture.RequireAuthed(t, c.IsAuthed)
			requireSent(t, c, ts.tracker, 5)
			require.NoError(t, ts.tracker.Wait(testfixture.DefaultDeliveryTimeout))

			stop()
			require.NoError(t, ts.tracker.Verify())
		})
	}
}

func TestFixtureConcurrentTopology(t *testing.T) {
	const pairs = 4
	const msgCount = 10000
	for i := 0; i < pairs; i++ {
		poolOn := i%2 == 0
		t.Run(fmt.Sprintf("pair_%d_pool_%v", i, poolOn), func(t *testing.T) {
			t.Parallel()
			addr := testfixture.FreeAddr(t)
			ts := newTestServer(addr, poolOn)
			ts.start(t)

			c, stop := newClient(t, addr, WithUid("uid"))
			startClient(t, c)
			testfixture.RequireAuthed(t, c.IsAuthed)

			for id := uint64(0); id < msgCount; id++ {
				requireSent(t, c, ts.tracker, id)
			}
			require.NoError(t, ts.tracker.Wait(testfixture.DefaultDeliveryTimeout))
			stop()
			require.NoError(t, ts.tracker.Verify())
		})
	}
}

// gnetGoroutines counts goroutines running gnet event loops or tickers.
// Every client and server in this package is stopped by its test, so the
// count must drop to zero once they have shut down.
func gnetGoroutines() int {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "github.com/panjf2000/gnet/v2.") {
			n++
		}
	}
	return n
}

// openFDs counts this process's open file descriptors.
func openFDs(t *testing.T) int {
	t.Helper()
	d, err := os.Open("/dev/fd")
	require.NoError(t, err, "cannot count open descriptors")
	defer d.Close()
	// Names only: stat-ing each entry would trip over transient descriptors.
	names, err := d.Readdirnames(-1)
	require.NoError(t, err, "cannot count open descriptors")
	return len(names)
}

// requireNoLiveResources checks that addr is released, no gnet goroutine is
// left and the descriptor count is back to at most before plus slack.
func requireNoLiveResources(t *testing.T, addr string, fdsBefore, slack int) {
	t.Helper()
	require.NoError(t, testfixture.WaitAddrReleased(addr, testfixture.DefaultReadyTimeout))
	var live int
	err := testfixture.WaitFor(func() bool {
		live = gnetGoroutines()
		return live == 0
	}, testfixture.DefaultReadyTimeout)
	require.NoError(t, err, "%d gnet goroutines still running after stop", live)
	var fds int
	err = testfixture.WaitFor(func() bool {
		fds = openFDs(t)
		return fds <= fdsBefore+slack
	}, testfixture.DefaultReadyTimeout)
	require.NoError(t, err, "open descriptors grew from %d to %d", fdsBefore, fds)
}

// Stages at which cleanupScenario can be forced to fail.
const (
	failNone      = ""
	failReadiness = "readiness"
	failSend      = "send"
	failDelivery  = "delivery"
)

// cleanupScenario runs one client/server exchange. Client and server teardown
// is registered on tb as soon as each is constructed or started, so it runs
// even when a readiness, Send or delivery assertion ends the test early.
// failAt makes one of those assertions fail deterministically; with failNone
// the scenario shuts both down explicitly and returns normally.
func cleanupScenario(tb testing.TB, addr string, failAt string) {
	ts := newTestServer(addr, true)
	gate := newConnGate(ts, "accept")
	if failAt == failReadiness {
		gate.mode.Store("withhold")
	}
	ts.start(tb)

	c, stopClient := newClient(tb, addr, WithUid("uid"))
	startClient(tb, c)

	readyTimeout := testfixture.DefaultReadyTimeout
	if failAt == failReadiness {
		readyTimeout = shortWait
	}
	if err := testfixture.WaitAuthed(c.IsAuthed, readyTimeout); err != nil {
		tb.Fatal(err)
	}

	if failAt == failSend {
		// Reconnects are never acknowledged, so Send stays rejected.
		gate.mode.Store("withhold")
		c.CloseConn()
	}
	requireSent(tb, c, ts.tracker, 1)

	deliveryTimeout := testfixture.DefaultDeliveryTimeout
	if failAt == failDelivery {
		// An obligation for an ID that is never sent cannot complete.
		ts.tracker.Expect(2)
		deliveryTimeout = shortWait
	}
	require.NoError(tb, ts.tracker.Wait(deliveryTimeout))

	stopClient()
	ts.stop()
	require.NoError(tb, ts.tracker.Verify())
}

func TestFixtureCleanup(t *testing.T) {
	addr := testfixture.FreeAddr(t)
	fdsBefore := openFDs(t)

	// Explicit shutdown inside the scenario must release everything before
	// any registered cleanup runs.
	t.Run("explicit", func(t *testing.T) {
		cleanupScenario(t, addr, failNone)
		requireNoLiveResources(t, addr, fdsBefore, 4)
	})
	requireNoLiveResources(t, addr, fdsBefore, 4)
}

// recordingTB lets a scenario fail without failing the enclosing test. It
// records failures and cleanups; the embedded TB serves everything else.
type recordingTB struct {
	testing.TB

	mu       sync.Mutex
	failures []string
	cleanups []func()
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Cleanup(f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, f)
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Error(args ...any) { r.Errorf("%s", fmt.Sprint(args...)) }
func (r *recordingTB) Fatal(args ...any) { r.Error(args...); r.FailNow() }
func (r *recordingTB) FailNow()          { runtime.Goexit() }

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.Errorf(format, args...)
	r.FailNow()
}

func (r *recordingTB) Failed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.failures) > 0
}

// run executes fn like a test body and then its cleanups in reverse order.
func (r *recordingTB) run(t *testing.T, fn func(tb testing.TB)) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(r)
	}()
	_, err := testfixture.Receive(done, testfixture.DefaultDeliveryTimeout)
	require.NoError(t, err, "scenario did not finish")
	for i := len(r.cleanups) - 1; i >= 0; i-- {
		r.cleanups[i]()
	}
}

func TestFixtureCleanupOnFailure(t *testing.T) {
	cases := []struct {
		failAt string
		want   string
	}{
		{failReadiness, fmt.Sprintf("client not authenticated within %s", shortWait)},
		{failSend, "send 1 rejected: connect is not connected"},
		{failDelivery, "missing=1 [2]"},
	}
	for _, tc := range cases {
		t.Run(tc.failAt, func(t *testing.T) {
			addr := testfixture.FreeAddr(t)
			fdsBefore := openFDs(t)

			r := &recordingTB{TB: t}
			r.run(t, func(tb testing.TB) { cleanupScenario(tb, addr, tc.failAt) })

			// Exactly the forced failure; teardown itself reported nothing.
			require.Len(t, r.failures, 1, "failures: %q", r.failures)
			require.Contains(t, r.failures[0], tc.want)
			// Client teardown and server teardown were both registered.
			require.Len(t, r.cleanups, 2)
			requireNoLiveResources(t, addr, fdsBefore, 4)
		})
	}
}

// Never-started clients must not leak their gnet poller. Each leaked poller
// holds one descriptor, so repeating the never-started fixtures exposes a
// leak as descriptor growth.
func TestFixtureNeverStartedClientsReleaseDescriptors(t *testing.T) {
	const rounds = 50
	before := openFDs(t)
	for i := 0; i < rounds; i++ {
		t.Run("rejected_send", TestFixtureRejectedSendLeavesNoObligation)
		t.Run("write_error", TestFixtureSendWriteError)
	}
	after := openFDs(t)
	require.LessOrEqual(t, after-before, 4,
		"open descriptors grew from %d to %d over %d rounds", before, after, rounds)
}
