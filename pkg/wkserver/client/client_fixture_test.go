package client

import (
	"fmt"
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

// newObservedClient creates a client whose event handler is wrapped by a
// dialObserver. The client is not started.
func newObservedClient(t *testing.T, addr string) (*Client, *dialObserver) {
	t.Helper()
	c := New(addr, WithUid("uid"))
	obs := &dialObserver{clientEvent: c.event, failedDial: make(chan struct{})}
	unused := c.cli
	gc, err := gnet.NewClient(obs, gnet.WithTicker(true), gnet.WithMulticore(true))
	require.NoError(t, err)
	c.cli = gc
	require.NoError(t, unused.Stop()) // release the never-started poller
	t.Cleanup(c.pool.Release)
	return c, obs
}

func startClient(t *testing.T, c *Client) func() {
	t.Helper()
	require.NoError(t, c.Start())
	var once sync.Once
	stop := func() { once.Do(c.Stop) }
	t.Cleanup(stop)
	return stop
}

type testServer struct {
	*wkserver.Server
	addr    string
	tracker *testfixture.Tracker

	connMu sync.Mutex
	conn   gnet.Conn
}

func newTestServer(t *testing.T, addr string, poolOn bool) *testServer {
	t.Helper()
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

func (ts *testServer) start(t *testing.T) {
	t.Helper()
	require.NoError(t, ts.Start())
	t.Cleanup(func() {
		ts.Stop()
		assert.NoError(t, testfixture.WaitAddrReleased(ts.addr, testfixture.DefaultReadyTimeout))
	})
	testfixture.RequireListening(t, ts.addr)
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

func requireSent(t *testing.T, c *Client, tr *testfixture.Tracker, id uint64) {
	t.Helper()
	if err := send(c, tr, id); err != nil {
		t.Fatalf("send %d rejected: %v (pending=%d)", id, err, tr.Pending())
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
			c, obs := newObservedClient(t, addr)
			stop := startClient(t, c)

			// No listener exists yet, so the first dial must fail.
			_, err := testfixture.Receive(obs.failedDial, testfixture.DefaultReadyTimeout)
			require.NoError(t, err, "first dial failure not observed")
			require.False(t, c.IsAuthed())

			// The schedule that hung the legacy fixtures: Send before auth.
			ts := newTestServer(t, addr, poolOn)
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
	ts := newTestServer(t, addr, false)
	gate := newConnGate(ts, "withhold")
	ts.start(t)

	c := New(addr, WithUid("uid"))
	t.Cleanup(c.pool.Release)
	stop := startClient(t, c)

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
	ts := newTestServer(t, addr, false)
	gate := newConnGate(ts, "reject")
	ts.start(t)

	c := New(addr, WithUid("uid"))
	t.Cleanup(c.pool.Release)
	stop := startClient(t, c)

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
	c := New(testfixture.FreeAddr(t))
	defer c.cancel()
	defer c.pool.Release()

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
	c := New(testfixture.FreeAddr(t))
	defer c.cancel()
	defer c.pool.Release()

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
			ts := newTestServer(t, addr, poolOn)
			ts.start(t)

			c := New(addr, WithUid("uid"))
			t.Cleanup(c.pool.Release)
			stop := startClient(t, c)
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
			ts := newTestServer(t, addr, poolOn)
			ts.start(t)

			c := New(addr, WithUid("uid"))
			t.Cleanup(c.pool.Release)
			stop := startClient(t, c)
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

func TestFixtureCleanup(t *testing.T) {
	addr := testfixture.FreeAddr(t)

	func() {
		ts := newTestServer(t, addr, true)
		require.NoError(t, ts.Start())
		testfixture.RequireListening(t, addr)

		c := New(addr, WithUid("uid"))
		defer c.pool.Release()
		require.NoError(t, c.Start())
		testfixture.RequireAuthed(t, c.IsAuthed)
		requireSent(t, c, ts.tracker, 1)
		require.NoError(t, ts.tracker.Wait(testfixture.DefaultDeliveryTimeout))

		c.Stop()
		ts.Stop()
		require.NoError(t, ts.tracker.Verify())
	}()

	require.NoError(t, testfixture.WaitAddrReleased(addr, testfixture.DefaultReadyTimeout))
	var live int
	err := testfixture.WaitFor(func() bool {
		live = gnetGoroutines()
		return live == 0
	}, testfixture.DefaultReadyTimeout)
	require.NoError(t, err, "%d gnet goroutines still running after stop", live)
}
