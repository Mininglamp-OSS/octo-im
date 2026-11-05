package client

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/require"
)

func waitAttempt(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("connection attempt did not finish")
	}
}

var generationLoggerOnce sync.Once

func newGenerationClient(t *testing.T, handler gnet.EventHandler) (*Client, net.Listener) {
	t.Helper()
	generationLoggerOnce.Do(func() { wklog.Configure(wklog.NewOptions()) })
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	c := New(l.Addr().String())
	// Keep real dialing/enrollment, but schedule generations explicitly.
	if handler == nil {
		handler = c.event
	}
	c.cli, err = gnet.NewClient(handler)
	require.NoError(t, err)
	require.NoError(t, c.Start())
	t.Cleanup(func() {
		if c.cancelCtx.Err() == nil {
			c.Stop()
		}
		c.pool.Release()
	})
	return c, l
}

func acceptGenerationConn(t *testing.T, l net.Listener) net.Conn {
	t.Helper()
	require.NoError(t, l.(*net.TCPListener).SetDeadline(time.Now().Add(3*time.Second)))
	gc, err := l.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { _ = gc.Close() })
	return gc
}

func requirePeerClosed(t *testing.T, peer net.Conn) {
	t.Helper()
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(3*time.Second)))
	_, err := peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "superseded connection must be closed")
}

func TestStaleDialCannotReplaceNewerConnection(t *testing.T) {
	for _, success := range []bool{false, true} {
		name := "late_failure"
		if success {
			name = "late_success"
		}
		t.Run(name, func(t *testing.T) {
			c, l := newGenerationClient(t, nil)
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var calls atomic.Int64
			c.dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
				if calls.Add(1) != 1 {
					return (&net.Dialer{}).DialContext(ctx, network, addr)
				}
				raw, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				close(entered)
				<-release // Deliberately return a result after cancellation.
				if !success && err == nil {
					_ = raw.Close()
					return nil, errors.New("old attempt failed")
				}
				return raw, err
			}
			cn := c.conn()
			old := cn.prepareDial()
			go func() { cn.startDial(old); close(done) }()
			waitAttempt(t, entered)
			oldPeer := acceptGenerationConn(t, l)
			c.CloseConn()
			require.ErrorIs(t, old.ctx.Err(), context.Canceled)
			current := cn.prepareDial()
			require.Greater(t, current.generation, old.generation)
			cn.startDial(current)
			_ = acceptGenerationConn(t, l)
			newer := cn.transport()
			require.NotNil(t, newer)
			require.Equal(t, connected, cn.status.Load())
			unblock()
			waitAttempt(t, done)
			require.Same(t, newer, cn.transport())
			require.Equal(t, connected, cn.status.Load())
			identity := make(chan *connAttempt, 1)
			require.NoError(t, newer.Wake(func(gc gnet.Conn, err error) error {
				identity <- gc.Context().(connCtx).attempt
				return nil
			}))
			select {
			case actual := <-identity:
				require.Same(t, current, actual)
			case <-time.After(3 * time.Second):
				t.Fatal("connection identity unavailable")
			}
			requirePeerClosed(t, oldPeer)
		})
	}
}

type blockedOpenEvent struct {
	gnet.BuiltinEventEngine
	opened  chan gnet.Conn
	release chan struct{}
}

func (e *blockedOpenEvent) OnOpen(gc gnet.Conn) ([]byte, gnet.Action) {
	e.opened <- gc
	<-e.release
	return nil, gnet.None
}

func TestCloseDuringEnrollmentDiscardsOldTransport(t *testing.T) {
	e := &blockedOpenEvent{opened: make(chan gnet.Conn, 2), release: make(chan struct{})}
	c, l := newGenerationClient(t, e)
	var once sync.Once
	unblock := func() { once.Do(func() { close(e.release) }) }
	t.Cleanup(unblock)
	a := c.conn().prepareDial()
	done := make(chan struct{})
	go func() { c.conn().startDial(a); close(done) }()
	select {
	case <-e.opened:
	case <-time.After(3 * time.Second):
		t.Fatal("connection not enrolled")
	}
	oldPeer := acceptGenerationConn(t, l)
	c.CloseConn()
	unblock()
	waitAttempt(t, done)
	require.Nil(t, c.conn().transport())
	require.Equal(t, disconnect, c.conn().status.Load())
	requirePeerClosed(t, oldPeer)
	b := c.conn().prepareDial()
	c.conn().startDial(b)
	newer := c.conn().transport()
	require.NotNil(t, newer)
	// Even a close callback delayed until after B is connected belongs to A.
	c.event.OnClose(&generationConn{attempt: a}, nil)
	require.Same(t, newer, c.conn().transport())
	require.Equal(t, connected, c.conn().status.Load())
}

func TestCancelledScheduledDialDoesNotStart(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	var calls int
	c.dialContext = func(context.Context, string, string) (net.Conn, error) {
		calls++
		return nil, errors.New("unreachable")
	}
	a := c.conn().prepareDial()
	require.Nil(t, c.conn().prepareDial(), "only one current attempt may be reserved")
	c.CloseConn()
	b := c.conn().prepareDial()
	c.conn().startDial(a)
	require.Zero(t, calls)
	require.Equal(t, connecting, c.conn().status.Load())
	c.conn().startDial(b)
	require.Equal(t, 1, calls)
	require.Equal(t, disconnect, c.conn().status.Load())
}

// A scripted transport can pause an auth completion at an exact boundary without
// sleeping or depending on a remote server's scheduler.
type generationConn struct {
	gnet.Conn
	attempt *connAttempt
	write   func([]byte) error
	closed  atomic.Int64
}

func (c *generationConn) Context() any                                    { return connCtx{attempt: c.attempt} }
func (c *generationConn) Close() error                                    { c.closed.Add(1); return nil }
func (c *generationConn) AsyncWrite(b []byte, _ gnet.AsyncCallback) error { return c.write(b) }
func (c *generationConn) InboundBuffered() int                            { return 0 }

func installGeneration(t *testing.T, c *Client, write func([]byte) error) (*connAttempt, *generationConn) {
	t.Helper()
	cn := c.conn()
	a := cn.prepareDial()
	require.NotNil(t, a)
	gc := &generationConn{attempt: a, write: write}
	cn.mu.Lock()
	cn.gc = gc
	cn.status.Store(connected)
	cn.mu.Unlock()
	return a, gc
}

func authID(t *testing.T, data []byte) uint64 {
	t.Helper()
	req := &proto.Connect{}
	err := req.Unmarshal(data[proto.MagicNumberStartLength+proto.MsgTypeLength+proto.MsgContentLength:])
	require.NoError(t, err)
	return req.Id
}

func TestStaleAuthCannotChangeNewerConnection(t *testing.T) {
	for _, authErr := range []bool{false, true} {
		name := "late_success"
		if authErr {
			name = "late_failure"
		}
		t.Run(name, func(t *testing.T) {
			c := New("127.0.0.1:1")
			defer c.cancel()
			defer c.pool.Release()
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var oldID uint64
			_, oldGC := installGeneration(t, c, func(data []byte) error {
				oldID = authID(t, data)
				// A successful reply is already queued before the connection is
				// invalidated, but the auth goroutine has not published it yet.
				if !authErr {
					c.w.Trigger(oldID, &proto.Connack{Id: oldID, Status: proto.StatusOK})
				}
				close(entered)
				<-release
				if authErr {
					return errors.New("old auth failed")
				}
				return nil
			})
			a, gc := c.conn().prepareAuth()
			go func() { c.conn().startAuth(a, gc); close(done) }()
			waitAttempt(t, entered)
			c.CloseConn()
			_, newGC := installGeneration(t, c, func(data []byte) error {
				id := authID(t, data)
				c.w.Trigger(id, &proto.Connack{Id: id, Status: proto.StatusOK})
				return nil
			})
			b, bgc := c.conn().prepareAuth()
			unblock()
			waitAttempt(t, done)
			require.Same(t, newGC, c.conn().transport())
			require.Equal(t, authing, c.conn().status.Load(), "A must not authenticate or disconnect B")
			require.EqualValues(t, 1, oldGC.closed.Load())
			require.False(t, c.w.IsRegistered(oldID), "old auth wait entry must be released")
			c.conn().startAuth(b, bgc)
			require.True(t, c.IsAuthed(), "B must still authenticate normally")
		})
	}
}

func TestOldCallbacksCannotResetNewHeartbeatOrCloseConnection(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	_, oldGC := installGeneration(t, c, nil)
	c.CloseConn()
	_, newGC := installGeneration(t, c, nil)
	cn := c.conn()
	cn.mu.Lock()
	cn.status.Store(authed)
	cn.idleTick, cn.timeoutTick = 5, 7
	cn.mu.Unlock()
	c.event.OnClose(oldGC, nil)
	require.Equal(t, gnet.Close, c.event.OnTraffic(oldGC))
	require.Equal(t, 5, cn.idleTick)
	require.Equal(t, 7, cn.timeoutTick)
	require.True(t, c.IsAuthed())
	require.Zero(t, newGC.closed.Load())
	c.event.OnClose(newGC, nil)
	require.False(t, c.IsAuthed())
	require.Nil(t, cn.transport())
}

func TestCloseCancelsAuthAndReleasesWaiter(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	written := make(chan uint64, 1)
	_, gc := installGeneration(t, c, func(data []byte) error { written <- authID(t, data); return nil })
	a, gcAuth := c.conn().prepareAuth()
	done := make(chan struct{})
	go func() { c.conn().startAuth(a, gcAuth); close(done) }()
	var id uint64
	select {
	case id = <-written:
	case <-time.After(3 * time.Second):
		t.Fatal("auth not written")
	}
	c.CloseConn()
	waitAttempt(t, done)
	require.False(t, c.w.IsRegistered(id))
	require.EqualValues(t, 1, gc.closed.Load())
	require.Equal(t, disconnect, c.conn().status.Load())
}

func TestStopInvalidatesConnectionAndPreventsNewDial(t *testing.T) {
	c, l := newGenerationClient(t, nil)
	a := c.conn().prepareDial()
	c.conn().startDial(a)
	peer := acceptGenerationConn(t, l)
	c.Stop()
	require.Equal(t, disconnect, c.conn().status.Load())
	require.Nil(t, c.conn().transport())
	require.Nil(t, c.conn().prepareDial())
	requirePeerClosed(t, peer)
}

func TestCloseCancelsBlockedDialImmediately(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	entered, done := make(chan struct{}), make(chan struct{})
	c.dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	a := c.conn().prepareDial()
	go func() { c.conn().startDial(a); close(done) }()
	waitAttempt(t, entered)
	c.CloseConn()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("CloseConn must cancel dialing without waiting for the dial deadline")
	}
	require.Equal(t, disconnect, c.conn().status.Load())
}

func TestCancelledScheduledAuthDoesNotWriteToSuccessor(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	var writes int
	_, oldGC := installGeneration(t, c, func([]byte) error { writes++; return nil })
	a, gc := c.conn().prepareAuth()
	c.CloseConn()
	_, newGC := installGeneration(t, c, func([]byte) error { writes++; return nil })
	c.conn().startAuth(a, gc)
	require.Zero(t, writes)
	require.Same(t, newGC, c.conn().transport())
	require.EqualValues(t, 1, oldGC.closed.Load())
	require.Equal(t, connected, c.conn().status.Load())
}

func TestHeartbeatTimeoutClosesOnlyCurrentGeneration(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	var heartbeats int
	a, gc := installGeneration(t, c, func([]byte) error { heartbeats++; return nil })
	cn := c.conn()
	cn.status.Store(authed)
	for i := 0; i < c.opts.HeartbeatTick; i++ {
		cn.tick()
	}
	require.Equal(t, 1, heartbeats)
	require.True(t, cn.receive(a), "a valid current-generation packet resets the timeout")
	for i := 0; i < c.opts.HeartbeatTimeoutTick-1; i++ {
		cn.tick()
	}
	require.True(t, c.IsAuthed())
	cn.tick()
	require.False(t, c.IsAuthed())
	require.EqualValues(t, 1, gc.closed.Load())
	require.ErrorIs(t, a.ctx.Err(), context.Canceled)
	require.NotNil(t, cn.prepareDial(), "heartbeat failure must permit reconnect")
}

func TestConcurrentDialReservationsHaveOneWinner(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	var wg sync.WaitGroup
	var winners atomic.Int64
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.conn().prepareDial() != nil {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, winners.Load())
	c.CloseConn()
}

func TestAdmittedReplyCompletesOriginalWaitWithoutTouchingSuccessor(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	installGeneration(t, c, nil)
	id := c.reqIDGen.Inc()
	wait := c.w.Register(id)
	defer c.w.Trigger(id, nil)
	body, err := (&proto.Response{Id: id, Status: proto.StatusOK, Body: []byte("old reply")}).Marshal()
	require.NoError(t, err)
	// The packet was decoded before CloseConn, but its pool worker runs after B
	// connects. It must complete A's request without resetting B's heartbeat.
	c.CloseConn()
	_, newGC := installGeneration(t, c, nil)
	cn := c.conn()
	cn.idleTick, cn.timeoutTick = 5, 7
	c.handleData(body, proto.MsgTypeResp, "127.0.0.1:1")
	select {
	case x := <-wait:
		require.Equal(t, []byte("old reply"), x.(*proto.Response).Body)
	default:
		t.Fatal("an already received response was discarded")
	}
	require.Same(t, newGC, cn.transport())
	require.Equal(t, connected, cn.status.Load())
	require.Equal(t, 5, cn.idleTick)
	require.Equal(t, 7, cn.timeoutTick)
}

func TestInFlightWriteStaysOnItsAuthenticatedConnection(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	_, oldGC := installGeneration(t, c, func([]byte) error { close(entered); <-release; return nil })
	c.conn().status.Store(authed)
	done := make(chan error, 1)
	go func() { done <- c.Write([]byte("old write")) }()
	waitAttempt(t, entered)
	closed := make(chan struct{})
	go func() { c.CloseConn(); close(closed) }()
	waitAttempt(t, closed)
	var newWrites int
	_, newGC := installGeneration(t, c, func([]byte) error { newWrites++; return nil })
	unblock()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("write did not finish")
	}
	require.Zero(t, newWrites, "A's admitted write must not leak onto unauthenticated B")
	require.EqualValues(t, 1, oldGC.closed.Load())
	require.Same(t, newGC, c.conn().transport())
	require.EqualError(t, c.Write([]byte("new write")), "connect is not connected")
}
