package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestDialHasDeadlineAndRetries(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	calls := 0
	c.dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 2*time.Second {
			t.Fatal("dial must have bounded deadline")
		}
		return nil, errors.New("unreachable")
	}
	for i := 0; i < 2; i++ {
		c.conn().startDial(c.conn().prepareDial())
		if c.conn().status.Load() != disconnect {
			t.Fatal("failed dial prevents retry")
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}
func TestStopCancelsBlockedDial(t *testing.T) {
	c := New("127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()
	entered := make(chan struct{})
	done := make(chan struct{})
	c.dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	a := c.conn().prepareDial()
	go func() { c.conn().startDial(a); close(done) }()
	<-entered
	c.cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("dial not cancelled")
	}
}
