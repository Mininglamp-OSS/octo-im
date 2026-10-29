// Package testfixture holds test-only helpers shared by the wkserver test
// suites. It must not be imported by production code.
//
// The helpers replace fixed sleeps with bounded waits on observable state and
// keep message accounting exact, so a fixture fails with a diagnostic instead
// of hanging until the package timeout.
package testfixture

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

const (
	// DefaultReadyTimeout bounds listener and authentication readiness waits.
	DefaultReadyTimeout = 10 * time.Second
	// DefaultDeliveryTimeout bounds final receive waits.
	DefaultDeliveryTimeout = 30 * time.Second

	pollInterval = 5 * time.Millisecond
)

// FreeAddr returns a "tcp://127.0.0.1:<port>" address whose port was free at
// the time of the call, so concurrently running packages do not share ports.
func FreeAddr(tb testing.TB) string {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve free port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		tb.Fatalf("release reserved port %s: %v", addr, err)
	}
	return "tcp://" + addr
}

// HostPort strips the "tcp://" scheme from a wkserver address.
func HostPort(addr string) string {
	if i := strings.Index(addr, "://"); i >= 0 {
		return addr[i+3:]
	}
	return addr
}

// WaitFor polls cond until it returns true or timeout elapses. The timeout is
// a deadlock guard only; ordering comes from the observed condition.
func WaitFor(cond func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met within %s", timeout)
		}
		time.Sleep(pollInterval)
	}
}

// WaitListening waits until addr accepts TCP connections.
func WaitListening(addr string, timeout time.Duration) error {
	hp := HostPort(addr)
	var lastErr error
	err := WaitFor(func() bool {
		c, err := net.DialTimeout("tcp", hp, 100*time.Millisecond)
		if err != nil {
			lastErr = err
			return false
		}
		_ = c.Close()
		return true
	}, timeout)
	if err != nil {
		return fmt.Errorf("listener %s not ready within %s: last dial error: %v", hp, timeout, lastErr)
	}
	return nil
}

// WaitAuthed waits until isAuthed reports an authenticated client.
func WaitAuthed(isAuthed func() bool, timeout time.Duration) error {
	if err := WaitFor(isAuthed, timeout); err != nil {
		return fmt.Errorf("client not authenticated within %s", timeout)
	}
	return nil
}

// WaitAddrReleased waits until addr can be bound again, proving the listener
// was closed.
func WaitAddrReleased(addr string, timeout time.Duration) error {
	hp := HostPort(addr)
	var lastErr error
	err := WaitFor(func() bool {
		l, err := net.Listen("tcp", hp)
		if err != nil {
			lastErr = err
			return false
		}
		lastErr = l.Close()
		return lastErr == nil
	}, timeout)
	if err != nil {
		return fmt.Errorf("listener %s still bound after %s: %v", hp, timeout, lastErr)
	}
	return nil
}

// RequireListening fails the test if addr does not accept connections in time.
func RequireListening(tb testing.TB, addr string) {
	tb.Helper()
	if err := WaitListening(addr, DefaultReadyTimeout); err != nil {
		tb.Fatal(err)
	}
}

// RequireAuthed fails the test if the client does not authenticate in time.
func RequireAuthed(tb testing.TB, isAuthed func() bool) {
	tb.Helper()
	if err := WaitAuthed(isAuthed, DefaultReadyTimeout); err != nil {
		tb.Fatal(err)
	}
}

// ErrNotReceived is returned by Receive when no value arrives in time.
var ErrNotReceived = errors.New("value not received")

// Receive waits for one value from ch, bounded by timeout.
func Receive[T any](ch <-chan T, timeout time.Duration) (T, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v, nil
	case <-timer.C:
		var zero T
		return zero, fmt.Errorf("%w within %s", ErrNotReceived, timeout)
	}
}
