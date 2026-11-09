package raftgroup

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func awaitOwnerStopResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("owner request did not finish")
		return nil
	}
}

func awaitOwnerStopSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("owner lifecycle barrier was not reached")
	}
}

func TestOwnerStopRejectsUnacceptedRequestAndCompletesAcceptedWork(t *testing.T) {
	g := New(NewOptions(WithTickInterval(time.Hour)))
	require.NoError(t, g.Start())
	t.Cleanup(g.Stop)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	want := errors.New("accepted callback completed")
	accepted := make(chan error, 1)
	go func() {
		accepted <- g.Do(context.Background(), "slot", func(IRaft) error {
			close(entered)
			<-release
			return want
		})
	}()
	awaitOwnerStopSignal(t, entered)

	// The owner is executing the first callback and cannot receive this one.
	// Stop must release its sender without ever accepting or running it.
	var unacceptedRan atomic.Bool
	unaccepted := make(chan error, 1)
	go func() {
		unaccepted <- g.Do(context.Background(), "other-slot", func(IRaft) error {
			unacceptedRan.Store(true)
			return nil
		})
	}()
	stopped := make(chan struct{})
	go func() { g.Stop(); close(stopped) }()
	awaitOwnerStopSignal(t, g.stopper.ShouldStop())
	require.ErrorIs(t, awaitOwnerStopResult(t, unaccepted), ErrGroupStopped)
	require.False(t, unacceptedRan.Load())
	select {
	case err := <-accepted:
		t.Fatalf("accepted request returned before its callback completed: %v", err)
	case <-stopped:
		t.Fatal("Stop returned while the owner callback was still running")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	require.ErrorIs(t, awaitOwnerStopResult(t, accepted), want)
	awaitOwnerStopSignal(t, stopped)
	require.False(t, unacceptedRan.Load(), "a rejected sender must not run after shutdown")
	require.ErrorIs(t, g.Do(context.Background(), "after-stop", func(IRaft) error {
		t.Error("stopped owner executed a new request")
		return nil
	}), ErrGroupStopped)
}

func TestOwnerCancellationWaitsForAcceptedCallback(t *testing.T) {
	g := New(NewOptions(WithTickInterval(time.Hour)))
	require.NoError(t, g.Start())
	t.Cleanup(g.Stop)
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	want := errors.New("callback result after cancellation")
	result := make(chan error, 1)
	var captured int
	go func() {
		result <- g.Do(ctx, "slot", func(IRaft) error {
			close(entered)
			<-release
			captured = 42
			close(finished)
			return want
		})
	}()
	awaitOwnerStopSignal(t, entered)
	cancel()
	select {
	case err := <-result:
		t.Fatalf("cancellation let an accepted callback outlive its caller: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	require.ErrorIs(t, awaitOwnerStopResult(t, result), want)
	awaitOwnerStopSignal(t, finished)
	require.Equal(t, 42, captured, "callback results remain safe to consume after Do returns")
}

type ownerStopRemovalEvent struct {
	entered chan struct{}
	release chan struct{}
}

func (*ownerStopRemovalEvent) OnAddRaft(IRaft) {}
func (e *ownerStopRemovalEvent) OnRemoveRaft(IRaft) {
	close(e.entered)
	<-e.release
}

func TestOwnerStopCompletesAcceptedRemoval(t *testing.T) {
	event := &ownerStopRemovalEvent{entered: make(chan struct{}), release: make(chan struct{})}
	g := New(NewOptions(WithTickInterval(time.Hour), WithEvent(event)))
	r := &lifecycleRaft{waitTestRaft: &waitTestRaft{}}
	g.AddRaft(r)
	require.NoError(t, g.Start())
	t.Cleanup(g.Stop)
	unblock := sync.OnceFunc(func() { close(event.release) })
	t.Cleanup(unblock)
	result := make(chan error, 1)
	go func() {
		result <- g.Do(context.Background(), "slot", func(current IRaft) error {
			g.RemoveRaft(current)
			return nil
		})
	}()
	awaitOwnerStopSignal(t, event.entered)
	stopped := make(chan struct{})
	go func() { g.Stop(); close(stopped) }()
	awaitOwnerStopSignal(t, g.stopper.ShouldStop())
	select {
	case err := <-result:
		t.Fatalf("removal returned while its accepted callback remained active: %v", err)
	case <-stopped:
		t.Fatal("Stop returned before the accepted removal callback completed")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	require.NoError(t, awaitOwnerStopResult(t, result))
	awaitOwnerStopSignal(t, stopped)
	require.Nil(t, g.GetRaft("slot"))
}
