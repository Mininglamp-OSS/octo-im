package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// fakeOwner is a storage owner whose handoff is pending until done closes.
// Before that it reports incomplete quiescence; afterwards it returns err.
type fakeOwner struct {
	done       chan struct{}
	err        error
	incomplete error
	calls      int
}

func newFakeOwner(incomplete error) *fakeOwner {
	return &fakeOwner{done: make(chan struct{}), incomplete: incomplete}
}

func (o *fakeOwner) finish(err error) *fakeOwner {
	o.err = err
	close(o.done)
	return o
}

func (o *fakeOwner) stop(ctx context.Context) error {
	o.calls++
	select {
	case <-o.done:
		return o.err
	default:
	}
	select {
	case <-o.done:
		return o.err
	case <-ctx.Done():
		return fmt.Errorf("%w: %w", o.incomplete, ctx.Err())
	}
}

type shutdownFixture struct {
	config, slot, channel *fakeOwner
	listener              net.Listener
	independent, closes   int
	closeErr              error
	shutdown              *ownerShutdown
}

// newShutdownFixture wires fake owners and a real TCP listener as the
// storage-independent resource.
func newShutdownFixture(t *testing.T) *shutdownFixture {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &shutdownFixture{
		config:   newFakeOwner(raft.ErrIncompleteQuiescence),
		slot:     newFakeOwner(raftgroup.ErrIncompleteQuiescence),
		channel:  newFakeOwner(raftgroup.ErrIncompleteQuiescence),
		listener: ln,
	}
	f.shutdown = &ownerShutdown{
		config:  func(ctx context.Context) error { return f.config.stop(ctx) },
		slot:    func(ctx context.Context) error { return f.slot.stop(ctx) },
		channel: func(ctx context.Context) error { return f.channel.stop(ctx) },
		independent: func() {
			f.independent++
			require.NoError(t, f.listener.Close())
		},
		closeShared: func() error { f.closes++; return f.closeErr },
	}
	return f
}

func (f *shutdownFixture) stop(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return f.shutdown.stop(ctx)
}

// requireListenerReleased proves the independent teardown released the port.
func (f *shutdownFixture) requireListenerReleased(t *testing.T) {
	ln, err := net.Listen("tcp", f.listener.Addr().String())
	require.NoError(t, err, "storage-independent teardown must release the listener")
	require.NoError(t, ln.Close())
}

func TestServerShutdownOwnership(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	closeFailed := errors.New("storage close failed")
	sharedFailed := errors.New("shared db close failed")

	t.Run("normal", func(t *testing.T) {
		f := newShutdownFixture(t)
		f.config.finish(nil)
		f.slot.finish(nil)
		f.channel.finish(nil)
		require.NoError(t, f.stop(time.Second))
		require.NoError(t, f.stop(time.Second), "repeat Stop")
		require.Equal(t, []int{1, 1}, []int{f.independent, f.closes})
		f.requireListenerReleased(t)
	})

	t.Run("config timeout", func(t *testing.T) {
		f := newShutdownFixture(t)
		f.slot.finish(nil)
		f.channel.finish(nil)
		err := f.stop(20 * time.Millisecond)
		require.ErrorIs(t, err, raft.ErrIncompleteQuiescence)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, []int{0, 0}, []int{f.slot.calls, f.channel.calls}, "dependent owners are not stopped under live Config workers")
		require.Equal(t, []int{1, 0}, []int{f.independent, f.closes}, "independent teardown runs; shared storage stays open")
		f.requireListenerReleased(t)

		require.ErrorIs(t, f.stop(20*time.Millisecond), context.DeadlineExceeded, "repeat Stop while pending")
		require.Equal(t, []int{1, 0}, []int{f.independent, f.closes})
		f.config.finish(nil)
		require.NoError(t, f.stop(time.Second), "a later Stop finishes the pending handoff")
		require.Equal(t, []int{1, 1}, []int{f.slot.calls, f.channel.calls})
		require.Equal(t, []int{1, 1}, []int{f.independent, f.closes})
	})

	t.Run("config close error", func(t *testing.T) {
		// Workers exited but Close failed: dependent teardown proceeds and the
		// error is reported on every call.
		f := newShutdownFixture(t)
		f.config.finish(closeFailed)
		f.slot.finish(nil)
		f.channel.finish(nil)
		for i := 0; i < 2; i++ {
			err := f.stop(time.Second)
			require.ErrorIs(t, err, closeFailed)
			require.NotErrorIs(t, err, raft.ErrIncompleteQuiescence)
		}
		require.Equal(t, []int{1, 1}, []int{f.independent, f.closes})
		f.requireListenerReleased(t)
	})

	t.Run("slot timeout", func(t *testing.T) {
		f := newShutdownFixture(t)
		f.config.finish(nil)
		f.channel.finish(nil)
		err := f.stop(20 * time.Millisecond)
		require.ErrorIs(t, err, raftgroup.ErrIncompleteQuiescence)
		require.Equal(t, 1, f.channel.calls, "the channel lifecycle still starts")
		require.Equal(t, []int{1, 0}, []int{f.independent, f.closes}, "the shared DB stays open under live slot workers")
		f.requireListenerReleased(t)
		f.slot.finish(nil)
		require.NoError(t, f.stop(time.Second))
		require.Equal(t, []int{1, 1}, []int{f.independent, f.closes})
	})

	t.Run("channel timeout", func(t *testing.T) {
		f := newShutdownFixture(t)
		f.config.finish(nil)
		f.slot.finish(nil)
		err := f.stop(20 * time.Millisecond)
		require.ErrorIs(t, err, raftgroup.ErrIncompleteQuiescence)
		require.Equal(t, []int{1, 0}, []int{f.independent, f.closes})
		f.requireListenerReleased(t)
		f.channel.finish(nil)
		require.NoError(t, f.stop(time.Second))
		require.Equal(t, []int{1, 1}, []int{f.independent, f.closes})
	})

	t.Run("shared close error", func(t *testing.T) {
		f := newShutdownFixture(t)
		f.closeErr = sharedFailed
		f.config.finish(nil)
		f.slot.finish(nil)
		f.channel.finish(nil)
		require.ErrorIs(t, f.stop(time.Second), sharedFailed)
		require.ErrorIs(t, f.stop(time.Second), sharedFailed, "repeat Stop keeps the close result")
		require.Equal(t, []int{1, 1}, []int{f.independent, f.closes})
		f.requireListenerReleased(t)
	})
}
