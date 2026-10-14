package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type handoffOwner struct {
	done chan struct{}
	err  error
}

func (o *handoffOwner) WaitStopped(ctx context.Context) error {
	select {
	case <-o.done:
		return o.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestServerShutdownConfigOwnership(t *testing.T) {
	for _, mode := range []string{"success", "timeout", "close error"} {
		t.Run(mode, func(t *testing.T) {
			owner := &handoffOwner{done: make(chan struct{})}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			closed := false
			finish := func() error { closed = true; return nil }
			sentinel := errors.New("storage close failed")
			if mode != "timeout" {
				if mode == "close error" {
					owner.err = sentinel
				}
				close(owner.done)
			}
			err := stopConfigOwner(ctx, owner, finish)
			switch mode {
			case "success":
				require.NoError(t, err)
				require.True(t, closed)
			case "close error":
				require.ErrorIs(t, err, sentinel)
				require.False(t, closed)
			case "timeout":
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.False(t, closed, "dependent resources must remain owned")
				close(owner.done)
				require.NoError(t, stopConfigOwner(context.Background(), owner, finish))
				require.True(t, closed)
			}
		})
	}
}
