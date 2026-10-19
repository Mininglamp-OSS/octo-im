package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
	"github.com/stretchr/testify/require"
)

// A fresh recovery snapshot can be ahead of the expiry clock. It must not
// enqueue removal of a live session, including after a wall-clock rollback.
func TestCheckInvalidConnClockRegression(t *testing.T) {
	previous := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = previous })

	for _, tc := range []struct {
		name   string
		offset int64
		remove bool
	}{
		{name: "fresh", offset: 0},
		{name: "recovery_one_second_ahead", offset: 1},
		{name: "clock_rollback", offset: 600},
		{name: "genuinely_idle", offset: -int64(options.G.ConnIdleTime.Seconds()) - 1, remove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := &userHandler{Uid: "expiry-clock-test"}
			u.pending.eventQueue = eventbus.NewEventQueue("expiry-clock-test")
			conn := &eventbus.Conn{
				Uid: u.Uid, Auth: true, NodeId: 1, ConnId: 7,
				LastActive: uint64(int64(fasttime.UnixTimestamp()) + tc.offset),
			}
			u.checkInvalidConn([]*eventbus.Conn{conn})
			if !tc.remove {
				require.Zero(t, u.pending.eventQueue.Len(), "a fresh session must remain available for delivery")
				return
			}
			events := u.pending.eventQueue.Slice(1, u.pending.eventQueue.LastIndex()+1)
			require.Len(t, events, 1, "normal idle cleanup must still work")
			require.Equal(t, eventbus.EventConnRemove, events[0].Type)
			require.Same(t, conn, events[0].Conn)
		})
	}
}
