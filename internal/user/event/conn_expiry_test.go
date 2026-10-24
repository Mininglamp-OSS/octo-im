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

func TestUserTickPreservesExpiryCadence(t *testing.T) {
	previous := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = previous })

	for _, replaceBeforeCheck := range []bool{false, true} {
		name := "expired_and_live_connections"
		if replaceBeforeCheck {
			name = "refreshed_connection_before_check"
		}
		t.Run(name, func(t *testing.T) {
			u := &userHandler{Uid: "expiry-cadence", conns: newConns()}
			u.pending.eventQueue = eventbus.NewEventQueue("expiry-cadence")
			now := fasttime.UnixTimestamp()
			expired := &eventbus.Conn{
				Uid: u.Uid, Auth: true, NodeId: 1, ConnId: 1,
				LastActive: now - uint64(options.G.ConnIdleTime.Seconds()) - 1,
			}
			u.conns.add(expired)
			u.conns.add(&eventbus.Conn{
				Uid: u.Uid, Auth: true, NodeId: 1, ConnId: 2, LastActive: now,
			})
			u.conns.add(&eventbus.Conn{
				Uid: u.Uid, Auth: true, NodeId: 1, ConnId: 3, LastActive: now + 600,
			})
			for i := 0; i < 9; i++ {
				u.tick()
				require.Zero(t, u.pending.eventQueue.Len(), "expiry must wait for the tenth tick")
			}
			if replaceBeforeCheck {
				u.conns.updateConn(expired.ConnId, expired.NodeId, &eventbus.Conn{
					Uid: u.Uid, Auth: true, NodeId: 1, ConnId: 1, LastActive: now,
				})
			}
			u.tick()
			if replaceBeforeCheck {
				require.Zero(t, u.pending.eventQueue.Len(), "expiry must inspect the current connection snapshot")
				return
			}
			events := u.pending.eventQueue.Slice(1, u.pending.eventQueue.LastIndex()+1)
			require.Len(t, events, 1, "only the idle connection should be removed")
			require.Equal(t, eventbus.EventConnRemove, events[0].Type)
			require.Same(t, expired, events[0].Conn)
		})
	}
}
