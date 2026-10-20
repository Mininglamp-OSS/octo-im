package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
	"github.com/stretchr/testify/require"
)

func TestChannelHandlerTimeoutClockRegression(t *testing.T) {
	old := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = old })
	timeout := int64(options.G.Poller.ChannelTimeout.Seconds())

	for _, tc := range []struct {
		name    string
		offset  int64
		expired bool
	}{
		{name: "fresh"},
		{name: "one_second_ahead", offset: 1},
		{name: "clock_rollback", offset: 600},
		{name: "before_timeout", offset: -timeout + 1},
		{name: "at_timeout", offset: -timeout},
		{name: "past_timeout", offset: -timeout - 1, expired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Check exact boundaries within one cached-clock tick. Retry only
			// when its background ticker advanced during the observation.
			for attempt := 0; attempt < 10; attempt++ {
				now := fasttime.UnixTimestamp()
				c := &channelHandler{lastActive: uint64(int64(now) + tc.offset)}
				got := c.isTimeout()
				if fasttime.UnixTimestamp() != now {
					continue
				}
				require.Equal(t, tc.expired, got)
				return
			}
			t.Fatal("cached clock changed during every timeout observation")
		})
	}
}

func TestChannelPollerClearClockRegression(t *testing.T) {
	old := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = old })

	for _, tc := range []struct {
		name   string
		offset int64
		remove bool
	}{
		{name: "fresh"},
		{name: "clock_rollback", offset: 600},
		{name: "genuinely_idle", offset: -int64(options.G.Poller.ChannelTimeout.Seconds()) - 1, remove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &poller{waitlist: newLinkedList(), eventPool: &EventPool{}}
			c := newChannelHandler("expiry-channel", 2, p)
			var pending *eventbus.Event
			if !tc.remove {
				pending = &eventbus.Event{Type: eventbus.EventChannelOnSend}
				c.addEvent(pending)
			}
			p.waitlist.push(c)
			// Assign after addEvent, which refreshes the handler timestamp.
			c.lastActive = uint64(int64(fasttime.UnixTimestamp()) + tc.offset)
			p.tickCount = options.G.Poller.ClearIntervalTick - 1
			p.tick()

			if tc.remove {
				require.Nil(t, p.handler(c.channelId, c.channelType), "an idle handler must still be removed")
				return
			}
			require.Same(t, c, p.handler(c.channelId, c.channelType), "the handler must survive the sweep")
			require.True(t, c.hasEvent(), "queued work must remain reachable after the sweep")
			events := c.events()
			require.Len(t, events, 1)
			require.Same(t, pending, events[0])
		})
	}
}
