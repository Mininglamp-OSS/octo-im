package manager

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type retrySessionUsers struct {
	eventbus.IUser
	conn   *eventbus.Conn
	events []*eventbus.Event
}

func (u *retrySessionUsers) ConnById(string, uint64, int64) *eventbus.Conn { return u.conn }
func (u *retrySessionUsers) AddEvent(_ string, event *eventbus.Event) {
	u.events = append(u.events, event)
}

func newRetryManagerForSessionTest() *RetryManager {
	r := NewRetryManager()
	for i := range r.retryQueues {
		r.retryQueues[i] = NewRetryQueue(i, r)
	}
	return r
}

func TestRetrySkipsReusedConnectionIdFromAnotherSession(t *testing.T) {
	oldOptions, oldUser := options.G, eventbus.User
	t.Cleanup(func() {
		options.G, eventbus.User = oldOptions, oldUser
	})
	options.G = options.New()
	users := &retrySessionUsers{conn: &eventbus.Conn{
		Uid: "u", NodeId: 2, ConnId: 7, OwnerBootID: "new-boot", SessionID: "new-session",
	}}
	eventbus.RegisterUser(users)
	r := newRetryManagerForSessionTest()
	msg := &types.RetryMessage{
		Uid: "u", FromNode: 2, ConnId: 7, OwnerBootID: "old-boot", SessionID: "old-session",
		MessageId: 9, RecvPacket: &wkproto.RecvPacket{Payload: []byte("plaintext")},
	}

	r.retry(msg)

	require.Empty(t, users.events)
	require.Zero(t, r.RetryMessageCount(), "a stale-session retry must not be rescheduled")
}

func TestRetryKeepsMatchingSession(t *testing.T) {
	oldOptions, oldUser := options.G, eventbus.User
	t.Cleanup(func() {
		options.G, eventbus.User = oldOptions, oldUser
	})
	options.G = options.New()
	conn := &eventbus.Conn{
		Uid: "u", NodeId: 2, ConnId: 7, OwnerBootID: "boot", SessionID: "session",
	}
	users := &retrySessionUsers{conn: conn}
	eventbus.RegisterUser(users)
	r := newRetryManagerForSessionTest()
	packet := &wkproto.RecvPacket{Payload: []byte("plaintext")}
	msg := &types.RetryMessage{
		Uid: "u", FromNode: 2, ConnId: 7, OwnerBootID: "boot", SessionID: "session",
		MessageId: 9, RecvPacket: packet,
	}

	r.retry(msg)

	require.Equal(t, 1, r.RetryMessageCount())
	require.Len(t, users.events, 1)
	require.Same(t, conn, users.events[0].Conn)
	require.Same(t, packet, users.events[0].Frame)
}

func TestRetryKeepsMatchingLegacySocketBirth(t *testing.T) {
	oldOptions, oldUser := options.G, eventbus.User
	t.Cleanup(func() {
		options.G, eventbus.User = oldOptions, oldUser
	})
	options.G = options.New()
	conn := &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, Uptime: 101}
	users := &retrySessionUsers{conn: conn}
	eventbus.RegisterUser(users)
	r := newRetryManagerForSessionTest()
	packet := &wkproto.RecvPacket{Payload: []byte("plaintext")}
	msg := &types.RetryMessage{
		Uid: "u", FromNode: 2, ConnId: 7, Uptime: 101,
		MessageId: 9, RecvPacket: packet,
	}

	r.retry(msg)

	require.Equal(t, 1, r.RetryMessageCount())
	require.Len(t, users.events, 1)
	require.Same(t, conn, users.events[0].Conn)
	require.Same(t, packet, users.events[0].Frame)
}

func TestRetryRejectsReusedLegacyConnectionID(t *testing.T) {
	oldOptions, oldUser := options.G, eventbus.User
	t.Cleanup(func() {
		options.G, eventbus.User = oldOptions, oldUser
	})
	options.G = options.New()
	users := &retrySessionUsers{conn: &eventbus.Conn{Uid: "u", NodeId: 2, ConnId: 7, Uptime: 102}}
	eventbus.RegisterUser(users)
	r := newRetryManagerForSessionTest()

	r.retry(&types.RetryMessage{
		Uid: "u", FromNode: 2, ConnId: 7, Uptime: 101,
		MessageId: 9, RecvPacket: &wkproto.RecvPacket{Payload: []byte("plaintext")},
	})

	require.Empty(t, users.events)
	require.Zero(t, r.RetryMessageCount())
}
