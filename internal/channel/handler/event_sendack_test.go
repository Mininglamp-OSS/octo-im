package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type sendAckUser struct {
	eventbus.IUser
	events   []*eventbus.Event
	advances int
}

func (u *sendAckUser) AddEvent(_ string, event *eventbus.Event) { u.events = append(u.events, event) }
func (u *sendAckUser) Advance(string)                           { u.advances++ }

func TestSendAckSkipsAmbiguousPersistenceOutcome(t *testing.T) {
	oldOptions, oldUser := options.G, eventbus.User
	t.Cleanup(func() { options.G, eventbus.User = oldOptions, oldUser })
	options.G = options.New()
	u := &sendAckUser{}
	eventbus.RegisterUser(u)
	h := &Handler{}
	conn := &eventbus.Conn{Uid: "u"}

	h.sendack(&eventbus.ChannelContext{Events: []*eventbus.Event{
		{Conn: conn, Frame: &wkproto.SendPacket{ClientSeq: 1}, PersistenceOutcomeUnknown: true},
	}})

	require.Empty(t, u.events)
	require.Zero(t, u.advances)
}
