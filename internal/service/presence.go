package service

import (
	"context"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
)

// Presence derives user authority's routing view from authenticated physical sessions.
var Presence IPresence

type IPresence interface {
	Track(wknet.Conn)
	Prepare(wknet.Conn, *eventbus.Conn)
	Authenticate(*eventbus.Conn) bool
	Close(wknet.Conn)
	Forget(*eventbus.Conn)
	Recover(context.Context, []string) error
	Verify(context.Context, *eventbus.Conn) (*eventbus.Conn, error)
	SetRoutes()
	Run(context.Context)
}
