package client

import (
	"time"

	"github.com/panjf2000/gnet/v2"
)

type clientEvent struct {
	c *Client
}

func (c *clientEvent) OnTick() (delay time.Duration, action gnet.Action) {

	delay = 100 * time.Millisecond
	for _, conn := range c.c.conns {

		if a := conn.prepareDial(); a != nil {
			go conn.connect(a)
		}
		conn.tick()
	}
	return
}

func (c *clientEvent) OnClose(gc gnet.Conn, err error) (action gnet.Action) {
	if ctx, ok := gc.Context().(connCtx); ok && ctx.attempt != nil {
		c.c.conn().disconnectAttempt(ctx.attempt)
	}
	return
}

func (c *clientEvent) OnTraffic(gc gnet.Conn) (action gnet.Action) {
	ctx, ok := gc.Context().(connCtx)
	if !ok || !c.c.conn().currentAttempt(ctx.attempt) {
		return gnet.Close
	}
	return c.c.conn().onTraffic(gc, ctx.attempt)
}

func (c *clientEvent) OnShutdown(gnet.Engine) {
	c.c.Foucs("gnet shutdown")
}

func (c *clientEvent) OnBoot(eng gnet.Engine) (action gnet.Action) {
	c.c.eng = eng
	return
}

func (c *clientEvent) OnOpen(gc gnet.Conn) (out []byte, action gnet.Action) {
	return
}
