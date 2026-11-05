package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
	es "github.com/panjf2000/gnet/v2/pkg/errors"

	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type connStatus uint8

const (
	disconnect connStatus = iota // 断开
	connecting                   // 连接中
	connected                    // 已连接
	authing                      // 认证中
	authed                       //已认证

)

type conn struct {
	status      atomic.Value // 是否已认证
	c           *Client
	addr        string // 服务端地址
	mu          sync.Mutex
	gc          gnet.Conn
	idleTick    int // 空闲tick数
	timeoutTick int

	generation uint64
	attempt    *connAttempt // protected with gc, status transitions and heartbeat ticks by mu

	wklog.Log
}

func newConn(addr string, c *Client) *conn {
	cn := &conn{
		c:    c,
		Log:  wklog.NewWKLog(fmt.Sprintf("conn[%s]", addr)),
		addr: addr,
	}

	cn.status.Store(disconnect)

	return cn
}

func (c *conn) asyncWrite(b []byte) error {
	c.mu.Lock()
	status, gc := c.status.Load(), c.gc
	c.mu.Unlock()
	if status != authed {
		return errors.New("connect is not connected")
	}
	if gc == nil {
		return errors.New("conn is nil")
	}
	return gc.AsyncWrite(b, nil)
}

func (c *conn) transport() gnet.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gc
}

// Reserve the generation before scheduling work. CloseConn can invalidate even
// an attempt whose goroutine has not started yet.
func (c *conn) prepareDial() *connAttempt {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status.Load() != disconnect || c.c.cancelCtx.Err() != nil {
		return nil
	}
	c.generation++
	ctx, cancel := context.WithCancel(c.c.cancelCtx)
	a := &connAttempt{generation: c.generation, ctx: ctx, cancel: cancel}
	c.attempt = a
	c.status.Store(connecting)
	return a
}

func (c *conn) startDial(a *connAttempt) {
	if a.ctx.Err() != nil {
		return
	}
	var network, addr string
	var err error
	if strings.Contains(c.addr, "://") {
		network, addr, err = parseProtoAddr(c.addr)
		if err != nil {
			c.Error("parse addr failed", zap.Error(err), zap.String("addr", c.addr))
			c.disconnectAttempt(a)
			return
		}
	} else {
		network, addr = "tcp", c.addr
	}

	// gnet.DialContext's context is connection metadata, not cancellation.
	ctx, cancel := context.WithTimeout(a.ctx, 2*time.Second)
	defer cancel()
	raw, err := c.c.dialContext(ctx, network, addr)
	if err != nil {
		c.disconnectAttempt(a)
		return
	}
	if a.ctx.Err() != nil {
		_ = raw.Close()
		c.disconnectAttempt(a)
		return
	}
	// EnrollContext owns raw. Capture the attempt before the blocking call;
	// an old dial must never inherit a newer connection's identity.
	gc, err := c.c.cli.EnrollContext(raw, connCtx{attempt: a})
	if err != nil {
		c.disconnectAttempt(a)
		return
	}
	c.mu.Lock()
	if c.attempt != a || a.ctx.Err() != nil || c.status.Load() != connecting {
		c.mu.Unlock()
		_ = gc.Close()
		return
	}
	c.gc = gc
	c.status.Store(connected)
	c.mu.Unlock()
	c.Foucs("conn success")
}

func (c *conn) prepareAuth() (*connAttempt, gnet.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status.Load() != connected || c.attempt == nil || c.attempt.ctx.Err() != nil {
		return nil, nil
	}
	c.status.Store(authing)
	return c.attempt, c.gc
}

func (c *conn) startAuth(a *connAttempt, gc gnet.Conn) {
	err := c.sendAuth(a, gc)
	if err != nil {
		c.Foucs("auth failed", zap.Error(err))
		c.disconnectAttempt(a)
		return
	}
	c.mu.Lock()
	if c.attempt == a && a.ctx.Err() == nil && c.status.Load() == authing {
		c.idleTick, c.timeoutTick = 0, 0
		c.status.Store(authed)
		c.mu.Unlock()
		c.Foucs("auth success")
		return
	}
	c.mu.Unlock()
}

func (c *conn) sendAuth(a *connAttempt, gc gnet.Conn) error {
	if err := a.ctx.Err(); err != nil {
		return err
	}
	conn := &proto.Connect{
		Id:    c.c.reqIDGen.Inc(),
		Uid:   c.c.opts.Uid,
		Token: c.c.opts.Token,
	}
	data, err := conn.Marshal()
	if err != nil {
		c.Error("conn auth failed", zap.Error(err))
		return err
	}
	msgData, err := c.c.proto.Encode(data, proto.MsgTypeConnect)
	if err != nil {
		c.Error("conn auth failed", zap.Error(err))
		return err
	}

	waitC := c.c.w.Register(conn.Id)
	defer c.c.w.Trigger(conn.Id, nil)
	if gc == nil {
		return errors.New("conn is nil")
	}
	err = gc.AsyncWrite(msgData, nil)
	if err != nil {
		c.Error("write failed", zap.Error(err))
		return err
	}

	timeoutCtx, cancel := context.WithTimeout(a.ctx, time.Second*4)
	defer cancel()
	select {
	case x := <-waitC:
		if x == nil {
			return errors.New("unknown error")
		}
		ack := x.(*proto.Connack)
		if ack.Status != proto.StatusOK {
			return fmt.Errorf("connect error：%d", ack.Status)
		}
		return nil
	case <-timeoutCtx.Done():
		return timeoutCtx.Err()
	}
}

func (c *conn) tick() {
	c.mu.Lock()
	if c.status.Load() != authed {
		c.mu.Unlock()
		return
	}
	c.idleTick++
	c.timeoutTick++
	if c.timeoutTick >= c.c.opts.HeartbeatTimeoutTick {
		gc := c.disconnectLocked()
		c.mu.Unlock()
		if gc != nil {
			_ = gc.Close()
		}
		c.Foucs("node client heartbeat timeout,reconnect")
		return
	}
	gc := c.gc
	heartbeat := c.idleTick >= c.c.opts.HeartbeatTick
	if heartbeat {
		c.idleTick = 0
	}
	c.mu.Unlock()
	if heartbeat {
		c.sendHeartbeat(gc)
	}
}

// 发送心跳
func (c *conn) sendHeartbeat(gc gnet.Conn) {
	data, err := c.c.proto.Encode([]byte{proto.MsgTypeHeartbeat.Uint8()}, proto.MsgTypeHeartbeat)
	if err != nil {
		c.Warn("encode heartbeat error", zap.Error(err))
		return
	}
	if gc == nil {
		c.Warn("send heartbeat failed, conn is nil")
		return
	}
	err = gc.AsyncWrite(data, nil)
	if err != nil {
		c.Warn("send heartbeat error", zap.Error(err))
		return
	}
}

// Caller holds mu. Invalidate the attempt before closing the transport, so a
// delayed OnClose, dial or authentication completion cannot affect its successor.
func (c *conn) disconnectLocked() gnet.Conn {
	if c.attempt != nil {
		c.attempt.cancel()
		c.attempt = nil
	}
	gc := c.gc
	c.gc = nil
	c.idleTick, c.timeoutTick = 0, 0
	c.status.Store(disconnect)
	return gc
}

func (c *conn) disconnectAttempt(a *connAttempt) {
	c.mu.Lock()
	if a != nil && c.attempt != a {
		c.mu.Unlock()
		return
	}
	gc := c.disconnectLocked()
	c.mu.Unlock()
	if gc != nil {
		_ = gc.Close()
	}
}

func (c *conn) close(err error) {
	c.disconnectAttempt(nil)
	c.Foucs("client close", zap.String("id", c.c.opts.Uid), zap.String("addr", c.addr), zap.Error(err))
}

func (c *conn) currentAttempt(a *connAttempt) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return a != nil && c.attempt == a && a.ctx.Err() == nil
}

func (c *conn) receive(a *connAttempt) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if a == nil || c.attempt != a || a.ctx.Err() != nil {
		return false
	}
	c.idleTick, c.timeoutTick = 0, 0
	return true
}

func (c *conn) onTraffic(gc gnet.Conn, a *connAttempt) gnet.Action {
	if gc.InboundBuffered() == 0 {
		return gnet.None
	}
	batchCount := 0
	for i := 0; i < c.c.batchRead; i++ {
		if gc.InboundBuffered() == 0 {
			break
		}
		data, msgType, _, err := c.c.proto.Decode(gc)
		if err == io.ErrShortBuffer { // 表示数据不够了
			break
		}
		if err != nil {
			c.Foucs("decode error", zap.Error(err))
			return gnet.Close
		}
		if len(data) == 0 {
			break
		}
		if !c.receive(a) {
			return gnet.Close
		}
		batchCount++
		remoteAddr := gc.RemoteAddr().String()
		err = c.c.pool.Submit(func() {
			// An admitted reply may still complete its original request after
			// disconnect. Request IDs are unique across connection generations.
			c.c.handleData(data, msgType, remoteAddr)
		})
		if err != nil {
			c.Error("submit failed", zap.Error(err))
		}
	}

	if batchCount == c.c.batchRead && gc.InboundBuffered() > 0 {
		if gc.InboundBuffered() > 1024*1024 {
			c.Warn("client: inbound buffered is too large", zap.Int("buffered", gc.InboundBuffered()))
		}
		if err := gc.Wake(nil); err != nil { // 这里调用wake避免丢失剩余的数据
			c.Error("failed to wake up the connection", zap.Error(err))
			return gnet.Close
		}
	}
	return gnet.None
}

func parseProtoAddr(protoAddr string) (string, string, error) {
	protoAddr = strings.ToLower(protoAddr)
	if strings.Count(protoAddr, "://") != 1 {
		return "", "", es.ErrInvalidNetworkAddress
	}
	pair := strings.SplitN(protoAddr, "://", 2)
	proto, addr := pair[0], pair[1]
	switch proto {
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6", "unix":
	default:
		return "", "", es.ErrUnsupportedProtocol
	}
	if addr == "" {
		return "", "", es.ErrInvalidNetworkAddress
	}
	return proto, addr, nil
}

// Immutable identity and cancellation shared by dial, auth and gnet callbacks.
type connAttempt struct {
	generation uint64
	ctx        context.Context
	cancel     context.CancelFunc
}

type connCtx struct {
	attempt *connAttempt
}
