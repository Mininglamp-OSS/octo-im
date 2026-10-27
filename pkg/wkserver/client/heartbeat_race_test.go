package client

import (
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

func TestHeartbeatCountersConcurrentTrafficAndTick(t *testing.T) {
	c := New("tcp://127.0.0.1:1", func(o *Options) { o.HeartbeatTick = 1 << 30; o.HeartbeatTimeoutTick = 1 << 30 })
	defer c.cancel()
	defer c.pool.Release()
	// Exercise the actual traffic reset and ticker concurrently without opening
	// a network connection or reaching heartbeat/reconnect thresholds.
	var workers sync.WaitGroup
	workers.Add(2)
	start := make(chan struct{})
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 10000; i++ {
			c.conn().tick()
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 10000; i++ {
			c.handleData(nil, proto.MsgTypeHeartbeat, "")
		}
	}()
	close(start)
	workers.Wait()
}

func TestHeartbeatTrafficExtendsConnectionDeadline(t *testing.T) {
	c := New("tcp://127.0.0.1:1", func(o *Options) { o.HeartbeatTick = 2; o.HeartbeatTimeoutTick = 2 })
	defer c.cancel()
	defer c.pool.Release()
	c.conn().status.Store(authed)
	c.conn().tick()
	require.True(t, c.IsAuthed())
	c.handleData(nil, proto.MsgTypeHeartbeat, "")
	c.conn().tick()
	require.True(t, c.IsAuthed(), "received traffic resets the idle deadline")
	c.conn().tick()
	require.False(t, c.IsAuthed(), "a peer with no subsequent traffic must reconnect")
}
