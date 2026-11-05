package wkserver_test

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
)

var recoveryLoggerOnce sync.Once

func configureRecoveryLogger() {
	recoveryLoggerOnce.Do(func() { wklog.Configure(wklog.NewOptions()) })
}

func TestStartupDialRecovery(t *testing.T) {
	// Initialize the process logger before concurrent networking callbacks.
	configureRecoveryLogger()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := "tcp://" + l.Addr().String()
	l.Close()
	s := wkserver.New(addr)
	var authenticated atomic.Int64
	s.Route(s.Options().ConnPath, func(ctx *wkserver.Context) {
		authenticated.Add(1)
		ctx.WriteConnack(&proto.Connack{Id: ctx.ConnReq().Id, Status: proto.StatusOK})
	})
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	c := client.New(addr, client.WithUid("dial-recovery"))
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	wait := func(generation int64) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if authenticated.Load() >= generation && c.IsAuthed() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("authentication did not recover")
	}
	wait(1)
	c.CloseConn()
	// CloseConn is asynchronous: merely observing the old authed flag would
	// let this test pass without any actual reconnect/handshake.
	wait(2)
}
