package wkserver_test

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/require"
)

func TestReconnectPreservesRequestsAndBatchDelivery(t *testing.T) {
	configureRecoveryLogger()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := "tcp://" + l.Addr().String()
	require.NoError(t, l.Close())
	s := wkserver.New(addr)
	var authenticated atomic.Int64
	s.Route(s.Options().ConnPath, func(ctx *wkserver.Context) {
		authenticated.Add(1)
		ctx.WriteConnack(&proto.Connack{Id: ctx.ConnReq().Id, Status: proto.StatusOK})
	})
	s.Route("/echo", func(ctx *wkserver.Context) { ctx.Write(ctx.Body()) })
	received := make(chan *proto.Message, 1000)
	s.OnMessage(func(_ gnet.Conn, m *proto.Message) { received <- m })
	require.NoError(t, s.Start())
	defer s.Stop()
	c := client.New(addr, client.WithUid("generation-delivery"))
	c.Route("/echo", func(ctx *client.Context) { ctx.Write(ctx.Body()) })
	require.NoError(t, c.Start())
	defer c.Stop()
	for generation := 1; generation <= 5; generation++ {
		require.Eventually(t, func() bool {
			return authenticated.Load() >= int64(generation) && c.IsAuthed()
		}, 5*time.Second, 5*time.Millisecond)
		for i := 0; i < 20; i++ {
			body := []byte(fmt.Sprintf("generation-%d-request-%d", generation, i))
			resp, err := c.Request("/echo", body)
			require.NoError(t, err)
			require.Equal(t, body, resp.Body)
			resp, err = s.Request("generation-delivery", "/echo", body)
			require.NoError(t, err)
			require.Equal(t, body, resp.Body)
		}
		want := make(map[uint64]string)
		batch := make([]*proto.Message, 0, 50)
		for i := 0; i < 100; i++ {
			id := uint64(generation*1000 + i)
			body := fmt.Sprintf("generation-%d-message-%d", generation, i)
			want[id] = body
			m := &proto.Message{Id: id, MsgType: 1, Content: []byte(body)}
			if i < 50 {
				require.NoError(t, c.Send(m))
			} else {
				batch = append(batch, m)
			}
		}
		require.NoError(t, c.BatchSend(batch))
		for len(want) > 0 {
			select {
			case m := <-received:
				body, ok := want[m.Id]
				require.True(t, ok, "unexpected or duplicate message %d", m.Id)
				require.Equal(t, body, string(m.Content))
				delete(want, m.Id)
			case <-time.After(3 * time.Second):
				t.Fatalf("missing %d messages in generation %d", len(want), generation)
			}
		}
		if generation < 5 {
			c.CloseConn()
		}
	}
	require.Empty(t, received)
}

func TestDelayedPeerClosePreservesReplacement(t *testing.T) {
	configureRecoveryLogger()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := "tcp://" + l.Addr().String()
	require.NoError(t, l.Close())

	const uid = "overlapping-peer"
	s := wkserver.New(addr, wkserver.WithRequestTimeout(time.Second))
	closed := make(chan string, 2)
	s.Route(s.Options().ClosePath, func(ctx *wkserver.Context) { closed <- ctx.PeerUID() })
	require.NoError(t, s.Start())
	t.Cleanup(s.Stop)

	openPeer := func(reply string) func() {
		c := client.New(addr, client.WithUid(uid))
		c.Route("/which-peer", func(ctx *client.Context) { ctx.Write([]byte(reply)) })
		require.NoError(t, c.Start())
		stop := sync.OnceFunc(c.Stop)
		t.Cleanup(stop)
		require.Eventually(t, c.IsAuthed, 3*time.Second, 5*time.Millisecond)
		return stop
	}
	requirePeer := func(reply string) {
		resp, err := s.Request(uid, "/which-peer", nil)
		require.NoError(t, err)
		require.Equal(t, reply, string(resp.Body))
	}
	waitForClose := func() {
		select {
		case peer := <-closed:
			require.Equal(t, uid, peer)
		case <-time.After(3 * time.Second):
			t.Fatal("server did not handle the closed connection")
		}
	}

	stopOld := openPeer("old")
	requirePeer("old")

	// Keep the old socket open until the replacement has authenticated.
	stopReplacement := openPeer("replacement")
	requirePeer("replacement")
	stopOld()
	waitForClose()
	requirePeer("replacement")

	stopReplacement()
	waitForClose()
	_, err = s.Request(uid, "/which-peer", nil)
	require.EqualError(t, err, "conn is nil")
}
