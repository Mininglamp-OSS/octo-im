package wkserver_test

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestServerRoute(t *testing.T) {

	addr := "tcp://127.0.0.1:10000"
	s := wkserver.New(addr)
	s.Route("/test", func(c *wkserver.Context) {
		c.Write([]byte("test2"))
	})
	err := s.Start()
	assert.NoError(t, err)
	defer s.Stop()

	cli := client.New(addr, client.WithUid("uid"))
	err = cli.Start()
	assert.NoError(t, err)
	defer cli.Stop()

	time.Sleep(time.Millisecond * 200)

	resp, err := cli.Request("/test", []byte("test"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("test2"), resp.Body)

	cli.Route("/hi", func(c *client.Context) {
		c.Write([]byte("world"))
	})

	resp, err = s.Request("uid", "/hi", []byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("world"), resp.Body)
}

func TestServerOnMessage(t *testing.T) {
	addr := "tcp://:10001"
	s := wkserver.New(addr)

	recvMsg := make(chan *proto.Message, 1)
	s.OnMessage(func(conn gnet.Conn, m *proto.Message) {
		recvMsg <- m
	})
	err := s.Start()
	assert.NoError(t, err)
	defer s.Stop()

	cli := client.New(addr, client.WithUid("uid"))
	err = cli.Start()
	assert.NoError(t, err)
	defer cli.Stop()

	time.Sleep(time.Millisecond * 200)

	rm := raftpb.Message{
		To:   1,
		From: 2,
		Type: raftpb.MsgApp,
		Entries: []raftpb.Entry{
			{
				Type: raftpb.EntryNormal,
				Data: []byte("hello"),
			},
		},
	}
	rmData, _ := rm.Marshal()
	err = cli.Send(&proto.Message{
		MsgType: 1,
		Content: rmData,
	})
	assert.NoError(t, err)

	m := <-recvMsg

	resultRM := raftpb.Message{}
	err = resultRM.Unmarshal(m.Content)
	assert.NoError(t, err)

	assert.Equal(t, rm.To, resultRM.To)
	assert.Equal(t, rm.From, resultRM.From)
	assert.Equal(t, rm.Type, resultRM.Type)
	assert.Equal(t, rm.Entries[0].Type, resultRM.Entries[0].Type)
	assert.Equal(t, rm.Entries[0].Data, resultRM.Entries[0].Data)
}

func TestReconnect(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := "tcp://" + l.Addr().String()
	require.NoError(t, l.Close())

	s := wkserver.New(addr)
	var handshakes atomic.Int64
	s.Route(s.Options().ConnPath, func(ctx *wkserver.Context) {
		handshakes.Add(1)
		ctx.WriteConnack(&proto.Connack{Id: ctx.ConnReq().Id, Status: proto.StatusOK})
	})
	require.NoError(t, s.Start())
	defer s.Stop()

	cli := client.New(addr, client.WithUid("uid"))
	require.NoError(t, cli.Start())
	defer cli.Stop()

	require.Eventually(t, func() bool {
		return handshakes.Load() == 1 && cli.IsAuthed()
	}, 200*time.Millisecond, time.Millisecond)

	cli.CloseConn()

	// The replacement may authenticate before the caller can observe
	// the disconnected state. Require a second server handshake instead.
	require.Eventually(t, func() bool {
		return handshakes.Load() == 2 && cli.IsAuthed()
	}, 200*time.Millisecond, time.Millisecond)
}
