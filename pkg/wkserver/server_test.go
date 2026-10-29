package wkserver_test

import (
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/internal/testfixture"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

// startServer starts s and waits until its listener accepts connections.
func startServer(t *testing.T, s *wkserver.Server, addr string) {
	t.Helper()
	require.NoError(t, s.Start())
	t.Cleanup(func() {
		s.Stop()
		assert.NoError(t, testfixture.WaitAddrReleased(addr, testfixture.DefaultReadyTimeout))
	})
	testfixture.RequireListening(t, addr)
}

// startClient starts a client and waits until it is authenticated. The
// returned stop function is idempotent and also registered for cleanup.
func startClient(t *testing.T, addr string) (*client.Client, func()) {
	t.Helper()
	cli := client.New(addr, client.WithUid("uid"))
	require.NoError(t, cli.Start())
	var once sync.Once
	stop := func() { once.Do(cli.Stop) }
	t.Cleanup(stop)
	testfixture.RequireAuthed(t, cli.IsAuthed)
	return cli, stop
}

func TestServerRoute(t *testing.T) {
	addr := testfixture.FreeAddr(t)
	s := wkserver.New(addr)
	s.Route("/test", func(c *wkserver.Context) {
		c.Write([]byte("test2"))
	})
	startServer(t, s, addr)

	cli, _ := startClient(t, addr)

	resp, err := cli.Request("/test", []byte("test"))
	require.NoError(t, err)
	assert.Equal(t, []byte("test2"), resp.Body)

	cli.Route("/hi", func(c *client.Context) {
		c.Write([]byte("world"))
	})

	resp, err = s.Request("uid", "/hi", []byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, []byte("world"), resp.Body)
}

func TestServerOnMessage(t *testing.T) {
	for _, tc := range []struct {
		name   string
		poolOn bool
	}{
		{name: "message_pool", poolOn: true},
		{name: "direct_dispatch", poolOn: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testServerOnMessage(t, tc.poolOn)
		})
	}
}

func testServerOnMessage(t *testing.T, poolOn bool) {
	addr := testfixture.FreeAddr(t)
	s := wkserver.New(addr, wkserver.WithMessagePoolOn(poolOn))

	tracker := testfixture.NewTracker()
	recvMsg := make(chan *proto.Message, 1)
	s.OnMessage(func(conn gnet.Conn, m *proto.Message) {
		tracker.Received(m.Id)
		select {
		case recvMsg <- m:
		default: // a duplicate is reported by the tracker; never block the server
		}
	})
	startServer(t, s, addr)

	cli, stopClient := startClient(t, addr)

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
	rmData, err := rm.Marshal()
	require.NoError(t, err)

	const id = 1
	tracker.Expect(id)
	err = cli.Send(&proto.Message{
		Id:      id,
		MsgType: 1,
		Content: rmData,
	})
	if err != nil {
		tracker.Cancel(id)
		t.Fatalf("send rejected after authentication: %v", err)
	}

	m, err := testfixture.Receive(recvMsg, testfixture.DefaultDeliveryTimeout)
	require.NoError(t, err, "pending=%d", tracker.Pending())
	require.NoError(t, tracker.Wait(testfixture.DefaultDeliveryTimeout))

	resultRM := raftpb.Message{}
	require.NoError(t, resultRM.Unmarshal(m.Content))

	assert.Equal(t, rm.To, resultRM.To)
	assert.Equal(t, rm.From, resultRM.From)
	assert.Equal(t, rm.Type, resultRM.Type)
	require.Len(t, resultRM.Entries, 1)
	assert.Equal(t, rm.Entries[0].Type, resultRM.Entries[0].Type)
	assert.Equal(t, rm.Entries[0].Data, resultRM.Entries[0].Data)

	// Stop the transport before the exactly-once check so a late duplicate
	// cannot slip in after verification.
	stopClient()
	require.NoError(t, tracker.Verify())
}

func TestReconnect(t *testing.T) {
	addr := testfixture.FreeAddr(t)
	s := wkserver.New(addr)
	startServer(t, s, addr)

	cli, _ := startClient(t, addr)

	cli.CloseConn()
	assert.Equal(t, false, cli.IsAuthed())

	testfixture.RequireAuthed(t, cli.IsAuthed)
}
