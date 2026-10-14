package test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/internal/testfixture"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

// startServer starts s and waits until its listener accepts connections.
func startServer(tb testing.TB, s *wkserver.Server, addr string) {
	tb.Helper()
	require.NoError(tb, s.Start())
	tb.Cleanup(func() {
		s.Stop()
		assert.NoError(tb, testfixture.WaitAddrReleased(addr, testfixture.DefaultReadyTimeout))
	})
	testfixture.RequireListening(tb, addr)
}

// startClient starts a client and waits until it is authenticated. The
// returned stop function is idempotent and also registered for cleanup.
func startClient(tb testing.TB, addr string) (*client.Client, func()) {
	tb.Helper()
	cli := client.New(addr, client.WithUid("uid"))
	require.NoError(tb, cli.Start())
	var once sync.Once
	stop := func() { once.Do(cli.Stop) }
	tb.Cleanup(stop)
	testfixture.RequireAuthed(tb, cli.IsAuthed)
	return cli, stop
}

func TestSendAndRecv(t *testing.T) {
	for _, tc := range []struct {
		name     string
		poolOn   bool
		msgCount int
	}{
		{name: "direct_dispatch", poolOn: false, msgCount: 100000},
		// Kept below the default message pool size: the non-blocking pool
		// drops submissions once saturated, which is not what this test covers.
		{name: "message_pool", poolOn: true, msgCount: 10000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testSendAndRecv(t, tc.poolOn, tc.msgCount)
		})
	}
}

func testSendAndRecv(t *testing.T, poolOn bool, msgCount int) {
	addr := testfixture.FreeAddr(t)

	s := wkserver.New(addr, wkserver.WithMessagePoolOn(poolOn))

	tracker := testfixture.NewTracker()
	s.OnMessage(func(conn gnet.Conn, m *proto.Message) {
		tracker.Received(m.Id)
	})
	startServer(t, s, addr)

	cli, stopClient := startClient(t, addr)

	for i := 0; i < msgCount; i++ {
		id := uint64(i)
		tracker.Expect(id)
		err := cli.Send(&proto.Message{
			Id:      id,
			MsgType: 112349,
			Content: []byte("hellohellohellohellohellohellohellohellohellohellohellohellohellohellohello"),
		})
		if err != nil {
			// A rejected Send never reaches the server; release its obligation
			// and stop, rather than waiting for a delivery that cannot happen.
			tracker.Cancel(id)
			t.Fatalf("send %d rejected: %v (pending=%d)", id, err, tracker.Pending())
		}
	}

	require.NoError(t, tracker.Wait(testfixture.DefaultDeliveryTimeout))
	stopClient()
	require.NoError(t, tracker.Verify())
}

func TestRequestResp(t *testing.T) {
	addr := testfixture.FreeAddr(t)
	s := wkserver.New(addr)
	s.Route("/test", func(c *wkserver.Context) {
		c.Write([]byte("hello"))
	})
	startServer(t, s, addr)

	cli, _ := startClient(t, addr)

	var count = 10000

	var wg sync.WaitGroup
	wg.Add(count)

	var tcount atomic.Int64
	for i := 0; i < count; i++ {
		go func(ii int) {
			defer wg.Done()
			resp, err := cli.Request("/test", []byte(fmt.Sprintf("hi--%d", ii)))
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, []byte("hello"), resp.Body)
			tcount.Add(1)
		}(i)
	}

	// Every goroutine finishes within the client's own request timeout, so
	// this wait is bounded without an extra deadline.
	wg.Wait()
	assert.Equal(t, int64(count), tcount.Load())
}

func BenchmarkRequestResp(b *testing.B) {
	addr := testfixture.FreeAddr(b)
	s := wkserver.New(addr)
	s.Route("/test", func(c *wkserver.Context) {
		c.Write([]byte("hello"))
	})
	startServer(b, s, addr)

	cli, _ := startClient(b, addr)

	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		// 每个并发的测试 goroutine 会运行此代码
		for pb.Next() {
			resp, err := cli.Request("/test", []byte("hi"))
			if !assert.NoError(b, err) {
				continue
			}
			assert.Equal(b, []byte("hello"), resp.Body)
		}
	})
}
