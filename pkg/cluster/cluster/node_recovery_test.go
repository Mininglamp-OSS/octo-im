package cluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
	"github.com/stretchr/testify/require"
)

// This diagnostic uses the normal production mode and an actual loopback peer.
// Only batch scheduling is manual, to hold the unauthenticated interval fixed.
// The peer authenticates only after the queued message has been examined.
func TestAcceptedMessageSurvivesPeerRecovery(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := "tcp://" + listener.Addr().String()
	require.NoError(t, listener.Close())

	received := make(chan string, 8)
	server := wkserver.New(addr)
	server.OnMessage(func(_ gnet.Conn, msg *proto.Message) {
		received <- string(msg.Content)
	})
	require.NoError(t, server.Start())
	defer server.Stop()

	opts := NewOptions()
	opts.SendQueueLength = 10
	opts.MaxSendQueueSize = 1024 * 1024
	node := NewImprovedNode(1004, "round3-diagnostic", addr, opts)
	defer node.Stop()
	require.False(t, node.isTestMode(), "must exercise production behavior")
	require.False(t, node.client.IsAuthed())

	const pending = "round3-accepted-before-peer-authentication"
	const control = "round3-control-after-peer-authentication"
	require.NoError(t, node.Send(&proto.Message{MsgType: 123, Content: []byte(pending)}))
	require.True(t, node.hasMessages())
	t.Logf("accepted: authed=%v queue=%v pending=%v sent=%v dropped=%v retried=%v",
		node.client.IsAuthed(), node.GetStats()["queue_queue_length"], node.GetStats()["backpressure_pending_count"],
		node.GetStats()["perf_total_sent"], node.GetStats()["perf_total_dropped"], node.GetStats()["perf_total_retried"])
	node.processBatch(context.Background())
	t.Logf("after unauthenticated batch: authed=%v queue=%v pending=%v sent=%v dropped=%v retried=%v",
		node.client.IsAuthed(), node.GetStats()["queue_queue_length"], node.GetStats()["backpressure_pending_count"],
		node.GetStats()["perf_total_sent"], node.GetStats()["perf_total_dropped"], node.GetStats()["perf_total_retried"])

	require.NoError(t, node.client.Start())
	require.Eventually(t, node.client.IsAuthed, 5*time.Second, 10*time.Millisecond,
		"the real peer must authenticate before testing recovery")
	require.NoError(t, node.Send(&proto.Message{MsgType: 123, Content: []byte(control)}))
	node.processBatch(context.Background())

	gotPending, gotControl := false, false
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
collect:
	for {
		select {
		case msg := <-received:
			t.Logf("real peer received: %s", msg)
			gotPending = gotPending || msg == pending
			gotControl = gotControl || msg == control
			if gotPending && gotControl {
				break collect
			}
		case <-timer.C:
			break collect
		}
	}
	require.True(t, gotControl, "healthy-connection control must arrive")
	require.True(t, gotPending,
		"accepted message vanished during the unauthenticated batch; real peer recovered and received the control, but the accepted message was neither retained nor retried")
}
