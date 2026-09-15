package client

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

func TestBatchSendRejectsMissingTransportWithoutPanicking(t *testing.T) {
	c := New("tcp://127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()

	// A disconnect can clear the transport immediately after an authentication
	// check. BatchSend must return the batch to its caller for retry, not panic.
	c.conn().status.Store(authed)
	err := c.BatchSend([]*proto.Message{
		{MsgType: 1, Content: []byte("one")},
		{MsgType: 2, Content: []byte("two")},
	})
	require.EqualError(t, err, "conn is nil")
}

func TestBatchSendRejectsUnauthenticatedConnection(t *testing.T) {
	c := New("tcp://127.0.0.1:1")
	defer c.cancel()
	defer c.pool.Release()

	err := c.BatchSend([]*proto.Message{{MsgType: 1, Content: []byte("one")}})
	require.EqualError(t, err, "connect is not connected")
}
