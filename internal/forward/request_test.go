package forward

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type requestCluster struct {
	icluster.ICluster
	request func(context.Context, uint64, string, []byte) (*proto.Response, error)
}

func (c *requestCluster) RequestWithContext(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	return c.request(ctx, node, path, body)
}

func TestRequestReResolvesAfterAmbiguousSend(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	leader := uint64(2)
	var nodes []uint64
	var bodies [][]byte
	service.Cluster = &requestCluster{request: func(_ context.Context, node uint64, _ string, body []byte) (*proto.Response, error) {
		nodes = append(nodes, node)
		bodies = append(bodies, append([]byte(nil), body...))
		if len(nodes) == 1 {
			leader = 3
			return nil, errors.New("response lost after admission")
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}}
	events := []*eventbus.Event{{Frame: &wkproto.SendPacket{ClientMsgNo: "stable"}}}
	require.NoError(t, Request(UserPath, []byte("immutable encrypted packet"), events, func() uint64 { return leader }, 1, nil))
	require.Equal(t, []uint64{2, 3}, nodes)
	require.Equal(t, bodies[0], bodies[1])
}

func TestRequestDoesNotReplayUnkeyedOrTransientSend(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	for _, packet := range []*wkproto.SendPacket{{}, {Framer: wkproto.Framer{NoPersist: true}, ClientMsgNo: "key"}} {
		calls := 0
		service.Cluster = &requestCluster{request: func(context.Context, uint64, string, []byte) (*proto.Response, error) {
			calls++
			return nil, context.DeadlineExceeded
		}}
		require.Error(t, Request(UserPath, nil, []*eventbus.Event{{Frame: packet}}, func() uint64 { return 2 }, 1, nil))
		require.Equal(t, 1, calls)
	}
}

func TestRequestBudgetAndPermanentRejection(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	calls := 0
	service.Cluster = &requestCluster{request: func(context.Context, uint64, string, []byte) (*proto.Response, error) {
		calls++
		return &proto.Response{Status: proto.StatusNotFound}, nil
	}}
	event := &eventbus.Event{Frame: &wkproto.SendPacket{ClientMsgNo: "key"}}
	require.Error(t, Request(UserPath, nil, []*eventbus.Event{event}, func() uint64 { return 2 }, 1, nil))
	require.Equal(t, 1, calls, "mixed-version endpoint must fail explicitly without legacy fallback")
	event.ForwardDeadline = time.Now().Add(60 * time.Millisecond).UnixMilli()
	start := time.Now()
	require.Error(t, Request(UserPath, nil, []*eventbus.Event{event}, func() uint64 { return 0 }, 1, nil))
	require.Less(t, time.Since(start), time.Second)
	event.ForwardHops = maxHops
	_, _, err := Envelope(nil, []*eventbus.Event{event})
	require.Error(t, err)
}
