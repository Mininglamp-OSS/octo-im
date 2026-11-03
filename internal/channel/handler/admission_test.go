package handler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type admissionCluster struct {
	icluster.ICluster
	admit     func(func(func())) error
	request   func() (*proto.Response, error)
	slot      uint64
	inspect   func([]byte)
	resolve   func() (uint64, error)
	requestTo func(context.Context, uint64, []byte) (*proto.Response, error)
}

func (c *admissionCluster) AdmitLocalChannelMessage(_ context.Context, _ string, _ uint8, _ uint64, _ int64, enqueue func(func())) error {
	return c.admit(enqueue)
}
func (c *admissionCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) {
	if c.resolve != nil {
		return c.resolve()
	}
	if c.slot != 0 {
		return c.slot, nil
	}
	return 2, nil
}
func (c *admissionCluster) LeaderIdOfChannel(string, uint8) (uint64, error) { return 2, nil }
func (c *admissionCluster) RequestWithContext(ctx context.Context, node uint64, _ string, body []byte) (*proto.Response, error) {
	if c.inspect != nil {
		c.inspect(body)
	}
	if c.requestTo != nil {
		return c.requestTo(ctx, node, body)
	}
	return c.request()
}

func TestHTTPAdmissionWaitsForHandoffAndRefreshesRoute(t *testing.T) {
	h, _, e := setupAdmission(t)
	calls := 0
	service.Cluster = &admissionCluster{
		resolve: func() (uint64, error) {
			if calls < 4 {
				return 2, nil
			}
			return 3, nil
		},
		requestTo: func(ctx context.Context, node uint64, body []byte) (*proto.Response, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.LessOrEqual(t, time.Until(deadline), maxHTTPAdmissionWait)
			req, ok := decodeAdmission(body)
			require.True(t, ok)
			require.Equal(t, e.MessageId, req.events[0].MessageId)
			require.Equal(t, "key", req.events[0].Frame.(*wkproto.SendPacket).ClientMsgNo)
			calls++
			if calls <= 4 {
				require.Equal(t, uint64(2), node)
				return &proto.Response{Status: admissionUnavailable}, nil
			}
			require.Equal(t, uint64(3), node)
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}
	require.NoError(t, h.admitHTTPMessage(context.Background(), "room", 2, e))
	require.Equal(t, 5, calls, "handoff may need more than the former three attempts")
	require.Zero(t, h.admissionWaiters.count.Load())
	require.Zero(t, h.admissionWaiters.bytes.Load())
}

func TestHTTPAdmissionWaitHasThreeSecondDeadline(t *testing.T) {
	h, _, e := setupAdmission(t)
	calls := 0
	var attemptCtx context.Context
	service.Cluster = &admissionCluster{requestTo: func(ctx context.Context, _ uint64, _ []byte) (*proto.Response, error) {
		calls++
		attemptCtx = ctx
		return &proto.Response{Status: admissionUnavailable}, nil
	}}
	start := time.Now()
	require.ErrorIs(t, h.admitHTTPMessage(context.Background(), "room", 2, e), service.ErrMessageAdmissionUnavailable)
	deadline, _ := attemptCtx.Deadline()
	require.WithinDuration(t, start.Add(3*time.Second), deadline, 100*time.Millisecond)
	require.ErrorIs(t, attemptCtx.Err(), context.DeadlineExceeded)
	require.Greater(t, calls, 1)
	require.Zero(t, h.admissionWaiters.count.Load())
	require.Zero(t, h.admissionWaiters.bytes.Load())
}

func TestHTTPAdmissionWaitHonorsCallerCancellation(t *testing.T) {
	h, _, e := setupAdmission(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	service.Cluster = &admissionCluster{request: func() (*proto.Response, error) {
		calls++
		cancel()
		return &proto.Response{Status: admissionUnavailable}, nil
	}}
	require.ErrorIs(t, h.admitHTTPMessage(ctx, "room", 2, e), service.ErrMessageAdmissionUnavailable)
	require.Equal(t, 1, calls)
	require.Zero(t, h.admissionWaiters.count.Load())
	require.Zero(t, h.admissionWaiters.bytes.Load())
	require.ErrorIs(t, h.admitHTTPMessage(ctx, "room", 2, e), service.ErrMessageAdmissionUnavailable)
	require.Equal(t, 1, calls, "cancelled requests must not send another admission RPC")
}

func TestHTTPAdmissionStopsWaitingOnUnknownOrDenied(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *proto.Response
		err      error
		want     error
	}{
		{"lost response", nil, context.DeadlineExceeded, service.ErrMessageAdmissionUnknown},
		{"unknown response", &proto.Response{Status: admissionUnknown}, nil, service.ErrMessageAdmissionUnknown},
		{"permission denied", &proto.Response{Status: admissionDenied}, nil, service.ErrMessageAdmissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _, e := setupAdmission(t)
			calls := 0
			service.Cluster = &admissionCluster{request: func() (*proto.Response, error) {
				calls++
				if calls == 1 {
					return &proto.Response{Status: admissionUnavailable}, nil
				}
				return tc.response, tc.err
			}}
			require.ErrorIs(t, h.admitHTTPMessage(context.Background(), "room", 2, e), tc.want)
			require.Equal(t, 2, calls)
			require.Zero(t, h.admissionWaiters.count.Load())
			require.Zero(t, h.admissionWaiters.bytes.Load())
		})
	}
}

func TestHTTPAdmissionWaitRecoversMissingRoute(t *testing.T) {
	h, _, e := setupAdmission(t)
	lookups, sends := 0, 0
	service.Cluster = &admissionCluster{
		resolve: func() (uint64, error) {
			lookups++
			if lookups == 1 {
				return 0, errors.New("slot election")
			}
			return 2, nil
		},
		request: func() (*proto.Response, error) {
			sends++
			return &proto.Response{Status: proto.StatusOK}, nil
		},
	}
	require.NoError(t, h.admitHTTPMessage(context.Background(), "room", 2, e))
	require.Equal(t, 2, lookups)
	require.Equal(t, 1, sends)
}

func TestHTTPAdmissionWaitRejectsWhenBudgetExhausted(t *testing.T) {
	h, _, e := setupAdmission(t)
	require.True(t, h.admissionWaiters.acquire(maxHTTPAdmissionWaitBytes))
	defer h.admissionWaiters.release(maxHTTPAdmissionWaitBytes)
	calls := 0
	service.Cluster = &admissionCluster{request: func() (*proto.Response, error) {
		calls++
		return &proto.Response{Status: admissionUnavailable}, nil
	}}
	require.ErrorIs(t, h.admitHTTPMessage(context.Background(), "room", 2, e), service.ErrMessageAdmissionUnavailable)
	require.Equal(t, 1, calls)
	require.Equal(t, int64(1), h.admissionWaiters.count.Load())
	require.Equal(t, int64(maxHTTPAdmissionWaitBytes), h.admissionWaiters.bytes.Load())
}

func TestHTTPAdmissionWaitBudgetBoundsCountAndBytes(t *testing.T) {
	var w httpAdmissionWaiters
	require.False(t, w.acquire(0))
	require.False(t, w.acquire(maxHTTPAdmissionWaitBytes+1))
	require.True(t, w.acquire(maxHTTPAdmissionWaitBytes))
	require.False(t, w.acquire(1))
	w.release(maxHTTPAdmissionWaitBytes)
	for i := 0; i < maxHTTPAdmissionWaiters; i++ {
		require.True(t, w.acquire(1))
	}
	require.False(t, w.acquire(1))
	for i := 0; i < maxHTTPAdmissionWaiters; i++ {
		w.release(1)
	}
	require.Zero(t, w.count.Load())
	require.Zero(t, w.bytes.Load())
	require.True(t, w.acquire(maxHTTPAdmissionWaitBytes), "completed requests release capacity")
	w.release(maxHTTPAdmissionWaitBytes)
}

func setupAdmission(t *testing.T) (*Handler, *forwardChannel, *eventbus.Event) {
	oldOpts, oldCluster, oldChannel, oldAdmit := options.G, service.Cluster, eventbus.Channel, service.AdmitHTTPMessage
	t.Cleanup(func() {
		options.G, service.Cluster, eventbus.Channel, service.AdmitHTTPMessage = oldOpts, oldCluster, oldChannel, oldAdmit
	})
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	sink := &forwardChannel{}
	eventbus.RegisterChannel(sink)
	e := &eventbus.Event{Type: eventbus.EventChannelOnSend, MessageId: 42, ReasonCode: wkproto.ReasonSuccess, Conn: &eventbus.Conn{Uid: "u", DeviceId: options.G.SystemDeviceId}, Frame: &wkproto.SendPacket{ChannelID: "room", ChannelType: 2, ClientMsgNo: "key", Payload: []byte("body")}}
	return NewHandler(), sink, e
}

func TestHTTPAdmissionRejectsBeforeEnqueueAndDoesNotWaitForProcessing(t *testing.T) {
	h, sink, e := setupAdmission(t)
	req := forwardChannelEventReq{channelId: "room", channelType: 2, events: eventbus.EventBatch{e}}
	body, err := encodeAdmission(req.channelId, req.channelType, req.events[0])
	require.NoError(t, err)
	c := &admissionCluster{admit: func(func(func())) error { return errors.New("draining") }}
	service.Cluster = c
	require.Equal(t, admissionUnavailable, h.acceptPersistAdmission(context.Background(), body))
	require.Empty(t, sink.events)
	completed := false
	c.admit = func(enqueue func(func())) error { enqueue(func() { completed = true }); return nil }
	require.Equal(t, proto.StatusOK, h.acceptPersistAdmission(context.Background(), body))
	require.Len(t, sink.events, 1)
	require.Equal(t, eventbus.EventChannelPersistAdmitted, sink.events[0].Type)
	require.False(t, completed, "HTTP admission must remain asynchronous")
	require.Nil(t, sink.events[0].Clone().CompleteAdmission, "distribution must not release an admission twice")
	sink.events[0].CompleteAdmission()
	require.True(t, completed)
}

func TestHTTPAdmissionNeverRetriesUnknownOutcome(t *testing.T) {
	h, _, e := setupAdmission(t)
	calls := 0
	service.Cluster = &admissionCluster{request: func() (*proto.Response, error) { calls++; return nil, context.DeadlineExceeded }}
	require.ErrorIs(t, h.admitHTTPMessage(context.Background(), "room", 2, e), service.ErrMessageAdmissionUnknown)
	require.Equal(t, 1, calls)
	calls = 0
	service.Cluster = &admissionCluster{request: func() (*proto.Response, error) {
		calls++
		if calls == 1 {
			return &proto.Response{Status: admissionRetry}, nil
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}}
	require.NoError(t, h.admitHTTPMessage(context.Background(), "room", 2, e))
	require.Equal(t, 2, calls)
}

func TestHTTPAdmissionForwardsPermissionResultToPersistence(t *testing.T) {
	h, _, e := setupAdmission(t)
	oldPermission := service.Permission
	t.Cleanup(func() { service.Permission = oldPermission })
	service.Permission = service.NewPermissionService(nil)
	e.ReasonCode = 0
	e.Frame.(*wkproto.SendPacket).ChannelType = wkproto.ChannelTypeInfo
	req := forwardChannelEventReq{channelId: "room", channelType: wkproto.ChannelTypeInfo, events: eventbus.EventBatch{e}}
	body, err := encodeAdmission(req.channelId, req.channelType, req.events[0])
	require.NoError(t, err)
	inspected := false
	service.Cluster = &admissionCluster{slot: 1, inspect: func(data []byte) {
		forwarded, ok := decodeAdmission(data)
		require.True(t, ok)
		require.Equal(t, wkproto.ReasonSuccess, forwarded.events[0].ReasonCode)
		require.Len(t, h.toPersistMessages("room", wkproto.ChannelTypeInfo, forwarded.events), 1)
		inspected = true
	}, request: func() (*proto.Response, error) { return &proto.Response{Status: proto.StatusOK}, nil }}
	require.Equal(t, proto.StatusOK, h.acceptHTTPAdmission(context.Background(), body))
	require.True(t, inspected)
	require.Equal(t, admissionDenied, h.acceptPersistAdmission(context.Background(), body), "unapproved input must never receive an admission acknowledgement")
}

func TestHTTPAdmissionStreamFrameRoundTrip(t *testing.T) {
	_, _, e := setupAdmission(t)
	e.Frame.(*wkproto.SendPacket).Setting = wkproto.SettingStream
	req := forwardChannelEventReq{channelId: "u1@u2", channelType: wkproto.ChannelTypePerson, events: eventbus.EventBatch{e}}
	body, err := encodeAdmission(req.channelId, req.channelType, req.events[0])
	require.NoError(t, err)
	decoded, ok := decodeAdmission(body)
	require.True(t, ok)
	require.Equal(t, *e.Frame.(*wkproto.SendPacket), *decoded.events[0].Frame.(*wkproto.SendPacket))
}
