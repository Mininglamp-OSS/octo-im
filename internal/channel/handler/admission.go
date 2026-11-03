package handler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// An internal DTO avoids coupling HTTP-only fields (including stream settings)
// to the client SEND wire codec. The original packet is preserved verbatim.
type httpAdmissionRequest struct {
	ChannelID   string             `json:"channel_id"`
	ChannelType uint8              `json:"channel_type"`
	MessageID   int64              `json:"message_id"`
	FromUID     string             `json:"from_uid"`
	TagKey      string             `json:"tag_key"`
	Approved    bool               `json:"approved"`
	Packet      wkproto.SendPacket `json:"packet"`
	Track       track.Message      `json:"track"`
}

func encodeAdmission(id string, typ uint8, e *eventbus.Event) ([]byte, error) {
	p, ok := e.Frame.(*wkproto.SendPacket)
	if !ok || p == nil || e.Conn == nil || !options.G.IsSystemDevice(e.Conn.DeviceId) {
		return nil, service.ErrMessageAdmissionDenied
	}
	return json.Marshal(httpAdmissionRequest{ChannelID: id, ChannelType: typ, MessageID: e.MessageId,
		FromUID: e.Conn.Uid, TagKey: e.TagKey, Approved: e.ReasonCode == wkproto.ReasonSuccess, Packet: *p, Track: e.Track})
}

const (
	httpAdmissionPath                 = "/wk/channel/httpAdmission/v1"
	persistAdmissionPath              = "/wk/channel/persistAdmission/v1"
	admissionRetry       proto.Status = 3 // definitely not enqueued: route changed
	admissionUnavailable proto.Status = 5 // definitely not enqueued: closed/full
	admissionUnknown     proto.Status = 6 // request/response transport failure
	admissionDenied      proto.Status = 7
)

func (h *Handler) setAdmissionRoutes() {
	service.Cluster.Route(httpAdmissionPath, func(c *wkserver.Context) {
		ctx, cancel := h.WithTimeout()
		defer cancel()
		c.WriteStatus(h.acceptHTTPAdmission(ctx, c.Body()))
	})
	service.Cluster.Route(persistAdmissionPath, func(c *wkserver.Context) {
		ctx, cancel := h.WithTimeout()
		defer cancel()
		c.WriteStatus(h.acceptPersistAdmission(ctx, c.Body()))
	})
}

func (h *Handler) admitHTTPMessage(ctx context.Context, id string, typ uint8, e *eventbus.Event) error {
	data, err := encodeAdmission(id, typ, e)
	if err != nil {
		return err
	}
	// Retry only an explicit rejection before enqueue, never a lost response.
	for attempt := 0; attempt < 3; attempt++ {
		node, err := service.Cluster.SlotLeaderIdOfChannel(id, typ)
		if err != nil || node == 0 {
			return service.ErrMessageAdmissionUnavailable
		}
		status := h.requestAdmission(ctx, node, httpAdmissionPath, data)
		switch status {
		case proto.StatusOK:
			return nil
		case admissionUnknown:
			return service.ErrMessageAdmissionUnknown
		case admissionDenied:
			return service.ErrMessageAdmissionDenied
		case admissionRetry:
			select {
			case <-ctx.Done():
				return service.ErrMessageAdmissionUnavailable
			case <-time.After(10 * time.Millisecond):
			}
		default:
			return service.ErrMessageAdmissionUnavailable
		}
	}
	return service.ErrMessageAdmissionUnavailable
}

func (h *Handler) requestAdmission(ctx context.Context, node uint64, path string, body []byte) proto.Status {
	if options.G.IsLocalNode(node) {
		if path == httpAdmissionPath {
			return h.acceptHTTPAdmission(ctx, body)
		}
		return h.acceptPersistAdmission(ctx, body)
	}
	resp, err := service.Cluster.RequestWithContext(ctx, node, path, body)
	if err != nil || resp == nil {
		return admissionUnknown
	}
	return resp.Status
}

func decodeAdmission(body []byte) (*forwardChannelEventReq, bool) {
	var req httpAdmissionRequest
	if json.Unmarshal(body, &req) != nil || req.ChannelID == "" || req.ChannelType == 0 || req.MessageID <= 0 || req.FromUID == "" || req.Packet.NoPersist || req.Packet.ClientMsgNo == "" {
		return nil, false
	}
	e := &eventbus.Event{Type: eventbus.EventChannelOnSend, MessageId: req.MessageID,
		Conn: &eventbus.Conn{Uid: req.FromUID, DeviceId: options.G.SystemDeviceId}, Frame: &req.Packet, TagKey: req.TagKey, Track: req.Track}
	if req.Approved {
		e.ReasonCode = wkproto.ReasonSuccess
	}
	return &forwardChannelEventReq{channelId: req.ChannelID, channelType: req.ChannelType, events: eventbus.EventBatch{e}}, true
}

func (h *Handler) acceptHTTPAdmission(ctx context.Context, body []byte) proto.Status {
	req, ok := decodeAdmission(body)
	if !ok {
		return admissionDenied
	}
	node, err := service.Cluster.SlotLeaderIdOfChannel(req.channelId, req.channelType)
	if err != nil || !options.G.IsLocalNode(node) {
		return admissionRetry
	}
	// Metadata and permissions still belong to the slot owner. Only the
	// asynchronous persistence job moves to the current channel leader.
	ec := &eventbus.ChannelContext{ChannelId: req.channelId, ChannelType: req.channelType, Events: req.events, SlotLeaderId: node}
	h.permission(ec)
	if req.events[0].ReasonCode != wkproto.ReasonSuccess {
		return admissionDenied
	}
	body, err = encodeAdmission(req.channelId, req.channelType, req.events[0])
	if err != nil {
		return admissionDenied
	}
	leader, err := service.Cluster.LeaderIdOfChannel(req.channelId, req.channelType)
	if err != nil || leader == 0 {
		return admissionUnavailable
	}
	return h.requestAdmission(ctx, leader, persistAdmissionPath, body)
}

func (h *Handler) acceptPersistAdmission(ctx context.Context, body []byte) proto.Status {
	req, ok := decodeAdmission(body)
	if !ok {
		return admissionDenied
	}
	e := req.events[0]
	if e.ReasonCode != wkproto.ReasonSuccess {
		return admissionDenied
	}
	err := service.Cluster.AdmitLocalChannelMessage(ctx, req.channelId, req.channelType, uint64(e.MessageId), int64(len(body)), func(done func()) {
		e.Type = eventbus.EventChannelPersistAdmitted
		e.CompleteAdmission = done
		eventbus.Channel.AddEvent(req.channelId, req.channelType, e)
		eventbus.Channel.Advance(req.channelId, req.channelType)
	})
	if err != nil {
		return admissionUnavailable
	}
	return proto.StatusOK
}

func (h *Handler) onPersistAdmitted(ctx *eventbus.ChannelContext) {
	defer func() {
		for _, e := range ctx.Events {
			if e.CompleteAdmission != nil {
				e.CompleteAdmission()
				e.CompleteAdmission = nil
			}
		}
	}()
	h.persist(ctx)
	h.sendack(ctx)
}
