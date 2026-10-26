package handler

import (
	"container/list"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

const (
	distributionPath           = "/wk/forward/distribution/v1"
	distributionRetryTimeout   = 30 * time.Second
	distributionMaxAttempts    = 12
	distributionAdmissionLimit = 65536
	distributionAdmissionTTL   = 2 * distributionRetryTimeout
	distributionMaxBody        = 32 << 20
	distributionSnapshotBudget = 64 << 20
)

var errInvalidDistribution = errors.New("invalid distribution admission")

// distributionRequest carries one immutable recipient version for one node.
// It has its own RPC route, leaving the legacy event encoding unchanged.
type distributionRequest struct {
	source, target uint64
	channel        string
	channelType    uint8
	key            string
	uids           []string
	events         eventbus.EventBatch
}

func (r *distributionRequest) encode() ([]byte, error) {
	data, err := r.events.Encode()
	if err != nil {
		return nil, err
	}
	e := wkproto.NewEncoder()
	e.WriteUint8(1)
	e.WriteUint64(r.source)
	e.WriteUint64(r.target)
	e.WriteString(r.channel)
	e.WriteUint8(r.channelType)
	e.WriteString(r.key)
	e.WriteUint32(uint32(len(r.uids)))
	for _, uid := range r.uids {
		e.WriteString(uid)
	}
	e.WriteBinary(data)
	if len(e.Bytes()) > distributionMaxBody {
		return nil, errInvalidDistribution
	}
	return e.Bytes(), nil
}

func decodeDistribution(data []byte) (*distributionRequest, error) {
	if len(data) > distributionMaxBody {
		return nil, errInvalidDistribution
	}
	d := wkproto.NewDecoder(data)
	version, err := d.Uint8()
	if err != nil || version != 1 {
		return nil, errInvalidDistribution
	}
	r := &distributionRequest{}
	if r.source, err = d.Uint64(); err != nil {
		return nil, err
	}
	if r.target, err = d.Uint64(); err != nil {
		return nil, err
	}
	if r.channel, err = d.String(); err != nil {
		return nil, err
	}
	if r.channelType, err = d.Uint8(); err != nil {
		return nil, err
	}
	if r.key, err = d.String(); err != nil {
		return nil, err
	}
	count, err := d.Uint32()
	if err != nil || uint64(count) > uint64(len(data))/2 {
		return nil, errInvalidDistribution
	}
	r.uids = make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		uid, err := d.String()
		if err != nil || uid == "" {
			return nil, errInvalidDistribution
		}
		r.uids = append(r.uids, uid)
	}
	batch, err := d.Binary()
	if err != nil {
		return nil, err
	}
	if err = r.events.Decode(batch); err != nil {
		return nil, err
	}
	rest, err := d.BinaryAll()
	if err != nil || len(rest) != 0 || r.source == 0 || r.target == 0 || r.key == "" || r.channel == "" || len(r.events) == 0 {
		return nil, errInvalidDistribution
	}
	for _, event := range r.events {
		if event.Type != eventbus.EventChannelDistribute || event.TagKey != r.key || event.Conn == nil {
			return nil, errInvalidDistribution
		}
		switch frame := event.Frame.(type) {
		case *wkproto.SendPacket:
			if frame == nil || event.MessageId == 0 {
				return nil, errInvalidDistribution
			}
		case *wkproto.EventPacket:
			if frame == nil || frame.Id == "" {
				return nil, errInvalidDistribution
			}
		default:
			return nil, errInvalidDistribution
		}
	}
	return r, nil
}

type distributionID struct {
	source      uint64
	channel     string
	channelType uint8
	message     int64
	streamEvent [32]byte
}

// distributionEventID separates stream events sharing a parent message ID.
func distributionEventID(r *distributionRequest, event *eventbus.Event) distributionID {
	id := distributionID{source: r.source, channel: r.channel, channelType: r.channelType, message: event.MessageId}
	if frame, ok := event.Frame.(*wkproto.EventPacket); ok {
		hash := sha256.New()
		_, _ = hash.Write([]byte(frame.Id))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(frame.Type))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(frame.Data)
		copy(id.streamEvent[:], hash.Sum(nil))
	}
	return id
}

type distributionReceipt struct {
	key     distributionID
	version string
	expires time.Time
}

// acceptDistribution reserves deduplication entries before admitting any event.
// A successful reply acknowledges snapshot ownership, not final client delivery.
func (h *Handler) acceptDistribution(data []byte) proto.Status {
	r, err := decodeDistribution(data)
	if err != nil || r.target != options.G.Cluster.NodeId {
		return forward.StatusInvalid
	}
	h.distributionMu.Lock()
	defer h.distributionMu.Unlock()
	now := h.distributionTime()
	if h.distributionReceipts == nil {
		h.distributionReceipts = make(map[distributionID]*list.Element)
		h.distributionOrder = list.New()
	}
	for front := h.distributionOrder.Front(); front != nil; front = h.distributionOrder.Front() {
		receipt := front.Value.(distributionReceipt)
		if now.Before(receipt.expires) {
			break
		}
		delete(h.distributionReceipts, receipt.key)
		h.distributionOrder.Remove(front)
	}
	fresh := make([]*eventbus.Event, 0, len(r.events))
	batchIDs := make(map[distributionID]struct{}, len(r.events))
	for _, event := range r.events {
		id := distributionEventID(r, event)
		if entry := h.distributionReceipts[id]; entry != nil {
			if entry.Value.(distributionReceipt).version != r.key {
				return forward.StatusInvalid
			}
			continue
		}
		if _, duplicate := batchIDs[id]; duplicate {
			continue
		}
		batchIDs[id] = struct{}{}
		fresh = append(fresh, event)
	}
	limit := h.distributionLimit
	if limit == 0 {
		limit = distributionAdmissionLimit
	}
	if len(h.distributionReceipts)+len(fresh) > limit {
		return forward.StatusRetry
	}
	reservation := len(data) + len(r.uids)*32 + len(fresh)*512
	byteLimit := h.distributionByteLimit
	if byteLimit == 0 {
		byteLimit = distributionSnapshotBudget
	}
	if len(fresh) > 0 && reservation > byteLimit-h.distributionBytes {
		return forward.StatusRetry
	}
	var remaining atomic.Int64
	remaining.Store(int64(len(fresh)))
	if len(fresh) > 0 {
		h.distributionBytes += reservation
	}
	release := func() {
		if remaining.Add(-1) == 0 {
			h.distributionMu.Lock()
			h.distributionBytes -= reservation
			h.distributionMu.Unlock()
		}
	}
	tag := &types.Tag{Key: r.key, ChannelId: r.channel, ChannelType: r.channelType,
		Nodes: []*types.Node{{LeaderId: r.target, Uids: r.uids}}}
	for _, event := range fresh {
		id := distributionEventID(r, event)
		h.distributionReceipts[id] = h.distributionOrder.PushBack(distributionReceipt{id, r.key, now.Add(distributionAdmissionTTL)})
		event.SourceNodeId = r.source
		event.Distribution = &eventbus.DistributionState{Snapshot: tag, Deadline: now.Add(distributionRetryTimeout), Release: release}
		eventbus.Channel.AddEvent(r.channel, r.channelType, event)
	}
	if len(fresh) > 0 {
		eventbus.Channel.Advance(r.channel, r.channelType)
	}
	return proto.StatusOK
}

func (h *Handler) distributionTime() time.Time {
	if h.distributionNow != nil {
		return h.distributionNow()
	}
	return time.Now()
}

// forwardDistribution retries an ambiguous outcome only through the idempotent
// snapshot route. An explicit missing-route response can safely use legacy Send.
func (h *Handler) forwardDistribution(node uint64, tag *types.Tag, channel string, channelType uint8, events []*eventbus.Event) error {
	r := &distributionRequest{source: options.G.Cluster.NodeId, target: node,
		channel: channel, channelType: channelType, key: tag.Key, uids: tag.GetNodeUsers(node), events: events}
	body, err := r.encode()
	if err != nil {
		return err
	}
	if !h.forwardGate.TryAcquire() {
		return forward.ErrUnavailable
	}
	defer h.forwardGate.Release()
	deadline := time.Now().Add(750 * time.Millisecond)
	for _, event := range events {
		if event.Distribution != nil && event.Distribution.Deadline.Before(deadline) {
			deadline = event.Distribution.Deadline
		}
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	resp, err := service.Cluster.RequestWithContext(ctx, node, distributionPath, body)
	if err != nil {
		return err
	}
	if resp == nil {
		return forward.ErrOutcomeUnknown
	}
	if resp.Status == proto.StatusNotFound {
		legacy := &forwardChannelEventReq{fromNode: options.G.Cluster.NodeId, channelId: channel, channelType: channelType, events: events}
		data, err := legacy.encode()
		if err != nil {
			return err
		}
		return h.sendToNode(node, &proto.Message{MsgType: uint32(msgForwardChannelEvent), Content: data})
	}
	if resp.Status == forward.StatusInvalid {
		return errInvalidDistribution
	}
	if resp.Status != proto.StatusOK {
		return fmt.Errorf("distribution admission status %d", resp.Status)
	}
	return nil
}
