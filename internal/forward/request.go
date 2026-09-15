// Package forward owns bounded recovery of inter-node event admission.
// An admission response is not a persistence or delivery acknowledgement.
package forward

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

const (
	UserPath                   = "/wk/forward/user/v1"
	ChannelPath                = "/wk/forward/channel/v1"
	StatusRetry   proto.Status = 3
	StatusInvalid proto.Status = 4
	Budget                     = 5 * time.Second
	maxHops                    = 4
)

var ErrUnavailable = errors.New("event forwarding unavailable")

// Envelope bounds retries across successive owners, including already admitted
// events whose authority changes while they wait in an event queue.
func Envelope(body []byte, events []*eventbus.Event) ([]byte, time.Time, error) {
	deadline := time.Now().Add(Budget).UnixMilli()
	var hops uint8
	for _, e := range events {
		if e.ForwardDeadline != 0 && e.ForwardDeadline < deadline {
			deadline = e.ForwardDeadline
		}
		if e.ForwardHops > hops {
			hops = e.ForwardHops
		}
	}
	if hops >= maxHops || deadline <= time.Now().UnixMilli() {
		return nil, time.Time{}, ErrUnavailable
	}
	data := make([]byte, 9+len(body))
	binary.BigEndian.PutUint64(data, uint64(deadline))
	data[8] = hops + 1
	copy(data[9:], body)
	return data, time.UnixMilli(deadline), nil
}

func Decode(data []byte) (body []byte, deadline int64, hops uint8, err error) {
	if len(data) < 9 {
		return nil, 0, 0, ErrUnavailable
	}
	deadline, hops = int64(binary.BigEndian.Uint64(data)), data[8]
	if deadline <= time.Now().UnixMilli() || deadline > time.Now().Add(Budget+time.Second).UnixMilli() || hops == 0 || hops > maxHops {
		return nil, 0, 0, ErrUnavailable
	}
	return data[9:], deadline, hops, nil
}

// Request re-resolves authority on every attempt. No background goroutines or
// additional queue are created; the caller's bounded event worker owns waiting.
// Ambiguous responses may be retried only for explicitly replayable batches.
func Request(path string, body []byte, events []*eventbus.Event, target func() uint64, local uint64, accept func([]byte) proto.Status) error {
	data, deadline, err := Envelope(body, events)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	replayable := Replayable(events)
	for attempt := 0; attempt < 6; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		node := target()
		status := StatusRetry
		if node == local && node != 0 {
			status = accept(data)
		} else if node != 0 {
			attemptCtx, done := context.WithTimeout(ctx, 750*time.Millisecond)
			resp, requestErr := service.Cluster.RequestWithContext(attemptCtx, node, path, data)
			done()
			if requestErr != nil || resp == nil {
				if !replayable {
					return fmt.Errorf("%w: request outcome unknown", ErrUnavailable)
				}
			} else {
				status = resp.Status
			}
		}
		if status == proto.StatusOK {
			return nil
		}
		if status != StatusRetry {
			return fmt.Errorf("%w: status %d", ErrUnavailable, status)
		}
		if attempt == 5 {
			break
		}
		timer := time.NewTimer(time.Duration(50<<min(attempt, 3)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ErrUnavailable
}

func Replayable(events []*eventbus.Event) bool {
	if len(events) == 0 {
		return false
	}
	for _, e := range events {
		switch packet := e.Frame.(type) {
		case *wkproto.SendPacket:
			if packet.NoPersist || packet.ClientMsgNo == "" {
				return false
			}
		case *wkproto.SendackPacket, *wkproto.PingPacket, *wkproto.PongPacket:
		default:
			return false
		}
	}
	return true
}

// Fail returns an explicitly retryable SENDACK. Its message identity remains
// unset because forwarding cannot determine whether an ambiguous send committed.
func Fail(events []*eventbus.Event) {
	for _, e := range events {
		packet, ok := e.Frame.(*wkproto.SendPacket)
		if !ok || packet == nil || e.Conn == nil {
			continue
		}
		eventbus.User.ConnWrite(e.ReqId, e.Conn, &wkproto.SendackPacket{
			Framer: packet.Framer, ClientSeq: packet.ClientSeq, ClientMsgNo: packet.ClientMsgNo,
			ReasonCode: wkproto.ReasonNodeNotMatch,
		})
		eventbus.User.Advance(e.Conn.Uid)
	}
}
