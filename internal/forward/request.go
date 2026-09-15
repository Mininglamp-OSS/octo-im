// Package forward owns bounded recovery of inter-node event admission.
// An admission response is not a persistence or delivery acknowledgement.
package forward

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

const (
	UserPath                    = "/wk/forward/user/v1"
	ChannelPath                 = "/wk/forward/channel/v1"
	CapabilityPath              = "/wk/forward/capability/v1"
	StatusRetry    proto.Status = 3
	StatusInvalid  proto.Status = 4
	Budget                      = 5 * time.Second
	maxHops                     = 4
)

var (
	ErrUnavailable    = errors.New("event forwarding unavailable")
	ErrOutcomeUnknown = errors.New("event forwarding outcome unknown")
)

type capabilityEntry struct {
	supported bool
	expiresAt time.Time
}

// CapabilityCache probes a peer before the first side-effecting v1 request.
// A failed read-only probe can safely select the legacy transport during a
// rolling upgrade without guessing after an admission response is lost.
type CapabilityCache struct {
	mu      sync.Mutex
	entries map[uint64]capabilityEntry
}

func RegisterCapabilityRoute() {
	service.Cluster.Route(CapabilityPath, func(c *wkserver.Context) {
		c.WriteStatus(proto.StatusOK)
	})
}

func (c *CapabilityCache) Supports(node uint64) bool {
	now := time.Now()
	c.mu.Lock()
	if entry, ok := c.entries[node]; ok && now.Before(entry.expiresAt) {
		c.mu.Unlock()
		return entry.supported
	}
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	resp, err := service.Cluster.RequestWithContext(ctx, node, CapabilityPath, nil)
	cancel()
	supported := err == nil && resp != nil && resp.Status == proto.StatusOK

	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[uint64]capabilityEntry)
	}
	c.entries[node] = capabilityEntry{supported: supported, expiresAt: now.Add(5 * time.Second)}
	c.mu.Unlock()
	return supported
}

// Envelope bounds retries across successive owners, including already admitted
// events whose authority changes while they wait in an event queue.
func Envelope(body []byte, events []*eventbus.Event) ([]byte, time.Time, error) {
	now := time.Now()
	deadline := now.Add(Budget).UnixMilli()
	var hops uint8
	for _, e := range events {
		if e.ForwardDeadline != 0 && e.ForwardDeadline < deadline {
			deadline = e.ForwardDeadline
		}
		if e.ForwardHops > hops {
			hops = e.ForwardHops
		}
	}
	if hops >= maxHops || deadline <= now.UnixMilli() {
		return nil, time.Time{}, ErrUnavailable
	}
	data := make([]byte, 9+len(body))
	binary.BigEndian.PutUint64(data, uint64(deadline-now.UnixMilli()))
	data[8] = hops + 1
	copy(data[9:], body)
	return data, time.UnixMilli(deadline), nil
}

func Decode(data []byte) (body []byte, deadline int64, hops uint8, err error) {
	if len(data) < 9 {
		return nil, 0, 0, ErrUnavailable
	}
	remaining, hops := time.Duration(binary.BigEndian.Uint64(data))*time.Millisecond, data[8]
	if remaining <= 0 || hops == 0 || hops > maxHops {
		return nil, 0, 0, ErrUnavailable
	}
	if remaining > Budget {
		remaining = Budget
	}
	deadline = time.Now().Add(remaining).UnixMilli()
	return data[9:], deadline, hops, nil
}

// Request re-resolves authority after explicit retry responses. No background
// goroutines or additional queue are created; the caller's bounded event worker
// owns waiting. A transport error is outcome-ambiguous and is never replayed.
func Request(path string, body []byte, events []*eventbus.Event, target func() uint64, local uint64, accept func([]byte) proto.Status) error {
	return request(path, body, events, target, local, accept, nil, nil)
}

// RequestCompatible uses a read-only capability probe before any remote v1
// admission. Legacy fallback is allowed only while no v1 outcome is ambiguous.
func RequestCompatible(path string, body []byte, events []*eventbus.Event, target func() uint64, local uint64, accept func([]byte) proto.Status, capabilities *CapabilityCache, legacy func(uint64) error) error {
	return request(path, body, events, target, local, accept, capabilities, legacy)
}

func request(path string, body []byte, events []*eventbus.Event, target func() uint64, local uint64, accept func([]byte) proto.Status, capabilities *CapabilityCache, legacy func(uint64) error) error {
	data, deadline, err := Envelope(body, events)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	zeroTargets := 0
	for attempt := 0; attempt < 6; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		node := target()
		status := StatusRetry
		if node == local && node != 0 {
			zeroTargets = 0
			status = accept(data)
		} else if node != 0 {
			zeroTargets = 0
			if capabilities != nil && legacy != nil && !capabilities.Supports(node) {
				return legacy(node)
			}
			attemptCtx, done := context.WithTimeout(ctx, 750*time.Millisecond)
			resp, requestErr := service.Cluster.RequestWithContext(attemptCtx, node, path, data)
			done()
			if requestErr != nil || resp == nil {
				return fmt.Errorf("%w: request outcome unknown", ErrOutcomeUnknown)
			} else {
				status = resp.Status
			}
		} else {
			zeroTargets++
			if zeroTargets >= 2 {
				return ErrUnavailable
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

// Fail only answers pre-persistence SEND events. A post-commit distribution
// failure must not contradict the success ACK already sent to the producer.
func Fail(events []*eventbus.Event, cause error) {
	for _, e := range events {
		packet, ok := e.Frame.(*wkproto.SendPacket)
		if !ok || packet == nil || e.Conn == nil ||
			(e.Type != eventbus.EventOnSend && e.Type != eventbus.EventChannelOnSend) ||
			options.G.IsSystemDevice(e.Conn.DeviceId) {
			continue
		}
		reasonCode := wkproto.ReasonNodeNotMatch
		if errors.Is(cause, ErrOutcomeUnknown) {
			reasonCode = wkproto.ReasonSystemError
		}
		eventbus.User.ConnWrite(e.ReqId, e.Conn, &wkproto.SendackPacket{
			Framer: packet.Framer, ClientSeq: packet.ClientSeq, ClientMsgNo: packet.ClientMsgNo,
			ReasonCode: reasonCode,
		})
		eventbus.User.Advance(e.Conn.Uid)
	}
}
