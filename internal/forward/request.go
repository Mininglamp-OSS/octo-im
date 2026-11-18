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
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
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
	MaxHops                     = 4
	maxHops                     = MaxHops
)

var (
	ErrUnavailable    = errors.New("event forwarding unavailable")
	ErrOutcomeUnknown = errors.New("event forwarding outcome unknown")
	ErrPeerNoAck      = errors.New("peer does not provide confirmed forward admission (no ACK)")

	capabilityV2Magic = [4]byte{'W', 'K', 'F', '2'}
)

type PeerCapMode uint8

const (
	PeerCapUnknown PeerCapMode = iota
	PeerCapBaseV1
	PeerCapPartAV2
	PeerCapUnsupported
)

// ForwardOutcome explicitly distinguishes confirmed admission from retryable rejection,
// ambiguous transport failure, and unsupported no-ACK peers (B5).
type ForwardOutcome uint8

const (
	ForwardAccepted ForwardOutcome = iota + 1
	ForwardExplicitRetry
	ForwardOutcomeUnknown
	ForwardUnsupportedPeerNoAck
)

type capabilityEntry struct {
	nodeID    uint64
	mode      PeerCapMode
	version   uint8
	expiresAt time.Time
}

func BuildCapabilityBody() []byte {
	return []byte{'W', 'K', 'F', '2', 1}
}

// Gate reserves only a bounded fraction of an event worker pool for forwarding
// waits. Admission is non-blocking so a peer outage cannot occupy every shared
// worker and stall unrelated users or channels.
type Gate struct {
	slots chan struct{}
}

func NewGate(workerCount int) *Gate {
	limit := workerCount / 4
	if limit < 1 {
		limit = 1
	}
	return &Gate{slots: make(chan struct{}, limit)}
}

func (g *Gate) TryAcquire() bool {
	if g == nil {
		return true
	}
	select {
	case g.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (g *Gate) Release() {
	if g != nil {
		<-g.slots
	}
}

// OnlyRetryableSends reports whether every event can receive a retryable SENDACK.
func OnlyRetryableSends(events []*eventbus.Event) bool {
	if len(events) == 0 {
		return false
	}
	for _, event := range events {
		if event == nil || event.Conn == nil || options.G.IsSystemDevice(event.Conn.DeviceId) ||
			(event.Type != eventbus.EventOnSend && event.Type != eventbus.EventChannelOnSend) {
			return false
		}
		if packet, ok := event.Frame.(*wkproto.SendPacket); !ok || packet == nil {
			return false
		}
	}
	return true
}

// CapabilityCache probes a peer before the first side-effecting v1 request.
// Transport timeouts return PeerCapUnknown without poisoning the cache.
type CapabilityCache struct {
	mu      sync.Mutex
	entries map[uint64]capabilityEntry
}

func RegisterCapabilityRouteOn(cl icluster.ICluster) {
	if cl == nil {
		return
	}
	cl.Route(CapabilityPath, func(c *wkserver.Context) {
		c.Write(BuildCapabilityBody())
	})
}

func RegisterCapabilityRoute() {
	RegisterCapabilityRouteOn(service.Cluster)
}

func (c *CapabilityCache) InvalidateAll() {
	if c == nil {
		return
	}
	c.mu.Lock()
	clear(c.entries)
	c.mu.Unlock()
}

func (c *CapabilityCache) InvalidateNode(node uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.entries, node)
	c.mu.Unlock()
}

func (c *CapabilityCache) ProbeOn(ctx context.Context, cl icluster.ICluster, node uint64) PeerCapMode {
	if c == nil {
		return PeerCapUnknown
	}
	if cl == nil {
		cl = service.Cluster
	}
	if cl == nil || node == 0 {
		return PeerCapUnknown
	}
	now := time.Now()
	c.mu.Lock()
	if entry, ok := c.entries[node]; ok && entry.nodeID == node && entry.mode != PeerCapUnknown && now.Before(entry.expiresAt) {
		c.mu.Unlock()
		return entry.mode
	}
	c.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	probeCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	resp, err := cl.RequestWithContext(probeCtx, node, CapabilityPath, nil)
	cancel()
	if err != nil || resp == nil {
		return PeerCapUnknown
	}

	var mode PeerCapMode
	var ver uint8
	ttl := 5 * time.Second
	if resp.Status == proto.StatusOK {
		if len(resp.Body) == 0 {
			mode = PeerCapBaseV1
			ver = 0
			ttl = 5 * time.Second
		} else if len(resp.Body) == 5 &&
			resp.Body[0] == capabilityV2Magic[0] &&
			resp.Body[1] == capabilityV2Magic[1] &&
			resp.Body[2] == capabilityV2Magic[2] &&
			resp.Body[3] == capabilityV2Magic[3] &&
			resp.Body[4] == 1 {
			mode = PeerCapPartAV2
			ver = resp.Body[4]
			ttl = 30 * time.Second
		} else {
			// Unknown non-empty capability payload or unsupported capability version fails closed (B5).
			mode = PeerCapUnsupported
			ttl = 5 * time.Second
		}
	} else {
		mode = PeerCapUnsupported
		ttl = 5 * time.Second
	}

	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[uint64]capabilityEntry)
	}
	c.entries[node] = capabilityEntry{nodeID: node, mode: mode, version: ver, expiresAt: now.Add(ttl)}
	c.mu.Unlock()
	return mode
}

func (c *CapabilityCache) Probe(ctx context.Context, node uint64) PeerCapMode {
	return c.ProbeOn(ctx, service.Cluster, node)
}

func (c *CapabilityCache) Supports(ctx context.Context, node uint64) bool {
	mode := c.ProbeOn(ctx, service.Cluster, node)
	return mode == PeerCapBaseV1 || mode == PeerCapPartAV2
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

func batchHasPersistedEvent(events []*eventbus.Event) bool {
	for _, ev := range events {
		if ev != nil && ev.MessageSeq > 0 {
			return true
		}
	}
	return false
}

// Request re-resolves authority after explicit retry responses. No background
// goroutines or additional queue are created; the caller's bounded event worker
// owns waiting. A transport error is outcome-ambiguous and is never replayed.
func Request(path string, body []byte, events []*eventbus.Event, target func() uint64, local uint64, accept func([]byte) proto.Status) error {
	return RequestOnCluster(service.Cluster, path, body, events, target, local, accept, nil, nil)
}

// RequestCompatible uses a read-only capability probe before any remote v1
// admission. Legacy fallback is allowed only while no v1 outcome is ambiguous
// and only for pre-persistence events.
func RequestCompatible(path string, body []byte, events []*eventbus.Event, target func() uint64, local uint64, accept func([]byte) proto.Status, capabilities *CapabilityCache, legacy func(uint64) error) error {
	return RequestOnCluster(service.Cluster, path, body, events, target, local, accept, capabilities, legacy)
}

func RequestOnCluster(cl icluster.ICluster, path string, body []byte, events []*eventbus.Event, target func() uint64, local uint64, accept func([]byte) proto.Status, capabilities *CapabilityCache, legacy func(uint64) error) error {
	_, err := RequestDetailedOnCluster(cl, path, body, events, target, local, accept, capabilities, legacy)
	return err
}

func RequestDetailedOnCluster(
	cl icluster.ICluster,
	path string,
	body []byte,
	events []*eventbus.Event,
	target func() uint64,
	local uint64,
	accept func([]byte) proto.Status,
	capabilities *CapabilityCache,
	legacy func(uint64) error,
) (ForwardOutcome, error) {
	if cl == nil {
		cl = service.Cluster
	}
	data, deadline, err := Envelope(body, events)
	if err != nil {
		return ForwardExplicitRetry, err
	}
	persisted := batchHasPersistedEvent(events)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	zeroTargets := 0
	sawNoAckPeer := false
	for attempt := 0; attempt < 6; attempt++ {
		if err := ctx.Err(); err != nil {
			if sawNoAckPeer {
				return ForwardUnsupportedPeerNoAck, fmt.Errorf("%w: %v", ErrPeerNoAck, err)
			}
			return ForwardExplicitRetry, err
		}
		node := target()
		status := StatusRetry
		if node == local && node != 0 {
			zeroTargets = 0
			status = accept(data)
		} else if node != 0 {
			zeroTargets = 0
			if cl == nil {
				return ForwardExplicitRetry, ErrUnavailable
			}
			if capabilities != nil {
				mode := capabilities.ProbeOn(ctx, cl, node)
				if mode != PeerCapBaseV1 && mode != PeerCapPartAV2 {
					if err := ctx.Err(); err != nil {
						if persisted {
							return ForwardUnsupportedPeerNoAck, fmt.Errorf("%w: %v", ErrPeerNoAck, err)
						}
						return ForwardExplicitRetry, err
					}
					// B5 + V12-3: Never downgrade persisted (MessageSeq > 0) events to fire-and-forget legacy Send
					// on PeerCapUnknown or PeerCapUnsupported. Send-only success is NOT confirmed admission.
					if persisted {
						sawNoAckPeer = true
						if mode == PeerCapUnsupported {
							return ForwardUnsupportedPeerNoAck, fmt.Errorf("%w: peer %d does not support confirmed forward admission", ErrPeerNoAck, node)
						}
						status = StatusRetry
						goto nextAttempt
					}
					if legacy != nil {
						if lErr := legacy(node); lErr != nil {
							return ForwardExplicitRetry, lErr
						}
						return ForwardAccepted, nil
					}
					status = StatusRetry
					goto nextAttempt
				}
			}
			attemptCtx, done := context.WithTimeout(ctx, 750*time.Millisecond)
			resp, requestErr := cl.RequestWithContext(attemptCtx, node, path, data)
			done()
			if requestErr != nil || resp == nil {
				return ForwardOutcomeUnknown, fmt.Errorf("%w: request outcome unknown", ErrOutcomeUnknown)
			}
			status = resp.Status
		} else {
			zeroTargets++
			if zeroTargets >= 2 {
				return ForwardExplicitRetry, ErrUnavailable
			}
		}
	nextAttempt:
		if status == proto.StatusOK {
			return ForwardAccepted, nil
		}
		if status != StatusRetry {
			return ForwardExplicitRetry, fmt.Errorf("%w: status %d", ErrUnavailable, status)
		}
		if attempt == 5 {
			break
		}
		timer := time.NewTimer(time.Duration(50<<min(attempt, 3)) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			if sawNoAckPeer {
				return ForwardUnsupportedPeerNoAck, fmt.Errorf("%w: %v", ErrPeerNoAck, ctx.Err())
			}
			return ForwardExplicitRetry, ctx.Err()
		case <-timer.C:
		}
	}
	if sawNoAckPeer {
		return ForwardUnsupportedPeerNoAck, fmt.Errorf("%w: peer capability unconfirmed", ErrPeerNoAck)
	}
	return ForwardExplicitRetry, ErrUnavailable
}

// Fail only answers pre-persistence SEND events. A post-commit distribution
// failure must not contradict the success ACK already sent to the producer.
func Fail(events []*eventbus.Event, cause error) {
	if errors.Is(cause, ErrOutcomeUnknown) {
		return
	}
	for _, e := range events {
		if e == nil || e.MessageSeq > 0 {
			continue
		}
		packet, ok := e.Frame.(*wkproto.SendPacket)
		if !ok || packet == nil || e.Conn == nil ||
			(e.Type != eventbus.EventOnSend && e.Type != eventbus.EventChannelOnSend) ||
			options.G.IsSystemDevice(e.Conn.DeviceId) {
			continue
		}
		eventbus.User.ConnWrite(e.ReqId, e.Conn, &wkproto.SendackPacket{
			Framer: packet.Framer, ClientSeq: packet.ClientSeq, ClientMsgNo: packet.ClientMsgNo,
			ReasonCode: wkproto.ReasonNodeNotMatch,
		})
		eventbus.User.Advance(e.Conn.Uid)
	}
}
