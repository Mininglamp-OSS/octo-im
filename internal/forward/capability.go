package forward

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
)

// Verdict reasons. A not-ready verdict is either a definitive negative (the
// peer answered with a non-OK status) or inconclusive (no answer at all).
const (
	CapabilityUnsupported  = "capability_unsupported"
	CapabilityInconclusive = "capability_inconclusive"
)

// Probe result classes. Only ProbeDefinitiveNegative is cached as unsupported;
// every class without a response is inconclusive and never cached.
const (
	ProbeOK                 = "ok"
	ProbeDefinitiveNegative = "definitive_negative"
	ProbeTimeout            = "timeout"
	ProbeNotAuthed          = "not_authed"
	ProbeTransport          = "transport"
	ProbeCallerCancelled    = "caller_cancelled"
)

// CapabilityPolicy retains positive evidence, shares one in-flight probe per
// peer and never turns a probe without a response into a negative. The probe
// runs on the caller's goroutine; no background work is started.
type CapabilityPolicy struct {
	// PositiveFresh serves a supported peer without probing.
	PositiveFresh time.Duration
	// PositiveHard bounds how long a supported peer is served while refresh
	// probes stay inconclusive. Refreshes never extend it.
	PositiveHard time.Duration
	// NegativeTTL caches a definitive non-OK response.
	NegativeTTL time.Duration
	// ProbeSteps are the per-attempt timeouts of one probe ladder.
	ProbeSteps []time.Duration
	// SendReserve is the fraction of the caller's remaining budget a probe
	// leaves for the request it gates.
	SendReserve float64
	// OnProbe and OnStaleServe are optional observability hooks, called
	// without the cache lock held.
	OnProbe      func(class string, latency time.Duration)
	OnStaleServe func()
}

// CapabilityVerdict is the outcome of one Check.
type CapabilityVerdict struct {
	Supported bool
	// Definitive is set when a peer answered with a non-OK status.
	Definitive bool
	// Reason is CapabilityUnsupported or CapabilityInconclusive when not
	// supported.
	Reason string
	// Cached is set when no probe result of this call decided the verdict.
	Cached bool
	// Stale is set when a positive older than PositiveFresh was served.
	Stale bool
	// ErrClass is the last probe failure class, if any.
	ErrClass string
}

type probeCall struct {
	done  chan struct{}
	class string // written before done is closed
}

type capabilityState struct {
	okAt         time.Time
	negUntil     time.Time
	failures     uint64
	lastErrClass string
	inflight     *probeCall
}

// Check reports whether node supports the probed route. With a nil Policy it
// is the legacy probe: any failure is cached as unsupported.
func (c *CapabilityCache) Check(ctx context.Context, node uint64) CapabilityVerdict {
	p := c.Policy
	if p == nil {
		if c.supportsLegacy(ctx, node) {
			return CapabilityVerdict{Supported: true}
		}
		return CapabilityVerdict{Reason: CapabilityUnsupported}
	}

	c.mu.Lock()
	if c.states == nil {
		c.states = make(map[uint64]*capabilityState)
	}
	st := c.states[node]
	if st == nil {
		st = &capabilityState{}
		c.states[node] = st
	}
	now := c.clock()
	if !st.okAt.IsZero() && now.Sub(st.okAt) >= p.PositiveHard {
		st.okAt = time.Time{}
	}
	switch {
	case now.Before(st.negUntil):
		c.mu.Unlock()
		return CapabilityVerdict{Definitive: true, Reason: CapabilityUnsupported, Cached: true}
	case !st.okAt.IsZero() && now.Sub(st.okAt) < p.PositiveFresh:
		c.mu.Unlock()
		return CapabilityVerdict{Supported: true, Cached: true}
	case !st.okAt.IsZero():
		// Stale positive: one caller refreshes and waits for its probe,
		// bounded by the ladder and the send reserve; others do not wait.
		if st.inflight != nil {
			c.mu.Unlock()
			c.staleServed()
			return CapabilityVerdict{Supported: true, Cached: true, Stale: true}
		}
		call := c.startProbe(st)
		c.mu.Unlock()
		class := c.runProbe(ctx, node, call)
		switch class {
		case ProbeOK:
			return CapabilityVerdict{Supported: true}
		case ProbeDefinitiveNegative:
			return CapabilityVerdict{Definitive: true, Reason: CapabilityUnsupported}
		}
		c.staleServed()
		return CapabilityVerdict{Supported: true, Cached: true, Stale: true, ErrClass: class}
	}

	// Absent: join or start the in-flight probe.
	if call := st.inflight; call != nil {
		c.mu.Unlock()
		select {
		case <-call.done:
			return verdictOf(call.class)
		case <-ctx.Done():
			return CapabilityVerdict{Reason: CapabilityInconclusive, ErrClass: ProbeCallerCancelled}
		}
	}
	if ctx.Err() != nil {
		c.mu.Unlock()
		return CapabilityVerdict{Reason: CapabilityInconclusive, ErrClass: ProbeCallerCancelled}
	}
	call := c.startProbe(st)
	c.mu.Unlock()
	class := c.runProbe(ctx, node, call)
	if ctx.Err() != nil {
		return CapabilityVerdict{Reason: CapabilityInconclusive, ErrClass: ProbeCallerCancelled}
	}
	return verdictOf(class)
}

func verdictOf(class string) CapabilityVerdict {
	switch class {
	case ProbeOK:
		return CapabilityVerdict{Supported: true}
	case ProbeDefinitiveNegative:
		return CapabilityVerdict{Definitive: true, Reason: CapabilityUnsupported}
	}
	return CapabilityVerdict{Reason: CapabilityInconclusive, ErrClass: class}
}

// startProbe must be called with c.mu held.
func (c *CapabilityCache) startProbe(st *capabilityState) *probeCall {
	call := &probeCall{done: make(chan struct{})}
	st.inflight = call
	return call
}

// runProbe runs the ladder, records its result and releases waiters. The
// ladder uses its own contexts so a cancelled caller cannot fail a probe that
// other callers share; the caller's deadline only bounds how long it may run.
func (c *CapabilityCache) runProbe(ctx context.Context, node uint64, call *probeCall) string {
	class := c.ladder(ctx, node)
	c.mu.Lock()
	st := c.states[node]
	now := c.clock()
	switch class {
	case ProbeOK:
		st.okAt, st.negUntil = now, time.Time{}
	case ProbeDefinitiveNegative:
		st.okAt, st.negUntil = time.Time{}, now.Add(c.Policy.NegativeTTL)
	default:
		st.failures++
		st.lastErrClass = class
	}
	st.inflight = nil
	call.class = class
	close(call.done)
	c.mu.Unlock()
	return class
}

func (c *CapabilityCache) ladder(ctx context.Context, node uint64) string {
	p := c.Policy
	var limit time.Time
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		limit = time.Now().Add(remaining - time.Duration(float64(remaining)*p.SendReserve))
	}
	path := c.Path
	if path == "" {
		path = CapabilityPath
	}
	class := ProbeTimeout
	for _, step := range p.ProbeSteps {
		timeout := step
		if !limit.IsZero() {
			timeout = min(timeout, time.Until(limit))
		}
		if timeout <= 0 {
			break
		}
		probeCtx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		resp, err := c.doRequest(probeCtx, node, path)
		cancel()
		class = classifyProbeErr(err, resp)
		if p.OnProbe != nil {
			p.OnProbe(class, time.Since(start))
		}
		if class == ProbeOK || class == ProbeDefinitiveNegative {
			return class
		}
	}
	return class
}

// classifyProbeErr decides only on whether a response arrived; the error
// string buckets are for logs and metrics.
func classifyProbeErr(err error, resp *proto.Response) string {
	switch {
	case err == nil && resp != nil && resp.Status == proto.StatusOK:
		return ProbeOK
	case err == nil && resp != nil:
		return ProbeDefinitiveNegative
	case err == nil:
		return ProbeTransport
	case errors.Is(err, context.DeadlineExceeded):
		return ProbeTimeout
	case errors.Is(err, context.Canceled):
		return ProbeCallerCancelled
	case strings.Contains(err.Error(), "not authed"):
		return ProbeNotAuthed
	}
	return ProbeTransport
}

func (c *CapabilityCache) doRequest(ctx context.Context, node uint64, path string) (*proto.Response, error) {
	if c.request != nil {
		return c.request(ctx, node, path)
	}
	return service.Cluster.RequestWithContext(ctx, node, path, nil)
}

func (c *CapabilityCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *CapabilityCache) staleServed() {
	if c.Policy.OnStaleServe != nil {
		c.Policy.OnStaleServe()
	}
}
