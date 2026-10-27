package forward

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func testPolicy() *CapabilityPolicy {
	return &CapabilityPolicy{
		PositiveFresh: 30 * time.Second,
		PositiveHard:  5 * time.Minute,
		NegativeTTL:   5 * time.Second,
		ProbeSteps:    []time.Duration{250 * time.Millisecond, time.Second},
		SendReserve:   0.5,
	}
}

func newPolicyCache(request func(context.Context, uint64, string) (*proto.Response, error)) (*CapabilityCache, *testClock) {
	clock := &testClock{now: time.Unix(1_000_000, 0)}
	return &CapabilityCache{Path: "/probe", Policy: testPolicy(), request: request, now: clock.Now}, clock
}

func okResponse() *proto.Response { return &proto.Response{Status: proto.StatusOK} }

func TestCapabilityInconclusiveProbeIsNeverCachedAsNegative(t *testing.T) {
	failures := map[string]func() (*proto.Response, error){
		ProbeTimeout:   func() (*proto.Response, error) { return nil, context.DeadlineExceeded },
		ProbeNotAuthed: func() (*proto.Response, error) { return nil, errors.New("connect not authed") },
		ProbeTransport: func() (*proto.Response, error) { return nil, errors.New("conn is nil") },
		"nil_response": func() (*proto.Response, error) { return nil, nil },
		"write_error":  func() (*proto.Response, error) { return nil, errors.New("write: broken pipe") },
	}
	for name, fail := range failures {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			healthy := atomic.Bool{}
			cache, _ := newPolicyCache(func(context.Context, uint64, string) (*proto.Response, error) {
				calls.Add(1)
				if healthy.Load() {
					return okResponse(), nil
				}
				return fail()
			})
			v := cache.Check(context.Background(), 2)
			require.False(t, v.Supported)
			require.False(t, v.Definitive)
			require.Equal(t, CapabilityInconclusive, v.Reason)
			require.EqualValues(t, 2, calls.Load(), "both ladder steps run")

			healthy.Store(true)
			v = cache.Check(context.Background(), 2)
			require.True(t, v.Supported, "the next caller probes again")
			require.False(t, v.Cached)
			require.EqualValues(t, 3, calls.Load())
		})
	}
}

func TestCapabilityNonOKResponseCachesUnsupportedForNegativeTTL(t *testing.T) {
	var calls atomic.Int32
	cache, clock := newPolicyCache(func(context.Context, uint64, string) (*proto.Response, error) {
		calls.Add(1)
		return &proto.Response{Status: proto.StatusNotFound}, nil
	})
	v := cache.Check(context.Background(), 2)
	require.Equal(t, CapabilityVerdict{Definitive: true, Reason: CapabilityUnsupported}, v)
	require.EqualValues(t, 1, calls.Load(), "a definitive answer ends the ladder")

	clock.Advance(4 * time.Second)
	v = cache.Check(context.Background(), 2)
	require.Equal(t, CapabilityVerdict{Definitive: true, Reason: CapabilityUnsupported, Cached: true}, v)
	require.EqualValues(t, 1, calls.Load())

	clock.Advance(time.Second)
	cache.Check(context.Background(), 2)
	require.EqualValues(t, 2, calls.Load(), "the negative expires after NegativeTTL")
}

func TestCapabilityConcurrentColdCallersShareOneProbe(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	cache, _ := newPolicyCache(func(context.Context, uint64, string) (*proto.Response, error) {
		calls.Add(1)
		<-release
		return okResponse(), nil
	})
	const callers = 200
	var wg sync.WaitGroup
	verdicts := make([]CapabilityVerdict, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			verdicts[i] = cache.Check(context.Background(), 2)
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	require.EqualValues(t, 1, calls.Load())
	for _, v := range verdicts {
		require.True(t, v.Supported)
	}
}

func TestCapabilityStalePositiveConcurrentCallersDoNotWaitForRefresh(t *testing.T) {
	var calls atomic.Int32
	var mode atomic.Int32 // 0 ok, 1 blocking timeout, 2 fast timeout
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var staleServes atomic.Int32
	cache, clock := newPolicyCache(func(ctx context.Context, _ uint64, _ string) (*proto.Response, error) {
		calls.Add(1)
		switch mode.Load() {
		case 1:
			entered <- struct{}{}
			<-release
			return nil, context.DeadlineExceeded
		case 2:
			return nil, context.DeadlineExceeded
		}
		return okResponse(), nil
	})
	cache.Policy.OnStaleServe = func() { staleServes.Add(1) }
	require.True(t, cache.Check(context.Background(), 2).Supported)

	clock.Advance(29 * time.Second)
	require.Equal(t, CapabilityVerdict{Supported: true, Cached: true}, cache.Check(context.Background(), 2))
	require.EqualValues(t, 1, calls.Load(), "fresh positives are not probed")

	clock.Advance(2 * time.Second)
	mode.Store(1)
	refreshed := make(chan CapabilityVerdict, 1)
	go func() { refreshed <- cache.Check(context.Background(), 2) }()
	<-entered
	start := time.Now()
	v := cache.Check(context.Background(), 2)
	require.Less(t, time.Since(start), 100*time.Millisecond, "a concurrent stale caller does not wait for the refresh")
	require.Equal(t, CapabilityVerdict{Supported: true, Cached: true, Stale: true}, v)
	release <- struct{}{} // first ladder step times out
	<-entered
	close(release)
	v = <-refreshed
	require.True(t, v.Supported, "an inconclusive refresh keeps the stale positive")
	require.True(t, v.Stale)
	require.Equal(t, ProbeTimeout, v.ErrClass)
	require.EqualValues(t, 2, staleServes.Load())

	// An inconclusive refresh does not extend the positive past PositiveHard.
	mode.Store(2)
	clock.Advance(5*time.Minute - 31*time.Second)
	v = cache.Check(context.Background(), 2)
	require.False(t, v.Supported)
	require.Equal(t, CapabilityInconclusive, v.Reason)

	// A successful refresh makes a stale positive fresh again.
	mode.Store(0)
	require.True(t, cache.Check(context.Background(), 2).Supported)
	clock.Advance(time.Minute)
	require.True(t, cache.Check(context.Background(), 2).Supported)
	require.Equal(t, CapabilityVerdict{Supported: true, Cached: true}, cache.Check(context.Background(), 2))
}

// The caller that starts a stale-positive refresh runs the probe ladder
// before its own send. The wait is bounded by the ladder and leaves the send
// reserve of its budget; an inconclusive refresh still serves the positive.
func TestCapabilityStaleRefreshInitiatorWaitIsBoundedBySendReserve(t *testing.T) {
	var refreshing atomic.Bool
	cache, clock := newPolicyCache(func(ctx context.Context, _ uint64, _ string) (*proto.Response, error) {
		if !refreshing.Load() {
			return okResponse(), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	require.True(t, cache.Check(context.Background(), 2).Supported)
	clock.Advance(31 * time.Second)
	refreshing.Store(true)

	budget, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	start := time.Now()
	v := cache.Check(budget, 2)
	waited := time.Since(start)
	require.Equal(t, CapabilityVerdict{Supported: true, Cached: true, Stale: true, ErrClass: ProbeTimeout}, v)
	require.GreaterOrEqual(t, waited, 150*time.Millisecond, "the initiator waits for its probe")
	require.Less(t, waited, 350*time.Millisecond)
	require.NoError(t, budget.Err(), "the refresh never consumes the send reserve")

	// Without a deadline the wait is the full ladder, 250 ms + 1 s here.
	start = time.Now()
	v = cache.Check(context.Background(), 2)
	require.True(t, v.Stale)
	require.InDelta(t, 1250*time.Millisecond, time.Since(start), float64(150*time.Millisecond))
}

func TestCapabilityStaleRefreshDefinitiveNegativeStopsServing(t *testing.T) {
	negative := atomic.Bool{}
	cache, clock := newPolicyCache(func(context.Context, uint64, string) (*proto.Response, error) {
		if negative.Load() {
			return &proto.Response{Status: proto.StatusNotFound}, nil
		}
		return okResponse(), nil
	})
	require.True(t, cache.Check(context.Background(), 2).Supported)
	clock.Advance(31 * time.Second)
	negative.Store(true)
	require.Equal(t, CapabilityVerdict{Definitive: true, Reason: CapabilityUnsupported}, cache.Check(context.Background(), 2))
	require.Equal(t, CapabilityVerdict{Definitive: true, Reason: CapabilityUnsupported, Cached: true}, cache.Check(context.Background(), 2))
}

func TestCapabilityLadderLeavesTheSendReserve(t *testing.T) {
	var timeouts []time.Duration
	var mu sync.Mutex
	cache, _ := newPolicyCache(func(ctx context.Context, _ uint64, _ string) (*proto.Response, error) {
		deadline, _ := ctx.Deadline()
		mu.Lock()
		timeouts = append(timeouts, time.Until(deadline))
		mu.Unlock()
		return nil, context.DeadlineExceeded
	})

	cache.Check(context.Background(), 2)
	require.Len(t, timeouts, 2)
	require.InDelta(t, 250*time.Millisecond, timeouts[0], float64(20*time.Millisecond))
	require.InDelta(t, time.Second, timeouts[1], float64(20*time.Millisecond))

	// Half of a 10 s budget leaves room for both steps.
	timeouts = nil
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cache.Check(ctx, 3)
	require.Len(t, timeouts, 2)
	require.InDelta(t, time.Second, timeouts[1], float64(20*time.Millisecond))

	// Half of a 400 ms budget caps step 1 and leaves nothing for step 2.
	timeouts = nil
	cache.request = func(ctx context.Context, _ uint64, _ string) (*proto.Response, error) {
		deadline, _ := ctx.Deadline()
		timeouts = append(timeouts, time.Until(deadline))
		<-ctx.Done()
		return nil, ctx.Err()
	}
	short, cancelShort := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancelShort()
	start := time.Now()
	v := cache.Check(short, 4)
	require.Len(t, timeouts, 1)
	require.InDelta(t, 200*time.Millisecond, timeouts[0], float64(30*time.Millisecond))
	require.Less(t, time.Since(start), 350*time.Millisecond)
	require.Equal(t, CapabilityVerdict{Reason: CapabilityInconclusive, ErrClass: ProbeTimeout}, v)
	require.NoError(t, short.Err(), "the probe never consumes the send reserve")
}

func TestCapabilityCallerCancellationDoesNotPoisonSharedProbe(t *testing.T) {
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	cache, _ := newPolicyCache(func(ctx context.Context, _ uint64, _ string) (*proto.Response, error) {
		calls.Add(1)
		close(entered)
		select {
		case <-release:
			return okResponse(), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader := make(chan CapabilityVerdict, 1)
	go func() { leader <- cache.Check(leaderCtx, 2) }()
	<-entered

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	cancelled := make(chan CapabilityVerdict, 1)
	go func() { cancelled <- cache.Check(waiterCtx, 2) }()
	waiter := make(chan CapabilityVerdict, 1)
	go func() { waiter <- cache.Check(context.Background(), 2) }()
	time.Sleep(20 * time.Millisecond)

	cancelWaiter()
	require.Equal(t, CapabilityVerdict{Reason: CapabilityInconclusive, ErrClass: ProbeCallerCancelled}, <-cancelled)
	cancelLeader()
	time.Sleep(20 * time.Millisecond)
	close(release)

	require.Equal(t, CapabilityVerdict{Reason: CapabilityInconclusive, ErrClass: ProbeCallerCancelled}, <-leader)
	require.True(t, (<-waiter).Supported, "a cancelled caller does not cancel the shared probe")
	require.Equal(t, CapabilityVerdict{Supported: true, Cached: true}, cache.Check(context.Background(), 2))
	require.EqualValues(t, 1, calls.Load())

	// A caller whose context is already done does not start a probe.
	done, cancel := context.WithCancel(context.Background())
	cancel()
	require.Equal(t, ProbeCallerCancelled, cache.Check(done, 9).ErrClass)
	require.EqualValues(t, 1, calls.Load())
}

func TestCapabilityNilPolicyKeepsLegacyFailureCaching(t *testing.T) {
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	var calls int
	var timeouts []time.Duration
	service.Cluster = &requestCluster{request: func(ctx context.Context, _ uint64, path string, _ []byte) (*proto.Response, error) {
		calls++
		deadline, _ := ctx.Deadline()
		timeouts = append(timeouts, time.Until(deadline))
		require.Equal(t, CapabilityPath, path)
		return nil, errors.New("connect not authed")
	}}
	cache := &CapabilityCache{}
	require.False(t, cache.Supports(context.Background(), 2))
	require.False(t, cache.Supports(context.Background(), 2))
	require.Equal(t, 1, calls, "legacy caches any failure as unsupported")
	require.Len(t, timeouts, 1)
	require.InDelta(t, 250*time.Millisecond, timeouts[0], float64(20*time.Millisecond))
	require.Equal(t, CapabilityVerdict{Reason: CapabilityUnsupported}, cache.Check(context.Background(), 2))
	require.Equal(t, 1, calls)
	require.Nil(t, cache.states, "the policy state is never touched without a policy")
}

func TestClassifyProbeErrOnlyTreatsAResponseAsDefinitive(t *testing.T) {
	require.Equal(t, ProbeOK, classifyProbeErr(nil, okResponse()))
	require.Equal(t, ProbeDefinitiveNegative, classifyProbeErr(nil, &proto.Response{Status: proto.StatusError}))
	require.Equal(t, ProbeTransport, classifyProbeErr(nil, nil))
	require.Equal(t, ProbeTimeout, classifyProbeErr(context.DeadlineExceeded, nil))
	require.Equal(t, ProbeCallerCancelled, classifyProbeErr(context.Canceled, nil))
	require.Equal(t, ProbeNotAuthed, classifyProbeErr(errors.New("connect not authed"), nil))
	require.Equal(t, ProbeTransport, classifyProbeErr(errors.New("conn is nil"), &proto.Response{Status: proto.StatusOK}))
}
