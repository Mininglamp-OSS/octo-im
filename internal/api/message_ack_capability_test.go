package api

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/sendresult"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

type probeCluster struct {
	icluster.ICluster
	request func(ctx context.Context, node uint64, path string) (*proto.Response, error)
}

func (c *probeCluster) RequestWithContext(ctx context.Context, node uint64, path string, _ []byte) (*proto.Response, error) {
	return c.request(ctx, node, path)
}

// useCapabilityCache wires a real send-result capability cache to a fake
// peer and returns the number of probe attempts made.
func useCapabilityCache(t *testing.T, a *sendAcker, policy forward.CapabilityPolicy, probe func(n int32) (*proto.Response, error)) *atomic.Int32 {
	t.Helper()
	old := service.Cluster
	t.Cleanup(func() { service.Cluster = old })
	var probes atomic.Int32
	service.Cluster = &probeCluster{request: func(_ context.Context, _ uint64, path string) (*proto.Response, error) {
		require.Equal(t, sendresult.CapabilityPath, path)
		return probe(probes.Add(1))
	}}
	cache := &forward.CapabilityCache{Path: sendresult.CapabilityPath, Policy: &policy}
	a.check = cache.Check
	return &probes
}

func TestAPISendProbeTimeoutThenOKSubmitsWithout503(t *testing.T) {
	a, leader := newTestAcker(t, 2, 2*time.Second)
	probes := useCapabilityCache(t, a, sendResultPolicy, func(n int32) (*proto.Response, error) {
		if n == 1 {
			return nil, context.DeadlineExceeded
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	})
	ack, err := sendTo(a, "g")
	require.NoError(t, err)
	require.NotZero(t, ack.messageId)
	require.Equal(t, 1, leader.attempts("g"))
	require.EqualValues(t, 2, probes.Load())

	// The positive is retained: the next send does not probe.
	_, err = sendTo(a, "g2")
	require.NoError(t, err)
	require.EqualValues(t, 2, probes.Load())
}

func TestAPISendPermanentlyInconclusiveIsNotPersisted(t *testing.T) {
	a, leader := newTestAcker(t, 2, 2*time.Second)
	probes := useCapabilityCache(t, a, sendResultPolicy, func(int32) (*proto.Response, error) {
		return nil, context.DeadlineExceeded
	})
	r := a.ready(context.Background(), "g", 2)
	require.False(t, r.ok)
	require.Equal(t, forward.CapabilityInconclusive, r.reason)
	require.Equal(t, uint64(2), r.leader)

	_, err := sendTo(a, "g")
	require.ErrorIs(t, err, errSendNotPersisted)
	require.Zero(t, leader.attempts("g"), "nothing is submitted without positive evidence")
	require.Zero(t, a.registry.Pending())

	// Inconclusive is never cached: the next send probes again.
	before := probes.Load()
	_, err = sendTo(a, "g")
	require.ErrorIs(t, err, errSendNotPersisted)
	require.Greater(t, probes.Load(), before)
}

func TestAPISendDefinitiveNegativeIsNotPersisted(t *testing.T) {
	a, leader := newTestAcker(t, 2, time.Second)
	useCapabilityCache(t, a, sendResultPolicy, func(int32) (*proto.Response, error) {
		return &proto.Response{Status: proto.StatusNotFound}, nil
	})
	r := a.ready(context.Background(), "g", 2)
	require.Equal(t, forward.CapabilityUnsupported, r.reason)
	require.True(t, r.verdict.Definitive)
	_, err := sendTo(a, "g")
	require.ErrorIs(t, err, errSendNotPersisted)
	require.Zero(t, leader.attempts("g"))
}

func TestAPISendStalePositiveWithoutReportIsUnknown(t *testing.T) {
	a, leader := newTestAcker(t, 2, 300*time.Millisecond)
	policy := sendResultPolicy
	policy.PositiveFresh = time.Nanosecond // every positive is already stale
	healthy := atomic.Bool{}
	healthy.Store(true)
	useCapabilityCache(t, a, policy, func(int32) (*proto.Response, error) {
		if healthy.Load() {
			return &proto.Response{Status: proto.StatusOK}, nil
		}
		return nil, context.DeadlineExceeded
	})
	require.True(t, a.ready(context.Background(), "seed", 2).ok)

	// A rolled-back leader no longer answers and never reports.
	healthy.Store(false)
	leader.behaviors["g"] = []attemptBehavior{silentCommitAttempt}
	_, err := sendTo(a, "g")
	require.ErrorIs(t, err, errSendOutcomeUnknown, "submitted without a report is 504, never 200 or 503")
	require.Equal(t, 1, leader.attempts("g"))
}

func TestAPISendRetryableResultRechecksReady(t *testing.T) {
	a, leader := newTestAcker(t, 2, 2*time.Second)
	useCapabilityCache(t, a, sendResultPolicy, func(int32) (*proto.Response, error) {
		return &proto.Response{Status: proto.StatusOK}, nil
	})
	check := a.check
	var checks atomic.Int32
	a.check = func(ctx context.Context, node uint64) forward.CapabilityVerdict {
		checks.Add(1)
		return check(ctx, node)
	}
	leader.behaviors["g"] = []attemptBehavior{unavailableAttempt, persistAttempt}
	_, err := sendTo(a, "g")
	require.NoError(t, err)
	require.Equal(t, 2, leader.attempts("g"))
	require.EqualValues(t, 2, checks.Load(), "every replay re-checks the leader")
}

func TestSendResultPolicyConstants(t *testing.T) {
	require.Equal(t, 30*time.Second, sendResultPolicy.PositiveFresh)
	require.Equal(t, 5*time.Minute, sendResultPolicy.PositiveHard)
	require.Equal(t, 5*time.Second, sendResultPolicy.NegativeTTL)
	require.Equal(t, []time.Duration{250 * time.Millisecond, time.Second}, sendResultPolicy.ProbeSteps)
	require.Equal(t, 0.5, sendResultPolicy.SendReserve)
	require.Same(t, &sendResultPolicy, sendResultCapabilities.Policy)
}
