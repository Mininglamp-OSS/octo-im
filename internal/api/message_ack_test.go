package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/sendresult"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

// attemptBehavior is what the simulated slot leader does with one attempt.
type attemptBehavior int

const (
	persistAttempt      attemptBehavior = iota // commit (or deduplicate) and report persisted
	commitLateUnknown                          // commit, but the proposal deadline fires first: report unknown
	unknownNotCommitted                        // report unknown without committing
	unavailableAttempt                         // definitely not committed, retryable
	rejectAttempt                              // definitely not committed, not retryable
	silentAttempt                              // never report (lost report or stalled queue)
	silentCommitAttempt                        // commit but never report
)

// fakeLeader models the channel idempotency path: one durable message per
// (sender, client_msg_no); a replay returns the canonical first message.
type fakeLeader struct {
	mu        sync.Mutex
	registry  *sendresult.Registry
	behaviors map[string][]attemptBehavior // per channel; the last one repeats
	durable   map[string]int64             // client_msg_no -> canonical id
	seq       uint64
	submitted []*eventbus.Event
	reports   sync.WaitGroup
}

func newFakeLeader(registry *sendresult.Registry) *fakeLeader {
	return &fakeLeader{registry: registry, behaviors: map[string][]attemptBehavior{}, durable: map[string]int64{}}
}

func (l *fakeLeader) attempts(channelId string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.submitted {
		if e.Frame.(*wkproto.SendPacket).ChannelID == channelId {
			n++
		}
	}
	return n
}

func (l *fakeLeader) submit(channelId string, _ uint8, e *eventbus.Event) {
	packet := e.Frame.(*wkproto.SendPacket)
	l.mu.Lock()
	n := 0
	for _, prev := range l.submitted {
		if prev.Frame.(*wkproto.SendPacket).ChannelID == packet.ChannelID {
			n++
		}
	}
	l.submitted = append(l.submitted, e)
	plan := l.behaviors[packet.ChannelID]
	behavior := persistAttempt
	if len(plan) > 0 {
		behavior = plan[min(n, len(plan)-1)]
	}
	commit := func() (int64, uint64) {
		if id, ok := l.durable[packet.ClientMsgNo]; ok {
			return id, 1
		}
		l.seq++
		l.durable[packet.ClientMsgNo] = e.MessageId
		return e.MessageId, l.seq
	}
	res := sendresult.Result{Key: e.MessageId}
	switch behavior {
	case persistAttempt:
		res.MessageId, res.MessageSeq = commit()
		res.Outcome = sendresult.OutcomePersisted
	case commitLateUnknown:
		commit()
		res.Outcome = sendresult.OutcomeUnknown
	case unknownNotCommitted:
		res.Outcome = sendresult.OutcomeUnknown
	case unavailableAttempt:
		res.Outcome = sendresult.OutcomeUnavailable
	case rejectAttempt:
		res.Outcome, res.Reason = sendresult.OutcomeRejected, wkproto.ReasonNotAllowSend
	case silentCommitAttempt:
		commit()
		l.mu.Unlock()
		return
	case silentAttempt:
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()
	// The report crosses nodes asynchronously.
	l.reports.Add(1)
	go func() {
		defer l.reports.Done()
		l.registry.Complete(res)
	}()
}

func (l *fakeLeader) durableCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.durable)
}

func setupAckOptions(t *testing.T) {
	t.Helper()
	old := options.G
	t.Cleanup(func() { options.G = old })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
}

func newTestAcker(t *testing.T, leaderNode uint64, budget time.Duration) (*sendAcker, *fakeLeader) {
	t.Helper()
	setupAckOptions(t)
	registry := sendresult.NewRegistry()
	leader := newFakeLeader(registry)
	var ids atomic.Int64
	ids.Store(1000)
	a := &sendAcker{
		Log:         wklog.NewWKLog("sendAckerTest"),
		registry:    registry,
		submit:      leader.submit,
		leader:      func(string, uint8) (uint64, error) { return leaderNode, nil },
		supports:    func(context.Context, uint64) bool { return true },
		newId:       func() int64 { return ids.Add(1) },
		budget:      func() time.Duration { return budget },
		maxAttempts: maxSendAttempts,
	}
	t.Cleanup(func() {
		leader.reports.Wait()
		require.Zero(t, registry.Pending(), "every waiter must be removed when the send returns")
	})
	return a, leader
}

func sendTo(a *sendAcker, channel string) (sendAck, error) {
	return a.sendMessage(messageSendReq{FromUID: "alice", Payload: []byte("hi")}, channel, wkproto.ChannelTypeGroup, "cmn-"+channel)
}

func TestAPISendAcknowledgesOnlyAfterPersistence(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Second)
	ack, err := sendTo(a, "ok")
	require.NoError(t, err)
	require.Equal(t, int64(1001), ack.messageId)
	require.Equal(t, uint64(1), ack.messageSeq)
	require.Equal(t, 1, leader.attempts("ok"))
	e := leader.submitted[0]
	require.Equal(t, uint64(1), e.SourceNodeId, "the leader reports to the accepting node")
	require.True(t, options.G.IsSystemDevice(e.Conn.DeviceId))
}

func TestAPISendDefiniteFailureIsNotRetried(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Second)
	leader.behaviors["denied"] = []attemptBehavior{rejectAttempt}
	_, err := sendTo(a, "denied")
	var rejected *sendRejectedError
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, wkproto.ReasonNotAllowSend, rejected.reason)
	require.Equal(t, 1, leader.attempts("denied"), "a non-retryable failure must not be replayed")
	require.Zero(t, leader.durableCount())
}

func TestAPISendAmbiguousFailureNeverSucceeds(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Second)
	leader.behaviors["stall"] = []attemptBehavior{unknownNotCommitted}
	_, err := sendTo(a, "stall")
	require.ErrorIs(t, err, errSendOutcomeUnknown)
	require.Equal(t, maxSendAttempts, leader.attempts("stall"), "retries are bounded")
}

func TestAPISendUnavailableExhaustionIsNotPersisted(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Second)
	leader.behaviors["down"] = []attemptBehavior{unavailableAttempt}
	_, err := sendTo(a, "down")
	require.ErrorIs(t, err, errSendNotPersisted)
	require.Equal(t, maxSendAttempts, leader.attempts("down"))
}

func TestAPISendRetryRecoversAfterAmbiguousFailure(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Second)
	leader.behaviors["recover"] = []attemptBehavior{unknownNotCommitted, unavailableAttempt, persistAttempt}
	ack, err := sendTo(a, "recover")
	require.NoError(t, err)
	require.Equal(t, int64(1003), ack.messageId)
	require.Equal(t, 3, leader.attempts("recover"))
	require.Equal(t, 1, leader.durableCount())
}

func TestAPISendRetryDeduplicatesLateFirstCommit(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Second)
	leader.behaviors["late"] = []attemptBehavior{commitLateUnknown, persistAttempt}
	ack, err := sendTo(a, "late")
	require.NoError(t, err)
	require.Equal(t, 2, leader.attempts("late"))
	require.Equal(t, 1, leader.durableCount(), "the replay must not create a second durable message")
	require.Equal(t, leader.submitted[0].MessageId, ack.messageId, "the canonical id is the late first attempt")
	for _, e := range leader.submitted {
		require.Equal(t, "cmn-late", e.Frame.(*wkproto.SendPacket).ClientMsgNo, "every attempt carries the same idempotency key")
	}
}

func TestAPISendRejectionAfterAmbiguousAttemptStaysUnknown(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Second)
	leader.behaviors["mixed"] = []attemptBehavior{commitLateUnknown, rejectAttempt}
	_, err := sendTo(a, "mixed")
	require.ErrorIs(t, err, errSendOutcomeUnknown)
}

func TestAPISendWaiterTimeoutIsUnknownAndCleansUp(t *testing.T) {
	a, leader := newTestAcker(t, 1, 80*time.Millisecond)
	leader.behaviors["lost"] = []attemptBehavior{silentCommitAttempt}
	start := time.Now()
	_, err := sendTo(a, "lost")
	require.ErrorIs(t, err, errSendOutcomeUnknown)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, 1, leader.attempts("lost"), "no replay while an attempt is unresolved")
	require.Zero(t, a.registry.Pending())
	// A report that arrives after the request returned is dropped.
	require.False(t, a.registry.Complete(sendresult.Result{Key: leader.submitted[0].MessageId, Outcome: sendresult.OutcomePersisted}))
}

func TestAPISendShutdownReleasesWaiters(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Minute)
	leader.behaviors["held"] = []attemptBehavior{silentAttempt}
	done := make(chan error, 1)
	go func() {
		_, err := sendTo(a, "held")
		done <- err
	}()
	require.Eventually(t, func() bool { return a.registry.Pending() == 1 }, time.Second, time.Millisecond)
	a.registry.Close()
	select {
	case err := <-done:
		require.ErrorIs(t, err, errSendOutcomeUnknown)
	case <-time.After(time.Second):
		t.Fatal("shutdown must release a waiting send")
	}
	// After shutdown nothing is submitted and nothing can have committed.
	_, err := sendTo(a, "after")
	require.ErrorIs(t, err, errSendNotPersisted)
	require.Zero(t, leader.attempts("after"))
}

func TestAPISendRequiresReportingLeader(t *testing.T) {
	a, leader := newTestAcker(t, 2, time.Second)
	a.supports = func(context.Context, uint64) bool { return false }
	_, err := sendTo(a, "old")
	require.ErrorIs(t, err, errSendNotPersisted)
	require.Zero(t, leader.attempts("old"), "an older leader never reports; do not submit through it")

	a.supports = func(context.Context, uint64) bool { return true }
	_, err = sendTo(a, "new")
	require.NoError(t, err)
}

func TestAPISendLeaderLookupFailureIsNotPersisted(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Second)
	a.leader = func(string, uint8) (uint64, error) { return 0, errors.New("no slot leader") }
	_, err := sendTo(a, "x")
	require.ErrorIs(t, err, errSendNotPersisted)
	require.Zero(t, leader.attempts("x"))
}

func TestAPISendNoPersistKeepsQueuedAck(t *testing.T) {
	a, leader := newTestAcker(t, 1, time.Second)
	leader.behaviors["cmd"] = []attemptBehavior{silentAttempt}
	req := messageSendReq{FromUID: "alice", Payload: []byte("hi")}
	req.Header.NoPersist = 1
	ack, err := a.sendMessage(req, "cmd", wkproto.ChannelTypeGroup, "cmn")
	require.NoError(t, err)
	require.NotZero(t, ack.messageId)
	require.Zero(t, leader.submitted[0].SourceNodeId, "nothing is persisted, so nothing is reported")
}

func serveMessage(t *testing.T, a *sendAcker, path, body string) (int, map[string]any) {
	t.Helper()
	old := defaultSendAcker
	defaultSendAcker = a
	t.Cleanup(func() { defaultSendAcker = old })
	router := wkhttp.New()
	newMessage(nil).route(router)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	router.GetGinRoute().ServeHTTP(resp, req)
	out := map[string]any{}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out), resp.Body.String())
	return resp.Code, out
}

func TestMessageSendHTTPOutcomes(t *testing.T) {
	a, leader := newTestAcker(t, 1, 100*time.Millisecond)
	leader.behaviors["g1"] = []attemptBehavior{persistAttempt}
	leader.behaviors["g2"] = []attemptBehavior{unknownNotCommitted}
	leader.behaviors["g3"] = []attemptBehavior{unavailableAttempt}
	leader.behaviors["g4"] = []attemptBehavior{rejectAttempt}
	payload := `"payload":"aGk="`

	code, out := serveMessage(t, a, "/message/send", `{"from_uid":"alice","channel_id":"g1","channel_type":2,"client_msg_no":"k1",`+payload+`}`)
	require.Equal(t, http.StatusOK, code)
	data := out["data"].(map[string]any)
	require.NotZero(t, data["message_id"])
	require.EqualValues(t, 1, data["message_seq"])
	require.Equal(t, "k1", data["client_msg_no"])

	code, out = serveMessage(t, a, "/message/send", `{"from_uid":"alice","channel_id":"g2","channel_type":2,"client_msg_no":"k2",`+payload+`}`)
	require.Equal(t, http.StatusGatewayTimeout, code)
	require.Equal(t, "unknown", out["data"].(map[string]any)["outcome"])
	require.Equal(t, "k2", out["data"].(map[string]any)["client_msg_no"])

	code, out = serveMessage(t, a, "/message/send", `{"from_uid":"alice","channel_id":"g3","channel_type":2,`+payload+`}`)
	require.Equal(t, http.StatusServiceUnavailable, code)
	require.Equal(t, "not_persisted", out["data"].(map[string]any)["outcome"])
	require.NotEmpty(t, out["data"].(map[string]any)["client_msg_no"], "a generated key is returned so the caller can replay idempotently")

	code, _ = serveMessage(t, a, "/message/send", `{"from_uid":"alice","channel_id":"g4","channel_type":2,`+payload+`}`)
	require.Equal(t, http.StatusBadRequest, code)
}

func TestMessageSendBatchOutcomes(t *testing.T) {
	a, leader := newTestAcker(t, 1, 100*time.Millisecond)
	leader.behaviors["u2"] = []attemptBehavior{rejectAttempt}
	leader.behaviors["u3"] = []attemptBehavior{unavailableAttempt}
	body := `{"from_uid":"alice","subscribers":["u1","u2","u3"],"payload":"aGk="}`
	code, out := serveMessage(t, a, "/message/sendbatch", body)
	require.Equal(t, http.StatusOK, code, "definite failures keep the existing partial-failure contract")
	require.ElementsMatch(t, []any{"u2", "u3"}, out["fail_uids"])
	require.Len(t, out["reason"], 2)

	leader.behaviors["u5"] = []attemptBehavior{unknownNotCommitted}
	body = `{"from_uid":"alice","subscribers":["u4","u5","u6"],"payload":"aGk="}`
	code, out = serveMessage(t, a, "/message/sendbatch", body)
	require.Equal(t, http.StatusGatewayTimeout, code, "an ambiguous subscriber must not yield HTTP 200")
	require.Equal(t, []any{"u5"}, out["fail_uids"])
	require.Equal(t, []any{"u5"}, out["unknown_uids"])
	require.Equal(t, 1, leader.attempts("u4"))
	require.Equal(t, 1, leader.attempts("u6"))
}
