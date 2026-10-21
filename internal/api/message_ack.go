package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/sendresult"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const (
	// maxSendAttempts bounds submissions of one API send. Attempts share the
	// client_msg_no, so the channel idempotency path turns a replay of an
	// attempt that committed late into a duplicate of the first message.
	maxSendAttempts = 3
	sendRetryDelay  = 50 * time.Millisecond
	// sendBatchConcurrency bounds the subscriber sends one batch waits on.
	sendBatchConcurrency = 16
)

var (
	// errSendNotPersisted: no attempt can have committed. Replaying is safe.
	errSendNotPersisted = errors.New("message not persisted: channel leader unavailable")
	// errSendOutcomeUnknown: an attempt may have committed or may still commit.
	errSendOutcomeUnknown = errors.New("message persistence outcome unknown: retry with the same client_msg_no")
)

type sendRejectedError struct{ reason wkproto.ReasonCode }

func (e *sendRejectedError) Error() string {
	return fmt.Sprintf("message rejected: %s", e.reason.String())
}

// sendAck is a persisted API send.
type sendAck struct {
	messageId  int64 // canonical message id
	messageSeq uint64
}

// sendAcker makes an API send acknowledge persistence instead of queueing.
// The slot leader reports every attempt through sendresult; a missing report
// is outcome unknown, never success.
type sendAcker struct {
	wklog.Log
	registry    *sendresult.Registry
	submit      func(channelId string, channelType uint8, event *eventbus.Event)
	leader      func(channelId string, channelType uint8) (uint64, error)
	supports    func(ctx context.Context, node uint64) bool
	newId       func() int64
	budget      func() time.Duration
	maxAttempts int
	retryDelay  time.Duration
}

var sendResultCapabilities = &forward.CapabilityCache{Path: sendresult.CapabilityPath}

var defaultSendAcker = &sendAcker{
	Log:      wklog.NewWKLog("sendAcker"),
	registry: sendresult.Default,
	submit: func(channelId string, channelType uint8, event *eventbus.Event) {
		eventbus.Channel.SendMessage(channelId, channelType, event)
		event.Track.Record(track.PositionStart)
		eventbus.Channel.Advance(channelId, channelType)
	},
	// service.Cluster is assigned after package initialisation.
	leader: func(channelId string, channelType uint8) (uint64, error) {
		return service.Cluster.SlotLeaderIdOfChannel(channelId, channelType)
	},
	supports: sendResultCapabilities.Supports,
	newId:    func() int64 { return options.G.GenMessageId() },
	// The wait spans forwarding plus one channel persistence deadline; it
	// reuses the cluster request deadline rather than adding a new timeout.
	budget:      func() time.Duration { return options.G.Cluster.ReqTimeout },
	maxAttempts: maxSendAttempts,
	retryDelay:  sendRetryDelay,
}

// ready reports whether the current slot leader will report the outcome. A
// leader on an older build never reports, so submitting through it could only
// end in outcome unknown.
func (a *sendAcker) ready(ctx context.Context, channelId string, channelType uint8) bool {
	leader, err := a.leader(channelId, channelType)
	if err != nil || leader == 0 {
		return false
	}
	if options.G.IsLocalNode(leader) {
		return true
	}
	if !a.supports(ctx, leader) {
		a.Warn("channel slot leader does not report API send results", zap.Uint64("leader", leader), zap.String("channelId", channelId))
		return false
	}
	return true
}

// send submits up to maxAttempts events built by build and returns once one
// attempt is persisted, the send is definitely rejected, or the budget ends.
// A replay is made only after every earlier attempt has reported a retryable
// outcome.
func (a *sendAcker) send(channelId string, channelType uint8, build func(messageId int64) *eventbus.Event) (sendAck, error) {
	ctx, cancel := context.WithTimeout(context.Background(), a.budget())
	defer cancel()
	results := make(chan sendresult.Result, a.maxAttempts)
	keys := make([]int64, 0, a.maxAttempts)
	defer func() {
		for _, key := range keys {
			a.registry.Unregister(key)
		}
	}()
	outstanding, ambiguous := 0, false
wait:
	for {
		if outstanding == 0 {
			if len(keys) == a.maxAttempts || ctx.Err() != nil {
				break
			}
			if len(keys) > 0 && a.retryDelay > 0 {
				select {
				case <-ctx.Done():
					break wait
				case <-a.registry.Done():
					break wait
				case <-time.After(a.retryDelay):
				}
			}
			if !a.ready(ctx, channelId, channelType) {
				break
			}
			key := a.newId()
			if err := a.registry.Register(key, results); err != nil {
				break
			}
			keys = append(keys, key)
			outstanding++
			event := build(key)
			event.SourceNodeId = options.G.Cluster.NodeId
			a.submit(channelId, channelType, event)
		}
		select {
		case res := <-results:
			outstanding--
			switch res.Outcome {
			case sendresult.OutcomePersisted:
				return sendAck{messageId: res.MessageId, messageSeq: res.MessageSeq}, nil
			case sendresult.OutcomeRejected:
				if ambiguous || outstanding > 0 {
					// An earlier attempt may still be committed.
					return sendAck{}, errSendOutcomeUnknown
				}
				return sendAck{}, &sendRejectedError{reason: res.Reason}
			case sendresult.OutcomeUnknown:
				ambiguous = true
			}
		case <-ctx.Done():
			break wait
		case <-a.registry.Done():
			break wait
		}
	}
	if ambiguous || outstanding > 0 {
		a.Warn("API send outcome unknown", zap.String("channelId", channelId), zap.Int("attempts", len(keys)))
		return sendAck{}, errSendOutcomeUnknown
	}
	return sendAck{}, errSendNotPersisted
}

// responseSendError keeps the existing error shape and gives the two
// persistence outcomes their own HTTP status so neither is read as success.
func responseSendError(c *wkhttp.Context, err error, clientMsgNo string) {
	status, outcome := 0, ""
	switch {
	case errors.Is(err, errSendOutcomeUnknown):
		status, outcome = http.StatusGatewayTimeout, "unknown"
	case errors.Is(err, errSendNotPersisted):
		status, outcome = http.StatusServiceUnavailable, "not_persisted"
	default:
		c.ResponseError(err)
		return
	}
	c.JSON(status, gin.H{
		"msg":    err.Error(),
		"status": status,
		"data": gin.H{
			"client_msg_no": clientMsgNo,
			"outcome":       outcome,
		},
	})
}
