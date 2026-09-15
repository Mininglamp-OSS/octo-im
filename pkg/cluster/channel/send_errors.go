package channel

import (
	"context"
	"errors"
	"net"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

var (
	ErrSendUnavailable    = errors.New("channel send temporarily unavailable")
	ErrSendOutcomeUnknown = errors.New("channel send outcome unknown")
)

// IsRetryableSendError classifies availability failures without making content
// conflicts or malformed results retryable. It never implies a write was absent.
func IsRetryableSendError(err error) bool {
	var networkError net.Error
	return errors.Is(err, ErrSendUnavailable) || errors.Is(err, ErrSendOutcomeUnknown) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) || errors.Is(err, types.ErrNotLeader) ||
		errors.Is(err, types.ErrStopped) || errors.Is(err, types.ErrPaused) ||
		errors.Is(err, types.ErrProposalDropped) || errors.Is(err, ErrNoLeader) || errors.As(err, &networkError)
}

// IsAmbiguousSendError reports failures that can happen after a proposal or
// remote request was accepted. Callers without a durable idempotency key must
// not turn these failures into an instruction to replay the SEND.
func IsAmbiguousSendError(err error) bool {
	var networkError net.Error
	return errors.Is(err, ErrSendOutcomeUnknown) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) || errors.As(err, &networkError)
}
