package service

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
)

var (
	ErrMessageAdmissionUnavailable = errors.New("message admission temporarily unavailable")
	ErrMessageAdmissionUnknown     = errors.New("message admission outcome unknown")
	ErrMessageAdmissionDenied      = errors.New("message admission denied")
)

// AdmitHTTPMessage returns after the current channel leader has accepted local
// asynchronous work. It does not wait for Raft persistence or delivery.
var AdmitHTTPMessage func(context.Context, string, uint8, *eventbus.Event) error
