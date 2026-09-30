package wkdb

import "errors"

// PermanentApplyError marks malformed committed commands or persisted invariant
// violations. Retrying cannot repair these; callers must fail visibly without
// advancing the applied index. Ordinary storage errors remain retryable.
type PermanentApplyError struct{ Err error }

func (e *PermanentApplyError) Error() string { return e.Err.Error() }
func (e *PermanentApplyError) Unwrap() error { return e.Err }

var (
	// ErrMessageNotFound      = errors.New("message not found")
	// ErrUserNotExist         = errors.New("user not exist")
	// ErrDeviceNotExist       = errors.New("device not exist")
	// ErrConversationNotExist = errors.New("conversation not exist")
	// ErrSessionNotExist      = errors.New("session not exist")
	ErrNotFound        = errors.New("not found")
	ErrInvalidUserId   = errors.New("invalid user id")
	ErrInvalidDeviceId = errors.New("invalid device id")
	ErrAlreadyExist    = errors.New("already exist")
)
