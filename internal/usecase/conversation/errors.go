package conversation

import "errors"

// ErrLatestMessageUnavailable means a delete boundary cannot be derived from
// the channel log because no latest message payload is available.
var ErrLatestMessageUnavailable = errors.New("conversation latest message unavailable")
