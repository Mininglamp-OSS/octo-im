package channel

import (
	"context"
	"errors"
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSendErrorRetryability(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, types.ErrNotLeader, types.ErrStopped, errMessageLeaderChanged, ErrSendUnavailable} {
		require.True(t, IsRetryableSendError(fmt.Errorf("append: %w", err)))
	}
	require.False(t, IsRetryableSendError(ErrMessageConflict))
	require.False(t, IsRetryableSendError(errors.New("malformed canonical result")))
}
