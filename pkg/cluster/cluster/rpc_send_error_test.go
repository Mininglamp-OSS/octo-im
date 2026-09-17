package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/stretchr/testify/require"
)

func TestChannelProposalFailureStatusPreservesAmbiguity(t *testing.T) {
	require.Equal(t, channelProposalUnavailable, channelProposalFailureStatus(context.DeadlineExceeded))
	require.Equal(t, channelProposalOutcomeUnknown, channelProposalFailureStatus(fmt.Errorf("apply wait: %w", channel.ErrSendOutcomeUnknown)))
	require.Equal(t, channelProposalUnavailable, channelProposalFailureStatus(types.ErrNotLeader))
	require.Equal(t, proto.StatusError, channelProposalFailureStatus(errors.New("content rejected")))
}

func TestChannelProposalResponsePreservesAmbiguity(t *testing.T) {
	for _, response := range []*proto.Response{nil, {Status: channelProposalOutcomeUnknown}} {
		err := channelProposalResponseError(response)
		require.ErrorIs(t, err, channel.ErrSendOutcomeUnknown)
		require.True(t, channel.IsAmbiguousSendError(err))
	}
	require.ErrorIs(t, channelProposalResponseError(&proto.Response{Status: channelProposalUnavailable}), channel.ErrSendUnavailable)
	require.NoError(t, channelProposalResponseError(&proto.Response{Status: proto.StatusOK}))
	require.False(t, channel.IsAmbiguousSendError(channel.ErrSendUnavailable))
}
