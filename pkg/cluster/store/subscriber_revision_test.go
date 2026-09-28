package store

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestManagedSubscriberSnapshotFencesCommittedLegacyCommands(t *testing.T) {
	s, slots := recoveryStore(t)
	cfg := recoveryConfig()
	cfg.Interval = time.Hour
	require.NoError(t, s.StartSubscriberRecovery(cfg, func(context.Context, wkdb.SubscriberWork) error { return nil }))
	receipt, err := s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{
		Mode: "reconcile", OperationID: "business-1", BusinessRevision: 1, ChannelID: "managed", ChannelType: 2, UIDs: []string{"a"},
		Channel: &wkdb.ChannelInfo{ChannelId: "managed", ChannelType: 2, Disband: true, Large: true},
	})
	require.NoError(t, err)
	receipt, err = s.CompleteSubscriberOperation(context.Background(), receipt)
	require.NoError(t, err)
	require.Equal(t, "complete", receipt.State)
	// Pretend old requests were already admitted before authority activation.
	// Rejecting them as retryable Apply errors would stall this slot forever.
	metadata, err := EncodeChannelInfo(wkdb.ChannelInfo{ChannelId: "managed", ChannelType: 2}, CmdVersionChannelInfo)
	require.NoError(t, err)
	commands := []*CMD{
		NewCMDWithVersion(CMDUpdateChannelInfo, metadata, CmdVersionChannelInfo),
		NewCMD(CMDRemoveSubscribers, EncodeChannelUids("managed", 2, []string{"a"})),
		NewCMD(CMDAddSubscribers, EncodeMembers("managed", 2, []wkdb.Member{{Id: 10, Uid: "b"}})),
		NewCMD(CMDAddDenylist, EncodeMembers("managed", 2, []wkdb.Member{{Id: 11, Uid: "a"}})),
		NewCMD(CMDDeleteChannel, EncodeChannel("managed", 2)),
	}
	for _, cmd := range commands {
		data, err := cmd.Marshal()
		require.NoError(t, err)
		_, err = slots.ProposeUntilApplied(slots.GetSlotId("managed"), data)
		require.NoError(t, err, "stale legacy command must not stall Apply")
	}
	info, err := s.DB().GetChannel("managed", 2)
	require.NoError(t, err)
	require.True(t, info.Disband)
	require.True(t, info.Large)
	members, err := s.DB().GetSubscribers("managed", 2)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "a", members[0].Uid)
	denied, err := s.DB().GetDenylist("managed", 2)
	require.NoError(t, err)
	require.Empty(t, denied)
	require.Error(t, s.UpdateChannelInfo(wkdb.ChannelInfo{ChannelId: "managed", ChannelType: 2}))
	// A later authoritative revision still progresses on the same slot.
	receipt, err = s.SubmitSubscriberOperation(context.Background(), wkdb.SubscriberOperation{
		Mode: "reconcile", OperationID: "business-2", BusinessRevision: 2, ChannelID: "managed", ChannelType: 2, UIDs: []string{"a", "b"},
		Channel: &wkdb.ChannelInfo{ChannelId: "managed", ChannelType: 2},
	})
	require.NoError(t, err)
	receipt, err = s.CompleteSubscriberOperation(context.Background(), receipt)
	require.NoError(t, err)
	require.Equal(t, "complete", receipt.State)
}
