package eventbus

import (
	"testing"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestEventClonePreservesForwardAndPresenceState(t *testing.T) {
	event := &Event{
		Type: EventOnSend, Conn: &Conn{Uid: "u", OwnerBootID: "boot", SessionID: "session"},
		Frame:           &wkproto.SendPacket{ClientMsgNo: "stable"},
		ForwardDeadline: 123456789, ForwardHops: 2,
		PersistedDuplicate: true, PersistenceOutcomeUnknown: true, PresenceReconciled: true,
		MessageId: 11, MessageSeq: 12, ReasonCode: wkproto.ReasonSuccess,
		TagKey: "tag", ToUid: "receiver", SourceNodeId: 3, Index: 4,
		OfflineUsers: []string{"offline"}, ChannelId: "channel", ChannelType: 2, ReqId: "request",
	}

	clone := event.Clone()

	require.NotSame(t, event, clone)
	require.Equal(t, event, clone)
	require.Same(t, event.Conn, clone.Conn)
	require.Same(t, event.Frame, clone.Frame)
}

func TestEventRecoveryMetadataDoesNotChangeLegacyEncoding(t *testing.T) {
	event := &Event{Type: EventConnRemove, Conn: &Conn{Uid: "u", NodeId: 1, ConnId: 2}}
	before, err := (EventBatch{event}).Encode()
	require.NoError(t, err)
	size := event.Size()
	event.ForwardDeadline, event.ForwardHops = 123456789, 2
	event.PersistedDuplicate, event.PersistenceOutcomeUnknown, event.PresenceReconciled = true, true, true

	after, err := (EventBatch{event}).Encode()

	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, size, event.Size())
	var decoded EventBatch
	require.NoError(t, decoded.Decode(after))
	require.Len(t, decoded, 1)
	require.Zero(t, decoded[0].ForwardDeadline)
	require.Zero(t, decoded[0].ForwardHops)
	require.False(t, decoded[0].PersistedDuplicate)
	require.False(t, decoded[0].PersistenceOutcomeUnknown)
	require.False(t, decoded[0].PresenceReconciled)
}
