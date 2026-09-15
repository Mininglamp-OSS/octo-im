package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPersistCanonicalRetryResults(t *testing.T) {
	e := func(id int64, noPersist bool, reason wkproto.ReasonCode) *eventbus.Event {
		return &eventbus.Event{MessageId: id, ReasonCode: reason, Frame: &wkproto.SendPacket{Framer: wkproto.Framer{NoPersist: noPersist}, ClientSeq: uint64(id), ClientMsgNo: "same"}}
	}
	events := []*eventbus.Event{e(10, false, wkproto.ReasonSuccess), e(11, true, wkproto.ReasonSuccess), e(12, false, wkproto.ReasonSystemError), e(13, false, wkproto.ReasonSuccess)}
	messages := []wkdb.Message{{RecvPacket: wkproto.RecvPacket{MessageID: 10}}, {RecvPacket: wkproto.RecvPacket{MessageID: 13}}}
	results := types.ProposeRespSet{{Id: 10, Index: 7, CanonicalID: 5, Duplicate: true}, {Id: 13, Index: 7, CanonicalID: 5, Duplicate: true}}
	require.NoError(t, applyPersistResults(events, messages, results))
	for _, i := range []int{0, 3} {
		require.Equal(t, int64(5), events[i].MessageId)
		require.Equal(t, uint64(7), events[i].MessageSeq)
		require.Equal(t, wkproto.ReasonSuccess, events[i].ReasonCode)
	}
	require.True(t, events[0].PersistedDuplicate)
	require.True(t, events[3].PersistedDuplicate)
	require.Empty(t, newlyPersistedMessages(events, messages))
	require.False(t, shouldRunPersistSideEffects(events[0]))
	require.True(t, shouldDistributePersistResult(events[0]))
	require.True(t, events[0].Clone().PersistedDuplicate)
	require.Equal(t, int64(11), events[1].MessageId)
	require.Equal(t, int64(12), events[2].MessageId)
	require.Equal(t, uint64(13), events[3].Frame.(*wkproto.SendPacket).ClientSeq)
	for _, m := range messages {
		require.Equal(t, int64(5), m.MessageID)
		require.Equal(t, uint32(7), m.MessageSeq)
	}
	// The duplicate still receives the canonical success ACK and is distributed
	// again in case the first process committed but crashed before dispatch.
	// External plugin and webhook side effects remain suppressed.
	require.Equal(t, wkproto.ReasonSuccess, events[0].ReasonCode)
}
func TestPersistRejectsIncompleteCanonicalResponse(t *testing.T) {
	for _, rs := range []types.ProposeRespSet{nil, {{Id: 10, Index: 1}}, {{Id: 11, Index: 1, CanonicalID: 10}}, {{Id: 10, Index: 0, CanonicalID: 10}}} {
		events := []*eventbus.Event{{MessageId: 10, ReasonCode: wkproto.ReasonSuccess, Frame: &wkproto.SendPacket{}}}
		msgs := []wkdb.Message{{RecvPacket: wkproto.RecvPacket{MessageID: 10}}}
		require.Error(t, applyPersistResults(events, msgs, rs))
		require.Equal(t, int64(10), events[0].MessageId)
		require.Zero(t, events[0].MessageSeq)
	}
}
