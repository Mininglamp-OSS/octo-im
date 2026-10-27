package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/forward"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/sendresult"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type sendResultCluster struct {
	icluster.ICluster
	sent map[uint64][]*proto.Message
	err  error
}

func (c *sendResultCluster) Send(node uint64, m *proto.Message) error {
	if c.sent == nil {
		c.sent = map[uint64][]*proto.Message{}
	}
	c.sent[node] = append(c.sent[node], m)
	return c.err
}

func setupSendResult(t *testing.T, node uint64) *sendResultCluster {
	t.Helper()
	oldOptions, oldCluster, oldRegistry := options.G, service.Cluster, sendresult.Default
	t.Cleanup(func() { options.G, service.Cluster, sendresult.Default = oldOptions, oldCluster, oldRegistry })
	options.G = options.New()
	options.G.Cluster.NodeId = node
	c := &sendResultCluster{}
	service.Cluster = c
	sendresult.Default = sendresult.NewRegistry()
	return c
}

func apiSendEvent(id int64, source uint64) *eventbus.Event {
	return &eventbus.Event{
		Type:         eventbus.EventChannelOnSend,
		Conn:         &eventbus.Conn{Uid: "alice", DeviceId: options.G.SystemDeviceId},
		Frame:        &wkproto.SendPacket{ClientMsgNo: "k"},
		MessageId:    id,
		ReasonCode:   wkproto.ReasonSuccess,
		SourceNodeId: source,
	}
}

func TestOnlyAPISendsWaitForSendResult(t *testing.T) {
	setupSendResult(t, 1)
	require.True(t, waitsForSendResult(apiSendEvent(1, 2)))

	older := apiSendEvent(1, 0) // API node on an older build, or a plugin send
	require.False(t, waitsForSendResult(older))

	client := apiSendEvent(1, 2)
	client.Conn = &eventbus.Conn{Uid: "alice", DeviceId: "phone"}
	require.False(t, waitsForSendResult(client), "connection sends keep the SENDACK path")

	noPersist := apiSendEvent(1, 2)
	noPersist.Frame = &wkproto.SendPacket{Framer: wkproto.Framer{NoPersist: true}}
	require.False(t, waitsForSendResult(noPersist))

	distribute := apiSendEvent(1, 2)
	distribute.Type = eventbus.EventChannelDistribute
	require.False(t, waitsForSendResult(distribute))
}

func TestSendResultClassification(t *testing.T) {
	setupSendResult(t, 1)
	waiter := func(reason wkproto.ReasonCode) sendResultWaiter {
		e := apiSendEvent(7, 2)
		e.ReasonCode = reason
		return sendResultWaiter{event: e, key: 7, attempted: reason == wkproto.ReasonSuccess}
	}
	cases := []struct {
		name      string
		denied    bool
		storeErr  error
		resultErr error
		want      sendresult.Outcome
	}{
		{name: "deadline after proposal", storeErr: context.DeadlineExceeded, want: sendresult.OutcomeUnknown},
		{name: "outcome unknown", storeErr: channel.ErrSendOutcomeUnknown, want: sendresult.OutcomeUnknown},
		{name: "not leader", storeErr: rafttypes.ErrNotLeader, want: sendresult.OutcomeUnavailable},
		{name: "unavailable", storeErr: channel.ErrSendUnavailable, want: sendresult.OutcomeUnavailable},
		{name: "conflict", storeErr: channel.ErrMessageConflict, want: sendresult.OutcomeRejected},
		{name: "invalid response after commit", resultErr: errors.New("invalid persistence result"), want: sendresult.OutcomeUnknown},
		{name: "permission denied", denied: true, want: sendresult.OutcomeRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := waiter(wkproto.ReasonSuccess)
			if tc.denied {
				w = waiter(wkproto.ReasonNotAllowSend)
			}
			if tc.storeErr != nil {
				markPersistFailure([]*eventbus.Event{w.event}, tc.storeErr)
			}
			if tc.resultErr != nil {
				markPersistFailure([]*eventbus.Event{w.event}, tc.resultErr)
			}
			res := sendResultOf(w, tc.storeErr, tc.resultErr)
			require.Equal(t, tc.want, res.Outcome)
			require.Equal(t, int64(7), res.Key)
			require.Zero(t, res.MessageId, "only a persisted result carries an id")
		})
	}
}

func TestSendResultReportsCanonicalDuplicate(t *testing.T) {
	setupSendResult(t, 1)
	// A replay of a message whose first attempt committed late.
	e := apiSendEvent(20, 2)
	waiters := collectSendResultWaiters([]*eventbus.Event{e})
	msgs := []wkdb.Message{{RecvPacket: wkproto.RecvPacket{MessageID: 20}}}
	require.NoError(t, applyPersistResults([]*eventbus.Event{e}, msgs, rafttypes.ProposeRespSet{{Id: 20, Index: 9, CanonicalID: 10, Duplicate: true}}))
	res := sendResultOf(waiters[0], nil, nil)
	require.Equal(t, sendresult.OutcomePersisted, res.Outcome)
	require.Equal(t, int64(20), res.Key, "keyed by the attempt, not the canonical id")
	require.Equal(t, int64(10), res.MessageId)
	require.Equal(t, uint64(9), res.MessageSeq)
}

func TestSendResultRoutesToAcceptingNode(t *testing.T) {
	c := setupSendResult(t, 1)
	local := make(chan sendresult.Result, 1)
	require.NoError(t, sendresult.Default.Register(1, local))
	h := NewHandler()
	waiters := collectSendResultWaiters([]*eventbus.Event{apiSendEvent(1, 1), apiSendEvent(2, 3), apiSendEvent(3, 3)})
	h.reportSendResults(waiters, channel.ErrSendUnavailable, nil)

	require.Equal(t, sendresult.OutcomeUnavailable, (<-local).Outcome)
	require.Len(t, c.sent[3], 1, "one report per accepting node")
	msg := c.sent[3][0]
	require.Equal(t, uint32(msgSendResult), msg.MsgType)

	// The accepting node completes its waiters from the report.
	remote := make(chan sendresult.Result, 2)
	require.NoError(t, sendresult.Default.Register(2, remote))
	require.NoError(t, sendresult.Default.Register(3, remote))
	h.OnMessage(msg)
	got := []int64{(<-remote).Key, (<-remote).Key}
	require.ElementsMatch(t, []int64{2, 3}, got)
	require.Zero(t, sendresult.Default.Pending())
}

func TestSendResultReportFailureLeavesWaiterToTimeOut(t *testing.T) {
	c := setupSendResult(t, 1)
	c.err = errors.New("peer down")
	h := NewHandler()
	h.reportSendResults(collectSendResultWaiters([]*eventbus.Event{apiSendEvent(5, 3)}), nil, nil)
	require.Len(t, c.sent[3], 1)
}

func TestForwardFailureReleasesLocalAPIWaiter(t *testing.T) {
	setupSendResult(t, 1)
	ch := make(chan sendresult.Result, 2)
	require.NoError(t, sendresult.Default.Register(11, ch))
	require.NoError(t, sendresult.Default.Register(12, ch))
	forward.Fail([]*eventbus.Event{apiSendEvent(11, 1), apiSendEvent(12, 4)}, errors.New("send failed"))
	res := <-ch
	require.Equal(t, int64(11), res.Key)
	require.Equal(t, sendresult.OutcomeUnknown, res.Outcome, "a transport failure does not prove the peer never received the batch")
	require.Equal(t, 1, sendresult.Default.Pending(), "only waiters accepted by this node are released")

	// An admission-ambiguous forward may still be reported by the peer.
	require.NoError(t, sendresult.Default.Register(13, ch))
	forward.Fail([]*eventbus.Event{apiSendEvent(13, 1)}, forward.ErrOutcomeUnknown)
	require.Equal(t, 2, sendresult.Default.Pending())
}
