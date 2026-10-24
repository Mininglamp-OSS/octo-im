package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/sendresult"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

// sendResultWaiter is an API send whose accepting node waits for its outcome.
// key is captured before persistence rewrites MessageId to the canonical id.
type sendResultWaiter struct {
	event     *eventbus.Event
	key       int64
	attempted bool
}

// waitsForSendResult: only API sends stamp SourceNodeId on a system-device
// channel send. Older API nodes leave it zero and keep the asynchronous path.
func waitsForSendResult(e *eventbus.Event) bool {
	if e == nil || e.SourceNodeId == 0 || e.Type != eventbus.EventChannelOnSend || e.Conn == nil || !options.G.IsSystemDevice(e.Conn.DeviceId) {
		return false
	}
	packet, ok := e.Frame.(*wkproto.SendPacket)
	return ok && packet != nil && !packet.NoPersist
}

func collectSendResultWaiters(events []*eventbus.Event) []sendResultWaiter {
	var waiters []sendResultWaiter
	for _, e := range events {
		if waitsForSendResult(e) {
			waiters = append(waiters, sendResultWaiter{event: e, key: e.MessageId, attempted: e.ReasonCode == wkproto.ReasonSuccess})
		}
	}
	return waiters
}

// sendResultOf classifies one attempt. storeErr is the AppendMessages error;
// resultErr is a rejected response to a successful AppendMessages call, so the
// batch may be committed and the outcome is unknown.
func sendResultOf(w sendResultWaiter, storeErr, resultErr error) sendresult.Result {
	res := sendresult.Result{Key: w.key, Reason: w.event.ReasonCode}
	switch {
	case !w.attempted:
		res.Outcome = sendresult.OutcomeRejected
	case storeErr != nil && channel.IsAmbiguousSendError(storeErr):
		res.Outcome = sendresult.OutcomeUnknown
	case storeErr != nil && channel.IsRetryableSendError(storeErr):
		res.Outcome = sendresult.OutcomeUnavailable
	case storeErr != nil:
		res.Outcome = sendresult.OutcomeRejected
	case resultErr != nil:
		res.Outcome = sendresult.OutcomeUnknown
	case w.event.ReasonCode != wkproto.ReasonSuccess:
		res.Outcome = sendresult.OutcomeRejected
	default:
		res.Outcome, res.MessageId, res.MessageSeq = sendresult.OutcomePersisted, w.event.MessageId, w.event.MessageSeq
	}
	return res
}

func (h *Handler) reportSendResults(waiters []sendResultWaiter, storeErr, resultErr error) {
	if len(waiters) == 0 {
		return
	}
	byNode := make(map[uint64][]sendresult.Result)
	for _, w := range waiters {
		byNode[w.event.SourceNodeId] = append(byNode[w.event.SourceNodeId], sendResultOf(w, storeErr, resultErr))
	}
	for node, results := range byNode {
		if options.G.IsLocalNode(node) {
			for _, res := range results {
				sendresult.Default.Complete(res)
			}
			continue
		}
		// A lost report leaves the waiter to time out as outcome unknown.
		if err := h.sendToNode(node, &proto.Message{MsgType: uint32(msgSendResult), Content: sendresult.Encode(results)}); err != nil {
			h.Warn("send result report failed", zap.Error(err), zap.Uint64("node", node), zap.Int("results", len(results)))
		}
	}
}

func (h *Handler) onSendResult(m *proto.Message) {
	results, err := sendresult.Decode(m.Content)
	if err != nil {
		h.Error("onSendResult: decode failed", zap.Error(err))
		return
	}
	for _, res := range results {
		sendresult.Default.Complete(res)
	}
}
