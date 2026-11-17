package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"go.uber.org/zap"
)

func (h *Handler) webhook(ctx *eventbus.ChannelContext) {
	wmKey, hasWMKey := MakeChannelWatermarkKey(ctx.ChannelId, ctx.ChannelType)
	opts := h.opts()
	whSvc := h.webhookService()
	if opts == nil || !opts.WebhookOn(types.EventMsgNotify) || whSvc == nil {
		if hasWMKey {
			for _, ev := range ctx.Events {
				if ev != nil && ev.MessageSeq > 0 {
					_ = h.seqTracker.RecordCommittedForce(wmKey, StageWebhookNotify, ev.MessageSeq)
					ev.WebhookDone = true
				}
			}
		}
		releaseEventTickets(ctx.Events)
		return
	}
	leasedEvents := make([]*eventbus.Event, 0, len(ctx.Events))
	leasedFlags := make([]bool, 0, len(ctx.Events))

	for _, ev := range ctx.Events {
		if ev == nil {
			continue
		}
		if ev.MessageSeq == 0 {
			leasedEvents = append(leasedEvents, ev)
			leasedFlags = append(leasedFlags, false)
			continue
		}
		if !hasWMKey {
			leasedEvents = append(leasedEvents, ev)
			leasedFlags = append(leasedFlags, false)
			continue
		}
		skip, leased := h.seqTracker.BeginStage(wmKey, StageWebhookNotify, ev.MessageSeq)
		if skip {
			if h.seqTracker.HasCommitted(wmKey, StageWebhookNotify, ev.MessageSeq) {
				ev.WebhookDone = true
				if ev.Ticket != nil {
					ev.Ticket.Release()
					ev.Ticket = nil
				}
			} else {
				// Concurrent worker is executing StageWebhookNotify; retain in recovery only if it fails, or re-check shortly.
				_ = h.recoveryMgr.Enqueue(ctx.ChannelId, ctx.ChannelType, RecoveryStageWebhook, eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev)
			}
			continue
		}
		if !leased {
			_ = h.recoveryMgr.Enqueue(ctx.ChannelId, ctx.ChannelType, RecoveryStageWebhook, eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev)
			continue
		}
		leasedEvents = append(leasedEvents, ev)
		leasedFlags = append(leasedFlags, true)
	}

	if len(leasedEvents) == 0 {
		return
	}

	msgs := h.toPersistMessages(ctx.ChannelId, ctx.ChannelType, leasedEvents)
	if len(msgs) > 0 {
		if err := whSvc.AppendMessageOfNotifyQueue(msgs); err != nil {
			h.Error("webhook append message of notify queue failed",
				zap.Error(err),
				zap.Int("msgs", len(leasedEvents)),
				zap.String("channelId", ctx.ChannelId),
				zap.Uint8("channelType", ctx.ChannelType))
			for i, ev := range leasedEvents {
				if leasedFlags[i] && hasWMKey {
					h.seqTracker.AbortStage(wmKey, StageWebhookNotify, ev.MessageSeq)
				}
				if ev.MessageSeq > 0 {
					_ = h.recoveryMgr.Enqueue(ctx.ChannelId, ctx.ChannelType, RecoveryStageWebhook, eventbus.SlotBitset{}, eventbus.SlotBitset{}, ev)
				} else if ev.Ticket != nil {
					ev.Ticket.Release()
					ev.Ticket = nil
				}
			}
			return
		}
	}

	for i, ev := range leasedEvents {
		if leasedFlags[i] && hasWMKey {
			h.seqTracker.CommitStage(wmKey, StageWebhookNotify, ev.MessageSeq)
		}
		ev.WebhookDone = true
		if ev.Ticket != nil {
			ev.Ticket.Release()
			ev.Ticket = nil
		}
	}
}
