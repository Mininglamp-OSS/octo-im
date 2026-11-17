package plugin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	clusterchannel "github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/WuKongIM/wkrpc"
	"go.uber.org/zap"
)

func (a *rpc) messageSend(c *wkrpc.Context) {
	req := &pluginproto.SendReq{}
	err := req.Unmarshal(c.Body())
	if err != nil {
		a.Error("SendReq unmarshal failed", zap.Error(err))
		c.WriteErr(err)
		return
	}
	if strings.TrimSpace(req.FromUid) == "" {
		req.FromUid = options.G.SystemUID
	}

	if len(req.Payload) <= 0 {
		a.Error("SendReq payload is empty")
		c.WriteErr(errors.New("payload is empty"))
		return
	}
	channelId := req.ChannelId
	channelType := uint8(req.ChannelType)
	if strings.TrimSpace(channelId) == "" {
		a.Error("SendReq channelId is empty")
		c.WriteErr(errors.New("channelId is empty"))
		return
	}
	if options.IsSpecialChar(channelId) {
		a.Error("SendReq channelId is special char")
		c.WriteErr(errors.New("channelId is special char"))
		return
	}

	if req.Header == nil {
		req.Header = &pluginproto.Header{
			RedDot:    false,
			SyncOnce:  false,
			NoPersist: false,
		}
	}

	clientMsgNo := req.ClientMsgNo
	if strings.TrimSpace(clientMsgNo) == "" {
		clientMsgNo = fmt.Sprintf("%s0", wkutil.GenUUID())
	}
	fakeChannelId := channelId
	if channelType == wkproto.ChannelTypePerson {
		fakeChannelId = options.GetFakeChannelIDWith(req.FromUid, channelId)
	} else if channelType == wkproto.ChannelTypeAgent {
		fakeChannelId = options.GetAgentChannelIDWith(req.FromUid, channelId)
	}
	if req.Header.SyncOnce && !options.G.IsOnlineCmdChannel(channelId) && channelType != wkproto.ChannelTypeTemp {
		fakeChannelId = options.G.OrginalConvertCmdChannel(fakeChannelId)
	}

	if len(channelId) > eventbus.MaxWireStringBytes || len(fakeChannelId) > eventbus.MaxWireStringBytes ||
		len(req.FromUid) > eventbus.MaxWireStringBytes || len(clientMsgNo) > eventbus.MaxWireStringBytes {
		c.WriteErr(eventbus.ErrWireChannelIDTooLong)
		return
	}

	sendPacket := &wkproto.SendPacket{
		Framer: wkproto.Framer{
			RedDot:    req.Header.RedDot,
			SyncOnce:  req.Header.SyncOnce,
			NoPersist: req.Header.NoPersist,
		},
		ClientMsgNo: clientMsgNo,
		ChannelID:   channelId,
		ChannelType: channelType,
		Payload:     req.Payload,
	}
	messageId := options.G.GenMessageId()

	event := &eventbus.Event{
		Conn: &eventbus.Conn{
			Uid:      req.FromUid,
			DeviceId: options.G.SystemDeviceId,
		},
		Type:       eventbus.EventChannelOnSend,
		Frame:      sendPacket,
		MessageId:  messageId,
		ReasonCode: wkproto.ReasonSuccess,
		Track: track.Message{
			PreStart: time.Now(),
		},
	}

	// V11-3: Synchronously check permission, reserve PostCommitBudget, and commit to Raft BEFORE returning SendResp.
	if !sendPacket.NoPersist && service.Store != nil && service.Store.HasChannel() {
		if service.Permission != nil {
			rc, pErr := service.Permission.HasPermissionForChannel(fakeChannelId, channelType)
			if pErr != nil {
				c.WriteErr(pErr)
				return
			}
			if rc != wkproto.ReasonSuccess {
				c.WriteErr(fmt.Errorf("channel permission denied: %s", rc.String()))
				return
			}
			rc, sErr := service.Permission.HasPermissionForSender(fakeChannelId, channelType, service.SenderInfo{
				UID:      req.FromUid,
				DeviceID: options.G.SystemDeviceId,
			})
			if sErr != nil {
				c.WriteErr(sErr)
				return
			}
			if rc != wkproto.ReasonSuccess {
				c.WriteErr(fmt.Errorf("sender permission denied: %s", rc.String()))
				return
			}
		}

		tks, rErr := eventbus.DefaultPostCommitBudget.ReserveWithGrace(context.Background(), 50*time.Millisecond, fakeChannelId, []*eventbus.Event{event})
		if rErr != nil {
			c.WriteErr(rErr)
			return
		}
		event.Ticket = tks[0]

		persistMsg := wkdb.Message{
			RecvPacket: wkproto.RecvPacket{
				Framer: wkproto.Framer{
					RedDot:    sendPacket.Framer.RedDot,
					SyncOnce:  sendPacket.Framer.SyncOnce,
					NoPersist: sendPacket.Framer.NoPersist,
				},
				MessageID:   messageId,
				ClientMsgNo: clientMsgNo,
				FromUID:     req.FromUid,
				ChannelID:   fakeChannelId,
				ChannelType: channelType,
				Timestamp:   int32(time.Now().Unix()),
				Payload:     req.Payload,
			},
		}
		timeout := 8 * time.Second
		if options.G != nil && options.G.Channel.ProcessTimeout > 0 {
			timeout = options.G.Channel.ProcessTimeout
		}
		deadlineCtx, cancel := context.WithTimeout(context.Background(), timeout)
		var appendErr error
		delay := 15 * time.Millisecond
		for attempt := 0; attempt < 8; attempt++ {
			results, aErr := service.Store.AppendMessages(deadlineCtx, fakeChannelId, channelType, []wkdb.Message{persistMsg})
			if aErr == nil {
				if len(results) != 1 || results[0] == nil || results[0].CanonicalID == 0 || results[0].Index == 0 {
					appendErr = errors.New("invalid plugin persistence result")
					break
				}
				event.MessageId = int64(results[0].CanonicalID)
				event.MessageSeq = results[0].Index
				event.PersistedDuplicate = results[0].Duplicate
				event.ReasonCode = wkproto.ReasonSuccess
				appendErr = nil
				break
			}
			appendErr = aErr
			retryable := clusterchannel.IsRetryableSendError(aErr) && !clusterchannel.IsAmbiguousSendError(aErr)
			if !retryable || attempt == 7 {
				break
			}
			select {
			case <-deadlineCtx.Done():
				attempt = 8
			case <-time.After(delay):
			}
			delay = min(delay*2, 100*time.Millisecond)
		}
		cancel()
		if appendErr != nil {
			event.Ticket.Release()
			event.Ticket = nil
			c.WriteErr(appendErr)
			return
		}
	} else if sendPacket.NoPersist {
		tks, rErr := eventbus.DefaultTransientBudget.ReserveWithGrace(context.Background(), 50*time.Millisecond, fakeChannelId, []*eventbus.Event{event})
		if rErr != nil {
			c.WriteErr(rErr)
			return
		}
		event.Ticket = tks[0]
	}

	event.Track.Record(track.PositionStart)
	if eventbus.Channel != nil && eventbus.Channel.TrySendMessage(fakeChannelId, channelType, event) {
		eventbus.Channel.Advance(fakeChannelId, channelType)
	} else if event.MessageSeq > 0 && eventbus.EnqueuePersistedRecoveryFallback(fakeChannelId, channelType, event) {
		// Persisted message safely owned by RecoveryManager.
	} else {
		if event.Ticket != nil {
			event.Ticket.Release()
			event.Ticket = nil
		}
		if event.MessageSeq == 0 {
			c.WriteErr(errors.New("channel queue busy"))
			return
		}
	}

	resp := &pluginproto.SendResp{
		MessageId: event.MessageId,
	}
	data, err := resp.Marshal()
	if err != nil {
		a.Error("SendResp marshal failed", zap.Error(err))
		c.WriteErr(err)
		return
	}
	c.Write(data)
}

func (a *rpc) channelMessages(c *wkrpc.Context) {
	channelMessageBatchReq := &pluginproto.ChannelMessageBatchReq{}
	err := channelMessageBatchReq.Unmarshal(c.Body())
	if err != nil {
		a.Error("ChannelMessages unmarshal failed", zap.Error(err))
		c.WriteErr(err)
		return
	}

	resps := make([]*pluginproto.ChannelMessageResp, 0, len(channelMessageBatchReq.ChannelMessageReqs))
	for _, req := range channelMessageBatchReq.ChannelMessageReqs {
		limit := int(req.Limit)
		if limit > 500 {
			limit = 500
		}
		msgs, err := service.Store.LoadNextRangeMsgs(req.ChannelId, uint8(req.ChannelType), uint64(req.StartMessageSeq), 0, limit)
		if err != nil {
			a.Error("LoadNextRangeMsgs failed", zap.Error(err))
			c.WriteErr(err)
			return
		}

		messageResps := make([]*pluginproto.Message, 0, len(msgs))
		for _, msg := range msgs {
			messageResps = append(messageResps, &pluginproto.Message{
				MessageId:   msg.MessageID,
				ClientMsgNo: msg.ClientMsgNo,
				ChannelId:   msg.ChannelID,
				ChannelType: uint32(msg.ChannelType),
				MessageSeq:  uint64(msg.MessageSeq),
				From:        msg.FromUID,
				Payload:     msg.Payload,
				Topic:       msg.Topic,
				StreamNo:    msg.StreamNo,
				StreamId:    msg.StreamId,
				Timestamp:   uint32(msg.Timestamp),
			})
		}

		resp := &pluginproto.ChannelMessageResp{
			ChannelId:       req.ChannelId,
			ChannelType:     req.ChannelType,
			StartMessageSeq: req.StartMessageSeq,
			Messages:        messageResps,
		}
		resps = append(resps, resp)
	}
	channelMsgResp := &pluginproto.ChannelMessageBatchResp{
		ChannelMessageResps: resps,
	}

	data, err := channelMsgResp.Marshal()
	if err != nil {
		a.Error("ChannelMessageBatchResp marshal failed", zap.Error(err))
		c.WriteErr(err)
		return
	}
	c.Write(data)
}
