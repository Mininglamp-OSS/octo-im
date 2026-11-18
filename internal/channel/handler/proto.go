package handler

import (
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// 范围 eventbus.ChannelEventMsgMin ~ options.ChannelEventMsgMax
type msgType uint32

const (
	// 转发用户事件
	msgForwardChannelEvent msgType = 3002
)

type forwardChannelEventReq struct {
	fromNode    uint64
	channelId   string
	channelType uint8
	events      eventbus.EventBatch
	hasSlotMask bool
	slotMask    eventbus.SlotBitset
}

func (f *forwardChannelEventReq) encode() ([]byte, error) {
	return f.encodeWithTrailer(false)
}

func (f *forwardChannelEventReq) encodeWithTrailer(includeTrailer bool) ([]byte, error) {
	if len(f.channelId) > eventbus.MaxWireStringBytes {
		return nil, fmt.Errorf("%w: len=%d", eventbus.ErrWireChannelIDTooLong, len(f.channelId))
	}
	enc := wkproto.NewEncoder()
	enc.WriteString(f.channelId)
	enc.WriteUint8(f.channelType)
	enc.WriteUint64(f.fromNode)
	if len(f.events) > 0 {
		data, err := f.events.Encode()
		if err != nil {
			return nil, err
		}
		enc.WriteBytes(data)
	}
	out := enc.Bytes()
	if includeTrailer {
		if f.hasSlotMask {
			var err error
			out, err = eventbus.EncodeSlotMaskTrailer(out, true, true, f.slotMask)
			if err != nil {
				return nil, err
			}
		}
		out = eventbus.EncodeStageMetaTrailer(out, true, f.events)
	}
	return out, nil
}

func (f *forwardChannelEventReq) decode(data []byte) error {
	dec := wkproto.NewDecoder(data)
	var err error

	if f.channelId, err = dec.String(); err != nil {
		return err
	}
	if f.channelType, err = dec.Uint8(); err != nil {
		return err
	}
	if f.fromNode, err = dec.Uint64(); err != nil {
		return err
	}
	eventData, err := dec.BinaryAll()
	if err != nil {
		return err
	}
	if len(eventData) > 0 {
		rem, err := f.events.DecodeWithRemainder(eventData)
		if err != nil {
			return err
		}
		if len(rem) > 0 {
			hasMask, mask, rest, err := eventbus.DecodeSlotMaskTrailer(rem)
			if err != nil {
				return err
			}
			if hasMask {
				f.hasSlotMask = true
				f.slotMask = mask
				for _, ev := range f.events {
					if ev != nil {
						ev.HasTargetSlotMask = true
						ev.TargetSlotMask = mask.Clone()
					}
				}
			}
			if len(rest) > 0 {
				if err := eventbus.DecodeStageMetaTrailer(rest, f.events); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
