package store

import (
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// Slot ownership of the business tables. Every write path proposes to the
// slot returned here, and every read of the same rows is fenced on the same
// slot, so read and write selectors cannot drift apart.
//
//	table                                   owner slot
//	subscriber, channel info, denylist,     channelSlot(channelId)
//	allowlist, message event,
//	channel cluster config
//	user, device                            uidSlot(uid)
//	conversation (create, delete, update)   uidSlot(uid)
//	conversation read position              conversationSeqSlot: uid for
//	                                        person channels, otherwise the
//	                                        channel id
//	system uid, tester, plugin user         globalSlot
//
// Messages, channel last message seq and channel raft state are owned by
// channel raft, not by a slot, and are not fenced here.

// globalSlot holds cluster-global tables.
const globalSlot uint32 = 0

// A Store without a Slot (tests, tools) has no fence either; its reads stay
// unfenced and the selectors are never used for proposals.
func (s *Store) channelSlot(channelId string) uint32 {
	if s.opts.Slot == nil {
		return 0
	}
	return s.opts.Slot.GetSlotId(channelId)
}

func (s *Store) uidSlot(uid string) uint32 {
	if s.opts.Slot == nil {
		return 0
	}
	return s.opts.Slot.GetSlotId(uid)
}

// conversationSeqSlot is the slot UpdateConversationIfSeqGreaterAsync
// proposes to.
func (s *Store) conversationSeqSlot(uid string, channelId string, channelType uint8) uint32 {
	if channelType == wkproto.ChannelTypePerson {
		return s.uidSlot(uid)
	}
	return s.channelSlot(channelId)
}

// conversationSlots are the slots that can write one conversation row: the
// uid slot creates it, and for a non-person channel the channel slot updates
// its read position. Both must be readable to serve the row.
func (s *Store) conversationSlots(uid string, channelId string, channelType uint8) []uint32 {
	uidSlot := s.uidSlot(uid)
	seqSlot := s.conversationSeqSlot(uid, channelId, channelType)
	if seqSlot == uidSlot {
		return []uint32{uidSlot}
	}
	return []uint32{uidSlot, seqSlot}
}

func (s *Store) fence() icluster.SlotReadFence {
	return s.opts.ReadFence
}

// readOwned fences a read of rows owned by one slot (check, read, re-check).
func readOwned[T any](s *Store, slotId uint32, read func() (T, error)) (T, error) {
	return icluster.FencedRead(s.fence(), slotId, read)
}

// readConversations fences a read of a user's conversations: the uid slot
// before and after the read, and after the read also the channel slot of
// every returned non-person row. If any of them is fenced the whole result
// is discarded; a partial list is never returned as complete.
func (s *Store) readConversations(uid string, read func() ([]wkdb.Conversation, error)) ([]wkdb.Conversation, error) {
	fence := s.fence()
	if fence == nil {
		return read()
	}
	uidSlot := s.uidSlot(uid)
	if err := fence.CheckSlotReadable(uidSlot); err != nil {
		return nil, err
	}
	rows, err := read()
	if ferr := fence.CheckSlotReadable(uidSlot); ferr != nil {
		return nil, ferr
	}
	if err != nil {
		return nil, err
	}
	checked := map[uint32]struct{}{uidSlot: {}}
	for _, row := range rows {
		slotId := s.conversationSeqSlot(uid, row.ChannelId, row.ChannelType)
		if _, ok := checked[slotId]; ok {
			continue
		}
		checked[slotId] = struct{}{}
		if ferr := fence.CheckSlotReadable(slotId); ferr != nil {
			return nil, ferr
		}
	}
	return rows, nil
}
