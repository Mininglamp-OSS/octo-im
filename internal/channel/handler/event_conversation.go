package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
)

type targetedConversationPusher interface {
	PushForTargetUsers(channelId string, channelType uint8, tagKey string, targetUIDs []string, events []*eventbus.Event)
}

func (h *Handler) conversation(channelId string, channelType uint8, tagKey string, events []*eventbus.Event) {
	cm := h.conversationManager()
	if cm == nil {
		return
	}
	cm.Push(channelId, channelType, tagKey, events)
}

func (h *Handler) conversationForUsers(channelId string, channelType uint8, tagKey string, targetUIDs []string, events []*eventbus.Event) {
	if len(targetUIDs) == 0 || len(events) == 0 {
		return
	}
	cm := h.conversationManager()
	if cm == nil {
		return
	}
	if tp, ok := cm.(targetedConversationPusher); ok && tp != nil {
		tp.PushForTargetUsers(channelId, channelType, tagKey, targetUIDs, events)
		return
	}
	cm.Push(channelId, channelType, tagKey, events)
}
