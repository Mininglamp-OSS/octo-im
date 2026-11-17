package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
)

func (h *Handler) conversation(channelId string, channelType uint8, tagKey string, events []*eventbus.Event) {
	cm := h.conversationManager()
	if cm == nil {
		return
	}
	cm.Push(channelId, channelType, tagKey, events)
}
