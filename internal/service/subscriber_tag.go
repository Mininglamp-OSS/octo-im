package service

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"sync"
)

var subscriberTagLocks [64]sync.Mutex

// LockSubscriberTag serializes an authoritative tag fill with invalidation.
// Invalidation after a source change cannot be overwritten by a stale fill.
func LockSubscriberTag(ch string, tp uint8) func() {
	m := &subscriberTagLocks[key.ChannelToNum(ch, tp)%64]
	m.Lock()
	return m.Unlock
}

func InvalidateSubscriberTag(ch string, tp uint8) {
	unlock := LockSubscriberTag(ch, tp)
	defer unlock()
	tag := TagManager.GetChannelTag(ch, tp)
	TagManager.RemoveChannelTag(ch, tp)
	if tag != "" {
		TagManager.RemoveTag(tag)
	}
}
