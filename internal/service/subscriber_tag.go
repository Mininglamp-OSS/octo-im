package service

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"sync"
)

var subscriberTagLocks [64]sync.Mutex

type subscriberTagIdentity struct {
	channel string
	typeID  uint8
}
type subscriberTagGeneration struct {
	version uint64
	users   int
}

var subscriberTagVersions [64]map[subscriberTagIdentity]*subscriberTagGeneration

// BeginSubscriberTag tracks only active fills. Generations belong to a channel,
// while locks remain striped; release reclaims the entry when no fill can still
// publish an old generation, without retaining every historical channel forever.
func BeginSubscriberTag(ch string, tp uint8) (uint64, func()) {
	i := key.ChannelToNum(ch, tp) % 64
	id := subscriberTagIdentity{ch, tp}
	subscriberTagLocks[i].Lock()
	if subscriberTagVersions[i] == nil {
		subscriberTagVersions[i] = make(map[subscriberTagIdentity]*subscriberTagGeneration)
	}
	state := subscriberTagVersions[i][id]
	if state == nil {
		state = &subscriberTagGeneration{}
		subscriberTagVersions[i][id] = state
	}
	state.users++
	version := state.version
	subscriberTagLocks[i].Unlock()
	var once sync.Once
	return version, func() {
		once.Do(func() {
			subscriberTagLocks[i].Lock()
			defer subscriberTagLocks[i].Unlock()
			state.users--
			if state.users == 0 {
				delete(subscriberTagVersions[i], id)
			}
		})
	}
}

func PublishSubscriberTag(ch string, tp uint8, version uint64, tagKey string) bool {
	i := key.ChannelToNum(ch, tp) % 64
	subscriberTagLocks[i].Lock()
	defer subscriberTagLocks[i].Unlock()
	state := subscriberTagVersions[i][subscriberTagIdentity{ch, tp}]
	if state == nil || state.version != version {
		return false
	}
	TagManager.SetChannelTag(ch, tp, tagKey)
	return true
}

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
	if state := subscriberTagVersions[key.ChannelToNum(ch, tp)%64][subscriberTagIdentity{ch, tp}]; state != nil {
		state.version++
	}
	tag := TagManager.GetChannelTag(ch, tp)
	TagManager.RemoveChannelTag(ch, tp)
	if tag != "" {
		TagManager.RemoveTag(tag)
	}
}
