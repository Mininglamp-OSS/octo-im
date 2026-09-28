package service

import (
	"fmt"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/stretchr/testify/require"
	"testing"
)

type recoveryTagManager struct {
	ITagManager
	tag string
}

func (m *recoveryTagManager) GetChannelTag(string, uint8) string          { return m.tag }
func (m *recoveryTagManager) SetChannelTag(_ string, _ uint8, tag string) { m.tag = tag }
func (m *recoveryTagManager) RemoveChannelTag(string, uint8)              { m.tag = "" }
func (m *recoveryTagManager) RemoveTag(string)                            {}

func TestSubscriberTagInvalidationRejectsDelayedFill(t *testing.T) {
	old := TagManager
	defer func() { TagManager = old }()
	m := &recoveryTagManager{}
	TagManager = m
	beforeRead, release := BeginSubscriberTag("g", 2)
	defer release()
	InvalidateSubscriberTag("g", 2)
	require.False(t, PublishSubscriberTag("g", 2, beforeRead, "stale"))
	require.Empty(t, m.tag)
	version, releaseFresh := BeginSubscriberTag("g", 2)
	defer releaseFresh()
	require.True(t, PublishSubscriberTag("g", 2, version, "fresh"))
	require.Equal(t, "fresh", m.tag)
}

func TestSubscriberTagCollisionsDoNotInvalidateOtherChannels(t *testing.T) {
	old := TagManager
	defer func() { TagManager = old }()
	TagManager = &recoveryTagManager{}
	a, b := "collision-a", "collision-b"
	for candidate := 0; key.ChannelToNum(a, 2)%64 != key.ChannelToNum(b, 2)%64; candidate++ {
		b = fmt.Sprintf("collision-b-%d", candidate)
	}
	va, releaseA := BeginSubscriberTag(a, 2)
	vb, releaseB := BeginSubscriberTag(b, 2)
	defer releaseA()
	defer releaseB()
	InvalidateSubscriberTag(a, 2)
	require.False(t, PublishSubscriberTag(a, 2, va, "stale"))
	require.True(t, PublishSubscriberTag(b, 2, vb, "unrelated"))
	releaseA()
	releaseB()
	require.Empty(t, subscriberTagVersions[key.ChannelToNum(a, 2)%64], "completed fills must not retain channel entries")
}
