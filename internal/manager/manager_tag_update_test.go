package manager

import (
	"fmt"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/stretchr/testify/require"
)

// All uids live on node 1 except "orphan", whose slot has no leader.
type updateTestCluster struct{ icluster.ICluster }

func (updateTestCluster) GetSlotId(uid string) uint32 {
	if uid == "orphan" {
		return 2
	}
	return 1
}

func (updateTestCluster) SlotLeaderId(slot uint32) uint64 {
	if slot == 2 {
		return 0
	}
	return 1
}

func newUpdateTestTagManager(t *testing.T, uids []string) *TagManager {
	t.Helper()
	old := service.Cluster
	service.Cluster = updateTestCluster{}
	t.Cleanup(func() { service.Cluster = old })
	tm := NewTagManager(4, func() uint64 { return 0 })
	if uids != nil {
		tag, err := tm.MakeTag(uids)
		require.NoError(t, err)
		tm.SetChannelTag("g1", 2, tag.Key)
	}
	return tm
}

func channelTagUsers(t *testing.T, tm *TagManager) []string {
	t.Helper()
	tag := tm.Get(tm.GetChannelTag("g1", 2))
	require.NotNil(t, tag, "channel lost its tag")
	return tag.GetUsers()
}

func uidRange(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s%d", prefix, i)
	}
	return out
}

// Run f for every uid at the same time, like members joining or leaving one
// group together through the subscriber API.
func concurrently(uids []string, f func(uid string)) {
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, uid := range uids {
		wg.Add(1)
		go func(uid string) {
			defer wg.Done()
			<-start
			f(uid)
		}(uid)
	}
	close(start)
	wg.Wait()
}

func TestUpdateChannelTagUsersKeepsConcurrentAdds(t *testing.T) {
	tm := newUpdateTestTagManager(t, []string{"a"})
	added := uidRange("u", 64)

	concurrently(added, func(uid string) {
		require.NoError(t, tm.UpdateChannelTagUsers("g1", 2, []string{uid}, false))
	})

	require.ElementsMatch(t, append([]string{"a"}, added...), channelTagUsers(t, tm))
}

func TestUpdateChannelTagUsersKeepsConcurrentRemoves(t *testing.T) {
	removed := uidRange("u", 64)
	tm := newUpdateTestTagManager(t, append([]string{"a"}, removed...))

	concurrently(removed, func(uid string) {
		require.NoError(t, tm.UpdateChannelTagUsers("g1", 2, []string{uid}, true))
	})

	require.ElementsMatch(t, []string{"a"}, channelTagUsers(t, tm))
}

func TestUpdateChannelTagUsersMixedConcurrentChanges(t *testing.T) {
	leaving := uidRange("old", 32)
	joining := uidRange("new", 32)
	tm := newUpdateTestTagManager(t, append([]string{"a"}, leaving...))

	concurrently(append(append([]string{}, leaving...), joining...), func(uid string) {
		remove := uid[:3] == "old"
		require.NoError(t, tm.UpdateChannelTagUsers("g1", 2, []string{uid}, remove))
	})

	require.ElementsMatch(t, append([]string{"a"}, joining...), channelTagUsers(t, tm))
}

// Without a cached tag the next message builds one from the store.
func TestUpdateChannelTagUsersWithoutCachedTag(t *testing.T) {
	tm := newUpdateTestTagManager(t, nil)

	require.NoError(t, tm.UpdateChannelTagUsers("g1", 2, []string{"a"}, false))

	require.Empty(t, tm.GetChannelTag("g1", 2))
	require.Empty(t, tm.GetAllTags())
}

// A mapping to a tag that expired is dropped instead of being kept around.
func TestUpdateChannelTagUsersDropsStaleMapping(t *testing.T) {
	tm := newUpdateTestTagManager(t, []string{"a"})
	tm.RemoveTag(tm.GetChannelTag("g1", 2))

	require.NoError(t, tm.UpdateChannelTagUsers("g1", 2, []string{"b"}, false))

	require.Empty(t, tm.GetChannelTag("g1", 2))
}

// When the cached tag cannot be updated it is dropped, so the next message
// rebuilds it from the store instead of delivering with stale members.
func TestUpdateChannelTagUsersDropsTagOnFailure(t *testing.T) {
	tm := newUpdateTestTagManager(t, []string{"a", "b"})
	oldKey := tm.GetChannelTag("g1", 2)

	require.Error(t, tm.UpdateChannelTagUsers("g1", 2, []string{"orphan"}, false))

	require.Empty(t, tm.GetChannelTag("g1", 2))
	require.Nil(t, tm.Get(oldKey))
}
