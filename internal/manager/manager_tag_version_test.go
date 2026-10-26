package manager

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type channelVersionUpdater interface {
	UpdateChannelTag(string, uint8, []string, bool) error
}

func TestChannelTagUpdatePublishesNewVersion(t *testing.T) {
	for _, remove := range []bool{false, true} {
		t.Run(fmt.Sprint("remove=", remove), func(t *testing.T) {
			m := newSnapshotTestTagManager(t, []string{"fixed", "removed"})
			update, ok := any(m).(channelVersionUpdater)
			require.True(t, ok, "channel membership needs one atomic version-publication operation")
			uid := "new"
			if remove {
				uid = "removed"
			}
			require.NoError(t, update.UpdateChannelTag("g1", 2, []string{uid}, remove))
			key := m.GetChannelTag("g1", 2)
			require.NotEqual(t, "tag", key)
			require.NotNil(t, m.Get(key))
			require.ElementsMatch(t, []string{"fixed", "removed"}, m.Get("tag").GetUsers())
			want := []string{"fixed", "removed", "new"}
			if remove {
				want = []string{"fixed"}
			}
			require.ElementsMatch(t, want, m.Get(key).GetUsers())
		})
	}
}

func TestConcurrentChannelTagUpdatesKeepAllMembers(t *testing.T) {
	m := newSnapshotTestTagManager(t, []string{"fixed"})
	update, ok := any(m).(channelVersionUpdater)
	require.True(t, ok, "channel membership needs atomic publication")
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	want := []string{"fixed"}
	for i := 0; i < 16; i++ {
		uid := fmt.Sprintf("member-%d", i)
		want = append(want, uid)
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- update.UpdateChannelTag("g1", 2, []string{uid}, false)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.ElementsMatch(t, want, m.Get(m.GetChannelTag("g1", 2)).GetUsers())
	require.Equal(t, []string{"fixed"}, m.Get("tag").GetUsers())
}

func TestChannelTagNoOpKeepsVersion(t *testing.T) {
	m := newSnapshotTestTagManager(t, []string{"fixed"})
	update, ok := any(m).(channelVersionUpdater)
	require.True(t, ok, "channel membership needs atomic publication")
	require.NoError(t, update.UpdateChannelTag("g1", 2, []string{"fixed"}, false))
	require.NoError(t, update.UpdateChannelTag("g1", 2, []string{"absent"}, true))
	require.Equal(t, "tag", m.GetChannelTag("g1", 2))
}

func TestChannelTagHistoryIsBoundedWithoutMutatingHeldSnapshot(t *testing.T) {
	m := newSnapshotTestTagManager(t, []string{"fixed"})
	m.retiredLimit = 2
	held := m.Get("tag")
	for i := 0; i < 12; i++ {
		require.NoError(t, m.UpdateChannelTag("g1", 2, []string{fmt.Sprint(i)}, false))
	}
	require.Len(t, m.GetAllTags(), 3)
	require.Equal(t, 2, m.retired.Len())
	require.Nil(t, m.Get("tag"))
	require.Equal(t, []string{"fixed"}, held.GetUsers())
	require.NotNil(t, m.Get(m.GetChannelTag("g1", 2)))
}
