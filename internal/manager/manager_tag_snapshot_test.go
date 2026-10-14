package manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/stretchr/testify/require"
)

// Every uid lives on node 1, so a tag has a single node.
type snapshotTestCluster struct{ icluster.ICluster }

func (snapshotTestCluster) GetSlotId(string) uint32    { return 1 }
func (snapshotTestCluster) SlotLeaderId(uint32) uint64 { return 1 }

func newSnapshotTestTagManager(t *testing.T, uids []string) *TagManager {
	t.Helper()
	old := service.Cluster
	service.Cluster = snapshotTestCluster{}
	t.Cleanup(func() { service.Cluster = old })
	tm := NewTagManager(4, func() uint64 { return 0 })
	_, err := tm.MakeTagWithTagKey("tag", uids)
	require.NoError(t, err)
	tm.SetChannelTag("g1", 2, "tag")
	return tm
}

// Fan-out holds the tag returned by Get and ranges over node.Uids while
// subscriber changes are applied. The list it started with must not change.
func TestTagSnapshotSurvivesRemoveUsers(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b", "c", "d", "e"})
	inFlight := tm.Get("tag").Nodes[0].Uids

	require.NoError(t, tm.RemoveUsers("tag", []string{"b"}))

	require.Equal(t, []string{"a", "b", "c", "d", "e"}, inFlight, "in-flight fan-out saw the removal shift its list")
	require.Equal(t, []string{"a", "c", "d", "e"}, tm.Get("tag").Nodes[0].Uids)
}

func TestTagSnapshotSurvivesReAddingMember(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b", "c", "d", "e"})
	inFlight := tm.Get("tag").Nodes[0].Uids

	// Re-adding an existing member removes and appends it.
	require.NoError(t, tm.AddUsers("tag", []string{"b"}))

	require.Equal(t, []string{"a", "b", "c", "d", "e"}, inFlight, "in-flight fan-out saw the re-add rewrite its list")
	require.ElementsMatch(t, []string{"a", "b", "c", "d", "e"}, tm.Get("tag").Nodes[0].Uids)
}

func TestTagSnapshotKeepsKeyAcrossRename(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b"})
	inFlight := tm.Get("tag")

	require.NoError(t, tm.RenameTag("tag", "tag2"))

	// Fan-out stamps events with the key of the tag it delivered with.
	require.Equal(t, "tag", inFlight.Key)
	require.Equal(t, "tag2", tm.Get("tag2").Key)
	require.Nil(t, tm.Get("tag"))
}

// Readers fan out continuously while members leave and rejoin, as the channel
// handler and the subscriber API do. Every pass must see each member exactly
// once; the always-present members must never be skipped. Run with -race.
func TestTagFanoutDuringMembershipChanges(t *testing.T) {
	stable := []string{"a", "s", "m0", "m1", "m2", "m3"}
	churn := []string{"b", "c", "d"}
	tm := newSnapshotTestTagManager(t, append(append([]string{}, stable...), churn...))

	stop := make(chan struct{})
	var passes atomic.Int64
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				tag := tm.Get(tm.GetChannelTag("g1", 2))
				if tag == nil {
					continue
				}
				seen := map[string]int{}
				for _, node := range tag.Nodes {
					for _, uid := range node.Uids {
						seen[uid]++
					}
				}
				for uid, n := range seen {
					if n > 1 {
						errs <- fmt.Errorf("uid %s delivered %d times in one pass", uid, n)
						return
					}
				}
				for _, uid := range stable {
					if seen[uid] == 0 {
						errs <- fmt.Errorf("always-subscribed uid %s skipped", uid)
						return
					}
				}
				passes.Add(1)
			}
		}()
	}

	renames := 0
	update := func(uid string, remove bool) {
		key := tm.GetChannelTag("g1", 2)
		var err error
		if remove {
			err = tm.RemoveUsers(key, []string{uid})
		} else {
			err = tm.AddUsers(key, []string{uid})
		}
		require.NoError(t, err)
		renames++
		next := fmt.Sprintf("tag-%d", renames)
		require.NoError(t, tm.RenameTag(key, next))
		tm.SetChannelTag("g1", 2, next)
	}
	deadline := time.Now().Add(300 * time.Millisecond)
	for i := 0; time.Now().Before(deadline); i++ {
		uid := churn[i%len(churn)]
		update(uid, true)
		update(uid, false)
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	require.Positive(t, passes.Load())
}
