package manager

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
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

func TestTagSnapshotSurvivesSetChannelTag(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b"})
	inFlight := tm.Get("tag")

	tm.SetChannelTag("g2", 3, "tag")

	require.Equal(t, "g1", inFlight.ChannelId)
	require.EqualValues(t, 2, inFlight.ChannelType)
	require.Equal(t, "g2", tm.Get("tag").ChannelId)
	require.EqualValues(t, 3, tm.Get("tag").ChannelType)
}

// A reader that resolves a channel's key must find a tag already labelled
// with that channel: the mapping is set only after the labelled tag is
// published. Run with -race.
func TestSetChannelTagPublishesTagBeforeMapping(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a"})
	stop := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			key := tm.GetChannelTag("g9", 5)
			if key == "" {
				continue
			}
			if tag := tm.Get(key); tag == nil || tag.ChannelId != "g9" {
				errs <- fmt.Errorf("key %s resolved before its tag was labelled: %v", key, tag)
				return
			}
		}
	}()
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("k%d", i)
		_, err := tm.MakeTagWithTagKey(key, []string{"a"})
		require.NoError(t, err)
		tm.SetChannelTag("g9", 5, key)
	}
	close(stop)
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
}

// uids prefixed "n2" live on node 2, "orphan" on a slot without a leader, the
// rest on node 1.
type multiNodeTestCluster struct{ icluster.ICluster }

func (multiNodeTestCluster) GetSlotId(uid string) uint32 {
	switch {
	case uid == "orphan":
		return 3
	case strings.HasPrefix(uid, "n2"):
		return 2
	}
	return 1
}
func (multiNodeTestCluster) SlotLeaderId(slot uint32) uint64 {
	if slot == 3 {
		return 0
	}
	return uint64(slot)
}

func newMultiNodeTestTagManager(t *testing.T, uids []string) *TagManager {
	t.Helper()
	old := service.Cluster
	service.Cluster = multiNodeTestCluster{}
	t.Cleanup(func() { service.Cluster = old })
	tm := NewTagManager(4, func() uint64 { return 0 })
	_, err := tm.MakeTagWithTagKey("tag", uids)
	require.NoError(t, err)
	tm.SetChannelTag("g1", 2, "tag")
	return tm
}

func nodeUids(tag *types.Tag, leader uint64) []string {
	for _, node := range tag.Nodes {
		if node.LeaderId == leader {
			return node.Uids
		}
	}
	return nil
}

func TestTagSnapshotsAcrossNodes(t *testing.T) {
	tm := newMultiNodeTestTagManager(t, []string{"a1", "a2", "n2a", "n2b"})
	inFlight := tm.Get("tag")
	node1, node2 := nodeUids(inFlight, 1), nodeUids(inFlight, 2)

	require.NoError(t, tm.RemoveUsers("tag", []string{"n2a"}))
	require.NoError(t, tm.AddUsers("tag", []string{"a1", "n2c"}))

	require.Equal(t, []string{"a1", "a2"}, node1)
	require.Equal(t, []string{"n2a", "n2b"}, node2)
	require.ElementsMatch(t, []string{"a1", "a2"}, nodeUids(tm.Get("tag"), 1))
	require.ElementsMatch(t, []string{"n2b", "n2c"}, nodeUids(tm.Get("tag"), 2))
}

// A change that fails part-way must leave the published tag untouched.
func TestFailedMembershipChangeKeepsPublishedTag(t *testing.T) {
	tm := newMultiNodeTestTagManager(t, []string{"a1", "n2a"})
	before := tm.Get("tag")

	require.Error(t, tm.AddUsers("tag", []string{"n2b", "orphan"}))
	require.Error(t, tm.RemoveUsers("tag", []string{"a1", "orphan"}))

	require.Same(t, before, tm.Get("tag"))
	require.Equal(t, []string{"a1"}, nodeUids(before, 1))
	require.Equal(t, []string{"n2a"}, nodeUids(before, 2))
}

// Membership changes are not accesses: they must not keep an unused tag from
// expiring. A rename has always counted as one.
func TestMembershipChangeKeepsAccessState(t *testing.T) {
	tm := newSnapshotTestTagManager(t, []string{"a", "b"})
	tm.Get("tag")
	snap := tm.getTag("tag") // getTag does not record an access
	accessed, count := snap.LastGet(), snap.GetCount.Load()
	time.Sleep(2 * time.Millisecond)

	require.NoError(t, tm.AddUsers("tag", []string{"c"}))
	require.NoError(t, tm.RemoveUsers("tag", []string{"a"}))
	tm.SetChannelTag("g2", 3, "tag")
	got := tm.getTag("tag")
	require.Equal(t, accessed, got.LastGet())
	require.Equal(t, count, got.GetCount.Load())

	require.NoError(t, tm.RenameTag("tag", "tag2"))
	require.True(t, tm.getTag("tag2").LastGet().After(accessed))
}

// /tags reports an emptied node as "uids": [], and later copies keep it so.
func TestEmptiedNodeKeepsEmptyUidList(t *testing.T) {
	tm := newMultiNodeTestTagManager(t, []string{"a1", "n2a"})
	require.NoError(t, tm.RemoveUsers("tag", []string{"n2a"}))
	require.NoError(t, tm.AddUsers("tag", []string{"a2"}))

	var node2 *types.Node
	for _, node := range tm.Get("tag").Nodes {
		if node.LeaderId == 2 {
			node2 = node
		}
	}
	require.NotNil(t, node2)
	data, err := json.Marshal(node2)
	require.NoError(t, err)
	require.Contains(t, string(data), `"uids":[]`)
}
